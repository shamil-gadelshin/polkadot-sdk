// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! `SyncingEngine` is the actor responsible for syncing Substrate chain
//! to tip and keep the blockchain up to date with network updates.

mod syncing_service;

use crate::{
	pending_responses::{PendingResponses, ResponseEvent},
	schema::v1::{StateRequest, StateResponse},
	service::{
		self,
	},
	strategy::{
		state::StateStrategy
	},
	types::{
		BadPeer, ExtendedPeerInfo, OpaqueStateRequest, OpaqueStateResponse, PeerRequest, SyncEvent,
	},
	fast_sync_engine::syncing_service::{SyncingService, ToServiceCommand},
	LOG_TARGET,
};

use futures::{
	channel::oneshot,
	FutureExt, StreamExt,
};
use libp2p::{request_response::OutboundFailure, PeerId};
use log::{debug, error, info, trace};
use prost::Message;

use sc_client_api::{BlockBackend, ProofProvider};
use sc_network::{
	request_responses::{IfDisconnected, RequestFailure},
	types::ProtocolName,
	utils::LruHashSet,
};
use sc_utils::mpsc::{tracing_unbounded, TracingUnboundedReceiver, TracingUnboundedSender};
use sp_blockchain::{Error as ClientError};
use sp_runtime::{Justifications, traits::{Block as BlockT,}};
use std::{
	collections::{HashMap},
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	time::{ Instant },
};
use sc_consensus::import_queue::ImportQueueService;
use sc_consensus::IncomingBlock;
use sp_runtime::traits::{NumberFor, Zero};
use crate::state_request_handler::generate_protocol_name;
use crate::strategy::state::StateStrategyAction;

/// Peer information
#[derive(Clone, Debug)]
pub struct Peer<B: BlockT> {
	pub info: ExtendedPeerInfo<B>,
	/// Holds a set of blocks known to this peer.
	pub known_blocks: LruHashSet<B::Hash>,
}

mod rep {
	use sc_network::ReputationChange as Rep;
	/// We received a message that failed to decode.
	pub const BAD_MESSAGE: Rep = Rep::new(-(1 << 12), "Bad message");
	/// Peer is on unsupported protocol version.
	pub const BAD_PROTOCOL: Rep = Rep::new_fatal("Unsupported protocol");
	/// Reputation change when a peer refuses a request.
	pub const REFUSED: Rep = Rep::new(-(1 << 10), "Request refused");
	/// Reputation change when a peer doesn't respond in time to our messages.
	pub const TIMEOUT: Rep = Rep::new(-(1 << 10), "Request timeout");
}

pub struct FastSyncingEngine<B: BlockT, IQS> where
	IQS: ImportQueueService<B> + ?Sized,
{
	/// Syncing strategy.
	strategy: StateStrategy<B>,

	/// Number of peers we're connected to.
	num_connected: Arc<AtomicUsize>,

	/// Are we actively catching up with the chain?
	is_major_syncing: Arc<AtomicBool>,

	/// Network service.
	network_service: service::network::NetworkServiceHandle,

	/// Channel for receiving service commands
	service_rx: TracingUnboundedReceiver<ToServiceCommand<B>>,

	/// Set of channels for other protocols that have subscribed to syncing events.
	event_streams: Vec<TracingUnboundedSender<SyncEvent>>,

	/// All connected peers. Contains both full and light node peers.
	peers: HashMap<PeerId, Peer<B>>,

	/// When the syncing was started.
	///
	/// Stored as an `Option<Instant>` so once the initial wait has passed, `SyncingEngine`
	/// can reset the peer timers and continue with the normal eviction process.
	syncing_started: Option<Instant>,

	/// Pending responses
	pending_responses: PendingResponses<B>,

	/// Protocol name used to send out state requests
	state_request_protocol_name: ProtocolName,

	/// Handle to import queue.
	import_queue: Box<IQS>,

	last_block: Option<IncomingBlock<B>>,
}

impl<B: BlockT, IQS> FastSyncingEngine<B, IQS>
where
	B: BlockT,
	IQS: ImportQueueService<B> + ?Sized,
{
	pub fn new<Client: BlockBackend<B>
	+ ProofProvider<B>
	+ Send
	+ Sync
	+ 'static>(
		client: Arc<Client>,
		import_queue: Box<IQS>,
		network_service: service::network::NetworkServiceHandle,
		fork_id: Option<&str>,
		target_header: B::Header,
		target_body: Option<Vec<B::Extrinsic>>,
		target_justifications: Option<Justifications>,
		skip_proof: bool,
		initial_peers: impl Iterator<Item = (PeerId, NumberFor<B>)>,
	) -> Result<(Self, SyncingService<B>,), ClientError> {
		let genesis_hash = client
			.block_hash(Zero::zero())
			.ok()
			.flatten()
			.expect("Genesis block exists; qed");
		let state_request_protocol_name = generate_protocol_name(genesis_hash, fork_id).into();

		// Initialize syncing strategy.
		let strategy =
			StateStrategy::new(client.clone(), target_header, target_body, target_justifications, skip_proof, initial_peers);

		let (tx, service_rx) = tracing_unbounded("mpsc_chain_sync", 100_000);
		let num_connected = Arc::new(AtomicUsize::new(0));
		let is_major_syncing = Arc::new(AtomicBool::new(false));

		Ok((
			Self {
				import_queue,
				strategy,
				network_service,
				peers: HashMap::new(),
				num_connected: num_connected.clone(),
				is_major_syncing: is_major_syncing.clone(),
				service_rx,
				event_streams: Vec::new(),
				syncing_started: None,
				pending_responses: PendingResponses::new(),
				state_request_protocol_name,
				last_block: None,
			},
			SyncingService::new(tx, num_connected, is_major_syncing),
		))
	}

	pub async fn run(mut self) -> Option<IncomingBlock<B>> {
		self.syncing_started = Some(Instant::now());

		loop {
			tokio::select! {
				command = self.service_rx.select_next_some() =>
					self.process_service_command(command),
				response_event = self.pending_responses.select_next_some() =>
					self.process_response_event(response_event),
			}

			// Update atomic variables
			self.num_connected.store(self.peers.len(), Ordering::Relaxed);
			self.is_major_syncing.store(true, Ordering::Relaxed);

			// Process actions requested by a syncing strategy.
			match self.process_strategy_actions() {
				Ok(Some(_)) => {
					continue;
				}
				Ok(None) => {
					info!("State import finished.");
					break;
				}
				Err(e) => {
					error!("Terminating `SyncingEngine` due to fatal error: {e:?}");
					return None
				}
			}
		}

		return self.last_block.take();
	}

	fn process_strategy_actions(&mut self) -> Result<Option<()>, ClientError> {
		let actions = self.strategy.actions().collect::<Vec<_>>();
		if actions.is_empty(){
			return Err(ClientError::Backend("Fast sync failed - no further actions.".into()))
		}

		for action in actions.into_iter() {
			match action {
				StateStrategyAction::SendStateRequest { peer_id, request } => {
					println!("Sending state request: {peer_id}");
					self.send_state_request(peer_id, request);
				}
				StateStrategyAction::DropPeer(BadPeer(peer_id, rep)) => {
					self.pending_responses.remove(&peer_id);
					self.network_service
						.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					self.network_service.report_peer(peer_id, rep);

					trace!(target: LOG_TARGET, "{peer_id:?} dropped: {rep:?}.");
				}
				StateStrategyAction::ImportBlocks { origin, blocks } => {
					self.last_block = blocks.first().cloned();
					let block_len = blocks.len();
					self.import_queue.import_blocks(origin, blocks);

					info!("Import blocks finished, blocks len = {block_len}", );
					return Ok(None)
				}
				StateStrategyAction::Finished => {
					println!("StateStrategyAction::Finished");
				}
			}
		}

		Ok(Some(()))
	}

	fn process_service_command(&mut self, command: ToServiceCommand<B>) {
		match command {
			ToServiceCommand::Status(tx) => {
				let mut status = self.strategy.status();
				status.num_connected_peers = self.peers.len() as u32;
				let _ = tx.send(status);
			},
			ToServiceCommand::PeersInfo(tx) => {
				let peers_info = self
					.peers
					.iter()
					.map(|(peer_id, peer)| (*peer_id, peer.info.clone()))
					.collect();
				let _ = tx.send(peers_info);
			},
			ToServiceCommand::Start(tx) => {
				let _ = tx.send(());
			}
		}
	}

	fn send_state_request(&mut self, peer_id: PeerId, request: OpaqueStateRequest) {
		let (tx, rx) = oneshot::channel();

		self.pending_responses.insert(peer_id, PeerRequest::State, rx.boxed());

		match Self::encode_state_request(&request) {
			Ok(data) => {
				println!("Preparing state request: {}, peer_id={peer_id}", data.len());
				self.network_service.start_request(
					peer_id,
					self.state_request_protocol_name.clone(),
					data,
					tx,
					IfDisconnected::ImmediateError,
				);
			},
			Err(err) => {
				log::warn!(
					target: LOG_TARGET,
					"Failed to encode state request {request:?}: {err:?}",
				);
			},
		}
	}

	fn encode_state_request(request: &OpaqueStateRequest) -> Result<Vec<u8>, String> {
		let request: &StateRequest = request.0.downcast_ref().ok_or_else(|| {
			"Failed to downcast opaque state response during encoding, this is an \
				implementation bug."
				.to_string()
		})?;

		Ok(request.encode_to_vec())
	}

	fn decode_state_response(response: &[u8]) -> Result<OpaqueStateResponse, String> {
		println!("decode_state_response: {}", response.len());
		let response = StateResponse::decode(response)
			.map_err(|error| format!("Failed to decode state response: {error}"))?;

		Ok(OpaqueStateResponse(Box::new(response)))
	}

	fn process_response_event(&mut self, response_event: ResponseEvent<B>) {
		let ResponseEvent { peer_id, request, response } = response_event;
		println!("Process response event: {peer_id}");

		match response {
			Ok(Ok((resp, _))) => match request {
				PeerRequest::Block(req) => {
					error!("Unexpected PeerRequest::Block - {:?}", req);
				},
				PeerRequest::State => {
					let response = match Self::decode_state_response(&resp[..]) {
						Ok(proto) => proto,
						Err(e) => {
							debug!(
								target: LOG_TARGET,
								"Failed to decode state response from peer {peer_id:?}: {e:?}.",
							);
							self.network_service.report_peer(peer_id, rep::BAD_MESSAGE);
							self.network_service.disconnect_peer(
								peer_id,
								self.state_request_protocol_name.clone(),
							);
							return
						},
					};

					self.strategy.on_state_response(peer_id, response);
				},
				PeerRequest::WarpProof => {
					error!("Unexpected PeerRequest::WarpProof",);
				},
			},
			Ok(Err(e)) => {
				debug!(target: LOG_TARGET, "Request to peer {peer_id:?} failed: {e:?}.");

				match e {
					RequestFailure::Network(OutboundFailure::Timeout) => {
						self.network_service.report_peer(peer_id, rep::TIMEOUT);
						self.network_service
							.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					},
					RequestFailure::Network(OutboundFailure::UnsupportedProtocols) => {
						self.network_service.report_peer(peer_id, rep::BAD_PROTOCOL);
						self.network_service
							.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					},
					RequestFailure::Network(OutboundFailure::DialFailure) => {
						self.network_service
							.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					},
					RequestFailure::Refused => {
						self.network_service.report_peer(peer_id, rep::REFUSED);
						self.network_service
							.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					},
					RequestFailure::Network(OutboundFailure::ConnectionClosed) |
					RequestFailure::NotConnected => {
						self.network_service
							.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
					},
					RequestFailure::UnknownProtocol => {
						debug_assert!(false, "Block request protocol should always be known.");
					},
					RequestFailure::Obsolete => {
						debug_assert!(
							false,
							"Can not receive `RequestFailure::Obsolete` after dropping the \
								response receiver.",
						);
					},
				}
			},
			Err(oneshot::Canceled) => {
				trace!(
					target: LOG_TARGET,
					"Request to peer {peer_id:?} failed due to oneshot being canceled.",
				);
				self.network_service
					.disconnect_peer(peer_id, self.state_request_protocol_name.clone());
			},
		}
	}

	/// Returns the number of peers we're connected to and that are being queried.
	fn num_active_peers(&self) -> usize {
		self.pending_responses.len()
	}
}
