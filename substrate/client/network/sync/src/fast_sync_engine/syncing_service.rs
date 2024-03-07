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

use crate::types::{ExtendedPeerInfo, SyncEvent, SyncEventStream, SyncStatus, SyncStatusProvider};

use futures::{channel::oneshot, Stream};
use libp2p::PeerId;

use sc_consensus::{BlockImportError, BlockImportStatus, JustificationSyncLink, Link};
use sc_network::{NetworkBlock, NetworkSyncForkRequest};
use sc_utils::mpsc::{tracing_unbounded, TracingUnboundedSender};
use sp_runtime::traits::{Block as BlockT, NumberFor};

use std::{
	pin::Pin,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
};

/// Commands send to `SyncingEngine`
pub enum ToServiceCommand<B: BlockT> {
	Start(oneshot::Sender<()>),
	Status(oneshot::Sender<SyncStatus<B>>),
	PeersInfo(oneshot::Sender<Vec<(PeerId, ExtendedPeerInfo<B>)>>),
}

/// Handle for communicating with `SyncingEngine` asynchronously
#[derive(Clone)]
pub struct SyncingService<B: BlockT> {
	tx: TracingUnboundedSender<ToServiceCommand<B>>,
	/// Number of peers we're connected to.
	num_connected: Arc<AtomicUsize>,
	/// Are we actively catching up with the chain?
	is_major_syncing: Arc<AtomicBool>,
}

impl<B: BlockT> SyncingService<B> {
	/// Create new handle
	pub fn new(
		tx: TracingUnboundedSender<ToServiceCommand<B>>,
		num_connected: Arc<AtomicUsize>,
		is_major_syncing: Arc<AtomicBool>,
	) -> Self {
		Self { tx, num_connected, is_major_syncing }
	}

	/// Get peer information.
	pub async fn peers_info(
		&self,
	) -> Result<Vec<(PeerId, ExtendedPeerInfo<B>)>, oneshot::Canceled> {
		let (tx, rx) = oneshot::channel();
		let _ = self.tx.unbounded_send(ToServiceCommand::PeersInfo(tx));

		rx.await
	}

	/// Get sync status
	///
	/// Returns an error if `SyncingEngine` has terminated.
	pub async fn status(&self) -> Result<SyncStatus<B>, oneshot::Canceled> {
		let (tx, rx) = oneshot::channel();
		let _ = self.tx.unbounded_send(ToServiceCommand::Status(tx));

		rx.await
	}

	pub async fn start(&self) -> Result<(), oneshot::Canceled> {
		let (tx, rx) = oneshot::channel();
		let _ = self.tx.unbounded_send(ToServiceCommand::Start(tx));

		rx.await
	}
}

// #[async_trait::async_trait]
// impl<B: BlockT> SyncStatusProvider<B> for SyncingService<B> {
// 	/// Get high-level view of the syncing status.
// 	async fn status(&self) -> Result<SyncStatus<B>, ()> {
// 		let (rtx, rrx) = oneshot::channel();
//
// 		let _ = self.tx.unbounded_send(ToServiceCommand::Status(rtx));
// 		rrx.await.map_err(|_| ())
// 	}
// }
