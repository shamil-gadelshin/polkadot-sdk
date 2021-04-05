// Copyright 2019-2020 Parity Technologies (UK) Ltd.
// This file is part of Parity Bridges Common.

// Parity Bridges Common is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity Bridges Common is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity Bridges Common.  If not, see <http://www.gnu.org/licenses/>.

//! Autogenerated weights for pallet_bridge_messages
//!
//! THIS FILE WAS AUTO-GENERATED USING THE SUBSTRATE BENCHMARK CLI VERSION 2.0.1
//! DATE: 2021-02-11, STEPS: [50, ], REPEAT: 20
//! LOW RANGE: [], HIGH RANGE: []
//! EXECUTION: Some(Wasm), WASM-EXECUTION: Compiled
//! CHAIN: Some("local"), DB CACHE: 128

// Executed Command:
// target/release/rialto-bridge-node
// benchmark
// --chain=local
// --steps=50
// --repeat=20
// --pallet=pallet_bridge_messages
// --extrinsic=*
// --execution=wasm
// --wasm-execution=Compiled
// --heap-pages=4096
// --output=./modules/messages/src/weights.rs
// --template=./.maintain/rialto-weight-template.hbs

#![allow(clippy::all)]
#![allow(unused_parens)]
#![allow(unused_imports)]

use frame_support::{
	traits::Get,
	weights::{constants::RocksDbWeight, Weight},
};
use sp_std::marker::PhantomData;

/// Weight functions needed for pallet_bridge_messages.
pub trait WeightInfo {
	fn send_minimal_message_worst_case() -> Weight;
	fn send_1_kb_message_worst_case() -> Weight;
	fn send_16_kb_message_worst_case() -> Weight;
	fn increase_message_fee() -> Weight;
	fn receive_single_message_proof() -> Weight;
	fn receive_two_messages_proof() -> Weight;
	fn receive_single_message_proof_with_outbound_lane_state() -> Weight;
	fn receive_single_message_proof_1_kb() -> Weight;
	fn receive_single_message_proof_16_kb() -> Weight;
	fn receive_delivery_proof_for_single_message() -> Weight;
	fn receive_delivery_proof_for_two_messages_by_single_relayer() -> Weight;
	fn receive_delivery_proof_for_two_messages_by_two_relayers() -> Weight;
	fn send_messages_of_various_lengths(i: u32) -> Weight;
	fn receive_multiple_messages_proof(i: u32) -> Weight;
	fn receive_message_proofs_with_extra_nodes(i: u32) -> Weight;
	fn receive_message_proofs_with_large_leaf(i: u32) -> Weight;
	fn receive_multiple_messages_proof_with_outbound_lane_state(i: u32) -> Weight;
	fn receive_delivery_proof_for_multiple_messages_by_single_relayer(i: u32) -> Weight;
	fn receive_delivery_proof_for_multiple_messages_by_multiple_relayers(i: u32) -> Weight;
}

/// Weights for pallet_bridge_messages using the Rialto node and recommended hardware.
pub struct RialtoWeight<T>(PhantomData<T>);
impl<T: frame_system::Config> WeightInfo for RialtoWeight<T> {
	fn send_minimal_message_worst_case() -> Weight {
		(140_645_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(4 as Weight))
			.saturating_add(T::DbWeight::get().writes(12 as Weight))
	}
	fn send_1_kb_message_worst_case() -> Weight {
		(146_434_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(4 as Weight))
			.saturating_add(T::DbWeight::get().writes(12 as Weight))
	}
	fn send_16_kb_message_worst_case() -> Weight {
		(214_721_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(4 as Weight))
			.saturating_add(T::DbWeight::get().writes(12 as Weight))
	}
	fn increase_message_fee() -> Weight {
		(8_395_221_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(4 as Weight))
			.saturating_add(T::DbWeight::get().writes(3 as Weight))
	}
	fn receive_single_message_proof() -> Weight {
		(156_390_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_two_messages_proof() -> Weight {
		(269_316_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_with_outbound_lane_state() -> Weight {
		(174_342_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_1_kb() -> Weight {
		(186_621_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_16_kb() -> Weight {
		(487_028_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_delivery_proof_for_single_message() -> Weight {
		(144_893_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(6 as Weight))
			.saturating_add(T::DbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_two_messages_by_single_relayer() -> Weight {
		(151_134_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(7 as Weight))
			.saturating_add(T::DbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_two_messages_by_two_relayers() -> Weight {
		(212_650_000 as Weight)
			.saturating_add(T::DbWeight::get().reads(8 as Weight))
			.saturating_add(T::DbWeight::get().writes(4 as Weight))
	}
	fn send_messages_of_various_lengths(i: u32) -> Weight {
		(88_670_000 as Weight)
			.saturating_add((5_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(4 as Weight))
			.saturating_add(T::DbWeight::get().writes(12 as Weight))
	}
	fn receive_multiple_messages_proof(i: u32) -> Weight {
		(0 as Weight)
			.saturating_add((125_956_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_message_proofs_with_extra_nodes(i: u32) -> Weight {
		(462_389_000 as Weight)
			.saturating_add((11_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_message_proofs_with_large_leaf(i: u32) -> Weight {
		(120_744_000 as Weight)
			.saturating_add((8_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_multiple_messages_proof_with_outbound_lane_state(i: u32) -> Weight {
		(0 as Weight)
			.saturating_add((130_087_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(3 as Weight))
			.saturating_add(T::DbWeight::get().writes(1 as Weight))
	}
	fn receive_delivery_proof_for_multiple_messages_by_single_relayer(i: u32) -> Weight {
		(126_833_000 as Weight)
			.saturating_add((7_793_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(5 as Weight))
			.saturating_add(T::DbWeight::get().reads((1 as Weight).saturating_mul(i as Weight)))
			.saturating_add(T::DbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_multiple_messages_by_multiple_relayers(i: u32) -> Weight {
		(71_269_000 as Weight)
			.saturating_add((72_377_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(T::DbWeight::get().reads(5 as Weight))
			.saturating_add(T::DbWeight::get().reads((2 as Weight).saturating_mul(i as Weight)))
			.saturating_add(T::DbWeight::get().writes(3 as Weight))
			.saturating_add(T::DbWeight::get().writes((1 as Weight).saturating_mul(i as Weight)))
	}
}

// For backwards compatibility and tests
impl WeightInfo for () {
	fn send_minimal_message_worst_case() -> Weight {
		(140_645_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(4 as Weight))
			.saturating_add(RocksDbWeight::get().writes(12 as Weight))
	}
	fn send_1_kb_message_worst_case() -> Weight {
		(146_434_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(4 as Weight))
			.saturating_add(RocksDbWeight::get().writes(12 as Weight))
	}
	fn send_16_kb_message_worst_case() -> Weight {
		(214_721_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(4 as Weight))
			.saturating_add(RocksDbWeight::get().writes(12 as Weight))
	}
	fn increase_message_fee() -> Weight {
		(8_395_221_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(4 as Weight))
			.saturating_add(RocksDbWeight::get().writes(3 as Weight))
	}
	fn receive_single_message_proof() -> Weight {
		(156_390_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_two_messages_proof() -> Weight {
		(269_316_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_with_outbound_lane_state() -> Weight {
		(174_342_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_1_kb() -> Weight {
		(186_621_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_single_message_proof_16_kb() -> Weight {
		(487_028_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_delivery_proof_for_single_message() -> Weight {
		(144_893_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(6 as Weight))
			.saturating_add(RocksDbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_two_messages_by_single_relayer() -> Weight {
		(151_134_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(7 as Weight))
			.saturating_add(RocksDbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_two_messages_by_two_relayers() -> Weight {
		(212_650_000 as Weight)
			.saturating_add(RocksDbWeight::get().reads(8 as Weight))
			.saturating_add(RocksDbWeight::get().writes(4 as Weight))
	}
	fn send_messages_of_various_lengths(i: u32) -> Weight {
		(88_670_000 as Weight)
			.saturating_add((5_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(4 as Weight))
			.saturating_add(RocksDbWeight::get().writes(12 as Weight))
	}
	fn receive_multiple_messages_proof(i: u32) -> Weight {
		(0 as Weight)
			.saturating_add((125_956_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_message_proofs_with_extra_nodes(i: u32) -> Weight {
		(462_389_000 as Weight)
			.saturating_add((11_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_message_proofs_with_large_leaf(i: u32) -> Weight {
		(120_744_000 as Weight)
			.saturating_add((8_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_multiple_messages_proof_with_outbound_lane_state(i: u32) -> Weight {
		(0 as Weight)
			.saturating_add((130_087_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes(1 as Weight))
	}
	fn receive_delivery_proof_for_multiple_messages_by_single_relayer(i: u32) -> Weight {
		(126_833_000 as Weight)
			.saturating_add((7_793_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(5 as Weight))
			.saturating_add(RocksDbWeight::get().reads((1 as Weight).saturating_mul(i as Weight)))
			.saturating_add(RocksDbWeight::get().writes(3 as Weight))
	}
	fn receive_delivery_proof_for_multiple_messages_by_multiple_relayers(i: u32) -> Weight {
		(71_269_000 as Weight)
			.saturating_add((72_377_000 as Weight).saturating_mul(i as Weight))
			.saturating_add(RocksDbWeight::get().reads(5 as Weight))
			.saturating_add(RocksDbWeight::get().reads((2 as Weight).saturating_mul(i as Weight)))
			.saturating_add(RocksDbWeight::get().writes(3 as Weight))
			.saturating_add(RocksDbWeight::get().writes((1 as Weight).saturating_mul(i as Weight)))
	}
}