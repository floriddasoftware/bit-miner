//! bit_minner — Bitcoin observation / settlement support for QTM.
//!
//! Public surface used by qtm-api:
//! - [`BitcoinObservationPipeline`] (`bitcoinObserverpipeline.rs`)
//! - [`RenderState`] (`render.rs`)
//! - shared authorization / settlement types (`sharedtypes.rs`)

// Module paths preserve existing on-disk filenames (mixed case).

pub mod sharedtypes;

#[path = "broadcast.rs"]
pub mod broadcast;

#[path = "bitcoinreceipt.rs"]
pub mod bitcoinreceipt;

#[path = "bitcoinpayload.rs"]
pub mod bitcoinpayload;

#[path = "bitcoinmine.rs"]
pub mod bitcoinmine;

#[path = "miner.rs"]
pub mod miner;

#[path = "bitcoindispatch.rs"]
pub mod bitcoindispatch;

#[path = "render.rs"]
pub mod render;

#[path = "bitcoinObserverpipeline.rs"]
pub mod bitcoin_observation_pipeline;

// --- Primary exports for qtm-api ---

pub use bitcoin_observation_pipeline::BitcoinObservationPipeline;
pub use render::RenderState;

pub use sharedtypes::{
    Authorization, AuthorizationState, SettlementRequest, SettlementState,
};