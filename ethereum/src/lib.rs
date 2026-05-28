use helios_core::client::HeliosClient;
use spec::Ethereum;

pub mod builder;
#[cfg(not(target_arch = "wasm32"))]
pub mod code_store;
pub mod config;
pub mod consensus;
pub mod database;
pub(crate) mod evm;
pub mod rpc;
pub mod spec;

mod constants;

pub use builder::EthereumClientBuilder;
pub type EthereumClient = HeliosClient<Ethereum>;

// Phase 1 scaffold of the new alloy `Provider<N>`-compatible surface
// (issue nxm-rs/helios#15, tracking impl nxm-rs/helios#16). Re-exported
// here so embedders can reach `VerifiedHeliosProvider<Ethereum>` without
// importing through `helios-core`. The `EthereumClient` alias above
// continues to point at the legacy `HeliosClient<Ethereum>` for one
// release; it will be re-pointed once `VerifiedHeliosProvider` is
// feature-complete.
pub use helios_core::provider::{
    HealthStatus, SecurityEvent, Unverifiable, VerificationCounts, VerificationError,
    VerificationEvent, VerificationStatus, VerifiedHeliosProvider, VerifiedValue,
};
