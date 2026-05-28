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

pub use helios_core::provider::{
    HealthStatus, SecurityEvent, Unverifiable, VerificationCounts, VerificationError,
    VerificationEvent, VerificationStatus, VerifiedHeliosProvider, VerifiedValue,
};
