//! Alloy [`Provider<N>`]-compatible read surface for helios.
//!
//! [`VerifiedHeliosProvider<N>`] is a drop-in
//! `alloy::providers::Provider<N>` whose helios-backable read methods
//! block until consensus-anchored verification has succeeded. Methods
//! that cannot be backed by consensus proofs (gas estimators, fee
//! history, `block_number` at tip) return [`Unverifiable<T>`] from
//! inherent methods, forcing the caller to acknowledge at the call site
//! that they are trusting the RPC.
//!
//! ## Channel layout
//!
//! Verification activity is observable via four orthogonal channels on
//! [`VerificationStatus`]:
//!
//! | channel | type | semantics |
//! |---|---|---|
//! | `counts()` | `watch::Receiver<VerificationCounts>` | latest-value; cheap-poll for UIs |
//! | `health()` | `watch::Receiver<HealthStatus>` | sticky terminal state; `Tainted` flipped synchronously by the verifier |
//! | `consensus_status()` | `watch::Receiver<ConsensusStatus>` | consensus tip / head age / checkpoint |
//! | `take_security_events()` | `mpsc::Receiver<SecurityEvent>` | bounded, `try_send` (may drop on full) |
//! | `events_verbose()` | `broadcast::Receiver<VerificationEvent<N>>` | drop-oldest informational chatter |
//!
//! Load-bearing security invariant: `HealthStatus::Tainted` flips
//! synchronously on the first mismatch, **before** the security_events
//! queue is touched. Sign-gating that reads `health()` sees the taint
//! regardless of `security_events` backpressure or consumer scheduling.
//! Mismatches are produced by [`OptimisticHeliosProvider`]'s verifier
//! tasks; the verified-blocking [`VerifiedHeliosProvider`] does not
//! produce mismatches directly (it returns only consensus-anchored
//! values), but observes them on `health()` when both providers share
//! a [`VerificationStatus`].
//!
//! [`Provider<N>`]: alloy::providers::Provider
//! [`Unverifiable<T>`]: value::Unverifiable

pub mod builder;
pub mod error;
pub mod event;
pub mod optimistic;
pub mod persistence;
pub mod status;
pub mod value;
pub mod verified;

#[cfg(test)]
mod tests;

pub use builder::{HeliosProviderBuilder, Routing};
pub use error::{FailureInfo, MismatchInfo, VerificationError};
pub use event::{
    ConsensusStatus, HealthStatus, SecurityEvent, SkipReason, VerificationCounts,
    VerificationEvent, VerifiedSnapshot,
};
pub use optimistic::OptimisticHeliosProvider;
pub use persistence::{FileTaintStore, TaintConfig, TaintStore};
pub use status::{Scope, VerificationStatus};
pub use value::{Unverifiable, VerifiedValue};
pub use verified::{ChainIdMismatch, VerifiedHeliosProvider};
