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
//! | `health()` | `watch::Receiver<HealthStatus>` | sticky terminal state (`Stalled`) |
//! | `consensus_status()` | `watch::Receiver<ConsensusStatus>` | consensus tip / head age / checkpoint |
//! | `take_security_events()` | `mpsc::Receiver<SecurityEvent>` | bounded, `try_send` (may drop on full) |
//! | `events_verbose()` | `broadcast::Receiver<VerificationEvent<N>>` | drop-oldest informational chatter |
//!
//! The mismatch-detection and `Tainted`-flipping paths described in the
//! design issue land alongside the optimistic provider. Phase 1 surfaces
//! only failures (transport errors, consensus client failures); a verified
//! call that mismatches against an unverified value is not yet
//! detectable from this module.
//!
//! [`Provider<N>`]: alloy::providers::Provider
//! [`Unverifiable<T>`]: value::Unverifiable

pub mod error;
pub mod event;
pub mod status;
pub mod value;
pub mod verified;

#[cfg(test)]
mod tests;

pub use error::{FailureInfo, VerificationError};
pub use event::{
    ConsensusStatus, HealthStatus, SecurityEvent, SkipReason, VerificationCounts,
    VerificationEvent, VerifiedSnapshot,
};
pub use status::VerificationStatus;
pub use value::{Unverifiable, VerifiedValue};
pub use verified::VerifiedHeliosProvider;
