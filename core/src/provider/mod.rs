//! Alloy [`Provider<N>`]-compatible read surface for helios.
//!
//! This module exposes [`VerifiedHeliosProvider<N>`] — a drop-in
//! `alloy::providers::Provider<N>` whose read methods block until
//! consensus-anchored verification has succeeded. Methods that helios
//! cannot meaningfully verify (gas estimators, fee history,
//! `block_number` at tip) return [`Unverifiable<T>`] from inherent
//! methods, forcing the caller to acknowledge they are trusting the RPC
//! at that call site.
//!
//! The optimistic-first companion (`OptimisticHeliosProvider`),
//! per-screen [`scope`]-based barriers, and the [`Routing`] enum land in
//! Phase 2 — see issue #15 for the full design.
//!
//! ## Channel layout
//!
//! Verification activity is observable via four orthogonal channels on
//! [`VerificationStatus`]:
//!
//! | channel | type | semantics |
//! |---|---|---|
//! | `counts()` | `watch::Receiver<VerificationCounts>` | latest-value; cheap-poll for UIs |
//! | `health()` | `watch::Receiver<HealthStatus>` | sticky terminal state; `Tainted` flipped synchronously |
//! | `consensus_status()` | `watch::Receiver<ConsensusStatus>` | consensus tip / head age / checkpoint |
//! | `take_security_events()` | `mpsc::Receiver<SecurityEvent>` | bounded, backpressured — never drops |
//! | `events_verbose()` | `broadcast::Receiver<VerificationEvent<N>>` | drop-oldest informational chatter |
//!
//! The load-bearing security invariant: `HealthStatus::Tainted` flips
//! synchronously on the first mismatch, *before* the security_events
//! queue. This means the trust signal cannot be lost regardless of
//! event-stream backpressure — late or slow consumers of `health()` still
//! observe the tainted state correctly.
//!
//! [`Provider<N>`]: alloy::providers::Provider
//! [`Unverifiable<T>`]: value::Unverifiable
//! [`scope`]: VerificationStatus
//! [`Routing`]: VerifiedHeliosProvider

pub mod error;
pub mod event;
pub mod status;
pub mod value;
pub mod verified;

pub use error::{FailureInfo, MismatchInfo, VerificationError};
pub use event::{
    ConsensusStatus, HealthStatus, SecurityEvent, SkipReason, VerificationCounts,
    VerificationEvent, VerifiedSnapshot,
};
pub use status::VerificationStatus;
pub use value::{Unverifiable, VerifiedValue};
pub use verified::VerifiedHeliosProvider;
