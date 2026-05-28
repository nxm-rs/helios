//! Event / state types emitted by the provider's verification machinery.
//!
//! Three channels, by design:
//!
//! - [`VerificationCounts`] is delivered via `watch` — latest-value
//!   semantics, cheap to poll at frame rate (Flutter / RN UIs).
//! - [`HealthStatus`] is also `watch` — sticky terminal state for
//!   `Tainted` / `Stalled`. `Tainted` flips synchronously on the first
//!   mismatch, before the security_events queue, so the trust signal
//!   cannot be lost regardless of event-stream backpressure. This is the
//!   load-bearing security invariant.
//! - [`SecurityEvent`] is delivered via bounded `mpsc` with
//!   producer-backpressure — the verifier task awaits if the consumer is
//!   slow, never drops a security event.
//! - [`VerificationEvent`] is delivered via `broadcast` (drop-oldest) —
//!   informational chatter where lossy is acceptable.
//!
//! See issue #15 for the design.

use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;

use crate::provider::error::{FailureInfo, MismatchInfo};
use crate::provider::value::VerifiedValue;

/// Counters for verification activity, delivered via `watch::Receiver`.
#[derive(Debug, Clone, Default)]
pub struct VerificationCounts {
    pub pending: usize,
    pub verified: usize,
    pub mismatched: usize,
    pub failed: usize,
    pub last_change_at: Option<Instant>,
}

/// Sticky terminal state of the provider, delivered via `watch::Receiver`.
///
/// Late subscribers see current state immediately (e.g. `Tainted` survives
/// even if the subscriber joined after the mismatch).
#[derive(Debug, Clone)]
pub enum HealthStatus {
    Healthy,
    /// Consensus client hasn't made progress for longer than the configured
    /// stall threshold. The library auto-retries up to a bounded number of
    /// times; this state remains until either recovery succeeds or
    /// `on_exhausted` policy triggers.
    Stalled {
        since: Instant,
        restart_attempts: u32,
    },
    /// At least one mismatch has been observed against the configured
    /// execution RPC for this `(execution_rpc_url, chain_id)`. Cleared via
    /// `VerificationStatus::acknowledge_mismatch`.
    Tainted {
        first_mismatch: Box<MismatchInfo>,
    },
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Healthy
    }
}

/// Security-critical events. Delivered via bounded
/// `mpsc::Receiver<SecurityEvent>` with producer-backpressure semantics —
/// the verifier task awaits if the consumer is slow, never drops.
///
/// The actual trust signal lives on [`HealthStatus`]; this channel carries
/// the diagnostic detail (which call, which values).
#[derive(Debug, Clone)]
pub enum SecurityEvent {
    Mismatch(MismatchInfo),
    Failed(FailureInfo),
}

/// Informational verification events. Delivered via
/// `broadcast::Receiver<VerificationEvent>` with drop-oldest semantics —
/// `RecvError::Lagged(n)` surfaces as a synthetic [`VerificationEvent::Dropped`].
pub enum VerificationEvent<N: NetworkSpec> {
    /// A background verification completed successfully.
    Verified {
        method: &'static str,
        value: VerifiedValue<N>,
        took: Duration,
    },
    /// A method was skipped (mempool subscription, helios-can't-verify
    /// method, etc.).
    Skipped {
        method: &'static str,
        reason: SkipReason,
    },
    /// Synthetic event indicating the consumer fell behind and missed
    /// `count` informational events. Security events are never dropped, so
    /// this never indicates a missed `SecurityEvent`.
    Dropped {
        count: u64,
    },
}

// `VerifiedValue` is not `Clone` if `N::BlockResponse` etc. aren't, but
// the broadcast channel needs the message to be `Clone`. For the scaffold
// we just `impl Clone` manually so the types compose; later refinement
// may demand `Clone` bounds on `N`'s associated types.
impl<N: NetworkSpec> Clone for VerificationEvent<N> {
    fn clone(&self) -> Self {
        match self {
            Self::Verified { method, value, took } => Self::Verified {
                method,
                value: value.clone(),
                took: *took,
            },
            Self::Skipped { method, reason } => Self::Skipped {
                method,
                reason: reason.clone(),
            },
            Self::Dropped { count } => Self::Dropped { count: *count },
        }
    }
}

impl<N: NetworkSpec> std::fmt::Debug for VerificationEvent<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verified { method, took, .. } => {
                f.debug_struct("Verified").field("method", method).field("took", took).finish()
            }
            Self::Skipped { method, reason } => {
                f.debug_struct("Skipped").field("method", method).field("reason", reason).finish()
            }
            Self::Dropped { count } => f.debug_struct("Dropped").field("count", count).finish(),
        }
    }
}

/// Why a method was skipped by the verification machinery.
#[derive(Debug, Clone)]
pub enum SkipReason {
    /// Pre-consensus by definition (mempool subscriptions).
    Mempool,
    /// Method has no on-chain object to verify (e.g. `chain_id`, `net_version`).
    Unverifiable,
    /// Method is intentionally drift-tolerant (`block_number` at tip).
    DriftTolerant,
    /// Helios doesn't recognise the method (arbitrary `raw_request` etc.).
    UnknownMethod,
}

/// Snapshot returned by a successful [`super::VerificationStatus::barrier`]
/// wait. Carries the consensus tip at which the wait succeeded; the
/// signing path can refuse if this snapshot is too old by the time
/// signing finishes.
#[derive(Debug, Clone)]
pub struct VerifiedSnapshot {
    pub consensus_tip: u64,
    pub head_age: Duration,
    pub verified_at: Instant,
}

/// Consensus client state, delivered via its own `watch::Receiver` so it
/// doesn't tick the verification counters at slot cadence.
#[derive(Debug, Clone, Default)]
pub struct ConsensusStatus {
    pub tip: u64,
    pub head_age: Duration,
    pub checkpoint: Option<alloy::primitives::B256>,
    pub is_synced: bool,
}
