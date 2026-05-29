//! Event / state types emitted by the provider's verification machinery.
//!
//! - [`VerificationCounts`] is delivered via `watch` — latest-value
//!   semantics, cheap to poll at frame rate.
//! - [`HealthStatus`] is also `watch` — sticky terminal state. Late
//!   subscribers observe the current value immediately.
//! - [`SecurityEvent`] is delivered via bounded `mpsc`. The Phase 1
//!   verifier publishes via `try_send`, so events are dropped when the
//!   consumer is too slow rather than blocking the producer; a full
//!   producer-backpressure design lands with the optimistic provider.
//! - [`VerificationEvent`] is delivered via `broadcast` (drop-oldest) —
//!   informational chatter where lossy is acceptable.

use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;

use crate::provider::error::FailureInfo;
use crate::provider::value::VerifiedValue;

/// Counters for verification activity, delivered via `watch::Receiver`.
#[derive(Debug, Clone, Default)]
pub struct VerificationCounts {
    pub pending: usize,
    pub verified: usize,
    pub failed: usize,
    pub last_change_at: Option<Instant>,
}

/// Sticky terminal state of the provider, delivered via `watch::Receiver`.
///
/// The `Stalled` variant is a unit in this phase; the consensus
/// supervisor (Phase 2f) reintroduces fields carrying the stall
/// timestamp once a producer exists for them.
#[derive(Debug, Clone, Default)]
pub enum HealthStatus {
    #[default]
    Healthy,
    /// Consensus client has not made progress for longer than the
    /// configured stall threshold.
    Stalled,
}

/// Security-critical events. Delivered via bounded `mpsc::Receiver`.
#[derive(Debug, Clone)]
pub enum SecurityEvent {
    Failed(FailureInfo),
}

/// Informational verification events. Delivered via
/// `broadcast::Receiver<VerificationEvent>` with drop-oldest semantics —
/// `RecvError::Lagged(n)` surfaces as a synthetic [`VerificationEvent::Dropped`].
#[derive(Clone)]
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
    /// `count` informational events.
    Dropped { count: u64 },
}

impl<N: NetworkSpec> std::fmt::Debug for VerificationEvent<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verified { method, took, .. } => f
                .debug_struct("Verified")
                .field("method", method)
                .field("took", took)
                .finish(),
            Self::Skipped { method, reason } => f
                .debug_struct("Skipped")
                .field("method", method)
                .field("reason", reason)
                .finish(),
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
