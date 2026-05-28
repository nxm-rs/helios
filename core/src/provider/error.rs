//! Verification-failure error type for the [`VerifiedHeliosProvider`] and
//! [`OptimisticHeliosProvider`] surface.
//!
//! See issue #15 for the design.

use std::time::Instant;

use alloy::primitives::TxHash;

/// Error returned by verified-path operations on the helios provider.
#[derive(Debug, Clone)]
pub enum VerificationError {
    /// One or more verifications observed a mismatch between the unverified
    /// RPC's response and the verified consensus-backed answer. The provider
    /// is tainted; subsequent reads return `Tainted` until
    /// [`super::VerificationStatus::acknowledge_mismatch`] is called.
    Mismatched { calls: Vec<MismatchInfo> },

    /// One or more verifications failed before they could complete
    /// (transport error against the consensus RPC, proof-decoding failure,
    /// etc.). Not a trust failure, but the verified state is unknown.
    Failed { calls: Vec<FailureInfo> },

    /// A [`super::VerificationStatus::barrier`] wait timed out with calls
    /// still pending.
    Timeout { still_pending: usize },

    /// The provider has been tainted by a prior mismatch and the consumer
    /// hasn't called `acknowledge_mismatch`.
    Tainted,

    /// The consensus client has not made progress for longer than the
    /// configured stall threshold.
    Stalled { last_progress_at: Instant },

    /// A scope was dropped while a barrier was awaiting it.
    ScopeDropped,

    /// A transaction was broadcast successfully but dropped from the mempool
    /// (or replaced) before inclusion.
    TransactionDropped {
        last_seen_at: Instant,
        replaced_by: Option<TxHash>,
    },
}

/// Detail for a single mismatched call.
#[derive(Debug, Clone)]
pub struct MismatchInfo {
    pub method: &'static str,
    /// JSON-serialised unverified value. Boxed `String` to keep the enum
    /// stack footprint bounded; large values (full blocks, receipts) would
    /// otherwise blow the size.
    pub unverified: Box<str>,
    pub verified: Box<str>,
    pub at: Instant,
}

/// Detail for a single failed call.
#[derive(Debug, Clone)]
pub struct FailureInfo {
    pub method: &'static str,
    pub error: Box<str>,
    pub at: Instant,
}

impl std::fmt::Display for VerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mismatched { calls } => {
                write!(f, "verification mismatch in {} call(s)", calls.len())
            }
            Self::Failed { calls } => {
                write!(f, "verification failed in {} call(s)", calls.len())
            }
            Self::Timeout { still_pending } => {
                write!(f, "verification barrier timed out ({still_pending} still pending)")
            }
            Self::Tainted => write!(f, "provider tainted by prior mismatch"),
            Self::Stalled { .. } => write!(f, "consensus client stalled"),
            Self::ScopeDropped => write!(f, "scope dropped while barrier awaiting"),
            Self::TransactionDropped { replaced_by, .. } => match replaced_by {
                Some(by) => write!(f, "transaction dropped, replaced by {by:?}"),
                None => write!(f, "transaction dropped from mempool"),
            },
        }
    }
}

impl std::error::Error for VerificationError {}
