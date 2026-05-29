//! Error type returned by verified-path operations.

use std::time::Instant;

/// Error returned by verified-path operations on the helios provider.
#[derive(Debug, Clone)]
pub enum VerificationError {
    /// One or more verifications failed before they could complete
    /// (transport error against the consensus RPC, proof-decoding failure,
    /// etc.). Not a trust failure, but the verified state is unknown.
    Failed { calls: Vec<FailureInfo> },

    /// A [`super::VerificationStatus::barrier`] wait timed out with calls
    /// still pending.
    Timeout { still_pending: usize },
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
            Self::Failed { calls } => {
                write!(f, "verification failed in {} call(s)", calls.len())
            }
            Self::Timeout { still_pending } => {
                write!(
                    f,
                    "verification barrier timed out ({still_pending} still pending)"
                )
            }
        }
    }
}

impl std::error::Error for VerificationError {}
