//! Error type returned by verified-path operations.

use std::borrow::Cow;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Error returned by verified-path operations on the helios provider.
#[derive(Debug, Clone)]
pub enum VerificationError {
    /// One or more verifications observed a mismatch between the
    /// unverified RPC's response and the verified consensus-backed
    /// answer. The provider is tainted; subsequent reads through a
    /// barrier return [`Self::Tainted`] until
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
    /// hasn't called [`super::VerificationStatus::acknowledge_mismatch`].
    /// Barriers refuse with this until the taint is cleared.
    Tainted,
}

/// Detail for a single failed call.
#[derive(Debug, Clone)]
pub struct FailureInfo {
    pub method: &'static str,
    pub error: Box<str>,
    pub at: Instant,
}

/// Detail for a single mismatched call. The unverified and verified
/// values are JSON-serialised for diagnostic display so the struct can
/// be `Clone + Send + 'static` without depending on each method's
/// response type at the trait level.
///
/// `Serialize` / `Deserialize` are derived so the struct can be
/// persisted to disk via a [`super::TaintStore`]. `method` is a
/// [`Cow<'static, str>`] so producers pass static literals at zero
/// cost (`"eth_getBalance".into()` is a `Cow::Borrowed`); values
/// restored from disk are `Cow::Owned`. `at_unix_ms` is the absolute
/// wall-clock time the mismatch was first observed, in milliseconds
/// since the unix epoch — chosen over [`Instant`] because
/// `MismatchInfo` outlives the process across restarts, and absolute
/// time is what diagnostic UIs actually want anyway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MismatchInfo {
    pub method: Cow<'static, str>,
    /// JSON-serialised unverified value. Boxed `str` keeps the struct
    /// stack footprint bounded; large values (full blocks, receipts)
    /// would otherwise blow the size.
    pub unverified: Box<str>,
    pub verified: Box<str>,
    pub at_unix_ms: u64,
}

impl MismatchInfo {
    /// Construct a [`MismatchInfo`] for `method` with the unverified
    /// and verified values, stamping `at_unix_ms` with the current
    /// wall-clock time. Producers should use this rather than
    /// constructing the struct directly.
    pub fn now(
        method: impl Into<Cow<'static, str>>,
        unverified: impl Into<Box<str>>,
        verified: impl Into<Box<str>>,
    ) -> Self {
        Self {
            method: method.into(),
            unverified: unverified.into(),
            verified: verified.into(),
            at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }
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
                write!(
                    f,
                    "verification barrier timed out ({still_pending} still pending)"
                )
            }
            Self::Tainted => write!(f, "provider tainted by prior mismatch"),
        }
    }
}

impl std::error::Error for VerificationError {}
