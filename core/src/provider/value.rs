//! Verified-value carrier types.
//!
//! [`VerifiedValue`] is the typed payload of a successful background
//! verification — emitted on the `events_verbose` channel so consumers
//! that persist verified state don't have to re-issue the call to learn
//! what was proven.
//!
//! [`Unverifiable<T>`] wraps the return of methods that cannot be backed
//! by consensus proofs (gas estimators, fee history, `block_number` at
//! tip). Forcing the caller to call `into_inner()` makes "I'm trusting
//! the RPC for this" syntactically visible.

use alloy::primitives::{Bytes, B256, U256};
use alloy::rpc::types::{AccessListResult, EIP1186AccountProofResponse, Log};
use helios_common::network_spec::NetworkSpec;

/// Verified payload from a background verification, attached to a
/// `VerificationEvent::Verified`.
///
/// Marked `#[non_exhaustive]` so adding variants in minor releases is
/// non-breaking. Large variants are boxed to keep the enum's stack
/// footprint bounded.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum VerifiedValue<N: NetworkSpec> {
    Balance(U256),
    Nonce(u64),
    Code(Bytes),
    StorageSlot(B256),
    Proof(Box<EIP1186AccountProofResponse>),
    Block(Box<N::BlockResponse>),
    Transaction(Box<N::TransactionResponse>),
    Receipt(Box<N::ReceiptResponse>),
    Logs(Vec<Log>),
    Call(Bytes),
    GasEstimate(u64),
    AccessList(Box<AccessListResult>),
}

/// Wrapper for values the verified provider cannot back with consensus
/// proofs (gas estimators, fee history, `block_number` at tip).
///
/// The caller must call [`into_inner`] to extract the inner value, which
/// makes the "I am trusting the RPC for this" assumption syntactically
/// visible at the call site.
///
/// `Debug` is implemented manually and **does not** expose the inner
/// value — only the method name — so that `tracing::debug!(?x)`,
/// `assert_eq!` failure messages, and panic dumps cannot accidentally
/// surface a trusted value that the caller hasn't acknowledged via
/// [`into_inner`].
///
/// [`into_inner`]: Unverifiable::into_inner
#[derive(Clone)]
pub struct Unverifiable<T> {
    value: T,
    method: &'static str,
}

impl<T> std::fmt::Debug for Unverifiable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unverifiable")
            .field("method", &self.method)
            .field("value", &"<call .into_inner() to acknowledge>")
            .finish()
    }
}

impl<T> Unverifiable<T> {
    /// Construct an `Unverifiable<T>` carrying a value that came from the
    /// untrusted RPC for the given method name.
    pub const fn new(value: T, method: &'static str) -> Self {
        Self { value, method }
    }

    /// Extract the inner value. The caller acknowledges they are trusting
    /// the upstream RPC for this datum.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// JSON-RPC method name that produced this value, for diagnostics.
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// Peek at the inner value without consuming the wrapper. Useful for
    /// rendering paths where the caller is already on the "trusting" side
    /// of the boundary.
    pub const fn as_inner(&self) -> &T {
        &self.value
    }
}
