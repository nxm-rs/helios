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

use alloy::eips::eip2930::AccessList;
use alloy::primitives::{Bytes, Log, B256, U256};
use alloy::rpc::types::EIP1186AccountProofResponse;
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
    AccessList(Box<AccessList>),
}

/// Wrapper for values the verified provider cannot back with consensus
/// proofs (gas estimators, fee history, `block_number` at tip).
///
/// The caller must call [`into_inner`] to extract the inner value, which
/// makes the "I am trusting the RPC for this" assumption syntactically
/// visible at the call site.
///
/// [`into_inner`]: Unverifiable::into_inner
#[derive(Debug, Clone)]
pub struct Unverifiable<T> {
    value: T,
    method: &'static str,
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
