//! [`VerifiedHeliosProvider`] — drop-in `alloy::providers::Provider<N>`
//! whose read methods block until consensus-anchored verification has
//! succeeded.
//!
//! For methods helios cannot verify, see the `Unverifiable<T>` wrapper in
//! [`super::value`]. For the optimistic-first companion type, see
//! `OptimisticHeliosProvider` (Phase 2, not yet implemented).
//!
//! See issue #15 for the full design.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, BlockHash, Bytes, TxHash, B256, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::error::VerificationError;
use crate::provider::status::VerificationStatus;

/// Verified-blocking helios provider — drop-in `alloy::providers::Provider<N>`.
///
/// Every method on the `Provider<N>` trait that helios can back returns
/// only after consensus-anchored verification has succeeded. Methods that
/// helios cannot verify (gas estimators, fee history, `block_number` at
/// tip) return [`Unverifiable<T>`] from inherent methods, forcing the
/// caller to syntactically acknowledge they are trusting the RPC.
///
/// Cheap to clone — internally just an `Arc<Inner<N>>`.
///
/// [`Unverifiable<T>`]: super::value::Unverifiable
#[derive(Clone)]
pub struct VerifiedHeliosProvider<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

pub(crate) struct Inner<N: NetworkSpec> {
    /// The verified-path delegate. Phase 1 scaffold uses the existing
    /// `HeliosApi` trait object directly; subsequent phases will inline
    /// the relevant machinery here to lift `HeliosApi` to crate-private.
    helios: Arc<dyn HeliosApi<N>>,
    /// The forwarded-path delegate. Default `Provider<N>` method impls go
    /// through this for methods we don't override.
    root: RootProvider<N>,
    /// Shared verification-status handle. Wired into the verifier tasks
    /// the provider spawns per call.
    status: VerificationStatus<N>,
    _network: PhantomData<N>,
}

impl<N: NetworkSpec> VerifiedHeliosProvider<N> {
    /// Construct from a pre-built `HeliosApi` impl and an alloy
    /// `RootProvider<N>` over the same execution RPC.
    ///
    /// Most consumers won't call this directly — they use
    /// [`VerifiedHeliosProvider::builder`] (Phase 1 method, see
    /// `helios-ethereum` for the `Ethereum`-specialised constructor).
    pub fn from_parts(
        helios: Arc<dyn HeliosApi<N>>,
        root: RootProvider<N>,
        status: VerificationStatus<N>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                helios,
                root,
                status,
                _network: PhantomData,
            }),
        }
    }

    /// Returns the [`VerificationStatus`] handle for observing and gating
    /// on verification activity.
    pub fn verification_status(&self) -> &VerificationStatus<N> {
        &self.inner.status
    }

    /// The underlying [`HeliosApi`] — exposed for callers that want to
    /// reach the lower-level helios methods directly. Phase 1 makes this
    /// `pub`; later phases may demote it as the new provider absorbs the
    /// remaining `HeliosApi` surface.
    pub fn helios(&self) -> &dyn HeliosApi<N> {
        self.inner.helios.as_ref()
    }

    /// Block until the verified-receipt for this tx hash is observed.
    ///
    /// Companion to the alloy-standard `send_raw_transaction` (which
    /// returns the tx hash immediately on broadcast). The post-broadcast
    /// receipt poll lives here so the `Provider<N>` method's semantics
    /// match alloy's.
    pub async fn verified_receipt(
        &self,
        _hash: TxHash,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        todo!("phase 1: poll get_transaction_receipt against verified path")
    }

    /// Time-bounded variant of [`Self::verified_receipt`].
    pub async fn verified_receipt_with_timeout(
        &self,
        _hash: TxHash,
        _timeout: Duration,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        todo!("phase 1: implement verified_receipt_with_timeout")
    }

    /// Verified balance at the current head.
    ///
    /// Convenience wrapper around `Provider::get_balance` that uses the
    /// helios-verified path. Phase 1 scaffold; the real impl will go in
    /// the `Provider<N>` trait impl block once the verifier wiring is
    /// landed.
    pub async fn balance_verified(&self, _address: Address) -> Result<U256, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_balance, mark request_id pending")
    }

    /// Verified nonce at the current head.
    pub async fn nonce_verified(&self, _address: Address) -> Result<u64, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_nonce")
    }

    /// Verified code at the current head.
    pub async fn code_verified(&self, _address: Address) -> Result<Bytes, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_code")
    }

    /// Verified storage slot at the current head.
    pub async fn storage_verified(
        &self,
        _address: Address,
        _slot: U256,
    ) -> Result<B256, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_storage_at")
    }

    /// Verified logs matching the filter.
    pub async fn logs_verified(&self, _filter: &Filter) -> Result<Vec<Log>, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_logs")
    }

    /// Verified block by hash.
    pub async fn block_by_hash_verified(
        &self,
        _hash: BlockHash,
        _full_tx: bool,
    ) -> Result<Option<N::BlockResponse>, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_block")
    }

    /// Verified transaction receipt by hash. Used internally by
    /// [`Self::verified_receipt`].
    pub async fn transaction_receipt_verified(
        &self,
        _hash: TxHash,
    ) -> Result<Option<N::ReceiptResponse>, VerificationError> {
        todo!("phase 1: delegate to HeliosApi::get_transaction_receipt")
    }
}

// ---------------------------------------------------------------------
// `impl Provider<N>`
//
// Only `root()` has no default in alloy's `Provider<N>` trait at 2.0.5
// — every other method has a default implementation that calls through
// `client()` (which itself defaults to `self.root().client()`).
//
// In Phase 1 we provide `root()` returning the inner `RootProvider<N>`,
// which means all `Provider<N>` methods inherit the unverified-RPC
// behaviour transparently. Verified semantics for the Phase 1 method
// list are exposed via the `*_verified` inherent methods above; later
// phases override the `Provider<N>` defaults to make `get_balance` etc.
// blocking-on-verification at the trait level.
//
// This staging matters: a downstream consumer who upgrades their
// `Provider<N>` reference to `VerifiedHeliosProvider<N>` doesn't
// inadvertently get a different semantic for `get_balance` until the
// trait override lands — and the override lands together with the
// per-method matrix documentation in Phase 1 of the implementation.
// ---------------------------------------------------------------------

impl<N: NetworkSpec> Provider<N> for VerifiedHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.inner.root
    }

    // `client()` is intentionally NOT overridden — alloy's docs explicitly
    // forbid it; the default impl calls through `root().client()`.

    // Phase-1 verified overrides land in a follow-up commit on this same
    // PR once the verifier-task wiring is in place. For now, every
    // `Provider<N>` method inherits the default impl, which forwards to
    // the unverified RPC via `root()`.
    //
    // See issue nxm-rs/helios#16 for the per-method override checklist.
}

