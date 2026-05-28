//! [`VerifiedHeliosProvider`] — drop-in `alloy::providers::Provider<N>`
//! whose read methods block until consensus-anchored verification has
//! succeeded.
//!
//! For methods that cannot be verified, see the [`Unverifiable<T>`]
//! wrapper in [`super::value`].
//!
//! [`Unverifiable<T>`]: super::value::Unverifiable

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
/// cannot be verified (gas estimators, fee history, `block_number` at
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
    /// The verified-path delegate.
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
    /// reach the lower-level helios methods directly.
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
        todo!()
    }

    /// Time-bounded variant of [`Self::verified_receipt`].
    pub async fn verified_receipt_with_timeout(
        &self,
        _hash: TxHash,
        _timeout: Duration,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        todo!()
    }

    /// Verified balance at the current head.
    pub async fn balance_verified(&self, _address: Address) -> Result<U256, VerificationError> {
        todo!()
    }

    /// Verified nonce at the current head.
    pub async fn nonce_verified(&self, _address: Address) -> Result<u64, VerificationError> {
        todo!()
    }

    /// Verified code at the current head.
    pub async fn code_verified(&self, _address: Address) -> Result<Bytes, VerificationError> {
        todo!()
    }

    /// Verified storage slot at the current head.
    pub async fn storage_verified(
        &self,
        _address: Address,
        _slot: U256,
    ) -> Result<B256, VerificationError> {
        todo!()
    }

    /// Verified logs matching the filter.
    pub async fn logs_verified(&self, _filter: &Filter) -> Result<Vec<Log>, VerificationError> {
        todo!()
    }

    /// Verified block by hash.
    pub async fn block_by_hash_verified(
        &self,
        _hash: BlockHash,
        _full_tx: bool,
    ) -> Result<Option<N::BlockResponse>, VerificationError> {
        todo!()
    }

    /// Verified transaction receipt by hash. Used internally by
    /// [`Self::verified_receipt`].
    pub async fn transaction_receipt_verified(
        &self,
        _hash: TxHash,
    ) -> Result<Option<N::ReceiptResponse>, VerificationError> {
        todo!()
    }
}

// Only `root()` has no default impl on alloy's `Provider<N>` trait —
// every other method has a default that calls through `client()` (which
// defaults to `self.root().client()`). Providing `root()` here wires the
// type in as a `Provider<N>`; verified-blocking overrides for individual
// methods are added one at a time and replace the alloy default.
impl<N: NetworkSpec> Provider<N> for VerifiedHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.inner.root
    }
}
