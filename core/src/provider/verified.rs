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
use std::time::{Duration, Instant};

use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockHash, Bytes, TxHash, B256, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::error::{FailureInfo, VerificationError};
use crate::provider::event::VerificationEvent;
use crate::provider::status::VerificationStatus;
use crate::provider::value::VerifiedValue;

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
    helios: Arc<dyn HeliosApi<N>>,
    /// The forwarded-path delegate. Default `Provider<N>` method impls go
    /// through this for methods we don't override.
    root: RootProvider<N>,
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
        hash: TxHash,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        loop {
            if let Some(r) = self.transaction_receipt_verified(hash).await? {
                return Ok(r);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Time-bounded variant of [`Self::verified_receipt`].
    pub async fn verified_receipt_with_timeout(
        &self,
        hash: TxHash,
        timeout: Duration,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        match tokio::time::timeout(timeout, self.verified_receipt(hash)).await {
            Ok(r) => r,
            Err(_) => Err(VerificationError::Timeout { still_pending: 1 }),
        }
    }

    /// Verified balance at the current head.
    pub async fn balance_verified(&self, address: Address) -> Result<U256, VerificationError> {
        self.run_verified(
            "eth_getBalance",
            |h| async move { h.get_balance(address, BlockId::latest()).await },
            |v| VerifiedValue::Balance(*v),
        )
        .await
    }

    /// Verified nonce at the current head.
    pub async fn nonce_verified(&self, address: Address) -> Result<u64, VerificationError> {
        self.run_verified(
            "eth_getTransactionCount",
            |h| async move { h.get_nonce(address, BlockId::latest()).await },
            |v| VerifiedValue::Nonce(*v),
        )
        .await
    }

    /// Verified code at the current head.
    pub async fn code_verified(&self, address: Address) -> Result<Bytes, VerificationError> {
        self.run_verified(
            "eth_getCode",
            |h| async move { h.get_code(address, BlockId::latest()).await },
            |v| VerifiedValue::Code(v.clone()),
        )
        .await
    }

    /// Verified storage slot at the current head.
    pub async fn storage_verified(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<B256, VerificationError> {
        self.run_verified(
            "eth_getStorageAt",
            |h| async move { h.get_storage_at(address, slot, BlockId::latest()).await },
            |v| VerifiedValue::StorageSlot(*v),
        )
        .await
    }

    /// Verified logs matching the filter.
    pub async fn logs_verified(&self, filter: &Filter) -> Result<Vec<Log>, VerificationError> {
        let filter = filter.clone();
        self.run_verified(
            "eth_getLogs",
            |h| async move { h.get_logs(&filter).await },
            |v| VerifiedValue::Logs(v.clone()),
        )
        .await
    }

    /// Verified block by hash.
    pub async fn block_by_hash_verified(
        &self,
        hash: BlockHash,
        full_tx: bool,
    ) -> Result<Option<N::BlockResponse>, VerificationError> {
        self.run_verified_opt(
            "eth_getBlockByHash",
            |h| async move { h.get_block(BlockId::Hash(hash.into()), full_tx).await },
            |b| VerifiedValue::Block(Box::new(b.clone())),
        )
        .await
    }

    /// Verified transaction receipt by hash. Used internally by
    /// [`Self::verified_receipt`].
    pub async fn transaction_receipt_verified(
        &self,
        hash: TxHash,
    ) -> Result<Option<N::ReceiptResponse>, VerificationError> {
        self.run_verified_opt(
            "eth_getTransactionReceipt",
            |h| async move { h.get_transaction_receipt(hash).await },
            |r| VerifiedValue::Receipt(Box::new(r.clone())),
        )
        .await
    }

    /// Bump pending, await the verified-path call, record the outcome on
    /// [`VerificationStatus`], and emit a `Verified` event on the verbose
    /// channel when there are subscribers.
    async fn run_verified<T, F, Fut, M>(
        &self,
        method: &'static str,
        call: F,
        make_value: M,
    ) -> Result<T, VerificationError>
    where
        F: FnOnce(Arc<dyn HeliosApi<N>>) -> Fut,
        Fut: std::future::Future<Output = eyre::Result<T>>,
        M: FnOnce(&T) -> VerifiedValue<N>,
    {
        let started = Instant::now();
        self.inner.status._bump_pending();
        match call(self.inner.helios.clone()).await {
            Ok(value) => {
                let took = started.elapsed();
                self.inner.status._record_verified();
                self.inner
                    .status
                    ._emit_verbose_with(|| VerificationEvent::Verified {
                        method,
                        value: make_value(&value),
                        took,
                    });
                Ok(value)
            }
            Err(err) => {
                let info = FailureInfo {
                    method,
                    error: err.to_string().into_boxed_str(),
                    at: Instant::now(),
                };
                self.inner.status._record_failed(info.clone()).await;
                Err(VerificationError::Failed { calls: vec![info] })
            }
        }
    }

    /// `Option<T>`-returning sibling of [`Self::run_verified`]. The
    /// `Verified` event only carries a payload for the `Some` case.
    async fn run_verified_opt<T, F, Fut, M>(
        &self,
        method: &'static str,
        call: F,
        make_value: M,
    ) -> Result<Option<T>, VerificationError>
    where
        F: FnOnce(Arc<dyn HeliosApi<N>>) -> Fut,
        Fut: std::future::Future<Output = eyre::Result<Option<T>>>,
        M: FnOnce(&T) -> VerifiedValue<N>,
    {
        let started = Instant::now();
        self.inner.status._bump_pending();
        match call(self.inner.helios.clone()).await {
            Ok(Some(value)) => {
                let took = started.elapsed();
                self.inner.status._record_verified();
                self.inner
                    .status
                    ._emit_verbose_with(|| VerificationEvent::Verified {
                        method,
                        value: make_value(&value),
                        took,
                    });
                Ok(Some(value))
            }
            Ok(None) => {
                self.inner.status._record_verified();
                Ok(None)
            }
            Err(err) => {
                let info = FailureInfo {
                    method,
                    error: err.to_string().into_boxed_str(),
                    at: Instant::now(),
                };
                self.inner.status._record_failed(info.clone()).await;
                Err(VerificationError::Failed { calls: vec![info] })
            }
        }
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
