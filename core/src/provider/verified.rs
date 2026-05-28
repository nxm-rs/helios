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
use alloy::primitives::{Address, BlockHash, Bytes, TxHash, B256, U256, U64};
use alloy::providers::{Provider, ProviderCall, RootProvider, RpcWithBlock};
use alloy::rpc::types::{Filter, Log};
use alloy::transports::{TransportErrorKind, TransportResult};
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
    /// Wrap in [`tokio::time::timeout`] for a bounded wait.
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
        let handle = self.inner.status._bump_pending();
        match call(self.inner.helios.clone()).await {
            Ok(value) => {
                let took = started.elapsed();
                handle.record_verified();
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
                handle.record_failed(info.clone());
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
        let handle = self.inner.status._bump_pending();
        match call(self.inner.helios.clone()).await {
            Ok(Some(value)) => {
                let took = started.elapsed();
                handle.record_verified();
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
                handle.record_verified();
                Ok(None)
            }
            Err(err) => {
                let info = FailureInfo {
                    method,
                    error: err.to_string().into_boxed_str(),
                    at: Instant::now(),
                };
                handle.record_failed(info.clone());
                Err(VerificationError::Failed { calls: vec![info] })
            }
        }
    }
}

// The alloy `Provider<N>` trait has default impls for every method
// except `root()` — defaults call through `client()` which itself
// defaults to `self.root().client()`. Methods we override below replace
// the alloy default and route through the helios verified path;
// methods we don't override forward to the unverified RPC via `root()`.
// Builder methods (`get_balance` -> `RpcWithBlock`, etc.) honour the
// caller's `.block_id(...)` selection by deferring the verified call
// until the builder is awaited.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl<N: NetworkSpec> Provider<N> for VerifiedHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.inner.root
    }

    fn get_balance(&self, address: Address) -> RpcWithBlock<Address, U256, U256> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .run_verified(
                        "eth_getBalance",
                        |h| async move { h.get_balance(address, block_id).await },
                        |v| VerifiedValue::Balance(*v),
                    )
                    .await
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    fn get_transaction_count(
        &self,
        address: Address,
    ) -> RpcWithBlock<Address, U64, u64, fn(U64) -> u64> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .run_verified(
                        "eth_getTransactionCount",
                        |h| async move { h.get_nonce(address, block_id).await },
                        |v| VerifiedValue::Nonce(*v),
                    )
                    .await
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    fn get_code_at(&self, address: Address) -> RpcWithBlock<Address, Bytes> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .run_verified(
                        "eth_getCode",
                        |h| async move { h.get_code(address, block_id).await },
                        |v| VerifiedValue::Code(v.clone()),
                    )
                    .await
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    fn get_storage_at(
        &self,
        address: Address,
        key: U256,
    ) -> RpcWithBlock<(Address, U256), U256> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .run_verified(
                        "eth_getStorageAt",
                        |h| async move { h.get_storage_at(address, key, block_id).await },
                        |v| VerifiedValue::StorageSlot(*v),
                    )
                    .await
                    .map(|b| U256::from_be_bytes(b.0))
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        self.logs_verified(filter)
            .await
            .map_err(TransportErrorKind::custom)
    }

    fn get_transaction_receipt(
        &self,
        hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<N::ReceiptResponse>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            provider
                .transaction_receipt_verified(hash)
                .await
                .map_err(TransportErrorKind::custom)
        }))
    }
}
