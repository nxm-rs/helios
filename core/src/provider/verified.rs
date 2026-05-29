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

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, BlockHash, Bytes, StorageKey, TxHash, B256, U256, U64};
use alloy::providers::{
    Caller, EthCall, EthCallMany, EthCallManyParams, EthCallParams, EthGetBlock, Provider,
    ProviderCall, RootProvider, RpcWithBlock,
};
use alloy::rpc::types::state::StateOverride;
use alloy::rpc::types::{
    AccessListResult, BlockTransactionsKind, Bundle, EIP1186AccountProofResponse, EthCallResponse,
    FeeHistory, Filter, Log,
};
use alloy::transports::{TransportErrorKind, TransportResult};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::error::{FailureInfo, VerificationError};
use crate::provider::event::VerificationEvent;
use crate::provider::status::VerificationStatus;
use crate::provider::value::{Unverifiable, VerifiedValue};

/// Verified-blocking helios provider — drop-in `alloy::providers::Provider<N>`.
///
/// Methods routed through helios verification:
/// `get_balance`, `get_transaction_count`, `get_code_at`,
/// `get_storage_at`, `get_logs`, `get_transaction_receipt`,
/// `get_block`, `get_block_by_hash`, `get_proof`,
/// `get_transaction_by_hash`, `get_block_receipts`,
/// `call`, `estimate_gas`, `create_access_list`.
///
/// Methods that fall through to the unverified RPC via `RootProvider<N>`:
/// methods that helios cannot back at all (mempool subscriptions, raw
/// filter queries, etc.). For the gas / fee / tip / chain-id family
/// (`eth_gasPrice`, `eth_maxPriorityFeePerGas`, `eth_blobBaseFee`,
/// `eth_feeHistory`, `eth_blockNumber`, `eth_chainId`) the
/// trait-default methods still forward to the unverified RPC, but
/// inherent `*_unverifiable` methods returning [`Unverifiable<T>`]
/// give consumers a syntactic acknowledgement that they are trusting
/// the upstream.
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
    ///
    /// Intermediate "receipt not yet present" polls do not tick the
    /// verification counters or emit verbose events — only the terminal
    /// success goes through the verified accounting path. This avoids
    /// inflating `counts.verified` by ~120 per minute while a user waits
    /// for a transaction to land.
    pub async fn verified_receipt(
        &self,
        hash: TxHash,
    ) -> Result<N::ReceiptResponse, VerificationError> {
        loop {
            // Poll helios directly to avoid bumping the pending/verified
            // counters on every retry. We re-enter the accounted path
            // (`transaction_receipt_verified`) for the terminal success
            // so the verbose event still carries the receipt payload.
            match self.inner.helios.get_transaction_receipt(hash).await {
                Ok(Some(_)) => {
                    if let Some(r) = self.transaction_receipt_verified(hash).await? {
                        return Ok(r);
                    }
                    // Race: receipt vanished between the unaccounted poll
                    // and the accounted re-fetch. Continue polling.
                }
                Ok(None) => {}
                Err(err) => {
                    return Err(VerificationError::Failed {
                        calls: vec![FailureInfo {
                            method: "eth_getTransactionReceipt",
                            error: err.to_string().into_boxed_str(),
                            at: Instant::now(),
                        }],
                    });
                }
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
            |b| Some(VerifiedValue::Block(Box::new(b.clone()))),
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
            |r| Some(VerifiedValue::Receipt(Box::new(r.clone()))),
        )
        .await
    }

    /// Verified block at the given block id.
    pub async fn block_verified(
        &self,
        block_id: BlockId,
        full_tx: bool,
    ) -> Result<Option<N::BlockResponse>, VerificationError> {
        self.run_verified_opt(
            "eth_getBlockByNumber",
            |h| async move { h.get_block(block_id, full_tx).await },
            |b| Some(VerifiedValue::Block(Box::new(b.clone()))),
        )
        .await
    }

    /// Verified Merkle proof for an account at the given block id.
    pub async fn proof_verified(
        &self,
        address: Address,
        slots: Vec<B256>,
        block_id: BlockId,
    ) -> Result<EIP1186AccountProofResponse, VerificationError> {
        self.run_verified(
            "eth_getProof",
            move |h| async move { h.get_proof(address, &slots, block_id).await },
            |v| VerifiedValue::Proof(Box::new(v.clone())),
        )
        .await
    }

    /// Verified transaction by hash.
    pub async fn transaction_verified(
        &self,
        hash: TxHash,
    ) -> Result<Option<N::TransactionResponse>, VerificationError> {
        self.run_verified_opt(
            "eth_getTransactionByHash",
            |h| async move { h.get_transaction(hash).await },
            |t| Some(VerifiedValue::Transaction(Box::new(t.clone()))),
        )
        .await
    }

    /// Verified block receipts at the given block id.
    pub async fn block_receipts_verified(
        &self,
        block_id: BlockId,
    ) -> Result<Option<Vec<N::ReceiptResponse>>, VerificationError> {
        self.run_verified_opt(
            "eth_getBlockReceipts",
            |h| async move { h.get_block_receipts(block_id).await },
            // The verbose event carries only the first receipt as a sample;
            // shipping the whole vector would be wasteful for a chatty
            // informational stream. Empty receipts → no event (avoids
            // the type-confusing "eth_getBlockReceipts → VerifiedValue::Logs"
            // pairing the prior version produced).
            |rs| {
                rs.first()
                    .map(|r| VerifiedValue::Receipt(Box::new(r.clone())))
            },
        )
        .await
    }

    /// Verified `eth_call` against the given block + state overrides.
    pub async fn call_verified(
        &self,
        tx: N::TransactionRequest,
        block_id: BlockId,
        state_overrides: Option<StateOverride>,
    ) -> Result<Bytes, VerificationError> {
        self.run_verified(
            "eth_call",
            move |h| async move { h.call(&tx, block_id, state_overrides).await },
            |v| VerifiedValue::Call(v.clone()),
        )
        .await
    }

    /// Verified `eth_estimateGas` against the given block + state overrides.
    pub async fn estimate_gas_verified(
        &self,
        tx: N::TransactionRequest,
        block_id: Option<BlockId>,
        state_overrides: Option<StateOverride>,
    ) -> Result<u64, VerificationError> {
        self.run_verified(
            "eth_estimateGas",
            move |h| async move { h.estimate_gas(&tx, block_id, state_overrides).await },
            |v| VerifiedValue::GasEstimate(*v),
        )
        .await
    }

    /// Verified `eth_createAccessList` against the given block + state overrides.
    pub async fn create_access_list_verified(
        &self,
        tx: N::TransactionRequest,
        block_id: BlockId,
        state_overrides: Option<StateOverride>,
    ) -> Result<AccessListResult, VerificationError> {
        self.run_verified(
            "eth_createAccessList",
            move |h| async move { h.create_access_list(&tx, block_id, state_overrides).await },
            |v| VerifiedValue::AccessList(Box::new(v.clone())),
        )
        .await
    }

    /// Current gas price as the upstream RPC reports it. Wrapped in
    /// [`Unverifiable`] because helios cannot anchor the node's chosen
    /// `eth_gasPrice` heuristic against consensus — gas price is a
    /// node-local market estimate, not consensus-anchored state.
    pub async fn gas_price_unverifiable(&self) -> TransportResult<Unverifiable<u128>> {
        let v = self.inner.root.get_gas_price().await?;
        Ok(Unverifiable::new(v, "eth_gasPrice"))
    }

    /// Current EIP-1559 priority fee suggestion as the upstream RPC
    /// reports it. Wrapped in [`Unverifiable`] for the same reason as
    /// [`Self::gas_price_unverifiable`].
    pub async fn priority_fee_unverifiable(&self) -> TransportResult<Unverifiable<u128>> {
        let v = self.inner.root.get_max_priority_fee_per_gas().await?;
        Ok(Unverifiable::new(v, "eth_maxPriorityFeePerGas"))
    }

    /// Current blob base fee as the upstream RPC reports it. Wrapped in
    /// [`Unverifiable`] because blob base fee, like gas price, is a
    /// node-local estimate.
    pub async fn blob_base_fee_unverifiable(&self) -> TransportResult<Unverifiable<u128>> {
        let v = self.inner.root.get_blob_base_fee().await?;
        Ok(Unverifiable::new(v, "eth_blobBaseFee"))
    }

    /// Fee history across `block_count` recent blocks. Wrapped in
    /// [`Unverifiable`] because verifying every block's base-fee schedule
    /// against consensus for arbitrary historical ranges isn't
    /// trustlessly tractable from a light-client.
    pub async fn fee_history_unverifiable(
        &self,
        block_count: u64,
        last_block: BlockNumberOrTag,
        reward_percentiles: &[f64],
    ) -> TransportResult<Unverifiable<FeeHistory>> {
        let v = self
            .inner
            .root
            .get_fee_history(block_count, last_block, reward_percentiles)
            .await?;
        Ok(Unverifiable::new(v, "eth_feeHistory"))
    }

    /// Current head block number as the upstream RPC reports it. Wrapped
    /// in [`Unverifiable`] because the tip moves between observation and
    /// verification — by the time the helios consensus client confirms a
    /// number, a newer block has typically arrived. Drift-tolerant by
    /// design.
    pub async fn block_number_unverifiable(&self) -> TransportResult<Unverifiable<u64>> {
        let v = self.inner.root.get_block_number().await?;
        Ok(Unverifiable::new(v, "eth_blockNumber"))
    }

    /// Chain id as the upstream RPC reports it. Wrapped in
    /// [`Unverifiable`] because helios's chain id comes from a
    /// configured fork schedule at client build time — there is no
    /// consensus proof that the connected RPC speaks the same chain.
    ///
    /// Prefer [`Self::assert_chain_id_matches_helios`] at startup over
    /// reading this directly: it performs the cross-check against the
    /// chain id helios was configured for and returns an error if the
    /// RPC speaks a different chain.
    pub async fn chain_id_unverifiable(&self) -> TransportResult<Unverifiable<u64>> {
        let v = self.inner.root.get_chain_id().await?;
        Ok(Unverifiable::new(v, "eth_chainId"))
    }

    /// Assert that the upstream RPC's `eth_chainId` matches the chain
    /// id helios was configured for. Returns an error if they differ —
    /// the embedder must refuse to proceed (e.g. abort startup) on a
    /// mismatch, since a wrong-chain RPC defeats every other
    /// verification primitive.
    pub async fn assert_chain_id_matches_helios(&self) -> Result<(), ChainIdMismatch> {
        let helios_chain_id = self.inner.helios.get_chain_id().await;
        let rpc_chain_id = self
            .inner
            .root
            .get_chain_id()
            .await
            .map_err(|e| ChainIdMismatch::Rpc(e.to_string()))?;
        if helios_chain_id == rpc_chain_id {
            Ok(())
        } else {
            Err(ChainIdMismatch::Mismatch {
                helios: helios_chain_id,
                rpc: rpc_chain_id,
            })
        }
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
                self.inner.status._emit_verbose_with(|| {
                    Some(VerificationEvent::Verified {
                        method,
                        value: make_value(&value),
                        took,
                    })
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
    /// The `make_value` closure returns `Option<VerifiedValue<N>>` so
    /// callers whose payload is semantically empty (e.g. an empty
    /// receipts vec) can return `None` to skip the verbose event rather
    /// than emit a misleading sentinel like `VerifiedValue::Logs(vec![])`
    /// for an `eth_getBlockReceipts` event.
    async fn run_verified_opt<T, F, Fut, M>(
        &self,
        method: &'static str,
        call: F,
        make_value: M,
    ) -> Result<Option<T>, VerificationError>
    where
        F: FnOnce(Arc<dyn HeliosApi<N>>) -> Fut,
        Fut: std::future::Future<Output = eyre::Result<Option<T>>>,
        M: FnOnce(&T) -> Option<VerifiedValue<N>>,
    {
        let started = Instant::now();
        let handle = self.inner.status._bump_pending();
        match call(self.inner.helios.clone()).await {
            Ok(Some(value)) => {
                let took = started.elapsed();
                handle.record_verified();
                self.inner.status._emit_verbose_with(|| {
                    make_value(&value).map(|v| VerificationEvent::Verified {
                        method,
                        value: v,
                        took,
                    })
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

    fn get_storage_at(&self, address: Address, key: U256) -> RpcWithBlock<(Address, U256), U256> {
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

    fn get_block(&self, block: BlockId) -> EthGetBlock<N::BlockResponse> {
        let provider = self.clone();
        EthGetBlock::new_provider(
            block,
            Box::new(move |kind| {
                let provider = provider.clone();
                let full_tx = matches!(kind, BlockTransactionsKind::Full);
                ProviderCall::BoxedFuture(Box::pin(async move {
                    provider
                        .block_verified(block, full_tx)
                        .await
                        .map_err(TransportErrorKind::custom)
                }))
            }),
        )
    }

    fn get_block_by_hash(&self, hash: BlockHash) -> EthGetBlock<N::BlockResponse> {
        let provider = self.clone();
        EthGetBlock::new_provider(
            BlockId::Hash(hash.into()),
            Box::new(move |kind| {
                let provider = provider.clone();
                let full_tx = matches!(kind, BlockTransactionsKind::Full);
                ProviderCall::BoxedFuture(Box::pin(async move {
                    provider
                        .block_by_hash_verified(hash, full_tx)
                        .await
                        .map_err(TransportErrorKind::custom)
                }))
            }),
        )
    }

    fn get_proof(
        &self,
        address: Address,
        keys: Vec<StorageKey>,
    ) -> RpcWithBlock<(Address, Vec<StorageKey>), EIP1186AccountProofResponse> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            let keys = keys.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .proof_verified(address, keys, block_id)
                    .await
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    fn get_transaction_by_hash(
        &self,
        hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<N::TransactionResponse>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            provider
                .transaction_verified(hash)
                .await
                .map_err(TransportErrorKind::custom)
        }))
    }

    fn get_block_receipts(
        &self,
        block: BlockId,
    ) -> ProviderCall<(BlockId,), Option<Vec<N::ReceiptResponse>>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            provider
                .block_receipts_verified(block)
                .await
                .map_err(TransportErrorKind::custom)
        }))
    }

    fn call(&self, tx: N::TransactionRequest) -> EthCall<N, Bytes> {
        EthCall::call(self.clone(), tx).block(BlockId::pending())
    }

    fn estimate_gas(&self, tx: N::TransactionRequest) -> EthCall<N, U64, u64> {
        fn u64_from(v: U64) -> u64 {
            v.to::<u64>()
        }
        EthCall::gas_estimate(self.clone(), tx)
            .block(BlockId::pending())
            .map_resp(u64_from as fn(U64) -> u64)
    }

    fn create_access_list<'a>(
        &self,
        request: &'a N::TransactionRequest,
    ) -> RpcWithBlock<&'a N::TransactionRequest, AccessListResult> {
        let provider = self.clone();
        let tx = request.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            let tx = tx.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                provider
                    .create_access_list_verified(tx, block_id, None)
                    .await
                    .map_err(TransportErrorKind::custom)
            }))
        })
    }

    fn call_many<'req>(
        &self,
        bundles: &'req [Bundle],
    ) -> EthCallMany<'req, N, Vec<Vec<EthCallResponse>>> {
        // Route through our refusing Caller impl. Alloy's default
        // would resolve via `weak_client()` and bypass every override —
        // a silent dispatch to the unverified RPC.
        EthCallMany::new(self.clone(), bundles)
    }
}

/// Block overrides aren't representable in helios's `HeliosApi::call` /
/// `HeliosApi::estimate_gas` signatures today. If a caller chains
/// `EthCall::with_block_overrides`, the verified path refuses rather
/// than silently dropping the override (silent drop would be a
/// trust-model regression).
fn reject_block_overrides<N: alloy::network::Network>(
    params: &EthCallParams<N>,
) -> TransportResult<()> {
    if params.block_overrides().is_some() {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider does not support block_overrides on eth_call/eth_estimateGas",
        ))
    } else {
        Ok(())
    }
}

impl<N: NetworkSpec> Caller<N, Bytes> for VerifiedHeliosProvider<N> {
    fn call(
        &self,
        params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Bytes>> {
        reject_block_overrides(&params)?;
        let provider = self.clone();
        let block = params.block().unwrap_or_else(BlockId::pending);
        let overrides = params.overrides().cloned();
        let tx = params.into_data();
        Ok(ProviderCall::BoxedFuture(Box::pin(async move {
            provider
                .call_verified(tx, block, overrides)
                .await
                .map_err(TransportErrorKind::custom)
        })))
    }

    fn estimate_gas(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Bytes>> {
        // Caller<N, Bytes>::estimate_gas is only reached if someone calls
        // `EthCall::<N, Bytes>::gas_estimate(provider, tx)` directly,
        // which is a type-misuse (Resp should be U64 for gas). Refuse.
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider: estimate_gas via Caller<N, Bytes> is unsupported; use Provider::estimate_gas",
        ))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, Bytes>> {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider does not implement eth_callMany",
        ))
    }
}

impl<N: NetworkSpec> Caller<N, U64> for VerifiedHeliosProvider<N> {
    fn call(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, U64>> {
        // Symmetric to the above: Caller<N, U64>::call is only reached
        // via direct misuse.
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider: call via Caller<N, U64> is unsupported; use Provider::call",
        ))
    }

    fn estimate_gas(
        &self,
        params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, U64>> {
        reject_block_overrides(&params)?;
        let provider = self.clone();
        let block = params.block();
        let overrides = params.overrides().cloned();
        let tx = params.into_data();
        Ok(ProviderCall::BoxedFuture(Box::pin(async move {
            provider
                .estimate_gas_verified(tx, block, overrides)
                .await
                .map(U64::from)
                .map_err(TransportErrorKind::custom)
        })))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, U64>> {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider does not implement eth_callMany",
        ))
    }
}

// `Provider::call_many` resolves through `weak_client()` by default,
// which bypasses every override on the provider type — calls would go
// straight to the unverified RPC. Override returns an `EthCallMany`
// backed by a `Caller<N, Vec<Vec<EthCallResponse>>>` impl that
// refuses, so the bypass surfaces as a clear error rather than a
// silent trust-model regression.
impl<N: NetworkSpec> Caller<N, Vec<Vec<EthCallResponse>>> for VerifiedHeliosProvider<N> {
    fn call(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider: Caller<Vec<Vec<EthCallResponse>>>::call is unsupported",
        ))
    }

    fn estimate_gas(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider: Caller<Vec<Vec<EthCallResponse>>>::estimate_gas is unsupported",
        ))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "VerifiedHeliosProvider does not implement eth_callMany — helios cannot back the per-bundle verified path; fan out via Provider::call per bundle if needed",
        ))
    }
}

/// Error returned by [`VerifiedHeliosProvider::assert_chain_id_matches_helios`].
#[derive(Debug, Clone)]
pub enum ChainIdMismatch {
    /// The upstream RPC returned an error when asked for its chain id.
    Rpc(String),
    /// Helios's configured chain id and the RPC's reported chain id
    /// disagree. The embedder must refuse to use this provider.
    Mismatch { helios: u64, rpc: u64 },
}

impl std::fmt::Display for ChainIdMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(e) => write!(f, "could not read RPC chain id: {e}"),
            Self::Mismatch { helios, rpc } => write!(
                f,
                "chain id mismatch: helios configured for {helios}, RPC reports {rpc}"
            ),
        }
    }
}

impl std::error::Error for ChainIdMismatch {}
