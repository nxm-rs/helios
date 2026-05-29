//! [`OptimisticHeliosProvider`] — returns unverified RPC values
//! immediately and fans out background verification.
//!
//! For every overridden read method, the optimistic provider:
//! 1. issues the unverified call against [`RootProvider<N>`] and returns
//!    the value to the caller as soon as it arrives,
//! 2. spawns a background task that issues the verified-path call
//!    against [`HeliosApi<N>`] and compares the two responses,
//! 3. on a verified-vs-unverified mismatch, flips
//!    [`HealthStatus::Tainted`] *synchronously* before publishing the
//!    diagnostic [`SecurityEvent::Mismatch`] — see the load-bearing
//!    invariant documented in [`super`].
//!
//! ## Comparison strategy
//!
//! Scalar return types (`U256`, `u64`, `Bytes`, etc.) are compared by
//! value equality. Compound types (`Block`, `Receipt`, `Transaction`,
//! `Proof`) are compared by a **consensus-anchored projection** rather
//! than full JSON, because honest mainstream RPCs legitimately differ
//! from each other on derived/optional fields (`size`, `total_difficulty`,
//! `mix_hash`, `withdrawals`, etc.). A full-JSON comparison flips
//! `Tainted` against any honest RPC the first time the user reads a
//! compound type.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockHash, Bytes, StorageKey, TxHash, B256, U256, U64};
use alloy::providers::{
    Caller, EthCall, EthCallMany, EthCallManyParams, EthCallParams, EthGetBlock, Provider,
    ProviderCall, RootProvider, RpcWithBlock,
};
use alloy::rpc::types::state::StateOverride;
use alloy::rpc::types::{
    AccessListResult, BlockTransactionsKind, Bundle, EIP1186AccountProofResponse, EthCallResponse,
    Filter, Log,
};
use alloy::transports::{TransportErrorKind, TransportResult};
use helios_common::network_spec::NetworkSpec;
use serde::Serialize;

use crate::client::api::HeliosApi;
use crate::provider::error::{FailureInfo, MismatchInfo};
use crate::provider::status::VerificationStatus;

// tokio::spawn requires Send; wasm_bindgen_futures::spawn_local does not.
// MaybeSend collapses the two into one bound the verifier closures can satisfy
// on both targets.
#[cfg(not(target_arch = "wasm32"))]
trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}
#[cfg(target_arch = "wasm32")]
trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// Optimistic-first helios provider. Returns the unverified RPC value
/// immediately and verifies in the background.
///
/// Cheap to clone — internally just an `Arc<Inner<N>>`.
///
/// Share the [`VerificationStatus<N>`] with a sibling
/// [`super::VerifiedHeliosProvider<N>`] when an embedder wants both:
/// the optimistic provider drives unverified rendering and verification
/// fan-out, while the verified provider is reserved for sign-gated
/// reads (balance/nonce immediately before signing).
#[derive(Clone)]
pub struct OptimisticHeliosProvider<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

pub(crate) struct Inner<N: NetworkSpec> {
    helios: Arc<dyn HeliosApi<N>>,
    root: RootProvider<N>,
    status: VerificationStatus<N>,
    _network: PhantomData<N>,
}

impl<N: NetworkSpec> OptimisticHeliosProvider<N> {
    /// Construct from a pre-built [`HeliosApi`] impl, an alloy
    /// [`RootProvider<N>`] over the same execution RPC, and a shared
    /// [`VerificationStatus<N>`] handle.
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

    /// Returns the [`VerificationStatus`] handle for observing and
    /// gating on verification activity.
    pub fn verification_status(&self) -> &VerificationStatus<N> {
        &self.inner.status
    }

    /// Spawn a background verifier for a single read call.
    ///
    /// `project` produces a comparable representation of each value —
    /// typically a hash, root, or scalar — that captures only the
    /// consensus-anchored content. Two values that share a `project`
    /// output are treated as equivalent, even if their JSON shapes
    /// differ. This prevents honest divergence on derived/optional
    /// fields (`size`, `total_difficulty`, `mix_hash`, …) from
    /// flipping `Tainted` against a real RPC.
    ///
    /// The helios call is wrapped in `catch_unwind` so a panic in the
    /// verifier (proof-decoding bug, arithmetic overflow, fuzzy input)
    /// surfaces as `record_failed` with a descriptive `FailureInfo`
    /// rather than silently leaving counters at "pending → drop →
    /// Cancelled".
    fn spawn_verifier<T, U, F, Fut, P, R>(
        &self,
        method: &'static str,
        unverified: T,
        verify: F,
        project: P,
    ) where
        T: MaybeSend + 'static,
        U: MaybeSend + 'static,
        F: FnOnce(Arc<dyn HeliosApi<N>>) -> Fut + MaybeSend + 'static,
        Fut: Future<Output = eyre::Result<U>> + MaybeSend + 'static,
        P: Fn(&T, &U) -> (R, R) + MaybeSend + 'static,
        R: PartialEq + std::fmt::Debug,
    {
        use futures::future::FutureExt;
        let handle = self.inner.status._bump_pending();
        let helios = self.inner.helios.clone();
        #[cfg(not(target_arch = "wasm32"))]
        let run = tokio::spawn;
        #[cfg(target_arch = "wasm32")]
        let run = wasm_bindgen_futures::spawn_local;
        run(async move {
            let result = std::panic::AssertUnwindSafe(verify(helios))
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(verified)) => {
                    let (u_repr, v_repr) = project(&unverified, &verified);
                    if u_repr == v_repr {
                        handle.record_verified();
                    } else {
                        handle.record_mismatch(MismatchInfo::now(
                            method,
                            format!("{u_repr:?}"),
                            format!("{v_repr:?}"),
                        ));
                    }
                }
                Ok(Err(err)) => {
                    handle.record_failed(FailureInfo {
                        method,
                        error: err.to_string().into_boxed_str(),
                        at: Instant::now(),
                    });
                }
                Err(panic) => {
                    handle.record_failed(FailureInfo {
                        method,
                        error: format!("verifier panicked: {}", panic_message(panic))
                            .into_boxed_str(),
                        at: Instant::now(),
                    });
                }
            }
        });
    }
}

/// Extract a string message from a [`std::panic::catch_unwind`] payload.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Projector for scalar types: both sides serialised to a string
/// representation and compared. Use for `U256`, `u64`, `Bytes`, etc.
fn scalar_projection<T: Serialize>(u: &T, v: &T) -> (String, String) {
    let u_s = serde_json::to_string(u).unwrap_or_else(|_| "<unserializable>".into());
    let v_s = serde_json::to_string(v).unwrap_or_else(|_| "<unserializable>".into());
    (u_s, v_s)
}

/// Projector for block-shaped types. Compares only the consensus-
/// anchored block hash, derived from the header. Honest RPCs may
/// differ on `size`, `total_difficulty`, `mix_hash`, etc. — but the
/// block hash (or its substitute, the canonical JSON of the header)
/// is invariant across implementations.
/// Projector for option-of-block types. Maps `None` on both sides to
/// equality and falls back to the block hash on `Some`.
fn opt_block_projection<B>(u: &Option<B>, v: &Option<B>) -> (Option<B256>, Option<B256>)
where
    B: alloy::network::BlockResponse,
    B::Header: alloy::network::primitives::HeaderResponse,
{
    use alloy::network::primitives::HeaderResponse;
    (
        u.as_ref().map(|b| b.header().hash()),
        v.as_ref().map(|b| b.header().hash()),
    )
}

/// Projector for receipt-shaped types. Receipt encoding (via
/// `NetworkSpec::encode_receipt`) is consensus-defined; comparing the
/// encoded bytes captures exactly the consensus-anchored content.
fn receipt_projection<N: NetworkSpec>(
    u: &Option<N::ReceiptResponse>,
    v: &Option<N::ReceiptResponse>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    (
        u.as_ref().map(N::encode_receipt),
        v.as_ref().map(N::encode_receipt),
    )
}

/// Projector for transaction-shaped types. Transaction encoding via
/// `NetworkSpec::encode_transaction` is consensus-defined.
fn transaction_projection<N: NetworkSpec>(
    u: &Option<N::TransactionResponse>,
    v: &Option<N::TransactionResponse>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    (
        u.as_ref().map(N::encode_transaction),
        v.as_ref().map(N::encode_transaction),
    )
}

type EncodedReceipts = Option<Vec<Vec<u8>>>;

/// Projector for `Vec<Receipt>` from `eth_getBlockReceipts`. Compares
/// each receipt via consensus-defined encoding.
fn block_receipts_projection<N: NetworkSpec>(
    u: &Option<Vec<N::ReceiptResponse>>,
    v: &Option<Vec<N::ReceiptResponse>>,
) -> (EncodedReceipts, EncodedReceipts) {
    (
        u.as_ref()
            .map(|rs| rs.iter().map(N::encode_receipt).collect()),
        v.as_ref()
            .map(|rs| rs.iter().map(N::encode_receipt).collect()),
    )
}

/// Projector for `EIP1186AccountProofResponse`. Compares the consensus-
/// anchored fields: account state (nonce, balance, storage_hash,
/// code_hash) and the storage proof's key/value pairs. The Merkle
/// proof intermediate nodes are not compared (they're large and
/// derivable from the verified path).
fn proof_projection(
    u: &EIP1186AccountProofResponse,
    v: &EIP1186AccountProofResponse,
) -> (ProofKey, ProofKey) {
    (ProofKey::from(u), ProofKey::from(v))
}

#[derive(Debug, PartialEq, Eq)]
struct ProofKey {
    nonce: u64,
    balance: U256,
    storage_hash: B256,
    code_hash: B256,
    storage: Vec<(alloy::serde::storage::JsonStorageKey, U256)>,
}

impl From<&EIP1186AccountProofResponse> for ProofKey {
    fn from(p: &EIP1186AccountProofResponse) -> Self {
        Self {
            nonce: p.nonce,
            balance: p.balance,
            storage_hash: p.storage_hash,
            code_hash: p.code_hash,
            storage: p.storage_proof.iter().map(|s| (s.key, s.value)).collect(),
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl<N: NetworkSpec> Provider<N> for OptimisticHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.inner.root
    }

    fn get_balance(&self, address: Address) -> RpcWithBlock<Address, U256, U256> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                let unverified = provider
                    .inner
                    .root
                    .get_balance(address)
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier(
                    "eth_getBalance",
                    unverified,
                    move |h| async move { h.get_balance(address, block_id).await },
                    scalar_projection,
                );
                Ok(unverified)
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
                let unverified = provider
                    .inner
                    .root
                    .get_transaction_count(address)
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier(
                    "eth_getTransactionCount",
                    unverified,
                    move |h| async move { h.get_nonce(address, block_id).await },
                    scalar_projection,
                );
                Ok(unverified)
            }))
        })
    }

    fn get_code_at(&self, address: Address) -> RpcWithBlock<Address, Bytes> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                let unverified = provider
                    .inner
                    .root
                    .get_code_at(address)
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier(
                    "eth_getCode",
                    unverified.clone(),
                    move |h| async move { h.get_code(address, block_id).await },
                    scalar_projection,
                );
                Ok(unverified)
            }))
        })
    }

    fn get_storage_at(&self, address: Address, key: U256) -> RpcWithBlock<(Address, U256), U256> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                let unverified = provider
                    .inner
                    .root
                    .get_storage_at(address, key)
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier(
                    "eth_getStorageAt",
                    unverified,
                    move |h| async move {
                        let b = h.get_storage_at(address, key, block_id).await?;
                        Ok(U256::from_be_bytes(b.0))
                    },
                    scalar_projection,
                );
                Ok(unverified)
            }))
        })
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        let unverified = self.inner.root.get_logs(filter).await?;
        let filter = filter.clone();
        self.spawn_verifier(
            "eth_getLogs",
            unverified.clone(),
            move |h| async move { h.get_logs(&filter).await },
            scalar_projection,
        );
        Ok(unverified)
    }

    fn get_block(&self, block: BlockId) -> EthGetBlock<N::BlockResponse> {
        let provider = self.clone();
        EthGetBlock::new_provider(
            block,
            Box::new(move |kind| {
                let provider = provider.clone();
                let full_tx = matches!(kind, BlockTransactionsKind::Full);
                ProviderCall::BoxedFuture(Box::pin(async move {
                    let unverified = provider.inner.root.get_block(block).kind(kind).await?;
                    provider.spawn_verifier(
                        "eth_getBlockByNumber",
                        unverified.clone(),
                        move |h| async move { h.get_block(block, full_tx).await },
                        opt_block_projection,
                    );
                    Ok(unverified)
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
                    let unverified = provider
                        .inner
                        .root
                        .get_block_by_hash(hash)
                        .kind(kind)
                        .await?;
                    provider.spawn_verifier(
                        "eth_getBlockByHash",
                        unverified.clone(),
                        move |h| async move {
                            h.get_block(BlockId::Hash(hash.into()), full_tx).await
                        },
                        opt_block_projection,
                    );
                    Ok(unverified)
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
                let unverified = provider
                    .inner
                    .root
                    .get_proof(address, keys.clone())
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier(
                    "eth_getProof",
                    unverified.clone(),
                    move |h| async move { h.get_proof(address, &keys, block_id).await },
                    proof_projection,
                );
                Ok(unverified)
            }))
        })
    }

    fn get_transaction_by_hash(
        &self,
        hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<N::TransactionResponse>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            let unverified = provider.inner.root.get_transaction_by_hash(hash).await?;
            provider.spawn_verifier(
                "eth_getTransactionByHash",
                unverified.clone(),
                move |h| async move { h.get_transaction(hash).await },
                transaction_projection::<N>,
            );
            Ok(unverified)
        }))
    }

    fn get_transaction_receipt(
        &self,
        hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<N::ReceiptResponse>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            let unverified = provider.inner.root.get_transaction_receipt(hash).await?;
            provider.spawn_verifier(
                "eth_getTransactionReceipt",
                unverified.clone(),
                move |h| async move { h.get_transaction_receipt(hash).await },
                receipt_projection::<N>,
            );
            Ok(unverified)
        }))
    }

    fn get_block_receipts(
        &self,
        block: BlockId,
    ) -> ProviderCall<(BlockId,), Option<Vec<N::ReceiptResponse>>> {
        let provider = self.clone();
        ProviderCall::BoxedFuture(Box::pin(async move {
            let unverified = provider.inner.root.get_block_receipts(block).await?;
            provider.spawn_verifier(
                "eth_getBlockReceipts",
                unverified.clone(),
                move |h| async move { h.get_block_receipts(block).await },
                block_receipts_projection::<N>,
            );
            Ok(unverified)
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
                let unverified = provider
                    .inner
                    .root
                    .create_access_list(&tx)
                    .block_id(block_id)
                    .await?;
                let tx_for_verifier = tx.clone();
                provider.spawn_verifier(
                    "eth_createAccessList",
                    unverified.clone(),
                    move |h| async move {
                        h.create_access_list(&tx_for_verifier, block_id, None).await
                    },
                    access_list_projection,
                );
                Ok(unverified)
            }))
        })
    }

    fn call_many<'req>(
        &self,
        bundles: &'req [Bundle],
    ) -> EthCallMany<'req, N, Vec<Vec<EthCallResponse>>> {
        // Same gap as VerifiedHeliosProvider: the alloy default for
        // call_many routes via self.weak_client() bypassing every
        // override. Override returns an EthCallMany backed by our
        // refusing Caller<Vec<Vec<EthCallResponse>>>.
        EthCallMany::new(self.clone(), bundles)
    }
}

/// Projector for `AccessListResult`. Compares `access_list` and
/// `gas_used` (consensus-anchored execution outputs). The `error`
/// field is implementation-specific and not compared.
fn access_list_projection(
    u: &AccessListResult,
    v: &AccessListResult,
) -> ((U256, String), (U256, String)) {
    let u_al = serde_json::to_string(&u.access_list).unwrap_or_default();
    let v_al = serde_json::to_string(&v.access_list).unwrap_or_default();
    ((u.gas_used, u_al), (v.gas_used, v_al))
}

/// Block overrides aren't representable in helios's `HeliosApi::call` /
/// `HeliosApi::estimate_gas` signatures today. The optimistic Caller
/// would silently drop them on the verifier side, which would be a
/// trust-model regression — better to refuse the call up front.
fn reject_block_overrides<N: alloy::network::Network>(
    params: &EthCallParams<N>,
) -> TransportResult<()> {
    if params.block_overrides().is_some() {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider does not support block_overrides on eth_call/eth_estimateGas",
        ))
    } else {
        Ok(())
    }
}

impl<N: NetworkSpec> Caller<N, Bytes> for OptimisticHeliosProvider<N> {
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
            let unverified = call_via_root(&provider, tx.clone(), block, overrides.clone()).await?;
            let tx_for_verifier = tx;
            let overrides_for_verifier = overrides;
            provider.spawn_verifier(
                "eth_call",
                unverified.clone(),
                move |h| async move {
                    h.call(&tx_for_verifier, block, overrides_for_verifier)
                        .await
                },
                scalar_projection,
            );
            Ok(unverified)
        })))
    }

    fn estimate_gas(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Bytes>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider: estimate_gas via Caller<N, Bytes> is unsupported; use Provider::estimate_gas",
        ))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, Bytes>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider does not implement eth_callMany",
        ))
    }
}

impl<N: NetworkSpec> Caller<N, U64> for OptimisticHeliosProvider<N> {
    fn call(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, U64>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider: call via Caller<N, U64> is unsupported; use Provider::call",
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
            let unverified =
                estimate_gas_via_root(&provider, tx.clone(), block, overrides.clone()).await?;
            let tx_for_verifier = tx;
            let overrides_for_verifier = overrides;
            provider.spawn_verifier(
                "eth_estimateGas",
                unverified,
                move |h| async move {
                    h.estimate_gas(&tx_for_verifier, block, overrides_for_verifier)
                        .await
                },
                scalar_projection,
            );
            Ok(U64::from(unverified))
        })))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, U64>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider does not implement eth_callMany",
        ))
    }
}

async fn call_via_root<N: NetworkSpec>(
    provider: &OptimisticHeliosProvider<N>,
    tx: N::TransactionRequest,
    block: BlockId,
    overrides: Option<StateOverride>,
) -> TransportResult<Bytes> {
    provider
        .inner
        .root
        .call(tx)
        .block(block)
        .overrides_opt(overrides)
        .await
}

async fn estimate_gas_via_root<N: NetworkSpec>(
    provider: &OptimisticHeliosProvider<N>,
    tx: N::TransactionRequest,
    block: Option<BlockId>,
    overrides: Option<StateOverride>,
) -> TransportResult<u64> {
    let call = provider.inner.root.estimate_gas(tx);
    let call = match block {
        Some(b) => call.block(b),
        None => call,
    };
    call.overrides_opt(overrides).await
}

// Same call_many gap as VerifiedHeliosProvider: alloy's default
// resolves via `weak_client()` and bypasses every override. Refusing
// at the Caller layer surfaces the bypass as a clear error.
impl<N: NetworkSpec> Caller<N, Vec<Vec<EthCallResponse>>> for OptimisticHeliosProvider<N> {
    fn call(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider: Caller<Vec<Vec<EthCallResponse>>>::call is unsupported",
        ))
    }

    fn estimate_gas(
        &self,
        _params: EthCallParams<N>,
    ) -> TransportResult<ProviderCall<EthCallParams<N>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider: Caller<Vec<Vec<EthCallResponse>>>::estimate_gas is unsupported",
        ))
    }

    fn call_many(
        &self,
        _params: EthCallManyParams<'_>,
    ) -> TransportResult<ProviderCall<EthCallManyParams<'static>, Vec<Vec<EthCallResponse>>>> {
        Err(TransportErrorKind::custom_str(
            "OptimisticHeliosProvider does not implement eth_callMany — helios cannot back the per-bundle verified path; fan out via Provider::call per bundle if needed",
        ))
    }
}
