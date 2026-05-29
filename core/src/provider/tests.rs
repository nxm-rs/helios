use std::sync::Arc;
use std::time::Duration;

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, Bytes, B256, U128, U256, U64};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::state::StateOverride;
use alloy::rpc::types::{
    AccessListResult, EIP1186AccountProofResponse, FeeHistory, Filter, Log, SyncStatus,
};
use alloy::transports::mock::Asserter;
use async_trait::async_trait;
use eyre::Result;
use futures::future::{BoxFuture, FutureExt};
use helios_common::types::{SubEventRx, SubscriptionType};
use helios_ethereum::spec::Ethereum;

use super::error::{MismatchInfo, VerificationError};
use super::event::{HealthStatus, SecurityEvent, VerificationEvent};
use super::optimistic::OptimisticHeliosProvider;
use super::status::VerificationStatus;
use super::value::VerifiedValue;
use super::verified::VerifiedHeliosProvider;
use crate::client::api::HeliosApi;

type BalanceFn = Box<dyn Fn(Address, BlockId) -> BoxFuture<'static, Result<U256>> + Send + Sync>;
type NonceFn = Box<dyn Fn(Address, BlockId) -> BoxFuture<'static, Result<u64>> + Send + Sync>;
type LogsFn = Box<dyn Fn(Filter) -> BoxFuture<'static, Result<Vec<Log>>> + Send + Sync>;
type ReceiptFn = Box<
    dyn Fn(
            B256,
        ) -> BoxFuture<
            'static,
            Result<Option<<Ethereum as alloy::network::Network>::ReceiptResponse>>,
        > + Send
        + Sync,
>;
type TxReq = <Ethereum as alloy::network::Network>::TransactionRequest;
type CallFn = Box<
    dyn Fn(TxReq, BlockId, Option<StateOverride>) -> BoxFuture<'static, Result<Bytes>>
        + Send
        + Sync,
>;
type EstimateFn = Box<
    dyn Fn(TxReq, Option<BlockId>, Option<StateOverride>) -> BoxFuture<'static, Result<u64>>
        + Send
        + Sync,
>;
type AccessListFn = Box<
    dyn Fn(TxReq, BlockId, Option<StateOverride>) -> BoxFuture<'static, Result<AccessListResult>>
        + Send
        + Sync,
>;

struct MockHelios {
    get_balance_fn: BalanceFn,
    get_nonce_fn: NonceFn,
    get_logs_fn: LogsFn,
    get_transaction_receipt_fn: ReceiptFn,
    call_fn: CallFn,
    estimate_gas_fn: EstimateFn,
    create_access_list_fn: AccessListFn,
}

impl Default for MockHelios {
    fn default() -> Self {
        Self {
            get_balance_fn: Box::new(|_, _| {
                async { unimplemented!("MockHelios::get_balance not staged") }.boxed()
            }),
            get_nonce_fn: Box::new(|_, _| {
                async { unimplemented!("MockHelios::get_nonce not staged") }.boxed()
            }),
            get_logs_fn: Box::new(|_| {
                async { unimplemented!("MockHelios::get_logs not staged") }.boxed()
            }),
            get_transaction_receipt_fn: Box::new(|_| {
                async { unimplemented!("MockHelios::get_transaction_receipt not staged") }.boxed()
            }),
            call_fn: Box::new(|_, _, _| {
                async { unimplemented!("MockHelios::call not staged") }.boxed()
            }),
            estimate_gas_fn: Box::new(|_, _, _| {
                async { unimplemented!("MockHelios::estimate_gas not staged") }.boxed()
            }),
            create_access_list_fn: Box::new(|_, _, _| {
                async { unimplemented!("MockHelios::create_access_list not staged") }.boxed()
            }),
        }
    }
}

#[async_trait]
impl HeliosApi<Ethereum> for MockHelios {
    async fn wait_synced(&self) -> Result<()> {
        Ok(())
    }
    async fn shutdown(&self) {}
    async fn get_balance(&self, address: Address, block_id: BlockId) -> Result<U256> {
        (self.get_balance_fn)(address, block_id).await
    }
    async fn get_nonce(&self, address: Address, block_id: BlockId) -> Result<u64> {
        (self.get_nonce_fn)(address, block_id).await
    }
    async fn get_code(&self, _address: Address, _block_id: BlockId) -> Result<Bytes> {
        unimplemented!()
    }
    async fn get_storage_at(
        &self,
        _address: Address,
        _slot: U256,
        _block_id: BlockId,
    ) -> Result<B256> {
        unimplemented!()
    }
    async fn get_proof(
        &self,
        _address: Address,
        _slots: &[B256],
        _block_id: BlockId,
    ) -> Result<EIP1186AccountProofResponse> {
        unimplemented!()
    }
    async fn get_gas_price(&self) -> Result<U256> {
        unimplemented!()
    }
    async fn get_priority_fee(&self) -> Result<U256> {
        unimplemented!()
    }
    async fn get_blob_base_fee(&self) -> Result<U256> {
        unimplemented!()
    }
    async fn get_block_number(&self) -> Result<U256> {
        unimplemented!()
    }
    async fn get_block_transaction_count(&self, _block_id: BlockId) -> Result<Option<u64>> {
        unimplemented!()
    }
    async fn get_block(
        &self,
        _block_id: BlockId,
        _full_tx: bool,
    ) -> Result<Option<<Ethereum as alloy::network::Network>::BlockResponse>> {
        unimplemented!()
    }
    async fn send_raw_transaction(&self, _bytes: &[u8]) -> Result<B256> {
        unimplemented!()
    }
    async fn get_transaction(
        &self,
        _tx_hash: B256,
    ) -> Result<Option<<Ethereum as alloy::network::Network>::TransactionResponse>> {
        unimplemented!()
    }
    async fn get_transaction_by_block_and_index(
        &self,
        _block_id: BlockId,
        _index: u64,
    ) -> Result<Option<<Ethereum as alloy::network::Network>::TransactionResponse>> {
        unimplemented!()
    }
    async fn get_transaction_receipt(
        &self,
        tx_hash: B256,
    ) -> Result<Option<<Ethereum as alloy::network::Network>::ReceiptResponse>> {
        (self.get_transaction_receipt_fn)(tx_hash).await
    }
    async fn get_block_receipts(
        &self,
        _block_id: BlockId,
    ) -> Result<Option<Vec<<Ethereum as alloy::network::Network>::ReceiptResponse>>> {
        unimplemented!()
    }
    async fn call(
        &self,
        tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        block_id: BlockId,
        state_overrides: Option<StateOverride>,
    ) -> Result<Bytes> {
        (self.call_fn)(tx.clone(), block_id, state_overrides).await
    }
    async fn estimate_gas(
        &self,
        tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        block_id: Option<BlockId>,
        state_overrides: Option<StateOverride>,
    ) -> Result<u64> {
        (self.estimate_gas_fn)(tx.clone(), block_id, state_overrides).await
    }
    async fn create_access_list(
        &self,
        tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        block_id: BlockId,
        state_overrides: Option<StateOverride>,
    ) -> Result<AccessListResult> {
        (self.create_access_list_fn)(tx.clone(), block_id, state_overrides).await
    }
    async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        (self.get_logs_fn)(filter.clone()).await
    }
    async fn subscribe(&self, _sub_type: SubscriptionType) -> Result<SubEventRx<Ethereum>> {
        unimplemented!()
    }
    async fn get_filter_logs(&self, _filter_id: U256) -> Result<Vec<Log>> {
        unimplemented!()
    }
    async fn uninstall_filter(&self, _filter_id: U256) -> Result<bool> {
        unimplemented!()
    }
    async fn new_filter(&self, _filter: &Filter) -> Result<U256> {
        unimplemented!()
    }
    async fn new_block_filter(&self) -> Result<U256> {
        unimplemented!()
    }
    async fn get_client_version(&self) -> String {
        "mock".into()
    }
    async fn get_chain_id(&self) -> u64 {
        1
    }
    async fn get_coinbase(&self) -> Result<Address> {
        unimplemented!()
    }
    async fn syncing(&self) -> Result<SyncStatus> {
        unimplemented!()
    }
    async fn current_checkpoint(&self) -> Result<Option<B256>> {
        Ok(None)
    }
    fn new_checkpoints_recv(&self) -> Result<tokio::sync::watch::Receiver<Option<B256>>> {
        let (_, rx) = tokio::sync::watch::channel(None);
        Ok(rx)
    }
}

fn build_provider(helios: MockHelios) -> VerifiedHeliosProvider<Ethereum> {
    build_provider_with_asserter(helios).0
}

fn build_provider_with_asserter(
    helios: MockHelios,
) -> (VerifiedHeliosProvider<Ethereum>, Asserter) {
    let asserter = Asserter::new();
    let root: RootProvider<Ethereum> = RootProvider::new(RpcClient::mocked(asserter.clone()));
    let status = VerificationStatus::<Ethereum>::new();
    (
        VerifiedHeliosProvider::from_parts(Arc::new(helios), root, status),
        asserter,
    )
}

fn addr(byte: u8) -> Address {
    Address::from([byte; 20])
}

#[tokio::test]
async fn verified_call_ticks_verified_counter_and_emits_verbose() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(42)) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let mut counts = provider.verification_status().counts();
    let mut verbose = provider.verification_status().events_verbose();

    let value = provider.balance_verified(addr(1)).await.unwrap();
    assert_eq!(value, U256::from(42));

    let snapshot = counts.borrow_and_update().clone();
    assert_eq!(snapshot.verified, 1);
    assert_eq!(snapshot.pending, 0);
    assert_eq!(snapshot.failed, 0);

    let event = verbose.recv().await.expect("verbose event");
    match event {
        VerificationEvent::Verified { method, value, .. } => {
            assert_eq!(method, "eth_getBalance");
            assert!(matches!(value, VerifiedValue::Balance(v) if v == U256::from(42)));
        }
        other => panic!("expected Verified, got {other:?}"),
    }
}

#[tokio::test]
async fn failed_call_ticks_failed_counter_and_pushes_security_event() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Err(eyre::eyre!("upstream error")) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let mut security_rx = provider
        .verification_status()
        .take_security_events()
        .expect("security receiver not yet taken");

    let err = provider.balance_verified(addr(2)).await.unwrap_err();
    assert!(matches!(err, VerificationError::Failed { .. }));

    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.failed, 1);
    assert_eq!(counts.pending, 0);
    assert_eq!(counts.verified, 0);

    let event = security_rx.recv().await.expect("security event");
    assert!(matches!(event, SecurityEvent::Failed(info) if info.method == "eth_getBalance"));
}

#[tokio::test]
async fn take_security_events_is_take_once() {
    let provider = build_provider(MockHelios::default());
    assert!(provider
        .verification_status()
        .take_security_events()
        .is_some());
    assert!(provider
        .verification_status()
        .take_security_events()
        .is_none());
}

#[tokio::test]
async fn failed_call_does_not_taint() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Err(eyre::eyre!("rpc unreachable")) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let _ = provider.balance_verified(addr(3)).await;

    let health = provider.verification_status().health().borrow().clone();
    assert!(matches!(health, HealthStatus::Healthy));
}

#[tokio::test]
async fn barrier_with_no_pending_returns_immediately() {
    let provider = build_provider(MockHelios::default());
    let snapshot = provider.verification_status().barrier().await.unwrap();
    assert_eq!(snapshot.consensus_tip, 0);
}

#[tokio::test]
async fn barrier_resolves_after_pending_settles() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(7)) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    // Kick off a verified call and let it complete first; barrier opened
    // after must see zero pending and resolve OK.
    provider.balance_verified(addr(4)).await.unwrap();
    provider.verification_status().barrier().await.unwrap();
}

#[tokio::test]
async fn barrier_surfaces_failure() {
    // The verified call awaits a notify so we can interleave: open the
    // barrier while the call is pending, then release the notify so the
    // call fails and the barrier resolves with the failure.
    let release = Arc::new(tokio::sync::Notify::new());
    let release_for_mock = release.clone();
    let mock = MockHelios {
        get_balance_fn: Box::new(move |_, _| {
            let release = release_for_mock.clone();
            async move {
                release.notified().await;
                Err(eyre::eyre!("rpc down"))
            }
            .boxed()
        }),
        ..Default::default()
    };
    let provider = build_provider(mock);
    let status = provider.verification_status().clone();

    let call = tokio::spawn(async move { provider.balance_verified(addr(5)).await });

    while status.counts().borrow().pending == 0 {
        tokio::task::yield_now().await;
    }

    let barrier_fut = status.barrier();
    release.notify_one();
    let snapshot = barrier_fut.await;
    let _ = call.await;

    match snapshot {
        Err(VerificationError::Failed { calls }) => {
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].method, "eth_getBalance");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[tokio::test]
async fn barrier_with_timeout_reports_still_pending() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| {
            async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(U256::ZERO)
            }
            .boxed()
        }),
        ..Default::default()
    };
    let provider = build_provider(mock);
    let status = provider.verification_status().clone();

    let _call = tokio::spawn(async move { provider.balance_verified(addr(6)).await });

    while status.counts().borrow().pending == 0 {
        tokio::task::yield_now().await;
    }

    let result = status.barrier_with_timeout(Duration::from_millis(50)).await;
    match result {
        Err(VerificationError::Timeout { still_pending }) => {
            assert_eq!(still_pending, 1);
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn provider_trait_get_balance_routes_through_verified_path() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(99)) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let v = Provider::<Ethereum>::get_balance(&provider, addr(7))
        .await
        .unwrap();
    assert_eq!(v, U256::from(99));

    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 1);
}

#[tokio::test]
async fn dropping_pending_handle_releases_slot() {
    let status = VerificationStatus::<Ethereum>::new();
    {
        let _handle = status._bump_pending();
        assert_eq!(status.counts().borrow().pending, 1);
    }
    let counts = status.counts().borrow().clone();
    assert_eq!(counts.pending, 0);
    assert_eq!(counts.verified, 0);
    assert_eq!(counts.failed, 0);
}

#[tokio::test]
async fn caller_cancellation_releases_pending_slot() {
    // Mock awaits a notify before returning, so the verified call hangs
    // at the helios.get_balance().await — i.e., before record_verified
    // or record_failed runs. Cancelling the outer future via timeout
    // exercises the PendingHandle Drop path: the slot must be released
    // with no outcome counter ticked.
    let release = Arc::new(tokio::sync::Notify::new());
    let release_for_mock = release.clone();
    let mock = MockHelios {
        get_balance_fn: Box::new(move |_, _| {
            let release = release_for_mock.clone();
            async move {
                release.notified().await;
                Ok(U256::ZERO)
            }
            .boxed()
        }),
        ..Default::default()
    };
    let provider = build_provider(mock);
    let status = provider.verification_status().clone();

    let res = tokio::time::timeout(
        Duration::from_millis(50),
        provider.balance_verified(addr(22)),
    )
    .await;
    assert!(res.is_err(), "outer timeout should fire");

    let counts = status.counts().borrow().clone();
    assert_eq!(counts.pending, 0, "Drop path must release the slot");
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.verified, 0);

    release.notify_one();
}

#[tokio::test]
async fn provider_trait_call_routes_through_verified_path() {
    let mock = MockHelios {
        call_fn: Box::new(|_, _, _| async { Ok(Bytes::from_static(&[0xab, 0xcd])) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let tx = TxReq::default();
    let bytes = Provider::<Ethereum>::call(&provider, tx).await.unwrap();
    assert_eq!(bytes.as_ref(), &[0xab, 0xcd]);

    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 1);
    assert_eq!(counts.failed, 0);
}

#[tokio::test]
async fn provider_trait_estimate_gas_routes_through_verified_path() {
    let mock = MockHelios {
        estimate_gas_fn: Box::new(|_, _, _| async { Ok(21_000u64) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let tx = TxReq::default();
    let gas = Provider::<Ethereum>::estimate_gas(&provider, tx)
        .await
        .unwrap();
    assert_eq!(gas, 21_000);

    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 1);
}

#[tokio::test]
async fn provider_trait_create_access_list_routes_through_verified_path() {
    use alloy::eips::eip2930::{AccessList, AccessListItem};
    let item = AccessListItem {
        address: addr(33),
        storage_keys: vec![B256::ZERO],
    };
    let expected = AccessListResult {
        access_list: AccessList(vec![item]),
        gas_used: U256::from(50_000),
        error: None,
    };
    let expected_for_mock = expected.clone();
    let mock = MockHelios {
        create_access_list_fn: Box::new(move |_, _, _| {
            let expected = expected_for_mock.clone();
            async move { Ok(expected) }.boxed()
        }),
        ..Default::default()
    };
    let provider = build_provider(mock);

    let tx = TxReq::default();
    let result = Provider::<Ethereum>::create_access_list(&provider, &tx)
        .await
        .unwrap();
    assert_eq!(result.gas_used, U256::from(50_000));
    assert_eq!(result.access_list.0.len(), 1);

    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 1);
}

#[tokio::test]
async fn provider_trait_call_with_block_overrides_is_refused() {
    use alloy::rpc::types::BlockOverrides;
    let mock = MockHelios {
        call_fn: Box::new(|_, _, _| async { Ok(Bytes::new()) }.boxed()),
        ..Default::default()
    };
    let provider = build_provider(mock);
    let tx = TxReq::default();

    let err = Provider::<Ethereum>::call(&provider, tx)
        .with_block_overrides(BlockOverrides::default())
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("block_overrides"),
        "expected block_overrides refusal, got: {msg}"
    );

    // Mock was never invoked since the override is refused before dispatch.
    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 0);
    assert_eq!(counts.failed, 0);
}

#[tokio::test]
async fn provider_trait_call_many_is_refused_not_silently_bypassed() {
    use alloy::rpc::types::Bundle;
    let provider = build_provider(MockHelios::default());

    let bundles: [Bundle; 0] = [];
    let err = Provider::<Ethereum>::call_many(&provider, &bundles)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("eth_callMany"),
        "expected eth_callMany refusal, got: {msg}"
    );
}

#[tokio::test]
async fn gas_price_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U128::from(12_345u128));

    let v = provider.gas_price_unverifiable().await.unwrap();
    assert_eq!(v.method(), "eth_gasPrice");
    assert_eq!(v.into_inner(), 12_345);

    // Verification counters do not tick for unverifiable methods.
    let counts = provider.verification_status().counts().borrow().clone();
    assert_eq!(counts.verified, 0);
    assert_eq!(counts.failed, 0);
}

#[tokio::test]
async fn priority_fee_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U128::from(2_500_000_000u128));

    let v = provider.priority_fee_unverifiable().await.unwrap();
    assert_eq!(v.method(), "eth_maxPriorityFeePerGas");
    assert_eq!(v.into_inner(), 2_500_000_000);
}

#[tokio::test]
async fn blob_base_fee_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U128::from(1u128));

    let v = provider.blob_base_fee_unverifiable().await.unwrap();
    assert_eq!(v.method(), "eth_blobBaseFee");
    assert_eq!(v.into_inner(), 1);
}

#[tokio::test]
async fn block_number_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U64::from(19_000_000u64));

    let v = provider.block_number_unverifiable().await.unwrap();
    assert_eq!(v.method(), "eth_blockNumber");
    assert_eq!(v.into_inner(), 19_000_000);
}

#[tokio::test]
async fn chain_id_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U64::from(1u64));

    let v = provider.chain_id_unverifiable().await.unwrap();
    assert_eq!(v.method(), "eth_chainId");
    assert_eq!(v.into_inner(), 1);
}

#[tokio::test]
async fn fee_history_unverifiable_returns_wrapped_rpc_value() {
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    let expected = FeeHistory {
        oldest_block: 18_999_995,
        base_fee_per_gas: vec![10, 11, 12, 13, 14, 15],
        gas_used_ratio: vec![0.5, 0.6, 0.7, 0.8, 0.9],
        ..Default::default()
    };
    asserter.push_success(&expected);

    let v = provider
        .fee_history_unverifiable(5, BlockNumberOrTag::Latest, &[25.0, 75.0])
        .await
        .unwrap();
    assert_eq!(v.method(), "eth_feeHistory");
    assert_eq!(v.as_inner().oldest_block, 18_999_995);
    assert_eq!(v.into_inner().gas_used_ratio.len(), 5);
}

#[tokio::test]
async fn assert_chain_id_matches_helios_ok() {
    use super::verified::ChainIdMismatch;
    // MockHelios::get_chain_id returns 1 by default; assert the RPC
    // also returns 1.
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U64::from(1u64));
    let r = provider.assert_chain_id_matches_helios().await;
    assert!(matches!(r, Ok(())), "got {r:?}");
    let _ = ChainIdMismatch::Rpc("dummy".into());
}

#[tokio::test]
async fn assert_chain_id_matches_helios_errors_on_mismatch() {
    use super::verified::ChainIdMismatch;
    // Mock helios = 1, RPC = 137 → mismatch.
    let (provider, asserter) = build_provider_with_asserter(MockHelios::default());
    asserter.push_success(&U64::from(137u64));
    let err = provider.assert_chain_id_matches_helios().await.unwrap_err();
    assert!(
        matches!(
            err,
            ChainIdMismatch::Mismatch {
                helios: 1,
                rpc: 137
            }
        ),
        "got {err:?}"
    );
}

fn build_optimistic_with_asserter(
    helios: MockHelios,
) -> (
    OptimisticHeliosProvider<Ethereum>,
    Asserter,
    VerificationStatus<Ethereum>,
) {
    let asserter = Asserter::new();
    let root: RootProvider<Ethereum> = RootProvider::new(RpcClient::mocked(asserter.clone()));
    let status = VerificationStatus::<Ethereum>::new();
    (
        OptimisticHeliosProvider::from_parts(Arc::new(helios), root, status.clone()),
        asserter,
        status,
    )
}

#[tokio::test]
async fn optimistic_matching_value_ticks_verified_and_stays_healthy() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(100)) }.boxed()),
        ..Default::default()
    };
    let (provider, asserter, status) = build_optimistic_with_asserter(mock);
    asserter.push_success(&U256::from(100));

    let v = Provider::<Ethereum>::get_balance(&provider, addr(40))
        .await
        .unwrap();
    assert_eq!(v, U256::from(100));

    // Background verification has to settle. Spin briefly.
    let mut counts = status.counts();
    while counts.borrow().verified == 0 {
        let _ = counts.changed().await;
    }

    let snapshot = counts.borrow().clone();
    assert_eq!(snapshot.verified, 1);
    assert_eq!(snapshot.mismatched, 0);
    assert_eq!(snapshot.failed, 0);
    assert!(matches!(*status.health().borrow(), HealthStatus::Healthy));
}

#[tokio::test]
async fn optimistic_mismatch_flips_tainted_before_security_event() {
    // Mock helios returns 200; asserter (unverified RPC) returns 100.
    // Optimistic flow: caller sees 100 (unverified), background
    // verifier sees 200 from helios -> mismatch.
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(200)) }.boxed()),
        ..Default::default()
    };
    let (provider, asserter, status) = build_optimistic_with_asserter(mock);
    asserter.push_success(&U256::from(100));

    let mut health = status.health();
    let mut security_rx = status.take_security_events().expect("rx");

    let v = Provider::<Ethereum>::get_balance(&provider, addr(41))
        .await
        .unwrap();
    assert_eq!(v, U256::from(100), "caller sees unverified value");

    // Wait for Tainted on health(). This is the load-bearing assertion:
    // health() flips before security_rx receives the event.
    loop {
        let _ = health.changed().await;
        if matches!(*health.borrow(), HealthStatus::Tainted { .. }) {
            break;
        }
    }

    // health() is Tainted. Only NOW should the security event be visible.
    // Drain it and confirm shape.
    let event = security_rx.recv().await.expect("security event");
    match event {
        SecurityEvent::Mismatch(info) => {
            assert_eq!(info.method, "eth_getBalance");
            assert!(info.unverified.contains("64")); // 100 in hex
            assert!(info.verified.contains("c8")); // 200 in hex
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }

    let counts = status.counts().borrow().clone();
    assert_eq!(counts.verified, 0);
    assert_eq!(counts.mismatched, 1);
}

#[tokio::test]
async fn barrier_refuses_when_tainted() {
    let mock = MockHelios {
        get_balance_fn: Box::new(|_, _| async { Ok(U256::from(2)) }.boxed()),
        ..Default::default()
    };
    let (provider, asserter, status) = build_optimistic_with_asserter(mock);
    asserter.push_success(&U256::from(1));

    let _ = Provider::<Ethereum>::get_balance(&provider, addr(42)).await;
    let mut health = status.health();
    while !matches!(*health.borrow(), HealthStatus::Tainted { .. }) {
        let _ = health.changed().await;
    }

    let err = status.barrier().await.unwrap_err();
    assert!(matches!(err, VerificationError::Tainted), "got {err:?}");
}

#[tokio::test]
async fn acknowledge_mismatch_clears_tainted_only() {
    let status = VerificationStatus::<Ethereum>::new();
    // No taint -> noop.
    status.acknowledge_mismatch();
    assert!(matches!(*status.health().borrow(), HealthStatus::Healthy));

    // Force Tainted via the producer surface.
    let info = MismatchInfo {
        method: "eth_getBalance",
        unverified: "0x1".into(),
        verified: "0x2".into(),
        at: std::time::Instant::now(),
    };
    let handle = status._bump_pending();
    handle.record_mismatch(info);
    assert!(matches!(
        *status.health().borrow(),
        HealthStatus::Tainted { .. }
    ));

    status.acknowledge_mismatch();
    assert!(matches!(*status.health().borrow(), HealthStatus::Healthy));
}

#[tokio::test]
async fn acknowledge_mismatch_does_not_clobber_stalled() {
    let status = VerificationStatus::<Ethereum>::new();
    status._set_health(HealthStatus::Stalled);
    status.acknowledge_mismatch();
    assert!(matches!(*status.health().borrow(), HealthStatus::Stalled));
}

#[tokio::test]
async fn set_health_cannot_clobber_tainted() {
    let status = VerificationStatus::<Ethereum>::new();
    let info = MismatchInfo {
        method: "eth_getBalance",
        unverified: "0x1".into(),
        verified: "0x2".into(),
        at: std::time::Instant::now(),
    };
    let handle = status._bump_pending();
    handle.record_mismatch(info);
    assert!(matches!(
        *status.health().borrow(),
        HealthStatus::Tainted { .. }
    ));

    // A "supervisor recovery" call MUST NOT clobber Tainted.
    status._set_health(HealthStatus::Healthy);
    assert!(
        matches!(*status.health().borrow(), HealthStatus::Tainted { .. }),
        "_set_health(Healthy) silently overwrote Tainted"
    );

    // Stalled must also not clobber Tainted.
    status._set_health(HealthStatus::Stalled);
    assert!(
        matches!(*status.health().borrow(), HealthStatus::Tainted { .. }),
        "_set_health(Stalled) silently overwrote Tainted"
    );
}

#[tokio::test]
async fn acknowledge_mismatch_emits_security_event() {
    let status = VerificationStatus::<Ethereum>::new();
    let mut rx = status.take_security_events().unwrap();

    // First push a mismatch so there's something to acknowledge.
    let info = MismatchInfo {
        method: "eth_getBalance",
        unverified: "0x1".into(),
        verified: "0x2".into(),
        at: std::time::Instant::now(),
    };
    let handle = status._bump_pending();
    handle.record_mismatch(info);

    // Drain the Mismatch event.
    let _ = rx.recv().await;

    // Now acknowledge.
    status.acknowledge_mismatch();

    // A MismatchAcknowledged event should follow.
    let event = rx.recv().await.expect("MismatchAcknowledged");
    assert!(matches!(event, SecurityEvent::MismatchAcknowledged { .. }));
}

#[tokio::test]
async fn optimistic_get_logs_matching_value_ticks_verified() {
    // get_logs is the direct-async-fn override; cover that the shape
    // works through the spawn_verifier helper just like the builder ones.
    let mock = MockHelios {
        get_logs_fn: Box::new(|_| async { Ok(Vec::new()) }.boxed()),
        ..Default::default()
    };
    let (provider, asserter, status) = build_optimistic_with_asserter(mock);
    asserter.push_success(&Vec::<Log>::new());

    let logs = Provider::<Ethereum>::get_logs(&provider, &Filter::new())
        .await
        .unwrap();
    assert!(logs.is_empty());

    let mut counts = status.counts();
    while counts.borrow().verified == 0 {
        let _ = counts.changed().await;
    }
    assert_eq!(counts.borrow().verified, 1);
    assert_eq!(counts.borrow().mismatched, 0);
}

#[tokio::test]
async fn optimistic_get_transaction_receipt_some_none_mismatch_is_caught() {
    // ProviderCall<Option<T>> path. RPC says no receipt yet (None);
    // helios returns a (forged) receipt -> mismatch.
    let mock_receipt = nonexistent_receipt();
    let mock_receipt_for_mock = mock_receipt.clone();
    let mock = MockHelios {
        get_transaction_receipt_fn: Box::new(move |_| {
            let r = mock_receipt_for_mock.clone();
            async move { Ok(Some(r)) }.boxed()
        }),
        ..Default::default()
    };
    let (provider, asserter, status) = build_optimistic_with_asserter(mock);
    asserter.push_success(&Option::<<Ethereum as alloy::network::Network>::ReceiptResponse>::None);

    let v = Provider::<Ethereum>::get_transaction_receipt(&provider, B256::ZERO)
        .await
        .unwrap();
    assert!(v.is_none(), "caller sees the unverified None");

    let mut health = status.health();
    while !matches!(*health.borrow(), HealthStatus::Tainted { .. }) {
        let _ = health.changed().await;
    }

    let counts = status.counts().borrow().clone();
    assert_eq!(counts.mismatched, 1);
    assert_eq!(counts.verified, 0);
    let _ = mock_receipt;
}

fn nonexistent_receipt() -> <Ethereum as alloy::network::Network>::ReceiptResponse {
    helios_test_utils::rpc_tx_receipt()
}
