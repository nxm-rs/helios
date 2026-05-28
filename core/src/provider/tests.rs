use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::state::StateOverride;
use alloy::rpc::types::{AccessListResult, EIP1186AccountProofResponse, Filter, Log, SyncStatus};
use alloy::transports::mock::Asserter;
use async_trait::async_trait;
use eyre::Result;
use futures::future::{BoxFuture, FutureExt};
use helios_common::types::{SubEventRx, SubscriptionType};
use helios_ethereum::spec::Ethereum;

use super::error::VerificationError;
use super::event::{HealthStatus, SecurityEvent, VerificationEvent};
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

struct MockHelios {
    get_balance_fn: BalanceFn,
    get_nonce_fn: NonceFn,
    get_logs_fn: LogsFn,
    get_transaction_receipt_fn: ReceiptFn,
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
        _tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        _block_id: BlockId,
        _state_overrides: Option<StateOverride>,
    ) -> Result<Bytes> {
        unimplemented!()
    }
    async fn estimate_gas(
        &self,
        _tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        _block_id: Option<BlockId>,
        _state_overrides: Option<StateOverride>,
    ) -> Result<u64> {
        unimplemented!()
    }
    async fn create_access_list(
        &self,
        _tx: &<Ethereum as alloy::network::Network>::TransactionRequest,
        _block_id: BlockId,
        _state_overrides: Option<StateOverride>,
    ) -> Result<AccessListResult> {
        unimplemented!()
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
    let root: RootProvider<Ethereum> = RootProvider::new(RpcClient::mocked(Asserter::new()));
    let status = VerificationStatus::<Ethereum>::new();
    VerifiedHeliosProvider::from_parts(Arc::new(helios), root, status)
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
    assert!(provider.verification_status().take_security_events().is_some());
    assert!(provider.verification_status().take_security_events().is_none());
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

    let result = status
        .barrier_with_timeout(Duration::from_millis(50))
        .await;
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

    let res = tokio::time::timeout(Duration::from_millis(50), provider.balance_verified(addr(22)))
        .await;
    assert!(res.is_err(), "outer timeout should fire");

    let counts = status.counts().borrow().clone();
    assert_eq!(counts.pending, 0, "Drop path must release the slot");
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.verified, 0);

    release.notify_one();
}

