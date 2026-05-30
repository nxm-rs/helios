#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::sync::RwLock;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use alloy::{
    consensus::TrieAccount,
    primitives::{Address, Bytes, B256},
    rpc::types::{EIP1186AccountProofResponse, EIP1186StorageProof},
};
use schnellru::{ByLength, LruMap};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::{mpsc, oneshot};

#[cfg(not(target_arch = "wasm32"))]
use helios_common::code_store::CodeStore;
use helios_common::types::Account;

// Cache capacities
// High turnover due to block updates.
const ACCOUNTS_CACHE_SIZE: u32 = 128;

// Each entry is an LRU of slots per storage root.
const STORAGE_CACHE_SIZE: u32 = 64;
const STORAGE_SLOTS_PER_ROOT_CACHE_SIZE: u32 = 256;

// Code: Static and can be shared. Most valuable cache.
const CODE_CACHE_SIZE: u32 = 256;

/// Message handed to the background code persistence worker.
#[cfg(not(target_arch = "wasm32"))]
enum CodeMsg {
    /// A new code blob just landed in the LRU; persist on the next
    /// flush tick.
    Persist(B256, Bytes),
    /// Drain the buffer to the store now and signal completion.
    /// Used by callers that want to guarantee persistence before
    /// shutdown.
    Flush(oneshot::Sender<()>),
}

pub struct Cache {
    /// Storage proofs: content-addressed by storage_hash
    /// storage_hash -> slot -> full storage proof
    storage: RwLock<LruMap<B256, LruMap<B256, EIP1186StorageProof>>>,

    /// Code: content-addressed by code_hash
    /// code_hash -> bytecode
    code: RwLock<LruMap<B256, Bytes>>,

    /// Account proofs: block-specific
    /// (address, block_hash) -> account proof response (with empty storage_proof)
    accounts: RwLock<LruMap<(Address, B256), EIP1186AccountProofResponse>>,

    /// Sender into the background code persistence worker. `None`
    /// when no `CodeStore` is configured. Sends are fire-and-forget
    /// so the hot path never blocks on IO.
    #[cfg(not(target_arch = "wasm32"))]
    code_msg_tx: Option<mpsc::UnboundedSender<CodeMsg>>,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(LruMap::new(ByLength::new(STORAGE_CACHE_SIZE))),
            code: RwLock::new(LruMap::new(ByLength::new(CODE_CACHE_SIZE))),
            accounts: RwLock::new(LruMap::new(ByLength::new(ACCOUNTS_CACHE_SIZE))),
            #[cfg(not(target_arch = "wasm32"))]
            code_msg_tx: None,
        }
    }

    /// Build a `Cache` whose code sub-cache is backed by an external
    /// `CodeStore`. The store is consulted exactly once, here, to
    /// warm the in-memory LRU; subsequent reads are pure memory.
    ///
    /// A background tokio task is spawned that drains a channel of
    /// newly cached blobs on every `flush_interval` tick and hands
    /// the batch to `store.persist`. The hot insert path only
    /// touches the channel, never the store, so reads and writes
    /// are not coupled to IO latency.
    ///
    /// Must be called from inside an active tokio runtime, since the
    /// flush worker is a `tokio::spawn`-ed task.
    ///
    /// Use [`Cache::flush_code_store`] to force a synchronous flush
    /// before shutdown.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_code_store(store: Arc<dyn CodeStore>, flush_interval: Duration) -> Self {
        // Warm the in-memory LRU from whatever the store has on hand.
        // The LRU evicts beyond `CODE_CACHE_SIZE`, so the most useful
        // strategy for a `CodeStore` is to return its newest entries
        // last; older ones will fall out on insertion.
        let preload = store.load_all();
        let cache = Self::new();
        {
            let mut code = cache.code.write().unwrap_or_else(|e| e.into_inner());
            for (hash, bytes) in preload {
                code.insert(hash, bytes);
            }
        }

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(code_persistence_worker(rx, store, flush_interval));

        Self {
            code_msg_tx: Some(tx),
            ..cache
        }
    }

    /// Drain any code blobs still pending persistence and wait for
    /// the worker to write them to the `CodeStore`. Returns
    /// immediately if no store is configured or the worker has
    /// already exited.
    ///
    /// Intended for graceful shutdown paths: between two periodic
    /// flushes, up to one window's worth of newly cached code may be
    /// queued only in memory. Calling this before drop guarantees
    /// the queue is flushed; otherwise that window is the cost of
    /// the latency-first design.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn flush_code_store(&self) {
        let Some(tx) = &self.code_msg_tx else { return };
        let (notify_tx, notify_rx) = oneshot::channel();
        if tx.send(CodeMsg::Flush(notify_tx)).is_err() {
            // Worker has exited; nothing to flush.
            return;
        }
        let _ = notify_rx.await;
    }

    /// Insert a VERIFIED account proof response into the cache.
    ///
    /// This method distributes data to appropriate caches:
    /// - Account proof (with empty storage_proof) goes to accounts cache
    /// - Storage proofs go to the content-addressed storage cache
    /// - Code (if provided) goes to the content-addressed code cache
    ///
    /// # Arguments
    /// * `response` - The account proof response from RPC
    /// * `code` - Optional contract code (if it was fetched)
    /// * `block_hash` - The block hash this proof is valid for
    pub(crate) fn insert(
        &self,
        response: EIP1186AccountProofResponse,
        code: Option<Bytes>,
        block_hash: B256,
    ) {
        let storage_hash = response.storage_hash;

        if !response.storage_proof.is_empty() {
            let mut storage = self.storage.write().unwrap_or_else(|e| e.into_inner());

            if storage.peek(&storage_hash).is_none() {
                storage.insert(
                    storage_hash,
                    LruMap::new(ByLength::new(STORAGE_SLOTS_PER_ROOT_CACHE_SIZE)),
                );
            }

            if let Some(storage_map) = storage.get(&storage_hash) {
                for proof in &response.storage_proof {
                    storage_map.insert(proof.key.as_b256(), proof.clone());
                }
            }
        }

        if let Some(code) = code {
            {
                let mut code_cache = self.code.write().unwrap_or_else(|e| e.into_inner());
                code_cache.insert(response.code_hash, code.clone());
            }
            // Fire and forget into the persistence channel. `Bytes`
            // is `Arc`-backed, so the clone is cheap. A `send` error
            // means the worker has exited, which is fine: the
            // in-memory LRU is the source of truth.
            //
            // EIP-7702 delegation designators are skipped: they leak
            // which delegation targets the user has touched if the
            // data dir is inspected, and the disk-cache saving for a
            // 23-byte blob is rounding error compared to the
            // surrounding TLS handshake. Designators are still kept
            // in the in-memory LRU so revm hits the cache when it
            // calls a delegated EOA repeatedly within a session.
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(tx) = &self.code_msg_tx {
                if !is_delegation_designator(&code) {
                    let _ = tx.send(CodeMsg::Persist(response.code_hash, code));
                }
            }
        }

        let account_response = EIP1186AccountProofResponse {
            storage_proof: vec![],
            ..response
        };

        let mut accounts = self.accounts.write().unwrap_or_else(|e| e.into_inner());
        accounts.insert((account_response.address, block_hash), account_response);
    }

    /// Get code by code hash. Hash MUST come from a verified account proof.
    pub fn get_code(&self, code_hash: B256) -> Option<Bytes> {
        let mut code = self.code.write().unwrap_or_else(|e| e.into_inner());
        code.get(&code_hash).cloned()
    }

    /// Try to recover code for an address from any cached account entry.
    ///
    /// This is intentionally "optimistic": we only use the code if a freshly
    /// fetched proof confirms the same code hash.
    pub fn get_code_optimistically(&self, address: Address) -> Option<(B256, Bytes)> {
        let code_hash = {
            let accounts = self.accounts.read().unwrap_or_else(|e| e.into_inner());
            accounts.iter().find_map(|((cached_address, _), account)| {
                (*cached_address == address).then_some(account.code_hash)
            })?
        };

        let code = self.get_code(code_hash)?;
        Some((code_hash, code))
    }

    /// Insert a verified [`Account`] into the cache, distributing its data to
    /// the account-proof, storage-proof, and code sub-caches.
    pub(crate) fn insert_account(&self, address: Address, account: &Account, block_hash: B256) {
        self.insert(
            EIP1186AccountProofResponse {
                address,
                balance: account.account.balance,
                code_hash: account.account.code_hash,
                nonce: account.account.nonce,
                storage_hash: account.account.storage_root,
                account_proof: account.account_proof.clone(),
                storage_proof: account.storage_proof.clone(),
            },
            account.code.clone(),
            block_hash,
        );
    }

    /// Get an account proof response with requested storage slots.
    ///
    /// # Arguments
    /// * `address` - The address of the account
    /// * `slots` - The storage slots to get
    /// * `block_hash` - The block hash this proof is valid for
    ///
    /// # Returns
    /// * `(EIP1186AccountProofResponse, Vec<B256>)` - The account proof response and the missing slots
    pub fn get_account_proof(
        &self,
        address: Address,
        slots: &[B256],
        block_hash: B256,
    ) -> Option<(EIP1186AccountProofResponse, Vec<B256>)> {
        let account = {
            let mut accounts = self.accounts.write().unwrap_or_else(|e| e.into_inner());
            accounts.get(&(address, block_hash)).cloned()?
        };

        let storage_hash = account.storage_hash;
        let mut storage_proofs = Vec::new();
        let mut missing_slots = Vec::new();

        {
            let mut storage = self.storage.write().unwrap_or_else(|e| e.into_inner());
            if let Some(storage_map) = storage.get(&storage_hash) {
                for slot in slots {
                    if let Some(proof) = storage_map.get(slot) {
                        storage_proofs.push(proof.clone());
                    } else {
                        missing_slots.push(*slot);
                    }
                }
            } else if !slots.is_empty() {
                // No storage cached for this storage_hash, all slots are missing
                missing_slots.extend_from_slice(slots);
            }
        }

        let response = EIP1186AccountProofResponse {
            storage_proof: storage_proofs,
            ..account
        };

        Some((response, missing_slots))
    }

    /// Get an Account with requested storage slots.
    pub fn get_account(
        &self,
        address: Address,
        slots: &[B256],
        block_hash: B256,
    ) -> Option<(Account, Vec<B256>)> {
        let (response, missing_slots) = self.get_account_proof(address, slots, block_hash)?;

        let code = self.get_code(response.code_hash);

        let account = Account {
            account: TrieAccount {
                nonce: response.nonce,
                balance: response.balance,
                storage_root: response.storage_hash,
                code_hash: response.code_hash,
            },
            code,
            account_proof: response.account_proof,
            storage_proof: response.storage_proof,
        };

        Some((account, missing_slots))
    }
}

/// Background task that owns the persistence-side of a `Cache` with
/// a configured `CodeStore`. It batches inserts arriving on `rx`
/// and either:
///
/// 1. flushes on each `flush_interval` tick, or
/// 2. flushes immediately on a `CodeMsg::Flush` request, or
/// 3. drains and exits when the sender is dropped (cache going away).
#[cfg(not(target_arch = "wasm32"))]
async fn code_persistence_worker(
    mut rx: mpsc::UnboundedReceiver<CodeMsg>,
    store: Arc<dyn CodeStore>,
    flush_interval: Duration,
) {
    use tokio::time::{interval, MissedTickBehavior};

    let mut pending: Vec<(B256, Bytes)> = Vec::new();
    let mut ticker = interval(flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The first tick fires immediately by default; skip it so a
    // freshly built cache does not race a write before the consumer
    // has had any opportunity to insert anything.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                flush(&mut pending, store.as_ref());
            }
            msg = rx.recv() => match msg {
                None => {
                    // Sender side dropped; the cache is gone. Final
                    // best-effort flush and exit so we do not leak
                    // the task.
                    flush(&mut pending, store.as_ref());
                    return;
                }
                Some(CodeMsg::Persist(hash, code)) => {
                    pending.push((hash, code));
                }
                Some(CodeMsg::Flush(notify)) => {
                    flush(&mut pending, store.as_ref());
                    // If the caller has already dropped the receiver
                    // we still consider the flush complete; the IO
                    // has happened either way.
                    let _ = notify.send(());
                }
            }
        }
    }
}

/// Move `pending` into a fresh `Vec`, hand the batch to the store,
/// and leave `pending` empty for the next cycle.
#[cfg(not(target_arch = "wasm32"))]
fn flush(pending: &mut Vec<(B256, Bytes)>, store: &dyn CodeStore) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    store.persist(&batch);
}

/// Detect EIP-7702 delegation designators. A delegation designator
/// is exactly 23 bytes: the [`EIP7702_DELEGATION_DESIGNATOR`] magic
/// prefix `0xef0100` followed by the 20-byte delegation target
/// address.
///
/// We keep designators in the in-memory cache (revm needs them when
/// calling a delegated EOA) but skip persisting them: they are
/// short, so the disk-cache win is negligible, and writing them out
/// would leak which delegation targets the user has touched.
///
/// [`EIP7702_DELEGATION_DESIGNATOR`]: alloy::eips::eip7702::constants::EIP7702_DELEGATION_DESIGNATOR
#[cfg(not(target_arch = "wasm32"))]
fn is_delegation_designator(code: &Bytes) -> bool {
    use alloy::eips::eip7702::constants::EIP7702_DELEGATION_DESIGNATOR;
    const DESIGNATOR_LEN: usize = EIP7702_DELEGATION_DESIGNATOR.len() + 20;
    code.len() == DESIGNATOR_LEN && code.as_ref().starts_with(&EIP7702_DELEGATION_DESIGNATOR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256, U256};

    impl Cache {
        fn get_storage(&self, address: Address, slot: B256, block_hash: B256) -> Option<U256> {
            let storage_hash = {
                let mut accounts = self.accounts.write().unwrap_or_else(|e| e.into_inner());
                let account = accounts.get(&(address, block_hash))?;
                account.storage_hash
            };

            let mut storage = self.storage.write().unwrap_or_else(|e| e.into_inner());
            let storage_map = storage.get(&storage_hash)?;
            let proof = storage_map.get(&slot)?;

            Some(proof.value)
        }
    }

    fn mock_account_proof_response(
        address: Address,
        storage_hash: B256,
        code_hash: B256,
        storage_proofs: Vec<EIP1186StorageProof>,
    ) -> EIP1186AccountProofResponse {
        EIP1186AccountProofResponse {
            address,
            balance: U256::from(1000),
            code_hash,
            nonce: 1,
            storage_hash,
            account_proof: vec![],
            storage_proof: storage_proofs,
        }
    }

    fn mock_storage_proof(slot: B256, value: U256) -> EIP1186StorageProof {
        EIP1186StorageProof {
            key: slot.into(),
            value,
            proof: vec![],
        }
    }

    #[test]
    fn test_insert_and_get_storage() {
        let cache = Cache::new();

        let address = address!("0000000000000000000000000000000000000001");
        let block_hash = b256!("0000000000000000000000000000000000000000000000000000000000000001");
        let storage_hash =
            b256!("0000000000000000000000000000000000000000000000000000000000000002");
        let code_hash = b256!("0000000000000000000000000000000000000000000000000000000000000003");
        let slot = b256!("0000000000000000000000000000000000000000000000000000000000000004");
        let value = U256::from(42);

        let response = mock_account_proof_response(
            address,
            storage_hash,
            code_hash,
            vec![mock_storage_proof(slot, value)],
        );

        cache.insert(response, None, block_hash);

        // Should be able to retrieve storage
        assert_eq!(cache.get_storage(address, slot, block_hash), Some(value));

        // Non-existent slot should return None
        let other_slot = b256!("0000000000000000000000000000000000000000000000000000000000000005");
        assert_eq!(cache.get_storage(address, other_slot, block_hash), None);
    }

    #[test]
    fn test_content_addressed_storage_sharing() {
        let cache = Cache::new();

        let address1 = address!("0000000000000000000000000000000000000001");
        let address2 = address!("0000000000000000000000000000000000000002");
        let block_hash1 = b256!("0000000000000000000000000000000000000000000000000000000000000001");
        let block_hash2 = b256!("0000000000000000000000000000000000000000000000000000000000000002");
        let storage_hash =
            b256!("0000000000000000000000000000000000000000000000000000000000000003"); // Same!
        let code_hash = b256!("0000000000000000000000000000000000000000000000000000000000000004");
        let slot = b256!("0000000000000000000000000000000000000000000000000000000000000005");
        let value = U256::from(100);

        // Insert for address1 at block1 with storage
        let response1 = mock_account_proof_response(
            address1,
            storage_hash,
            code_hash,
            vec![mock_storage_proof(slot, value)],
        );
        cache.insert(response1, None, block_hash1);

        // Insert for address2 at block2 with SAME storage_hash but NO storage proofs
        let response2 = mock_account_proof_response(address2, storage_hash, code_hash, vec![]);
        cache.insert(response2, None, block_hash2);

        // address2 should be able to get the storage value because storage_hash matches!
        assert_eq!(cache.get_storage(address2, slot, block_hash2), Some(value));
    }

    #[test]
    fn test_get_account_proof_with_missing_slots() {
        let cache = Cache::new();

        let address = address!("0000000000000000000000000000000000000001");
        let block_hash = b256!("0000000000000000000000000000000000000000000000000000000000000001");
        let storage_hash =
            b256!("0000000000000000000000000000000000000000000000000000000000000002");
        let code_hash = b256!("0000000000000000000000000000000000000000000000000000000000000003");
        let slot1 = b256!("0000000000000000000000000000000000000000000000000000000000000004");
        let slot2 = b256!("0000000000000000000000000000000000000000000000000000000000000005");
        let slot3 = b256!("0000000000000000000000000000000000000000000000000000000000000006");

        // Insert with only slot1 cached
        let response = mock_account_proof_response(
            address,
            storage_hash,
            code_hash,
            vec![mock_storage_proof(slot1, U256::from(1))],
        );
        cache.insert(response, None, block_hash);

        // Request slot1, slot2, slot3
        let (result, missing) = cache
            .get_account_proof(address, &[slot1, slot2, slot3], block_hash)
            .unwrap();

        // Should have slot1 in response
        assert_eq!(result.storage_proof.len(), 1);
        assert_eq!(result.storage_proof[0].key.as_b256(), slot1);

        // Should report slot2 and slot3 as missing
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&slot2));
        assert!(missing.contains(&slot3));
    }

    #[test]
    fn test_code_caching() {
        let cache = Cache::new();

        let address = address!("0000000000000000000000000000000000000001");
        let block_hash = b256!("0000000000000000000000000000000000000000000000000000000000000001");
        let storage_hash =
            b256!("0000000000000000000000000000000000000000000000000000000000000002");
        let code_hash = b256!("0000000000000000000000000000000000000000000000000000000000000003");
        let code = Bytes::from_static(&[0x60, 0x80, 0x60, 0x40]);

        let response = mock_account_proof_response(address, storage_hash, code_hash, vec![]);
        cache.insert(response, Some(code.clone()), block_hash);

        // Should be able to retrieve code
        assert_eq!(cache.get_code(code_hash), Some(code.clone()));

        // get_account should include the code
        let (account, _) = cache.get_account(address, &[], block_hash).unwrap();
        assert_eq!(account.code, Some(code));
    }

    #[test]
    fn test_get_code_optimistically() {
        let cache = Cache::new();

        let address = address!("0000000000000000000000000000000000000001");
        let other_address = address!("0000000000000000000000000000000000000002");
        let block_hash1 = b256!("0000000000000000000000000000000000000000000000000000000000000001");
        let block_hash2 = b256!("0000000000000000000000000000000000000000000000000000000000000002");
        let storage_hash =
            b256!("0000000000000000000000000000000000000000000000000000000000000003");
        let code_hash = b256!("0000000000000000000000000000000000000000000000000000000000000004");
        let code = Bytes::from_static(&[0x60, 0x80, 0x60, 0x40]);

        let response = mock_account_proof_response(address, storage_hash, code_hash, vec![]);
        cache.insert(response, Some(code.clone()), block_hash1);

        // Same address at another block keeps the optimistic lookup valid.
        let response2 = mock_account_proof_response(address, storage_hash, code_hash, vec![]);
        cache.insert(response2, None, block_hash2);

        assert_eq!(
            cache.get_code_optimistically(address),
            Some((code_hash, code))
        );
        assert_eq!(cache.get_code_optimistically(other_address), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod code_store {
        use super::*;
        use std::sync::Mutex;

        /// Test double for `CodeStore`. Records the entries persisted
        /// so we can assert the cache routed them through correctly.
        struct MockStore {
            preload: Vec<(B256, Bytes)>,
            persisted: Arc<Mutex<Vec<(B256, Bytes)>>>,
        }

        impl helios_common::code_store::CodeStore for MockStore {
            fn load_all(&self) -> Vec<(B256, Bytes)> {
                self.preload.clone()
            }
            fn persist(&self, entries: &[(B256, Bytes)]) {
                self.persisted.lock().unwrap().extend_from_slice(entries);
            }
        }

        fn insert_code(cache: &Cache, code_hash: B256, code: Bytes) {
            let address = address!("0000000000000000000000000000000000000099");
            let storage_hash =
                b256!("00000000000000000000000000000000000000000000000000000000000000aa");
            let block_hash =
                b256!("00000000000000000000000000000000000000000000000000000000000000bb");
            let response = mock_account_proof_response(address, storage_hash, code_hash, vec![]);
            cache.insert(response, Some(code), block_hash);
        }

        #[tokio::test(flavor = "current_thread")]
        async fn warms_lru_from_store_on_construct() {
            let preload_hash =
                b256!("0000000000000000000000000000000000000000000000000000000000000010");
            let preload_code = Bytes::from_static(b"preloaded bytecode");

            let store = Arc::new(MockStore {
                preload: vec![(preload_hash, preload_code.clone())],
                persisted: Arc::new(Mutex::new(Vec::new())),
            });

            let cache = Cache::with_code_store(store, Duration::from_secs(30));

            assert_eq!(cache.get_code(preload_hash), Some(preload_code));
        }

        #[tokio::test(flavor = "current_thread")]
        async fn periodic_flush_persists_inserts() {
            let persisted = Arc::new(Mutex::new(Vec::new()));
            let store = Arc::new(MockStore {
                preload: vec![],
                persisted: persisted.clone(),
            });

            // Short flush interval so the test does not wait long.
            let cache = Cache::with_code_store(store, Duration::from_millis(25));

            let code_hash =
                b256!("0000000000000000000000000000000000000000000000000000000000000020");
            let code = Bytes::from_static(b"freshly fetched bytecode");
            insert_code(&cache, code_hash, code.clone());

            // The in-memory LRU is populated immediately; persistence
            // is decoupled and only happens on the next tick.
            assert_eq!(cache.get_code(code_hash), Some(code.clone()));
            assert!(persisted.lock().unwrap().is_empty());

            // Poll for up to 1 s for the persisted batch to arrive.
            // Real time is fine here: the worker fires roughly every
            // 25 ms.
            let start = std::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if !persisted.lock().unwrap().is_empty() {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(1),
                    "worker did not flush within 1 s"
                );
            }

            assert_eq!(*persisted.lock().unwrap(), vec![(code_hash, code)]);
        }

        #[tokio::test(flavor = "current_thread")]
        async fn eip7702_designators_skipped_for_persistence() {
            let persisted = Arc::new(Mutex::new(Vec::new()));
            let store = Arc::new(MockStore {
                preload: vec![],
                persisted: persisted.clone(),
            });

            let cache = Cache::with_code_store(store, Duration::from_millis(25));

            // Build a delegation designator using alloy's canonical
            // magic-prefix constant so this test follows the spec
            // (and breaks loudly if upstream ever moves the prefix).
            use alloy::eips::eip7702::constants::EIP7702_DELEGATION_DESIGNATOR;
            let target = [0x42u8; 20];
            let mut designator = EIP7702_DELEGATION_DESIGNATOR.to_vec();
            designator.extend_from_slice(&target);
            assert_eq!(designator.len(), 23);
            let designator_bytes = Bytes::from(designator);
            let designator_hash =
                b256!("0000000000000000000000000000000000000000000000000000000000000050");

            // And a real contract for the positive control: the
            // realistic blob still gets persisted, while the
            // designator does not.
            let contract_hash =
                b256!("0000000000000000000000000000000000000000000000000000000000000051");
            let contract_code = Bytes::from_static(b"\x60\x80\x60\x40real contract");

            insert_code(&cache, designator_hash, designator_bytes.clone());
            insert_code(&cache, contract_hash, contract_code.clone());

            // Designator must still be retrievable in memory: revm
            // needs it when calling a delegated EOA.
            assert_eq!(cache.get_code(designator_hash), Some(designator_bytes));

            // Wait up to 1 s for the real contract to land on disk;
            // by the time it does, the designator should still not
            // have been persisted.
            let start = std::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if persisted
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(h, _)| *h == contract_hash)
                {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(1),
                    "worker did not flush within 1 s"
                );
            }

            let snapshot = persisted.lock().unwrap().clone();
            assert!(
                snapshot.iter().all(|(h, _)| *h != designator_hash),
                "delegation designator must not be persisted, got: {snapshot:?}"
            );
            assert!(
                snapshot
                    .iter()
                    .any(|(h, b)| *h == contract_hash && b == &contract_code),
                "real contract code must be persisted, got: {snapshot:?}"
            );
        }

        #[tokio::test(flavor = "current_thread")]
        async fn flush_code_store_drains_synchronously() {
            let persisted = Arc::new(Mutex::new(Vec::new()));
            let store = Arc::new(MockStore {
                preload: vec![],
                persisted: persisted.clone(),
            });

            // Long interval so the test could not flush via the
            // ticker on its own.
            let cache = Cache::with_code_store(store, Duration::from_secs(3600));

            let code_hash =
                b256!("0000000000000000000000000000000000000000000000000000000000000030");
            let code = Bytes::from_static(b"awaiting flush");
            insert_code(&cache, code_hash, code.clone());

            assert!(persisted.lock().unwrap().is_empty());

            cache.flush_code_store().await;

            assert_eq!(*persisted.lock().unwrap(), vec![(code_hash, code)]);
        }
    }
}
