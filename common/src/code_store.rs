//! Pluggable persistence layer for the EVM code cache.
//!
//! The in-process EVM cache (see `helios_core::execution::cache::Cache`)
//! is purely in-memory by default. A `CodeStore` lets the consumer
//! plug in an external store (typically a filesystem directory) so
//! that the cache survives process restarts. This is most useful for
//! mobile and embedded consumers where every cold start would
//! otherwise re-fetch the same contract bytecode.
//!
//! ## Latency contract
//!
//! Cache reads never touch a `CodeStore`. The store is consulted
//! exactly once at construction (via [`load_all`]) to warm the
//! in-memory LRU, and is written to on a periodic background flush
//! (via [`persist`]). Inserts never block on the store: they push to
//! an unbounded channel that the flush worker drains on a timer.
//!
//! Errors are the implementation's responsibility to log; they must
//! not propagate. Persistence is a best-effort optimization, not a
//! correctness guarantee.
//!
//! [`load_all`]: CodeStore::load_all
//! [`persist`]: CodeStore::persist

use alloy::primitives::{Bytes, B256};

/// Trait for an external code store backing the cache's
/// content-addressed code sub-cache.
///
/// Implementations are typically constructed by the consumer (for
/// example `EthereumClientBuilder` builds a `FileCodeStore` rooted at
/// `<data_dir>/code_cache` when `data_dir` is configured) and handed
/// to the cache via `Cache::with_code_store`.
pub trait CodeStore: Send + Sync + 'static {
    /// Snapshot every persisted entry. Called once at `Cache`
    /// construction to warm the in-memory LRU.
    ///
    /// Sync because it runs from a sync constructor. IO cost is
    /// amortized as a one-time startup hit; the cache never reads
    /// from the store again.
    ///
    /// Entries may be returned in any order. The cache inserts them
    /// into a bounded LRU, so any beyond the in-memory cap will be
    /// evicted on the spot. Implementations that store more entries
    /// than the cache can hold should return their most-recently
    /// written entries last to keep them in memory.
    fn load_all(&self) -> Vec<(B256, Bytes)>;

    /// Persist a batch of newly cached entries.
    ///
    /// Called from the background flush worker on a fixed cadence,
    /// never on the hot path. Implementations should log and swallow
    /// any IO errors. Implementations are also responsible for
    /// enforcing their own size caps; the worker hands over whatever
    /// has been cached since the last flush and never decides what
    /// to evict.
    fn persist(&self, entries: &[(B256, Bytes)]);
}
