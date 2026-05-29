//! Taint persistence — store the [`MismatchInfo`] behind
//! [`HealthStatus::Tainted`] across process restarts so a wallet that
//! observed a mismatch yesterday still refuses to sign today.
//!
//! Wired by [`super::HeliosProviderBuilder::taint_config`]. At build
//! time the builder loads any persisted mismatch and pre-flips
//! `health()` to [`HealthStatus::Tainted`] before any caller can race
//! a verification call. A background task subscribes to `health()`
//! and writes Tainted to disk / clears it on acknowledgement.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use helios_common::network_spec::NetworkSpec;

use crate::provider::error::MismatchInfo;
use crate::provider::event::HealthStatus;
use crate::provider::status::VerificationStatus;

/// Backing store for the verifier's first observed mismatch. The store
/// holds at most one record (the *first* mismatch) — the trust state
/// is binary: tainted or not.
pub trait TaintStore: Send + Sync + 'static {
    /// Read the persisted mismatch, if any.
    fn load(&self) -> io::Result<Option<MismatchInfo>>;
    /// Replace the persisted mismatch with `info`. Called when the
    /// verifier first observes a mismatch.
    fn save(&self, info: &MismatchInfo) -> io::Result<()>;
    /// Remove the persisted mismatch. Called when the embedder calls
    /// [`super::VerificationStatus::acknowledge_mismatch`].
    fn clear(&self) -> io::Result<()>;
}

/// How [`super::HeliosProviderBuilder`] should persist verifier taint.
///
/// Embedders pick the scope they want for their threat model: a wallet
/// with one fixed RPC URL wants [`Self::DataDir`] keyed by that URL;
/// a CLI tool with ephemeral state wants [`Self::PerSession`].
pub enum TaintConfig {
    /// Taint dies with the process. No persistence is installed; the
    /// builder skips the load step and never spawns the persistence
    /// task. Default.
    PerSession,
    /// Persisted to a file under `dir`, keyed by `(chain_id, rpc_url)`
    /// so multiple RPC endpoints (mainnet, testnet, different providers)
    /// each have their own taint record. The file name is derived from
    /// the key with non-alphanumeric chars replaced by `_`.
    DataDir {
        dir: PathBuf,
        rpc_url: String,
        chain_id: u64,
    },
    /// Bring-your-own [`TaintStore`] implementation. Use this when none
    /// of the built-in variants fit (e.g., keyring-backed storage,
    /// remote KV store).
    Custom(Arc<dyn TaintStore>),
}

impl TaintConfig {
    pub(crate) fn into_store(self) -> Option<Arc<dyn TaintStore>> {
        match self {
            Self::PerSession => None,
            Self::DataDir {
                dir,
                rpc_url,
                chain_id,
            } => Some(Arc::new(FileTaintStore::new(dir, &rpc_url, chain_id))),
            Self::Custom(store) => Some(store),
        }
    }
}

/// File-backed [`TaintStore`] used by [`TaintConfig::DataDir`]. Stores
/// the mismatch as JSON at a path derived from `(chain_id, rpc_url)`.
pub struct FileTaintStore {
    path: PathBuf,
}

impl FileTaintStore {
    pub fn new(dir: PathBuf, rpc_url: &str, chain_id: u64) -> Self {
        let sanitized: String = rpc_url
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let path = dir.join(format!("helios-taint-{chain_id}-{sanitized}.json"));
        Self { path }
    }

    /// Path the store reads from / writes to. Exposed for diagnostics
    /// and tests.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl TaintStore for FileTaintStore {
    fn load(&self) -> io::Result<Option<MismatchInfo>> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) => {
                let info: MismatchInfo = serde_json::from_str(&s)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(info))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn save(&self, info: &MismatchInfo) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let s = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(&self.path, s)
    }

    fn clear(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Subscribe to `health()` and write `Tainted` to the store / clear on
/// `Healthy` transition. File I/O happens on `spawn_blocking` so the
/// runtime isn't blocked. Spawned by [`super::HeliosProviderBuilder`]
/// when a [`TaintConfig`] other than `PerSession` is configured.
pub(crate) fn spawn_taint_persistence<N: NetworkSpec>(
    status: &VerificationStatus<N>,
    store: Arc<dyn TaintStore>,
) {
    let mut health_rx = status.health();
    tokio::spawn(async move {
        while health_rx.changed().await.is_ok() {
            let current = health_rx.borrow().clone();
            let store = store.clone();
            tokio::task::spawn_blocking(move || match current {
                HealthStatus::Tainted { first_mismatch } => {
                    let _ = store.save(&first_mismatch);
                }
                HealthStatus::Healthy => {
                    let _ = store.clear();
                }
                HealthStatus::Stalled { .. } => {}
            });
        }
    });
}
