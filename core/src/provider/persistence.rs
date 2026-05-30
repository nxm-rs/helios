//! Taint persistence — store the [`MismatchInfo`] behind
//! [`HealthStatus::Tainted`] across process restarts so a wallet that
//! observed a mismatch yesterday still refuses to sign today.
//!
//! Wired by [`super::HeliosProviderBuilder::taint_config`]. At build
//! time the builder loads any persisted mismatch and pre-flips
//! `health()` to [`HealthStatus::Tainted`] before any caller can race
//! a verification call. A single-writer background task subscribes to
//! `health()` and writes Tainted to disk / clears it on
//! acknowledgement.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use helios_common::network_spec::NetworkSpec;
use sha2::{Digest, Sha256};

use crate::provider::error::MismatchInfo;
use crate::provider::event::HealthStatus;
use crate::provider::status::VerificationStatus;

/// Backing store for the verifier's first observed mismatch. The store
/// holds at most one record (the *first* mismatch) — the trust state
/// is binary: tainted or not.
pub trait TaintStore: Send + Sync + 'static {
    /// Read the persisted mismatch, if any.
    fn load(&self) -> io::Result<Option<MismatchInfo>>;
    /// Replace the persisted mismatch with `info`. Implementations must
    /// be atomic from the reader's perspective — a crash mid-write must
    /// not leave a half-written file that `load` reads as `Err` and the
    /// builder silently treats as "no taint" (fail-open).
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
    /// Persisted to a file under `dir`, keyed by `(chain_id, rpc_url)`.
    /// The file name embeds the SHA-256 hash of the key so semantically
    /// distinct RPC URLs that would sanitise to the same string (e.g.
    /// `https://eth.io/rpc` vs `https___eth_io_rpc`) get distinct
    /// files — taint cannot bleed across endpoints.
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

/// File-backed [`TaintStore`] used by [`TaintConfig::DataDir`].
///
/// File name: `helios-taint-{chain_id}-{sha256(chain_id || rpc_url)}.json`.
/// SHA-256 of the key is used (rather than naive char-replacement) so
/// distinct URLs always map to distinct files.
///
/// Writes are atomic: the new content is written to a `.tmp` sibling,
/// fsync'd, then renamed into place. A crash mid-write leaves the old
/// file intact rather than producing a half-written file that the
/// loader treats as `Err` and the builder silently fails open from.
pub struct FileTaintStore {
    path: PathBuf,
}

impl FileTaintStore {
    pub fn new(dir: PathBuf, rpc_url: &str, chain_id: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(chain_id.to_be_bytes());
        hasher.update([0u8]); // separator so chain_id || rpc_url is unambiguous
        hasher.update(rpc_url.as_bytes());
        let digest = hasher.finalize();
        let hex = hex::encode(&digest[..16]); // 128 bits is plenty for collision avoidance
        let path = dir.join(format!("helios-taint-{chain_id}-{hex}.json"));
        Self { path }
    }

    /// Path the store reads from / writes to. Exposed for diagnostics
    /// and tests.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn tmp_path(&self) -> PathBuf {
        let mut p = self.path.clone();
        let mut name = p
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        name.push_str(".tmp");
        p.set_file_name(name);
        p
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
        let tmp = self.tmp_path();
        {
            let mut f = std::fs::File::create(&tmp)?;
            use std::io::Write;
            f.write_all(s.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)
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
/// `Healthy` transition.
///
/// A **single** background worker task owns the store and processes
/// health updates serially. Each `changed()` notification is forwarded
/// over an internal mpsc to the worker, which calls `spawn_blocking`
/// to do the actual file I/O. This serialises concurrent observed
/// transitions so two `save`s or a `save` + `clear` can't race each
/// other on disk. Errors from the store are logged via
/// `tracing::warn!`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn_taint_persistence<N: NetworkSpec>(
    status: &VerificationStatus<N>,
    store: Arc<dyn TaintStore>,
) {
    let mut health_rx = status.health();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<HealthStatus>(8);

    // Watcher: every change observed on health() is enqueued on tx.
    tokio::spawn(async move {
        while health_rx.changed().await.is_ok() {
            let current = health_rx.borrow().clone();
            if tx.send(current).await.is_err() {
                break;
            }
        }
    });

    // Worker: serialises store writes. Holding the store on this single
    // task means save/clear never race against each other regardless of
    // how fast health() bounces.
    tokio::spawn(async move {
        while let Some(state) = rx.recv().await {
            let store = store.clone();
            let _ = tokio::task::spawn_blocking(move || match state {
                HealthStatus::Tainted { first_mismatch } => {
                    if let Err(e) = store.save(&first_mismatch) {
                        tracing::warn!(error = %e, "taint store save failed");
                    }
                }
                HealthStatus::Healthy => {
                    if let Err(e) = store.clear() {
                        tracing::warn!(error = %e, "taint store clear failed");
                    }
                }
                HealthStatus::Stalled => {}
            })
            .await;
        }
    });
}

// wasm32 has no blocking thread pool; file-based persistence isn't
// applicable. Embedders that need wasm persistence should plumb in
// an async store via a different mechanism.
#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn_taint_persistence<N: NetworkSpec>(
    _status: &VerificationStatus<N>,
    _store: Arc<dyn TaintStore>,
) {
}
