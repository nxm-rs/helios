//! Filesystem-backed [`CodeStore`] for the EVM code sub-cache.
//!
//! Each persisted blob lives at `<dir>/<codehash_hex>.bin`, written
//! through a tmp file and renamed for atomicity. Writes are batched
//! by the cache's background flush worker, so this module only sees
//! IO outside the hot read path.
//!
//! ## Size cap
//!
//! Bytecode is bounded above by EIP-170's 24 KiB ceiling and is
//! typically a few KiB. [`DEFAULT_FILE_CODE_STORE_ENTRIES`] is set
//! so that worst-case disk usage stays within a budget that makes
//! sense for mobile and embedded targets (a few MiB). When [`persist`]
//! would push the on-disk count over the cap, the oldest entries by
//! mtime are deleted first.
//!
//! [`persist`]: CodeStore::persist
//! [`CodeStore`]: helios_common::code_store::CodeStore

use std::{
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use alloy::primitives::{hex, Bytes, B256};
use tracing::warn;

use helios_common::code_store::CodeStore;

/// Default upper bound on persisted entries. At the EIP-170 24 KiB
/// ceiling that caps disk usage at roughly 12 MiB; typical bytecode
/// is smaller, so real usage tends to sit at 2-3 MiB.
///
/// Chosen as 2x the in-memory LRU capacity (`CODE_CACHE_SIZE` in
/// `helios-core`) so a session that exceeds the runtime working set
/// keeps a useful warm pool for the next cold start.
pub const DEFAULT_FILE_CODE_STORE_ENTRIES: usize = 512;

/// Persists code blobs as flat files in a directory.
///
/// Construct via [`FileCodeStore::new`] for the default cap, or
/// [`FileCodeStore::with_max_entries`] to override it (useful in
/// tests or for embedded targets with a tighter budget).
#[derive(Debug, Clone)]
pub struct FileCodeStore {
    dir: PathBuf,
    max_entries: usize,
}

impl FileCodeStore {
    /// Build a store rooted at `dir`, capped at the default entry count.
    pub fn new(dir: PathBuf) -> Self {
        Self::with_max_entries(dir, DEFAULT_FILE_CODE_STORE_ENTRIES)
    }

    /// Build a store rooted at `dir` with an explicit entry cap.
    pub fn with_max_entries(dir: PathBuf, max_entries: usize) -> Self {
        Self { dir, max_entries }
    }

    fn path_for(&self, code_hash: &B256) -> PathBuf {
        self.dir.join(format!("{}.bin", hex::encode(code_hash)))
    }

    /// Walk the directory and collect (path, mtime, parsed hash)
    /// tuples for every well-formed `.bin` file. Files with unparseable
    /// names or unreadable metadata are skipped.
    fn list_entries(&self) -> Vec<(PathBuf, SystemTime, B256)> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(it) => it,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("bin") {
                continue;
            }
            let Some(hash) = parse_hash_from_path(&path) else {
                continue;
            };
            let mtime = match entry.metadata().and_then(|m| m.modified()) {
                Ok(t) => t,
                Err(e) => {
                    warn!(target: "helios::code_store", "mtime({}) failed: {e}", path.display());
                    continue;
                }
            };
            out.push((path, mtime, hash));
        }
        out
    }

    /// After a write, if the directory exceeds the cap, delete the
    /// oldest files by mtime until at-or-below `max_entries`.
    fn enforce_cap(&self) {
        let mut entries = self.list_entries();
        if entries.len() <= self.max_entries {
            return;
        }
        // Oldest mtime first.
        entries.sort_by_key(|(_, mtime, _)| *mtime);
        let to_remove = entries.len() - self.max_entries;
        for (path, _, _) in entries.into_iter().take(to_remove) {
            if let Err(e) = fs::remove_file(&path) {
                warn!(target: "helios::code_store", "evict {} failed: {e}", path.display());
            }
        }
    }
}

/// Pull a `B256` out of a file path's stem (the bit before `.bin`).
fn parse_hash_from_path(path: &Path) -> Option<B256> {
    let name = path.file_stem()?.to_str()?;
    name.parse::<B256>().ok()
}

impl CodeStore for FileCodeStore {
    fn load_all(&self) -> Vec<(B256, Bytes)> {
        if let Err(e) = fs::create_dir_all(&self.dir) {
            warn!(
                target: "helios::code_store",
                "create_dir({}) failed: {e}",
                self.dir.display()
            );
            return Vec::new();
        }
        // Sort by mtime ascending so the youngest entries are
        // inserted into the cache's LRU last; if the on-disk count
        // exceeds the LRU capacity, only the oldest fall out.
        let mut entries = self.list_entries();
        entries.sort_by_key(|(_, mtime, _)| *mtime);

        let mut out = Vec::with_capacity(entries.len());
        for (path, _, hash) in entries {
            match fs::read(&path) {
                Ok(bytes) => out.push((hash, Bytes::from(bytes))),
                Err(e) => warn!(
                    target: "helios::code_store",
                    "read {} failed: {e}",
                    path.display()
                ),
            }
        }
        out
    }

    fn persist(&self, entries: &[(B256, Bytes)]) {
        if entries.is_empty() {
            return;
        }
        if let Err(e) = fs::create_dir_all(&self.dir) {
            warn!(
                target: "helios::code_store",
                "create_dir({}) failed: {e}",
                self.dir.display()
            );
            return;
        }
        for (hash, bytes) in entries {
            let path = self.path_for(hash);
            if path.exists() {
                continue;
            }
            // Atomic write: tmp file + rename. Avoids leaving a
            // half-written file behind if the process crashes
            // mid-write.
            let tmp = path.with_extension("bin.tmp");
            match fs::write(&tmp, bytes.as_ref()) {
                Ok(()) => {
                    if let Err(e) = fs::rename(&tmp, &path) {
                        warn!(
                            target: "helios::code_store",
                            "rename {} -> {} failed: {e}",
                            tmp.display(),
                            path.display()
                        );
                        // Best-effort cleanup of the tmp file.
                        let _ = fs::remove_file(&tmp);
                    }
                }
                Err(e) => warn!(
                    target: "helios::code_store",
                    "write {} failed: {e}",
                    tmp.display()
                ),
            }
        }
        self.enforce_cap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileCodeStore::new(dir.path().to_path_buf());

        let h1 = B256::repeat_byte(0x11);
        let b1 = Bytes::from_static(b"first contract bytecode");
        let h2 = B256::repeat_byte(0x22);
        let b2 = Bytes::from_static(b"second contract bytecode");

        store.persist(&[(h1, b1.clone()), (h2, b2.clone())]);

        // A fresh store on the same dir should observe both.
        let reloaded: HashMap<_, _> =
            FileCodeStore::new(dir.path().to_path_buf()).load_all().into_iter().collect();
        assert_eq!(reloaded.len(), 2);
        assert_eq!(reloaded.get(&h1), Some(&b1));
        assert_eq!(reloaded.get(&h2), Some(&b2));
    }

    #[test]
    fn cap_evicts_oldest_by_mtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileCodeStore::with_max_entries(dir.path().to_path_buf(), 2);

        let h1 = B256::repeat_byte(0x01);
        let h2 = B256::repeat_byte(0x02);
        let h3 = B256::repeat_byte(0x03);

        // Sleep between writes so mtimes are strictly ordered. mtime
        // granularity is filesystem-dependent (ext4: ns; tmpfs: ns;
        // some older or networked FS: 1s), so we space generously.
        store.persist(&[(h1, Bytes::from_static(b"a"))]);
        std::thread::sleep(std::time::Duration::from_millis(20));
        store.persist(&[(h2, Bytes::from_static(b"b"))]);
        std::thread::sleep(std::time::Duration::from_millis(20));
        store.persist(&[(h3, Bytes::from_static(b"c"))]);

        let reloaded: HashMap<_, _> =
            FileCodeStore::new(dir.path().to_path_buf()).load_all().into_iter().collect();
        assert_eq!(reloaded.len(), 2);
        assert!(
            !reloaded.contains_key(&h1),
            "oldest entry should have been evicted"
        );
        assert!(reloaded.contains_key(&h2));
        assert!(reloaded.contains_key(&h3));
    }

    #[test]
    fn persist_is_idempotent_for_same_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileCodeStore::new(dir.path().to_path_buf());

        let h = B256::repeat_byte(0xaa);
        store.persist(&[(h, Bytes::from_static(b"original"))]);
        // A second persist of the same hash is a no-op (file
        // already exists), so the original bytes remain.
        store.persist(&[(h, Bytes::from_static(b"replacement"))]);

        let reloaded = FileCodeStore::new(dir.path().to_path_buf()).load_all();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].1.as_ref(), b"original");
    }

    #[test]
    fn load_all_on_missing_dir_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist-yet");
        let store = FileCodeStore::new(path);
        assert!(store.load_all().is_empty());
    }
}
