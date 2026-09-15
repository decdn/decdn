//! The local, advisory `.decdn-manifest.json` skip-cache `decdn bundle pull`
//! writes into an output directory. It indexes every file the bundle(s) have
//! placed there — keyed by bundle-relative POSIX path, merged across runs — so a
//! later pull skips unchanged files without re-hashing and splices unchanged
//! byte ranges from bytes already on disk.
//!
//! The cache is advisory, not authoritative. A skip takes one of two paths: the
//! re-hash path confirms the on-disk bytes against the new manifest hash before
//! skipping; the fast path skips without re-hashing when the saved record's hash
//! agrees and the file's size and mtime are unchanged since that record was
//! written. A missing, corrupt, unreadable, or superseded record costs only a
//! re-hash, and `--overwrite` (or deleting the file) forces a full re-fetch. The
//! one residual trust is the fast path: a file edited in place without changing
//! its size or mtime is not re-hashed. Every spliced donor chunk is always
//! re-hashed before its bytes are used, so donor reuse never risks correctness.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::UNIX_EPOCH;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

/// File name of the local skip-cache, written at the pull output root.
pub(crate) const SAVED_MANIFEST_NAME: &str = ".decdn-manifest.json";

/// Current on-disk schema version. A file with any other version loads empty.
const SAVED_MANIFEST_VERSION: u32 = 1;

/// The local skip-cache: an index of the bundle files known to live under one
/// output root, keyed by bundle-relative POSIX path. Ordered (`BTreeMap`) so the
/// serialized form is deterministic across runs.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SavedManifest {
    /// On-disk schema version; see [`SAVED_MANIFEST_VERSION`].
    version: u32,
    /// Recorded files, keyed by bundle-relative POSIX path.
    files: BTreeMap<String, SavedFile>,
}

impl SavedManifest {
    /// The recorded state for `path`, if any.
    pub(crate) fn get(&self, path: &str) -> Option<&SavedFile> {
        self.files.get(path)
    }
}

/// One recorded file: the whole-file content address and size last materialized
/// at this path, the mtime observed for it (the cheap change gate), and its
/// range-dedup chunks when the source manifest carried them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SavedFile {
    /// The whole-file BLAKE3 content address, `b3:<hex>`.
    pub(crate) hash: String,
    /// The file's byte length.
    pub(crate) size: u64,
    /// The mtime observed for the materialized file.
    pub(crate) mtime: SavedMtime,
    /// Ordered `{hash, size}` range-dedup chunks, when the source manifest
    /// carried them; `None` for a plain (unhinted) entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) chunks: Option<Vec<SavedChunk>>,
}

/// One range-dedup chunk of a [`SavedFile`]: a BLAKE3 over a byte range and its
/// length. Chunk sizes sum to the file's `size`; offsets are the running sum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SavedChunk {
    /// The chunk's BLAKE3 content address, `b3:<hex>`.
    pub(crate) hash: String,
    /// The chunk's length in bytes.
    pub(crate) size: u64,
}

/// A file's modification time as whole seconds and sub-second nanos since the
/// Unix epoch — the cheap gate the skip-cache compares before a re-hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SavedMtime {
    /// Whole seconds since the Unix epoch.
    pub(crate) secs: u64,
    /// Sub-second nanoseconds.
    pub(crate) nanos: u32,
}

impl SavedMtime {
    /// The mtime from `meta`, or `None` when it is unavailable or before the
    /// Unix epoch (either forces a re-hash rather than a false fast-skip).
    pub(crate) fn of(meta: &std::fs::Metadata) -> Option<SavedMtime> {
        let d = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(SavedMtime {
            secs: d.as_secs(),
            nanos: d.subsec_nanos(),
        })
    }
}

/// Place a saved file's chunks at absolute offsets: `(hash, offset, len)` in
/// content order. `None` when the file has no chunks or the sizes do not sum to
/// its whole-file `size` (a malformed record — ignored, never fatal).
pub(crate) fn saved_hints(rec: &SavedFile) -> Option<Vec<(String, u64, u64)>> {
    let chunks = rec.chunks.as_ref()?;
    let mut out = Vec::with_capacity(chunks.len());
    let mut offset = 0u64;
    for c in chunks {
        out.push((c.hash.clone(), offset, c.size));
        offset = offset.checked_add(c.size)?;
    }
    (offset == rec.size).then_some(out)
}

/// Load the skip-cache at `out_root`. A missing, unreadable, malformed, or
/// wrong-version file yields an empty manifest — the cache is advisory, so its
/// absence only means every path falls to the re-hash gate.
pub(crate) fn load(out_root: &Path) -> SavedManifest {
    let path = out_root.join(SAVED_MANIFEST_NAME);
    let Ok(bytes) = std::fs::read(&path) else {
        return SavedManifest::default();
    };
    match serde_json::from_slice::<SavedManifest>(&bytes) {
        Ok(m) if m.version == SAVED_MANIFEST_VERSION => m,
        _ => SavedManifest::default(),
    }
}

/// Overlay `updates` onto `prior` (insert-or-replace per path, untouched paths
/// kept) and write the result atomically to `out_root`'s skip-cache.
pub(crate) fn merge_and_write(
    out_root: &Path,
    mut prior: SavedManifest,
    updates: BTreeMap<String, SavedFile>,
) -> anyhow::Result<()> {
    prior.version = SAVED_MANIFEST_VERSION;
    for (path, rec) in updates {
        prior.files.insert(path, rec);
    }
    let bytes = serde_json::to_vec_pretty(&prior).context("serialize saved manifest")?;
    let dest = out_root.join(SAVED_MANIFEST_NAME);
    let mut tmp = super::fetch::temp_in_parent(&dest).context("stage saved manifest")?;
    std::io::Write::write_all(tmp.as_file_mut(), &bytes).context("write saved manifest")?;
    tmp.as_file().sync_all().context("sync saved manifest")?;
    tmp.persist(&dest)
        .map_err(|e| e.error)
        .context("persist saved manifest")?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample() -> SavedFile {
        SavedFile {
            hash: "b3:aa".into(),
            size: 10,
            mtime: SavedMtime {
                secs: 1_757_800_000,
                nanos: 5,
            },
            chunks: Some(vec![SavedChunk {
                hash: "b3:bb".into(),
                size: 10,
            }]),
        }
    }

    #[test]
    fn write_then_load_round_trips() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut updates = BTreeMap::new();
        updates.insert("a/b.txt".to_string(), sample());
        merge_and_write(tmp.path(), SavedManifest::default(), updates).expect("write");

        let loaded = load(tmp.path());
        let got = loaded.get("a/b.txt").expect("entry present");
        assert_eq!(got.hash, "b3:aa");
        assert_eq!(got.size, 10);
        assert_eq!(
            got.mtime,
            SavedMtime {
                secs: 1_757_800_000,
                nanos: 5
            }
        );
    }

    #[test]
    fn missing_file_loads_empty() {
        let tmp = tempfile::tempdir().expect("tmp");
        assert!(load(tmp.path()).get("anything").is_none());
    }

    #[test]
    fn corrupt_file_loads_empty_not_error() {
        let tmp = tempfile::tempdir().expect("tmp");
        std::fs::write(tmp.path().join(SAVED_MANIFEST_NAME), b"{not json").expect("write");
        assert!(load(tmp.path()).get("anything").is_none());
    }

    #[test]
    fn merge_overlays_updates_and_keeps_untouched() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut first = BTreeMap::new();
        first.insert("keep.txt".to_string(), sample());
        first.insert("change.txt".to_string(), sample());
        merge_and_write(tmp.path(), SavedManifest::default(), first).expect("write1");

        let prior = load(tmp.path());
        let mut upd = BTreeMap::new();
        let mut changed = sample();
        changed.hash = "b3:cc".into();
        upd.insert("change.txt".to_string(), changed);
        merge_and_write(tmp.path(), prior, upd).expect("write2");

        let loaded = load(tmp.path());
        assert_eq!(loaded.get("keep.txt").expect("kept").hash, "b3:aa");
        assert_eq!(loaded.get("change.txt").expect("changed").hash, "b3:cc");
    }

    #[test]
    fn saved_hints_places_offsets_and_validates_sum() {
        let rec = SavedFile {
            hash: "b3:aa".into(),
            size: 30,
            mtime: SavedMtime { secs: 1, nanos: 0 },
            chunks: Some(vec![
                SavedChunk {
                    hash: "b3:c0".into(),
                    size: 10,
                },
                SavedChunk {
                    hash: "b3:c1".into(),
                    size: 20,
                },
            ]),
        };
        let got = saved_hints(&rec).expect("hints");
        assert_eq!(got, vec![("b3:c0".into(), 0, 10), ("b3:c1".into(), 10, 20)]);
    }

    #[test]
    fn saved_hints_rejects_bad_sum_and_absent_chunks() {
        let mut rec = SavedFile {
            hash: "b3:aa".into(),
            size: 99,
            mtime: SavedMtime { secs: 1, nanos: 0 },
            chunks: Some(vec![SavedChunk {
                hash: "b3:c0".into(),
                size: 10,
            }]),
        };
        assert!(saved_hints(&rec).is_none()); // 10 != 99
        rec.chunks = None;
        assert!(saved_hints(&rec).is_none());
    }
}
