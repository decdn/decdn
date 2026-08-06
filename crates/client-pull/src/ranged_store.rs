//! [`ClientRangedStore`]: the client-side [`RangedStore`](decdn_bao_range::RangedStore) backend (#1621
//! P2 Task 1) — a `.partial` data file plus a `.partial.obao4` outboard and a
//! persisted `.partial.ranges` present-range record, built on `bao-tree` /
//! `decdn-bao-range` only. No `iroh-blobs` dependency: `client-pull` must
//! stay iroh-blobs-free so the CLI's pull path links no blob store / AWS SDK
//! (#578).
//!
//! This file builds construction plus the read-only query methods
//! (`total_bytes`, `present_ranges`, `missing_ranges`, `read`,
//! `is_complete`). `admit` and `finalize` are stubbed pending P2 Task 2,
//! which will fill them in against the same `data_path` / `present` fields.
//!
//! The present-range record is the load-bearing shortcut that keeps
//! `present_ranges`/`missing_ranges`/`is_complete` O(1): rather than
//! re-deriving presence by re-hashing the `.partial` data against the
//! outboard on every call, [`ClientRangedStore::open`] trusts the record a
//! prior `admit`/`finalize` wrote (Task 2). The record itself is written with
//! a tempfile-plus-rename so a crash mid-write cannot tear it.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bao_tree::io::sync::ReadAt;
use bao_tree::{BaoTree, ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, RangedFuture, RangedStore, RangedStoreError};

/// Client-side [`RangedStore`]: a `.partial` data file, a `.partial.obao4`
/// pre-order outboard, and a persisted `.partial.ranges` present-range
/// record, all for one blob `(root, total_bytes)`.
///
/// `present` and `data_path` are held behind `Arc<Mutex<..>>` so Task 2's
/// `admit`/`finalize` bodies can move clones of them into
/// `tokio::task::spawn_blocking` closures without borrowing `self` across an
/// await point.
pub struct ClientRangedStore {
    /// BLAKE3 content root this store verifies against.
    root: [u8; 32],
    /// Whole-blob length in bytes, fixed at construction.
    total_bytes: u64,
    /// Tree geometry (`total_bytes` at [`IROH_BLOCK_SIZE`] chunk groups).
    tree: BaoTree,
    /// Current data file. `.partial` until `finalize` promotes it (Task 2).
    data_path: Arc<Mutex<PathBuf>>,
    /// Pre-order outboard sidecar (`{stem}.partial.obao4`).
    obao_path: PathBuf,
    /// Present-range record sidecar (`{stem}.partial.ranges`).
    ranges_path: PathBuf,
    /// Chunk ranges verified present, trusted from the record (loaded once at
    /// construction, updated in-memory + persisted by `admit`/`finalize`).
    present: Arc<Mutex<ChunkRanges>>,
}

impl std::fmt::Debug for ClientRangedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRangedStore")
            .field("root", &hex_prefix(&self.root))
            .field("total_bytes", &self.total_bytes)
            .field("obao_path", &self.obao_path)
            .field("ranges_path", &self.ranges_path)
            .finish_non_exhaustive()
    }
}

fn hex_prefix(root: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    root.iter().take(4).fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> RangedStoreError {
    RangedStoreError::Backend(Box::new(e))
}

fn lock_poisoned(what: &str) -> RangedStoreError {
    RangedStoreError::Backend(format!("{what} lock poisoned").into())
}

fn sidecar_paths(dir: &Path, stem: &str) -> (PathBuf, PathBuf, PathBuf) {
    let data = dir.join(format!("{stem}.partial"));
    let obao = dir.join(format!("{stem}.partial.obao4"));
    let ranges = dir.join(format!("{stem}.partial.ranges"));
    (data, obao, ranges)
}

/// Persist `present` to `path` via tempfile-plus-rename (atomic on the same
/// filesystem), so a crash mid-write cannot tear the record — the next
/// `open` reads either the old or the new record, never a partial one.
fn write_ranges_record(path: &Path, present: &ChunkRanges) -> io::Result<()> {
    let boundaries: Vec<u64> = present.boundaries().iter().map(|c| c.0).collect();
    let json = serde_json::to_vec(&boundaries)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    io::Write::write_all(&mut tmp, &json)?;
    tmp.persist(path).map_err(io::Error::other)?;
    Ok(())
}

/// Load a present-range record written by [`write_ranges_record`], folding
/// consecutive boundary pairs `[a, b)` into a fresh [`ChunkRanges`]. An
/// odd-length or otherwise malformed record is corrupt data, not a missing
/// file — surfaced as an `io::Error` rather than silently treated as empty.
fn read_ranges_record(path: &Path) -> io::Result<ChunkRanges> {
    let bytes = std::fs::read(path)?;
    let boundaries: Vec<u64> = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if !boundaries.len().is_multiple_of(2) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "present-range record at {} has an odd boundary count ({})",
                path.display(),
                boundaries.len()
            ),
        ));
    }
    let mut acc = ChunkRanges::empty();
    let mut it = boundaries.into_iter();
    while let Some(a) = it.next() {
        let Some(b) = it.next() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "present-range record at {} is truncated mid-pair",
                    path.display()
                ),
            ));
        };
        if a > b {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "present-range record at {} has a decreasing pair ({a}, {b})",
                    path.display()
                ),
            ));
        }
        acc |= ChunkRanges::from(ChunkNum(a)..ChunkNum(b));
    }
    Ok(acc)
}

impl ClientRangedStore {
    /// Create a brand-new `.partial` + sidecars for `(root, total_bytes)`
    /// under `dir`, keyed by `stem` (typically the hex content hash). The
    /// data file starts empty (Task 2's positioned writes make it sparse),
    /// the outboard file is zero-filled to exactly
    /// `BaoTree::outboard_size()` (the length [`decdn_bao_range::encode_verified_range`]
    /// and its callers require of a pre-order outboard for this blob size),
    /// and the ranges record starts empty.
    ///
    /// # Errors
    ///
    /// Any I/O failure creating the data file, outboard file, or ranges
    /// record.
    pub fn create(dir: &Path, stem: &str, root: [u8; 32], total_bytes: u64) -> io::Result<Self> {
        let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
        let (data_path, obao_path, ranges_path) = sidecar_paths(dir, stem);

        // Empty data file; Task 2's positioned writes make it sparse.
        File::create(&data_path)?;

        // Zero-filled outboard, sized exactly as a pre-order outboard for
        // this blob must be.
        let obao_file = File::create(&obao_path)?;
        obao_file.set_len(tree.outboard_size())?;
        drop(obao_file);

        let present = ChunkRanges::empty();
        write_ranges_record(&ranges_path, &present)?;

        Ok(Self {
            root,
            total_bytes,
            tree,
            data_path: Arc::new(Mutex::new(data_path)),
            obao_path,
            ranges_path,
            present: Arc::new(Mutex::new(present)),
        })
    }

    /// Reopen an existing `.partial` + sidecars for `(root, total_bytes)`
    /// under `dir`/`stem`. Loads `present` from the `.ranges` record in O(1)
    /// — it does NOT re-hash the data file against the outboard; the record
    /// is trusted as written by a prior `admit`/`finalize`.
    ///
    /// # Errors
    ///
    /// Any I/O failure reading the ranges record, including a corrupt
    /// (malformed) record.
    pub fn open(dir: &Path, stem: &str, root: [u8; 32], total_bytes: u64) -> io::Result<Self> {
        let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
        let (data_path, obao_path, ranges_path) = sidecar_paths(dir, stem);
        let present = read_ranges_record(&ranges_path)?;

        Ok(Self {
            root,
            total_bytes,
            tree,
            data_path: Arc::new(Mutex::new(data_path)),
            obao_path,
            ranges_path,
            present: Arc::new(Mutex::new(present)),
        })
    }

    /// The content root this store verifies against.
    #[must_use]
    pub const fn root(&self) -> [u8; 32] {
        self.root
    }

    /// The tree geometry for this blob, for Task 2's `admit`/`finalize`.
    #[must_use]
    pub const fn tree(&self) -> BaoTree {
        self.tree
    }

    /// The outboard sidecar path, for Task 2's `admit`/`finalize`.
    #[must_use]
    pub fn obao_path(&self) -> &Path {
        &self.obao_path
    }

    /// The present-range record path, for Task 2's `admit`/`finalize`.
    #[must_use]
    pub fn ranges_path(&self) -> &Path {
        &self.ranges_path
    }

    /// Shared handle to the current data file path, for Task 2's
    /// `spawn_blocking` closures.
    #[must_use]
    pub fn data_path_handle(&self) -> Arc<Mutex<PathBuf>> {
        Arc::clone(&self.data_path)
    }

    /// Shared handle to the in-memory present set, for Task 2's
    /// `spawn_blocking` closures.
    #[must_use]
    pub fn present_handle(&self) -> Arc<Mutex<ChunkRanges>> {
        Arc::clone(&self.present)
    }

    fn present_snapshot(&self) -> Result<ChunkRanges, RangedStoreError> {
        let guard = self.present.lock().map_err(|_| lock_poisoned("present"))?;
        Ok(guard.clone())
    }
}

impl RangedStore for ClientRangedStore {
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    fn present_ranges(&self) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move { self.present_snapshot() })
    }

    fn missing_ranges(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move {
            let aligned = decdn_bao_range::align_range(byte_offset, byte_len, self.total_bytes)?;
            let present = self.present_snapshot()?;
            Ok(aligned.chunk_ranges().clone() - &present)
        })
    }

    fn admit(&self, _range: AlignedRange, _bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        // TODO(P2 Task 2): verify `bao_bytes` against `self.root` using the
        // outboard at `self.obao_path`, write data + proof into the
        // `.partial` data/outboard files, extend `self.present`, and persist
        // the updated record to `self.ranges_path`.
        Box::pin(async move {
            Err(RangedStoreError::Backend(
                "not yet implemented (P2 Task 2)".into(),
            ))
        })
    }

    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, Bytes> {
        Box::pin(async move {
            let aligned = decdn_bao_range::align_range(byte_offset, byte_len, self.total_bytes)?;
            let present = self.present_snapshot()?;
            if !(aligned.chunk_ranges().clone() - &present).is_empty() {
                return Err(RangedStoreError::Backend(
                    "requested range not present".into(),
                ));
            }

            let read_start = byte_offset;
            let read_len = if byte_len == 0 {
                self.total_bytes.saturating_sub(byte_offset)
            } else {
                byte_len
            };

            let data_path = Arc::clone(&self.data_path);
            tokio::task::spawn_blocking(move || -> Result<Bytes, RangedStoreError> {
                let path = data_path.lock().map_err(|_| lock_poisoned("data_path"))?;
                let file = File::open(&*path).map_err(backend)?;
                let len = usize::try_from(read_len).map_err(backend)?;
                let mut buf = vec![0u8; len];
                if len > 0 {
                    file.read_exact_at(read_start, &mut buf).map_err(backend)?;
                }
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(backend)?
        })
    }

    fn is_complete(&self) -> RangedFuture<'_, bool> {
        Box::pin(async move { Ok(self.missing_ranges(0, 0).await?.is_empty()) })
    }

    fn finalize(&self) -> RangedFuture<'_, ()> {
        // TODO(P2 Task 2): once `is_complete()`, promote the `.partial` data
        // file to its final (non-`.partial`) path under `self.data_path`'s
        // lock and leave the sidecars for cleanup by the caller.
        Box::pin(async move {
            Err(RangedStoreError::Backend(
                "not yet implemented (P2 Task 2)".into(),
            ))
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    use super::*;

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tmp dir")
    }

    // --- record codec round-trip ---

    #[test]
    fn record_round_trip_empty() {
        let dir = tmp_dir();
        let path = dir.path().join("empty.ranges");
        let present = ChunkRanges::empty();
        write_ranges_record(&path, &present).expect("write");
        let loaded = read_ranges_record(&path).expect("read");
        assert_eq!(loaded, present);
    }

    #[test]
    fn record_round_trip_single_range() {
        let dir = tmp_dir();
        let path = dir.path().join("single.ranges");
        let present = ChunkRanges::from(ChunkNum(2)..ChunkNum(9));
        write_ranges_record(&path, &present).expect("write");
        let loaded = read_ranges_record(&path).expect("read");
        assert_eq!(loaded, present);
    }

    #[test]
    fn record_round_trip_disjoint_ranges() {
        let dir = tmp_dir();
        let path = dir.path().join("disjoint.ranges");
        let mut present = ChunkRanges::from(ChunkNum(0)..ChunkNum(3));
        present |= ChunkRanges::from(ChunkNum(10)..ChunkNum(15));
        write_ranges_record(&path, &present).expect("write");
        let loaded = read_ranges_record(&path).expect("read");
        assert_eq!(loaded, present);
    }

    #[test]
    fn record_rejects_odd_boundary_count() {
        let dir = tmp_dir();
        let path = dir.path().join("odd.ranges");
        std::fs::write(&path, serde_json::to_vec(&vec![1u64, 2, 3]).unwrap()).unwrap();
        let err = read_ranges_record(&path).expect_err("odd count must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // --- store query methods ---

    const GROUP: u64 = decdn_bao_range::CHUNK_GROUP_BYTES;

    fn store_with_present(total_bytes: u64, present: ChunkRanges) -> ClientRangedStore {
        let dir = tmp_dir();
        let store =
            ClientRangedStore::create(dir.path(), "blob", [7u8; 32], total_bytes).expect("create");
        {
            let mut guard = store.present.lock().expect("lock");
            *guard = present;
        }
        // Keep the tempdir alive for the store's lifetime by leaking it —
        // acceptable in a unit test; the OS reclaims it at process exit.
        std::mem::forget(dir);
        store
    }

    fn write_plaintext(store: &ClientRangedStore, data: &[u8]) {
        let path = store.data_path.lock().expect("lock").clone();
        std::fs::write(path, data).expect("write plaintext");
    }

    #[tokio::test]
    async fn total_bytes_reports_construction_value() {
        let store = store_with_present(3 * GROUP, ChunkRanges::empty());
        assert_eq!(store.total_bytes(), 3 * GROUP);
    }

    #[tokio::test]
    async fn present_ranges_reflects_hand_set_value() {
        let present = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let store = store_with_present(3 * GROUP, present.clone());
        let got = store.present_ranges().await.expect("present_ranges");
        assert_eq!(got, present);
    }

    #[tokio::test]
    async fn missing_ranges_interior_and_prefix() {
        let total = 4 * GROUP;
        // First group present, rest missing.
        let present = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let store = store_with_present(total, present);

        // Prefix: entirely within the present group -> nothing missing.
        let missing_prefix = store.missing_ranges(0, 1024).await.expect("missing prefix");
        assert!(missing_prefix.is_empty());

        // Interior spanning into the missing region -> non-empty.
        let missing_interior = store
            .missing_ranges(GROUP, GROUP)
            .await
            .expect("missing interior");
        assert!(!missing_interior.is_empty());
    }

    #[tokio::test]
    async fn is_complete_false_then_true() {
        let total = 2 * GROUP;
        let store = store_with_present(total, ChunkRanges::empty());
        assert!(!store.is_complete().await.expect("is_complete false"));

        {
            let mut guard = store.present.lock().expect("lock");
            *guard = ChunkRanges::from(ChunkNum(0)..ChunkNum::full_chunks(total));
        }
        assert!(store.is_complete().await.expect("is_complete true"));
    }

    #[tokio::test]
    async fn read_byte_exact_of_present_span() {
        let total = 2 * GROUP;
        let present = ChunkRanges::from(ChunkNum(0)..ChunkNum::full_chunks(total));
        let store = store_with_present(total, present);

        let mut data = vec![0u8; usize::try_from(total).expect("fits usize")];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i % 256).expect("i % 256 fits u8");
        }
        write_plaintext(&store, &data);

        let got = store.read(10, 20).await.expect("read");
        assert_eq!(got.as_ref(), &data[10..30]);
    }

    #[tokio::test]
    async fn missing_ranges_oob_is_alignment_error() {
        let total = GROUP;
        let store = store_with_present(total, ChunkRanges::empty());
        let err = store
            .missing_ranges(total, 1)
            .await
            .expect_err("oob must error");
        assert!(matches!(err, RangedStoreError::Alignment(_)));
    }

    #[tokio::test]
    async fn read_oob_is_alignment_error() {
        let total = GROUP;
        let store = store_with_present(total, ChunkRanges::empty());
        let err = store.read(total, 1).await.expect_err("oob must error");
        assert!(matches!(err, RangedStoreError::Alignment(_)));
    }

    #[tokio::test]
    async fn read_absent_span_is_backend_error() {
        let total = 2 * GROUP;
        // Only the second group present; ask for the (absent) first group.
        let present = ChunkRanges::from(ChunkNum::full_chunks(GROUP)..ChunkNum::full_chunks(total));
        let store = store_with_present(total, present);

        let err = store.read(0, 1024).await.expect_err("absent must error");
        assert!(matches!(err, RangedStoreError::Backend(_)));
    }
}
