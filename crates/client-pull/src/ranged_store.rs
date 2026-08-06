//! [`ClientRangedStore`]: the client-side [`RangedStore`](decdn_bao_range::RangedStore) backend (#1621
//! P2 Task 1) — a `.partial` data file plus a `.partial.obao4` outboard and a
//! persisted `.partial.ranges` present-range record, built on `bao-tree` /
//! `decdn-bao-range` only. No `iroh-blobs` dependency: `client-pull` must
//! stay iroh-blobs-free so the CLI's pull path links no blob store / AWS SDK
//! (#578).
//!
//! Construction plus the query methods (`total_bytes`, `present_ranges`,
//! `missing_ranges`, `read`, `is_complete`) came from P2 Task 1. `admit` and
//! `finalize` (P2 Task 2) fill in the write path against the same
//! `data_path` / `present` fields: `admit` verifies an interleaved bao range
//! against the root with `bao_tree::io::sync::decode_ranges` (a positioned,
//! sparse write plus outboard accumulation), and `finalize` runs one
//! `valid_ranges` sweep over the whole blob to either promote the `.partial`
//! file to its final path or surgically shrink `present` to the groups that
//! still verify.
//!
//! The present-range record is the load-bearing shortcut that keeps
//! `present_ranges`/`missing_ranges`/`is_complete` O(1): rather than
//! re-deriving presence by re-hashing the `.partial` data against the
//! outboard on every call, [`ClientRangedStore::open`] trusts the record a
//! prior `admit`/`finalize` wrote. The record itself is written with a
//! tempfile-plus-rename so a crash mid-write cannot tear it.

use std::fs::File;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bao_tree::io::DecodeError;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::{ReadAt, decode_ranges, valid_ranges};
use bao_tree::{BaoTree, ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, RangedFuture, RangedStore, RangedStoreError};

/// Client-side [`RangedStore`]: a `.partial` data file, a `.partial.obao4`
/// pre-order outboard, and a persisted `.partial.ranges` present-range
/// record, all for one blob `(root, total_bytes)`.
///
/// `present` and `data_path` are held behind `Arc<Mutex<..>>` so `admit` and
/// `finalize` can move clones of them into `tokio::task::spawn_blocking`
/// closures without borrowing `self` across an await point.
///
/// Concurrency: a single `ClientRangedStore` expects at most one in-flight
/// write operation at a time — do not call [`RangedStore::admit`] /
/// [`RangedStore::finalize`] concurrently on the same store (nor `admit`
/// concurrently with itself). Concurrent writers can race the present-range
/// record's write-and-rename (the record may under-claim, which is the safe
/// direction — the missing range is simply re-fetched on the next resume)
/// and, worse, an `admit` racing a `finalize` promote is undefined. Reads are
/// safe to interleave. The P3 driver drives one write op per blob at a time.
pub struct ClientRangedStore {
    /// BLAKE3 content root this store verifies against.
    root: [u8; 32],
    /// Whole-blob length in bytes, fixed at construction.
    total_bytes: u64,
    /// Tree geometry (`total_bytes` at [`IROH_BLOCK_SIZE`] chunk groups).
    tree: BaoTree,
    /// Current data file. `.partial` until `finalize` promotes it.
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

/// Split a bao decode failure into "the peer/origin lied" and "the stream
/// ended early", mirroring the taxonomy `sink.rs::classify_decode_error` uses
/// for the streaming path (ADR 038). A `DecodeError` reaching `admit` is
/// always a backend/transport/corruption fault, never an argument error: the
/// caller passes an already-[`AlignedRange`], so there is no bounds question
/// left to ask by the time bytes hit the decoder.
fn classify_decode_fault(err: DecodeError) -> RangedStoreError {
    match err {
        DecodeError::ParentHashMismatch(_) | DecodeError::LeafHashMismatch(_) => {
            RangedStoreError::Backend(format!("bao verification failed: {err}").into())
        }
        DecodeError::Io(io_err) => backend(io_err),
        not_found @ (DecodeError::ParentNotFound(_) | DecodeError::LeafNotFound(_)) => {
            RangedStoreError::Backend(format!("bao stream truncated mid-tree: {not_found}").into())
        }
    }
}

/// What the one [`valid_ranges`] sweep in `finalize` found.
enum FinalizeOutcome {
    /// Every chunk group verified: the `.partial` file was promoted to its
    /// final path.
    Promoted,
    /// At least one chunk group failed to verify; carries the surviving
    /// (still-valid) chunk ranges so the caller can shrink `present` to
    /// exactly them.
    Shrink(ChunkRanges),
}

fn sidecar_paths(dir: &Path, stem: &str) -> (PathBuf, PathBuf, PathBuf) {
    let data = dir.join(format!("{stem}.partial"));
    let obao = dir.join(format!("{stem}.partial.obao4"));
    let ranges = dir.join(format!("{stem}.partial.ranges"));
    (data, obao, ranges)
}

/// Persist `present` to `path` via tempfile-plus-rename (atomic on the same
/// filesystem), fsync'ing the temp file's contents before the rename. This
/// makes the record update both atomic (a crash never leaves a half-written
/// record, so the next `open` reads either the old or the new record, never
/// a partial one) and durable (the bytes are on disk before the rename that
/// makes them visible, so a crash right after the rename cannot leave a
/// torn/zero-length record).
///
/// It does NOT guarantee durability of the rename's directory entry itself —
/// there is no parent-directory `fsync`, so on a non-ordered filesystem a
/// crash can still lose the most recent record update (the rename never
/// became visible at all). That remains the safe direction: a lost update
/// only under-claims presence, and the corresponding data (already `fsync`'d
/// before the record was written) is simply re-listed as missing and
/// re-fetched on resume. Never an over-claim.
fn write_ranges_record(path: &Path, present: &ChunkRanges) -> io::Result<()> {
    let boundaries: Vec<u64> = present.boundaries().iter().map(|c| c.0).collect();
    let json = serde_json::to_vec(&boundaries)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    io::Write::write_all(&mut tmp, &json)?;
    // Durably write the record bytes before the atomic rename: without this,
    // a crash right after `persist`'s rename can leave a zero-length/torn
    // `.ranges` file on a non-ordered filesystem, which then hard-fails JSON
    // parsing in `read_ranges_record` on the next `open`.
    tmp.as_file().sync_all()?;
    tmp.persist(path)
        .map_err(|e| io::Error::new(e.error.kind(), e.error))?;
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
    /// data file starts empty (`admit`'s positioned writes make it sparse),
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

        // Empty data file; `admit`'s positioned writes make it sparse.
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
    /// If `finalize` already promoted this blob (the final, non-`.partial`
    /// file exists), `open` reconstructs a complete store that serves from
    /// the final file instead: it trusts the prior promote rather than
    /// re-hashing, and treats the whole blob as present. This also covers a
    /// crash between `finalize`'s rename and its (best-effort) sidecar
    /// cleanup — any leftover `.obao4`/`.ranges` sidecars are removed here.
    ///
    /// # Errors
    ///
    /// Any I/O failure reading the ranges record, including a corrupt
    /// (malformed) record.
    pub fn open(dir: &Path, stem: &str, root: [u8; 32], total_bytes: u64) -> io::Result<Self> {
        let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
        let (data_path, obao_path, ranges_path) = sidecar_paths(dir, stem);
        let final_path = dir.join(stem);

        if final_path.exists() {
            // Already finalized (or crashed after the rename but before
            // sidecar cleanup): the final file is the complete, verified
            // blob. Best-effort clean up any leftover sidecars and
            // reconstruct a complete store without re-hashing.

            // Cheap sanity check before trusting completeness: a truncated
            // final file (or a wrong caller-supplied `total_bytes`) must not
            // silently claim completeness, since `is_complete()`/
            // `missing_ranges()` would then lie and `read()` would fail
            // later instead. This is a length check only, not a re-hash —
            // the one verify pass already happened at `finalize`.
            let len = std::fs::metadata(&final_path)?.len();
            if len != total_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "finalized blob {stem}: file length {len} != expected total_bytes {total_bytes}"
                    ),
                ));
            }

            let _ = std::fs::remove_file(&obao_path);
            let _ = std::fs::remove_file(&ranges_path);

            let present = ChunkRanges::from(ChunkNum(0)..tree.chunks());
            return Ok(Self {
                root,
                total_bytes,
                tree,
                data_path: Arc::new(Mutex::new(final_path)),
                obao_path,
                ranges_path,
                present: Arc::new(Mutex::new(present)),
            });
        }

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

    /// The tree geometry for this blob, used by `admit`/`finalize`.
    #[must_use]
    pub const fn tree(&self) -> BaoTree {
        self.tree
    }

    /// The outboard sidecar path, used by `admit`/`finalize`.
    #[must_use]
    pub fn obao_path(&self) -> &Path {
        &self.obao_path
    }

    /// The present-range record path, used by `admit`/`finalize`.
    #[must_use]
    pub fn ranges_path(&self) -> &Path {
        &self.ranges_path
    }

    /// Shared handle to the current data file path, for `admit`'s and
    /// `finalize`'s `spawn_blocking` closures.
    #[must_use]
    pub fn data_path_handle(&self) -> Arc<Mutex<PathBuf>> {
        Arc::clone(&self.data_path)
    }

    /// Shared handle to the in-memory present set, for `admit`'s and
    /// `finalize`'s `spawn_blocking` closures.
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

    fn admit(&self, range: AlignedRange, bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            // `bao_bytes` is the combined wire format `encode_verified_range`
            // produces (and the shared conformance suite feeds every
            // backend): an 8-byte little-endian size header followed by the
            // interleaved bao body. `bao_tree::io::sync::decode_ranges` does
            // not parse that header itself (it already knows the tree shape
            // from the outboard) — mirrors the streaming path's
            // `combined[8..]` split in `sink.rs`.
            let body = bao_bytes
                .get(8..)
                .ok_or_else(|| {
                    RangedStoreError::Backend(
                        "admit: bao_bytes shorter than the 8-byte combined-format header".into(),
                    )
                })?
                .to_vec();

            let chunk_ranges = range.chunk_ranges().clone();
            let root = self.root;
            let tree = self.tree;
            let data_path = Arc::clone(&self.data_path);
            let obao_path = self.obao_path.clone();

            tokio::task::spawn_blocking(move || -> Result<(), RangedStoreError> {
                let path = data_path
                    .lock()
                    .map_err(|_| lock_poisoned("data_path"))?
                    .clone();
                let mut data_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(backend)?;
                let mut obao_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&obao_path)
                    .map_err(backend)?;

                // `&mut File` satisfies `WriteAt`/`ReadAt` via positioned-io's
                // blanket `impl<R: ReadAt + ?Sized> ReadAt for &mut R` /
                // `impl<W: WriteAt + ?Sized> WriteAt for &mut W`, so both the
                // decode target and the outboard's backing store can borrow
                // the files rather than consume them — `data_file` and
                // `obao_file` are still ours to `sync_all` once
                // `decode_ranges` returns.
                let mut outboard = PreOrderOutboard {
                    root: bao_tree::blake3::Hash::from(root),
                    tree,
                    data: &mut obao_file,
                };

                // Verifies every chunk group against `root` as it decodes (a
                // tampered range/outboard/root -> `DecodeError`), writes each
                // verified leaf at its true offset (sparse), and saves each
                // parent proof pair into the outboard.
                decode_ranges(
                    Cursor::new(body.as_slice()),
                    chunk_ranges.as_ref(),
                    &mut data_file,
                    &mut outboard,
                )
                .map_err(classify_decode_fault)?;

                data_file.sync_all().map_err(backend)?;
                obao_file.sync_all().map_err(backend)?;
                Ok(())
            })
            .await
            .map_err(backend)??;

            // Union into the in-memory present set, then persist the record.
            // Idempotent: re-admitting an already-present range re-verifies
            // and re-writes the same bytes, and the union is a no-op.
            let updated = {
                let mut guard = self.present.lock().map_err(|_| lock_poisoned("present"))?;
                *guard |= range.chunk_ranges().clone();
                guard.clone()
            };
            let ranges_path = self.ranges_path.clone();
            tokio::task::spawn_blocking(move || write_ranges_record(&ranges_path, &updated))
                .await
                .map_err(backend)?
                .map_err(backend)?;

            Ok(())
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
        Box::pin(async move {
            // Already finalized — e.g. this store was reconstructed by
            // `open()` from the promoted final file, whose sidecars (incl.
            // the `.obao4` the sweep below needs) are gone. Match the same
            // ".partial"-suffix check the promote branch uses below: no
            // `.partial` suffix on the current data path means there is
            // nothing left to sweep or promote.
            let current = {
                let guard = self
                    .data_path
                    .lock()
                    .map_err(|_| lock_poisoned("data_path"))?;
                guard.clone()
            };
            let is_partial = current
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".partial"));
            if !is_partial {
                return Ok(());
            }

            // Fast path: the record doesn't even claim completeness, so
            // there is nothing to promote and no point paying for a sweep.
            if !self.is_complete().await? {
                return Err(RangedStoreError::Incomplete);
            }

            let root = self.root;
            let tree = self.tree;
            let total_bytes = self.total_bytes;
            let obao_path = self.obao_path.clone();
            let ranges_path = self.ranges_path.clone();
            let data_path = Arc::clone(&self.data_path);

            // The one verify pass: recompute valid ranges over the whole
            // blob directly from data + outboard, rather than trusting the
            // (possibly stale, possibly bit-rotted) record. Either every
            // group verifies (promote) or it doesn't (surgically shrink to
            // exactly what still verifies).
            let outcome = tokio::task::spawn_blocking(
                move || -> Result<FinalizeOutcome, RangedStoreError> {
                    let current_path = data_path
                        .lock()
                        .map_err(|_| lock_poisoned("data_path"))?
                        .clone();

                    let obao_file = std::fs::OpenOptions::new()
                        .read(true)
                        .open(&obao_path)
                        .map_err(backend)?;
                    let data_file = std::fs::OpenOptions::new()
                        .read(true)
                        .open(&current_path)
                        .map_err(backend)?;
                    let outboard = PreOrderOutboard {
                        root: bao_tree::blake3::Hash::from(root),
                        tree,
                        data: obao_file,
                    };

                    let full = decdn_bao_range::align_range(0, 0, total_bytes)?;
                    let full_ranges = full.chunk_ranges().clone();

                    let mut valid = ChunkRanges::empty();
                    for r in valid_ranges(outboard, data_file, full_ranges.as_ref()) {
                        valid |= ChunkRanges::from(r.map_err(backend)?);
                    }

                    if (full_ranges.clone() - &valid).is_empty() {
                        // Every chunk group verifies: promote `.partial` to
                        // its final (non-`.partial`) path.
                        let file_name = current_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .ok_or_else(|| {
                                RangedStoreError::Backend("data path has no file name".into())
                            })?;
                        let final_name = file_name.strip_suffix(".partial").ok_or_else(|| {
                            RangedStoreError::Backend("data path is not a .partial file".into())
                        })?;
                        let final_path = current_path.with_file_name(final_name);

                        // Durability before the rename: the promoted file
                        // must contain every byte it claims to.
                        File::open(&current_path)
                            .and_then(|f| f.sync_all())
                            .map_err(backend)?;
                        std::fs::rename(&current_path, &final_path).map_err(backend)?;

                        {
                            let mut guard =
                                data_path.lock().map_err(|_| lock_poisoned("data_path"))?;
                            *guard = final_path;
                        }

                        // Sidecar cleanup is best-effort: a promoted blob is
                        // valid without them, so a delete failure must not
                        // fail a successful promote.
                        let _ = std::fs::remove_file(&obao_path);
                        let _ = std::fs::remove_file(&ranges_path);

                        Ok(FinalizeOutcome::Promoted)
                    } else {
                        Ok(FinalizeOutcome::Shrink(valid))
                    }
                },
            )
            .await
            .map_err(backend)??;

            match outcome {
                FinalizeOutcome::Promoted => Ok(()),
                FinalizeOutcome::Shrink(valid) => {
                    {
                        let mut guard =
                            self.present.lock().map_err(|_| lock_poisoned("present"))?;
                        *guard = valid.clone();
                    }
                    let ranges_path = self.ranges_path.clone();
                    tokio::task::spawn_blocking(move || write_ranges_record(&ranges_path, &valid))
                        .await
                        .map_err(backend)?
                        .map_err(backend)?;
                    // Drops only the bad groups: a subsequent `missing_ranges`
                    // reports exactly them, the caller re-admits just those,
                    // and a second `finalize` promotes.
                    Err(RangedStoreError::Incomplete)
                }
            }
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

    // --- admit / finalize ---

    /// Deterministic blob of `len` bytes plus its bao root and full pre-order
    /// outboard. Mirrors `crates/bao-range/src/conformance.rs::synth_blob` /
    /// `crates/cache/tests/range_pull.rs::make_blob` — an xorshift fill, not
    /// random, so runs are reproducible.
    fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
        let mut plaintext = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut plaintext {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        (root, plaintext, Bytes::from(ob.data))
    }

    /// Interleaved bao (combined format: 8-byte header + body) for `aligned`,
    /// ready to hand to `RangedStore::admit` — exactly what the shared
    /// conformance suite's `bao_for_range` produces.
    fn bao_for(root: [u8; 32], plaintext: &[u8], outboard: Bytes, aligned: &AlignedRange) -> Bytes {
        let s = usize::try_from(aligned.fetch_start()).expect("fits usize");
        let e = usize::try_from(aligned.fetch_end()).expect("fits usize");
        let data = plaintext.get(s..e).expect("aligned span in bounds");
        decdn_bao_range::encode_verified_range(root, aligned, data, outboard).expect("verifies")
    }

    /// Fresh `.partial` store for `(root, total_bytes)`, keeping its tempdir
    /// alive for the test's lifetime the same way `store_with_present` does.
    fn fresh_store(root: [u8; 32], total_bytes: u64) -> ClientRangedStore {
        let dir = tmp_dir();
        let store =
            ClientRangedStore::create(dir.path(), "blob", root, total_bytes).expect("create");
        std::mem::forget(dir);
        store
    }

    #[tokio::test]
    async fn admit_prefix_range_is_readable_and_present() {
        let total = 3 * GROUP;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let store = fresh_store(root, total);

        let aligned = decdn_bao_range::align_range(0, GROUP, total).expect("align");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);

        store
            .admit(aligned.clone(), bao_bytes)
            .await
            .expect("admit prefix");

        let present = store.present_ranges().await.expect("present_ranges");
        assert_eq!(&present, aligned.chunk_ranges());

        let got = store.read(0, GROUP).await.expect("read admitted prefix");
        assert_eq!(
            got.as_ref(),
            plaintext
                .get(..usize::try_from(GROUP).expect("fits"))
                .expect("slice")
        );

        // The `.partial` file holds the bytes at the right offset.
        let on_disk = std::fs::read(store.data_path.lock().expect("lock").clone()).expect("read");
        assert_eq!(
            on_disk.get(..usize::try_from(GROUP).expect("fits")),
            plaintext.get(..usize::try_from(GROUP).expect("fits"))
        );
    }

    #[tokio::test]
    async fn admit_corrupt_payload_is_backend_error_and_presence_unchanged() {
        let total = 2 * GROUP;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let store = fresh_store(root, total);

        let aligned = decdn_bao_range::align_range(0, GROUP, total).expect("align");
        let mut bao_bytes = bao_for(root, &plaintext, outboard, &aligned).to_vec();
        // Flip a byte well past the 8-byte header, inside the interleaved
        // proof+data body, so the corruption lands in bao-verified content.
        let flip_at = bao_bytes.len() - 1;
        if let Some(b) = bao_bytes.get_mut(flip_at) {
            *b ^= 0xFF;
        }

        let err = store
            .admit(aligned, Bytes::from(bao_bytes))
            .await
            .expect_err("corrupt payload must fail");
        assert!(matches!(err, RangedStoreError::Backend(_)));

        let present = store.present_ranges().await.expect("present_ranges");
        assert!(present.is_empty());
    }

    #[tokio::test]
    async fn finalize_promotes_when_every_group_admitted() {
        let total = 2 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let store = fresh_store(root, total);

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");

        store.finalize().await.expect("finalize promotes");

        let final_path = store.data_path.lock().expect("lock").clone();
        assert!(!final_path.to_string_lossy().ends_with(".partial"));
        let on_disk = std::fs::read(&final_path).expect("read final blob");
        assert_eq!(on_disk, plaintext);

        assert!(!store.obao_path.exists());
        assert!(!store.ranges_path.exists());

        // Post-finalize `read` still works through the moved `data_path`.
        let got = store.read(0, 0).await.expect("read post-finalize");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    #[tokio::test]
    async fn finalize_on_incomplete_store_is_incomplete_error() {
        let total = 2 * GROUP;
        let (root, _plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let store = fresh_store(root, total);

        let err = store.finalize().await.expect_err("incomplete must error");
        assert!(matches!(err, RangedStoreError::Incomplete));
    }

    #[tokio::test]
    async fn finalize_shrinks_to_valid_set_on_post_admit_corruption() {
        let total = 2 * GROUP;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let store = fresh_store(root, total);

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");

        // Corrupt one group directly in the `.partial` data file, bypassing
        // `admit` entirely — this is the "bit rot after admit" scenario
        // `finalize`'s verify sweep exists to catch.
        let data_path = store.data_path.lock().expect("lock").clone();
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&data_path)
                .expect("open data file");
            f.seek(SeekFrom::Start(0)).expect("seek");
            f.write_all(&[0xFFu8; 8]).expect("corrupt first group");
        }

        let err = store.finalize().await.expect_err("corruption must fail");
        assert!(matches!(err, RangedStoreError::Incomplete));

        // The first group's chunks are exactly what's now missing; the
        // second group is untouched and still present.
        let missing_first = store
            .missing_ranges(0, GROUP)
            .await
            .expect("missing_ranges first group");
        assert!(!missing_first.is_empty());

        let missing_second = store
            .missing_ranges(GROUP, GROUP)
            .await
            .expect("missing_ranges second group");
        assert!(missing_second.is_empty());

        // A second finalize, after re-admitting only the bad group, promotes.
        let aligned_first =
            decdn_bao_range::align_range(0, GROUP, total).expect("align first group");
        let outboard_for_reupload =
            Bytes::from(std::fs::read(store.obao_path.clone()).expect("read obao"));
        let bao_bytes = bao_for(root, &plaintext, outboard_for_reupload, &aligned_first);
        store
            .admit(aligned_first, bao_bytes)
            .await
            .expect("re-admit corrupted group");
        store.finalize().await.expect("second finalize promotes");
    }

    #[tokio::test]
    async fn open_after_finalize_reconstructs_complete_store() {
        let total = 2 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");
        store.finalize().await.expect("finalize promotes");
        drop(store);

        let reopened =
            ClientRangedStore::open(dir.path(), "blob", root, total).expect("open finalized");

        assert!(
            reopened.is_complete().await.expect("is_complete"),
            "reopened store must be complete"
        );
        let got = reopened.read(0, total).await.expect("read");
        assert_eq!(got.as_ref(), plaintext.as_slice());
        let missing = reopened.missing_ranges(0, 0).await.expect("missing_ranges");
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn open_recovers_from_crash_between_rename_and_sidecar_delete() {
        let total = 2 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");

        // Simulate the crash window: rename `.partial` -> final WITHOUT
        // deleting the sidecars, mirroring a crash between finalize's
        // rename and its best-effort sidecar cleanup.
        let partial_path = store.data_path.lock().expect("lock").clone();
        let final_path = dir.path().join("blob");
        std::fs::rename(&partial_path, &final_path).expect("simulate promote rename");
        let obao_path = store.obao_path.clone();
        let ranges_path = store.ranges_path.clone();
        assert!(obao_path.exists());
        assert!(ranges_path.exists());
        drop(store);

        let reopened =
            ClientRangedStore::open(dir.path(), "blob", root, total).expect("open post-crash");

        assert!(
            reopened.is_complete().await.expect("is_complete"),
            "reopened store must be complete"
        );
        let got = reopened.read(0, total).await.expect("read");
        assert_eq!(got.as_ref(), plaintext.as_slice());

        assert!(
            !obao_path.exists(),
            "leftover obao sidecar must be cleaned up"
        );
        assert!(
            !ranges_path.exists(),
            "leftover ranges sidecar must be cleaned up"
        );
    }

    #[tokio::test]
    async fn finalize_is_idempotent_on_reopened_finalized_store() {
        let total = 2 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");
        store.finalize().await.expect("finalize promotes");
        drop(store);

        let reopened =
            ClientRangedStore::open(dir.path(), "blob", root, total).expect("open finalized");

        // The `.obao4` sidecar `finalize`'s sweep would need is gone (open()
        // deleted it); a second `finalize` must not try to open it and must
        // instead be a no-op.
        reopened
            .finalize()
            .await
            .expect("finalize on already-finalized store is a no-op Ok");

        let got = reopened.read(0, total).await.expect("read still works");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    #[tokio::test]
    async fn open_rejects_final_file_with_wrong_length() {
        let total = 2 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

        let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
        let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit all");
        store.finalize().await.expect("finalize promotes");
        drop(store);

        // Truncate the promoted final file so its length no longer matches
        // the caller-supplied `total_bytes`.
        let final_path = dir.path().join("blob");
        let truncated_len = final_path
            .metadata()
            .expect("metadata")
            .len()
            .saturating_sub(1);
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&final_path)
            .expect("open final file");
        f.set_len(truncated_len).expect("truncate");
        drop(f);

        let err = ClientRangedStore::open(dir.path(), "blob", root, total)
            .expect_err("truncated final file must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
