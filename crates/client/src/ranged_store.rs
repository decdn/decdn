//! [`ClientRangedStore`]: the client-side [`RangedStore`](decdn_bao_range::RangedStore) backend
//! (#1621) — a `.partial` data file plus a `.partial.obao4` outboard and a
//! persisted `.partial.ranges` present-range record, built on `bao-tree` /
//! `decdn-bao-range` only. No `iroh-blobs` dependency: `decdn-client` must
//! stay iroh-blobs-free so the CLI's pull path links no blob store / AWS SDK
//! (#578).
//!
//! Construction plus the query methods (`total_bytes`, `present_ranges`,
//! `missing_ranges`, `read`, `is_complete`) query the record. `admit`,
//! `ingest_stream` and `finalize` provide the write path against the same
//! `data_path` / `present` fields: `admit` verifies an interleaved bao range
//! against the root with `bao_tree::io::sync::decode_ranges` (a positioned,
//! sparse write plus outboard accumulation), `ingest_stream` does the same
//! for a streamed range in durable checkpoint batches, and `finalize` runs one
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

use anyhow::Context as _;
use bao_tree::io::BaoContentItem;
use bao_tree::io::DecodeError;
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::{OutboardMut, ReadAt, WriteAt, decode_ranges, valid_ranges};
use bao_tree::{BaoTree, ChunkNum, ChunkRanges, TreeNode};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, RangedFuture, RangedStore, RangedStoreError};

use crate::sink::{StashedFault, classify_decode_error};

/// Client-side [`RangedStore`]: a `.partial` data file, a `.partial.obao4`
/// pre-order outboard, and a persisted `.partial.ranges` present-range
/// record, all for one blob `(root, total_bytes)`.
///
/// `present` and `data_path` are held behind `Arc<Mutex<..>>` so `admit`,
/// `finalize` and the `ingest_stream` checkpoint worker can move clones of them into
/// `tokio::task::spawn_blocking` closures without borrowing `self` across an
/// await point.
///
/// Concurrency: a single `ClientRangedStore` expects at most one in-flight
/// write operation at a time — do not call [`RangedStore::admit`] /
/// [`RangedStore::finalize`] concurrently on the same store (nor `admit`
/// concurrently with itself). Concurrent writers can race the present-range
/// record's write-and-rename (the record may under-claim, which is the safe
/// direction — the missing range is simply re-fetched on the next resume)
/// and, worse, an `admit` racing a `finalize` promote is undefined. Reads are
/// safe to interleave. The driver drives one write op per blob at a time.
/// [`ClientRangedStore::ingest_stream`] is the exception: the multi-source
/// scheduler runs it concurrently on disjoint ranges of one store, which is
/// safe because its checkpoints only union into `present` under the mutex and
/// never write the record.
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
    /// construction, updated in memory by `admit`/`ingest_stream`/`finalize`,
    /// persisted by `admit`/`finalize`/`flush_present_record`).
    present: Arc<Mutex<ChunkRanges>>,
    /// Test-only stall injected before each ingest checkpoint fsync, to model
    /// a slow disk.
    #[cfg(test)]
    fsync_delay: std::time::Duration,
    /// Test-only count of the ingest checkpoint fsyncs across every
    /// `ingest_stream` call on this store.
    #[cfg(test)]
    fsyncs: Arc<std::sync::atomic::AtomicU32>,
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
            #[cfg(test)]
            fsync_delay: std::time::Duration::ZERO,
            #[cfg(test)]
            fsyncs: Arc::default(),
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
                #[cfg(test)]
                fsync_delay: std::time::Duration::ZERO,
                #[cfg(test)]
                fsyncs: Arc::default(),
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
            #[cfg(test)]
            fsync_delay: std::time::Duration::ZERO,
            #[cfg(test)]
            fsyncs: Arc::default(),
        })
    }

    /// Whether a `.ranges` record for `stem` exists in `dir`, so that
    /// [`open_or_create`](Self::open_or_create) would resume it rather than
    /// create a fresh store.
    ///
    /// # Errors
    ///
    /// A stat of the record that fails for a reason other than `NotFound`.
    pub fn has_record(dir: &Path, stem: &str) -> io::Result<bool> {
        let (_data_path, _obao_path, ranges_path) = sidecar_paths(dir, stem);
        ranges_path.try_exists()
    }

    /// Reopen an existing `.partial` store for `(root, total_bytes)` if one is
    /// on disk, otherwise [`create`](Self::create) a fresh one. The presence of
    /// the `.partial.ranges` record is the resume signal: `create` writes it,
    /// and `admit`, `finalize` and `flush_present_record` rewrite it
    /// atomically, so a store with a record is resumable and one without is
    /// not.
    ///
    /// Keyed on the `.ranges` record alone, NOT on the promoted final file:
    /// once `finalize` promotes and deletes the sidecars, a re-fetch of the same
    /// stem starts fresh (there is nothing left to resume from) — the same
    /// behaviour the pre-#1608 CLI had, where `finalize`'s rename moved the
    /// `.partial` away and a re-run re-downloaded. A stale non-sidecar `.partial`
    /// (e.g. an old raw-format leftover at the same path) is discarded by
    /// `create`'s `File::create` truncation.
    ///
    /// # Errors
    ///
    /// Any I/O failure from the chosen [`open`](Self::open) / [`create`](Self::create),
    /// or a stat of the `.ranges` record that fails for a reason other than
    /// `NotFound` (the store is then neither opened nor created).
    pub fn open_or_create(
        dir: &Path,
        stem: &str,
        root: [u8; 32],
        total_bytes: u64,
    ) -> io::Result<Self> {
        let (_data_path, _obao_path, ranges_path) = sidecar_paths(dir, stem);
        // `try_exists`, not `exists`: a stat that fails for any reason but
        // `NotFound` must not read as "no record", because `create` truncates the
        // `.partial` and every byte already paid for in it.
        if ranges_path.try_exists()? {
            Self::open(dir, stem, root, total_bytes)
        } else {
            Self::create(dir, stem, root, total_bytes)
        }
    }

    /// Seed the on-disk state a *checkpointed* interrupted download leaves for
    /// the first `prefix_len` bytes (rounded UP to a chunk-group boundary) of
    /// `blob`: the positioned `.partial` prefix, the whole-blob `.obao4`
    /// pre-order outboard, and a `.ranges` record covering the prefix. A
    /// subsequent [`open`](Self::open) / [`open_or_create`](Self::open_or_create)
    /// resumes from it, so a fetch skips the recorded prefix and pulls only the
    /// suffix. `stem` = the output file name (e.g. `"blob.bin"`), so the sidecars
    /// sit beside `dir/<stem>` exactly where a fetch opens them.
    ///
    /// `prefix_len == blob.len() as u64` seeds a COMPLETE checkpointed partial:
    /// the record claims the whole blob, so a resume pulls nothing and
    /// [`finalize`](RangedStore::finalize) promotes for free.
    ///
    /// The whole `PreOrderMemOutboard::create` output is written to `.obao4`
    /// even for a short prefix. Presence is gated by the `.ranges` record, and
    /// `finalize`'s `valid_ranges` sweep only validates groups whose data is
    /// present, so the extra suffix proof pairs are harmless — they are exactly
    /// what a real resumed fetch's outboard would already hold once the suffix
    /// arrives.
    ///
    /// Test/fixture seam only (`#[cfg(any(test, feature = "test-util"))]`): it
    /// writes files directly without going through the verifying `admit` path,
    /// which is precisely what makes it a faithful stand-in for "a prior process
    /// left this checkpoint on disk".
    ///
    /// # Errors
    ///
    /// Any I/O failure writing the data / outboard / ranges sidecars, or an
    /// alignment failure (`prefix_len` past the blob end).
    #[cfg(any(test, feature = "test-util"))]
    pub fn seed_checkpointed_prefix(
        dir: &Path,
        stem: &str,
        blob: &[u8],
        prefix_len: u64,
    ) -> io::Result<()> {
        let total_bytes = u64::try_from(blob.len())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let (data_path, obao_path, ranges_path) = sidecar_paths(dir, stem);

        // Whole-blob pre-order outboard (exactly `BaoTree::outboard_size()` long,
        // the same length `create` zero-fills to).
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
        std::fs::write(&obao_path, &ob.data)?;

        // The chunk-group-aligned prefix `[0, prefix_len)`: its positioned data
        // (a plain write, since it starts at offset 0) and its present record.
        let aligned = decdn_bao_range::align_range(0, prefix_len, total_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let end = usize::try_from(aligned.fetch_end())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let prefix = blob.get(..end).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "aligned prefix end past blob")
        })?;
        std::fs::write(&data_path, prefix)?;

        write_ranges_record(&ranges_path, aligned.chunk_ranges())?;
        Ok(())
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

    #[cfg(test)]
    /// The present-range record path, used by `admit`/`finalize`.
    #[must_use]
    pub(crate) fn ranges_path(&self) -> &Path {
        &self.ranges_path
    }

    fn present_snapshot(&self) -> Result<ChunkRanges, RangedStoreError> {
        let guard = self.present.lock().map_err(|_| lock_poisoned("present"))?;
        Ok(guard.clone())
    }

    /// Flush the verified batch to disk no more than this many received
    /// content bytes apart, during [`Self::ingest_stream`]. An fsync per 16 KiB
    /// chunk group would cost thousands of fsyncs for a large gap; batching
    /// 4 MiB (256 groups) amortizes that cost and lets the decode loop run
    /// ahead of the disk.
    ///
    /// It also bounds the re-pay window. Content received since the last
    /// durable checkpoint is re-pulled and **re-paid** on resume. On a stream
    /// fault the call checkpoints every verified leaf before it returns, so a
    /// fault re-pays only the bytes past the last verified chunk group (unless
    /// a checkpoint itself fails). On a crash, or when the caller drops the
    /// future (a scheduler steal or stall), the unflushed batch is lost and
    /// the queued checkpoints can miss the resume read, so the re-pay is at
    /// most `INGEST_MAX_QUEUED_CHECKPOINTS + 1` intervals. Checkpointed
    /// (durably-recorded) bytes are never re-paid.
    ///
    /// It is a fsync-amortization knob, not a payment one: it sits four payment
    /// quanta (`decdn_protocol::client::CHUNK_BYTES`) wide, so a crash or a
    /// dropped future can re-pay up to `4 * (INGEST_MAX_QUEUED_CHECKPOINTS + 1)`
    /// chunks. Narrowing it toward one chunk would tighten that window at four
    /// times the fsync rate, which is a storage tradeoff rather than a
    /// payment-correctness one — the payer re-pays only what it genuinely
    /// re-pulls either way.
    pub(crate) const INGEST_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

    /// The most checkpoints one [`Self::ingest_stream`] call holds between the
    /// decode loop and durability: queued for the fsync worker, or written by
    /// it and not yet fsynced. The loop hands a full batch to the worker and
    /// keeps decoding. It waits only when this many checkpoints are not yet
    /// durable, so a slow fsync stalls payment only after the loop has run
    /// this many intervals ahead of the disk.
    ///
    /// Three checkpoints let one slow fsync overlap 12 MiB of further decode
    /// and payment. The bound caps both the undurable window (the crash and
    /// dropped-future re-pay bound on `INGEST_CHECKPOINT_BYTES`) and the
    /// memory per call, at `INGEST_MAX_QUEUED_CHECKPOINTS + 1` intervals. It
    /// also caps how many queued checkpoints the worker folds into one fsync,
    /// so a producer that never slows cannot defer every fsync forever.
    pub(crate) const INGEST_MAX_QUEUED_CHECKPOINTS: usize = 3;

    /// Stream the raw bao encoding of `range` (from `reader`) into the store:
    /// verify each chunk group against the root as
    /// [`bao_tree::io::fsm::ResponseDecoder`] decodes it, collect the verified
    /// leaves and parent proof pairs into a batch, and hand each batch of
    /// roughly `INGEST_CHECKPOINT_BYTES` (plus the remainder at completion) to
    /// a checkpoint worker as one durable checkpoint: positioned-write the
    /// leaves into the `.partial` data file, save the parents into the `.obao4`
    /// outboard, fsync both, then union the received prefix into `present`.
    ///
    /// Every file operation runs on a `tokio::task::spawn_blocking` worker,
    /// never on a runtime worker. The paid pull pays from inside this decode
    /// loop, so a runtime worker parked in an fsync would stop voucher sends
    /// on every stream that shares the runtime, and a decode loop that waits
    /// for each fsync would stop its own. The loop therefore queues each full
    /// batch and keeps decoding; it waits only when
    /// `INGEST_MAX_QUEUED_CHECKPOINTS` checkpoints are not yet durable. One
    /// worker at a time takes the checkpoints in order, writes each one, folds
    /// the checkpoints already queued behind it into the same fsync (up to
    /// `INGEST_MAX_QUEUED_CHECKPOINTS` per fsync), and only then extends
    /// `present`. The in-order worker keeps `present` a contiguous prefix and
    /// writes the outboard parents of an earlier batch before a later one. It
    /// runs only while checkpoints are queued. Memory per call is bounded by
    /// `INGEST_MAX_QUEUED_CHECKPOINTS + 1` batches.
    ///
    /// This is the streaming sibling of [`RangedStore::admit`]: `admit` takes
    /// a whole range's bao bytes already assembled in memory, while
    /// `ingest_stream` consumes them incrementally — the gap driver's fetch of
    /// a range too large to buffer whole.
    ///
    /// On success, returns `reader` so the caller can hand it to
    /// `BlobSource::finish` to drain the underlying pull to its stream end and
    /// recover the acked voucher watermark.
    ///
    /// `on_progress`, when set, is called with the CONTENT bytes received so far
    /// on THIS range (`received_end - range.fetch_start()`) after each verified
    /// leaf is received — the byte-progress hook the CLI's delivery bar drives
    /// (the driver offsets it by the already-present base to report whole-blob
    /// progress). It runs in the hot receive loop, so it must not block or panic.
    ///
    /// # Durability contract
    ///
    /// The same fsync-before-record invariant `write_ranges_record`'s
    /// callers already establish: a checkpoint never lets `present` (and
    /// therefore the persisted `.ranges` record) claim bytes that are not yet
    /// durable on disk. On a peer fault the call checkpoints the verified
    /// batch it holds and waits for every queued checkpoint, so it loses no
    /// verified leaf; the next `missing_ranges`/resume call re-fetches AND
    /// re-pays only the bytes past the last verified chunk group (see
    /// `INGEST_CHECKPOINT_BYTES`'s doc for the re-pay-window bound). A crash
    /// loses the unflushed batch and every queued checkpoint, so at most
    /// `INGEST_MAX_QUEUED_CHECKPOINTS + 1` intervals. Dropping the future
    /// loses the unflushed batch and detaches the queued checkpoints rather
    /// than cancelling them: the worker still writes the same verified bytes,
    /// fsyncs, and unions them into `present`, but possibly after the caller's
    /// next `missing_ranges` read, so that span can be re-fetched as well. It
    /// never loses (or re-claims) a byte that a prior checkpoint already made
    /// durable, and it never claims a byte that was not actually fsync'd.
    ///
    /// # Errors
    ///
    /// - The typed [`StashedFault`] the reader parked, if any (a stalled or
    ///   refusing peer) — takes precedence over the decoder's own complaint.
    /// - Otherwise the bao decode failure, classified by
    ///   `sink::classify_decode_error`: [`crate::HashMismatch`] for a
    ///   verification failure, a truncation error for a short stream.
    /// - Any I/O failure opening, writing or fsyncing the `.partial`/`.obao4`
    ///   files, or a checkpoint worker that panics. When the stream itself
    ///   failed, the stashed fault or decode failure outranks a checkpoint
    ///   failure, which is logged instead: it says what the peer did, which
    ///   decides retry and blame, and a local disk error does not make a bad
    ///   or truncated stream good.
    pub async fn ingest_stream<R>(
        &self,
        range: &AlignedRange,
        reader: R,
        on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
    ) -> anyhow::Result<R>
    where
        R: iroh_io::AsyncStreamReader + StashedFault + Send,
    {
        let mut flusher = IngestFlusher::open(self, range).await?;

        let root = bao_tree::blake3::Hash::from(self.root);
        let ranges = range.chunk_ranges().clone();
        let mut decoder = ResponseDecoder::new(root, ranges, self.tree, reader);

        // The contiguous prefix of `range`, in bytes, whose leaves have been
        // received and verified so far (not yet necessarily flushed — see
        // `batch_start` below).
        let mut received_end = range.fetch_start();
        // The prefix already handed to the checkpoint worker. Only the span
        // `[batch_start, received_end)` sits in `batch`.
        let mut batch_start = range.fetch_start();
        let mut batch = FlushBatch::default();

        loop {
            match decoder.next().await {
                ResponseDecoderNext::More((rest, Ok(BaoContentItem::Leaf(leaf)))) => {
                    received_end =
                        received_end
                            .max(leaf.offset.saturating_add(
                                u64::try_from(leaf.data.len()).unwrap_or(u64::MAX),
                            ));
                    batch.leaves.push((leaf.offset, leaf.data));
                    if let Some(cb) = on_progress {
                        cb(received_end.saturating_sub(range.fetch_start()));
                    }
                    if received_end.saturating_sub(batch_start) >= Self::INGEST_CHECKPOINT_BYTES {
                        flusher
                            .start(std::mem::take(&mut batch), received_end)
                            .await?;
                        batch_start = received_end;
                    }
                    decoder = rest;
                }
                ResponseDecoderNext::More((rest, Ok(BaoContentItem::Parent(parent)))) => {
                    batch.parents.push((parent.node, parent.pair));
                    decoder = rest;
                }
                ResponseDecoderNext::More((rest, Err(decode_err))) => {
                    let mut r = rest.finish();
                    // The decoder yields a leaf or parent only once it
                    // verifies, and yields the leaves of the one contiguous
                    // `range` in order, so `batch` holds verified items and
                    // `[fetch_start, received_end)` is a received prefix.
                    // Checkpoint it with the queued ones so the prefix is not
                    // re-paid on resume. The fault is the error that matters
                    // here.
                    if let Err(flush_err) = flusher.finish(batch, received_end).await {
                        warn_flush_failed_on_fault(&flush_err, range, received_end);
                    }
                    if let Some(fault) = r.take_fault() {
                        return Err(fault);
                    }
                    return Err(classify_decode_error(decode_err));
                }
                ResponseDecoderNext::Done(mut r) => {
                    let flush_result = flusher.finish(batch, received_end).await;
                    // A stashed fault outranks a local flush failure: it says
                    // what the peer did, which decides retry and blame.
                    if let Some(fault) = r.take_fault() {
                        if let Err(flush_err) = flush_result {
                            warn_flush_failed_on_fault(&flush_err, range, received_end);
                        }
                        return Err(fault);
                    }
                    flush_result?;
                    return Ok(r);
                }
            }
        }
    }

    /// Snapshot `present` now and return a future that writes that snapshot to
    /// the `.ranges` record on `spawn_blocking`, off the runtime workers. The
    /// snapshot is taken at the call, not when the future is first polled.
    fn write_present_record_blocking(
        &self,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send + 'static {
        let snapshot = self
            .present
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| io::Error::other(lock_poisoned("present")));
        let ranges_path = self.ranges_path.clone();
        async move {
            let snapshot = snapshot?;
            tokio::task::spawn_blocking(move || write_ranges_record(&ranges_path, &snapshot))
                .await
                .map_err(io::Error::other)?
        }
    }

    /// Persist the current in-memory `present` snapshot to the `.ranges`
    /// record. The single-writer flush point (spec §5.5): callers (the
    /// scheduler's flush owner for multi-source fetches, and `finalize`, and
    /// the end of single-source `drive`) invoke this so no two writers race
    /// the record file. `present` only ever grows and is unioned under the
    /// mutex AFTER data/outboard fsync (the `sync_and_union` helper's ordering),
    /// so the persisted record never claims a range that is not durably on
    /// disk.
    ///
    /// # Errors
    ///
    /// The `present` lock is poisoned, or the record's tempfile-plus-rename
    /// write fails.
    pub fn flush_present_record(&self) -> io::Result<()> {
        let snapshot = {
            let guard = self
                .present
                .lock()
                .map_err(|_| io::Error::other(lock_poisoned("present")))?;
            guard.clone()
        };
        write_ranges_record(&self.ranges_path, &snapshot)
    }
}

/// The open `.partial` data file and `.obao4` outboard of one
/// [`ClientRangedStore::ingest_stream`] call, held by its [`IngestPipeline`].
struct IngestFiles {
    data: File,
    outboard: PreOrderOutboard<File>,
}

/// The verified bao items one ingest checkpoint writes: leaves as
/// `(byte offset, data)` and parent proof pairs as `(node, pair)`.
#[derive(Default)]
struct FlushBatch {
    leaves: Vec<(u64, Bytes)>,
    parents: Vec<(TreeNode, (bao_tree::blake3::Hash, bao_tree::blake3::Hash))>,
}

impl FlushBatch {
    const fn is_empty(&self) -> bool {
        self.leaves.is_empty() && self.parents.is_empty()
    }
}

/// One verified batch on its way to durability: it checkpoints the prefix
/// `[range.fetch_start(), received_end)` and holds one of the
/// `INGEST_MAX_QUEUED_CHECKPOINTS` pipeline slots until that prefix is
/// fsynced.
struct Checkpoint {
    batch: FlushBatch,
    received_end: u64,
    slot: tokio::sync::OwnedSemaphorePermit,
}

/// The decode-loop side of one [`ClientRangedStore::ingest_stream`] call's
/// checkpoint pipeline. It queues each full batch on the shared
/// [`IngestPipeline`] and waits only for a free slot, never for an fsync.
///
/// A slot is taken before a checkpoint is queued and freed only once a
/// worker has made it durable, so at most `INGEST_MAX_QUEUED_CHECKPOINTS`
/// checkpoints are queued or unsynced at once.
///
/// Dropping the flusher stops the queueing. A running worker still drains
/// what is queued, fsyncs it and unions it into `present`, then exits
/// detached.
struct IngestFlusher {
    pipeline: Arc<IngestPipeline>,
    slots: Arc<tokio::sync::Semaphore>,
    /// The worker this flusher started last. Only this flusher starts
    /// workers, and a worker exits only once the queue is empty or it has
    /// failed, so this is the one worker that can still run or that failed.
    worker: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl IngestFlusher {
    /// Open `store`'s `.partial` data file and `.obao4` outboard for writing,
    /// on `spawn_blocking`, for the ingest of `range`.
    async fn open(store: &ClientRangedStore, range: &AlignedRange) -> anyhow::Result<Self> {
        let path = store
            .data_path
            .lock()
            .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("data_path")))?
            .clone();
        let obao_path = store.obao_path.clone();
        let root = bao_tree::blake3::Hash::from(store.root);
        let tree = store.tree;
        let files = tokio::task::spawn_blocking(move || -> io::Result<IngestFiles> {
            let data = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            let obao_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&obao_path)?;
            Ok(IngestFiles {
                data,
                outboard: PreOrderOutboard {
                    root,
                    tree,
                    data: obao_file,
                },
            })
        })
        .await??;
        let pipeline = IngestPipeline {
            state: Mutex::new(PipelineState::default()),
            files: Mutex::new(files),
            present: Arc::clone(&store.present),
            total_bytes: store.total_bytes,
            range: range.clone(),
            #[cfg(test)]
            fsync_delay: store.fsync_delay,
            #[cfg(test)]
            fsyncs: Arc::clone(&store.fsyncs),
        };
        Ok(Self {
            pipeline: Arc::new(pipeline),
            slots: Arc::new(tokio::sync::Semaphore::new(
                ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS,
            )),
            worker: None,
        })
    }

    /// Queue `batch` as the checkpoint of the prefix
    /// `[range.fetch_start(), received_end)`, and start a worker if none is
    /// running. Waits only while every slot holds a checkpoint that is not yet
    /// durable.
    ///
    /// # Errors
    ///
    /// The worker's own error (or panic) when a checkpoint failed.
    async fn start(&mut self, batch: FlushBatch, received_end: u64) -> anyhow::Result<()> {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|e| anyhow::anyhow!("ingest checkpoint slots closed: {e}"))?;
        let start_worker = {
            let mut state = self.pipeline.lock_state()?;
            if state.failed {
                None
            } else {
                state.queue.push_back(Checkpoint {
                    batch,
                    received_end,
                    slot,
                });
                Some(!std::mem::replace(&mut state.worker_running, true))
            }
        };
        match start_worker {
            Some(true) => {
                let pipeline = Arc::clone(&self.pipeline);
                self.worker = Some(tokio::task::spawn_blocking(move || pipeline.run()));
                Ok(())
            }
            Some(false) => Ok(()),
            None => match self.join().await {
                Err(e) => Err(e),
                Ok(()) => Err(anyhow::anyhow!("ingest checkpoint worker failed")),
            },
        }
    }

    /// Queue the final `batch`, if it holds anything, and wait until every
    /// checkpoint is durable.
    async fn finish(mut self, batch: FlushBatch, received_end: u64) -> anyhow::Result<()> {
        if !batch.is_empty() {
            self.start(batch, received_end).await?;
        }
        self.join().await
    }

    /// Wait for the last worker to exit and surface its result.
    async fn join(&mut self) -> anyhow::Result<()> {
        match self.worker.take() {
            Some(handle) => handle
                .await
                .map_err(|e| anyhow::anyhow!("ingest checkpoint worker failed: {e}"))?,
            None => Ok(()),
        }
    }
}

/// The checkpoint queue and open files of one
/// [`ClientRangedStore::ingest_stream`] call, shared by its decode loop and
/// its checkpoint worker.
///
/// A worker runs on `spawn_blocking` only while checkpoints are queued. It
/// takes them in order and exits when the queue is empty; the next queued
/// checkpoint starts a new one. The queue and the running flag share one
/// lock, so a checkpoint is never left queued with no worker to take it, and
/// at most one worker runs at a time. That keeps `present` a contiguous
/// prefix of `range` and writes an earlier batch's outboard parents first. A
/// worker that stays parked for the whole stream would instead hold a
/// blocking-pool thread per ingest and stop a paused test clock from
/// advancing.
struct IngestPipeline {
    state: Mutex<PipelineState>,
    /// Locked by the running worker for its whole run; never by the decode
    /// loop.
    files: Mutex<IngestFiles>,
    present: Arc<Mutex<ChunkRanges>>,
    total_bytes: u64,
    range: AlignedRange,
    /// Test-only stall before each fsync, to model a slow disk.
    #[cfg(test)]
    fsync_delay: std::time::Duration,
    /// Test-only count of the fsyncs the workers run.
    #[cfg(test)]
    fsyncs: Arc<std::sync::atomic::AtomicU32>,
}

/// The queue side of an [`IngestPipeline`], under its lock.
#[derive(Default)]
struct PipelineState {
    queue: std::collections::VecDeque<Checkpoint>,
    /// A worker is taking checkpoints from `queue`.
    worker_running: bool,
    /// A worker failed or panicked. No new checkpoint is queued.
    failed: bool,
}

impl IngestPipeline {
    fn lock_state(&self) -> anyhow::Result<std::sync::MutexGuard<'_, PipelineState>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("ingest pipeline")))
    }

    /// The worker body: checkpoint every queued batch until the queue is
    /// empty, or stop at the first failure. A failure (or a panic) marks the
    /// pipeline failed and drops the queued checkpoints, which frees their
    /// slots so the decode loop cannot wait on a slot that no worker will
    /// free.
    fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let mut exit = WorkerExit {
            pipeline: &self,
            clean: false,
        };
        let res = self.drain();
        match &res {
            Ok(()) => exit.clean = true,
            // Log here as well as returning the error: when the caller drops
            // `ingest_stream`, nobody awaits this worker, and the error would
            // otherwise vanish.
            Err(e) => tracing::warn!(
                error = %e,
                fetch_start = self.range.fetch_start(),
                fetch_end = self.range.fetch_end(),
                "ingest checkpoint failed"
            ),
        }
        drop(exit);
        res
    }

    /// Take the next checkpoint, write it, write the checkpoints queued
    /// behind it (up to `INGEST_MAX_QUEUED_CHECKPOINTS` per group), then
    /// fsync the group once and union its prefix into `present`.
    ///
    /// The group cap bounds how long a producer that never slows can defer
    /// an fsync. Every checkpoint holds a slot, so the slots already imply
    /// the cap; the loop states it locally. Every slot in the group frees only
    /// after the fsync, so the slots bound the bytes that are received but
    /// not yet durable.
    fn drain(&self) -> anyhow::Result<()> {
        let cap = ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS;
        let mut files = self
            .files
            .lock()
            .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("ingest files")))?;
        let mut slots = Vec::with_capacity(cap);
        while let Some(first) = self.pop(true)? {
            let mut received_end = write_checkpoint(&mut files, first, &mut slots)?;
            while slots.len() < cap {
                let Some(next) = self.pop(false)? else {
                    break;
                };
                received_end = received_end.max(write_checkpoint(&mut files, next, &mut slots)?);
            }
            #[cfg(test)]
            {
                std::thread::sleep(self.fsync_delay);
                self.fsyncs
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            sync_and_union(
                &files,
                &self.present,
                self.total_bytes,
                &self.range,
                received_end,
            )?;
            slots.clear();
        }
        Ok(())
    }

    /// Pop the next queued checkpoint. On an empty queue with `retire` set,
    /// clear `worker_running` under the same lock the decode loop queues
    /// under, so the next checkpoint starts a new worker.
    fn pop(&self, retire: bool) -> anyhow::Result<Option<Checkpoint>> {
        let mut state = self.lock_state()?;
        let next = state.queue.pop_front();
        if next.is_none() && retire {
            state.worker_running = false;
        }
        Ok(next)
    }
}

/// Marks the [`IngestPipeline`] failed when a worker exits on an error or a
/// panic.
struct WorkerExit<'a> {
    pipeline: &'a IngestPipeline,
    clean: bool,
}

impl Drop for WorkerExit<'_> {
    fn drop(&mut self) {
        if self.clean {
            return;
        }
        let mut state = self
            .pipeline
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed = true;
        state.worker_running = false;
        state.queue.clear();
    }
}

/// Write `checkpoint`'s batch, free its memory, and keep its slot in `slots`
/// until the group fsyncs. Returns the prefix end it checkpoints.
fn write_checkpoint(
    files: &mut IngestFiles,
    checkpoint: Checkpoint,
    slots: &mut Vec<tokio::sync::OwnedSemaphorePermit>,
) -> anyhow::Result<u64> {
    let Checkpoint {
        batch,
        received_end,
        slot,
    } = checkpoint;
    write_batch(files, &batch)?;
    slots.push(slot);
    Ok(received_end)
}

/// Log a flush failure that a stream fault outranks, so it is not lost.
fn warn_flush_failed_on_fault(err: &anyhow::Error, range: &AlignedRange, received_end: u64) {
    tracing::warn!(
        error = %err,
        fetch_start = range.fetch_start(),
        received_end,
        "ingest checkpoint failed while handling a stream fault"
    );
}

/// Write `batch`'s verified leaves into the `.partial` data file at their
/// offsets and its parent proof pairs into the `.obao4` outboard. Nothing is
/// durable, and `present` is unchanged, until [`sync_and_union`] runs.
fn write_batch(files: &mut IngestFiles, batch: &FlushBatch) -> anyhow::Result<()> {
    for (offset, data) in &batch.leaves {
        files
            .data
            .write_all_at(*offset, data)
            .with_context(|| format!("writing .partial data at offset {offset}"))?;
    }
    for (node, pair) in &batch.parents {
        files
            .outboard
            .save(*node, pair)
            .context("writing .obao4 outboard")?;
    }
    Ok(())
}

/// Durably checkpoint the prefix `[range.fetch_start(), received_end)` of
/// an in-progress [`ClientRangedStore::ingest_stream`] whose batches
/// [`write_batch`] has written: fsync the data and outboard files, THEN union
/// the corresponding chunk ranges into `present`. The fsync-before-union
/// ordering is load-bearing — see the durability contract on
/// [`ClientRangedStore::ingest_stream`].
///
/// `sync_data` (fdatasync) is enough: it persists the written blocks and
/// every metadata change needed to read them back, including the block
/// allocation of a sparse write and the size growth of the `.partial` file.
/// It skips only timestamps, which nothing here reads.
///
/// Does NOT persist the `.ranges` record — that is
/// [`ClientRangedStore::flush_present_record`]'s job. Several `ingest_stream`
/// calls can run concurrently on one store (the multi-source scheduler), so
/// writing the record here, per checkpoint, out of the `present` lock would
/// both race the record's write-and-rename across sources and serialize every
/// source on the record's fsync. `present` only ever grows and is unioned
/// under the mutex AFTER the data/outboard fsync, so whenever the record is
/// next flushed it never claims a range that is not durably on disk.
fn sync_and_union(
    files: &IngestFiles,
    present: &Mutex<ChunkRanges>,
    total_bytes: u64,
    range: &AlignedRange,
    received_end: u64,
) -> anyhow::Result<()> {
    files.data.sync_data().context("fsyncing .partial data")?;
    files
        .outboard
        .data
        .sync_data()
        .context("fsyncing .obao4 outboard")?;

    let received = decdn_bao_range::align_range(
        range.fetch_start(),
        received_end.saturating_sub(range.fetch_start()),
        total_bytes,
    )?;

    let mut guard = present
        .lock()
        .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("present")))?;
    *guard |= received.chunk_ranges().clone();
    Ok(())
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

            // Flush the in-memory `present` snapshot to the `.ranges` record
            // before the verify sweep: ingest checkpoints never persist the
            // record themselves (see `checkpoint`), so this is
            // the single-writer point that makes the on-disk record current.
            // A crash right after this and before promotion still leaves an
            // accurate record to resume from.
            self.write_present_record_blocking()
                .await
                .map_err(backend)?;

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

impl crate::source::IngestStore for ClientRangedStore {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<R>> + 'a>>
    where
        R: crate::source::BaoRangeReader + 'a,
    {
        Box::pin(self.ingest_stream(range, reader, on_progress))
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        let write = self.write_present_record_blocking();
        Box::pin(async move { Ok(write.await?) })
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tmp dir")
    }

    /// A `.ranges` record whose stat fails (here a symlink loop, `ELOOP`) is not
    /// read as "no record": `open_or_create` errors instead of creating, which
    /// would truncate the `.partial` and the bytes already paid for in it.
    #[cfg(unix)]
    #[test]
    fn open_or_create_does_not_truncate_on_a_failed_record_stat() {
        let dir = tmp_dir();
        let data = dir.path().join("blob.partial");
        std::fs::write(&data, b"paid bytes").expect("write partial");
        let ranges = dir.path().join("blob.partial.ranges");
        std::os::unix::fs::symlink(&ranges, &ranges).expect("self-referential symlink");

        let opened = ClientRangedStore::open_or_create(dir.path(), "blob", [0; 32], 10);
        assert!(opened.is_err(), "a failed stat must not create a store");
        assert_eq!(std::fs::read(&data).expect("read partial"), b"paid bytes");
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

    // --- seed_checkpointed_prefix (test-util fixture seam) ---

    #[tokio::test]
    async fn seed_prefix_resumes_from_the_recorded_prefix() {
        let total = 3 * GROUP + 123;
        let (root, plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();
        let seeded = 2 * GROUP;

        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &plaintext, seeded)
            .expect("seed prefix");
        let store = ClientRangedStore::open(dir.path(), "blob", root, total).expect("open seeded");

        // Present is exactly the aligned recorded prefix; the suffix is missing.
        let aligned = decdn_bao_range::align_range(0, seeded, total).expect("align prefix");
        let present = store.present_ranges().await.expect("present_ranges");
        assert_eq!(&present, aligned.chunk_ranges());

        let full = decdn_bao_range::align_range(0, 0, total).expect("align whole");
        let expected_missing = full.chunk_ranges().clone() - aligned.chunk_ranges();
        let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
        assert_eq!(missing, expected_missing);
        assert!(!store.is_complete().await.expect("is_complete"));

        // The recorded prefix is byte-exact readable off the `.partial`.
        let got = store.read(0, seeded).await.expect("read prefix");
        assert_eq!(
            got.as_ref(),
            plaintext
                .get(..usize::try_from(seeded).expect("fits"))
                .expect("slice")
        );
    }

    #[tokio::test]
    async fn seed_complete_prefix_is_complete_and_finalizes() {
        // An exact-multiple-of-group blob seeded COMPLETE (journey-5 shape): the
        // record claims the whole blob, so the store is complete on open and
        // `finalize` promotes with no further pulls.
        let total = 4 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tmp_dir();

        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &plaintext, total)
            .expect("seed complete");
        let store = ClientRangedStore::open(dir.path(), "blob", root, total).expect("open seeded");

        assert!(
            store.is_complete().await.expect("is_complete"),
            "a complete seed must open complete"
        );
        assert!(
            store
                .missing_ranges(0, 0)
                .await
                .expect("missing_ranges")
                .is_empty()
        );

        // The seeded outboard verifies the whole blob, so `finalize` promotes.
        store
            .finalize()
            .await
            .expect("finalize promotes seeded blob");
        let final_path = store.data_path.lock().expect("lock").clone();
        assert!(!final_path.to_string_lossy().ends_with(".partial"));
        let on_disk = std::fs::read(&final_path).expect("read promoted blob");
        assert_eq!(on_disk, plaintext);
    }

    // --- ingest_stream ---

    /// Total bytes covered by a [`ChunkRanges`], reconstructed from its
    /// boundary pairs the same way [`write_ranges_record`] encodes them
    /// (`ChunkNum` counts 1 KiB chunks).
    fn ranges_byte_len(ranges: &ChunkRanges) -> u64 {
        let boundaries = ranges.boundaries();
        let mut sum = 0u64;
        let mut it = boundaries.iter();
        while let (Some(a), Some(b)) = (it.next(), it.next()) {
            sum += (b.0 - a.0) * 1024;
        }
        sum
    }

    #[tokio::test]
    async fn ingest_stream_writes_positioned_and_reflects_presence() -> anyhow::Result<()> {
        let total = 3 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
        let store = fresh_store(root, total);

        // A prefix gap: the first two groups only.
        let aligned = decdn_bao_range::align_range(0, 2 * GROUP, total)?;
        let bao_bytes = bao_for(root, &plaintext, outboard.clone(), &aligned);
        let body = bao_bytes.slice(8..);
        let reader = store.ingest_stream(&aligned, body, None).await?;
        drop(reader);

        let present = store.present_ranges().await?;
        assert_eq!(&present, aligned.chunk_ranges());

        let got = store.read(0, 2 * GROUP).await?;
        assert_eq!(
            got.as_ref(),
            plaintext.get(..usize::try_from(2 * GROUP)?).expect("slice")
        );

        // The `.partial` file holds the bytes at the right (positioned)
        // offset, not appended.
        let on_disk = std::fs::read(store.data_path.lock().expect("lock").clone())?;
        assert_eq!(
            on_disk.get(..usize::try_from(2 * GROUP)?),
            plaintext.get(..usize::try_from(2 * GROUP)?)
        );

        // Ingest the remaining tail, then finalize promotes.
        let rest_aligned = decdn_bao_range::align_range(2 * GROUP, 0, total)?;
        let rest_bao = bao_for(root, &plaintext, outboard, &rest_aligned);
        let rest_body = rest_bao.slice(8..);
        let reader = store.ingest_stream(&rest_aligned, rest_body, None).await?;
        drop(reader);
        store.finalize().await.expect("finalize promotes");
        let final_bytes = store.read(0, total).await?;
        assert_eq!(final_bytes.as_ref(), plaintext.as_slice());

        Ok(())
    }

    #[tokio::test]
    async fn ingest_stream_mid_gap_fault_checkpoints_received_prefix() -> anyhow::Result<()> {
        // Big enough to cross at least one 4 MiB checkpoint interval before
        // the scripted fault lands.
        let total: u64 = 8 * 1024 * 1024;
        let plaintext = {
            let mut v = vec![0u8; usize::try_from(total)?];
            let mut x: u32 = 0x1234_5678;
            for b in &mut v {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                *b = x.to_le_bytes().first().copied().unwrap_or(0);
            }
            v
        };
        let source = crate::source::ScriptedSource::new(plaintext.clone())?
            .with_fault_after(5 * 1024 * 1024, || {
                anyhow::anyhow!("scripted mid-gap stall")
            });
        let root = source.root();
        let store_dir = tmp_dir();
        let mut store = ClientRangedStore::create(store_dir.path(), "blob", root, total)?;
        // Hold the 4 MiB checkpoint's fsync past the fault at 5 MiB, so the
        // checkpointed prefix below exists only if the fault path waits for it.
        store.fsync_delay = Duration::from_millis(300);

        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let (_header, reader) = {
            use crate::source::BlobSource;
            source.open(root, aligned.clone()).await?
        };

        let err = store
            .ingest_stream(&aligned, reader, None)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("mid-gap fault must surface as an error"))?;
        assert!(
            err.to_string().contains("scripted mid-gap stall")
                || format!("{err:?}").contains("scripted mid-gap stall"),
            "the parked typed fault must survive: {err}"
        );

        // Checkpoints no longer persist the `.ranges` record themselves
        // (single-writer flush point, spec §5.5) — a real caller reaches this
        // via `drive`'s post-gap-loop flush, but this test drives
        // `ingest_stream` directly, so it flushes explicitly here before
        // simulating the resumed process re-opening the store.
        store.flush_present_record()?;

        // Re-open the store fresh (simulating a resumed process) and inspect
        // the persisted record: it must reflect the checkpointed prefix —
        // more than one checkpoint interval's worth (proving the fault path
        // checkpointed its verified batch on top of the interval checkpoint),
        // but strictly less than the whole gap (proving the bytes past the
        // fault were NOT claimed).
        let reopened = ClientRangedStore::open(store_dir.path(), "blob", root, total)?;
        let present = reopened.present_ranges().await?;
        let present_bytes = ranges_byte_len(&present);

        assert!(
            present_bytes > 0,
            "a mid-gap fault must not lose the whole gap: present is empty"
        );
        assert!(
            present_bytes > ClientRangedStore::INGEST_CHECKPOINT_BYTES,
            "the fault must checkpoint the verified batch past the first interval too, \
             got {present_bytes} bytes"
        );
        assert!(
            present_bytes < total,
            "the un-checkpointed tail past the fault must not be claimed as present, \
             got {present_bytes} of {total} bytes"
        );

        // And the checkpointed prefix is genuinely readable/verified data,
        // not garbage — a byte-exact prefix of the plaintext.
        let checkpointed = reopened
            .read(0, present_bytes)
            .await
            .expect("checkpointed prefix must be readable");
        assert_eq!(
            checkpointed.as_ref(),
            plaintext
                .get(..usize::try_from(present_bytes)?)
                .expect("slice")
        );

        Ok(())
    }

    /// Deterministic `len`-byte blob (xorshift fill, mirrors `synth_blob`'s
    /// plaintext generation) — used by the concurrent-checkpoint test to build
    /// a fixed-content blob before splitting it into disjoint ranges.
    fn blob(len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        let mut x: u32 = 0x2545_f491;
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        v
    }

    /// Thin wrapper over `PreOrderMemOutboard::create`: the bao root and full
    /// pre-order outboard for an already-built blob, for tests that construct
    /// `data` themselves rather than through `synth_blob`.
    fn bao_root_and_outboard(data: &[u8]) -> ([u8; 32], Bytes) {
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
        (*ob.root.as_bytes(), Bytes::from(ob.data))
    }

    /// The header-less bao wire (content + interleaved proof) for `range` of
    /// `data`, ready to hand to [`ClientRangedStore::ingest_stream`] as a
    /// `Bytes` reader — a thin wrapper over `encode_verified_range`, mirroring
    /// `ScriptedSource::wire_for` (`crate::source`).
    fn scripted_reader_for(data: &[u8], range: &AlignedRange) -> anyhow::Result<Bytes> {
        let (root, outboard) = bao_root_and_outboard(data);
        let s = usize::try_from(range.fetch_start())?;
        let e = usize::try_from(range.fetch_end())?;
        let slice = data
            .get(s..e)
            .ok_or_else(|| anyhow::anyhow!("scripted range out of bounds"))?;
        let combined = decdn_bao_range::encode_verified_range(root, range, slice, outboard)?;
        let wire = combined
            .get(8..)
            .ok_or_else(|| anyhow::anyhow!("combined wire shorter than its 8-byte header"))?;
        Ok(Bytes::copy_from_slice(wire))
    }

    #[tokio::test]
    async fn concurrent_ingest_present_record_never_regresses() -> anyhow::Result<()> {
        // Two disjoint bao-aligned ranges of one blob, ingested concurrently, then
        // flushed. The persisted .ranges must equal the union of both ranges.
        let dir = tempfile::tempdir()?;
        let data = blob(8 * 1024 * 1024); // 8 MiB -> two 4 MiB halves, group-aligned
        let (root, _) = bao_root_and_outboard(&data);
        let store = ClientRangedStore::create(dir.path(), "b", root, data.len() as u64)?;

        let lo = decdn_bao_range::align_range(0, 4 * 1024 * 1024, data.len() as u64)?;
        let hi = decdn_bao_range::align_range(4 * 1024 * 1024, 4 * 1024 * 1024, data.len() as u64)?;

        let a = store.ingest_stream(&lo, scripted_reader_for(&data, &lo)?, None);
        let b = store.ingest_stream(&hi, scripted_reader_for(&data, &hi)?, None);
        let (ra, rb) = tokio::join!(a, b);
        ra?;
        rb?;

        store.flush_present_record()?;

        let on_disk = read_ranges_record(store.ranges_path())?;
        let expected = lo.chunk_ranges().clone() | hi.chunk_ranges().clone();
        assert_eq!(on_disk, expected);
        Ok(())
    }

    #[tokio::test]
    async fn ingest_stream_corrupt_bao_is_error_not_written() -> anyhow::Result<()> {
        let total = 2 * GROUP;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
        let store = fresh_store(root, total);

        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let mut bao_bytes = bao_for(root, &plaintext, outboard, &aligned).to_vec();
        // Flip a byte well past the 8-byte header, inside the interleaved
        // proof+data body.
        let flip_at = bao_bytes.len() - 1;
        if let Some(b) = bao_bytes.get_mut(flip_at) {
            *b ^= 0xFF;
        }
        let body = Bytes::from(bao_bytes).slice(8..);

        let err = store
            .ingest_stream(&aligned, body, None)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("corrupt payload must fail"))?;
        assert!(
            err.downcast_ref::<crate::HashMismatch>().is_some(),
            "corruption must surface as the typed HashMismatch, got: {err}"
        );

        // The flipped byte sits in the second group's leaf, so the first
        // group verified before the fault. The fault path checkpoints that
        // verified prefix and nothing past it: the corrupt group is never
        // claimed, and the claimed group reads back byte-exact.
        let first_group = decdn_bao_range::align_range(0, GROUP, total)?;
        let present = store.present_ranges().await?;
        assert_eq!(
            &present,
            first_group.chunk_ranges(),
            "presence must be exactly the verified prefix before the corrupt group"
        );
        let got = store.read(0, GROUP).await?;
        assert_eq!(
            got.as_ref(),
            plaintext.get(..usize::try_from(GROUP)?).expect("slice")
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn ingest_stream_slow_flush_does_not_block_the_runtime() -> anyhow::Result<()> {
        // The paid pull pays from its read/decode loop, so a worker thread
        // parked in a checkpoint fsync stops voucher sends on every lane that
        // shares the runtime (#2117). With a single worker, a stalled flush
        // must still leave that worker free to run other tasks.
        const FLUSH_DELAY: Duration = Duration::from_secs(3);
        const MAX_GAP: Duration = Duration::from_millis(1500);

        let dir = tempfile::tempdir()?;
        let data = blob(8 * 1024 * 1024); // two checkpoint intervals
        let total = data.len() as u64;
        let (root, _) = bao_root_and_outboard(&data);
        let mut store = ClientRangedStore::create(dir.path(), "b", root, total)?;
        store.fsync_delay = FLUSH_DELAY;
        let store = Arc::new(store);
        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let wire = scripted_reader_for(&data, &aligned)?;

        // Largest gap, in ms, between consecutive wakeups of a 20 ms ticker
        // sharing the one worker with the ingest.
        let max_gap_ms = Arc::new(AtomicU64::new(0));
        let ticker = tokio::spawn({
            let max_gap_ms = Arc::clone(&max_gap_ms);
            async move {
                let mut last = std::time::Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let now = std::time::Instant::now();
                    let gap =
                        u64::try_from(now.duration_since(last).as_millis()).unwrap_or(u64::MAX);
                    max_gap_ms.fetch_max(gap, Ordering::Relaxed);
                    last = now;
                }
            }
        });
        let ingest = tokio::spawn({
            let store = Arc::clone(&store);
            async move { store.ingest_stream(&aligned, wire, None).await.map(drop) }
        });

        ingest.await??;
        // Let the ticker wake once more, so a stall at the very end is recorded.
        tokio::time::sleep(Duration::from_millis(100)).await;
        ticker.abort();
        let max_gap = Duration::from_millis(max_gap_ms.load(Ordering::Relaxed));
        assert!(
            max_gap < MAX_GAP,
            "a slow flush blocked the runtime worker for {max_gap:?}"
        );

        let present = store.present_ranges().await?;
        assert_eq!(
            &present,
            decdn_bao_range::align_range(0, 0, total)?.chunk_ranges()
        );
        Ok(())
    }

    /// A store over `dir` for `data`, with every ingest fsync stalled by
    /// `fsync_delay`, plus the header-less wire of the whole blob.
    fn slow_fsync_store(
        dir: &Path,
        data: &[u8],
        fsync_delay: Duration,
    ) -> anyhow::Result<(ClientRangedStore, AlignedRange, Bytes)> {
        let total = u64::try_from(data.len())?;
        let (root, _) = bao_root_and_outboard(data);
        let mut store = ClientRangedStore::create(dir, "b", root, total)?;
        store.fsync_delay = fsync_delay;
        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let wire = scripted_reader_for(data, &aligned)?;
        Ok((store, aligned, wire))
    }

    /// `INGEST_CHECKPOINT_BYTES` times `n`, as a `usize` blob length.
    fn checkpoints(n: u64) -> anyhow::Result<usize> {
        Ok(usize::try_from(
            n * ClientRangedStore::INGEST_CHECKPOINT_BYTES,
        )?)
    }

    /// The decode loop, and so the payment it drives, runs past a checkpoint
    /// whose fsync stalls: four checkpoints fit the pipeline slots, so the
    /// whole blob is received before the first fsync lands. `present` extends
    /// only after the fsync.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_stream_decodes_past_a_slow_fsync() -> anyhow::Result<()> {
        const FSYNC_DELAY: Duration = Duration::from_secs(3);

        let dir = tempfile::tempdir()?;
        let data = blob(checkpoints(4)?);
        let total = u64::try_from(data.len())?;
        let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, FSYNC_DELAY)?;

        // When progress first reached `total`: the elapsed time, and whether
        // `present` was still empty then.
        let at_total: Mutex<Option<(Duration, bool)>> = Mutex::new(None);
        let started = std::time::Instant::now();
        let on_progress = |received: u64| {
            if received == total {
                let empty = store.present.lock().is_ok_and(|p| p.is_empty());
                if let Ok(mut slot) = at_total.lock() {
                    slot.get_or_insert((started.elapsed(), empty));
                }
            }
        };
        store
            .ingest_stream(&aligned, wire, Some(&on_progress))
            .await?;

        let (elapsed, empty) = at_total
            .lock()
            .map_err(|_| anyhow::anyhow!("progress lock poisoned"))?
            .ok_or_else(|| anyhow::anyhow!("progress never reached the total"))?;
        assert!(
            elapsed < FSYNC_DELAY,
            "the decode loop waited for a slow fsync: whole blob received after {elapsed:?}"
        );
        assert!(
            empty,
            "present must not extend before the first fsync lands"
        );
        assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
        Ok(())
    }

    /// The bytes received but not yet durable never exceed the pipeline
    /// slots plus the batch the loop is building:
    /// `(INGEST_MAX_QUEUED_CHECKPOINTS + 1) * INGEST_CHECKPOINT_BYTES`. That
    /// is the crash and dropped-future re-pay bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_stream_bounds_undurable_bytes() -> anyhow::Result<()> {
        let slots = u64::try_from(ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS)?;
        let bound = (slots + 1) * ClientRangedStore::INGEST_CHECKPOINT_BYTES;

        let dir = tempfile::tempdir()?;
        let data = blob(checkpoints(slots + 4)?);
        let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, Duration::from_secs(1))?;

        let max_undurable = AtomicU64::new(0);
        let on_progress = |received: u64| {
            let durable = store.present.lock().map_or(0, |p| ranges_byte_len(&p));
            max_undurable.fetch_max(received.saturating_sub(durable), Ordering::Relaxed);
        };
        store
            .ingest_stream(&aligned, wire, Some(&on_progress))
            .await?;

        let max_undurable = max_undurable.load(Ordering::Relaxed);
        assert!(
            max_undurable <= bound,
            "{max_undurable} bytes were received but not durable, over the {bound}-byte bound"
        );
        // The loop did run ahead of the stalled fsyncs, so the bound was
        // exercised rather than trivially met.
        assert!(
            max_undurable > 2 * ClientRangedStore::INGEST_CHECKPOINT_BYTES,
            "the loop never ran ahead of the disk: max undurable {max_undurable} bytes"
        );
        assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
        Ok(())
    }

    /// Checkpoints that queue behind a slow fsync share the next fsync. Three
    /// checkpoints fit the pipeline slots, so the loop queues all three
    /// without waiting. The first fsync holds at least the first one; the
    /// rest queue during its stall and the worker folds them into one second
    /// fsync. One fsync per checkpoint would be three.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_stream_coalesces_queued_checkpoints() -> anyhow::Result<()> {
        let slots = u64::try_from(ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS)?;

        let dir = tempfile::tempdir()?;
        let data = blob(checkpoints(slots)?);
        let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, Duration::from_secs(1))?;

        store.ingest_stream(&aligned, wire, None).await?;

        let fsyncs = store.fsyncs.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            (1..=2).contains(&fsyncs),
            "{slots} queued checkpoints took {fsyncs} fsyncs, want at most 2"
        );
        assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
        Ok(())
    }
}
