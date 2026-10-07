//! [`ClientRangedStore`]: the client-side [`RangedStore`](decdn_bao_range::RangedStore) backend
//! (#1621): a `.partial` data file positioned by offset plus a persisted
//! `.partial.ranges` record, built on `bao-tree` / `decdn-bao-range` only. No
//! `iroh-blobs` dependency: `decdn-client` must stay iroh-blobs-free so the
//! CLI's pull path links no blob store / AWS SDK (#578).
//!
//! The store is keyed by offset, not by size. A blob's size is a hint: the
//! store holds the current size `bound` the planner works to, and a `proven`
//! size once some leg proves one. Each leg verifies under the size its own
//! sender claims, because a range can verify only under its sender's tree:
//! `ingest_stream` realigns the leg under that claim and decodes with
//! `BaoTree::new(claim, IROH_BLOCK_SIZE)`. A verified leaf is a genuine byte
//! at its genuine offset under any claim, so the data file and the verified
//! chunk ranges do not depend on the size. A leg that verifies the final chunk
//! of its claim proves that size: only the true size verifies a final chunk.
//!
//! Construction plus the query methods (`total_bytes`, `present_ranges`,
//! `missing_ranges`, `read`, `is_complete`) query the record. `admit` and
//! `ingest_stream` write verified leaves at their offsets, and `finalize`
//! hashes `[0, proven)` with BLAKE3 against the root before it promotes the
//! `.partial` file to its final path.
//!
//! The record is the load-bearing shortcut that keeps
//! `present_ranges`/`missing_ranges`/`is_complete` O(1):
//! [`ClientRangedStore::open`] trusts the record a prior write persisted
//! rather than re-hashing the `.partial` data. The record itself is written
//! with a tempfile-plus-rename so a crash mid-write cannot tear it.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::Context as _;
use bao_tree::io::BaoContentItem;
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bao_tree::io::sync::{ReadAt, WriteAt};
use bao_tree::{BaoTree, ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, RangedFuture, RangedStore, RangedStoreError};

use crate::sink::{StashedFault, classify_decode_error};
use crate::source::IngestEnd;

/// A checkpoint-slot wait at least this long is logged at `debug`: ingest does
/// not read its stream while it waits (#2211).
const CHECKPOINT_WAIT_LOG_FLOOR: std::time::Duration = std::time::Duration::from_secs(1);

/// The read buffer `finalize` hashes the data file through, so a blob of any
/// size hashes in bounded memory.
const HASH_BUF_BYTES: usize = 1 << 20;

/// The size and presence state of one [`ClientRangedStore`], persisted as its
/// `.partial.ranges` record.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreState {
    /// The size, in bytes, the planner works to: the caller's hint until a leg
    /// proves a size, then the proven size.
    bound: u64,
    /// The size a verified final chunk proved, if any.
    proven: Option<u64>,
    /// Chunk ranges verified present at their offsets.
    present: ChunkRanges,
}

impl StoreState {
    /// A fresh state for a store that holds nothing yet.
    fn empty(bound: u64) -> Self {
        Self {
            bound,
            proven: None,
            present: ChunkRanges::empty(),
        }
    }

    /// Record that a verified final chunk proved `size`: the bound becomes it.
    const fn prove(&mut self, size: u64) {
        self.proven = Some(size);
        self.bound = size;
    }
}

/// The on-disk shape of a [`StoreState`]: `present` as the boundary list of
/// `ChunkRanges::boundaries` in `ChunkNum` units.
#[derive(serde::Serialize, serde::Deserialize)]
struct Record {
    bound: u64,
    proven: Option<u64>,
    boundaries: Vec<u64>,
}

/// Client-side [`RangedStore`]: a `.partial` data file positioned by offset
/// and a persisted `.partial.ranges` record of its size bound, its proven
/// size, and its verified chunk ranges, for one blob `root`.
///
/// `state` and `data_path` are held behind `Arc<Mutex<..>>` so `admit`,
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
/// safe because its checkpoints only union into `state` under the mutex and
/// never write the record.
pub struct ClientRangedStore {
    /// BLAKE3 content root this store verifies against.
    root: [u8; 32],
    /// Current data file. `.partial` until `finalize` promotes it.
    data_path: Arc<Mutex<PathBuf>>,
    /// Record sidecar (`{stem}.partial.ranges`).
    ranges_path: PathBuf,
    /// Size bound, proven size and verified chunk ranges, trusted from the
    /// record (loaded once at construction, updated in memory by
    /// `admit`/`ingest_stream`/`set_bound`, persisted by
    /// `admit`/`finalize`/`flush_present_record`).
    state: Arc<Mutex<StoreState>>,
    /// Woken each time an ingest checkpoint extends `present`
    /// ([`Self::present_grew`]).
    present_grew: Arc<tokio::sync::Notify>,
    /// Test-only stall injected before each ingest checkpoint fsync, to model
    /// a slow disk.
    #[cfg(test)]
    fsync_delay: std::time::Duration,
    /// Test-only count of the ingest checkpoint fsyncs across every
    /// `ingest_stream` call on this store.
    #[cfg(test)]
    fsyncs: Arc<std::sync::atomic::AtomicU32>,
    /// Test-only prefix end of each ingest checkpoint, in queue order.
    #[cfg(test)]
    checkpoint_ends: Arc<Mutex<Vec<u64>>>,
}

impl std::fmt::Debug for ClientRangedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRangedStore")
            .field("root", &hex_prefix(&self.root))
            .field("bound", &self.bound())
            .field("proven", &self.proven())
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

/// The BLAKE3 root of the empty blob.
fn empty_root() -> [u8; 32] {
    *blake3::hash(&[]).as_bytes()
}

/// The chunk ranges of the whole `[0, size)` blob.
fn whole(size: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum(0)..ChunkNum::chunks(size))
}

/// The `.partial` data file and `.partial.ranges` record paths for `stem`.
fn sidecar_paths(dir: &Path, stem: &str) -> (PathBuf, PathBuf) {
    let data = dir.join(format!("{stem}.partial"));
    let ranges = dir.join(format!("{stem}.partial.ranges"));
    (data, ranges)
}

/// Stream the first `len` bytes of `file` through BLAKE3 in bounded memory.
fn hash_prefix(file: &File, len: u64) -> io::Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; HASH_BUF_BYTES];
    let mut pos = 0u64;
    while pos < len {
        let want = usize::try_from(len.saturating_sub(pos))
            .map_or(HASH_BUF_BYTES, |left| left.min(HASH_BUF_BYTES));
        let slice = buf
            .get_mut(..want)
            .ok_or_else(|| io::Error::other("hash buffer slice"))?;
        file.read_exact_at(pos, slice)?;
        hasher.update(slice);
        pos = pos.saturating_add(u64::try_from(want).map_err(io::Error::other)?);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Whether the first `len` bytes of `file` hash to `root`. A file shorter than
/// `len` does not match.
fn prefix_matches(file: &File, len: u64, root: [u8; 32]) -> io::Result<bool> {
    match hash_prefix(file, len) {
        Ok(hash) => Ok(hash == root),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

/// Persist `state` to `path` via tempfile-plus-rename (atomic on the same
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
fn write_record(path: &Path, state: &StoreState) -> io::Result<()> {
    let record = Record {
        bound: state.bound,
        proven: state.proven,
        boundaries: state.present.boundaries().iter().map(|c| c.0).collect(),
    };
    let json =
        serde_json::to_vec(&record).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    io::Write::write_all(&mut tmp, &json)?;
    // Durably write the record bytes before the atomic rename: without this,
    // a crash right after `persist`'s rename can leave a zero-length/torn
    // `.ranges` file on a non-ordered filesystem, which then hard-fails JSON
    // parsing in `read_record` on the next `open`.
    tmp.as_file().sync_all()?;
    tmp.persist(path)
        .map_err(|e| io::Error::new(e.error.kind(), e.error))?;
    Ok(())
}

/// Load a record written by [`write_record`], folding consecutive boundary
/// pairs `[a, b)` into a fresh [`ChunkRanges`]. An odd-length or otherwise
/// malformed record, or one in another format, is corrupt data rather than a
/// missing file, and surfaces as an `io::Error` rather than as empty.
fn read_record(path: &Path) -> io::Result<StoreState> {
    let bytes = std::fs::read(path)?;
    let Record {
        bound,
        proven,
        boundaries,
    } = serde_json::from_slice(&bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unrecognised record format at {}: {e}", path.display()),
        )
    })?;
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
    Ok(StoreState {
        bound,
        proven,
        present: acc,
    })
}

impl ClientRangedStore {
    /// A store over `data_path` and `ranges_path` holding `state`. The empty
    /// root is the root of the empty blob alone, so it proves a size of zero
    /// on its own.
    fn with_state(
        root: [u8; 32],
        data_path: PathBuf,
        ranges_path: PathBuf,
        mut state: StoreState,
    ) -> Self {
        if root == empty_root() {
            state.prove(0);
        }
        Self {
            root,
            data_path: Arc::new(Mutex::new(data_path)),
            ranges_path,
            state: Arc::new(Mutex::new(state)),
            present_grew: Arc::default(),
            #[cfg(test)]
            fsync_delay: std::time::Duration::ZERO,
            #[cfg(test)]
            fsyncs: Arc::default(),
            #[cfg(test)]
            checkpoint_ends: Arc::default(),
        }
    }

    /// Create a brand-new `.partial` and record for `root` under `dir`, keyed
    /// by `stem` (typically the hex content hash). The data file starts empty
    /// (positioned writes make it sparse), and the record starts with
    /// `bound_hint` as its bound, no proven size, and nothing present.
    ///
    /// # Errors
    ///
    /// Any I/O failure creating the data file or the record.
    pub fn create(dir: &Path, stem: &str, root: [u8; 32], bound_hint: u64) -> io::Result<Self> {
        let (data_path, ranges_path) = sidecar_paths(dir, stem);
        File::create(&data_path)?;
        let store = Self::with_state(root, data_path, ranges_path, StoreState::empty(bound_hint));
        write_record(&store.ranges_path, &store.snapshot())?;
        Ok(store)
    }

    /// Reopen an existing `.partial` and record for `root` under
    /// `dir`/`stem`. The record supplies the bound, the proven size and the
    /// present ranges in O(1): `open` trusts the record a prior write
    /// persisted and does not re-hash the data file.
    ///
    /// If the final, non-`.partial` file exists and its BLAKE3 equals `root`,
    /// `finalize` already promoted this blob: `open` reconstructs a complete
    /// store that serves from the final file, with the file's length as the
    /// proven size and the whole of it present. This also covers a crash
    /// between `finalize`'s rename and its (best-effort) record cleanup: a
    /// leftover record is removed here. A final file of another blob is left
    /// alone, and `open` resumes this blob's `.partial` from its record.
    ///
    /// When a final file is present, `open` hashes it once against the root, a
    /// read of the whole file. Async callers run `open` on a blocking thread.
    ///
    /// # Errors
    ///
    /// Any I/O failure hashing the final file or reading the record,
    /// including a missing or corrupt (malformed) record.
    pub fn open(dir: &Path, stem: &str, root: [u8; 32]) -> io::Result<Self> {
        let (data_path, ranges_path) = sidecar_paths(dir, stem);
        let final_path = dir.join(stem);

        if final_path.exists() {
            let len = std::fs::metadata(&final_path)?.len();
            if prefix_matches(&File::open(&final_path)?, len, root)? {
                let _ = std::fs::remove_file(&ranges_path);
                let state = StoreState {
                    bound: len,
                    proven: Some(len),
                    present: whole(len),
                };
                return Ok(Self::with_state(root, final_path, ranges_path, state));
            }
        }

        let state = read_record(&ranges_path)?;
        Ok(Self::with_state(root, data_path, ranges_path, state))
    }

    /// Whether a `.ranges` record for `stem` exists in `dir`, so that
    /// [`open_or_create`](Self::open_or_create) would resume it rather than
    /// create a fresh store.
    ///
    /// # Errors
    ///
    /// A stat of the record that fails for a reason other than `NotFound`.
    pub fn has_record(dir: &Path, stem: &str) -> io::Result<bool> {
        let (_data_path, ranges_path) = sidecar_paths(dir, stem);
        ranges_path.try_exists()
    }

    /// Reopen an existing `.partial` store for `root` if one is on disk,
    /// otherwise [`create`](Self::create) a fresh one with `bound_hint` as its
    /// bound. The presence of the `.partial.ranges` record is the resume
    /// signal: `create` writes it, and `admit`, `finalize` and
    /// `flush_present_record` rewrite it atomically, so a store with a record
    /// is resumable and one without is not. A resumed store keeps the bound
    /// and proven size its record holds, whatever `bound_hint` says, and
    /// discards none of its partial.
    ///
    /// Keyed on the `.ranges` record alone, NOT on the promoted final file:
    /// once `finalize` promotes and deletes the record, a re-fetch of the same
    /// stem starts fresh, since there is nothing left to resume from. A
    /// `.partial` without a record is truncated by `create`'s `File::create`.
    /// Resuming a record runs [`open`](Self::open), which hashes a final file
    /// if one is present, so async callers run this on a blocking thread.
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
        bound_hint: u64,
    ) -> io::Result<Self> {
        let (_data_path, ranges_path) = sidecar_paths(dir, stem);
        // `try_exists`, not `exists`: a stat that fails for any reason but
        // `NotFound` must not read as "no record", because `create` truncates the
        // `.partial` and every byte already paid for in it.
        if ranges_path.try_exists()? {
            Self::open(dir, stem, root)
        } else {
            Self::create(dir, stem, root, bound_hint)
        }
    }

    /// Seed the on-disk state a *checkpointed* interrupted download leaves for
    /// the first `prefix_len` bytes (rounded UP to a chunk-group boundary) of
    /// `blob`: the positioned `.partial` prefix and a `.ranges` record whose
    /// bound is the blob's length and which covers the prefix. A subsequent
    /// [`open`](Self::open) / [`open_or_create`](Self::open_or_create) resumes
    /// from it, so a fetch skips the recorded prefix and pulls only the suffix.
    /// `stem` = the output file name (e.g. `"blob.bin"`), so the sidecars sit
    /// beside `dir/<stem>` exactly where a fetch opens them.
    ///
    /// `prefix_len == blob.len() as u64` seeds a COMPLETE checkpointed partial:
    /// the prefix holds the final chunk, so the record also proves the blob's
    /// length, a resume pulls nothing, and [`finalize`](RangedStore::finalize)
    /// promotes for free.
    ///
    /// Test/fixture seam only (`#[cfg(any(test, feature = "test-util"))]`): it
    /// writes files directly without going through the verifying `admit` path,
    /// which is precisely what makes it a faithful stand-in for "a prior process
    /// left this checkpoint on disk".
    ///
    /// # Errors
    ///
    /// Any I/O failure writing the data file or the record, or an alignment
    /// failure (`prefix_len` past the blob end).
    #[cfg(any(test, feature = "test-util"))]
    pub fn seed_checkpointed_prefix(
        dir: &Path,
        stem: &str,
        blob: &[u8],
        prefix_len: u64,
    ) -> io::Result<()> {
        let total_bytes = u64::try_from(blob.len())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let (data_path, ranges_path) = sidecar_paths(dir, stem);

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

        let mut state = StoreState::empty(total_bytes);
        state.present = aligned.chunk_ranges().clone();
        if aligned.fetch_end() == total_bytes {
            state.prove(total_bytes);
        }
        write_record(&ranges_path, &state)
    }

    /// The content root this store verifies against.
    #[must_use]
    pub const fn root(&self) -> [u8; 32] {
        self.root
    }

    /// The size, in bytes, the planner works to: the caller's hint until a
    /// leg proves a size, then the proven size.
    #[must_use]
    pub fn bound(&self) -> u64 {
        self.lock_state().bound
    }

    /// The size a verified final chunk proved, if any leg has proved one.
    #[must_use]
    pub fn proven(&self) -> Option<u64> {
        self.lock_state().proven
    }

    /// The byte spans the store holds verified present, as sorted, disjoint
    /// `(offset, len)` pairs with the last one clamped to the bound. Only
    /// fetched bytes are present: a caller that writes the data file by other
    /// means does not mark them. Read on a reopened store, these are the bytes
    /// earlier runs fetched, which a resumed fetch does not fetch again.
    #[must_use]
    pub fn present_byte_ranges(&self) -> Vec<(u64, u64)> {
        let state = self.snapshot();
        crate::driver::contiguous_byte_ranges(&state.present, state.bound)
    }

    /// Woken each time an ingest checkpoint makes more verified bytes durable
    /// and present. A reader waiting for bytes registers on it before reading
    /// the present frontier, so a checkpoint that lands between the read and
    /// the wait still wakes it.
    #[must_use]
    pub(crate) fn present_grew(&self) -> &tokio::sync::Notify {
        &self.present_grew
    }

    /// Move the planner's bound to `bound`. The record picks it up at its next
    /// flush. A proven size is final: once a leg has proved one, the bound
    /// stays at it and this does nothing.
    pub fn set_bound(&self, bound: u64) {
        let mut state = self.lock_state();
        if state.proven.is_none() {
            state.bound = bound;
        }
    }

    /// The state under its lock. A poisoned lock still holds a consistent
    /// state: every writer updates it in one assignment or one union.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    /// The record path, used by `admit`/`finalize`.
    #[must_use]
    pub(crate) fn ranges_path(&self) -> &Path {
        &self.ranges_path
    }

    /// A copy of the current state.
    fn snapshot(&self) -> StoreState {
        self.lock_state().clone()
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
    /// future (a scheduler cancel or stall), the unflushed batch is lost and
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
    ///
    /// Each call ramps up to this size from
    /// [`Self::INGEST_FIRST_CHECKPOINT_BYTES`], so the bound holds for every
    /// checkpoint.
    pub(crate) const INGEST_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

    /// The batch size of each [`Self::ingest_stream`] call's first checkpoint.
    /// Each later checkpoint doubles it ([`Self::next_checkpoint_bytes`]) up
    /// to [`Self::INGEST_CHECKPOINT_BYTES`].
    ///
    /// A reader of the verified stream sees a byte only once its checkpoint is
    /// durable and present, so the first checkpoint sets how soon the first
    /// bytes are readable. 64 KiB (four chunk groups) makes them readable
    /// after one small fsync. The ramp reaches the full interval within six
    /// checkpoints, so a long range costs only a few extra fsyncs.
    pub(crate) const INGEST_FIRST_CHECKPOINT_BYTES: u64 = 64 * 1024;

    /// The batch size of the checkpoint after one of `current` bytes: double
    /// it, capped at [`Self::INGEST_CHECKPOINT_BYTES`].
    pub(crate) const fn next_checkpoint_bytes(current: u64) -> u64 {
        let doubled = current.saturating_mul(2);
        if doubled < Self::INGEST_CHECKPOINT_BYTES {
            doubled
        } else {
            Self::INGEST_CHECKPOINT_BYTES
        }
    }

    /// The prefix one [`Self::ingest_stream`] call has checkpointed once
    /// `received` bytes of its range have arrived and the stream then stalls:
    /// the end of the last whole checkpoint of the ramp.
    #[cfg(test)]
    pub(crate) const fn checkpointed_len(received: u64) -> u64 {
        let mut size = Self::INGEST_FIRST_CHECKPOINT_BYTES;
        let mut end = 0;
        while end + size <= received {
            end += size;
            size = Self::next_checkpoint_bytes(size);
        }
        end
    }

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

    /// Stream the raw bao encoding of `range` (from `reader`) into the store,
    /// verified under `claimed_total`, the size the leg's sender signs.
    ///
    /// The sender serves `range` clamped to its own size, so this realigns
    /// `range` under `claimed_total` with
    /// [`decdn_bao_range::align_range_clamped`] and decodes under
    /// `BaoTree::new(claimed_total, IROH_BLOCK_SIZE)`. A range verifies only
    /// under its sender's tree, and a leaf that verifies is a genuine byte at
    /// its genuine offset under any claim, so the store's own bound plays no
    /// part in verification. A leg that verifies the final chunk of
    /// `claimed_total` proves that size: the store sets `proven` and its bound
    /// to it once that chunk is durable.
    ///
    /// The call verifies each chunk group against the root as
    /// [`bao_tree::io::fsm::ResponseDecoder`] decodes it, collects the verified
    /// leaves into a batch, and hands each batch (plus the remainder at
    /// completion) to a checkpoint worker as one durable checkpoint:
    /// positioned-write the leaves into the `.partial` data file, fsync it,
    /// union the received prefix into `present`, then wake the readers that
    /// wait for present bytes. The first batch is
    /// `INGEST_FIRST_CHECKPOINT_BYTES`, and each later one doubles up to
    /// `INGEST_CHECKPOINT_BYTES`.
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
    /// `present`. The in-order worker keeps `present` a contiguous prefix of
    /// the leg. It runs only while checkpoints are queued. Memory per call is bounded by
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
    /// The same fsync-before-record invariant `write_record`'s
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
    /// - A `claimed_total` that does not cover `range`'s start: an honest
    ///   sender refuses such a range rather than serving it.
    /// - [`crate::HashMismatch`] for a `claimed_total` of zero against a root
    ///   other than the empty blob's.
    /// - The typed [`StashedFault`] the reader parked, if any (a stalled or
    ///   refusing peer) — takes precedence over the decoder's own complaint.
    /// - Otherwise the bao decode failure, classified by
    ///   `sink::classify_decode_error`: [`crate::HashMismatch`] for a
    ///   verification failure (a wrong claim included), a truncation error for
    ///   a short stream.
    /// - Any I/O failure opening, writing or fsyncing the `.partial` file, or
    ///   a checkpoint worker that panics. When the stream itself failed, the
    ///   stashed fault or decode failure outranks a checkpoint failure, which
    ///   is logged instead: it says what the peer did, which decides retry and
    ///   blame, and a local disk error does not make a bad or truncated stream
    ///   good.
    pub async fn ingest_stream<R>(
        &self,
        range: &AlignedRange,
        reader: R,
        on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
    ) -> anyhow::Result<R>
    where
        R: iroh_io::AsyncStreamReader + StashedFault + Send,
    {
        self.ingest_stream_until(range, reader, on_progress, claimed_total, None)
            .await
            .map(|(reader, _)| reader)
    }

    /// [`Self::ingest_stream`] with an end the caller can lower while the leg
    /// streams. Once the verified prefix reaches `stop_at`, the call
    /// checkpoints that prefix and returns [`IngestEnd::Stopped`] without
    /// reading further; the bytes past it stay missing. A range the decoder
    /// reads to its end returns [`IngestEnd::Drained`].
    ///
    /// # Errors
    ///
    /// As [`Self::ingest_stream`].
    pub(crate) async fn ingest_stream_until<R>(
        &self,
        range: &AlignedRange,
        reader: R,
        on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&AtomicU64>,
    ) -> anyhow::Result<(R, IngestEnd)>
    where
        R: iroh_io::AsyncStreamReader + StashedFault + Send,
    {
        let range = &decdn_bao_range::align_range_clamped(
            range.fetch_start(),
            range.fetch_len(),
            claimed_total,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "the leg claims a {claimed_total}-byte blob, which does not cover its range \
                 start {}: {e}",
                range.fetch_start()
            )
        })?;
        // The empty blob has no leaf to verify and no chunk for a decoder to
        // anchor: its claim holds only against the empty root, which proves it.
        if claimed_total == 0 {
            if self.root != empty_root() {
                return Err(anyhow::Error::new(crate::HashMismatch));
            }
            self.lock_state().prove(0);
            return Ok((reader, IngestEnd::Drained));
        }
        let mut flusher = IngestFlusher::open(self, range, claimed_total).await?;

        let root = bao_tree::blake3::Hash::from(self.root);
        let ranges = range.chunk_ranges().clone();
        let tree = BaoTree::new(claimed_total, IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(root, ranges, tree, reader);

        // The contiguous prefix of `range`, in bytes, whose leaves have been
        // received and verified so far (not yet necessarily flushed — see
        // `batch_start` below).
        let mut received_end = range.fetch_start();
        // The prefix already handed to the checkpoint worker. Only the span
        // `[batch_start, received_end)` sits in `batch`.
        let mut batch_start = range.fetch_start();
        let mut batch = FlushBatch::default();
        // The size at which the batch in progress checkpoints.
        let mut batch_target = Self::INGEST_FIRST_CHECKPOINT_BYTES;

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
                    if received_end.saturating_sub(batch_start) >= batch_target {
                        flusher
                            .start(std::mem::take(&mut batch), received_end)
                            .await?;
                        batch_start = received_end;
                        batch_target = Self::next_checkpoint_bytes(batch_target);
                    }
                    // A steal lowered the end to a split this prefix reached:
                    // stop here, so the bytes past it go to the stealer alone.
                    // A prefix that reached the range's end drains as usual.
                    // `on_progress` above published this prefix before the
                    // end is read, the order a steal relies on.
                    if received_end < range.fetch_end()
                        && stop_at.is_some_and(|end| received_end >= end.load(Ordering::SeqCst))
                    {
                        let r = rest.finish();
                        flusher.finish(batch, received_end).await?;
                        return Ok((r, IngestEnd::Stopped));
                    }
                    decoder = rest;
                }
                // A parent only carries the proof the decoder has already
                // checked: the store keeps data, not proofs.
                ResponseDecoderNext::More((rest, Ok(BaoContentItem::Parent(_)))) => {
                    decoder = rest;
                }
                ResponseDecoderNext::More((rest, Err(decode_err))) => {
                    let mut r = rest.finish();
                    // The decoder yields a leaf only once it verifies, and
                    // yields the leaves of the one contiguous `range` in
                    // order, so `batch` holds verified leaves and
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
                    return Ok((r, IngestEnd::Drained));
                }
            }
        }
    }

    /// Snapshot the state now and return a future that writes that snapshot
    /// to the `.ranges` record on `spawn_blocking`, off the runtime workers.
    /// The snapshot is taken at the call, not when the future is first polled.
    fn write_record_blocking(
        &self,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send + 'static {
        let snapshot = self.snapshot();
        let ranges_path = self.ranges_path.clone();
        async move {
            tokio::task::spawn_blocking(move || write_record(&ranges_path, &snapshot))
                .await
                .map_err(io::Error::other)?
        }
    }

    /// Persist the current in-memory state to the `.ranges` record. The
    /// single-writer flush point: callers (the scheduler's flush owner for
    /// multi-source fetches, and `finalize`, and the end of single-source
    /// `drive`) invoke this so no two writers race the record file. `present`
    /// only ever grows and is unioned under the mutex AFTER the data fsync (the
    /// `sync_and_union` helper's ordering), so the persisted record never
    /// claims a range that is not durably on disk.
    ///
    /// # Errors
    ///
    /// The record's tempfile-plus-rename write fails.
    pub fn flush_present_record(&self) -> io::Result<()> {
        write_record(&self.ranges_path, &self.snapshot())
    }
}

impl ClientRangedStore {
    /// A guard that persists the present-range record if it drops before
    /// [`FlushOnDrop::disarm`].
    ///
    /// A fetch flushes the record when it returns, on success and on failure. A
    /// fetch future that is dropped instead (a caller's Ctrl-C) never gets there,
    /// so the ranges that landed since the last periodic flush would be fetched
    /// and paid for again on resume. Arm this before awaiting the fetch and
    /// disarm it once the fetch returns.
    pub(crate) const fn flush_on_drop(&self) -> FlushOnDrop<'_> {
        FlushOnDrop { store: Some(self) }
    }
}

/// See [`ClientRangedStore::flush_on_drop`].
pub(crate) struct FlushOnDrop<'a> {
    /// The store to flush on drop; `None` once disarmed.
    store: Option<&'a ClientRangedStore>,
}

impl FlushOnDrop<'_> {
    /// The fetch returned and flushed on its own path: dropping does nothing.
    pub(crate) fn disarm(mut self) {
        self.store = None;
    }
}

impl Drop for FlushOnDrop<'_> {
    /// A synchronous write on the dropping thread. A periodic flush still
    /// running on a blocking thread cannot corrupt it: each write renames its
    /// own temp file into place, and `present` only grows, so the worst case is
    /// an older snapshot landing last, which under-claims.
    fn drop(&mut self) {
        if let Some(store) = self.store.take()
            && let Err(error) = store.flush_present_record()
        {
            tracing::warn!(
                %error,
                "could not persist the present-range record of a dropped fetch; its \
                 unrecorded ranges are fetched again on resume"
            );
        }
    }
}

/// The verified leaves one ingest checkpoint writes, as `(byte offset, data)`.
#[derive(Default)]
struct FlushBatch {
    leaves: Vec<(u64, Bytes)>,
}

impl FlushBatch {
    const fn is_empty(&self) -> bool {
        self.leaves.is_empty()
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
    /// Test-only record of each queued checkpoint's prefix end.
    #[cfg(test)]
    checkpoint_ends: Arc<Mutex<Vec<u64>>>,
}

impl IngestFlusher {
    /// Open `store`'s `.partial` data file for writing, on `spawn_blocking`,
    /// for the ingest of `range` (aligned under `claim`) verified under
    /// `claim`.
    async fn open(
        store: &ClientRangedStore,
        range: &AlignedRange,
        claim: u64,
    ) -> anyhow::Result<Self> {
        let path = store
            .data_path
            .lock()
            .map_err(|_| anyhow::Error::new(lock_poisoned("data_path")))?
            .clone();
        let data = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
        })
        .await??;
        let pipeline = IngestPipeline {
            state: Mutex::new(PipelineState::default()),
            data: Mutex::new(data),
            store: Arc::clone(&store.state),
            present_grew: Arc::clone(&store.present_grew),
            claim,
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
            #[cfg(test)]
            checkpoint_ends: Arc::clone(&store.checkpoint_ends),
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
        #[cfg(test)]
        if let Ok(mut ends) = self.checkpoint_ends.lock() {
            ends.push(received_end);
        }
        let waiting_since = tokio::time::Instant::now();
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|e| anyhow::anyhow!("ingest checkpoint slots closed: {e}"))?;
        // While ingest waits here, its stream is not read, and the pull's
        // throughput floor still counts the time (#2211).
        let waited = waiting_since.elapsed();
        if waited >= CHECKPOINT_WAIT_LOG_FLOOR {
            tracing::debug!(
                received_end,
                waited_ms = waited.as_millis(),
                "ingest waited on a checkpoint slot"
            );
        }
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

/// The checkpoint queue and open data file of one
/// [`ClientRangedStore::ingest_stream`] call, shared by its decode loop and
/// its checkpoint worker.
///
/// A worker runs on `spawn_blocking` only while checkpoints are queued. It
/// takes them in order and exits when the queue is empty; the next queued
/// checkpoint starts a new one. The queue and the running flag share one
/// lock, so a checkpoint is never left queued with no worker to take it, and
/// at most one worker runs at a time. That keeps `present` a contiguous
/// prefix of `range`. A worker that stays parked for the whole stream would
/// instead hold a blocking-pool thread per ingest and stop a paused test clock
/// from advancing.
struct IngestPipeline {
    state: Mutex<PipelineState>,
    /// The `.partial` data file. Locked by the running worker for its whole
    /// run; never by the decode loop.
    data: Mutex<File>,
    /// The store's state, which each durable checkpoint extends.
    store: Arc<Mutex<StoreState>>,
    /// The store's [`ClientRangedStore::present_grew`], woken after each
    /// durable checkpoint.
    present_grew: Arc<tokio::sync::Notify>,
    /// The size the leg's sender claims, which the leg verifies under.
    claim: u64,
    /// The leg's range, aligned under `claim`.
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
            .map_err(|_| anyhow::Error::new(lock_poisoned("ingest pipeline")))
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
        let mut data = self
            .data
            .lock()
            .map_err(|_| anyhow::Error::new(lock_poisoned("ingest data file")))?;
        let mut slots = Vec::with_capacity(cap);
        while let Some(first) = self.pop(true)? {
            let mut received_end = write_checkpoint(&mut data, first, &mut slots)?;
            while slots.len() < cap {
                let Some(next) = self.pop(false)? else {
                    break;
                };
                received_end = received_end.max(write_checkpoint(&mut data, next, &mut slots)?);
            }
            #[cfg(test)]
            {
                std::thread::sleep(self.fsync_delay);
                self.fsyncs
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            sync_and_union(&data, &self.store, self.claim, &self.range, received_end)?;
            self.present_grew.notify_waiters();
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
    data: &mut File,
    checkpoint: Checkpoint,
    slots: &mut Vec<tokio::sync::OwnedSemaphorePermit>,
) -> anyhow::Result<u64> {
    let Checkpoint {
        batch,
        received_end,
        slot,
    } = checkpoint;
    write_batch(data, &batch)?;
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
/// offsets. Nothing is durable, and `present` is unchanged, until
/// [`sync_and_union`] runs.
fn write_batch(data: &mut File, batch: &FlushBatch) -> anyhow::Result<()> {
    for (offset, bytes) in &batch.leaves {
        data.write_all_at(*offset, bytes)
            .with_context(|| format!("writing .partial data at offset {offset}"))?;
    }
    Ok(())
}

/// Durably checkpoint the prefix `[range.fetch_start(), received_end)` of
/// an in-progress [`ClientRangedStore::ingest_stream`] whose batches
/// [`write_batch`] has written: fsync the data file, THEN union the
/// corresponding chunk ranges into `present`. The fsync-before-union
/// ordering is load-bearing: see the durability contract on
/// [`ClientRangedStore::ingest_stream`].
///
/// A prefix that reaches `claim` holds the final chunk of the claimed tree,
/// verified: the union also proves `claim`, setting `proven` and the bound to
/// it. A verified final chunk outranks an earlier proven size, which only a
/// record some other blob left can hold, so the new proof replaces it.
///
/// `sync_data` (fdatasync) is enough: it persists the written blocks and
/// every metadata change needed to read them back, including the block
/// allocation of a sparse write and the size growth of the `.partial` file.
/// It skips only timestamps, which nothing here reads.
///
/// Does NOT persist the `.ranges` record — that is
/// [`ClientRangedStore::flush_present_record`]'s job. Several `ingest_stream`
/// calls can run concurrently on one store (the multi-source scheduler), so
/// writing the record here, per checkpoint, out of the state lock would both
/// race the record's write-and-rename across sources and serialize every
/// source on the record's fsync. `present` only ever grows and is unioned
/// under the mutex AFTER the data fsync, so whenever the record is next
/// flushed it never claims a range that is not durably on disk.
fn sync_and_union(
    data: &File,
    store: &Mutex<StoreState>,
    claim: u64,
    range: &AlignedRange,
    received_end: u64,
) -> anyhow::Result<()> {
    data.sync_data().context("fsyncing .partial data")?;

    // `align_range` reads a zero length as "to the end of the blob", so an
    // empty prefix must not reach it. It adds nothing to `present`.
    if received_end <= range.fetch_start() {
        return Ok(());
    }
    let received = decdn_bao_range::align_range(
        range.fetch_start(),
        received_end.saturating_sub(range.fetch_start()),
        claim,
    )?;

    let mut state = store.lock().unwrap_or_else(PoisonError::into_inner);
    state.present |= received.chunk_ranges().clone();
    if received_end >= claim {
        if let Some(earlier) = state.proven.filter(|&p| p != claim) {
            tracing::warn!(
                earlier_proven = earlier,
                proven = claim,
                "a verified final chunk proves another size than the store held"
            );
        }
        state.prove(claim);
    }
    Ok(())
}

/// The assembled bao body `admit` streams through the ingest loop. An
/// in-memory buffer has no out-of-band failure mode: whatever the decoder says
/// about it is the whole story.
struct AdmitBody(Bytes);

impl iroh_io::AsyncStreamReader for AdmitBody {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.0.read_bytes(len).await
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        self.0.read::<L>().await
    }
}

impl StashedFault for AdmitBody {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

impl RangedStore for ClientRangedStore {
    /// The current bound: the planner's size until a leg proves one.
    fn total_bytes(&self) -> u64 {
        self.bound()
    }

    fn present_ranges(&self) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move { Ok(self.snapshot().present) })
    }

    /// An end past the bound clamps to it, as on the node's store: the bound
    /// can shrink under a caller that read it first. A start at or past the
    /// bound is an alignment error.
    fn missing_ranges(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move {
            let state = self.snapshot();
            let aligned = decdn_bao_range::align_range_clamped(byte_offset, byte_len, state.bound)?;
            Ok(aligned.chunk_ranges().clone() - &state.present)
        })
    }

    /// Verify `bao_bytes` under the size its own header claims and write it
    /// through [`ClientRangedStore::ingest_stream`], then persist the record.
    /// Idempotent: re-admitting an already-present range re-verifies and
    /// re-writes the same bytes, and the union is a no-op.
    fn admit(&self, range: AlignedRange, bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            // `bao_bytes` is the combined wire format `encode_verified_range`
            // produces (and the shared conformance suite feeds every
            // backend): an 8-byte little-endian size header, the sender's
            // claim, followed by the interleaved bao body.
            let short = || {
                RangedStoreError::Backend(
                    "admit: bao_bytes shorter than the 8-byte combined-format header".into(),
                )
            };
            let header: [u8; 8] = bao_bytes
                .get(..8)
                .and_then(|h| h.try_into().ok())
                .ok_or_else(short)?;
            let claim = u64::from_le_bytes(header);
            let body = bao_bytes.slice(8..);

            self.ingest_stream(&range, AdmitBody(body), None, claim)
                .await
                .map_err(|e| RangedStoreError::Backend(e.into()))?;
            self.write_record_blocking().await.map_err(backend)
        })
    }

    /// The bytes of `[byte_offset, byte_offset + byte_len)`, with an end past
    /// the bound clamped to it ([`Self::missing_ranges`]).
    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, Bytes> {
        Box::pin(async move {
            let state = self.snapshot();
            let aligned = decdn_bao_range::align_range_clamped(byte_offset, byte_len, state.bound)?;
            if !(aligned.chunk_ranges().clone() - &state.present).is_empty() {
                return Err(RangedStoreError::Backend(
                    "requested range not present".into(),
                ));
            }

            let read_start = byte_offset;
            let room = state.bound.saturating_sub(byte_offset);
            let read_len = if byte_len == 0 {
                room
            } else {
                byte_len.min(room)
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

    /// A size is proven and every byte of `[0, proven)` is present.
    fn is_complete(&self) -> RangedFuture<'_, bool> {
        Box::pin(async move {
            let state = self.snapshot();
            Ok(state
                .proven
                .is_some_and(|proven| (whole(proven) - &state.present).is_empty()))
        })
    }

    /// Promote a complete `.partial` to its final path once the BLAKE3 of
    /// `[0, proven)` equals the root. Every present leaf verified as it
    /// landed, so the hash is a whole-file defense against on-disk drift and
    /// against a partial some other blob left at this path.
    ///
    /// On a mismatch, including a data file shorter than the proven size, it
    /// returns [`crate::HashMismatch`] and keeps the partial file and its
    /// bound, and claims none of it present or proven: the hash cannot say
    /// which bytes are bad, so the next drive fetches the blob again over them
    /// rather than wedging on a record that claims them.
    fn finalize(&self) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            // Already finalized, e.g. this store was reconstructed by
            // `open()` from the promoted final file. Match the same
            // ".partial"-suffix check the promote below uses: no `.partial`
            // suffix on the current data path means there is nothing left to
            // hash or promote.
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

            // Fast path: the store is not complete, so there is nothing to
            // hash or promote.
            let state = self.snapshot();
            let Some(proven) = state.proven else {
                return Err(RangedStoreError::Incomplete);
            };
            if !(whole(proven) - &state.present).is_empty() {
                return Err(RangedStoreError::Incomplete);
            }

            // Flush the in-memory state to the `.ranges` record before the
            // hash: ingest checkpoints never persist the record themselves, so
            // this is the single-writer point that makes the on-disk record
            // current. A crash right after this and before promotion still
            // leaves an accurate record to resume from.
            self.write_record_blocking().await.map_err(backend)?;

            let root = self.root;
            let ranges_path = self.ranges_path.clone();
            let data_path = Arc::clone(&self.data_path);

            let promoted =
                tokio::task::spawn_blocking(move || -> Result<bool, RangedStoreError> {
                    let current_path = data_path
                        .lock()
                        .map_err(|_| lock_poisoned("data_path"))?
                        .clone();
                    let data_file = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&current_path)
                        .map_err(backend)?;

                    if !prefix_matches(&data_file, proven, root).map_err(backend)? {
                        return Ok(false);
                    }

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

                    // A data file sized past the proven end (a caller that
                    // pre-sized it to a larger hint) holds nothing past it.
                    if data_file.metadata().map_err(backend)?.len() > proven {
                        data_file.set_len(proven).map_err(backend)?;
                    }
                    // Durability before the rename: the promoted file must contain
                    // every byte it claims to.
                    data_file.sync_all().map_err(backend)?;
                    drop(data_file);
                    std::fs::rename(&current_path, &final_path).map_err(backend)?;

                    {
                        let mut guard = data_path.lock().map_err(|_| lock_poisoned("data_path"))?;
                        *guard = final_path;
                    }

                    // Record cleanup is best-effort: a promoted blob is valid
                    // without it, so a delete failure must not fail a successful
                    // promote.
                    let _ = std::fs::remove_file(&ranges_path);
                    Ok(true)
                })
                .await
                .map_err(backend)??;

            if promoted {
                return Ok(());
            }
            {
                let mut state = self.lock_state();
                *state = StoreState::empty(state.bound);
            }
            self.write_record_blocking().await.map_err(backend)?;
            Err(RangedStoreError::Backend(Box::new(crate::HashMismatch)))
        })
    }
}

impl crate::source::IngestStore for ClientRangedStore {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a AtomicU64>,
    ) -> crate::source::IngestFuture<'a, R>
    where
        R: crate::source::BaoRangeReader + 'a,
    {
        Box::pin(self.ingest_stream_until(range, reader, on_progress, claimed_total, stop_at))
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        let write = self.write_record_blocking();
        Box::pin(async move { Ok(write.await?) })
    }

    fn proven(&self) -> Option<u64> {
        Self::proven(self)
    }

    fn set_bound(&self, bound: u64) {
        Self::set_bound(self, bound);
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests;
