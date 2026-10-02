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
            .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("data_path")))?
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
        let mut data = self
            .data
            .lock()
            .map_err(|_| anyhow::anyhow!("{}", lock_poisoned("ingest data file")))?;
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

    /// Write `state` as a record and read it back.
    fn round_trip(name: &str, state: &StoreState) -> StoreState {
        let dir = tmp_dir();
        let path = dir.path().join(name);
        write_record(&path, state).expect("write");
        read_record(&path).expect("read")
    }

    #[test]
    fn record_round_trip_empty() {
        let state = StoreState::empty(10);
        assert_eq!(round_trip("empty.ranges", &state), state);
    }

    #[test]
    fn record_round_trip_single_range() {
        let mut state = StoreState::empty(9 * 1024);
        state.present = ChunkRanges::from(ChunkNum(2)..ChunkNum(9));
        assert_eq!(round_trip("single.ranges", &state), state);
    }

    #[test]
    fn record_round_trip_disjoint_ranges_and_a_proven_size() {
        let mut state = StoreState::empty(40 * 1024);
        state.present = ChunkRanges::from(ChunkNum(0)..ChunkNum(3));
        state.present |= ChunkRanges::from(ChunkNum(10)..ChunkNum(15));
        state.prove(15 * 1024);
        assert_eq!(round_trip("disjoint.ranges", &state), state);
    }

    #[test]
    fn record_rejects_odd_boundary_count() {
        let dir = tmp_dir();
        let path = dir.path().join("odd.ranges");
        let json = serde_json::json!({ "bound": 10, "proven": null, "boundaries": [1, 2, 3] });
        std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
        let err = read_record(&path).expect_err("odd count must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // --- store query methods ---

    const GROUP: u64 = decdn_bao_range::CHUNK_GROUP_BYTES;

    fn store_with_present(total_bytes: u64, present: ChunkRanges) -> ClientRangedStore {
        let dir = tmp_dir();
        let store =
            ClientRangedStore::create(dir.path(), "blob", [7u8; 32], total_bytes).expect("create");
        store.state.lock().expect("lock").present = present;
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
    async fn total_bytes_reports_the_bound() {
        let store = store_with_present(3 * GROUP, ChunkRanges::empty());
        assert_eq!(store.total_bytes(), 3 * GROUP);
        store.set_bound(5 * GROUP);
        assert_eq!(store.total_bytes(), 5 * GROUP);
        assert_eq!(store.proven(), None);
    }

    /// A proven size is final: moving the bound after a proof does nothing.
    #[tokio::test]
    async fn set_bound_is_a_no_op_once_a_size_is_proven() {
        let store = store_with_present(3 * GROUP, ChunkRanges::empty());
        store.state.lock().expect("lock").prove(2 * GROUP);
        store.set_bound(9 * GROUP);
        assert_eq!(store.bound(), 2 * GROUP);
        assert_eq!(store.proven(), Some(2 * GROUP));
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

    /// Complete means a proven size with every byte of it present.
    #[tokio::test]
    async fn is_complete_false_then_true() {
        let total = 2 * GROUP;
        let store = store_with_present(total, ChunkRanges::empty());
        assert!(!store.is_complete().await.expect("is_complete false"));

        store.state.lock().expect("lock").present = whole(total);
        assert!(
            !store.is_complete().await.expect("is_complete unproven"),
            "every byte of an unproven bound is not complete"
        );

        store.state.lock().expect("lock").prove(total);
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

    /// An end past the bound clamps to it, as on the node's store: the bound
    /// can shrink under a caller that read it first. Only a start at or past
    /// the bound is an alignment error.
    #[tokio::test]
    async fn an_end_past_the_bound_clamps() {
        let total = 2 * GROUP;
        let store = store_with_present(total, whole(total));
        let data: Vec<u8> = (0..total)
            .map(|i| u8::try_from(i % 251).expect("fits"))
            .collect();
        write_plaintext(&store, &data);

        let missing = store
            .missing_ranges(GROUP, 4 * GROUP)
            .await
            .expect("an end past the bound clamps");
        assert!(missing.is_empty());
        let got = store.read(GROUP, 4 * GROUP).await.expect("read clamps");
        assert_eq!(got.as_ref(), &data[usize::try_from(GROUP).expect("fits")..]);
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

        let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open finalized");

        assert!(
            reopened.is_complete().await.expect("is_complete"),
            "reopened store must be complete"
        );
        assert_eq!(
            reopened.proven(),
            Some(total),
            "the final file proves its length"
        );
        assert_eq!(reopened.bound(), total);
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
        // deleting the record, mirroring a crash between finalize's rename
        // and its best-effort record cleanup.
        let partial_path = store.data_path.lock().expect("lock").clone();
        let final_path = dir.path().join("blob");
        std::fs::rename(&partial_path, &final_path).expect("simulate promote rename");
        let ranges_path = store.ranges_path.clone();
        assert!(ranges_path.exists());
        drop(store);

        let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open post-crash");

        assert!(
            reopened.is_complete().await.expect("is_complete"),
            "reopened store must be complete"
        );
        let got = reopened.read(0, total).await.expect("read");
        assert_eq!(got.as_ref(), plaintext.as_slice());

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

        let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open finalized");

        // A second `finalize` on the promoted file is a no-op.
        reopened
            .finalize()
            .await
            .expect("finalize on already-finalized store is a no-op Ok");

        let got = reopened.read(0, total).await.expect("read still works");
        assert_eq!(got.as_ref(), plaintext.as_slice());
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
        let store = ClientRangedStore::open(dir.path(), "blob", root).expect("open seeded");
        assert_eq!(store.bound(), total);
        assert_eq!(
            store.proven(),
            None,
            "a prefix without the final chunk proves nothing"
        );

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
        let store = ClientRangedStore::open(dir.path(), "blob", root).expect("open seeded");

        assert_eq!(
            store.proven(),
            Some(total),
            "a complete seed proves its length"
        );
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

        // The seeded data hashes to the root, so `finalize` promotes.
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

    /// An ingest whose end is lowered stops once its verified prefix reaches
    /// it: the prefix is present, nothing past it is, and the wire it read is
    /// exactly the encoding of the shorter range, since a group-aligned end
    /// cuts a pre-order bao encoding between two items. An end at or past the
    /// range's end drains the range as usual.
    #[tokio::test]
    async fn ingest_stream_until_stops_at_a_lowered_end() -> anyhow::Result<()> {
        let total = 11 * GROUP + 321;
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
        let range = decdn_bao_range::align_range(GROUP, 9 * GROUP, total)?;
        let body = bao_for(root, &plaintext, outboard, &range).slice(8..);
        for stop in 2..10 {
            let store = fresh_store(root, total);
            let end = AtomicU64::new(stop * GROUP);
            let (rest, ended) = store
                .ingest_stream_until(&range, body.clone(), None, total, Some(&end))
                .await?;
            let kept = decdn_bao_range::align_range(GROUP, (stop - 1) * GROUP, total)?;
            assert_eq!(ended, IngestEnd::Stopped, "stop at group {stop}");
            assert_eq!(
                &store.present_ranges().await?,
                kept.chunk_ranges(),
                "stop at group {stop}"
            );
            assert_eq!(
                (body.len() - rest.len()) as u64,
                kept.wire_len(),
                "the wire read up to group {stop} is the shorter range's encoding"
            );
        }

        let store = fresh_store(root, total);
        let end = AtomicU64::new(10 * GROUP);
        let (rest, ended) = store
            .ingest_stream_until(&range, body.clone(), None, total, Some(&end))
            .await?;
        assert_eq!(
            ended,
            IngestEnd::Drained,
            "an end at the range's end drains"
        );
        assert!(rest.is_empty());
        assert_eq!(&store.present_ranges().await?, range.chunk_ranges());
        Ok(())
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
        let reader = store.ingest_stream(&aligned, body, None, total).await?;
        assert_eq!(store.proven(), None, "a prefix proves no size");
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
        let reader = store
            .ingest_stream(&rest_aligned, rest_body, None, total)
            .await?;
        assert_eq!(store.proven(), Some(total), "the tail proves the size");
        drop(reader);
        store.finalize().await.expect("finalize promotes");
        let final_bytes = store.read(0, total).await?;
        assert_eq!(final_bytes.as_ref(), plaintext.as_slice());

        Ok(())
    }

    /// A fault after a range's first parents and before its first leaf marks
    /// nothing present. No leaf verified, so the received prefix is empty, not
    /// the rest of the blob.
    #[tokio::test]
    async fn ingest_stream_fault_before_the_first_leaf_marks_nothing_present() -> anyhow::Result<()>
    {
        let total: u64 = 32 * GROUP;
        let plaintext: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        // The wire of a range deep in the tree opens with its parents: 64 bytes
        // is one parent pair and no leaf.
        let source = crate::source::ScriptedSource::new(plaintext)?
            .with_fault_after(64, || anyhow::anyhow!("scripted reset"));
        let root = source.root();
        let store_dir = tmp_dir();
        let store = ClientRangedStore::create(store_dir.path(), "blob", root, total)?;
        let aligned = decdn_bao_range::align_range(4 * GROUP, 4 * GROUP, total)?;
        let (_header, reader) = {
            use crate::source::BlobSource;
            source.open(root, aligned.clone()).await?
        };

        let err = store
            .ingest_stream(&aligned, reader, None, total)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("the fault must surface as an error"))?;
        assert!(format!("{err:#}").contains("scripted reset"), "{err:#}");
        assert!(
            store.present_ranges().await?.is_empty(),
            "no leaf landed, so nothing is present"
        );
        assert!(!store.is_complete().await?);
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
            .ingest_stream(&aligned, reader, None, total)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("mid-gap fault must surface as an error"))?;
        assert!(
            err.to_string().contains("scripted mid-gap stall")
                || format!("{err:?}").contains("scripted mid-gap stall"),
            "the parked typed fault must survive: {err}"
        );

        // Checkpoints do not persist the `.ranges` record themselves (the
        // single-writer flush point): a real caller reaches this
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
        let reopened = ClientRangedStore::open(store_dir.path(), "blob", root)?;
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

        let total = data.len() as u64;
        let a = store.ingest_stream(&lo, scripted_reader_for(&data, &lo)?, None, total);
        let b = store.ingest_stream(&hi, scripted_reader_for(&data, &hi)?, None, total);
        let (ra, rb) = tokio::join!(a, b);
        ra?;
        rb?;

        store.flush_present_record()?;

        let on_disk = read_record(store.ranges_path())?;
        let expected = lo.chunk_ranges().clone() | hi.chunk_ranges().clone();
        assert_eq!(on_disk.present, expected);
        assert_eq!(on_disk.proven, Some(total), "the high half holds the tail");
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
            .ingest_stream(&aligned, body, None, total)
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
            async move {
                store
                    .ingest_stream(&aligned, wire, None, total)
                    .await
                    .map(drop)
            }
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
    /// whose fsync stalls: the first four checkpoints of the ramp fit the
    /// pipeline slots plus the batch in progress, so the whole blob is
    /// received before the first fsync lands. `present` extends only after
    /// the fsync.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_stream_decodes_past_a_slow_fsync() -> anyhow::Result<()> {
        const FSYNC_DELAY: Duration = Duration::from_secs(3);

        let dir = tempfile::tempdir()?;
        let slots = ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS;
        let data = blob(usize::try_from(ramp_sum(slots + 1))?);
        let total = u64::try_from(data.len())?;
        let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, FSYNC_DELAY)?;

        // When progress first reached `total`: the elapsed time, and whether
        // `present` was still empty then.
        let at_total: Mutex<Option<(Duration, bool)>> = Mutex::new(None);
        let started = std::time::Instant::now();
        let on_progress = |received: u64| {
            if received == total {
                let empty = store.state.lock().is_ok_and(|s| s.present.is_empty());
                if let Ok(mut slot) = at_total.lock() {
                    slot.get_or_insert((started.elapsed(), empty));
                }
            }
        };
        store
            .ingest_stream(&aligned, wire, Some(&on_progress), total)
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
            let durable = store
                .state
                .lock()
                .map_or(0, |s| ranges_byte_len(&s.present));
            max_undurable.fetch_max(received.saturating_sub(durable), Ordering::Relaxed);
        };
        store
            .ingest_stream(&aligned, wire, Some(&on_progress), aligned.blob_size())
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

    /// Checkpoints that queue behind a slow fsync share the next fsync. The
    /// first three checkpoints of the ramp fit the pipeline slots, so the
    /// loop queues all three without waiting. The first fsync holds at least
    /// the first one; the rest queue during its stall and the worker folds
    /// them into one second fsync. One fsync per checkpoint would be three.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_stream_coalesces_queued_checkpoints() -> anyhow::Result<()> {
        let slots = ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS;

        let dir = tempfile::tempdir()?;
        let data = blob(usize::try_from(ramp_sum(slots))?);
        let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, Duration::from_secs(1))?;

        store
            .ingest_stream(&aligned, wire, None, aligned.blob_size())
            .await?;

        let fsyncs = store.fsyncs.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            (1..=2).contains(&fsyncs),
            "{slots} queued checkpoints took {fsyncs} fsyncs, want at most 2"
        );
        assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
        Ok(())
    }

    /// The batch size of each checkpoint of one call, in order, up to `n` of
    /// them: the ramp from `INGEST_FIRST_CHECKPOINT_BYTES` to
    /// `INGEST_CHECKPOINT_BYTES`.
    fn ramp(n: usize) -> Vec<u64> {
        std::iter::successors(
            Some(ClientRangedStore::INGEST_FIRST_CHECKPOINT_BYTES),
            |&b| Some(ClientRangedStore::next_checkpoint_bytes(b)),
        )
        .take(n)
        .collect()
    }

    /// The sum of the first `n` ramp steps.
    fn ramp_sum(n: usize) -> u64 {
        ramp(n).iter().sum()
    }

    /// Each call's checkpoints start small and double up to
    /// `INGEST_CHECKPOINT_BYTES`, so the first verified bytes are durable,
    /// and readable, after one small batch rather than a full interval.
    #[tokio::test]
    async fn ingest_stream_ramps_its_checkpoints() -> anyhow::Result<()> {
        let sizes = ramp(8);
        assert_eq!(sizes.first(), Some(&(64 * KIB)));
        assert_eq!(
            sizes.last(),
            Some(&ClientRangedStore::INGEST_CHECKPOINT_BYTES)
        );

        // Five ramp steps, then a tail shorter than the sixth.
        let total = ramp_sum(5) + 1024 * KIB;
        let data = blob(usize::try_from(total)?);
        let (root, _) = bao_root_and_outboard(&data);
        let dir = tempfile::tempdir()?;
        let store = ClientRangedStore::create(dir.path(), "b", root, total)?;
        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let wire = scripted_reader_for(&data, &aligned)?;
        store.ingest_stream(&aligned, wire, None, total).await?;

        let mut want: Vec<u64> = (1..=5).map(ramp_sum).collect();
        want.push(total);
        let ends = store
            .checkpoint_ends
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?
            .clone();
        assert_eq!(ends, want);
        Ok(())
    }

    /// A waiter on `present_grew` wakes when a checkpoint extends `present`,
    /// with no other signal.
    #[tokio::test]
    async fn a_checkpoint_wakes_present_waiters() -> anyhow::Result<()> {
        let total = 256 * KIB;
        let data = blob(usize::try_from(total)?);
        let (root, _) = bao_root_and_outboard(&data);
        let dir = tempfile::tempdir()?;
        let store = ClientRangedStore::create(dir.path(), "b", root, total)?;
        let aligned = decdn_bao_range::align_range(0, 0, total)?;
        let wire = scripted_reader_for(&data, &aligned)?;

        let grew = store.present_grew().notified();
        tokio::pin!(grew);
        grew.as_mut().enable();
        store.ingest_stream(&aligned, wire, None, total).await?;
        tokio::time::timeout(Duration::from_secs(1), grew)
            .await
            .map_err(|_| anyhow::anyhow!("no wakeup when present grew"))?;
        Ok(())
    }

    // --- offset-keyed store: each leg verifies under its own claim ---

    const KIB: u64 = 1024;

    /// Open `source` for `range`: the size its header signs, and the reader of
    /// its wire.
    async fn open_leg(
        source: &crate::source::ScriptedSource,
        range: &AlignedRange,
    ) -> anyhow::Result<(u64, crate::source::ScriptedReader)> {
        use crate::source::BlobSource;
        let (header, reader) = source.open(source.root(), range.clone()).await?;
        Ok((header.total_bytes, reader))
    }

    /// Mark `present` as verified, bypassing ingest.
    fn set_present(store: &ClientRangedStore, present: ChunkRanges) {
        store.state.lock().expect("lock").present = present;
    }

    /// A hint below the true size: the leg that covers the true tail verifies
    /// under its own claim, proves that size, and moves the bound to it.
    #[tokio::test]
    async fn a_leg_proves_its_claimed_size_and_sets_the_bound() -> anyhow::Result<()> {
        let truth = 1200 * KIB;
        let source = crate::source::ScriptedSource::new(blob(usize::try_from(truth)?))?;
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", source.root(), 1000 * KIB)?;
        assert_eq!(store.bound(), 1000 * KIB);
        assert_eq!(store.proven(), None);

        let tail = decdn_bao_range::align_range(1000 * KIB, 200 * KIB, truth)?;
        let (claim, reader) = open_leg(&source, &tail).await?;
        assert_eq!(claim, truth);
        store.ingest_stream(&tail, reader, None, claim).await?;

        assert_eq!(store.proven(), Some(truth));
        assert_eq!(store.bound(), truth);
        assert_eq!(store.total_bytes(), truth);
        assert_eq!(&store.present_ranges().await?, tail.chunk_ranges());
        Ok(())
    }

    /// A hint above the true size: the planner aligns the leg under its own
    /// bound, the node serves up to the blob's end, and the leg verifies under
    /// the node's claim. It lands genuine bytes and proves the smaller size.
    #[tokio::test]
    async fn a_leg_under_a_different_claim_lands_genuine_bytes() -> anyhow::Result<()> {
        let truth = 1024 * KIB;
        let data = blob(usize::try_from(truth)?);
        let source = crate::source::ScriptedSource::new(data.clone())?;
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", source.root(), 2048 * KIB)?;

        let leg = decdn_bao_range::align_range(0, truth, 2048 * KIB)?;
        let (claim, reader) = open_leg(&source, &leg).await?;
        assert_eq!(claim, truth);
        store.ingest_stream(&leg, reader, None, claim).await?;

        assert_eq!(store.proven(), Some(truth));
        assert_eq!(store.bound(), truth);
        assert!(store.is_complete().await?);
        assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
        Ok(())
    }

    /// A node that signs one byte more than the blob holds: its tail leg cannot
    /// verify under that claim, so nothing lands and nothing is proven.
    #[tokio::test]
    async fn a_lying_claim_on_the_final_chunk_is_rejected() -> anyhow::Result<()> {
        let truth = 3 * GROUP + 100;
        let source = crate::source::ScriptedSource::new(blob(usize::try_from(truth)?))?
            .signing_size(truth + 1);
        let store = fresh_store(source.root(), truth);

        let tail = decdn_bao_range::align_range(3 * GROUP, 0, truth)?;
        let (claim, reader) = open_leg(&source, &tail).await?;
        assert_eq!(claim, truth + 1);
        let result = store.ingest_stream(&tail, reader, None, claim).await;

        assert!(result.is_err(), "a lying final-chunk claim must not verify");
        assert!(store.present_ranges().await?.is_empty());
        assert_eq!(store.proven(), None);
        assert_eq!(store.bound(), truth);
        Ok(())
    }

    /// A resumed store keeps the bound its record holds, whatever the caller
    /// now hints, and discards none of the partial.
    #[tokio::test]
    async fn the_record_wins_over_a_new_hint() -> anyhow::Result<()> {
        let data = blob(40_000);
        let (root, _) = bao_root_and_outboard(&data);
        let dir = tmp_dir();
        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, GROUP)?;
        let partial = dir.path().join("blob.partial");
        let before = std::fs::read(&partial)?;

        let store = ClientRangedStore::open_or_create(dir.path(), "blob", root, 80_000)?;

        assert_eq!(store.bound(), 40_000);
        assert_eq!(store.proven(), None);
        assert_eq!(
            &store.present_ranges().await?,
            decdn_bao_range::align_range(0, GROUP, 40_000)?.chunk_ranges()
        );
        assert_eq!(std::fs::read(&partial)?, before, "nothing is discarded");
        Ok(())
    }

    /// Every byte of the bound present is not enough: `finalize` needs a
    /// proven size, and keeps the partial without one.
    #[tokio::test]
    async fn finalize_requires_a_proven_size() -> anyhow::Result<()> {
        let total = 2 * GROUP;
        let data = blob(usize::try_from(total)?);
        let (root, _) = bao_root_and_outboard(&data);
        let store = fresh_store(root, total);
        write_plaintext(&store, &data);
        set_present(
            &store,
            ChunkRanges::from(ChunkNum(0)..ChunkNum::full_chunks(total)),
        );

        assert!(!store.is_complete().await?);
        let err = store
            .finalize()
            .await
            .expect_err("an unproven size must not finalize");
        assert!(matches!(err, RangedStoreError::Incomplete), "{err:?}");
        let data_path = store.data_path.lock().expect("lock").clone();
        assert!(data_path.to_string_lossy().ends_with(".partial"));
        assert!(data_path.exists());
        Ok(())
    }

    /// `finalize` hashes the whole file against the root: a byte that changed
    /// on disk after ingest fails it. It keeps the partial file and the bound
    /// but claims nothing present, so the next ingest fetches over the bad
    /// bytes, and `finalize` then promotes.
    #[tokio::test]
    async fn finalize_hashes_the_whole_file() -> anyhow::Result<()> {
        let total = 2 * GROUP + 123;
        let data = blob(usize::try_from(total)?);
        let source = crate::source::ScriptedSource::new(data.clone())?;
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", source.root(), total)?;
        let whole = decdn_bao_range::align_range(0, 0, total)?;
        let (claim, reader) = open_leg(&source, &whole).await?;
        store.ingest_stream(&whole, reader, None, claim).await?;
        assert_eq!(store.proven(), Some(total));

        let partial = dir.path().join("blob.partial");
        let mut flipped = data.clone();
        flipped[usize::try_from(GROUP)?] ^= 0xFF;
        std::fs::write(&partial, &flipped)?;
        let err = store
            .finalize()
            .await
            .expect_err("a whole-file hash mismatch must fail");
        assert!(matches!(err, RangedStoreError::Backend(_)), "{err:?}");
        assert!(partial.exists(), "a failed finalize keeps the partial");
        assert_eq!(store.bound(), total, "a failed finalize keeps the bound");
        assert_eq!(store.proven(), None);
        assert!(store.present_ranges().await?.is_empty());
        let record = read_record(&dir.path().join("blob.partial.ranges"))?;
        assert_eq!(
            record,
            StoreState::empty(total),
            "the record drops the claim"
        );

        let (claim, reader) = open_leg(&source, &whole).await?;
        store.ingest_stream(&whole, reader, None, claim).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("blob"))?, data);
        assert!(!partial.exists());
        assert!(!dir.path().join("blob.partial.ranges").exists());
        Ok(())
    }

    /// The empty blob is proven only against the empty root.
    #[tokio::test]
    async fn an_empty_blob_is_proven_only_against_the_empty_root() -> anyhow::Result<()> {
        let empty = decdn_bao_range::align_range(0, 0, 0)?;

        let good = fresh_store(*blake3::hash(&[]).as_bytes(), 0);
        good.ingest_stream(&empty, Bytes::new(), None, 0).await?;
        assert_eq!(good.proven(), Some(0));
        assert!(good.is_complete().await?);
        good.finalize().await?;

        let bad = fresh_store([7u8; 32], 0);
        assert!(
            bad.ingest_stream(&empty, Bytes::new(), None, 0)
                .await
                .is_err(),
            "an empty claim must not verify against a non-empty root"
        );
        assert_eq!(bad.proven(), None);
        assert!(matches!(
            bad.finalize().await,
            Err(RangedStoreError::Incomplete)
        ));
        Ok(())
    }

    /// A blob of less than one chunk is proven by its only leaf and finalizes.
    #[tokio::test]
    async fn a_one_chunk_blob_is_proven_and_finalizes() -> anyhow::Result<()> {
        let data = blob(500);
        let source = crate::source::ScriptedSource::new(data.clone())?;
        let store = fresh_store(source.root(), 500);
        let whole = decdn_bao_range::align_range(0, 0, 500)?;
        let (claim, reader) = open_leg(&source, &whole).await?;
        store.ingest_stream(&whole, reader, None, claim).await?;

        assert_eq!(store.proven(), Some(500));
        store.finalize().await?;
        assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
        Ok(())
    }

    /// Fetch `data` whole into a fresh store at `dir`/`stem` and finalize it,
    /// leaving the promoted final file. Returns the blob's root.
    async fn finalize_blob(dir: &Path, stem: &str, data: &[u8]) -> anyhow::Result<[u8; 32]> {
        let source = crate::source::ScriptedSource::new(data.to_vec())?;
        let total = u64::try_from(data.len())?;
        let store = ClientRangedStore::create(dir, stem, source.root(), total)?;
        let leg = decdn_bao_range::align_range(0, 0, total)?;
        let (claim, reader) = open_leg(&source, &leg).await?;
        store.ingest_stream(&leg, reader, None, claim).await?;
        store.finalize().await?;
        Ok(source.root())
    }

    /// A final file of blob A at the path blob B is fetched to is not B: with
    /// B's partial on disk, `open` resumes the partial; with none,
    /// `open_or_create` starts B fresh. A's file is untouched either way.
    #[tokio::test]
    async fn open_resumes_the_partial_when_the_final_file_is_another_blob() -> anyhow::Result<()> {
        let a = blob(3 * usize::try_from(GROUP)?);
        let mut b = blob(5 * usize::try_from(GROUP)?);
        b.reverse();
        let b_root = bao_root_and_outboard(&b).0;
        let b_total = u64::try_from(b.len())?;

        // B's interrupted partial beside A's final file.
        let dir = tmp_dir();
        finalize_blob(dir.path(), "out.bin", &a).await?;
        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "out.bin", &b, 2 * GROUP)?;
        let store = ClientRangedStore::open(dir.path(), "out.bin", b_root)?;
        assert!(!store.is_complete().await?, "A's file must not complete B");
        assert_eq!(store.bound(), b_total);
        assert_eq!(store.proven(), None);
        assert_eq!(
            &store.present_ranges().await?,
            decdn_bao_range::align_range(0, 2 * GROUP, b_total)?.chunk_ranges()
        );
        assert!(dir.path().join("out.bin.partial.ranges").exists());
        assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);

        // No partial for B: a fresh store, A's file still untouched.
        let dir = tmp_dir();
        finalize_blob(dir.path(), "out.bin", &a).await?;
        assert!(ClientRangedStore::open(dir.path(), "out.bin", b_root).is_err());
        let store = ClientRangedStore::open_or_create(dir.path(), "out.bin", b_root, b_total)?;
        assert!(store.present_ranges().await?.is_empty());
        assert_eq!(store.proven(), None);
        assert_eq!(store.bound(), b_total);
        assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);
        Ok(())
    }

    /// A reopened store reports the byte spans its record holds, clamped to
    /// the bound: the checkpointed prefix, and the ragged final chunk once a
    /// fetch has it.
    #[tokio::test]
    async fn present_byte_ranges_reports_the_recorded_prefix() -> anyhow::Result<()> {
        let data = blob(3 * usize::try_from(GROUP)? + 99);
        let total = u64::try_from(data.len())?;
        let root = bao_root_and_outboard(&data).0;
        let dir = tmp_dir();
        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, GROUP)?;
        let store = ClientRangedStore::open(dir.path(), "blob", root)?;
        assert_eq!(store.present_byte_ranges(), vec![(0, GROUP)]);

        let dir = tmp_dir();
        ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, total)?;
        let store = ClientRangedStore::open(dir.path(), "blob", root)?;
        assert_eq!(store.present_byte_ranges(), vec![(0, total)]);

        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total)?;
        assert!(store.present_byte_ranges().is_empty());
        Ok(())
    }

    /// A final file that hashes to the root reopens complete, and `open`
    /// removes a stale record a crash left beside it (the positive path; the
    /// foreign-file path is `open_resumes_the_partial_when_the_final_file_is_another_blob`).
    #[tokio::test]
    async fn open_on_a_matching_final_file_is_complete_and_clears_the_record() -> anyhow::Result<()>
    {
        let a = blob(3 * usize::try_from(GROUP)? + 7);
        let dir = tmp_dir();
        let root = finalize_blob(dir.path(), "out.bin", &a).await?;
        let record = dir.path().join("out.bin.partial.ranges");
        write_record(&record, &StoreState::empty(1))?;

        let store = ClientRangedStore::open(dir.path(), "out.bin", root)?;
        assert!(!record.exists(), "the stale record is removed");
        assert!(store.is_complete().await?);
        assert_eq!(store.proven(), Some(u64::try_from(a.len())?));
        assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);
        Ok(())
    }

    /// A `.partial` cut short after ingest fails `finalize` as a mismatch, not
    /// as an I/O error: the store drops its claim, and a re-fetch completes.
    #[tokio::test]
    async fn a_truncated_partial_heals_on_the_next_fetch() -> anyhow::Result<()> {
        let total = 3 * GROUP + 99;
        let data = blob(usize::try_from(total)?);
        let source = crate::source::ScriptedSource::new(data.clone())?;
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", source.root(), total)?;
        let whole = decdn_bao_range::align_range(0, 0, total)?;
        let (claim, reader) = open_leg(&source, &whole).await?;
        store.ingest_stream(&whole, reader, None, claim).await?;

        let partial = dir.path().join("blob.partial");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&partial)?
            .set_len(GROUP)?;
        let err = store
            .finalize()
            .await
            .expect_err("a truncated partial must not finalize");
        let RangedStoreError::Backend(source_err) = &err else {
            panic!("expected a backend error, got {err:?}");
        };
        assert!(
            source_err.downcast_ref::<crate::HashMismatch>().is_some(),
            "a short partial is a hash mismatch, not an I/O error: {err:?}"
        );
        assert!(store.present_ranges().await?.is_empty());
        assert_eq!(store.proven(), None);
        assert!(partial.exists());

        let (claim, reader) = open_leg(&source, &whole).await?;
        store.ingest_stream(&whole, reader, None, claim).await?;
        store.finalize().await?;
        assert_eq!(std::fs::read(dir.path().join("blob"))?, data);
        Ok(())
    }
}
