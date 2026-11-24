//! [`ClientRangedStore`]: the client-side [`RangedStore`](decdn_bao_range::RangedStore)
//! backend over an iroh-blobs store (ADR 038, #2881).
//!
//! A store lives in a directory of its own, opened as a `BlobDir`: a single
//! download keeps one at `<dest>.decdn` beside its destination
//! (`store_dir_for`), and a bundle pull keeps one for every entry of the run.
//! The store holds the verified bytes, their outboard and the present ranges,
//! so a resumed download reads what an earlier run fetched from the store
//! alone. Once a blob is complete, `finalize` exports it to its destination.
//! Each run proves the blob's size again before it plans, so the store keeps
//! no size of its own beyond what the store's entry proves.
//!
//! An export moves the blob's data file to its destination, and the store's
//! entry then refers to that file. A store that outlives the export holds a
//! lost export once the file moves on: the entry reads complete, its bytes
//! cannot be read, and a fresh import into it is ignored. A fetch that meets
//! one ends with `StaleStoreEntry`, and only a new store fetches that blob
//! again.
//!
//! The store is keyed by offset. It holds the size `bound` the planner works
//! to and the `proven` size: a size proof sets both
//! ([`ClientRangedStore::prove_size`]), and a leg that verifies the final chunk
//! of its claimed tree proves that size. Each leg verifies under the size its
//! sender proves in the leg's header: `ingest_stream` aligns the leg under that
//! size and decodes with `BaoTree::new(size, IROH_BLOCK_SIZE)`. A verified leaf
//! is a genuine byte at its genuine offset, so the store's entry and the
//! verified chunk ranges do not depend on the size.
//!
//! `present` is kept in memory: read from the store's entry when the store
//! opens, and extended only after a batch of verified items is written to the
//! store. So a reader of `present` never asks the store for a byte it does not
//! hold.

use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bao_tree::io::BaoContentItem;
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bao_tree::{BaoTree, ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, RangedFuture, RangedStore, RangedStoreError};
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::blobs::{BlobStatus, ExportMode, ExportOptions};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::fs::options::Options;

use crate::sink::{StashedFault, decode_failure};
use crate::source::IngestEnd;

/// Verified items in flight between the decode loop and the store's import
/// task.
const IMPORT_CAP: usize = 32;

/// The directory a download to `dest` keeps its store in: `<dest>.decdn`,
/// beside `dest`.
///
/// # Errors
///
/// `dest` has no file name.
pub(crate) fn store_dir_for(dest: &Path) -> anyhow::Result<PathBuf> {
    let name = dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("download path {} has no file name", dest.display()))?;
    let mut dir = name.to_os_string();
    dir.push(".decdn");
    Ok(dest.with_file_name(dir))
}

/// The store holds a lost export of the blob a fetch needs: the entry reads
/// complete, but the file the blob was exported to is gone, so its bytes
/// cannot be read and a fresh import into the entry is ignored. Only a new
/// store fetches the blob again.
#[derive(Debug)]
pub(crate) struct StaleStoreEntry;

impl std::fmt::Display for StaleStoreEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "the download store holds a lost export of this file; the store is removed \
             when the run ends, and the next run fetches the file again",
        )
    }
}

impl std::error::Error for StaleStoreEntry {}

/// An iroh-blobs file store opened in one directory. One download, or one
/// bundle run, holds one, so no two processes open the same directory. The
/// store runs with no garbage collection: the caller removes the whole
/// directory once nothing in it is needed ([`Self::remove`]).
#[derive(Debug, Clone)]
pub(crate) struct BlobDir {
    fs: FsStore,
    path: PathBuf,
}

impl BlobDir {
    /// Open (or create) the store in `path`.
    ///
    /// # Errors
    ///
    /// The directory cannot be created, or the store cannot load: another
    /// process holds it, or its database is unreadable.
    pub(crate) async fn open(path: &Path) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| anyhow::anyhow!("create store dir {}: {e}", path.display()))?;
        let fs = FsStore::load_with_opts(path.join("blobs.db"), Options::new(path))
            .await
            .map_err(|e| anyhow::anyhow!("open store {}: {e}", path.display()))?;
        Ok(Self {
            fs,
            path: path.to_path_buf(),
        })
    }

    /// A [`ClientRangedStore`] for `root` in this directory, sized to
    /// `bound_hint` until a size is proven. `export_to` is where `finalize`
    /// puts the complete blob, if anywhere.
    ///
    /// # Errors
    ///
    /// The store cannot answer for `root`'s entry.
    pub(crate) async fn blob(
        &self,
        root: [u8; 32],
        bound_hint: u64,
        export_to: Option<PathBuf>,
    ) -> anyhow::Result<ClientRangedStore> {
        ClientRangedStore::open_in(Store::from(self.fs.clone()), root, bound_hint, export_to).await
    }

    /// The directory the store lives in.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The hashes of every blob the store holds only part of.
    ///
    /// # Errors
    ///
    /// The store cannot list its blobs or answer for one.
    pub(crate) async fn partial_hashes(&self) -> anyhow::Result<Vec<[u8; 32]>> {
        let store = Store::from(self.fs.clone());
        let hashes = store
            .blobs()
            .list()
            .hashes()
            .await
            .map_err(|e| anyhow::anyhow!("list store {}: {e}", self.path.display()))?;
        let mut partial = Vec::new();
        for hash in hashes {
            let status = store
                .blobs()
                .status(hash)
                .await
                .map_err(|e| anyhow::anyhow!("store status of {hash}: {e}"))?;
            if matches!(status, BlobStatus::Partial { .. }) {
                partial.push(*hash.as_bytes());
            }
        }
        Ok(partial)
    }

    /// Commit everything the store holds and stop it. A store that is dropped
    /// instead stops without waiting, and its last half second of writes may
    /// not reach its database; the store rebuilds what it can of them from
    /// its data files on the next open.
    ///
    /// # Errors
    ///
    /// The store's actor is gone.
    pub(crate) async fn shutdown(&self) -> anyhow::Result<()> {
        self.fs
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("shut down store {}: {e}", self.path.display()))
    }

    /// Stop the store and remove its directory.
    ///
    /// # Errors
    ///
    /// The store cannot stop, or the directory cannot be removed.
    pub(crate) async fn remove(self) -> anyhow::Result<()> {
        self.shutdown().await?;
        match tokio::fs::remove_dir_all(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow::anyhow!(
                "remove store dir {}: {e}",
                self.path.display()
            )),
        }
    }
}

/// The size and presence state of one [`ClientRangedStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreState {
    /// The size, in bytes, the planner works to: the caller's hint until a size
    /// is proven, then the proven size.
    bound: u64,
    /// The size a size proof or a verified final chunk proved, if any.
    proven: Option<u64>,
    /// Chunk ranges the store holds verified.
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

    /// Record that `size` is proven: the bound becomes it.
    const fn prove(&mut self, size: u64) {
        self.proven = Some(size);
        self.bound = size;
    }
}

/// Every chunk of a `size`-byte blob.
fn whole(size: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum(0)..ChunkNum::chunks(size))
}

/// The chunk group a `size`-byte blob ends in, as a chunk range.
fn last_group(size: u64) -> ChunkRanges {
    let group_chunks = 1u64 << IROH_BLOCK_SIZE.chunk_log();
    let end = ChunkNum::chunks(size);
    let start = end.0.saturating_sub(1) / group_chunks * group_chunks;
    ChunkRanges::from(ChunkNum(start)..end)
}

/// Client-side [`RangedStore`] for one blob `root`, over an iroh-blobs
/// [`Store`].
///
/// Concurrency: the multi-source scheduler runs `ingest_stream` concurrently
/// on disjoint ranges of one store; each call imports its own batches and
/// unions them into `present` under the state lock. `finalize` runs once,
/// after every ingest has returned.
pub struct ClientRangedStore {
    /// The store the blob's entry lives in. The handle keeps the store running.
    store: Store,
    /// The blob's hash in the store.
    hash: Hash,
    /// BLAKE3 content root this store verifies against.
    root: [u8; 32],
    /// Size bound, proven size and verified chunk ranges.
    state: Mutex<StoreState>,
    /// Woken each time an ingest batch makes more verified bytes present
    /// ([`Self::present_grew`]).
    present_grew: Arc<tokio::sync::Notify>,
    /// Where `finalize` exports the complete blob, if anywhere.
    export_to: Option<PathBuf>,
    /// Set once `finalize` exported the blob: the store's entry then refers to
    /// the exported file, which a second export must not copy onto itself.
    exported: AtomicBool,
}

impl std::fmt::Debug for ClientRangedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRangedStore")
            .field("hash", &format_args!("{}", self.hash.fmt_short()))
            .field("state", &*self.lock_state())
            .field("export_to", &self.export_to)
            .finish_non_exhaustive()
    }
}

impl ClientRangedStore {
    /// The size the batch of an ingest grows to: the decode loop writes its
    /// verified items to the store and marks them present no more than this
    /// many received content bytes apart.
    ///
    /// It also bounds what a dropped ingest loses: the verified items of the
    /// batch in progress are not in the store yet. It sits four payment quanta
    /// (`decdn_protocol::client::CHUNK_BYTES`) wide. The scheduler sizes its
    /// steal floor from it, so a stolen tail keeps at least one batch.
    pub(crate) const INGEST_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

    /// The size of each ingest's first batch. Each later batch doubles it
    /// ([`Self::next_checkpoint_bytes`]) up to [`Self::INGEST_CHECKPOINT_BYTES`].
    ///
    /// A reader of the verified stream sees a byte only once its batch is in
    /// the store, so the first batch sets how soon the first bytes are
    /// readable: 64 KiB is four chunk groups.
    pub(crate) const INGEST_FIRST_CHECKPOINT_BYTES: u64 = 64 * 1024;

    /// The batch size after one of `current` bytes: double it, capped at
    /// [`Self::INGEST_CHECKPOINT_BYTES`].
    pub(crate) const fn next_checkpoint_bytes(current: u64) -> u64 {
        let doubled = current.saturating_mul(2);
        if doubled < Self::INGEST_CHECKPOINT_BYTES {
            doubled
        } else {
            Self::INGEST_CHECKPOINT_BYTES
        }
    }

    /// The prefix one ingest has made present once `received` bytes of its
    /// range have arrived and the stream then stalls: the end of the last
    /// whole batch of the ramp.
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

    /// Wrap `store`'s entry for `root`, sized to `bound_hint` until a size is
    /// proven.
    ///
    /// A complete entry is present whole, at its proven size. A partial entry
    /// is present as its verified ranges. One whose store has not validated
    /// its size leaves out the chunk group its claimed size ends in: an entry
    /// whose completion record a crash lost holds every range while it reads
    /// partial, and fetching that one group again records it complete.
    ///
    /// # Errors
    ///
    /// The store cannot answer for the entry.
    pub(crate) async fn open_in(
        store: Store,
        root: [u8; 32],
        bound_hint: u64,
        export_to: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let hash = Hash::from_bytes(root);
        let mut state = StoreState::empty(bound_hint);
        if root == decdn_protocol::EMPTY_ROOT {
            state.prove(0);
        } else {
            let status = store
                .blobs()
                .status(hash)
                .await
                .map_err(|e| anyhow::anyhow!("store status of {hash}: {e}"))?;
            match status {
                BlobStatus::NotFound => {}
                BlobStatus::Complete { size } => {
                    state.prove(size);
                    state.present = whole(size);
                }
                BlobStatus::Partial { .. } => {
                    let bitfield = store
                        .blobs()
                        .observe(hash)
                        .await
                        .map_err(|e| anyhow::anyhow!("store bitfield of {hash}: {e}"))?;
                    let claim = bitfield.size();
                    let validated = bitfield.validated_size();
                    state.present = bitfield.ranges & whole(claim);
                    match validated {
                        Some(size) => state.prove(size),
                        None => state.present = state.present.clone() - &last_group(claim),
                    }
                }
            }
        }
        Ok(Self {
            store,
            hash,
            root,
            state: Mutex::new(state),
            present_grew: Arc::default(),
            export_to,
            exported: AtomicBool::new(false),
        })
    }

    /// Whether the store holds a lost export of the blob ([`StaleStoreEntry`]):
    /// its entry reads complete, but its first byte cannot be read.
    pub(crate) async fn is_lost_export(&self) -> bool {
        if self.root == decdn_protocol::EMPTY_ROOT {
            return false;
        }
        let complete = matches!(
            self.store.blobs().status(self.hash).await,
            Ok(BlobStatus::Complete { .. })
        );
        complete
            && self
                .store
                .blobs()
                .export_ranges(self.hash, 0..1)
                .concatenate()
                .await
                .is_err()
    }

    /// A fresh in-memory store for `root`, sized to `bound_hint` until a size
    /// is proven. Its store runs on the caller's runtime, so a test on a
    /// paused clock sees all of its work. Call it inside a runtime.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn in_memory(root: [u8; 32], bound_hint: u64) -> Self {
        let mut state = StoreState::empty(bound_hint);
        if root == decdn_protocol::EMPTY_ROOT {
            state.prove(0);
        }
        Self {
            store: Store::from(iroh_blobs::store::mem::MemStore::new()),
            hash: Hash::from_bytes(root),
            root,
            state: Mutex::new(state),
            present_grew: Arc::default(),
            export_to: None,
            exported: AtomicBool::new(false),
        }
    }

    /// A store for `root` in a file store in `dir`, sized to `bound_hint`
    /// until a size is proven: what `dir` holds for `root` from an earlier
    /// open, else nothing. [`Self::close`] stops the file store.
    ///
    /// # Errors
    ///
    /// The file store cannot open.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn on_disk(dir: &Path, root: [u8; 32], bound_hint: u64) -> anyhow::Result<Self> {
        BlobDir::open(dir).await?.blob(root, bound_hint, None).await
    }

    /// Commit and stop the store this view uses, so its directory can open
    /// again.
    ///
    /// # Errors
    ///
    /// The store's actor is gone.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn close(self) -> anyhow::Result<()> {
        self.store
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("shut down store: {e}"))
    }

    /// This store, exporting the complete blob to `path` at `finalize`.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn exporting_to(mut self, path: PathBuf) -> Self {
        self.export_to = Some(path);
        self
    }

    /// Seed the store a download to `dest` keeps (`<dest>.decdn`) with the
    /// chunk-group-aligned prefix `[0, prefix_len)` of `blob`, verified, as an
    /// earlier interrupted run would leave it. A prefix that covers the blob
    /// leaves it complete.
    ///
    /// # Errors
    ///
    /// The store cannot open, or `prefix_len` is past the blob's end.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn seed_download_prefix(
        dest: &Path,
        blob: &[u8],
        prefix_len: u64,
    ) -> anyhow::Result<()> {
        let total = u64::try_from(blob.len())?;
        let dir = BlobDir::open(&store_dir_for(dest)?).await?;
        let root = *bao_tree::io::outboard::PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE)
            .root
            .as_bytes();
        let store = dir.blob(root, total, None).await?;
        store.admit_prefix(blob, prefix_len).await?;
        dir.shutdown().await
    }

    /// Admit the chunk-group-aligned prefix `[0, prefix_len)` of `blob`, this
    /// store's blob, verified.
    ///
    /// # Errors
    ///
    /// `prefix_len` is past the blob's end, or `blob` is not this store's.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn admit_prefix(&self, blob: &[u8], prefix_len: u64) -> anyhow::Result<()> {
        let total = u64::try_from(blob.len())?;
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
        let aligned = decdn_bao_range::align_range(0, prefix_len, total)?;
        let start = usize::try_from(aligned.fetch_start())?;
        let end = usize::try_from(aligned.fetch_end())?;
        let data = blob
            .get(start..end)
            .ok_or_else(|| anyhow::anyhow!("aligned prefix past the blob"))?;
        let bao = decdn_bao_range::encode_verified_range(
            self.root,
            &aligned,
            data,
            Bytes::from(ob.data),
        )?;
        self.admit(aligned, bao).await?;
        Ok(())
    }

    /// The content root this store verifies against.
    #[must_use]
    pub const fn root(&self) -> [u8; 32] {
        self.root
    }

    /// The size, in bytes, the planner works to: the caller's hint until a
    /// size is proven, then the proven size.
    #[must_use]
    pub fn bound(&self) -> u64 {
        self.lock_state().bound
    }

    /// The proven size, if a size proof or a verified final chunk proved one.
    #[must_use]
    pub fn proven(&self) -> Option<u64> {
        self.lock_state().proven
    }

    /// The byte spans the store holds verified present, as sorted, disjoint
    /// `(offset, len)` pairs with the last one clamped to the bound. On a
    /// store just opened, these are the bytes earlier runs fetched, which a
    /// resumed fetch does not fetch again.
    #[must_use]
    pub fn present_byte_ranges(&self) -> Vec<(u64, u64)> {
        let state = self.lock_state();
        crate::driver::contiguous_byte_ranges(&state.present, state.bound)
    }

    /// Woken each time an ingest batch makes more verified bytes present. A
    /// reader waiting for bytes registers on it before reading the present
    /// frontier, so a batch that lands between the read and the wait still
    /// wakes it.
    #[must_use]
    pub(crate) fn present_grew(&self) -> &tokio::sync::Notify {
        &self.present_grew
    }

    /// Record `size` as proven: a node's size proof verified it against the
    /// root (ADR 005 § Bounded byte ranges), so the bound becomes it before
    /// any leg holds the final chunk. Only the true size proves, so an entry
    /// that proved another size is wrong: the fresh proof replaces it, with a
    /// warning.
    pub fn prove_size(&self, size: crate::ProvenSize) {
        let size = size.get();
        let mut state = self.lock_state();
        if state.proven == Some(size) {
            return;
        }
        if let Some(recorded) = state.proven {
            tracing::warn!(
                recorded,
                proven = size,
                "the store's entry proves another size; the fresh proof replaces it"
            );
        }
        state.prove(size);
    }

    /// The state under its lock. A poisoned lock still holds a consistent
    /// state: every writer updates it in one assignment or one union.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

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
    /// `claimed_total` proves that size once that chunk is in the store.
    ///
    /// The call verifies each chunk group against the root as
    /// [`bao_tree::io::fsm::ResponseDecoder`] decodes it and collects the
    /// verified items, parents included, into a batch. Each batch is written
    /// to the store as one import, then its leaves join `present` and the
    /// readers that wait for present bytes wake. The first batch is
    /// `INGEST_FIRST_CHECKPOINT_BYTES`, and each later one doubles up to
    /// `INGEST_CHECKPOINT_BYTES`. The store writes on its own runtime, so the
    /// decode loop, which pays from inside, never blocks a runtime worker on
    /// the disk.
    ///
    /// This is the streaming sibling of [`RangedStore::admit`]: `admit` takes
    /// a whole range's bao bytes already assembled in memory.
    ///
    /// On success, returns `reader` so the caller can hand it to
    /// `BlobSource::finish` to drain the underlying pull to its stream end and
    /// recover the acked voucher watermark.
    ///
    /// `on_progress`, when set, is called with the CONTENT bytes received so
    /// far on THIS range (`received_end - range.fetch_start()`) after each
    /// verified leaf. It runs in the hot receive loop, so it must not block or
    /// panic.
    ///
    /// On a peer fault the call writes the verified batch it holds before it
    /// returns, so a fault re-fetches and re-pays only the bytes past the last
    /// verified chunk group. Dropping the future loses the batch in progress.
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
    /// - The store refusing an import. When the stream itself failed, the
    ///   stashed fault or decode failure outranks it, and it is logged
    ///   instead: it says what the peer did, which decides retry and blame.
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
    /// streams. Once the verified prefix reaches `stop_at`, the call writes
    /// that prefix and returns [`IngestEnd::Stopped`] without reading further;
    /// the bytes past it stay missing. A range the decoder reads to its end
    /// returns [`IngestEnd::Drained`].
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
        let Some(size) = NonZeroU64::new(claimed_total) else {
            if self.root != decdn_protocol::EMPTY_ROOT {
                return Err(anyhow::Error::new(crate::HashMismatch));
            }
            self.lock_state().prove(0);
            return Ok((reader, IngestEnd::Drained));
        };

        let root = bao_tree::blake3::Hash::from(self.root);
        let tree = BaoTree::new(claimed_total, IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(root, range.chunk_ranges().clone(), tree, reader);

        // The contiguous prefix of `range` whose leaves have been received and
        // verified so far, and the part of it already in the store. Only the
        // span `[batch_start, received_end)` sits in `batch`.
        let mut received_end = range.fetch_start();
        let mut batch_start = range.fetch_start();
        let mut batch: Vec<BaoContentItem> = Vec::new();
        let mut batch_target = Self::INGEST_FIRST_CHECKPOINT_BYTES;

        loop {
            match decoder.next().await {
                ResponseDecoderNext::More((rest, Ok(item))) => {
                    let leaf_end = match &item {
                        BaoContentItem::Leaf(leaf) => Some(
                            leaf.offset
                                .saturating_add(u64::try_from(leaf.data.len()).unwrap_or(u64::MAX)),
                        ),
                        BaoContentItem::Parent(_) => None,
                    };
                    batch.push(item);
                    let Some(leaf_end) = leaf_end else {
                        decoder = rest;
                        continue;
                    };
                    received_end = received_end.max(leaf_end);
                    if let Some(cb) = on_progress {
                        cb(received_end.saturating_sub(range.fetch_start()));
                    }
                    if received_end.saturating_sub(batch_start) >= batch_target {
                        self.commit(size, std::mem::take(&mut batch), batch_start, received_end)
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
                        self.commit(size, batch, batch_start, received_end).await?;
                        return Ok((r, IngestEnd::Stopped));
                    }
                    decoder = rest;
                }
                ResponseDecoderNext::More((rest, Err(decode_err))) => {
                    let mut r = rest.finish();
                    // The decoder yields an item only once it verifies, so the
                    // batch holds verified items of a received prefix. Write
                    // it so the prefix is not re-paid on resume. The fault is
                    // the error that matters here.
                    if let Err(commit_err) =
                        self.commit(size, batch, batch_start, received_end).await
                    {
                        warn_commit_failed_on_fault(&commit_err, range, received_end);
                    }
                    return Err(decode_failure(&mut r, decode_err));
                }
                ResponseDecoderNext::Done(mut r) => {
                    let committed = self.commit(size, batch, batch_start, received_end).await;
                    // A stashed fault outranks a local store failure: it says
                    // what the peer did, which decides retry and blame.
                    if let Some(fault) = r.take_fault() {
                        if let Err(commit_err) = committed {
                            warn_commit_failed_on_fault(&commit_err, range, received_end);
                        }
                        return Err(fault);
                    }
                    committed?;
                    return Ok((r, IngestEnd::Drained));
                }
            }
        }
    }

    /// Write `items`, the verified items of `[start, end)` under a `size`-byte
    /// claim, to the store as one import, then mark `[start, end)` present and
    /// wake the readers. A prefix that reaches `size` holds the claimed tree's
    /// final chunk, verified, so it also proves `size`.
    async fn commit(
        &self,
        size: NonZeroU64,
        items: Vec<BaoContentItem>,
        start: u64,
        end: u64,
    ) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let handle = self
            .store
            .blobs()
            .import_bao(self.hash, size, IMPORT_CAP)
            .await
            .map_err(|e| anyhow::anyhow!("store import of {}: {e}", self.hash))?;
        for item in items {
            handle
                .tx
                .send(item)
                .await
                .map_err(|e| anyhow::anyhow!("store import of {}: {e}", self.hash))?;
        }
        drop(handle.tx);
        handle
            .rx
            .await
            .map_err(|e| anyhow::anyhow!("store import of {}: {e}", self.hash))?
            .map_err(|e| anyhow::anyhow!("store import of {}: {e}", self.hash))?;
        if end > start {
            let mut state = self.lock_state();
            state.present |= ChunkRanges::from(ChunkNum::full_chunks(start)..ChunkNum::chunks(end));
            let size = size.get();
            if end >= size {
                if let Some(earlier) = state.proven.filter(|&p| p != size) {
                    tracing::warn!(
                        earlier_proven = earlier,
                        proven = size,
                        "a verified final chunk proves another size than the store held"
                    );
                }
                state.prove(size);
            }
        }
        self.present_grew.notify_waiters();
        Ok(())
    }
}

/// Log a store failure that a stream fault outranks, so it is not lost.
fn warn_commit_failed_on_fault(err: &anyhow::Error, range: &AlignedRange, received_end: u64) {
    tracing::warn!(
        error = %err,
        fetch_start = range.fetch_start(),
        received_end,
        "writing the verified prefix failed while handling a stream fault"
    );
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

fn backend(e: impl std::fmt::Display) -> RangedStoreError {
    RangedStoreError::Backend(e.to_string().into())
}

impl RangedStore for ClientRangedStore {
    /// The current bound: the planner's size until a size is proven.
    fn total_bytes(&self) -> u64 {
        self.bound()
    }

    fn present_ranges(&self) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move { Ok(self.lock_state().present.clone()) })
    }

    /// An end past the bound clamps to it, as on the node's store: the bound
    /// can shrink under a caller that read it first. A start at or past the
    /// bound is an alignment error.
    fn missing_ranges(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, ChunkRanges> {
        Box::pin(async move {
            let state = self.lock_state().clone();
            let aligned = decdn_bao_range::align_range_clamped(byte_offset, byte_len, state.bound)?;
            Ok(aligned.chunk_ranges().clone() - &state.present)
        })
    }

    /// Verify `bao_bytes` under the size its own header claims and write it
    /// through [`ClientRangedStore::ingest_stream`]. Idempotent: re-admitting
    /// an already-present range re-verifies and re-writes the same bytes.
    fn admit(&self, range: AlignedRange, bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            // `bao_bytes` is the combined wire format `encode_verified_range`
            // produces: an 8-byte little-endian size header, the sender's
            // claim, followed by the interleaved bao body.
            let header: [u8; 8] = bao_bytes
                .get(..8)
                .and_then(|h| h.try_into().ok())
                .ok_or_else(|| {
                    RangedStoreError::Backend(
                        "admit: bao_bytes shorter than the 8-byte combined-format header".into(),
                    )
                })?;
            let claim = u64::from_le_bytes(header);
            let body = bao_bytes.slice(8..);
            self.ingest_stream(&range, AdmitBody(body), None, claim)
                .await
                .map_err(|e| RangedStoreError::Backend(e.into()))?;
            Ok(())
        })
    }

    /// The bytes of `[byte_offset, byte_offset + byte_len)`, with an end past
    /// the bound clamped to it ([`Self::missing_ranges`]). A zero length reads
    /// to the bound.
    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, Bytes> {
        Box::pin(async move {
            let state = self.lock_state().clone();
            let aligned = decdn_bao_range::align_range_clamped(byte_offset, byte_len, state.bound)?;
            if !(aligned.chunk_ranges().clone() - &state.present).is_empty() {
                return Err(RangedStoreError::Backend(
                    "requested range not present".into(),
                ));
            }
            let room = state.bound.saturating_sub(byte_offset);
            let len = if byte_len == 0 {
                room
            } else {
                byte_len.min(room)
            };
            if len == 0 {
                return Ok(Bytes::new());
            }
            let data = self
                .store
                .blobs()
                .export_ranges(self.hash, byte_offset..byte_offset.saturating_add(len))
                .concatenate()
                .await
                .map_err(backend)?;
            Ok(Bytes::from(data))
        })
    }

    /// A size is proven and every byte of `[0, proven)` is present.
    fn is_complete(&self) -> RangedFuture<'_, bool> {
        Box::pin(async move {
            let state = self.lock_state();
            Ok(state
                .proven
                .is_some_and(|proven| (whole(proven) - &state.present).is_empty()))
        })
    }

    /// Export the complete blob to its destination, moving the store's data
    /// file there where the file system allows and copying it otherwise.
    /// Every byte verified as it landed, and the store records the blob
    /// complete only once its whole tree verified, so the export needs no
    /// hash of its own.
    ///
    /// A store whose entry holds every range but is not recorded complete
    /// drops its last chunk group from `present` and returns
    /// [`RangedStoreError::Incomplete`]: fetching that group again records the
    /// blob complete.
    fn finalize(&self) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            if self.exported.load(Ordering::SeqCst) {
                return Ok(());
            }
            let Some(proven) = self.lock_state().proven else {
                return Err(RangedStoreError::Incomplete);
            };
            if !self.is_complete().await? {
                return Err(RangedStoreError::Incomplete);
            }
            if proven == 0 {
                if let Some(path) = &self.export_to {
                    tokio::fs::write(path, b"").await.map_err(backend)?;
                }
                self.exported.store(true, Ordering::SeqCst);
                return Ok(());
            }
            let status = self
                .store
                .blobs()
                .status(self.hash)
                .await
                .map_err(backend)?;
            if !matches!(status, BlobStatus::Complete { .. }) {
                let mut state = self.lock_state();
                state.present = state.present.clone() - &last_group(proven);
                return Err(RangedStoreError::Incomplete);
            }
            if let Some(path) = &self.export_to {
                // The store exports only to an absolute path; a download's
                // destination can be relative to the working directory.
                let target = std::path::absolute(path).map_err(backend)?;
                self.store
                    .blobs()
                    .export_with_opts(ExportOptions {
                        hash: self.hash,
                        mode: ExportMode::TryReference,
                        target,
                    })
                    .finish()
                    .await
                    .map_err(backend)?;
            }
            self.exported.store(true, Ordering::SeqCst);
            Ok(())
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

    /// Commit the store's pending writes to its database.
    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        Box::pin(async move {
            self.store
                .sync_db()
                .await
                .map_err(|e| anyhow::anyhow!("store sync: {e}"))
        })
    }

    fn proven(&self) -> Option<u64> {
        Self::proven(self)
    }

    fn prove_size(&self, size: crate::ProvenSize) {
        Self::prove_size(self, size);
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
