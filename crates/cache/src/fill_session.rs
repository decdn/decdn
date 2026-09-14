//! `FillSession` — the cache-resident fill-coordination primitive the
//! serve-miss legs share (ADR 038).
//!
//! # Why this exists
//!
//! The serve leg must emit ONE coherent whole-range bao verified stream
//! while the pull leg fills the cache incrementally beside it. `bao_tree`'s async
//! [`encode_ranges_validated`](bao_tree::io::fsm::encode_ranges_validated) is the
//! right engine — it walks the range pre-order, reading leaf DATA and, for every
//! internal node, the `(left, right)` hash pair from an [`Outboard`]. The data the
//! serve reads from the cache (via ranged export, gated on the present-range watch).
//! But the OUTBOARD it cannot: iroh-blobs keeps a partial blob's outboard in actor
//! memory and exposes no reader, and a pre-order walk needs proof hashes for
//! not-yet-filled right-spine subtrees (the root's `right_hash` is emitted before
//! any right-half data lands) that are NOT derivable from present data.
//!
//! So the cache keeps its own copy, captured as each range admits (the pull leg
//! already receives those exact proof nodes in the verified upstream bao it ingests,
//! and the cache's `admit_bao_stream` captures them via [`FillSession::capture`] into
//! a whole-tree buffer). The serve leg reads them through a [`SessionOutboardReader`],
//! whose [`Outboard::load`] AWAITS a node that has not been captured yet — racing the
//! registry-wide fill liveness so a fill that dies (or a genuinely missing node after
//! a clean pull) fails the serve rather than hanging.
//!
//! # The outboard is per-HASH, shared by every fill of that hash
//!
//! A blob is BLAKE3-addressed, so its tree geometry — and thus every captured
//! `(left, right)` node — is a property of the HASH, not of any single pull. When two
//! serve-misses want overlapping bytes of one hash, one opens a pull for its
//! remainder and the other for its own; both admit into the SAME store and each admit
//! captures proof nodes into the ONE per-hash [`HashOutboard`]. Because
//! `attach ∪ remainder = R` by construction, the two pulls together capture every
//! proof node a coherent encode of `R` needs, into that single buffer. A serve leg
//! reading `R` therefore does not care which pull filled what — it reads the shared
//! per-hash outboard and the source-agnostic present-range watch.
//!
//! The one thing that must generalize past a single pull is termination: "await this
//! node, or fail" has to consult ALL live fills whose `covered` ranges intersect the
//! node's byte span, not one pull's terminal signal. [`FillSession::range_still_live`]
//! (backed by [`FillRegistry::range_still_live`]) answers exactly that — it fails a
//! parked reader iff no live session's `covered` still intersects the range, which is
//! the N-fill generalization of the single-pull failure race.
//!
//! # Ownership seam (cache owns the structure, the node drives it)
//!
//! The pull leg (paid peer or own-origin) is node-side and alone knows when/why it
//! terminated. So the session exposes [`FillSession::mark_ended`]; the node's pull
//! leg calls it on every exit. The cache owns the DATA STRUCTURE; the node DRIVES
//! it. [`FillError`] is a flattened terminal message (`anyhow::Error` is not `Clone`).

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, PoisonError, Weak};

use bao_tree::io::fsm::Outboard;
use bao_tree::{BaoTree, BlockSize, ChunkRanges, TreeNode, blake3};
use decdn_bao_range::align_range;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::Hash;

/// iroh-blobs' block size — 16 KiB chunk groups (`2^4` 1 KiB chunks). Identical by
/// construction to `decdn_bao_range::IROH_BLOCK_SIZE` and
/// `iroh_blobs::store::IROH_BLOCK_SIZE`; the tree geometry (and thus every node
/// offset) must match the store's, since the captured hashes are the store's.
const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

/// Bytes per outboard node (a `(left, right)` blake3 hash pair).
const HASH_PAIR_BYTES: usize = 64;

/// Bytes per bao chunk (the `ChunkNum` unit). `ChunkNum(n)` starts at `n * 1024`.
const CHUNK_BYTES: u64 = 1024;

/// A flattened terminal error for a fill: the node's pull leg records why its pull
/// ended, and the serve-side [`SessionOutboardReader`] / data reader surface it.
/// A `String` message rather than `anyhow::Error` because the outcome must be
/// `Clone` (every observer reads it) and `anyhow::Error` is not.
#[derive(Debug, Clone)]
pub struct FillError(String);

impl FillError {
    /// Wrap a flattened terminal message.
    #[must_use]
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for FillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FillError {}

/// The pre-order outboard buffer + a per-node "captured" flag. Guarded together;
/// the lock is never held across an await.
///
/// Both vectors are allocated lazily on the first capture (see
/// [`HashOutboard::write_pair`]), not at construction: empty ⇒ nothing captured
/// yet, which [`HashOutboard::try_load`] reads as "absent" exactly as an all-false
/// `captured` did. This keeps the MB-scale zeroing for a large blob off the global
/// registry lock that [`HashOutboard::new`] runs under.
#[derive(Debug)]
struct OutboardState {
    /// `tree.outboard_size()` bytes once sized: node `n`'s pair lives at
    /// `tree.pre_order_offset(n) * 64`. Empty until the first capture sizes it.
    bytes: Vec<u8>,
    /// `captured[i]` is set once node index `i` (its `pre_order_offset`) is saved.
    /// Empty until the first capture sizes it, alongside `bytes`.
    captured: Vec<bool>,
}

/// The captured whole-tree outboard for ONE hash, shared by every [`FillSession`]
/// that fills that hash. A blob's tree geometry is fixed by its BLAKE3 root, so a
/// node captured by any pull — of any range, from any source — is valid for every
/// serve leg reading that hash. The registry keys exactly one of these per hash and
/// every session for the hash references it, which is what makes partial-overlap
/// proof-sourcing free: pull A (admitting `attach`) and pull B (admitting
/// `remainder`) capture into the same buffer, and a serve leg for `R = attach ∪
/// remainder` reads every node either supplied.
#[derive(Debug)]
pub struct HashOutboard {
    tree: BaoTree,
    root: blake3::Hash,
    state: StdMutex<OutboardState>,
    /// Notified after every [`Self::capture`] so a parked [`Outboard::load`]
    /// re-checks. Runtime-agnostic, so it crosses the pull/serve runtimes safely.
    captured: Notify,
    /// Notified when ANY session for this hash ends or is cancelled, so a parked
    /// reader re-checks [`FillRegistry::range_still_live`] and fails a range no live
    /// pull will fill. Shared as an `Arc` so a data reader on the node side can hold
    /// an owned handle without naming this cache-private type.
    liveness: Arc<Notify>,
}

impl HashOutboard {
    /// Build the empty outboard for a `total_bytes`-byte blob rooted at `root`.
    ///
    /// O(1): the `tree.outboard_size()`-byte buffer is NOT allocated here — the
    /// first [`Self::write_pair`] sizes it under the per-hash `state` lock. This
    /// keeps a session's outboard construction under the global [`FillRegistry`]
    /// lock free of MB-scale zeroing, so one large-blob claim no longer stalls
    /// every other hash's claim/wakeup for the allocation duration, and a session
    /// that only adopts the canonical outboard allocates nothing to discard.
    #[must_use]
    fn new(root: blake3::Hash, total_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            tree: BaoTree::new(total_bytes, IROH_BLOCK_SIZE),
            root,
            state: StdMutex::new(OutboardState {
                bytes: Vec::new(),
                captured: Vec::new(),
            }),
            captured: Notify::new(),
            liveness: Arc::new(Notify::new()),
        })
    }

    /// Write one internal node's `(left, right)` hash pair into `state`, returning
    /// whether the pair was actually persisted. `false` for a leaf (no outboard
    /// slot), an offset past `usize`, or a slot outside the buffers — nothing was
    /// stored, so the caller must NOT wake readers on its behalf. `true` only once
    /// both the hash bytes and the captured flag are set, which is exactly the state
    /// [`Self::try_load`] reads back. Idempotent: re-saving a node (a re-admitted
    /// range) overwrites with the same bytes. Does NOT notify; the caller wakes
    /// parked readers once the batch is in, so an N-node admit fires `captured` a
    /// single time rather than N.
    fn write_pair(
        &self,
        state: &mut OutboardState,
        node: TreeNode,
        pair: (blake3::Hash, blake3::Hash),
    ) -> bool {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return false; // leaf: no hash pair in the outboard
        };
        let Ok(idx) = usize::try_from(offset) else {
            return false; // offset beyond usize: no addressable slot
        };
        // Lazily size the outboard on the first capture, under this per-hash
        // `state` lock rather than the global registry lock `new` runs under. A
        // real interior node (a `Some` offset) implies `outboard_size() >= 64`, so
        // an empty buffer here always means "not yet sized", never a zero-node
        // tree. Sized exactly once: every later capture sees a non-empty buffer.
        // Bail if the size is not `usize`-representable rather than allocating
        // `usize::MAX` — the same "not addressable, not capturable" degrade as the
        // `idx` guard above, never a process-aborting allocation.
        if state.bytes.is_empty() {
            let Ok(size) = usize::try_from(self.tree.outboard_size()) else {
                return false;
            };
            state.bytes = vec![0u8; size];
            state.captured = vec![false; size / HASH_PAIR_BYTES];
        }
        let byte_off = idx.saturating_mul(HASH_PAIR_BYTES);
        let (l, r) = pair;
        let bytes_written = if let Some(slot) =
            state.bytes.get_mut(byte_off..byte_off + HASH_PAIR_BYTES)
            && let Some((left, right)) = slot.split_at_mut_checked(32)
        {
            left.copy_from_slice(l.as_bytes());
            right.copy_from_slice(r.as_bytes());
            true
        } else {
            false
        };
        let flag_set = if let Some(flag) = state.captured.get_mut(idx) {
            *flag = true;
            true
        } else {
            false
        };
        bytes_written && flag_set
    }

    /// Capture one internal node's `(left, right)` hash pair, then wake parked
    /// readers. A `node` with no outboard slot (a leaf) is ignored and does not notify.
    fn capture(&self, node: TreeNode, pair: (blake3::Hash, blake3::Hash)) {
        let wrote = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            self.write_pair(&mut state, node, pair)
        };
        if wrote {
            self.captured.notify_waiters();
        }
    }

    /// Capture a whole admit's worth of node pairs under one lock acquisition, then
    /// fire `captured` exactly once. A serve leg parked on any of these nodes reads
    /// them all back the same as a per-node [`Self::capture`] loop would, but every
    /// parked reader wakes once per admit instead of once per node — the admit-time
    /// notify no longer scales with the number of proof nodes it carries. Leaves are
    /// ignored; the notify fires only if at least one internal node was written.
    fn capture_many(
        &self,
        pairs: impl IntoIterator<Item = (TreeNode, (blake3::Hash, blake3::Hash))>,
    ) {
        let wrote = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            pairs.into_iter().fold(false, |acc, (node, pair)| {
                self.write_pair(&mut state, node, pair) || acc
            })
        };
        if wrote {
            self.captured.notify_waiters();
        }
    }

    /// Read node `idx`'s captured pair, or `None` if not captured yet. Never holds
    /// the lock across an await.
    fn try_load(&self, idx: usize) -> Option<(blake3::Hash, blake3::Hash)> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.captured.get(idx).copied() != Some(true) {
            return None;
        }
        let byte_off = idx.saturating_mul(HASH_PAIR_BYTES);
        let slot = state.bytes.get(byte_off..byte_off + HASH_PAIR_BYTES)?;
        let (l, r) = slot.split_at_checked(32)?;
        let left: [u8; 32] = l.try_into().ok()?;
        let right: [u8; 32] = r.try_into().ok()?;
        Some((blake3::Hash::from(left), blake3::Hash::from(right)))
    }
}

/// One in-flight fill of a `total_bytes`-byte blob rooted at `root`.
///
/// Holds a reference to the per-hash [`HashOutboard`] (captured by the cache admit
/// path via [`Self::capture`], read by [`SessionOutboardReader`]), the pull's
/// terminal outcome ([`Self::mark_ended`] / [`Self::outcome`]), the ranges this fill
/// covers ([`Self::set_covered`]), and the shared downstream paid frontier the pull
/// leg paces against ([`Self::served_frontier`] / [`Self::served_advanced`]). Shared
/// behind an `Arc`; every field is interior-mutable, so both legs hold
/// `Arc<FillSession>` and coordinate on it.
#[derive(Debug)]
pub struct FillSession {
    /// The per-hash captured outboard. A freshly built session starts with its own,
    /// but registration ([`FillRegistry::register_fill`] / [`FillRegistry::claim`])
    /// swaps in the hash's canonical one via [`Self::adopt_outboard`], so all
    /// sessions for a hash — and every reader they mint — share the ONE buffer. The
    /// swap runs under the registry lock before the session is handed to a pull leg
    /// or a serve leg, so no capture or read observes a stale outboard.
    outboard: StdMutex<Arc<HashOutboard>>,
    /// The pull leg's terminal outcome — `None` while running, `Some(Ok)` on a
    /// clean end, `Some(Err)` on a failure.
    ended: StdMutex<Option<Result<(), FillError>>>,
    /// The client's PAID content frontier: the serve leg stores it after each
    /// voucher batch commits, the pull leg's `WindowPacer` reads it to bound
    /// `pulled − served_paid ≤ window`, plus one floor to serve `serve_demand`. An
    /// `Arc` so a pull leg on its own runtime
    /// can hold an owned handle.
    served_paid: Arc<AtomicU64>,
    /// Notified after each `served_paid` or `serve_demand` advance, so a parked pull
    /// re-decides exactly when a downstream voucher clears or a serve leg starts
    /// waiting on bytes.
    served_paid_advanced: Arc<Notify>,
    /// The content end of the furthest span a serve leg has waited on (a high-water
    /// mark, never lowered): its data reader raises it when its present-range
    /// snapshot misses a leaf, and its [`SessionOutboardReader`] before it parks on
    /// a proof node no pull has captured. When it lies within one chunk group past a
    /// pull's frontier, that pull's pacer draws one window floor even with its window
    /// full. Without it, a pull window that closes before the serve leg's credit
    /// window leaves both legs waiting on each other.
    serve_demand: Arc<AtomicU64>,
    /// The chunk ranges THIS fill will produce — exactly the bytes this pull
    /// fetches (its `missing_ranges ∩ R`). [`FillRegistry::range_still_live`]
    /// intersects a reader's node range against the union of all live sessions'
    /// `covered` to decide whether to keep awaiting or fail. Set at registration via
    /// [`Self::set_covered`]; defaults to [`ChunkRanges::all`] so a session built but
    /// not yet range-scoped coalesces conservatively.
    covered: StdMutex<ChunkRanges>,
    /// Live observer count: the pull owner plus every serve leg attached to this
    /// fill. An [`ObserverLease`] increments on attach and decrements on drop; when
    /// it reaches 0 while the pull is still running ([`Self::outcome`] is `None`),
    /// the last-out drop fires [`Self::cancel`] to stop ingest (#1610).
    observers: AtomicUsize,
    /// Cancels the pull when the last observer leaves before the pull ends. The
    /// node's pull leg selects on it to abort ingest; the tagged partial persists
    /// for a later resume.
    cancel: CancellationToken,
    /// A back-link to the owning registry + this session's hash, set once at
    /// registration ([`Self::bind_registry`]). It lets a reader consult ALL
    /// live sessions for the hash via [`FillRegistry::range_still_live`]. A session
    /// built standalone (in tests) leaves this unset and falls back to its own
    /// single-fill liveness — correct, because a standalone session is the only fill.
    registry: OnceLock<(Weak<FillRegistry>, Hash)>,
    /// The node's pull-thread [`JoinHandle`](std::thread::JoinHandle), parked here by
    /// the owner right after it spawns the pull, so whichever observer leaves LAST
    /// joins it. That frees an owner whose OWN client finishes first from parking in
    /// the join while other observers still stream: a non-last-out release returns
    /// immediately, and the true last-out [`ObserverLease`] drop takes the handle back
    /// out to join OFF the map lock. `None` when no pull was spawned (an attach, or a
    /// spawn that failed).
    pull_thread: StdMutex<Option<std::thread::JoinHandle<()>>>,
}

impl FillSession {
    /// Construct the fill session for a `total_bytes`-byte blob rooted at `root`.
    /// The served frontier starts at 0; the orchestration seeds it to the request's
    /// content start before either leg streams. The session starts with a private
    /// per-hash outboard, replaced by the registry's canonical one at registration.
    #[must_use]
    pub fn new(root: blake3::Hash, total_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            outboard: StdMutex::new(HashOutboard::new(root, total_bytes)),
            ended: StdMutex::new(None),
            served_paid: Arc::new(AtomicU64::new(0)),
            served_paid_advanced: Arc::new(Notify::new()),
            serve_demand: Arc::new(AtomicU64::new(0)),
            covered: StdMutex::new(ChunkRanges::all()),
            observers: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            registry: OnceLock::new(),
            pull_thread: StdMutex::new(None),
        })
    }

    /// Park the owner's pull-thread handle on the session so whichever observer leaves
    /// LAST joins it. Called once, on an owning branch, right after the pull thread is
    /// spawned. The pure-attach path never calls this — it drives no pull of its own.
    ///
    /// Refuses to overwrite an already-parked handle: a session has exactly one owning
    /// pull, so a second park cannot happen — but if a future regression called this
    /// twice, overwriting would DROP the first handle and detach its thread, orphaning
    /// a pull that keeps paying/draining. So keep the first (the one the last-out
    /// observer will join) and `debug_assert` the double-park loudly in tests/dev.
    pub fn set_pull_handle(&self, handle: std::thread::JoinHandle<()>) {
        let mut slot = self
            .pull_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        debug_assert!(
            slot.is_none(),
            "a FillSession parks exactly one pull-thread handle; a second park would \
             detach the first and orphan its pull"
        );
        if slot.is_none() {
            *slot = Some(handle);
        }
    }

    /// Take the parked pull-thread handle, if any. Called only by the last-out lease
    /// teardown, so exactly one caller ever receives it — the one that then joins the
    /// thread off-task.
    fn take_pull_handle(&self) -> Option<std::thread::JoinHandle<()>> {
        self.pull_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The per-hash outboard this session currently references. Cloned out (a cheap
    /// `Arc` bump) rather than borrowed, so the swap lock is never held across a
    /// capture or a read.
    fn outboard(&self) -> Arc<HashOutboard> {
        self.outboard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Adopt the hash's canonical outboard (shared with sibling sessions). Called
    /// once, under the registry lock, before this session drives a pull or mints a
    /// reader — so every later capture / read lands in the one shared buffer.
    fn adopt_outboard(&self, shared: Arc<HashOutboard>) {
        *self.outboard.lock().unwrap_or_else(PoisonError::into_inner) = shared;
    }

    /// Bind this session to its registry + hash, so a reader it mints can ask the
    /// registry which live fills still cover a range. Called once at registration.
    fn bind_registry(&self, registry: Weak<FillRegistry>, hash: Hash) {
        let _ = self.registry.set((registry, hash));
    }

    /// Set the chunk ranges this fill will produce. Called once at registration,
    /// before the pull streams — the registry reads it to plan observer attach and
    /// to answer [`Self::range_still_live`].
    pub fn set_covered(&self, covered: ChunkRanges) {
        *self.covered.lock().unwrap_or_else(PoisonError::into_inner) = covered;
    }

    /// A clone of this fill's `covered` ranges.
    fn covered_ranges(&self) -> ChunkRanges {
        self.covered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The blob's total byte length. The attach path signs its `StreamResponse`
    /// from this without re-running the upstream header handshake — the session
    /// already knows the geometry from its `BaoTree`.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.outboard().tree.size()
    }

    /// The live observer count (pull owner + attached serve legs).
    #[must_use]
    pub fn observer_count(&self) -> usize {
        self.observers.load(Ordering::Acquire)
    }

    /// Whether the pull has been cancelled because the last observer left.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The pull leg's cancellation token — it selects on this to abort ingest when
    /// the last observer leaves before the pull completes.
    #[must_use]
    pub const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Whether this session is dead: the pull has recorded a terminal outcome, or
    /// the fill was cancelled because its last observer left. A dead session must
    /// not block a fresh pull (its `covered` is excluded from the coverage union)
    /// and must not be an attach target.
    #[must_use]
    fn is_dead(&self) -> bool {
        self.outcome().is_some() || self.cancel.is_cancelled()
    }

    /// Mint a serve-side [`SessionOutboardReader`] over this hash's shared outboard.
    /// The reader snapshots the (already-adopted) outboard and holds this session so
    /// its termination race can reach the registry.
    #[must_use]
    pub fn outboard_reader(self: &Arc<Self>) -> SessionOutboardReader {
        SessionOutboardReader {
            session: Arc::clone(self),
            outboard: self.outboard(),
        }
    }

    /// The shared PAID content frontier the pull leg's `WindowPacer` bounds against.
    #[must_use]
    pub const fn served_frontier(&self) -> &Arc<AtomicU64> {
        &self.served_paid
    }

    /// Notified after each [`Self::served_frontier`] or [`Self::serve_demand`]
    /// advance.
    #[must_use]
    pub const fn served_advanced(&self) -> &Arc<Notify> {
        &self.served_paid_advanced
    }

    /// The content end of the furthest span a serve leg awaits. The pull leg's pacer
    /// may always draw up to it.
    #[must_use]
    pub const fn serve_demand(&self) -> &Arc<AtomicU64> {
        &self.serve_demand
    }

    /// Record that a serve leg awaits content up to `end`, and wake each parked pull
    /// whose demand frontier this raises. The demand goes to every live fill of the
    /// hash, not only this one: under coalescing a sibling pull may be the one that
    /// produces the awaited span. A pull acts on it only when `end` lies within one
    /// chunk group past its own frontier, so a demand far from a pull costs nothing.
    pub fn demand_up_to(&self, end: u64) {
        let registry = self
            .registry
            .get()
            .and_then(|(weak, hash)| weak.upgrade().map(|registry| (registry, *hash)));
        match registry {
            Some((registry, hash)) => registry.raise_demand(hash, end),
            None => self.raise_own_demand(end),
        }
    }

    /// Raise this session's own demand frontier to `end`, waking its parked pull if
    /// the frontier moved.
    fn raise_own_demand(&self, end: u64) {
        if self.serve_demand.fetch_max(end, Ordering::AcqRel) < end {
            self.served_paid_advanced.notify_waiters();
        }
    }

    /// The per-hash liveness signal, notified whenever any session for this hash
    /// ends or is cancelled. A serve-side data reader awaits it (alongside the
    /// present-range watch) so a range no live pull will fill fails the read rather
    /// than hanging. Handed out as an owned `Arc<Notify>` so the node's data reader
    /// need not name the cache-private [`HashOutboard`].
    #[must_use]
    pub fn liveness_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.outboard().liveness)
    }

    /// Whether any LIVE fill of this hash still covers `range`, so a not-yet-present
    /// node / leaf in `range` may still be filled. A registry-bound session consults
    /// EVERY live session for the hash (the N-fill generalization); a standalone
    /// session (unregistered, in tests) is the only fill, so it answers from its own
    /// liveness. Returns `false` when nothing live covers `range` — the caller then
    /// fails the read instead of hanging.
    #[must_use]
    pub fn range_still_live(&self, range: &ChunkRanges) -> bool {
        match self.registry.get() {
            Some((weak, hash)) => match weak.upgrade() {
                Some(registry) => registry.range_still_live(*hash, range),
                // The registry (the whole cache) is gone; nothing can fill anything.
                None => false,
            },
            // Standalone: this session is the only fill, so its liveness decides.
            None => !self.is_dead() && !(&self.covered_ranges() & range).is_empty(),
        }
    }

    /// Capture one internal node's `(left, right)` hash pair into the per-hash
    /// outboard. Idempotent; a leaf (no outboard slot) is ignored. Every
    /// `admit_bao_stream` for the hash, from any pull or source, calls this — so the
    /// one shared buffer accumulates the whole tree's proof.
    pub fn capture(&self, node: TreeNode, pair: (blake3::Hash, blake3::Hash)) {
        self.outboard().capture(node, pair);
    }

    /// Capture a whole admit's node pairs into the per-hash outboard in one shot,
    /// waking parked serve legs once for the batch rather than once per node. The
    /// admit path collects an admitted range's proof nodes and hands them here, so a
    /// large range's fill no longer fires a wake per node it carries.
    pub fn capture_many(
        &self,
        pairs: impl IntoIterator<Item = (TreeNode, (blake3::Hash, blake3::Hash))>,
    ) {
        self.outboard().capture_many(pairs);
    }

    /// Record the pull leg's terminal outcome and wake parked readers. Idempotent
    /// on the wake; the outcome is set once by the pull leg on its single exit.
    /// Fires the per-hash liveness so a reader awaiting a range this fill covered
    /// re-checks [`Self::range_still_live`] at once.
    pub fn mark_ended(&self, result: Result<(), FillError>) {
        {
            let mut guard = self.ended.lock().unwrap_or_else(PoisonError::into_inner);
            *guard = Some(result);
        }
        self.outboard().liveness.notify_waiters();
    }

    /// The pull leg's terminal outcome, if it has ended: `Some(Ok)` clean,
    /// `Some(Err)` failed. `None` while the pull is still running.
    #[must_use]
    pub fn outcome(&self) -> Option<Result<(), FillError>> {
        self.ended
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Serve-side [`Outboard`] over a hash's shared [`HashOutboard`]. Its
/// [`Outboard::load`] awaits a not-yet-captured node, racing the registry-wide fill
/// liveness for the no-hang guarantee: it fails iff no live fill still covers the
/// node's byte range. Minted via [`FillSession::outboard_reader`].
#[derive(Debug)]
pub struct SessionOutboardReader {
    session: Arc<FillSession>,
    /// The shared per-hash outboard, snapshot at mint (after the session adopted the
    /// canonical one), so every `load` reads whatever any pull for the hash captured.
    outboard: Arc<HashOutboard>,
}

impl SessionOutboardReader {
    /// The terminal error for a node no live fill will supply: a precise message
    /// when this session's own pull ended (the standalone / single-fill case), a
    /// generic one when the covering fills were siblings.
    fn dead_range_error(&self, node: TreeNode) -> io::Error {
        match self.session.outcome() {
            Some(Err(msg)) => io::Error::other(format!(
                "upstream pull failed before supplying outboard node {node:?}: {msg}"
            )),
            Some(Ok(())) => io::Error::other(format!(
                "upstream pull completed but outboard node {node:?} was never captured"
            )),
            None => io::Error::other(format!(
                "no live fill covers outboard node {node:?}; every covering pull ended"
            )),
        }
    }
}

impl Outboard for SessionOutboardReader {
    fn root(&self) -> blake3::Hash {
        self.outboard.root
    }

    fn tree(&self) -> BaoTree {
        self.outboard.tree
    }

    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.outboard.tree.pre_order_offset(node) else {
            return Ok(None); // leaf: bao_tree sources this from the data reader
        };
        let idx = usize::try_from(offset).unwrap_or(usize::MAX);
        let node_range = ChunkRanges::from(node.chunk_range());
        loop {
            // Register the wakers BEFORE inspecting shared state, so a capture /
            // fill-end recorded concurrently cannot slip between the check and the
            // await.
            let captured = self.outboard.captured.notified();
            let liveness = self.outboard.liveness.notified();
            tokio::pin!(captured);
            tokio::pin!(liveness);
            captured.as_mut().enable();
            liveness.as_mut().enable();

            if let Some(pair) = self.outboard.try_load(idx) {
                return Ok(Some(pair));
            }

            // Not captured yet. If no live fill still covers this node's range, the
            // proof node will never arrive — decide now. A clean end may lag one
            // capture behind the outcome, so re-check once before failing.
            if !self.session.range_still_live(&node_range) {
                if let Some(pair) = self.outboard.try_load(idx) {
                    return Ok(Some(pair));
                }
                return Err(self.dead_range_error(node));
            }

            // A pull captures this pair only once it fetches into the node's range.
            // Ask for the first byte of that range: a pull whose window has closed
            // would otherwise wait for a payment that this parked encode blocks.
            let node_start = node.chunk_range().start.to_bytes();
            self.session
                .demand_up_to(node_start.saturating_add(1).min(self.outboard.tree.size()));

            // Await the next capture or a liveness change, then re-check.
            tokio::select! {
                biased;
                () = liveness.as_mut() => {}
                () = captured.as_mut() => {}
            }
        }
    }
}

/// RAII observer handle on a [`FillSession`]. One is minted for the pull owner and
/// one per attached serve leg ([`FillRegistry::claim`]). Releasing it decrements the
/// session's observer count; the last-out release, if the pull is still running,
/// cancels the pull so a fill no client is waiting on stops ingesting (#1610) — and
/// hands back the session's parked pull-thread handle so THAT caller joins it
/// off-task (the owner-join hand-off: whoever leaves LAST joins the pull, freeing an
/// owner whose own client finished first).
///
/// The decrement + cancel decision runs UNDER the registry map lock — the same
/// lock `claim`/`register_fill` hold when they attach an observer. That mutual
/// exclusion closes the resurrection race: a concurrent claim cannot `fetch_add` a
/// session 0→1 in the window between this drop's last-out `fetch_sub` (1→0) and its
/// `cancel()`, so no live observer can end up bound to a cancelled fill. The lease
/// therefore holds a [`Weak`] back to the registry (plus the session's `hash`) to
/// reach that lock at drop. If the upgrade fails the registry is gone, so the fill
/// is moot and the drop just skips. It only TAKES the pull handle under the lock;
/// the JOIN always runs off it.
#[derive(Debug)]
pub struct ObserverLease {
    session: Arc<FillSession>,
    registry: Weak<FillRegistry>,
    hash: Hash,
    /// Cleared once teardown has run. [`Self::release`] consumes the lease and runs
    /// teardown explicitly; the value then still `Drop`s, so this flag makes the
    /// second pass a no-op — the decrement fires exactly once.
    armed: bool,
}

impl ObserverLease {
    /// Release the lease at a serve leg's teardown, handing back the parked
    /// pull-thread handle when THIS release is the last one out. The caller joins that
    /// handle OFF the registry map lock and off its accept task (on a blocking
    /// thread). A non-last-out release returns `None`, so an owner whose own client
    /// finished first returns at once, leaving the pull filling for the remaining
    /// observers — the last of which cancels it (#1610 preserved). Named so teardown
    /// reads as intent rather than an incidental drop.
    #[must_use]
    pub fn release(mut self) -> Option<std::thread::JoinHandle<()>> {
        self.teardown()
    }

    /// The last-out decrement + cancel, shared by [`Self::release`] and [`Drop`].
    /// Runs under the registry map lock — the SAME lock `claim`/`register_fill` hold
    /// to attach — so the decrement, the map removal, and the cancel decision cannot
    /// interleave with an attach (the #1610 resurrection-race fix). It only TAKES the
    /// pull handle here; it never JOINS under the lock. Idempotent via `armed`.
    fn teardown(&mut self) -> Option<std::thread::JoinHandle<()>> {
        if !self.armed {
            return None;
        }
        self.armed = false;
        let Some(registry) = self.registry.upgrade() else {
            // Registry dropped — the fill (and its map entry) are gone; nothing can
            // attach, so the decrement + cancel are moot. Skip.
            return None;
        };
        // Take the SAME lock `claim`/`register_fill` hold to attach, so the
        // decrement, the map removal, and the cancel decision cannot interleave with
        // an attach.
        let mut map = registry.map.lock().unwrap_or_else(PoisonError::into_inner);
        // `fetch_sub` returns the PREVIOUS value; `1` means this drop took the
        // count to 0.
        let prev = self.session.observers.fetch_sub(1, Ordering::AcqRel);
        if prev != 1 {
            return None;
        }
        // Last observer left: free the session from the map so dead sessions do not
        // accumulate forever. Remove by pointer identity so a sibling session for the
        // same hash survives; drop the `hash` key if its Vec empties (the per-hash
        // outboard then drops once every reader releases it).
        if let Some(entry) = map.get_mut(&self.hash) {
            entry.sessions.retain(|s| !Arc::ptr_eq(s, &self.session));
            if entry.sessions.is_empty() {
                map.remove(&self.hash);
            }
        }
        // Cancel only if the pull has not already ended — a completed pull needs no
        // cancel, and a failed one already terminated. The session is removed from the
        // map regardless. Firing the per-hash liveness wakes any reader parked on a
        // range this fill covered so it re-checks at once.
        if self.session.outcome().is_none() {
            self.session.cancel.cancel();
        }
        self.session.outboard().liveness.notify_waiters();
        // Hand the parked pull-thread handle to this last-out caller to join off-task.
        // Taking (not joining) it here keeps the map lock free of a blocking join.
        self.session.take_pull_handle()
    }
}

impl Drop for ObserverLease {
    fn drop(&mut self) {
        let _ = self.teardown();
    }
}

/// The atomic outcome of [`FillRegistry::claim`]: a serve-miss either coalesces
/// wholly onto live pulls (`Attach`), owns a fresh pull for its whole range
/// (`Owner`), or — on a partial overlap — owns a fresh pull for its non-overlapping
/// REMAINDER while attaching to a live sibling for the overlap (`Mixed`). Every
/// variant carries the [`FillSession`]s and [`ObserverLease`]s the caller holds
/// for the life of its serve leg. `claim` decides the split AND registers any new
/// owner session under ONE map-lock acquisition, so two concurrent fresh misses for
/// the same blob cannot both open a pull.
#[derive(Debug)]
pub enum FillClaim {
    /// A live pull already covers the whole request; run a serve leg over `session`
    /// and open NO new pull. `lease` is an observer lease on the existing fill.
    Attach {
        /// The live session to serve from (largest overlap with the request).
        session: Arc<FillSession>,
        /// The observer lease held for the life of the attaching serve leg.
        lease: ObserverLease,
    },
    /// No live pull covers the request; the caller owns the freshly-registered
    /// `session` (scoped to the WHOLE request) and must spawn the pull. `lease` is
    /// the owner lease (observer count starts at 1).
    Owner {
        /// The freshly-registered session the caller must drive a pull for.
        session: Arc<FillSession>,
        /// The owner lease (count 1) held for the life of the owning pull.
        lease: ObserverLease,
    },
    /// A partial overlap: a live sibling covers a prefix/suffix of the request. The
    /// caller OWNS a fresh pull for the contiguous `remainder` (`[remainder_offset,
    /// remainder_offset + remainder_len)`) and ATTACHES to the sibling for the
    /// overlap, serving the whole request from the shared per-hash cache + outboard.
    /// Two pulls, each fetching its bytes once.
    Mixed {
        /// The freshly-registered session covering the remainder; drive its pull.
        owner: Arc<FillSession>,
        /// The owner lease (count 1) for the remainder pull.
        owner_lease: ObserverLease,
        /// The live sibling covering the overlap; its pull is shared, not re-opened.
        attach: Arc<FillSession>,
        /// The observer lease on the sibling, held for the life of the serve leg.
        attach_lease: ObserverLease,
        /// The remainder's byte offset — the pull leg fetches `[offset, offset+len)`.
        remainder_offset: u64,
        /// The remainder's byte length.
        remainder_len: u64,
    },
}

/// Total chunk count spanned by a (disjoint, sorted) chunk-range set. Boundaries
/// come in `[start, end)` pairs; a trailing unpaired boundary (an open-ended set)
/// contributes nothing, which is fine for the finite `covered ∩ R` overlaps this
/// measures.
fn chunk_span(ranges: &ChunkRanges) -> u64 {
    ranges
        .boundaries()
        .chunks_exact(2)
        .map(|pair| match pair {
            [start, end] => end.0.saturating_sub(start.0),
            _ => 0,
        })
        .sum()
}

/// The single contiguous byte span `[offset, offset+len)` of a chunk-range set, or
/// `None` if it is empty or split into two or more disjoint pieces. A `Mixed` claim
/// only opens a remainder pull when the remainder is one contiguous range (a
/// prefix/suffix overlap), because the pull leg fetches one `[offset, len)` gap; a
/// remainder split by an interior overlap falls back to a conservative whole-request
/// pull. `total` clamps the final (possibly ragged) group to the blob end.
fn contiguous_byte_span(ranges: &ChunkRanges, total: u64) -> Option<(u64, u64)> {
    match ranges.boundaries() {
        [start, end] => {
            let offset = start.0.saturating_mul(CHUNK_BYTES);
            let end_bytes = end.0.saturating_mul(CHUNK_BYTES).min(total);
            Some((offset, end_bytes.saturating_sub(offset)))
        }
        _ => None,
    }
}

/// One hash's live in-flight fills plus the ONE per-hash outboard they all capture
/// into and serve from.
#[derive(Debug)]
struct HashEntry {
    /// The canonical per-hash outboard, adopted by every session for this hash.
    outboard: Arc<HashOutboard>,
    /// The live fills for this hash (each a distinct pull). Removed by pointer
    /// identity when a session's last observer leaves.
    sessions: Vec<Arc<FillSession>>,
}

/// Range-aware coalescing registry: the live in-flight fills, keyed by hash, plus a
/// per-hash captured outboard. A serve-miss consults [`Self::claim`] to split its
/// range into an attach (covered by a running pull) and a remainder (fetch it), and
/// [`Self::range_still_live`] answers a parked reader's "keep awaiting or fail?".
/// Purely synchronous range math under a std `Mutex`; the lock is never held across
/// an await.
#[derive(Debug, Default)]
pub struct FillRegistry {
    map: StdMutex<HashMap<Hash, HashEntry>>,
}

impl FillRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: StdMutex::new(HashMap::new()),
        }
    }

    /// Increment `session`'s observer count and mint its RAII lease. The caller
    /// MUST already hold the map lock — the increment shares the lock the lease's
    /// drop takes for its decrement, so attach and teardown are mutually exclusive.
    fn mint_lease(self: &Arc<Self>, session: &Arc<FillSession>, hash: Hash) -> ObserverLease {
        session.observers.fetch_add(1, Ordering::AcqRel);
        ObserverLease {
            session: Arc::clone(session),
            registry: Arc::downgrade(self),
            hash,
            armed: true,
        }
    }

    /// Insert a freshly-built `session` for `hash` under the held map lock: adopt the
    /// hash's canonical outboard (or seed it from this session if it is the first),
    /// bind the session's registry back-link, and publish it. The session's
    /// `covered` must already be set by the caller.
    fn insert_session(
        self: &Arc<Self>,
        map: &mut HashMap<Hash, HashEntry>,
        hash: Hash,
        session: &Arc<FillSession>,
    ) {
        let entry = map.entry(hash).or_insert_with(|| HashEntry {
            outboard: session.outboard(),
            sessions: Vec::new(),
        });
        session.adopt_outboard(Arc::clone(&entry.outboard));
        session.bind_registry(Arc::downgrade(self), hash);
        entry.sessions.push(Arc::clone(session));
    }

    /// Publish a new pull's `session` (already scoped via [`FillSession::set_covered`]
    /// to the bytes it will fetch) and return the owner lease (observer count starts
    /// at 1). The entry stays in the map until the owning pull is joined node-side.
    /// A test-facing helper; production registers via [`Self::claim`].
    pub fn register_fill(
        self: &Arc<Self>,
        hash: Hash,
        session: &Arc<FillSession>,
    ) -> ObserverLease {
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        self.insert_session(&mut map, hash, session);
        self.mint_lease(session, hash)
    }

    /// The blob length of a LIVE in-flight fill for `hash`, if one runs — an
    /// ADVISORY peek the serve-miss path uses to skip its upstream header handshake
    /// when a pull it can coalesce onto already exists. Returns the `total_bytes` of
    /// the first non-dead session under `hash` (every session for one hash shares
    /// the blob's geometry, so any live one's length is THE length); `None` when no
    /// live fill exists (empty, or only dead sessions remain). Advisory only: a
    /// concurrent last-observer drop can retire the peeked session before the
    /// caller's atomic [`Self::claim`], which then owns a fresh pull and opens the
    /// leg late — so this NEVER replaces `claim`, it only removes the handshake in
    /// the common coalescing case.
    #[must_use]
    pub fn in_flight_total(&self, hash: Hash) -> Option<u64> {
        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        map.get(&hash)?
            .sessions
            .iter()
            .find(|session| !session.is_dead())
            .map(|session| session.total_bytes())
    }

    /// Raise the serve-demand frontier of every live fill of `hash` to `end`
    /// ([`FillSession::demand_up_to`]).
    fn raise_demand(&self, hash: Hash, end: u64) {
        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = map.get(&hash) {
            for session in entry.sessions.iter().filter(|s| !s.is_dead()) {
                session.raise_own_demand(end);
            }
        }
    }

    /// Whether any LIVE fill of `hash` still covers `range`. A parked reader calls
    /// this (via [`FillSession::range_still_live`]) to decide "keep awaiting a
    /// capture / present-range advance, or fail because no pull will ever fill this".
    /// `true` iff some live session's `covered` intersects `range`.
    #[must_use]
    pub fn range_still_live(&self, hash: Hash, range: &ChunkRanges) -> bool {
        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        match map.get(&hash) {
            Some(entry) => entry
                .sessions
                .iter()
                .any(|s| !s.is_dead() && !(&s.covered_ranges() & range).is_empty()),
            None => false,
        }
    }

    /// Atomically decide attach/own/mixed AND register any new owner session, all
    /// under ONE map-lock acquisition. This closes the coalescing TOCTOU race a
    /// separate plan-then-register pair would leave open: between two lock
    /// acquisitions another thread can register, so two concurrent fresh misses for
    /// the same blob would both see an empty registry and both open a pull. `claim`
    /// holds the lock across the whole decision, so exactly one of two racing
    /// identical claims registers an owner and the other attaches.
    ///
    /// `R = align_range(offset, len, total)`. On an align error or empty `R`, the
    /// caller takes the Owner path (its own fetch surfaces any out-of-bounds error).
    /// Otherwise, over LIVE sessions, `covered_union = ⋃ covered`,
    /// `attach = R ∩ covered_union`, `remainder = R − covered_union`:
    /// - `attach` empty → OWNER of the whole `R`.
    /// - `remainder` empty (a live pull covers all of `R`) → ATTACH to the
    ///   largest-overlap session; `make_session` is NOT called.
    /// - both non-empty, and the largest-overlap sibling covers ALL of `attach`, and
    ///   `R − sibling.covered` is ONE contiguous span → MIXED: own a pull for that
    ///   remainder, attach the sibling for the overlap.
    /// - otherwise (a multi-sibling union, or an interior overlap splitting the
    ///   remainder in two) → OWNER of the whole `R`, the conservative fallback that
    ///   never double-pulls or wedges. Range-serving lands the general case later.
    ///
    /// `make_session` runs under the lock only on an owning branch (never
    /// built-and-dropped on a pure attach); it does no await (a std lock) and only
    /// allocates, so holding the lock across it is safe.
    pub fn claim(
        self: &Arc<Self>,
        hash: Hash,
        offset: u64,
        len: u64,
        total: u64,
        make_session: impl FnOnce() -> Arc<FillSession>,
    ) -> FillClaim {
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);

        // R = the chunk ranges the request spans. An align error (out-of-bounds) or
        // an empty R has no coalescable range, so fall straight to the Owner path.
        let r = match align_range(offset, len, total) {
            Ok(aligned) => aligned.chunk_ranges().clone(),
            Err(_) => ChunkRanges::empty(),
        };

        if !r.is_empty() {
            let mut covered_union = ChunkRanges::empty();
            let mut best: Option<(Arc<FillSession>, ChunkRanges, u64)> = None;
            if let Some(entry) = map.get(&hash) {
                for session in &entry.sessions {
                    // Dead sessions (ended or cancelled) may never deliver, so their
                    // `covered` neither blocks a fresh pull nor is an attach target.
                    // Reading `is_dead` under the map lock pins it against a
                    // concurrent last-out lease drop (which fires `cancel()` under
                    // this same lock).
                    if session.is_dead() {
                        continue;
                    }
                    let covered = session.covered_ranges();
                    let overlap = &r & &covered;
                    if !overlap.is_empty() {
                        let span = chunk_span(&overlap);
                        if best
                            .as_ref()
                            .is_none_or(|(_, _, best_span)| span > *best_span)
                        {
                            best = Some((Arc::clone(session), covered.clone(), span));
                        }
                    }
                    covered_union |= covered;
                }
            }

            let attach = &r & &covered_union;
            let remainder = &r - &covered_union;

            if !attach.is_empty()
                && let Some((sibling, sib_covered, _)) = best
            {
                if remainder.is_empty() {
                    // Whole R covered by live pulls → pure ATTACH to the sibling.
                    let lease = self.mint_lease(&sibling, hash);
                    return FillClaim::Attach {
                        session: sibling,
                        lease,
                    };
                }
                // Partial overlap. Coalesce only when ONE sibling covers all of the
                // overlap AND the bytes it does not cover form ONE contiguous span
                // the remainder pull can fetch as a single gap. Otherwise fall
                // through to a conservative whole-R owner (no double-pull, no wedge).
                let attach_covered_by_sibling = (&attach - &sib_covered).is_empty();
                let remainder_vs_sibling = &r - &sib_covered;
                if attach_covered_by_sibling
                    && let Some((rem_offset, rem_len)) =
                        contiguous_byte_span(&remainder_vs_sibling, total)
                {
                    let owner = make_session();
                    owner.set_covered(remainder_vs_sibling);
                    self.insert_session(&mut map, hash, &owner);
                    let owner_lease = self.mint_lease(&owner, hash);
                    let attach_lease = self.mint_lease(&sibling, hash);
                    return FillClaim::Mixed {
                        owner,
                        owner_lease,
                        attach: sibling,
                        attach_lease,
                        remainder_offset: rem_offset,
                        remainder_len: rem_len,
                    };
                }
            }
        }

        // OWNER: no overlap, an empty/unalignable R, or the conservative fallback.
        // Scope the session to the WHOLE R (so a later identical claim attaches),
        // register + mint the owner lease under the SAME lock.
        let owner = make_session();
        owner.set_covered(r);
        self.insert_session(&mut map, hash, &owner);
        let lease = self.mint_lease(&owner, hash);
        FillClaim::Owner {
            session: owner,
            lease,
        }
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
    use super::{FillError, FillSession, IROH_BLOCK_SIZE};
    use bao_tree::io::fsm::Outboard;
    use bao_tree::{BaoTree, blake3};

    fn h(byte: u8) -> blake3::Hash {
        blake3::Hash::from([byte; 32])
    }

    /// A blob spanning several chunk groups, so the tree has interior nodes with
    /// real pre-order offsets to save and read back.
    const TOTAL: u64 = 5 * 16 * 1024 + 321;

    #[tokio::test]
    async fn capture_then_load_round_trips_each_internal_node() {
        let session = FillSession::new(h(0xAA), TOTAL);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);

        // Capture a distinct pair into every internal node, then read them all back.
        let mut saved = Vec::new();
        for (i, node) in tree.pre_order_nodes_iter().enumerate() {
            if tree.pre_order_offset(node).is_some() {
                let tag = u8::try_from(i % 251).unwrap();
                let pair = (h(tag), h(tag.wrapping_add(101)));
                session.capture(node, pair);
                saved.push((node, pair));
            }
        }
        for (node, pair) in saved {
            assert_eq!(reader.load(node).await.unwrap(), Some(pair));
        }
    }

    #[tokio::test]
    async fn outboard_buffer_is_allocated_lazily_on_first_capture() {
        // A freshly built session (as `make_session` builds one under the global
        // registry lock) must allocate no outboard buffer — the MB-scale zeroing
        // is deferred to the first capture, off that lock.
        let session = FillSession::new(h(0xDD), TOTAL);
        assert!(
            session.outboard().state.lock().unwrap().bytes.is_empty(),
            "construction allocates no outboard buffer under the registry lock"
        );

        // The first capture sizes the buffer to the full pre-order outboard.
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");
        session.capture(node, (h(1), h(2)));
        assert_eq!(
            session.outboard().state.lock().unwrap().bytes.len() as u64,
            tree.outboard_size(),
            "the first capture sizes the buffer to the whole outboard"
        );
    }

    #[tokio::test]
    async fn capture_many_round_trips_each_internal_node() {
        // One batched `capture_many` must land every pair a per-node `capture` loop
        // would, so a serve leg reads back the whole tree the same way.
        let session = FillSession::new(h(0xBB), TOTAL);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);

        let mut batch = Vec::new();
        for (i, node) in tree.pre_order_nodes_iter().enumerate() {
            if tree.pre_order_offset(node).is_some() {
                let tag = u8::try_from(i % 251).unwrap();
                batch.push((node, (h(tag), h(tag.wrapping_add(101)))));
            }
        }
        session.capture_many(batch.clone());
        for (node, pair) in batch {
            assert_eq!(reader.load(node).await.unwrap(), Some(pair));
        }
    }

    #[tokio::test]
    async fn load_awaits_then_resolves_on_capture_many() {
        // A parked reader must wake from the batch's single notify, not only from a
        // per-node one — the notify fires once for the whole admit.
        let session = FillSession::new(h(0xCC), TOTAL);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");
        let pair = (h(7), h(9));

        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        assert!(
            !load.is_finished(),
            "load must park until the node is captured"
        );
        session.capture_many([(node, pair)]);
        assert_eq!(load.await.unwrap().unwrap(), Some(pair));
    }

    #[tokio::test]
    async fn leaf_node_loads_none_without_awaiting() {
        // A single-chunk-group blob is one leaf node with no interior hash pairs, so
        // `load(root)` must return `None` immediately (bao_tree sources the leaf from
        // the data reader) and never park — even though nothing was ever captured.
        let small = 4 * 1024;
        let session = FillSession::new(h(1), small);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(small, IROH_BLOCK_SIZE);
        let root = tree.root();
        assert!(
            tree.pre_order_offset(root).is_none(),
            "a one-group tree's root is a leaf"
        );
        assert_eq!(reader.load(root).await.unwrap(), None);
    }

    #[tokio::test]
    async fn load_awaits_then_resolves_on_capture() {
        let session = FillSession::new(h(2), TOTAL);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");
        let pair = (h(7), h(9));

        // Load races ahead of capture: it must block, then wake on capture.
        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        assert!(
            !load.is_finished(),
            "load must park until the node is captured"
        );
        session.capture(node, pair);
        assert_eq!(load.await.unwrap().unwrap(), Some(pair));
    }

    #[tokio::test]
    async fn load_fails_when_pull_ends_err_before_capture() {
        let session = FillSession::new(h(3), TOTAL);
        let mut reader = session.outboard_reader();
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");

        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        session.mark_ended(Err(FillError::new("upstream died")));
        let err = load
            .await
            .unwrap()
            .expect_err("a failed pull must fail the load");
        assert!(err.to_string().contains("upstream pull failed"));
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod fill_registry_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use bao_tree::io::fsm::Outboard;
    use bao_tree::{BaoTree, ChunkRanges, blake3};
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range};

    use super::{FillClaim, FillError, FillRegistry, FillSession};
    use crate::{CHUNK_GROUP_BYTES, Hash};

    /// One chunk group of bytes, the alignment granularity `claim` snaps to.
    const G: u64 = CHUNK_GROUP_BYTES;

    fn store_hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    /// Whether the registry map still holds an entry (a non-empty session Vec) for
    /// `hash`. Reaches the private `map` field directly — the discriminating check
    /// for last-observer removal, which a coverage query alone cannot distinguish
    /// from the `is_dead` skip.
    fn mapped(reg: &FillRegistry, hash: Hash) -> bool {
        reg.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .is_some_and(|entry| !entry.sessions.is_empty())
    }

    fn root(byte: u8) -> blake3::Hash {
        blake3::Hash::from([byte; 32])
    }

    fn hb(byte: u8) -> blake3::Hash {
        blake3::Hash::from([byte; 32])
    }

    /// The chunk ranges covering the byte span `[start, start+len)` of a `total`
    /// blob (`len == 0` = to end), built via the same `align_range` the registry
    /// uses so `covered` and the expected splits cannot drift.
    fn ranges(start: u64, len: u64, total: u64) -> ChunkRanges {
        align_range(start, len, total)
            .expect("aligned range")
            .chunk_ranges()
            .clone()
    }

    /// The first interior node whose byte span lies wholly inside the chunk range
    /// `[start_group, end_group)` (in groups), for exercising per-hash proof reads.
    fn interior_node_in(total: u64, start_group: u64, end_group: u64) -> bao_tree::TreeNode {
        let tree = BaoTree::new(total, IROH_BLOCK_SIZE);
        let want = ranges(start_group * G, (end_group - start_group) * G, total);
        tree.pre_order_nodes_iter()
            .find(|n| {
                tree.pre_order_offset(*n).is_some()
                    && (&ChunkRanges::from(n.chunk_range()) - &want).is_empty()
            })
            .expect("an interior node inside the range")
    }

    /// A registered session is bound to the registry, so `range_still_live` consults
    /// the whole registry (not just the session's own liveness).
    fn register(
        reg: &Arc<FillRegistry>,
        hash: Hash,
        root: blake3::Hash,
        total: u64,
        cov: ChunkRanges,
    ) -> (Arc<FillSession>, super::ObserverLease) {
        let s = FillSession::new(root, total);
        s.set_covered(cov);
        let lease = reg.register_fill(hash, &s);
        (s, lease)
    }

    /// A proof read parked on an uncaptured node demands the first byte of that
    /// node's range, so a pull whose window has closed still fetches far enough to
    /// capture the pair. Without it the encode waits on the pull and the pull waits
    /// on a payment the parked encode blocks (#1893).
    #[tokio::test]
    async fn a_parked_proof_read_demands_the_first_byte_of_its_node() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x4A);
        let (owner, _ol) = register(&reg, hash, hb(0x4A), total, ranges(0, 0, total));

        let node = interior_node_in(total, 4, 8);
        let node_start = node.chunk_range().start.to_bytes();
        assert!(node_start >= 4 * G, "the node lies past the first half");
        let mut reader = owner.outboard_reader();

        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        assert!(!load.is_finished(), "load parks until the node is captured");
        assert_eq!(
            owner.serve_demand().load(Ordering::Acquire),
            node_start + 1,
            "the parked read demands one byte into the node's range"
        );

        let pair = (hb(3), hb(4));
        owner.capture(node, pair);
        assert_eq!(load.await.unwrap().unwrap(), Some(pair));
    }

    /// Serve demand reaches every LIVE fill of the hash, never regresses, and skips a
    /// dead fill: under coalescing the pull that produces the awaited span may be a
    /// sibling of the reader's own session.
    #[test]
    fn demand_reaches_every_live_fill_and_never_regresses() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x4B);
        let (a, _al) = register(&reg, hash, hb(0x4B), total, ranges(0, 4 * G, total));
        let (b, _bl) = register(&reg, hash, hb(0x4B), total, ranges(4 * G, 4 * G, total));
        let (dead, _dl) = register(&reg, hash, hb(0x4B), total, ranges(2 * G, 2 * G, total));
        dead.mark_ended(Err(FillError::new("ended before the demand")));

        a.demand_up_to(5 * G);
        assert_eq!(a.serve_demand().load(Ordering::Acquire), 5 * G);
        assert_eq!(
            b.serve_demand().load(Ordering::Acquire),
            5 * G,
            "a live sibling receives the demand"
        );
        assert_eq!(
            dead.serve_demand().load(Ordering::Acquire),
            0,
            "a dead fill is skipped"
        );

        b.demand_up_to(3 * G);
        assert_eq!(
            a.serve_demand().load(Ordering::Acquire),
            5 * G,
            "a lower demand never regresses the frontier"
        );
    }

    /// Two sessions for one hash share the ONE per-hash outboard: a node captured via
    /// the first session is loadable through a reader minted from the second. This is
    /// the precondition for partial-overlap serving — a serve leg reads proof no
    /// matter which pull captured it.
    #[tokio::test]
    async fn siblings_share_one_per_hash_outboard() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x40);

        let (a, _al) = register(&reg, hash, hb(0x40), total, ranges(0, 4 * G, total));
        let (b, _bl) = register(&reg, hash, hb(0x40), total, ranges(4 * G, 4 * G, total));

        // Capture an interior node through A; a reader minted from B must load it.
        let node = interior_node_in(total, 0, 8);
        let pair = (hb(1), hb(2));
        a.capture(node, pair);

        let mut reader_b = b.outboard_reader();
        assert_eq!(
            reader_b.load(node).await.unwrap(),
            Some(pair),
            "B's reader sees a node A captured — one shared per-hash outboard"
        );
    }

    /// A reader minted from a session that does NOT cover a node's range must FAIL
    /// when the sibling that DOES cover it dies — even though the minting session is
    /// still live. This is the N-fill termination the coherent encoder needs so a
    /// partial-overlap serve fails (never hangs) if a coalesced sibling pull dies.
    #[tokio::test]
    async fn reader_fails_when_the_only_covering_sibling_dies() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x41);

        // Owner covers [0,4g); sibling covers [4g,8g).
        let (owner, _ol) = register(&reg, hash, hb(0x41), total, ranges(0, 4 * G, total));
        let (sibling, _sl) = register(&reg, hash, hb(0x41), total, ranges(4 * G, 4 * G, total));

        // A node wholly inside [4g,8g) — covered only by the sibling.
        let node = interior_node_in(total, 4, 8);
        let mut reader = owner.outboard_reader();

        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        assert!(
            !load.is_finished(),
            "load parks until the sibling supplies it"
        );

        // The sibling dies without capturing the node; the owner (which does not
        // cover it) is still live. The read must fail, not hang.
        sibling.mark_ended(Err(FillError::new("sibling upstream died")));
        let err = load
            .await
            .unwrap()
            .expect_err("no live fill covers the node — the read must fail");
        assert!(err.to_string().contains("no live fill covers"));
    }

    /// (a) Same range: a second serve-miss for the exact range an in-flight pull
    /// covers attaches wholly and raises the count to 2 (`claim`).
    #[test]
    fn claim_same_range_attaches() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xA1);

        let FillClaim::Owner {
            session,
            lease: _owner,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xA1), total))
        else {
            panic!("first whole-range claim owns");
        };
        let FillClaim::Attach {
            session: attached,
            lease: _l,
        } = reg.claim(hash, 0, 0, total, || {
            panic!("attach must not build a session")
        })
        else {
            panic!("second identical claim attaches");
        };
        assert!(Arc::ptr_eq(&attached, &session), "binds the same session");
        assert_eq!(session.observer_count(), 2, "owner + one attached observer");
    }

    /// (b) Disjoint halves: a claim for the second half of a pull that only covers
    /// the first half OWNS its own pull; no coalescing.
    #[test]
    fn claim_disjoint_halves_both_own() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xB2);

        let FillClaim::Owner {
            session: lo,
            lease: _lo,
        } = reg.claim(hash, 0, half, total, || FillSession::new(root(0xB2), total))
        else {
            panic!("first disjoint claim owns");
        };
        let FillClaim::Owner {
            session: hi,
            lease: _hi,
        } = reg.claim(hash, half, total - half, total, || {
            FillSession::new(root(0xB2), total)
        })
        else {
            panic!("disjoint second claim owns its own pull");
        };
        assert!(!Arc::ptr_eq(&lo, &hi), "distinct owner sessions");
        assert_eq!(lo.observer_count(), 1, "no cross-attach");
    }

    /// (c) Partial overlap (prefix): a sibling covers `[0,3g)`; a request for
    /// `[2g,5g)` MIXES — owns a pull for the contiguous remainder `[3g,5g)` and
    /// attaches the sibling for the `[2g,3g)` overlap.
    #[test]
    fn claim_partial_prefix_overlap_mixes() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xC3);

        let (sibling, _sl) = register(&reg, hash, hb(0xC3), total, ranges(0, 3 * G, total));

        let claim = reg.claim(hash, 2 * G, 3 * G, total, || {
            FillSession::new(hb(0xC3), total)
        });
        let FillClaim::Mixed {
            owner,
            attach,
            remainder_offset,
            remainder_len,
            ..
        } = claim
        else {
            panic!("a prefix overlap with a contiguous remainder mixes");
        };
        assert!(
            Arc::ptr_eq(&attach, &sibling),
            "attaches the overlapping sibling"
        );
        assert_eq!(
            owner.covered_ranges(),
            ranges(3 * G, 2 * G, total),
            "owner covers the remainder [3g,5g)"
        );
        assert_eq!((remainder_offset, remainder_len), (3 * G, 2 * G));
        assert_eq!(sibling.observer_count(), 2, "sibling owner + our attach");
        assert_eq!(owner.observer_count(), 1, "our own remainder pull");
    }

    /// An interior overlap splits the remainder into two disjoint pieces, which one
    /// `[offset, len)` pull cannot express — so `claim` conservatively OWNS the whole
    /// request rather than double-pull or wedge.
    #[test]
    fn claim_interior_overlap_falls_back_to_whole_owner() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xC4);

        // Sibling covers a MIDDLE slice [3g,4g).
        let (sibling, _sl) = register(&reg, hash, hb(0xC4), total, ranges(3 * G, G, total));

        // Request [0,8g): overlap [3g,4g), remainder [0,3g) ∪ [4g,8g) — two pieces.
        let FillClaim::Owner { session, lease: _l } =
            reg.claim(hash, 0, 0, total, || FillSession::new(hb(0xC4), total))
        else {
            panic!("a split remainder falls back to a whole-request owner");
        };
        assert_eq!(
            session.covered_ranges(),
            ranges(0, 0, total),
            "the fallback owner covers the whole request"
        );
        assert_eq!(
            sibling.observer_count(),
            1,
            "no attach on the fallback path"
        );
    }

    /// A subset claim (its `R` fully inside a live pull's `covered`) ATTACHES.
    #[test]
    fn claim_subset_attaches() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x32);

        let FillClaim::Owner {
            session: owner,
            lease: _o,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x32), total))
        else {
            panic!("first whole-range claim owns");
        };
        let FillClaim::Attach {
            session: attached,
            lease: _l,
        } = reg.claim(hash, 0, half, total, || panic!("subset must attach"))
        else {
            panic!("a subset of the covered range attaches");
        };
        assert!(Arc::ptr_eq(&attached, &owner), "attaches to the owner");
        assert_eq!(owner.observer_count(), 2, "owner + attached");
    }

    /// `make_session` MUST NOT run on the attach branch — a coalescing serve leg
    /// allocates no outboard buffer.
    #[test]
    fn claim_make_session_not_called_on_attach() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x34);

        let owner = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x34), total));
        assert!(matches!(owner, FillClaim::Owner { .. }), "first claim owns");
        let attach = reg.claim(hash, 0, 0, total, || {
            panic!("make_session must not be called on the attach branch")
        });
        assert!(
            matches!(attach, FillClaim::Attach { .. }),
            "second attaches"
        );
    }

    /// (d) Lease teardown: the last observer leaving before the pull ends cancels
    /// the pull and removes the session from the map; an earlier leaver does not.
    #[test]
    fn last_observer_leaving_cancels_and_removes() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xD4);

        let FillClaim::Owner {
            session,
            lease: owner,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xD4), total))
        else {
            panic!("owns");
        };
        assert_eq!(session.observer_count(), 1, "owner is one observer");
        assert!(mapped(&reg, hash), "registered session is mapped");

        let FillClaim::Attach {
            session: _a,
            lease: second,
        } = reg.claim(hash, 0, 0, total, || panic!("attach"))
        else {
            panic!("attaches");
        };
        assert_eq!(session.observer_count(), 2);

        drop(second);
        assert!(!session.is_cancelled(), "owner still waits — no cancel");
        assert_eq!(session.observer_count(), 1);
        assert!(mapped(&reg, hash), "non-last leaver keeps it mapped");

        drop(owner);
        assert!(
            session.is_cancelled(),
            "last observer left — pull cancelled"
        );
        assert_eq!(session.observer_count(), 0);
        assert!(!mapped(&reg, hash), "last observer left — session removed");
    }

    /// A sibling session for the same hash survives when one session's last observer
    /// leaves: removal is by pointer identity, and the hash key (and its shared
    /// outboard) stay while any session remains under it.
    #[test]
    fn removing_one_session_keeps_a_sibling_for_the_same_hash() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x29);

        let (first, first_owner) = register(&reg, hash, hb(0x29), total, ranges(0, half, total));
        let (second, _second_owner) = register(
            &reg,
            hash,
            hb(0x29),
            total,
            ranges(half, total - half, total),
        );

        drop(first_owner);
        assert!(mapped(&reg, hash), "sibling keeps the hash key alive");
        assert!(first.is_cancelled(), "the emptied session cancelled");

        // A serve-miss for the second half still attaches to the surviving sibling.
        let FillClaim::Attach {
            session: attached,
            lease: _l,
        } = reg.claim(hash, half, total - half, total, || panic!("attach"))
        else {
            panic!("attaches to the surviving sibling");
        };
        assert!(Arc::ptr_eq(&attached, &second), "the sibling is bound");
    }

    /// A cancelled session is dead: `claim` must NOT attach to it, and its `covered`
    /// must NOT suppress a fresh pull.
    #[test]
    fn claim_skips_cancelled_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xE5);

        let FillClaim::Owner {
            session,
            lease: owner,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xE5), total))
        else {
            panic!("owns");
        };
        drop(owner);
        assert!(session.is_cancelled(), "cancel fired on last-out drop");

        let FillClaim::Owner {
            session: fresh,
            lease: _l,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xE5), total))
        else {
            panic!("a dead session must not block a fresh owner");
        };
        assert!(!Arc::ptr_eq(&fresh, &session), "a genuinely fresh session");
    }

    /// `range_still_live` is false once every covering session is dead, and true
    /// while any live session still covers the range.
    #[test]
    fn range_still_live_tracks_covering_sessions() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x50);
        let probe = ranges(4 * G, 2 * G, total); // [4g,6g)

        let (a, _al) = register(&reg, hash, hb(0x50), total, ranges(0, 6 * G, total));
        let (b, _bl) = register(&reg, hash, hb(0x50), total, ranges(4 * G, 4 * G, total));
        assert!(reg.range_still_live(hash, &probe), "two live coverers");

        a.mark_ended(Ok(()));
        assert!(reg.range_still_live(hash, &probe), "b still covers [4g,6g)");
        b.mark_ended(Ok(()));
        assert!(!reg.range_still_live(hash, &probe), "no live coverer left");
    }

    /// `FillSession::total_bytes` reports the blob length the session was built with,
    /// so the attach path can sign its `StreamResponse` without a header handshake.
    #[test]
    fn total_bytes_reports_blob_length() {
        let total = 5 * G + 321;
        let session = FillSession::new(root(0x35), total);
        assert_eq!(session.total_bytes(), total);
    }

    /// The pull-thread handle is parked on the session and handed back to whichever
    /// observer leaves LAST. An owner whose own client finishes first (a non-last-out
    /// release) gets `None`, so its accept task returns at once while the pull keeps
    /// filling; the remaining observer, on its last-out release, takes the handle to
    /// join. This is the owner-join hand-off (#1664): the finished owner is no longer
    /// parked in the join while another observer streams.
    #[test]
    fn last_out_release_takes_the_pull_handle() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x40);

        let FillClaim::Owner {
            session,
            lease: owner,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x40), total))
        else {
            panic!("owns");
        };
        // Owner parks the pull-thread handle on the shared session after spawning it.
        session.set_pull_handle(std::thread::spawn(|| {}));

        // A second observer attaches.
        let FillClaim::Attach {
            session: _a,
            lease: observer,
        } = reg.claim(hash, 0, 0, total, || panic!("attach"))
        else {
            panic!("attaches");
        };
        assert_eq!(session.observer_count(), 2, "owner + attached");

        // Owner leaves FIRST (count 2 → 1, not last-out): no handle handed back, so its
        // accept task is freed immediately; the pull is NOT cancelled.
        assert!(
            owner.release().is_none(),
            "a non-last-out release must not take the pull handle"
        );
        assert_eq!(session.observer_count(), 1);
        assert!(
            !session.is_cancelled(),
            "an observer still streams — no cancel"
        );

        // The last observer leaving (count 1 → 0) takes the handle to join off-task and
        // cancels the pull.
        let handle = observer
            .release()
            .expect("the last-out release must hand back the pull handle");
        handle.join().expect("the parked pull thread joins");
        assert_eq!(session.observer_count(), 0);
        assert!(
            session.is_cancelled(),
            "last observer left — pull cancelled"
        );
    }

    /// The spawn-failure path parks no handle, so a last-out release must still be
    /// safe: it runs the decrement + cancel and simply returns `None` (nothing to
    /// join). Guards the owner error arm that releases its lease without ever storing
    /// a pull thread.
    #[test]
    fn last_out_release_without_a_parked_handle_returns_none() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x41);

        let FillClaim::Owner {
            session,
            lease: owner,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x41), total))
        else {
            panic!("owns");
        };
        assert!(
            owner.release().is_none(),
            "no handle parked → nothing to join"
        );
        assert!(
            session.is_cancelled(),
            "sole observer left — pull cancelled"
        );
    }

    /// `in_flight_total` peeks a LIVE fill's blob length so the serve-miss path can
    /// skip its upstream header handshake when it will coalesce onto that pull:
    /// `None` on an empty registry, `Some(total)` while a live pull runs, `None`
    /// once that pull's last observer leaves (the session is removed).
    #[test]
    fn in_flight_total_peeks_live_fill_length() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x40);

        assert_eq!(
            reg.in_flight_total(hash),
            None,
            "an empty registry has no in-flight fill"
        );

        let session = FillSession::new(root(0x40), total);
        session.set_covered(ranges(0, 0, total));
        let owner = reg.register_fill(hash, &session);
        assert_eq!(
            reg.in_flight_total(hash),
            Some(total),
            "a live fill reports its blob length"
        );

        // Last observer leaves -> the session is removed from the map -> nothing in
        // flight, so a fresh serve-miss must handshake.
        drop(owner);
        assert_eq!(
            reg.in_flight_total(hash),
            None,
            "a removed fill is not in flight"
        );
    }

    /// An ENDED session is dead: `in_flight_total` skips it even while it is still
    /// mapped, because there is nothing live to coalesce onto -- the caller must
    /// handshake.
    #[test]
    fn in_flight_total_skips_ended_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x41);

        let session = FillSession::new(root(0x41), total);
        session.set_covered(ranges(0, 0, total));
        let _owner = reg.register_fill(hash, &session);
        assert_eq!(reg.in_flight_total(hash), Some(total));

        session.mark_ended(Err(FillError::new("upstream died")));
        assert_eq!(
            reg.in_flight_total(hash),
            None,
            "an ended session is not attachable in flight"
        );
    }

    /// With a dead session and a LIVE sibling for the same hash, `in_flight_total`
    /// reports the live one's length -- the dead session is skipped, not the whole
    /// hash.
    #[test]
    fn in_flight_total_reports_live_sibling_past_a_dead_one() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x42);

        let dead = FillSession::new(root(0x42), total);
        dead.set_covered(ranges(0, half, total));
        let _dead_owner = reg.register_fill(hash, &dead);
        dead.mark_ended(Err(FillError::new("first pull died")));

        let live = FillSession::new(root(0x42), total);
        live.set_covered(ranges(half, total - half, total));
        let _live_owner = reg.register_fill(hash, &live);

        assert_eq!(
            reg.in_flight_total(hash),
            Some(total),
            "a live sibling is reported past the dead session"
        );
    }
}
