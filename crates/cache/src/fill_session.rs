//! `FillSession` — the cache-resident fill-coordination primitive the decoupled
//! serve-miss legs share (ADR 038).
//!
//! # Why this exists
//!
//! The decoupled serve leg must emit ONE coherent whole-range bao verified stream
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
//! this whole-tree buffer). The serve leg reads them through a
//! [`SessionOutboardReader`], whose [`Outboard::load`] AWAITS a node that has not been
//! captured yet — racing the pull's terminal signal so a pull that fails (or a
//! genuinely missing node after a clean pull) fails the serve rather than hanging.
//!
//! Because the pull admits front-to-back and the first admit carries the whole
//! right-spine down to the first gap, every node the encoder needs is captured by
//! an admit that has already happened by the time the encoder reaches it.
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
use std::sync::{Arc, Mutex as StdMutex, PoisonError, Weak};

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
#[derive(Debug)]
struct OutboardState {
    /// `tree.outboard_size()` bytes: node `n`'s pair lives at
    /// `tree.pre_order_offset(n) * 64`.
    bytes: Vec<u8>,
    /// `captured[i]` is set once node index `i` (its `pre_order_offset`) is saved.
    captured: Vec<bool>,
}

/// One in-flight fill of a `total_bytes`-byte blob rooted at `root`.
///
/// Holds the shared whole-tree outboard (captured by the cache admit path via
/// [`Self::capture`], read by [`SessionOutboardReader`]), the pull's terminal
/// outcome ([`Self::mark_ended`] / [`Self::outcome`]), and the shared downstream
/// paid frontier the pull leg paces against ([`Self::served_frontier`] /
/// [`Self::served_advanced`]). Shared behind an `Arc`; every field is
/// interior-mutable, so both legs hold `Arc<FillSession>` and coordinate on it.
#[derive(Debug)]
pub struct FillSession {
    tree: BaoTree,
    root: blake3::Hash,
    state: StdMutex<OutboardState>,
    /// Notified after every [`Self::capture`] so a parked [`Outboard::load`]
    /// re-checks. Runtime-agnostic, so it crosses the pull/serve runtimes safely.
    advanced: Notify,
    /// The pull leg's terminal outcome — `None` while running, `Some(Ok)` on a
    /// clean end, `Some(Err)` on a failure.
    ended: StdMutex<Option<Result<(), FillError>>>,
    /// Fired by [`Self::mark_ended`] so a parked reader / data reader re-checks.
    ended_notify: Notify,
    /// The client's PAID content frontier: the serve leg stores it after each
    /// voucher batch commits, the pull leg's `WindowPacer` reads it to bound
    /// `pulled − served_paid ≤ window`. An `Arc` so a pull leg on its own runtime
    /// can hold an owned handle.
    served_paid: Arc<AtomicU64>,
    /// Notified after each `served_paid` advance so a parked pull re-decides
    /// exactly when a downstream voucher clears.
    served_paid_advanced: Arc<Notify>,
    /// The chunk ranges THIS fill will produce — exactly the bytes this pull
    /// fetches (its `missing_ranges ∩ R`). [`FillRegistry::fill_plan`] intersects a
    /// serve-miss range against the union of all live sessions' `covered` to decide
    /// `attach` / `remainder`. Set at registration via [`Self::set_covered`];
    /// defaults to [`ChunkRanges::all`] so a session built but not yet range-scoped
    /// coalesces conservatively.
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
}

impl FillSession {
    /// Construct the fill session for a `total_bytes`-byte blob rooted at `root`.
    /// The served frontier starts at 0; the orchestration seeds it to the request's
    /// content start before either leg streams.
    #[must_use]
    pub fn new(root: blake3::Hash, total_bytes: u64) -> Arc<Self> {
        let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
        let size = usize::try_from(tree.outboard_size()).unwrap_or(usize::MAX);
        let nodes = size / HASH_PAIR_BYTES;
        Arc::new(Self {
            tree,
            root,
            state: StdMutex::new(OutboardState {
                bytes: vec![0u8; size],
                captured: vec![false; nodes],
            }),
            advanced: Notify::new(),
            ended: StdMutex::new(None),
            ended_notify: Notify::new(),
            served_paid: Arc::new(AtomicU64::new(0)),
            served_paid_advanced: Arc::new(Notify::new()),
            covered: StdMutex::new(ChunkRanges::all()),
            observers: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
        })
    }

    /// Set the chunk ranges this fill will produce. Called once at registration,
    /// before the pull streams — the registry reads it to plan observer attach.
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
        self.tree.size()
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
    /// not block a fresh pull ([`FillRegistry::fill_plan`] excludes its `covered`
    /// from the union) and must not be an attach target.
    #[must_use]
    fn is_dead(&self) -> bool {
        self.outcome().is_some() || self.cancel.is_cancelled()
    }

    /// Mint a serve-side [`SessionOutboardReader`] over this session's outboard.
    #[must_use]
    pub fn outboard_reader(self: &Arc<Self>) -> SessionOutboardReader {
        SessionOutboardReader {
            session: Arc::clone(self),
        }
    }

    /// The shared PAID content frontier the pull leg's `WindowPacer` bounds against.
    #[must_use]
    pub const fn served_frontier(&self) -> &Arc<AtomicU64> {
        &self.served_paid
    }

    /// Notified after each [`Self::served_frontier`] advance.
    #[must_use]
    pub const fn served_advanced(&self) -> &Arc<Notify> {
        &self.served_paid_advanced
    }

    /// The signal fired when the pull leg terminates ([`Self::mark_ended`]). A
    /// serve-side reader registers a waiter on it BEFORE inspecting [`Self::outcome`]
    /// so a terminal outcome recorded concurrently cannot slip past the check.
    #[must_use]
    pub const fn ended_signal(&self) -> &Notify {
        &self.ended_notify
    }

    /// Capture one internal node's `(left, right)` hash pair. Idempotent: re-saving
    /// a node (a re-admitted range) overwrites with the same bytes and re-notifies,
    /// which is harmless. A `node` with no outboard slot (a leaf) is ignored.
    pub fn capture(&self, node: TreeNode, pair: (blake3::Hash, blake3::Hash)) {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return; // leaf: no hash pair in the outboard
        };
        let idx = usize::try_from(offset).unwrap_or(usize::MAX);
        let byte_off = idx.saturating_mul(HASH_PAIR_BYTES);
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (l, r) = pair;
            if let Some(slot) = state.bytes.get_mut(byte_off..byte_off + HASH_PAIR_BYTES)
                && let Some((left, right)) = slot.split_at_mut_checked(32)
            {
                left.copy_from_slice(l.as_bytes());
                right.copy_from_slice(r.as_bytes());
            }
            if let Some(flag) = state.captured.get_mut(idx) {
                *flag = true;
            }
        }
        self.advanced.notify_waiters();
    }

    /// Record the pull leg's terminal outcome and wake any parked reader. Idempotent
    /// on the wake; the outcome is set once by the pull leg on its single exit.
    pub fn mark_ended(&self, result: Result<(), FillError>) {
        {
            let mut guard = self.ended.lock().unwrap_or_else(PoisonError::into_inner);
            *guard = Some(result);
        }
        self.ended_notify.notify_waiters();
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

/// Serve-side [`Outboard`] over a [`FillSession`]'s captured proof nodes. Its
/// [`Outboard::load`] awaits a not-yet-captured node, racing the pull's terminal
/// outcome for the no-hang guarantee. Minted via [`FillSession::outboard_reader`].
#[derive(Debug)]
pub struct SessionOutboardReader {
    session: Arc<FillSession>,
}

impl Outboard for SessionOutboardReader {
    fn root(&self) -> blake3::Hash {
        self.session.root
    }

    fn tree(&self) -> BaoTree {
        self.session.tree
    }

    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.session.tree.pre_order_offset(node) else {
            return Ok(None); // leaf: bao_tree sources this from the data reader
        };
        let idx = usize::try_from(offset).unwrap_or(usize::MAX);
        loop {
            // Register the wakers BEFORE inspecting shared state, so a capture /
            // pull-end recorded concurrently cannot slip between the check and the
            // await.
            let advanced = self.session.advanced.notified();
            let ended = self.session.ended_signal().notified();
            tokio::pin!(advanced);
            tokio::pin!(ended);
            advanced.as_mut().enable();
            ended.as_mut().enable();

            if let Some(pair) = self.session.try_load(idx) {
                return Ok(Some(pair));
            }

            // Not captured yet. If the pull has ended, decide now: a failure fails the
            // serve; a clean end means every proof node was supplied, so one more check
            // settles it — a still-missing node is a genuine inconsistency, not a wait.
            if let Some(outcome) = self.session.outcome() {
                if let Some(pair) = self.session.try_load(idx) {
                    return Ok(Some(pair));
                }
                return match outcome {
                    Err(msg) => Err(io::Error::other(format!(
                        "upstream pull failed before supplying outboard node {node:?}: {msg}"
                    ))),
                    Ok(()) => Err(io::Error::other(format!(
                        "upstream pull completed but outboard node {node:?} was never captured"
                    ))),
                };
            }

            // Await the next capture or the pull ending, then re-check.
            tokio::select! {
                biased;
                () = ended.as_mut() => {}
                () = advanced.as_mut() => {}
            }
        }
    }
}

/// RAII observer handle on a [`FillSession`]. One is minted for the pull owner
/// ([`FillRegistry::register_fill`]) and one per attached serve leg
/// ([`FillRegistry::fill_plan`]). Dropping it decrements the session's observer
/// count; the last-out drop, if the pull is still running, cancels the pull so a
/// fill no client is waiting on stops ingesting (#1610).
///
/// The decrement + cancel decision runs UNDER the registry map lock — the same
/// lock `fill_plan`/`register_fill` hold when they attach an observer. That mutual
/// exclusion closes the resurrection race: a concurrent `fill_plan` cannot
/// `fetch_add` a session 0→1 in the window between this drop's last-out
/// `fetch_sub` (1→0) and its `cancel()`, so no live observer can end up bound to a
/// cancelled fill. The lease therefore holds a [`Weak`] back to the registry (plus
/// the session's `hash`) to reach that lock at drop. If the upgrade fails the
/// registry is gone, so the fill is moot and the drop just skips.
#[derive(Debug)]
pub struct ObserverLease {
    session: Arc<FillSession>,
    registry: Weak<FillRegistry>,
    hash: Hash,
}

impl ObserverLease {
    /// Explicitly release the lease. Equivalent to `drop(self)`; named so a serve
    /// leg's teardown reads as intent rather than an incidental drop.
    pub fn detach(self) {}
}

impl Drop for ObserverLease {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            // Registry dropped — the fill (and its map entry) are gone; nothing can
            // attach, so the decrement + cancel are moot. Skip.
            return;
        };
        // Take the SAME lock `fill_plan`/`register_fill` hold to attach, so the
        // decrement, the map removal, and the cancel decision cannot interleave with
        // an attach.
        let mut map = registry.map.lock().unwrap_or_else(PoisonError::into_inner);
        // `fetch_sub` returns the PREVIOUS value; `1` means this drop took the
        // count to 0.
        let prev = self.session.observers.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // Last observer left: free the session from the map so dead sessions do
            // not accumulate forever (a slow leak — `fill_plan` skips them via
            // `is_dead` but never frees them). Remove by pointer identity so a sibling
            // session for the same hash survives; drop the `hash` key if its Vec empties.
            if let Some(sessions) = map.get_mut(&self.hash) {
                sessions.retain(|s| !Arc::ptr_eq(s, &self.session));
                if sessions.is_empty() {
                    map.remove(&self.hash);
                }
            }
            // Cancel only if the pull has not already ended — a completed pull needs
            // no cancel, and a failed one already terminated. The session is removed
            // from the map regardless.
            if self.session.outcome().is_none() {
                self.session.cancel.cancel();
            }
        }
    }
}

/// The plan for serving a miss of range `R` against the in-flight fills for a hash.
/// `attach` is `R ∩ covered_union` (bytes an already-running pull will produce, so
/// the serve leg attaches instead of opening a duplicate pull); `remainder` is
/// `R − covered_union` (bytes no live pull covers, which the caller must fetch).
/// `session` is `Some` iff `attach` is non-empty — the live session with the
/// largest overlap, plus a fresh [`ObserverLease`] the caller holds for the life of
/// its serve leg.
#[derive(Debug)]
pub struct FillPlan {
    /// `R ∩ covered_union` — bytes an in-flight pull will produce.
    pub attach: ChunkRanges,
    /// `R − covered_union` — bytes no live pull covers; the caller fetches these.
    pub remainder: ChunkRanges,
    /// The session to attach to (largest-overlap) and the observer lease, when
    /// `attach` is non-empty.
    pub session: Option<(Arc<FillSession>, ObserverLease)>,
}

/// The atomic outcome of [`FillRegistry::claim`]: a serve-miss either coalesces
/// onto a live pull (`Attach`) or becomes the owner of a fresh one (`Owner`). Both
/// carry the target [`FillSession`] and an [`ObserverLease`] the caller holds for
/// the life of its serve leg. Unlike the two-call [`FillRegistry::fill_plan`] +
/// [`FillRegistry::register_fill`] sequence, `claim` decides attach-vs-own AND
/// registers under ONE map-lock acquisition, so two concurrent fresh misses for the
/// same blob cannot both see an empty registry and both open a pull.
#[derive(Debug)]
pub enum FillClaim {
    /// A live pull already covers the request; run a serve leg over `session` and
    /// open NO new pull. `lease` is an observer lease on the existing fill.
    Attach {
        /// The live session to serve from (largest overlap with the request).
        session: Arc<FillSession>,
        /// The observer lease held for the life of the attaching serve leg.
        lease: ObserverLease,
    },
    /// No live pull covers the request; the caller owns the freshly-registered
    /// `session` and must spawn the pull for the whole request range. `lease` is the
    /// owner lease (observer count starts at 1).
    Owner {
        /// The freshly-registered session the caller must drive a pull for.
        session: Arc<FillSession>,
        /// The owner lease (count 1) held for the life of the owning pull.
        lease: ObserverLease,
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

/// Range-aware coalescing registry: the live in-flight fills, keyed by hash. A
/// serve-miss consults [`Self::fill_plan`] to split its range into `attach`
/// (covered by a running pull — attach an observer) and `remainder` (fetch it),
/// and [`Self::register_fill`] to publish a new pull's session. Purely synchronous
/// range math under a std `Mutex`; the lock is never held across an await.
#[derive(Debug, Default)]
pub struct FillRegistry {
    map: StdMutex<HashMap<Hash, Vec<Arc<FillSession>>>>,
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
        }
    }

    /// Publish a new pull's `session` (already scoped via
    /// [`FillSession::set_covered`] to the bytes it will fetch) and return the
    /// owner lease (observer count starts at 1). The entry stays in the map until
    /// the owning pull is joined node-side.
    pub fn register_fill(self: &Arc<Self>, hash: Hash, session: Arc<FillSession>) -> ObserverLease {
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        let lease = self.mint_lease(&session, hash);
        map.entry(hash).or_default().push(session);
        lease
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
            .iter()
            .find(|session| !session.is_dead())
            .map(|session| session.total_bytes())
    }

    /// Plan a serve-miss of `[offset, offset+len)` (`len == 0` = to end) against the
    /// live fills for `hash`. Aligns the request to chunk-group boundaries, then
    /// splits it into `attach = R ∩ covered_union` and `remainder = R − covered_union`.
    /// If `attach` is non-empty, binds to the live session with the largest overlap
    /// and increments its observer count (returned as an [`ObserverLease`]).
    #[must_use]
    pub fn fill_plan(self: &Arc<Self>, hash: Hash, offset: u64, len: u64, total: u64) -> FillPlan {
        let empty = || FillPlan {
            attach: ChunkRanges::empty(),
            remainder: ChunkRanges::empty(),
            session: None,
        };
        let Ok(aligned) = align_range(offset, len, total) else {
            // An out-of-bounds request has no coalescable range; the caller's own
            // path surfaces the error when it tries to fetch.
            return empty();
        };
        let r = aligned.chunk_ranges().clone();

        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        let mut covered_union = ChunkRanges::empty();
        let mut best: Option<(Arc<FillSession>, u64)> = None;
        if let Some(sessions) = map.get(&hash) {
            for session in sessions {
                // A dead session (ended or cancelled) may never deliver its bytes,
                // so its `covered` must neither block a fresh pull nor be an attach
                // target. Removal from the map is node-side on pull-thread join;
                // here we just skip it. Reading `is_dead` under the map lock pins
                // the decision against a concurrent last-out lease drop, which fires
                // `cancel()` under this same lock.
                if session.is_dead() {
                    continue;
                }
                let covered = session.covered_ranges();
                let overlap = &r & &covered;
                if !overlap.is_empty() {
                    let span = chunk_span(&overlap);
                    if best.as_ref().is_none_or(|(_, best_span)| span > *best_span) {
                        best = Some((Arc::clone(session), span));
                    }
                }
                covered_union |= covered;
            }
        }

        let attach = &r & &covered_union;
        let remainder = &r - &covered_union;
        let session = if attach.is_empty() {
            None
        } else {
            best.map(|(session, _)| {
                let lease = self.mint_lease(&session, hash);
                (session, lease)
            })
        };
        FillPlan {
            attach,
            remainder,
            session,
        }
    }

    /// Atomically decide attach-vs-own AND register, all under ONE map-lock
    /// acquisition. This closes the coalescing TOCTOU race that the separate
    /// [`Self::fill_plan`] (read) + [`Self::register_fill`] (write) calls leave open:
    /// between those two lock acquisitions another thread can register, so two
    /// concurrent fresh misses for the same blob both see an empty registry and both
    /// open a pull. `claim` holds the map lock across the whole decision, so exactly
    /// one of two racing identical claims registers an owner and the other attaches.
    ///
    /// `R = align_range(offset, len, total)`. On an align error or empty `R`, the
    /// caller takes the Owner path (its own fetch surfaces any out-of-bounds error).
    /// Otherwise `remainder = R − covered_union` over LIVE sessions:
    /// - `remainder.is_empty()` (a live pull covers all of `R`) → ATTACH to the
    ///   largest-overlap session; `make_session` is NOT called.
    /// - else → OWNER: build the session (only now — `make_session` allocates an
    ///   outboard buffer, so it is never built-and-dropped on the attach branch),
    ///   set its `covered` to the WHOLE `R` (the conservative cut: this owner fetches
    ///   everything it will serve, even on a partial overlap — partial-overlap de-dup
    ///   is a deferred follow-up), register it, and return the owner lease (count 1).
    ///
    /// `make_session` runs under the lock, which is safe: it does no await (a std
    /// lock) and only allocates.
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
            let mut best: Option<(Arc<FillSession>, u64)> = None;
            if let Some(sessions) = map.get(&hash) {
                for session in sessions {
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
                        if best.as_ref().is_none_or(|(_, best_span)| span > *best_span) {
                            best = Some((Arc::clone(session), span));
                        }
                    }
                    covered_union |= covered;
                }
            }

            let remainder = &r - &covered_union;
            if remainder.is_empty() {
                // Fully covered by live pulls → ATTACH. `best` is `Some` whenever a
                // non-empty `R` is covered by the union (some session must overlap);
                // the `if let` is defensive against the unreachable `None`.
                if let Some((session, _)) = best {
                    let lease = self.mint_lease(&session, hash);
                    return FillClaim::Attach { session, lease };
                }
            }
        }

        // OWNER: no live pull covers the request (or `R` is empty/unalignable). Build
        // the session now — never on the attach branch — and scope it to the WHOLE
        // `R` (the conservative cut). Register + mint the owner lease under the SAME
        // lock, so a racing identical claim sees this session and attaches.
        let session = make_session();
        session.set_covered(r);
        let lease = self.mint_lease(&session, hash);
        map.entry(hash).or_default().push(Arc::clone(&session));
        FillClaim::Owner { session, lease }
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

    use bao_tree::{ChunkRanges, blake3};
    use decdn_bao_range::align_range;

    use super::{FillClaim, FillError, FillRegistry, FillSession};
    use crate::{CHUNK_GROUP_BYTES, Hash};

    /// One chunk group of bytes, the alignment granularity `fill_plan` snaps to.
    const G: u64 = CHUNK_GROUP_BYTES;

    fn store_hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    /// Whether the registry map still holds an entry (a non-empty session Vec) for
    /// `hash`. Reaches the private `map` field directly — the discriminating check
    /// for last-observer removal, which `fill_plan` alone cannot distinguish from
    /// the `is_dead` skip.
    fn mapped(reg: &FillRegistry, hash: Hash) -> bool {
        reg.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .is_some_and(|sessions| !sessions.is_empty())
    }

    fn root(byte: u8) -> blake3::Hash {
        blake3::Hash::from([byte; 32])
    }

    /// The chunk ranges covering the byte span `[start, start+len)` of a `total`
    /// blob (`len == 0` = to end), built via the same `align_range` the registry
    /// uses so `covered` and the expected `attach`/`remainder` cannot drift.
    fn ranges(start: u64, len: u64, total: u64) -> ChunkRanges {
        align_range(start, len, total)
            .expect("aligned range")
            .chunk_ranges()
            .clone()
    }

    /// (a) Same range: a second serve-miss for the exact range an in-flight pull
    /// covers attaches wholly, leaves no remainder, and raises the count to 2.
    #[test]
    fn same_range_coalesces_to_one_pull() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xA1);

        let session = FillSession::new(root(0xA1), total);
        session.set_covered(ranges(0, 0, total)); // [0, total)
        let _owner = reg.register_fill(hash, Arc::clone(&session));

        let plan = reg.fill_plan(hash, 0, 0, total);
        assert_eq!(
            plan.attach,
            ranges(0, 0, total),
            "attach is the whole range"
        );
        assert!(plan.remainder.is_empty(), "nothing left to fetch");
        let (attached, _lease) = plan.session.expect("attaches to the in-flight pull");
        assert!(Arc::ptr_eq(&attached, &session), "binds the same session");
        assert_eq!(session.observer_count(), 2, "owner + one attached observer");
    }

    /// (b) Disjoint halves: a serve-miss for the second half of a pull that only
    /// covers the first half attaches nothing, and the full request falls to
    /// `remainder`; the first session's count is untouched.
    #[test]
    fn disjoint_halves_do_not_attach() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xB2);

        let session = FillSession::new(root(0xB2), total);
        session.set_covered(ranges(0, half, total)); // [0, half)
        let _owner = reg.register_fill(hash, Arc::clone(&session));

        let plan = reg.fill_plan(hash, half, total - half, total); // [half, total)
        assert!(plan.attach.is_empty(), "no overlap, nothing to attach");
        assert_eq!(
            plan.remainder,
            ranges(half, total - half, total),
            "the whole request must be fetched"
        );
        assert!(plan.session.is_none(), "no session bound");
        assert_eq!(session.observer_count(), 1, "count untouched");
    }

    /// (c) Partial overlap: a request straddling the covered edge splits into an
    /// `attach` for the covered part and a `remainder` for the rest, and binds an
    /// observer to the overlapping session.
    #[test]
    fn partial_overlap_splits() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xC3);

        let session = FillSession::new(root(0xC3), total);
        session.set_covered(ranges(0, 3 * G, total)); // [0, 3g)
        let _owner = reg.register_fill(hash, Arc::clone(&session));

        let plan = reg.fill_plan(hash, 2 * G, 3 * G, total); // [2g, 5g)
        assert_eq!(plan.attach, ranges(2 * G, G, total), "attach == [2g, 3g)");
        assert_eq!(
            plan.remainder,
            ranges(3 * G, 2 * G, total),
            "remainder == [3g, 5g)"
        );
        assert!(plan.session.is_some(), "binds the overlapping session");
        assert_eq!(session.observer_count(), 2, "owner + one attached observer");
    }

    /// (d) Lease teardown: the last observer leaving before the pull ends cancels
    /// the pull; an earlier leaver does not.
    #[test]
    fn last_observer_leaving_cancels() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xD4);

        let session = FillSession::new(root(0xD4), total);
        session.set_covered(ranges(0, 0, total));
        let owner = reg.register_fill(hash, Arc::clone(&session));
        assert_eq!(session.observer_count(), 1, "owner is one observer");

        let plan = reg.fill_plan(hash, 0, 0, total);
        let (_attached, second) = plan.session.expect("attaches a second observer");
        assert_eq!(session.observer_count(), 2);

        drop(second);
        assert!(
            !session.is_cancelled(),
            "the owner still waits — pull must not cancel"
        );
        assert_eq!(session.observer_count(), 1);

        drop(owner);
        assert!(
            session.is_cancelled(),
            "the last observer left — pull is cancelled"
        );
        assert_eq!(session.observer_count(), 0);
    }

    /// The last observer leaving removes the session from the registry map, so dead
    /// sessions do not accumulate forever. An earlier leaver keeps the session
    /// mapped (a fresh serve-miss still attaches to it). The discriminating check is
    /// the map key itself: `fill_plan` already skips a dead-but-mapped session via
    /// `is_dead`, so only a direct map inspection distinguishes removal from that
    /// pre-existing safety net.
    #[test]
    fn last_observer_leaving_removes_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x28);

        let session = FillSession::new(root(0x28), total);
        session.set_covered(ranges(0, 0, total)); // [0, total)
        let owner = reg.register_fill(hash, Arc::clone(&session));
        assert_eq!(session.observer_count(), 1, "owner is one observer");
        assert!(mapped(&reg, hash), "registered session is in the map");

        let plan = reg.fill_plan(hash, 0, 0, total);
        let (_attached, second) = plan.session.expect("attaches a second observer");
        assert_eq!(session.observer_count(), 2, "owner + attached");

        // Drop the attached lease (count 2 → 1): not last-out, session STILL mapped,
        // so a fresh serve-miss attaches to it.
        drop(second);
        assert_eq!(session.observer_count(), 1);
        assert!(
            mapped(&reg, hash),
            "non-last leaver keeps the session mapped"
        );
        let plan = reg.fill_plan(hash, 0, 0, total);
        assert!(
            plan.session.is_some(),
            "session still present — a fresh miss attaches"
        );
        assert!(!plan.attach.is_empty(), "attach is non-empty");
        // That planning minted a third observer lease; release it so the owner is the
        // sole remaining observer.
        drop(plan.session);
        assert_eq!(session.observer_count(), 1, "back to the owner only");

        // Drop the owner (count 1 → 0): last-out REMOVES the session from the map, so
        // the hash key is gone and a fresh serve-miss opens a new pull.
        drop(owner);
        assert_eq!(session.observer_count(), 0);
        assert!(
            !mapped(&reg, hash),
            "last observer left — session removed from the map"
        );
        let plan = reg.fill_plan(hash, 0, 0, total);
        assert!(
            plan.session.is_none(),
            "session removed — nothing to attach"
        );
        assert!(plan.attach.is_empty(), "removed session covers nothing");
        assert_eq!(
            plan.remainder,
            ranges(0, 0, total),
            "the whole range is a fresh pull"
        );
    }

    /// A sibling session for the same hash survives when one session's last observer
    /// leaves: removal is by pointer identity, and the hash key stays while any
    /// session remains under it.
    #[test]
    fn removing_one_session_keeps_a_sibling_for_the_same_hash() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x29);

        // Two disjoint-range sessions for the same hash (each its own pull).
        let first = FillSession::new(root(0x29), total);
        first.set_covered(ranges(0, half, total)); // [0, half)
        let first_owner = reg.register_fill(hash, Arc::clone(&first));

        let second = FillSession::new(root(0x29), total);
        second.set_covered(ranges(half, total - half, total)); // [half, total)
        let _second_owner = reg.register_fill(hash, Arc::clone(&second));

        // Drop the first session's sole observer: it is removed, but the sibling and
        // the hash key remain, so a serve-miss for the second half still attaches.
        drop(first_owner);
        assert!(mapped(&reg, hash), "sibling keeps the hash key alive");
        let plan = reg.fill_plan(hash, half, total - half, total);
        let (attached, _lease) = plan.session.expect("attaches to the surviving sibling");
        assert!(
            Arc::ptr_eq(&attached, &second),
            "the surviving sibling is bound, not the removed session"
        );
    }

    /// A cancelled session is dead: `fill_plan` must NOT attach to it, and its
    /// `covered` must NOT suppress a fresh pull — the whole range falls to
    /// `remainder`. Guards against attaching a new observer to a fill being torn
    /// down (the #1610 resurrection hazard).
    #[test]
    fn fill_plan_skips_cancelled_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xE5);

        let session = FillSession::new(root(0xE5), total);
        session.set_covered(ranges(0, 0, total)); // [0, total)
        let owner = reg.register_fill(hash, Arc::clone(&session));

        // Drop the sole owner so the last-out lease fires cancel (count 1 → 0).
        drop(owner);
        assert!(session.is_cancelled(), "cancel fired on last-out drop");
        assert_eq!(session.observer_count(), 0);

        let plan = reg.fill_plan(hash, 0, 0, total);
        assert!(
            plan.session.is_none(),
            "must not attach to a cancelled session"
        );
        assert!(plan.attach.is_empty(), "a dead session covers nothing");
        assert_eq!(
            plan.remainder,
            ranges(0, 0, total),
            "the whole range is a fresh pull"
        );
        assert_eq!(
            session.observer_count(),
            0,
            "no observer attached to the dead session"
        );
    }

    /// An ended session (terminal outcome recorded) is likewise dead: `fill_plan`
    /// must not attach and must not let its `covered` block a fresh pull.
    #[test]
    fn fill_plan_skips_ended_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xF6);

        let session = FillSession::new(root(0xF6), total);
        session.set_covered(ranges(0, 0, total)); // [0, total)
        let _owner = reg.register_fill(hash, Arc::clone(&session));

        session.mark_ended(Err(FillError::new("upstream died")));

        let plan = reg.fill_plan(hash, 0, 0, total);
        assert!(
            plan.session.is_none(),
            "must not attach to an ended session"
        );
        assert!(plan.attach.is_empty(), "a dead session covers nothing");
        assert_eq!(
            plan.remainder,
            ranges(0, 0, total),
            "the whole range is a fresh pull"
        );
    }

    /// Structural serialization invariant: register (count 1) + attach (count 2),
    /// then drop both leases. The decrement + cancel decision runs under the map
    /// lock, so the non-last drop must NOT cancel and the last-out drop must cancel
    /// exactly once, ending at count 0. Deterministic — no sleeps, no threads.
    #[test]
    fn teardown_decrement_and_cancel_are_lock_serialized() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x17);

        let session = FillSession::new(root(0x17), total);
        session.set_covered(ranges(0, 0, total));
        let owner = reg.register_fill(hash, Arc::clone(&session));
        let plan = reg.fill_plan(hash, 0, 0, total);
        let (_attached, second) = plan.session.expect("attaches a second observer");
        assert_eq!(session.observer_count(), 2, "owner + attached");

        // Drop the attached lease first: count 2 → 1, not last-out, no cancel.
        drop(second);
        assert_eq!(session.observer_count(), 1);
        assert!(!session.is_cancelled(), "not the last observer — no cancel");

        // Drop the owner: count 1 → 0, last-out, cancels exactly once.
        drop(owner);
        assert_eq!(session.observer_count(), 0);
        assert!(session.is_cancelled(), "last observer left — cancelled");
    }

    // The `claim` tests below assert STRUCTURAL outcomes (which variant, which
    // session, observer counts). Concurrent interleaving is prevented not by these
    // sequential tests but by `claim`'s single map-lock acquisition: it decides
    // attach-vs-own AND registers under one lock, so a true multi-thread race test
    // would be flaky where the lock already guarantees atomicity structurally.

    /// A fresh miss on an empty registry OWNS; a second identical miss ATTACHES to
    /// that same owner session, raising its observer count to 2. This is the exact
    /// race `claim` closes: with `fill_plan` + `register_fill` split, both misses
    /// could see an empty registry and both open a pull.
    #[test]
    fn claim_first_owns_second_attaches_same_range() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x30);

        let first = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x30), total));
        let FillClaim::Owner {
            session: owner,
            lease: _owner_lease,
        } = first
        else {
            panic!("first claim on an empty registry must own");
        };
        assert_eq!(owner.observer_count(), 1, "owner is one observer");

        let second = reg.claim(hash, 0, 0, total, || {
            panic!("second claim must attach, not build a session")
        });
        let FillClaim::Attach {
            session: attached,
            lease: _attach_lease,
        } = second
        else {
            panic!("second identical claim must attach to the live pull");
        };
        assert!(
            Arc::ptr_eq(&attached, &owner),
            "attaches to the same owner session"
        );
        assert_eq!(owner.observer_count(), 2, "owner + one attached observer");
    }

    /// Two disjoint-range claims for the same hash each OWN a distinct session — no
    /// overlap means no coalescing, so two live pulls run.
    #[test]
    fn claim_disjoint_both_own() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x31);

        let first = reg.claim(hash, 0, half, total, || FillSession::new(root(0x31), total));
        let FillClaim::Owner {
            session: lo,
            lease: _lo,
        } = first
        else {
            panic!("first disjoint claim must own");
        };

        let second = reg.claim(hash, half, total - half, total, || {
            FillSession::new(root(0x31), total)
        });
        let FillClaim::Owner {
            session: hi,
            lease: _hi,
        } = second
        else {
            panic!("disjoint second claim must own its own pull");
        };
        assert!(!Arc::ptr_eq(&lo, &hi), "distinct owner sessions");
        assert!(mapped(&reg, hash), "two live sessions under the hash");
    }

    /// A subset claim (its `R` fully inside a live pull's `covered`) ATTACHES: the
    /// remainder is empty, so no new pull opens.
    #[test]
    fn claim_subset_attaches() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x32);

        let owner = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x32), total));
        let FillClaim::Owner {
            session: owner,
            lease: _owner,
        } = owner
        else {
            panic!("first whole-range claim must own");
        };

        let sub = reg.claim(hash, 0, half, total, || {
            panic!("a subset claim must attach, not build a session")
        });
        let FillClaim::Attach {
            session: attached,
            lease: _lease,
        } = sub
        else {
            panic!("a subset of the covered range must attach");
        };
        assert!(Arc::ptr_eq(&attached, &owner), "attaches to the owner");
        assert_eq!(owner.observer_count(), 2, "owner + attached");
    }

    /// A superset claim (its `R` exceeds a live pull's `covered`, so the remainder is
    /// non-empty) OWNS, and — per the conservative cut — its session covers the WHOLE
    /// `R`, not just the remainder. Verified by a third claim over `[0, total)`
    /// attaching to the superset owner (which now covers all of it).
    #[test]
    fn claim_superset_owns_and_covers_whole_r() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x33);

        // First owner covers only [0, half).
        let first = reg.claim(hash, 0, half, total, || FillSession::new(root(0x33), total));
        let FillClaim::Owner {
            session: _lower,
            lease: _lower_lease,
        } = first
        else {
            panic!("first partial claim must own");
        };

        // Superset [0, total): remainder [half, total) is non-empty → OWNER.
        let sup = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x33), total));
        let FillClaim::Owner {
            session: superset,
            lease: _sup_lease,
        } = sup
        else {
            panic!("a superset with a non-empty remainder must own");
        };
        assert_eq!(
            superset.covered_ranges(),
            ranges(0, 0, total),
            "the owner covers the WHOLE R (conservative cut), not just the remainder"
        );

        // A third whole-range claim attaches to the superset owner (largest overlap),
        // proving it covers all of [0, total).
        let third = reg.claim(hash, 0, 0, total, || {
            panic!("the third claim must attach to the whole-range owner")
        });
        let FillClaim::Attach {
            session: attached,
            lease: _third_lease,
        } = third
        else {
            panic!("a whole-range claim must attach to the whole-range owner");
        };
        assert!(
            Arc::ptr_eq(&attached, &superset),
            "attaches to the superset owner that covers the whole range"
        );
    }

    /// `make_session` MUST NOT run on the attach branch — a serve leg that coalesces
    /// allocates no outboard buffer. A closure that panics if invoked proves it.
    #[test]
    fn claim_make_session_not_called_on_attach() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x34);

        let owner = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x34), total));
        assert!(matches!(owner, FillClaim::Owner { .. }), "first claim owns");

        // If `claim` invokes this on the attach path, the test panics and fails.
        let attach = reg.claim(hash, 0, 0, total, || {
            panic!("make_session must not be called on the attach branch")
        });
        assert!(
            matches!(attach, FillClaim::Attach { .. }),
            "second identical claim attaches without building a session"
        );
    }

    /// `FillSession::total_bytes` reports the blob length the session was built with,
    /// so the attach path can sign its `StreamResponse` without a header handshake.
    #[test]
    fn total_bytes_reports_blob_length() {
        let total = 5 * G + 321;
        let session = FillSession::new(root(0x35), total);
        assert_eq!(session.total_bytes(), total);
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
        let owner = reg.register_fill(hash, Arc::clone(&session));
        assert_eq!(
            reg.in_flight_total(hash),
            Some(total),
            "a live fill reports its blob length"
        );

        // Last observer leaves → the session is removed from the map → nothing in
        // flight, so a fresh serve-miss must handshake.
        drop(owner);
        assert_eq!(
            reg.in_flight_total(hash),
            None,
            "a removed fill is not in flight"
        );
    }

    /// An ENDED session is dead: `in_flight_total` skips it even while it is still
    /// mapped, because there is nothing live to coalesce onto — the caller must
    /// handshake.
    #[test]
    fn in_flight_total_skips_ended_session() {
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x41);

        let session = FillSession::new(root(0x41), total);
        session.set_covered(ranges(0, 0, total));
        let _owner = reg.register_fill(hash, Arc::clone(&session));
        assert_eq!(reg.in_flight_total(hash), Some(total));

        session.mark_ended(Err(FillError::new("upstream died")));
        assert_eq!(
            reg.in_flight_total(hash),
            None,
            "an ended session is not attachable in flight"
        );
    }

    /// With a dead session and a LIVE sibling for the same hash, `in_flight_total`
    /// reports the live one's length — the dead session is skipped, not the whole
    /// hash.
    #[test]
    fn in_flight_total_reports_live_sibling_past_a_dead_one() {
        let total = 8 * G;
        let half = 4 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0x42);

        let dead = FillSession::new(root(0x42), total);
        dead.set_covered(ranges(0, half, total));
        let _dead_owner = reg.register_fill(hash, Arc::clone(&dead));
        dead.mark_ended(Err(FillError::new("first pull died")));

        let live = FillSession::new(root(0x42), total);
        live.set_covered(ranges(half, total - half, total));
        let _live_owner = reg.register_fill(hash, Arc::clone(&live));

        assert_eq!(
            reg.in_flight_total(hash),
            Some(total),
            "a live sibling is reported past the dead session"
        );
    }
}
