//! `FillSession` — the cache-resident fill-coordination primitive the decoupled
//! serve-miss legs share (#1621 B3, ADR 038, Path A).
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

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use bao_tree::io::fsm::Outboard;
use bao_tree::{BaoTree, BlockSize, ChunkRanges, TreeNode, blake3};
use tokio::sync::Notify;

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
    /// The ranges THIS fill will produce. Unused in B3.1 (single observer); the
    /// range-aware coalescing registry (B3.2) reads it to decide observer attach.
    #[allow(dead_code, reason = "range-aware coalescing lands in B3.2")]
    covered: ChunkRanges,
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
            covered: ChunkRanges::all(),
        })
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
