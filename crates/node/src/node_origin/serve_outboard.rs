//! `SharedOutboard` — the whole-tree bao outboard the decoupled serve-miss legs
//! share (#1621 B2 part 2, ADR 038, Path A).
//!
//! # Why this exists
//!
//! The decoupled serve leg must emit ONE coherent whole-range bao verified stream
//! while the pull leg fills the cache incrementally beside it. `bao_tree`'s async
//! [`encode_ranges_validated`](bao_tree::io::fsm::encode_ranges_validated) is the
//! right engine — it walks the range pre-order, reading leaf DATA and, for every
//! internal node, the `(left, right)` hash pair from an
//! [`Outboard`]. The data the serve reads from the
//! cache (via ranged export, gated on the present-range watch). But the OUTBOARD
//! it cannot: iroh-blobs keeps a partial blob's outboard in actor memory and
//! exposes no reader, and a pre-order walk needs proof hashes for
//! not-yet-filled right-spine subtrees (the root's `right_hash` is emitted before
//! any right-half data lands) that are NOT derivable from present data.
//!
//! So the node keeps its own copy. The pull leg already receives those exact proof
//! nodes in the verified upstream bao it ingests; [`OutboardWriter::save`] captures
//! them into this whole-tree buffer as each gap admits. The serve leg reads them
//! through [`OutboardReader`], whose [`Outboard::load`] AWAITS a node that has not
//! been captured yet — racing the pull's terminal signal so a pull that fails (or a
//! genuinely missing node after a clean pull) fails the serve rather than hanging.
//!
//! Because the pull admits front-to-back and the first admit carries the whole
//! right-spine down to the first gap, every node the encoder needs is captured by
//! an admit that has already happened by the time the encoder reaches it.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};

use bao_tree::io::fsm::Outboard;
use bao_tree::{BaoTree, BlockSize, TreeNode, blake3};
use tokio::sync::Notify;

/// iroh-blobs' block size — 16 KiB chunk groups (`2^4` 1 KiB chunks). Identical by
/// construction to `decdn_bao_range::IROH_BLOCK_SIZE` and
/// `iroh_blobs::store::IROH_BLOCK_SIZE`; the tree geometry (and thus every node
/// offset) must match the store's, since the captured hashes are the store's.
const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

/// Bytes per outboard node (a `(left, right)` blake3 hash pair).
const HASH_PAIR_BYTES: usize = 64;

/// The whole-tree outboard shared by the two serve-miss legs: [`OutboardWriter`]
/// (pull side, capture) and [`OutboardReader`] (serve side, awaiting read) both
/// hold an `Arc` to one [`Inner`].
struct Inner {
    tree: BaoTree,
    root: blake3::Hash,
    /// Pre-order hash-pair bytes + a per-node "captured" flag. Guarded together;
    /// the lock is never held across an await.
    state: StdMutex<State>,
    /// Notified after every [`OutboardWriter::save`] so a parked [`Outboard::load`]
    /// re-checks. Runtime-agnostic, so it crosses the pull/serve runtimes safely.
    advanced: Notify,
}

struct State {
    /// `tree.outboard_size()` bytes: node `n`'s pair lives at
    /// `tree.pre_order_offset(n) * 64`.
    bytes: Vec<u8>,
    /// `captured[i]` is set once node index `i` (its `pre_order_offset`) is saved.
    captured: Vec<bool>,
}

/// Handle to a shared outboard used by the PULL leg to capture proof nodes from
/// the verified upstream bao. Cheap to clone (`Arc`); `save` is interior-mutable.
#[derive(Clone)]
#[allow(dead_code, reason = "capture wiring lands with the serve encoder")]
pub(crate) struct OutboardWriter {
    inner: Arc<Inner>,
}

/// Handle to a shared outboard used by the SERVE leg as the
/// [`Outboard`] for the async encoder. Its
/// [`Outboard::load`] awaits a not-yet-captured node, racing the pull's terminal
/// outcome for the no-hang guarantee.
#[allow(dead_code, reason = "read wiring lands with the serve encoder")]
pub(crate) struct OutboardReader {
    inner: Arc<Inner>,
    /// The pull leg fires this after recording `pull_result` (same Arcs the serve
    /// leg already coordinates on).
    pull_ended: Arc<Notify>,
    /// The pull leg's terminal outcome — `None` while running.
    pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
}

/// Construct the shared outboard for a `total_bytes`-byte blob rooted at `root`,
/// returning the pull-side writer and a factory for serve-side readers.
#[allow(
    dead_code,
    reason = "constructed by the orchestration with the serve encoder"
)]
pub(crate) fn shared_outboard(
    root: blake3::Hash,
    total_bytes: u64,
) -> (OutboardWriter, ReaderFactory) {
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    let size = usize::try_from(tree.outboard_size()).unwrap_or(usize::MAX);
    let nodes = size / HASH_PAIR_BYTES;
    let inner = Arc::new(Inner {
        tree,
        root,
        state: StdMutex::new(State {
            bytes: vec![0u8; size],
            captured: vec![false; nodes],
        }),
        advanced: Notify::new(),
    });
    (
        OutboardWriter {
            inner: Arc::clone(&inner),
        },
        ReaderFactory { inner },
    )
}

/// Mints [`OutboardReader`]s bound to the pull leg's terminal signals. Separate
/// from construction so the writer can be built (and capture can start) before the
/// serve leg wires its `pull_ended` / `pull_result` Arcs.
#[allow(dead_code, reason = "used by the serve encoder wiring")]
pub(crate) struct ReaderFactory {
    inner: Arc<Inner>,
}

impl ReaderFactory {
    #[allow(dead_code, reason = "used by the serve encoder wiring")]
    pub(crate) fn reader(
        &self,
        pull_ended: Arc<Notify>,
        pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
    ) -> OutboardReader {
        OutboardReader {
            inner: Arc::clone(&self.inner),
            pull_ended,
            pull_result,
        }
    }
}

impl OutboardWriter {
    /// Capture one internal node's `(left, right)` hash pair. Idempotent: re-saving
    /// a node (a re-admitted range) overwrites with the same bytes and re-notifies,
    /// which is harmless. A `node` with no outboard slot (a leaf) is ignored.
    #[allow(dead_code, reason = "called by the capture hook")]
    pub(crate) fn save(&self, node: TreeNode, pair: (blake3::Hash, blake3::Hash)) {
        let Some(offset) = self.inner.tree.pre_order_offset(node) else {
            return; // leaf: no hash pair in the outboard
        };
        let idx = usize::try_from(offset).unwrap_or(usize::MAX);
        let byte_off = idx.saturating_mul(HASH_PAIR_BYTES);
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        self.inner.advanced.notify_waiters();
    }
}

impl OutboardReader {
    /// Read node `idx`'s captured pair, or `None` if not captured yet. Never holds
    /// the lock across an await.
    fn try_load(&self, idx: usize) -> Option<(blake3::Hash, blake3::Hash)> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    /// The pull leg's terminal outcome, if it has ended: `Some(Ok)` clean,
    /// `Some(Err(msg))` failed (flattened — `anyhow::Error` is not `Clone`).
    fn pull_outcome(&self) -> Option<Result<(), String>> {
        match self.pull_result.lock() {
            Ok(guard) => match &*guard {
                None => None,
                Some(Ok(())) => Some(Ok(())),
                Some(Err(e)) => Some(Err(format!("{e:#}"))),
            },
            Err(_poisoned) => Some(Err("upstream pull result lock poisoned".to_string())),
        }
    }
}

impl Outboard for OutboardReader {
    fn root(&self) -> blake3::Hash {
        self.inner.root
    }

    fn tree(&self) -> BaoTree {
        self.inner.tree
    }

    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.inner.tree.pre_order_offset(node) else {
            return Ok(None); // leaf: bao_tree sources this from the data reader
        };
        let idx = usize::try_from(offset).unwrap_or(usize::MAX);
        loop {
            // Register the wakers BEFORE inspecting shared state, so a save / pull-end
            // recorded concurrently cannot slip between the check and the await.
            let advanced = self.inner.advanced.notified();
            let ended = self.pull_ended.notified();
            tokio::pin!(advanced);
            tokio::pin!(ended);
            advanced.as_mut().enable();
            ended.as_mut().enable();

            if let Some(pair) = self.try_load(idx) {
                return Ok(Some(pair));
            }

            // Not captured yet. If the pull has ended, decide now: a failure fails the
            // serve; a clean end means every proof node was supplied, so one more check
            // settles it — a still-missing node is a genuine inconsistency, not a wait.
            if let Some(outcome) = self.pull_outcome() {
                if let Some(pair) = self.try_load(idx) {
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
    use super::{IROH_BLOCK_SIZE, shared_outboard};
    use bao_tree::io::fsm::Outboard;
    use bao_tree::{BaoTree, blake3};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::Notify;

    fn h(byte: u8) -> blake3::Hash {
        blake3::Hash::from([byte; 32])
    }

    /// A blob spanning several chunk groups, so the tree has interior nodes with
    /// real pre-order offsets to save and read back.
    const TOTAL: u64 = 5 * 16 * 1024 + 321;

    #[tokio::test]
    async fn save_then_load_round_trips_each_internal_node() {
        let (writer, factory) = shared_outboard(h(0xAA), TOTAL);
        let reader = factory.reader(Arc::new(Notify::new()), Arc::new(StdMutex::new(None)));
        let mut reader = reader;
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);

        // Save a distinct pair into every internal node, then read them all back.
        let mut saved = Vec::new();
        for (i, node) in tree.pre_order_nodes_iter().enumerate() {
            if tree.pre_order_offset(node).is_some() {
                let tag = u8::try_from(i % 251).unwrap();
                let pair = (h(tag), h(tag.wrapping_add(101)));
                writer.save(node, pair);
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
        // the data reader) and never park — even though nothing was ever saved.
        let small = 4 * 1024;
        let (_writer, factory) = shared_outboard(h(1), small);
        let mut reader = factory.reader(Arc::new(Notify::new()), Arc::new(StdMutex::new(None)));
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
        let (writer, factory) = shared_outboard(h(2), TOTAL);
        let pull_ended = Arc::new(Notify::new());
        let mut reader = factory.reader(Arc::clone(&pull_ended), Arc::new(StdMutex::new(None)));
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");
        let pair = (h(7), h(9));

        // Load races ahead of capture: it must block, then wake on save.
        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        assert!(
            !load.is_finished(),
            "load must park until the node is captured"
        );
        writer.save(node, pair);
        assert_eq!(load.await.unwrap().unwrap(), Some(pair));
    }

    #[tokio::test]
    async fn load_fails_when_pull_ends_err_before_capture() {
        let (_writer, factory) = shared_outboard(h(3), TOTAL);
        let pull_ended = Arc::new(Notify::new());
        let pull_result = Arc::new(StdMutex::new(None));
        let mut reader = factory.reader(Arc::clone(&pull_ended), Arc::clone(&pull_result));
        let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
        let node = tree
            .pre_order_nodes_iter()
            .find(|n| tree.pre_order_offset(*n).is_some())
            .expect("an interior node exists");

        let load = tokio::spawn(async move { reader.load(node).await });
        tokio::task::yield_now().await;
        *pull_result.lock().unwrap() = Some(Err(anyhow::anyhow!("upstream died")));
        pull_ended.notify_waiters();
        let err = load
            .await
            .unwrap()
            .expect_err("a failed pull must fail the load");
        assert!(err.to_string().contains("upstream pull failed"));
    }
}
