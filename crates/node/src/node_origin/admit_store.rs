//! `NodeAdmitStore` — the node's pull-leg [`decdn_client_pull::IngestStore`]
//! over [`decdn_cache::CacheEngine::admit_bao_stream`] (#1621).
//!
//! `drive()`'s gap-driven pull loop needs a store that both answers
//! [`decdn_bao_range::RangedStore`] queries (present/missing ranges, read,
//! finalize) and can ingest a gap's raw bao wire. [`NodeRangedStore`]
//! already answers the queries over the cache; this type adds the ingest half
//! by wrapping a `NodeRangedStore` and streaming each gap straight into
//! [`decdn_cache::CacheEngine::admit_bao_stream`], which admits the
//! bytes as a B0-tagged partial without buffering the whole gap in memory.
//!
//! This has to live in `node`, not `cache` or `client-pull`: it names both
//! `decdn_cache`'s store and `decdn_client_pull`'s [`IngestStore`] trait, and
//! #578 forbids either of those crates depending on the other. `node` is the
//! one crate that already depends on both.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use decdn_bao_range::{AlignedRange, RangedFuture, RangedStore};
use decdn_cache::{CacheEngine, FillSession, Hash, NodeRangedStore};
use decdn_client_pull::{BaoRangeReader, IngestStore};

/// The node's pull-leg store: [`RangedStore`] queries delegate to a
/// [`NodeRangedStore`], and [`IngestStore::ingest_stream`] admits each gap via
/// [`CacheEngine::admit_bao_stream`]. When a [`FillSession`] is present, the cache's
/// admit path captures each admitted range's proof nodes into the serve leg's shared
/// outboard (#1621 B3, ADR 038) so the serve leg can drive a coherent whole-range
/// encode while the pull fills incrementally — the node just threads the session in.
#[allow(dead_code, reason = "wired by Task 11's driver construction")]
pub(crate) struct NodeAdmitStore {
    inner: NodeRangedStore,
    /// Serve-leg fill session. `None` when no serve leg reads beside this pull (e.g.
    /// the admit-only unit tests). Passed to [`CacheEngine::admit_bao_stream`], which
    /// captures the admitted range's outboard proof nodes into it cache-side.
    session: Option<Arc<FillSession>>,
}

#[allow(dead_code, reason = "wired by Task 11's driver construction")]
impl NodeAdmitStore {
    /// Wrap `engine`'s view of `hash` (a `total_bytes`-byte blob) as the node's
    /// pull-leg store. When `session` is wired, the cache's admit path captures each
    /// admitted range's outboard proof nodes into it (the serve leg's shared outboard).
    pub(crate) const fn new(
        engine: CacheEngine,
        hash: Hash,
        total_bytes: u64,
        session: Option<Arc<FillSession>>,
    ) -> Self {
        Self {
            inner: NodeRangedStore::new(engine, hash, total_bytes),
            session,
        }
    }
}

impl RangedStore for NodeAdmitStore {
    fn total_bytes(&self) -> u64 {
        self.inner.total_bytes()
    }

    fn present_ranges(&self) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        self.inner.present_ranges()
    }

    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        self.inner.missing_ranges(byte_offset, byte_len)
    }

    fn admit(&self, range: AlignedRange, bao_bytes: bytes::Bytes) -> RangedFuture<'_, ()> {
        self.inner.admit(range, bao_bytes)
    }

    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, bytes::Bytes> {
        self.inner.read(byte_offset, byte_len)
    }

    fn is_complete(&self) -> RangedFuture<'_, bool> {
        self.inner.is_complete()
    }

    fn finalize(&self) -> RangedFuture<'_, ()> {
        self.inner.finalize()
    }
}

impl IngestStore for NodeAdmitStore {
    /// Streams `reader`'s raw bao wire for `range` straight into the cache via
    /// [`CacheEngine::admit_bao_stream`], which verifies each chunk group
    /// against the store's rooted hash as it lands and admits the range as a
    /// B0-tagged partial.
    ///
    /// `on_progress` is accepted for the trait but unused on this path: the
    /// node's progress metering happens at the SERVE leg, which
    /// meters what it forwards to the downstream client — not at this shared
    /// upstream ingest. If `admit_bao_stream` grows a progress hook later, it
    /// plugs in here.
    ///
    /// The `R: BaoRangeReader` bound (`AsyncStreamReader + StashedFault +
    /// Send`) satisfies `admit_bao_stream`'s `R: AsyncStreamReader + Send`, so
    /// `reader` passes straight through — the drained reader `admit_bao_stream`
    /// returns is handed back as-is, preserving its `StashedFault` for the
    /// caller's [`decdn_client_pull::BlobSource::finish`].
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        _on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<R>> + 'a>>
    where
        R: BaoRangeReader + 'a,
    {
        Box::pin(async move {
            // `admit_bao_stream` verifies + admits the range and, when a
            // [`FillSession`] is wired, captures its outboard proof nodes into it
            // cache-side (no-op when no serve leg reads beside this pull).
            // Front-to-back admits union to the whole tree.
            let drained = self
                .inner
                .engine()
                .admit_bao_stream(
                    self.inner.hash(),
                    range.chunk_ranges().clone(),
                    self.inner.total_bytes(),
                    reader,
                    self.session.as_ref(),
                )
                .await
                .map_err(anyhow::Error::from)?;
            Ok(drained)
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)] // tests
mod tests {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{IROH_BLOCK_SIZE, RangedStore, align_range, encode_verified_range};
    use decdn_cache::CacheEngine;
    use decdn_client_pull::IngestStore;
    use decdn_client_pull::sink::StashedFault;
    use iroh_io::AsyncStreamReader;

    use super::NodeAdmitStore;

    /// Deterministic pseudo-random blob, matching the generator the cache
    /// crate's own `admit_bao_stream` tests use (`crates/cache/src/engine.rs`).
    fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
        let mut plaintext = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut plaintext {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
        (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
    }

    /// A minimal in-memory [`decdn_client_pull::BaoRangeReader`]: an
    /// [`AsyncStreamReader`] over a `Bytes` cursor plus a trivial
    /// [`StashedFault`] that never parks anything (mirrors the shape of
    /// `crates/client-pull/src/source.rs`'s `ScriptedReader` test double).
    struct MemReader {
        wire: Bytes,
    }

    impl AsyncStreamReader for MemReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
            let take = self.wire.len().min(len);
            Ok(self.wire.split_to(take))
        }

        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            if self.wire.len() < L {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "MemReader exhausted before a fixed-size bao read",
                ));
            }
            let got = self.wire.split_to(L);
            let mut out = [0u8; L];
            out.copy_from_slice(&got);
            Ok(out)
        }
    }

    impl StashedFault for MemReader {
        fn take_fault(&mut self) -> Option<anyhow::Error> {
            None
        }
    }

    /// Build the header-less bao wire for an interior aligned range of a
    /// synthetic 4-group blob, exactly as `crates/cache/src/engine.rs`'s
    /// `admit_bao_stream` tests do: `encode_verified_range` produces the
    /// 8-byte-size-header-prefixed combined encoding, and the wire
    /// `admit_bao_stream` consumes is header-less (ADR 038; the size comes
    /// from the caller's `total_bytes` instead).
    fn interior_range_wire() -> (
        [u8; 32],
        u64,
        decdn_bao_range::AlignedRange,
        bao_tree::ChunkRanges,
        Bytes,
    ) {
        let group = decdn_cache::CHUNK_GROUP_BYTES;
        let total = 4 * group;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let aligned = align_range(group, group, total).expect("align interior group");
        let s = aligned.fetch_start() as usize;
        let e = aligned.fetch_end() as usize;
        let combined = encode_verified_range(root, &aligned, &plaintext[s..e], outboard)
            .expect("encode verified range");
        assert!(combined.len() > 8, "combined wire must carry the header");
        let header_less = combined.slice(8..);
        let ranges = aligned.chunk_ranges().clone();
        (root, total, aligned, ranges, header_less)
    }

    /// `ingest_stream` streams a gap's header-less bao wire into the cache and
    /// the store reports the range present but the blob still partial.
    #[tokio::test]
    async fn node_admit_store_ingests_a_partial_range() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let (root, total, aligned, _ranges, wire) = interior_range_wire();
        let hash = decdn_cache::Hash::from(root);
        let store = NodeAdmitStore::new(engine, hash, total, None);

        let reader = MemReader { wire };
        let mut drained = IngestStore::ingest_stream(&store, &aligned, reader, None)
            .await
            .unwrap();
        assert_eq!(
            drained.read_bytes(1).await.unwrap().len(),
            0,
            "the reader is fully drained by admit_bao_stream"
        );

        let present = RangedStore::present_ranges(&store).await.unwrap();
        assert!(!present.is_empty(), "the ingested range must be present");
        assert!(
            !RangedStore::is_complete(&store).await.unwrap(),
            "a single interior group of a 4-group blob must not be complete"
        );
    }

    /// `RangedStore` queries delegate to the inner `NodeRangedStore`: the
    /// full aligned range is missing before ingest, and the gap shrinks after.
    #[tokio::test]
    async fn node_admit_store_delegates_ranged_store_queries() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let (root, total, aligned, ranges, wire) = interior_range_wire();
        let hash = decdn_cache::Hash::from(root);
        let store = NodeAdmitStore::new(engine, hash, total, None);

        let before =
            RangedStore::missing_ranges(&store, aligned.fetch_start(), aligned.fetch_len())
                .await
                .unwrap();
        assert_eq!(
            &before, &ranges,
            "before ingest, the whole aligned range must be missing"
        );

        let reader = MemReader { wire };
        let _drained = IngestStore::ingest_stream(&store, &aligned, reader, None)
            .await
            .unwrap();

        let after = RangedStore::missing_ranges(&store, aligned.fetch_start(), aligned.fetch_len())
            .await
            .unwrap();
        assert!(
            after.is_empty(),
            "after ingest, the just-admitted range must no longer be missing"
        );
    }

    /// The capture invariant the coherent serve encoder relies on (#1621 B2 part 2,
    /// ADR 038): admitting a blob feeds the shared outboard exactly the blob's true
    /// pre-order outboard. Every interior node the encoder will `load` must match
    /// `bao_tree`'s own outboard for the same content, so the re-encoded downstream
    /// wire is byte-identical to a single whole-blob encode.
    #[tokio::test]
    async fn capture_reconstructs_the_true_outboard() {
        use bao_tree::BaoTree;
        use bao_tree::io::fsm::Outboard as FsmOutboard;
        use bao_tree::io::sync::Outboard as SyncOutboard;

        let group = decdn_cache::CHUNK_GROUP_BYTES;
        let total = 5 * group + 321; // several groups plus a ragged tail
        let (root, plaintext, outboard) = synth_blob(total as usize);

        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let hash = decdn_cache::Hash::from(root);

        let session = decdn_cache::FillSession::new(bao_tree::blake3::Hash::from(root), total);
        let store = NodeAdmitStore::new(engine, hash, total, Some(std::sync::Arc::clone(&session)));

        // Admit the whole blob in one range → the capture unions to the whole tree.
        let aligned = align_range(0, total, total).expect("align whole blob");
        let combined =
            encode_verified_range(root, &aligned, &plaintext, outboard).expect("encode whole blob");
        let wire = combined.slice(8..);
        IngestStore::ingest_stream(&store, &aligned, MemReader { wire }, None)
            .await
            .unwrap();

        // Compare the captured outboard against the blob's true outboard, node by node.
        let mut reader = session.outboard_reader();
        let truth = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
        let tree = BaoTree::new(total, IROH_BLOCK_SIZE);
        let mut internal = 0u64;
        for node in tree.pre_order_nodes_iter() {
            if tree.pre_order_offset(node).is_some() {
                internal += 1;
                let got = FsmOutboard::load(&mut reader, node).await.unwrap();
                let want = SyncOutboard::load(&truth, node).unwrap();
                assert_eq!(
                    got, want,
                    "captured node {node:?} must match the true outboard"
                );
            }
        }
        assert!(
            internal > 0,
            "a multi-group blob has interior nodes to capture"
        );
    }
}
