//! `ProgressiveSource` (#1130 Task 4): the window-paced bao source the retained
//! fused `window_forward_loop` (`serve_via_local_outboard`, #1130) drives — a
//! local stream-while-store pull from an already-held outboard
//! ([`decdn_cache::LocalOutboardPull`], Task 3).
//!
//! The upstream node-to-node variant (`NodeProgressivePull`) was retired here in
//! #1621 B2 part 2: the node→node serve-miss now runs the gap-driven pull leg
//! ([`crate::node_origin::run_pull_leg`]) instead of the fused loop, so
//! only the local twin still drives this enum. [`LocalOutboardPull`] has no upstream
//! provider to score, so its `finish`/`abandon`/`abandon_corrupt` carry no
//! [`TeeVerdict`] / cause — the dispatch below simply drops those arguments.

use bytes::Bytes;
use decdn_cache::LocalOutboardPull;

use crate::node_origin::TeeVerdict;

/// A live window-paced bao source: a local stream-while-store pull. Retained for
/// the [`ClientHandler::serve_via_local_outboard`](super::ClientHandler) twin, whose
/// fused loop drives the same pump/finish/abandon call sites the node→node path used
/// before it moved to the decoupled pull leg (#1621 B2 part 2).
#[derive(Debug)]
pub(crate) enum ProgressiveSource {
    /// A local pull streaming an already-held origin's plaintext through the
    /// bao encoder (#1130 Task 3): no upstream to pay or score.
    LocalOutboard(LocalOutboardPull),
}

impl ProgressiveSource {
    /// The promised **wire** byte count of this pull. See
    /// [`LocalOutboardPull::expected_wire_bytes`].
    #[must_use]
    pub const fn expected_wire_bytes(&self) -> u64 {
        match self {
            Self::LocalOutboard(pull) => pull.expected_wire_bytes(),
        }
    }

    /// Read the next wire chunk, delegated to whichever variant is live.
    ///
    /// # Errors
    ///
    /// Propagates the live variant's `next_chunk` error.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        match self {
            Self::LocalOutboard(pull) => pull.next_chunk().await,
        }
    }

    /// Finalize a cleanly-completed pull. `_tee_verdict` is ignored — a local pull
    /// has no upstream provider to score, and its own encoder is the integrity check
    /// ([`LocalOutboardPull::finish`]).
    ///
    /// # Errors
    ///
    /// Propagates the live variant's `finish` error.
    pub async fn finish(self, _tee_verdict: TeeVerdict) -> anyhow::Result<()> {
        // A local pull has no upstream provider to score; its own encoder is the
        // integrity check ([`LocalOutboardPull::finish`]), so the verdict is dropped.
        match self {
            Self::LocalOutboard(pull) => pull.finish().await,
        }
    }

    /// Abandon the pull (downstream dropped, underpaid, or a `next_chunk`
    /// errored). `_cause` is ignored — the local twin has no upstream provider to
    /// score ([`LocalOutboardPull::abandon`]).
    pub fn abandon(self, _cause: Option<&anyhow::Error>) {
        // No upstream provider to score on the local twin — `cause` is dropped.
        match self {
            Self::LocalOutboard(pull) => pull.abandon(),
        }
    }

    /// Abandon the pull because the teed bao stream failed verification
    /// mid-fill. See [`LocalOutboardPull::abandon_corrupt`].
    pub fn abandon_corrupt(self) {
        match self {
            Self::LocalOutboard(pull) => pull.abandon_corrupt(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_cache::{
        CHUNK_GROUP_BYTES, CacheEngine, Hash, Origin, OriginFetch, OriginKind, OriginPullError,
        OutboardFetch,
    };

    use super::ProgressiveSource;

    /// A minimal origin serving one blob plus its `{H}.obao4` outboard, so a
    /// real [`decdn_cache::LocalOutboardPull`] can be constructed from this
    /// crate's tests via the same public `CacheEngine::open_local_outboard_pull`
    /// path the production wiring will use (mirrors
    /// `decdn-cache`'s own `OutboardStubOrigin` test double).
    #[derive(Debug)]
    struct OutboardStubOrigin {
        data: Bytes,
        hash: Hash,
        outboard: Bytes,
    }

    impl Origin for OutboardStubOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                Ok(OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>>
        {
            let matches = hash == self.hash;
            let len = u64::try_from(self.data.len()).unwrap_or(u64::MAX);
            Box::pin(async move { Ok(matches.then_some(len)) })
        }

        fn fetch_outboard(
            &self,
            hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                OutboardFetch::Found(self.outboard.clone())
            } else {
                OutboardFetch::NotFound
            };
            Box::pin(async move { Ok(result) })
        }
    }

    /// A blob spanning several chunk groups plus a partial final group, so
    /// the bao tree has real interior nodes.
    fn test_blob() -> Vec<u8> {
        let size = 3 * CHUNK_GROUP_BYTES + 77;
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    /// Constructs a real [`decdn_cache::LocalOutboardPull`] (via a real
    /// [`CacheEngine`] over a stub origin, the only way to obtain one — it
    /// has no public constructor of its own), wraps it in
    /// `ProgressiveSource::LocalOutboard`, and proves the enum dispatches
    /// `expected_wire_bytes` and a drained `next_chunk` to that arm rather
    /// than to a hand-rolled compile-only check.
    #[tokio::test]
    async fn progressive_source_dispatches_local() -> anyhow::Result<()> {
        let data = test_blob();
        // Matches `decdn_bao_range::IROH_BLOCK_SIZE` (ADR 038: chunk-group log 4), which this
        // crate does not depend on directly — see `crates/bao-range/src/lib.rs`.
        let block_size = bao_tree::BlockSize::from_chunk_log(4);
        let outboard = PreOrderMemOutboard::create(&data, block_size);
        let hash = Hash::new(&data);
        let origin = OutboardStubOrigin {
            data: Bytes::from(data.clone()),
            hash,
            outboard: Bytes::from(outboard.data.clone()),
        };

        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let Some((header, pull)) = engine.open_local_outboard_pull(hash).await? else {
            anyhow::bail!("expected Some((header, pull)) — outboard + data are both served");
        };
        anyhow::ensure!(
            header.total_bytes == data.len() as u64,
            "header.total_bytes should be the plaintext length"
        );

        let mut source = ProgressiveSource::LocalOutboard(pull);
        let expected_wire_bytes = source.expected_wire_bytes();
        anyhow::ensure!(
            expected_wire_bytes > 0,
            "expected_wire_bytes should be nonzero"
        );

        let mut wire_len: u64 = 0;
        while let Some(chunk) = source.next_chunk().await? {
            wire_len += chunk.len() as u64;
        }
        anyhow::ensure!(
            wire_len == expected_wire_bytes,
            "drained next_chunk total ({wire_len}) should equal expected_wire_bytes() ({expected_wire_bytes})",
        );

        Ok(())
    }
}
