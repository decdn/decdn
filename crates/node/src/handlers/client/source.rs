//! `ProgressiveSource` (#1130 Task 4): an enum wrapper over the two kinds of
//! window-paced bao source the `cdn/client/v1` serve loop (#856) can drive —
//! an upstream node-to-node pull ([`NodeProgressivePull`]) or a local
//! stream-while-store pull from an already-held outboard
//! ([`decdn_cache::LocalOutboardPull`], Task 3). Task 5 retypes the window
//! serve loop to this enum so the same pump/finish/abandon call sites work
//! for both.
//!
//! [`LocalOutboardPull`] has no upstream provider to score, so its
//! `finish`/`abandon`/`abandon_corrupt` carry no [`TeeVerdict`] / cause — the
//! dispatch below simply drops those arguments on that arm.

// Nothing in `decdn-node` constructs a `ProgressiveSource` yet — only this module's
// own tests do. Task 5 (#1130) retypes the window serve loop to this enum, at which
// point every variant/method below is live. Blanket-allowed here rather than
// per-item so the wiring commit's diff is just deleting this line.
#![allow(dead_code)]

use bytes::Bytes;
use decdn_cache::LocalOutboardPull;

use crate::node_origin::{NodeProgressivePull, TeeVerdict};

/// A live window-paced bao source: either an upstream node-to-node pull or a
/// local stream-while-store pull. See the module docs for the dispatch
/// rationale.
///
/// `NodeProgressivePull` is the larger variant (it carries the settlement
/// guard, watermark bookkeeping, and channel context a paid upstream pull
/// needs); `LocalOutboardPull` is a thin bridge over two `mpsc` channels.
/// Not boxed: this is the common, per-request path (every window pull-through
/// fill constructs one), and boxing would still allocate on that path — it
/// would just move the cost from "larger enum" to "one more heap alloc" for a
/// same-sized value.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum ProgressiveSource {
    /// An upstream node's window-paced pull (#856): paid, scored, and
    /// watermarked.
    Node(NodeProgressivePull),
    /// A local pull streaming an already-held origin's plaintext through the
    /// bao encoder (#1130 Task 3): no upstream to pay or score.
    LocalOutboard(LocalOutboardPull),
}

impl ProgressiveSource {
    /// The promised **wire** byte count of this pull, delegated to whichever
    /// variant is live. See [`NodeProgressivePull::expected_wire_bytes`] /
    /// [`LocalOutboardPull::expected_wire_bytes`].
    #[must_use]
    pub const fn expected_wire_bytes(&self) -> u64 {
        match self {
            Self::Node(pull) => pull.expected_wire_bytes(),
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
            Self::Node(pull) => pull.next_chunk().await,
            Self::LocalOutboard(pull) => pull.next_chunk().await,
        }
    }

    /// Finalize a cleanly-completed pull. The `Node` arm passes `tee_verdict`
    /// through for reputation scoring ([`NodeProgressivePull::finish`]); the
    /// `LocalOutboard` arm ignores it — a local pull has no upstream
    /// provider to score, and its own encoder is the integrity check
    /// ([`LocalOutboardPull::finish`]).
    ///
    /// # Errors
    ///
    /// Propagates the live variant's `finish` error.
    pub async fn finish(self, tee_verdict: TeeVerdict) -> anyhow::Result<()> {
        match self {
            Self::Node(pull) => pull.finish(tee_verdict).await,
            Self::LocalOutboard(pull) => pull.finish().await,
        }
    }

    /// Abandon the pull (downstream dropped, underpaid, or a `next_chunk`
    /// errored). The `Node` arm scores the provider when `cause` is
    /// supplied ([`NodeProgressivePull::abandon`]); the `LocalOutboard` arm
    /// ignores `cause` — there is no provider to score
    /// ([`LocalOutboardPull::abandon`]).
    pub fn abandon(self, cause: Option<&anyhow::Error>) {
        match self {
            Self::Node(pull) => pull.abandon(cause),
            Self::LocalOutboard(pull) => pull.abandon(),
        }
    }

    /// Abandon the pull because the teed bao stream failed verification
    /// mid-fill, delegated to whichever variant is live. See
    /// [`NodeProgressivePull::abandon_corrupt`] /
    /// [`LocalOutboardPull::abandon_corrupt`].
    pub fn abandon_corrupt(self) {
        match self {
            Self::Node(pull) => pull.abandon_corrupt(),
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
