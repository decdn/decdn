//! `LocalOutboardPull` (#1130 stream-while-store): once a node holds an
//! origin's published `{H}.obao4` outboard, it can stream the origin's
//! PLAINTEXT bytes through
//! [`decdn_bao_range::streaming::encode_whole_blob_headerless`] on the fly,
//! producing the same header-less bao wire shape a node-to-node upstream
//! pull ([`crate::CacheEngine::open_tee_sink`]'s counterpart on the *serve*
//! side) sends — so a paying client's cache-miss serve can start before the
//! whole blob has landed locally. Obtain via
//! [`crate::CacheEngine::open_local_outboard_pull`].
//!
//! Simpler than a node-to-node pull ([`crate` sibling upstream pulls in
//! `decdn-node`): there is no upstream to pay and no provider to score, so
//! this carries no vouchers, no reputation, and no `TeeVerdict` — the
//! encoder's own bao verification against the content root is the only
//! integrity check.
//!
//! ## The bridge
//!
//! Two bounded [`tokio::sync::mpsc`] channels connect the async world (the
//! origin's [`crate::origin::OriginByteStream`]) to a `spawn_blocking`
//! encoder thread:
//!
//! - **plaintext channel** (async producer → blocking consumer): a
//!   [`tokio::spawn`] task drains the origin stream into a bounded channel of
//!   `io::Result<Bytes>`; a private `PlaintextReader` adapts the receiving
//!   half into [`std::io::Read`] via `blocking_recv`.
//! - **wire channel** (blocking producer → async consumer): a private
//!   `WireWriter` adapts the sending half into [`std::io::Write`] via
//!   `blocking_send`; the encoder writes header-less bao wire bytes into it
//!   as it verifies, and [`LocalOutboardPull::next_chunk`] drains the
//!   receiving half.
//!
//! Both channels are small and bounded so memory stays flat regardless of
//! blob size — the origin fetch, the encode, and the downstream serve are
//! paced against each other by channel backpressure rather than buffering
//! the whole blob at any stage.

use std::io::{self, Read, Write};

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use decdn_bao_range::RangeVerifyError;
use decdn_bao_range::streaming::encode_whole_blob_headerless;

use crate::Hash;
use crate::origin::OriginByteStream;
use crate::range_pull::bao_encoded_size;

/// The origin-advertised total plaintext byte size backing a
/// [`LocalOutboardPull`], returned alongside it by
/// [`crate::CacheEngine::open_local_outboard_pull`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalOutboardHeader {
    /// Total plaintext bytes of the blob (from [`crate::CacheEngine::origin_size`]).
    pub total_bytes: u64,
}

/// A live, header-less bao-wire stream of an origin's plaintext, verified
/// on the fly against a locally-held `{H}.obao4` outboard. Obtain via
/// [`crate::CacheEngine::open_local_outboard_pull`]; drive with
/// [`Self::next_chunk`] to `Ok(None)`, then call [`Self::finish`] — or, on an
/// early exit, [`Self::abandon`] / [`Self::abandon_corrupt`].
#[derive(Debug)]
pub struct LocalOutboardPull {
    expected_wire_bytes: u64,
    wire_rx: mpsc::Receiver<Bytes>,
    encoder: Option<JoinHandle<Result<(), RangeVerifyError>>>,
    /// The task draining the origin's plaintext stream into the encoder's
    /// input channel. Aborted on every terminal path — the "prompt stop"
    /// for an early exit; the natural drop cascade (below) would eventually
    /// unwind it anyway, but aborting is immediate.
    stream_task: JoinHandle<()>,
}

/// Adapts the receiving half of the plaintext channel into
/// [`std::io::Read`] for the blocking encoder thread. Holds one leftover
/// [`Bytes`] chunk across calls when a caller's buffer is smaller than a
/// channel item.
struct PlaintextReader {
    rx_plain: mpsc::Receiver<io::Result<Bytes>>,
    leftover: Bytes,
}

impl Read for PlaintextReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.leftover.is_empty() {
            match self.rx_plain.blocking_recv() {
                None => return Ok(0), // Origin stream ended: clean EOF.
                Some(Err(e)) => return Err(e),
                Some(Ok(b)) => self.leftover = b,
            }
        }
        let n = self.leftover.len().min(buf.len());
        let (Some(src), Some(dst)) = (self.leftover.get(..n), buf.get_mut(..n)) else {
            return Ok(0);
        };
        dst.copy_from_slice(src);
        self.leftover = self.leftover.split_off(n);
        Ok(n)
    }
}

/// Adapts the sending half of the wire channel into [`std::io::Write`] for
/// the blocking encoder thread.
struct WireWriter {
    tx_wire: mpsc::Sender<Bytes>,
}

impl Write for WireWriter {
    /// Splits `buf` into [`decdn_protocol::CHUNK_SIZE`]-bounded pieces before
    /// sending, one per `blocking_send`. `encode_whole_blob_headerless` writes
    /// in whatever granularity the bao encoder buffers internally (a whole
    /// 16 KiB chunk-group's interleaved proof+data can land in one `write`
    /// call) — larger than the wire protocol's per-frame [`ChunkData`]
    /// cap. Every consumer of [`LocalOutboardPull::next_chunk`] (the window
    /// serve loop, via [`crate::CacheEngine::open_local_outboard_pull`])
    /// relays each yielded [`Bytes`] straight into one `ChunkData` frame — the
    /// same assumption the node-to-node upstream-pull path in `decdn-node`
    /// satisfies for free because its chunks were already framed to
    /// `CHUNK_SIZE` by the upstream peer. Chunking here, rather than at the
    /// `next_chunk` call site, keeps that invariant true for every consumer
    /// without each one re-deriving it.
    ///
    /// [`ChunkData`]: decdn_protocol::client::ChunkData
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for piece in buf.chunks(decdn_protocol::CHUNK_SIZE) {
            self.tx_wire
                .blocking_send(Bytes::copy_from_slice(piece))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "wire receiver dropped"))?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LocalOutboardPull {
    /// Spawn the bridge + `spawn_blocking` encoder over an already-fetched
    /// outboard and the origin's plaintext stream. Called only by
    /// [`crate::CacheEngine::open_local_outboard_pull`], which has already
    /// resolved `total_bytes` and the winning origin's outboard.
    pub(crate) fn spawn(
        hash: Hash,
        total_bytes: u64,
        outboard: Bytes,
        stream: OriginByteStream,
    ) -> Self {
        let expected_wire_bytes = bao_encoded_size(total_bytes, &bao_tree::ChunkRanges::all());

        let (tx_plain, rx_plain) = mpsc::channel::<io::Result<Bytes>>(4);
        let stream_task = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(item) = stream.next().await {
                if tx_plain.send(item).await.is_err() {
                    break;
                }
            }
        });

        let (tx_wire, wire_rx) = mpsc::channel::<Bytes>(8);
        let root = *hash.as_bytes();
        let reader = PlaintextReader {
            rx_plain,
            leftover: Bytes::new(),
        };
        let mut writer = WireWriter { tx_wire };
        let encoder = tokio::task::spawn_blocking(move || {
            encode_whole_blob_headerless(root, total_bytes, outboard, reader, &mut writer)
        });

        Self {
            expected_wire_bytes,
            wire_rx,
            encoder: Some(encoder),
            stream_task,
        }
    }

    /// The promised **wire** byte count — the bao-encoded size of the blob
    /// (content plus interleaved proof, ADR 038, header-less per
    /// [`encode_whole_blob_headerless`]'s contract). A serve loop uses it as
    /// the delivery budget.
    #[must_use]
    pub const fn expected_wire_bytes(&self) -> u64 {
        self.expected_wire_bytes
    }

    /// Read the next header-less bao wire chunk. `Ok(None)` signals a clean
    /// EOF — the encoder task ran to completion and verified every chunk
    /// group against the content root.
    ///
    /// # Errors
    ///
    /// A verification or I/O failure from the encoder (tampered origin
    /// bytes, a foreign/corrupt outboard, or an origin transport error mid-
    /// stream) surfaces here once the wire channel drains — callers route it
    /// to their corruption/abort handling.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        match self.wire_rx.recv().await {
            Some(chunk) => Ok(Some(chunk)),
            None => self.join_encoder().await.map(|()| None),
        }
    }

    /// Await the encoder task (if not already joined) and translate its
    /// outcome. Idempotent: once joined, subsequent calls are `Ok(())`.
    async fn join_encoder(&mut self) -> anyhow::Result<()> {
        let Some(handle) = self.encoder.take() else {
            return Ok(());
        };
        match handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow::Error::from(e)),
            Err(join_err) => {
                Err(anyhow::Error::from(join_err)
                    .context("local outboard pull encoder task panicked"))
            }
        }
    }

    /// Finalize a cleanly-drained pull (the caller has already seen
    /// `next_chunk` return `Ok(None)`, or wants to confirm completion
    /// without draining further): confirms the encoder task completed
    /// successfully, then stops the plaintext-forwarding task.
    ///
    /// # Errors
    ///
    /// Propagates a still-pending encoder failure (see [`Self::next_chunk`]).
    pub async fn finish(mut self) -> anyhow::Result<()> {
        let result = self.join_encoder().await;
        self.stream_task.abort();
        result
    }

    /// Abandon the pull (the downstream client dropped, or the caller no
    /// longer wants the bytes). Aborts the plaintext-forwarding task; the
    /// encoder handle and both channel halves drop with `self` — dropping
    /// `wire_rx` makes the encoder's next `blocking_send` fail, which makes
    /// it exit, which drops its `PlaintextReader`/`rx_plain`, which makes
    /// the (already-aborted) stream task's `send` fail too. No explicit
    /// `Drop` impl: the terminal methods consume `self`, so the ordinary
    /// field-drop cascade unwinds the whole pipeline on its own.
    pub fn abandon(self) {
        self.stream_task.abort();
    }

    /// Abandon the pull because a *caller-side* check (not the encoder's own
    /// bao verification, which already surfaces through [`Self::next_chunk`]
    /// / [`Self::finish`] as `Err`) found the content corrupt — e.g. a
    /// signed manifest mismatch discovered after wire bytes were already
    /// forwarded. Same mechanics as [`Self::abandon`]; kept as a distinct,
    /// self-documenting call site for that caller-side corruption path.
    pub fn abandon_corrupt(self) {
        self.stream_task.abort();
    }
}
