//! The coherent whole-range bao encoder that produces the decoupled serve leg's
//! downstream wire (ADR 038).
//!
//! A SINGLE [`bao_tree::io::fsm::encode_ranges_validated`] walks the requested
//! range in pre-order and emits ONE coherent verified stream — byte-identical to a
//! whole encode — while the pull leg fills the cache incrementally beside it:
//!
//! - leaf DATA comes from [`AwaitingDataReader`], which blocks on the store's
//!   present-range watch until the leaf's content lands (racing the pull's terminal
//!   signal for the no-hang guarantee), then reads it;
//! - the proof `(left, right)` hash pairs come from the serve leg's shared
//!   [`decdn_cache::SessionOutboardReader`], fed by the pull leg's capture;
//! - the encoded bytes are pushed through a bounded channel ([`ChannelWriter`]) and
//!   re-cut into `CHUNK_SIZE` `cdn/client/v1` frames by [`CoherentFrameProducer`].
//!
//! The encode future and the frame consumer run CONCURRENTLY on the one serve task
//! (the bounded channel backpressures the encoder), so `CoherentFrameProducer`
//! exposes a `next_frame()` frame-pull interface to the serve loop. Everything here
//! is `Send` (the iroh accept bound): the cache streams are `Send`, held only behind
//! `&mut self`.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bao_tree::ChunkRanges;
use bao_tree::io::fsm::encode_ranges_validated;
use bytes::{Bytes, BytesMut};
use decdn_bao_range::{RangedStore, align_range};
use decdn_cache::{FillSession, NodeRangedStore, PresentRangeWatch, ServeStore};
use decdn_protocol::CHUNK_SIZE;
use futures_util::StreamExt;
use iroh_io::{AsyncSliceReader, AsyncStreamWriter};
use tokio::sync::mpsc;

/// How long the data reader polls for the store to MATERIALIZE the blob (admit its
/// first chunk group) before a present-range watch can be opened.
const WATCH_OPEN_RETRY: Duration = Duration::from_millis(25);

/// Bounded backpressure between the encoder and the frame consumer: the encoder
/// parks once this many encoded chunks are buffered ahead of delivery, so the
/// upstream-paced pull is never outrun by an unbounded local encode.
const ENCODE_CHANNEL_CAP: usize = 8;

/// An [`AsyncSliceReader`] over the cache that AWAITS the pull leg filling a leaf's
/// content before reading it — the data axis of the coherent encode. Owns a
/// [`NodeRangedStore`] (a cheap `CacheEngine` handle), so the encode future can take
/// it by value and stay `'static` + `Send`.
struct AwaitingDataReader {
    store: NodeRangedStore,
    total: u64,
    /// Live present-range watch, opened lazily once the blob materializes.
    watch: Option<PresentRangeWatch>,
    /// The shared fill session: its terminal signal races the present-range watch so
    /// a pull that could not fill a gap fails the read rather than hanging.
    session: Arc<FillSession>,
}

impl AwaitingDataReader {
    fn new(store: NodeRangedStore, total: u64, session: Arc<FillSession>) -> Self {
        Self {
            store,
            total,
            watch: None,
            session,
        }
    }

    /// Does the store currently hold the whole byte range `[offset, offset + len)`?
    ///
    /// Takes `&mut self` (though it mutates nothing) so the future holds a
    /// `&mut AwaitingDataReader` rather than `&AwaitingDataReader` across the store
    /// await: the reader carries a `Send`-but-not-`Sync` [`PresentRangeWatch`], so
    /// `&AwaitingDataReader` is not `Send` and would make the whole serve future
    /// non-`Send` — which the iroh `ProtocolHandler::accept` bound forbids.
    async fn present_covers(&mut self, offset: u64, len: u64) -> io::Result<bool> {
        let missing = self
            .store
            .missing_ranges(offset, len)
            .await
            .map_err(io::Error::other)?;
        Ok(missing.is_empty())
    }
}

impl AsyncSliceReader for AwaitingDataReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let need = len as u64;
        loop {
            if self.present_covers(offset, need).await? {
                break;
            }

            // Register the pull-ended waiter BEFORE re-inspecting shared state, so a
            // terminal outcome recorded concurrently cannot slip past the check.
            let session = Arc::clone(&self.session);
            let mut ended = Box::pin(session.ended_signal().notified());
            ended.as_mut().enable();

            // Terminal pull outcome? A failed pull fails the read; a clean pull means
            // the bytes are authoritatively cached — one more check settles a lag.
            if let Some(outcome) = self.session.outcome() {
                if self.present_covers(offset, need).await? {
                    break;
                }
                return Err(match outcome {
                    Err(msg) => io::Error::other(format!(
                        "upstream pull failed before content [{offset}, +{len}) landed: {msg}"
                    )),
                    Ok(()) => io::Error::other(format!(
                        "upstream pull completed but content [{offset}, +{len}) is missing"
                    )),
                });
            }

            // Ensure a watch is open; it errors until the blob materializes —
            // tolerate that with a bounded poll racing `pull_ended`, then retry.
            if self.watch.is_none() {
                match self.store.observe().await {
                    Ok(w) => self.watch = Some(w),
                    Err(_not_materialized) => {
                        tokio::select! {
                            biased;
                            () = ended.as_mut() => {}
                            () = tokio::time::sleep(WATCH_OPEN_RETRY) => {}
                        }
                        continue;
                    }
                }
            }

            // Await the next present-range advance vs the pull ending, then re-check.
            let advanced = async {
                match self.watch.as_mut() {
                    Some(w) => w.next().await.map(|_ranges| ()),
                    None => None,
                }
            };
            tokio::select! {
                biased;
                () = ended.as_mut() => {}
                closed = advanced => {
                    if closed.is_none() {
                        self.watch = None; // watch stream ended; re-open next pass
                    }
                }
            }
        }

        self.store
            .read(offset, need)
            .await
            .map_err(io::Error::other)
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.total)
    }
}

/// An [`AsyncStreamWriter`] that forwards the encoder's sequential output into a
/// bounded channel the frame consumer drains. A closed receiver (the serve aborted)
/// surfaces as a write error, ending the encode.
struct ChannelWriter {
    tx: mpsc::Sender<Bytes>,
}

impl AsyncStreamWriter for ChannelWriter {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes(Bytes::copy_from_slice(data)).await
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.tx
            .send(data)
            .await
            .map_err(|_| io::Error::other("serve encode: downstream frame channel closed"))
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Drives the coherent whole-range encode and re-cuts its output into `CHUNK_SIZE`
/// frames. [`Self::next_frame`] yields the next
/// wire frame, `None` once the whole range is delivered, `Err` on an encode fault
/// (a gap the pull could not fill, or a proof/verify error) — on which the serve
/// leg must not send `StreamEnd`.
pub(super) struct CoherentFrameProducer {
    /// The running encode future, taken out while polled and re-stored if it parks.
    /// `None` once it has completed (its channel sender is then dropped, so the
    /// receiver drains and ends).
    enc: Option<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>>,
    rx: mpsc::Receiver<Bytes>,
    frame_buf: BytesMut,
}

impl CoherentFrameProducer {
    /// Build the producer for request range `[offset, end)` of `hash` (a
    /// `total`-byte blob), reading data from `store` (awaiting the pull) and proof
    /// nodes from a [`decdn_cache::SessionOutboardReader`] minted from the shared `session`.
    pub(super) fn new(
        store: NodeRangedStore,
        session: Arc<FillSession>,
        offset: u64,
        end: u64,
        total: u64,
    ) -> Self {
        // The chunk-group-aligned ranges the client's verified stream covers (ADR
        // 038). `end == offset` (empty request) yields empty ranges — an empty
        // stream — handled naturally by the encoder.
        let ranges = if end > offset {
            align_range(offset, end - offset, total)
                .map_or_else(|_| ChunkRanges::empty(), |a| a.chunk_ranges().clone())
        } else {
            ChunkRanges::empty()
        };

        let outboard = session.outboard_reader();
        let data = AwaitingDataReader::new(store, total, session);
        let (tx, rx) = mpsc::channel(ENCODE_CHANNEL_CAP);
        let writer = ChannelWriter { tx };

        let enc: Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> = Box::pin(async move {
            let mut writer = writer;
            let mut data = data;
            let mut outboard = outboard;
            encode_ranges_validated(&mut data, &mut outboard, ranges.as_ref(), &mut writer)
                .await
                .map_err(|e| anyhow::anyhow!("coherent range encode failed: {e}"))
        });

        Self {
            enc: Some(enc),
            rx,
            frame_buf: BytesMut::new(),
        }
    }

    /// The next `CHUNK_SIZE` wire frame (or the shorter final remainder), `None`
    /// once the whole range is delivered.
    pub(super) async fn next_frame(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            if self.frame_buf.len() >= CHUNK_SIZE {
                let take = self.frame_buf.len().min(CHUNK_SIZE);
                return Ok(Some(self.frame_buf.split_to(take).freeze()));
            }
            if let Some(bytes) = self.pump().await? {
                self.frame_buf.extend_from_slice(&bytes);
            } else {
                // Encoder finished and channel drained: flush any final partial
                // frame, then signal completion.
                if self.frame_buf.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(self.frame_buf.split().freeze()));
            }
        }
    }

    /// Advance the encode and/or receive its next output chunk. `Some(bytes)` when a
    /// chunk arrives; `None` once the encoder is done AND the channel is drained; an
    /// encode fault propagates as `Err`.
    async fn pump(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            match self.enc.take() {
                Some(mut fut) => {
                    tokio::select! {
                        biased;
                        res = fut.as_mut() => {
                            // Encoder finished: do NOT re-store `fut`, so its channel
                            // sender drops and the receiver will drain then end.
                            res?;
                        }
                        recv = self.rx.recv() => {
                            self.enc = Some(fut); // still encoding — keep the future
                            return Ok(recv);
                        }
                    }
                }
                None => return Ok(self.rx.recv().await),
            }
        }
    }
}
