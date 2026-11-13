//! The decoupled downstream **serve leg** of the node serve-miss driver (ADR
//! 037).
//!
//! [`ClientHandler::serve_leg`] is the seller half of the two-leg serve-miss.
//! It walks the requested range `R = [offset, offset + len)` in order and
//! delivers ALL of it to the paying client. The downstream wire is produced by a
//! coherent whole-range bao encoder ([`super::serve_encoder::CoherentFrameProducer`],
//! ADR 038): it reads leaf DATA from the cache the pull leg fills — awaiting the
//! store's present-range frontier when a span has not landed yet — and proof nodes
//! from the shared outboard the pull captures, so the whole range is ONE coherent
//! verified stream. Credit, takedown, and client-disconnect handling run over the
//! cache the pull leg fills beside this task: the byte source is the local store,
//! read as a coherent verified stream, not an upstream connection.
//!
//! # Coordination with the pull leg (shared `FillSession` state)
//!
//! The pull leg runs on its own runtime. The legs share the `FillSession` behind an
//! `Arc`, whose frontiers and wakeups work across runtimes:
//!
//! - **`served_paid`** — the content frontier the downstream lane has paid for:
//!   this stream's delivered wire plus the credit its lane holds, mapped into
//!   content through [`content_paid_frontier`]. The serve leg raises it before each
//!   frame it cuts. The pull leg's `CreditPacer` reads it, and draws only what the
//!   lane has paid for, plus [`decdn_client::PULL_AHEAD_BYTES`]. The node's upstream
//!   payment for a stream therefore never exceeds the credit its lane holds, and
//!   the pull draws nothing while the lane holds none.
//! - **`serve_demand`** — the content end of the span this leg's encoder waits
//!   on, for a leaf or a proof node the store does not hold yet, stood through the
//!   leg's `DemandSlot` only while its frame producer has no encoded bytes left,
//!   and withdrawn once bytes flow again. It reaches only the fill whose pull
//!   fetches the awaited byte, and that pull reads the nearest standing demand, so
//!   another leg waiting further down the blob never hides this one. The leg cuts a
//!   frame of at most `max(credit, one chunk group)` wire bytes, so its encoder
//!   waits on the first byte the pull has not fetched, within one group past the
//!   pull's frontier. The pull draws [`decdn_client::PULL_AHEAD_BYTES`] past the
//!   credit frontier, so neither leg waits on the other forever.
//! - **Downstream wakeup** — every move of `served_paid` or the nearest `serve_demand` wakes
//!   a pull leg parked in `PaceDecision::Wait` (`DownstreamWatch::past`), so it
//!   re-decides exactly when the credit frontier advances or this leg starts
//!   waiting.
//! - **Pull outcome + liveness** — the pull records its outcome with
//!   `FillSession::mark_ended`, which fires the per-hash liveness signal. A serve
//!   read parked on a gap races the present-range watch against that signal and
//!   re-checks `range_still_live`: once no live fill covers the gap the read
//!   settles instead of hanging. It is served from the store when the store
//!   holds the bytes and proof pairs, and fails otherwise. This is the no-hang
//!   guarantee.
//!
//! The serve leg owns termination: it is what fulfils `R` for the client, so its
//! completion (or error) ends the serve and drops the pull leg.

use decdn_cache::{FillSession, NodeRangedStore};
use decdn_client::sink::content_paid_frontier;
use iroh::endpoint::{ConnectionError, StoppedError};

use crate::metrics::FirstByteClock;

use super::credit::{Acquired, SendGuards};
use super::outcome::{ServeEnd, ServeStop};
use super::proof_wait::ProofWaitFault;
use super::serve_waits::{ServeWaits, Wait};
use super::voucher::StreamAnchor;
use super::wire::chunk_frame_bufs;
use super::{
    Arc, B256, BufferedProofReader, CHUNK_BYTES, CHUNK_GROUP_BYTES, ClientHandler, ClientMessage,
    Connection, Hash, LaneDeliveryState, LaneKey, Mutex, RecvStream, SendStream,
    ServeRejectReason,
};

/// The downstream credit frontier a serve leg publishes to its pull leg.
#[derive(Debug, Clone, Copy)]
struct CreditFrontier {
    /// The content offset the leg's wire starts at: the chunk-group floor of the
    /// request's offset. [`content_paid_frontier`] requires a group-aligned start.
    fetch_start: u64,
    /// The blob's whole-blob size.
    total_bytes: u64,
}

impl CreditFrontier {
    /// Publish the content frontier that `paid_wire` bytes of this stream's wire
    /// cover, to the owning fill and to every observer fill that this leg also
    /// paces.
    ///
    /// The mapping is conservative: [`content_paid_frontier`] returns the largest
    /// chunk-group boundary provably inside the paid wire prefix, so the pull never
    /// overshoots the credit. The raise is forward-only and guarded on THIS leg's
    /// start: an owning session's frontier starts at the raw `byte_offset`, at or
    /// above its group floor `fetch_start`, so the guard is a no-op there, and N
    /// whole-range observers advance the SHARED frontier with the pull's pacer
    /// binding on the MAX-over-observers frontier. An observer ATTACHED at an offset
    /// the owner's paid prefix has not reached yet must not lift that prefix past
    /// bytes nobody paid for. Its payment extends the frontier once the prefix
    /// reaches it (`FillSession::extend_served_from`).
    fn publish(self, session: &FillSession, also_pace: &[Arc<FillSession>], paid_wire: u64) {
        let served = content_paid_frontier(self.fetch_start, self.total_bytes, paid_wire);
        session.extend_served_from(self.fetch_start, served);
        // Under partial-overlap coalescing each attached sibling pull produces the
        // OVERLAP this leg also consumes and bills; the same guard applies to every
        // sibling.
        for extra in also_pace {
            extra.extend_served_from(self.fetch_start, served);
        }
    }
}

impl ClientHandler {
    /// Deliver the requested range `R = [offset, offset + len)` to the paying
    /// client from the cache, awaiting a concurrent pull leg to fill any gaps —
    /// the downstream (seller) half of the decoupled serve-miss (ADR
    /// 037). `len == 0` means "to the end of the blob".
    ///
    /// Delivery, credit, the in-flight takedown boundary, and client-disconnect
    /// handling all read from the cache: a coherent whole-range bao encoder reads
    /// the cache the pull leg fills. The caller (orchestration) has
    /// already proven lane authority, run the pre-flight gates, and signed +
    /// sent the `StreamResponse`; the pull leg fills the store beside this call.
    /// Consumes neither stream — the caller does.
    ///
    /// # Money semantics
    ///
    /// The node sends a frame only after it has debited the lane's credit for it
    /// (ADR 003 § Credit Window), so completion never leaves delivered bytes
    /// unpaid. The credit counters are wire quantities (content plus interleaved
    /// bao proof, ADR 038). The shared frontier the pull leg paces against is mapped
    /// back into content space via [`content_paid_frontier`], and it covers only
    /// what the lane has paid for. A lane with no credit draws nothing upstream.
    ///
    /// # Errors
    ///
    /// A client disconnect (the write / proof read surfaces it), a rate-check
    /// bail, a lane-store `record` failure, an `encode_range` fault, an offset at
    /// or past the blob end, or a gap the pull leg could not fill. On any error the
    /// caller drops the pull leg, which stops the upstream spend and persists the
    /// buyer watermark. The peer-attributable errors carry
    /// [`PeerFault`](super::wire::PeerFault) or
    /// [`ClientPaymentFault`](super::wire::ClientPaymentFault); the rest reach the
    /// dispatch sink's `error!` as node faults. An error after a proof credited
    /// bytes also carries [`PaidProgress`](super::wire::PaidProgress).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn serve_leg(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        store: NodeRangedStore,
        session: Arc<FillSession>,
        also_pace: &[Arc<FillSession>],
        lane: &Arc<Mutex<LaneDeliveryState>>,
        hash: Hash,
        lane_key: LaneKey,
        client_node_id: B256,
        rate_per_mb: u64,
        offset: u64,
        len: u64,
        total_bytes: u64,
        first_byte: FirstByteClock,
    ) -> anyhow::Result<ServeEnd> {
        // Bytes that accepted proofs added to the lane. Owned here so every exit of
        // the loop, `?` included, passes through the one `PaidProgress` tag: the
        // dispatch sink splits a peer that left into declined or abandoned by it.
        let mut paid: u64 = 0;
        // Wire bytes this stream wrote. A stream that ends without a clean close
        // leaves its end offset on the lane for a backfill.
        let mut delivered: u64 = 0;
        // Refresh recency before the first byte goes out, so the blob does not
        // rank oldest for eviction while it streams. Recency only: the frequency
        // sighting waits for `StreamEnd`.
        self.cache.touch_recency(hash);
        Self::log_backfill(lane_key, hash, self.apply_backfill(lane, hash, offset).await);
        let ended = self
            .serve_leg_loop(
                conn,
                send,
                recv,
                store,
                session,
                also_pace,
                lane,
                hash,
                lane_key,
                client_node_id,
                rate_per_mb,
                offset,
                len,
                total_bytes,
                first_byte,
                &mut paid,
                &mut delivered,
            )
            .await;
        if !matches!(ended, Ok(ServeEnd::Completed { .. })) && delivered > 0 {
            let fetch_start = offset - offset % CHUNK_GROUP_BYTES;
            let end = content_paid_frontier(fetch_start, total_bytes, delivered);
            self.note_unclean_end(lane, hash, end).await;
        }
        ended.map_err(|e| super::wire::tag_paid_progress(e, paid))
    }

    /// Await `frame`, the encoder's next frame, unless the stream ends first.
    ///
    /// `client_left` is the send stream's `stopped()` future. Before the leg
    /// finishes its stream, it resolves when the client stops the stream or the
    /// connection closes for any reason, this node's own close included. A frame
    /// that is ready wins. [`client_left_error`] attributes the end, and a client
    /// that left meters a pull-through abandon.
    async fn frame_unless_client_left<T>(
        &self,
        frame: impl Future<Output = anyhow::Result<T>>,
        client_left: std::pin::Pin<
            &mut impl Future<Output = Result<Option<super::VarInt>, StoppedError>>,
        >,
    ) -> anyhow::Result<T> {
        tokio::select! {
            biased;
            frame = frame => frame.map_err(|e| self.meter_frame_fault(e)),
            left = client_left => {
                let (err, abandoned) = client_left_error(left);
                if abandoned {
                    self.metrics.node_pull_through_client_abandoned();
                }
                Err(err)
            }
        }
    }

    /// End the stream for a frame read of `hash` that failed with `e`. A read
    /// of bytes the node cannot land under its cap on bytes held for the blob
    /// is a refusal, not a fault: the node served the `delivered` bytes that
    /// landed, refuses the rest with a mid-stream `Declined`, and the requester
    /// asks another node. Such a read is one this serve's own fill ended at the
    /// cap ([`decdn_cache::FillError::held_bytes_cap`]), or one of bytes no fill
    /// landed ([`decdn_cache::is_unlanded`]) while the store already holds the
    /// cap's worth of the blob: a sibling fill that covered them ended at the
    /// cap. Every other error passes through unchanged.
    async fn stop_at_held_cap(
        &self,
        send: &mut SendStream,
        e: anyhow::Error,
        hash: Hash,
        delivered: u64,
    ) -> anyhow::Result<ServeEnd> {
        let cap = self
            .pull_through_origin
            .as_ref()
            .map_or(0, |origin| origin.max_blob_size_bytes());
        let at_cap = e.chain().any(decdn_cache::ended_at_held_bytes_cap)
            || (cap > 0
                && e.chain().any(decdn_cache::is_unlanded)
                && self
                    .cache
                    .present_ranges(hash)
                    .await
                    .is_ok_and(|present| present.held_bytes() >= cap));
        if !at_cap {
            return Err(e);
        }
        tracing::info!(
            %hash,
            delivered,
            cap,
            error = %format_args!("{e:#}"),
            "pull-through fill ended at this node's cap on bytes held; refusing the rest"
        );
        self.write_midstream_refusal(send, ServeRejectReason::BlobTooLarge)
            .await
            .map_err(|write| write.context(format!("{e:#}")))?;
        Ok(ServeEnd::Stopped {
            stop: ServeStop::BlobTooLarge,
            bytes: delivered,
        })
    }

    /// [`Self::acquire_credit`], with the miss leg's client-abandon metering: a
    /// peer-attributable fault or a stop that the payer caused counts as a
    /// pull-through abandon. A chunk that passes its proof-wait ceiling does not:
    /// its connection was still sending, so a slow path or a busy sibling stream
    /// may be the cause rather than a client that left. A node-side fault carries
    /// neither marker, and the dispatch sink meters it on
    /// `decdn_serve_stream_node_fault_total` instead.
    #[allow(clippy::too_many_arguments)]
    async fn acquire_credit_for_leg(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        waits: &mut ServeWaits,
        guards: &mut SendGuards,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        client_node_id: B256,
        rate_per_mb: u64,
        len: u64,
        charge: bool,
        paid: &mut u64,
    ) -> anyhow::Result<Acquired> {
        match self
            .acquire_credit(
                conn,
                send,
                recv,
                reader,
                anchor,
                waits,
                guards,
                hash,
                lane_key,
                lane,
                client_node_id,
                rate_per_mb,
                len,
                charge,
                paid,
            )
            .await
        {
            Ok(Acquired::Stopped(
                stop @ (ServeStop::VoucherRejected | ServeStop::ProofBudgetExhausted),
            )) => {
                self.metrics.node_pull_through_client_abandoned();
                Ok(Acquired::Stopped(stop))
            }
            Ok(other) => Ok(other),
            Err(e) => {
                if super::wire::is_peer_attributable(&e) && !ProofWaitFault::is_past_ceiling(&e)
                {
                    self.metrics.node_pull_through_client_abandoned();
                }
                Err(e)
            }
        }
    }

    /// The body of [`Self::serve_leg`]. `paid` counts the bytes accepted proofs
    /// added to the lane, and `delivered` the wire bytes written. The caller reads
    /// both when the loop ends.
    // One linear, ADR-ordered miss-leg serve loop; splitting it would scatter the
    // ordering invariants across helpers (same rationale as `deliver`).
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    async fn serve_leg_loop(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        store: NodeRangedStore,
        session: Arc<FillSession>,
        also_pace: &[Arc<FillSession>],
        lane: &Arc<Mutex<LaneDeliveryState>>,
        hash: Hash,
        lane_key: LaneKey,
        client_node_id: B256,
        rate_per_mb: u64,
        offset: u64,
        len: u64,
        total_bytes: u64,
        first_byte: FirstByteClock,
        paid: &mut u64,
        delivered: &mut u64,
    ) -> anyhow::Result<ServeEnd> {
        // An offset at or past the blob end has no bytes to deliver, and the
        // clamp below would fold it into the legitimate `end == offset` empty
        // request — a signed non-empty promise answered with zero bytes and a
        // clean `StreamEnd`. Refuse it instead. `dispatch`'s bounds gate already
        // refuses the same ranges with `RangeNotSatisfiable` before it signs, so
        // this is a backstop for a caller that skips that gate; it mirrors
        // `align_range_clamped`'s own bound: an end past the blob clamps below
        // rather than being refused here.
        anyhow::ensure!(
            total_bytes == 0 || offset < total_bytes,
            "serve offset {offset} is at or past the {total_bytes}-byte blob end"
        );

        // Resolve the request end. `len == 0` ⇒ to the blob end (driver
        // convention); otherwise clamp to the tree size.
        let end = super::serve_encoder::serve_end(offset, len, total_bytes);
        let offset = offset.min(end);
        // The wire this leg delivers starts at the chunk-group floor of `offset`
        // (`align_range` snaps the fetch start down), so credited wire maps back to
        // content from THAT boundary — `content_paid_frontier` requires a
        // group-aligned fetch start. Equal to `offset` for an aligned request.
        let frontier = CreditFrontier {
            fetch_start: (offset / CHUNK_GROUP_BYTES).saturating_mul(CHUNK_GROUP_BYTES),
            total_bytes,
        };

        let chunk_bytes = CHUNK_BYTES;

        // Records the loop's waits and the path's state at every exit
        // (#2348), from the loop's first await on.
        let mut waits = ServeWaits::start(conn);
        let mut first_byte = Some(first_byte);
        // One buffered proof reader for the whole stream: every read goes through
        // it so a pipelined proof buffered ahead of the current one is not lost.
        let mut reader = BufferedProofReader::default();
        // This stream's chain anchor — which epoch it has been told about, so a
        // bare reveal arriving on it can be placed (ADR 003 §Concurrent
        // Streams, Rule 1). Per stream and in memory only: a restart drops the
        // streams, and the lane's durable record keeps the strongest claim's
        // chain state.
        let mut anchor = StreamAnchor::default();
        // The takedown, pool and signer checks that run before each send.
        let mut guards = self.send_guards(lane, &lane_key).await;

        // The coherent whole-range bao encoder (ADR 038): ONE verified stream for
        // `R`, produced incrementally — leaf data awaited from the cache the pull
        // fills, proof nodes from the shared `outboard` the pull captures.
        let mut producer = super::serve_encoder::CoherentFrameProducer::new(
            store,
            Arc::clone(&session),
            offset,
            end,
            total_bytes,
        )?;

        // Resolves once the client stops the stream or the connection drops. A
        // frame wait touches neither stream, so without it a serve leg parked on
        // the pull would never learn its client left, and would hold its lease and
        // the pull alive (#2194).
        let mut client_left = std::pin::pin!(send.stopped());

        loop {
            // The lane must hold credit before the pull leg draws anything. A
            // payer that never pays costs the node no upstream payment, because
            // this wait ends only when a proof raises the lane's credit.
            let mut credit = self.lane_credit(lane).await;
            if credit == 0 {
                if let Acquired::Stopped(stop) = self
                    .acquire_credit_for_leg(
                        conn,
                        send,
                        recv,
                        &mut reader,
                        &mut anchor,
                        &mut waits,
                        &mut guards,
                        hash,
                        lane_key,
                        lane,
                        client_node_id,
                        rate_per_mb,
                        1,
                        false,
                        paid,
                    )
                    .await?
                {
                    return Ok(ServeEnd::Stopped {
                        stop,
                        bytes: *delivered,
                    });
                }
                credit = self.lane_credit(lane).await;
            }
            // The credit frontier the pull leg paces against: what this stream has
            // sent plus what its lane has paid for and not yet sent.
            frontier.publish(&session, also_pace, delivered.saturating_add(credit));

            // The next frame is cut to the lane's credit and to the payment-chunk
            // boundary ([`ClientHandler::frame_target`]), so the cut asks for
            // bytes the pull may draw. The opening frame is small, so the first
            // byte waits for a few chunk groups of pull rather than a whole chunk.
            let target = if *delivered == 0 {
                self.first_frame_target(chunk_bytes, credit)
            } else {
                self.frame_target(*delivered % chunk_bytes, chunk_bytes, credit)
            };
            let frame = match waits
                .time(
                    Wait::Store,
                    self.frame_unless_client_left(
                        producer.next_frame_chunks(target),
                        client_left.as_mut(),
                    ),
                )
                .await
            {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(e) => return self.stop_at_held_cap(send, e, hash, *delivered).await,
            };
            let clen = frame.total() as u64;

            // The node sends the frame only after it has debited the lane's credit
            // for it. A stop here leaves the credit on the lane, and the caller
            // drops the pull leg, which bounds the upstream spend.
            if let Acquired::Stopped(stop) = self
                .acquire_credit_for_leg(
                    conn,
                    send,
                    recv,
                    &mut reader,
                    &mut anchor,
                    &mut waits,
                    &mut guards,
                    hash,
                    lane_key,
                    lane,
                    client_node_id,
                    rate_per_mb,
                    clen,
                    true,
                    paid,
                )
                .await?
            {
                return Ok(ServeEnd::Stopped {
                    stop,
                    bytes: *delivered,
                });
            }

            // Assemble before the write, and outside the metered block: a framing
            // fault here is the node's own bug, and metering it as a client
            // abandon would file it under the peer's behaviour.
            let bufs = chunk_frame_bufs(&frame).map_err(|e| self.meter_frame_fault(e))?;
            // A downstream drop surfaces here as a peer-attributable `Err` (#856
            // client-disconnect shape) and meters the client abandon. A write to
            // a stream this node already closed carries no marker: the dispatch
            // sink meters it as a node fault instead. Either way, propagate so
            // the caller drops the pull leg.
            if let Err(e) = waits
                .time(Wait::Send, self.write_chunk_bufs(send, bufs))
                .await
            {
                if super::wire::is_peer_attributable(&e) {
                    self.metrics.node_pull_through_client_abandoned();
                }
                return Err(e);
            }
            if let Some(clock) = first_byte.take() {
                clock.record(&self.metrics);
            }
            *delivered = delivered.saturating_add(clen);
            self.shed.record_egress(clen);
            self.metrics.bytes_served(clen);
        }

        // Fully delivered, and every byte was paid for before it was sent. Every
        // byte was bao-verified into the cache by the pull leg's admit before this
        // leg read it, so the served bytes are sound.
        waits
            .time(
                Wait::Send,
                self.write_message(send, &ClientMessage::StreamEnd),
            )
            .await?;
        let _ = send.finish();
        // Drain the client's send half to its FIN before `recv` drops, so the
        // client's finish is not stopped by the implicit `STOP_SENDING(0)` a
        // dropped `RecvStream` issues (see `drain_recv_to_fin`).
        super::drain_recv_to_fin(recv).await;
        // ADR 040: emit the ONE hit sighting for this served request, only after the
        // terminal StreamEnd frame is written — so a serve that fails to finish cleanly
        // is never counted. This is necessarily after the concurrent pull leg filled the
        // whole range and ran its fill-time admission read, so the serve's `observe`
        // follows the admission estimate read, preserving the ordering invariant even
        // though the two legs run concurrently. One sighting per served blob, regardless
        // of range or interval count.
        self.cache.observe_hit(hash);
        // ADR 041: credit the realized operator margin back to the source that
        // speculatively warmed this blob (a no-op for an untagged / non-speculative
        // hash), on the same clean-completion edge as the ADR 040 hit sighting.
        self.credit_warming_serve(hash, *delivered);
        Ok(ServeEnd::Completed { bytes: *delivered })
    }
}

/// Attribute a serve leg's stream ending while the leg waits on the pull: the error
/// to end the serve with, and whether the client abandoned it.
///
/// - The client stopped the stream, or the connection was lost on the client's
///   side: a [`PeerFault`](super::wire::PeerFault) and an abandon, as a write to a
///   gone client is. The connection error stays in the chain.
/// - This node closed the connection (shutdown): a `PeerFault`, as the write path
///   files it, but no abandon. The client did not leave.
/// - The stream state is gone before this node finished it (`Ok(None)`), or a 0-RTT
///   rejection: faults in this node's own stream state machine, so no marker, as
///   `write_chunk_error` files them.
fn client_left_error(left: Result<Option<super::VarInt>, StoppedError>) -> (anyhow::Error, bool) {
    const WAITING: &str = "while the serve waited on the pull";
    match left {
        Ok(Some(code)) => (
            anyhow::Error::new(super::wire::PeerFault)
                .context(format!("client stopped the stream (code {code}) {WAITING}")),
            true,
        ),
        Err(e @ StoppedError::ConnectionLost(ConnectionError::LocallyClosed)) => (
            anyhow::Error::new(e)
                .context(super::wire::PeerFault)
                .context(format!("this node closed the connection {WAITING}")),
            false,
        ),
        Err(e @ StoppedError::ConnectionLost(_)) => (
            anyhow::Error::new(e)
                .context(super::wire::PeerFault)
                .context(format!("client connection lost {WAITING}")),
            true,
        ),
        Ok(None) => (
            anyhow::anyhow!("send stream closed on this node {WAITING}"),
            false,
        ),
        Err(e @ StoppedError::ZeroRttRejected) => (
            anyhow::Error::new(e).context(format!("send stream {WAITING}")),
            false,
        ),
    }
}

#[cfg(test)]
mod tests;
