//! An open `cdn/client/v1` stream whose verified answer is in hand and whose
//! first voucher is not yet sent ([`OpenedStream`]).
//!
//! Every paid pull starts as one: the open sends the [`StreamRequest`],
//! reads the signed [`StreamResponse`] and verifies it, and the pull then
//! adopts the stream ([`OpenedStream::into_pull`]). Discovery stops after the
//! answer ([`discovery_open`]): an answer costs the node nothing before the
//! first voucher, so an open is how a fetch learns a node's quote, its proven
//! size of the blob, and its coverage. The fetch adopts the winner's stream
//! and cancels the others ([`OpenedStream::cancel`]).
//!
//! [`StreamRequest`]: decdn_protocol::client::StreamRequest
//! [`StreamResponse`]: decdn_protocol::client::StreamResponse

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256};
use anyhow::Context as _;
use decdn_protocol::client::{StreamResponse, StreamResponseExt};
use decdn_protocol::{CHUNK_BYTES, Coverage};
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, RecvStream, SendStream};

use crate::connection::ConnOwner;
use crate::path_rtt::{DIRECT_PATH_GRACE, PathRtt, path_rtt};
use crate::{
    BlobTooLarge, ConnSource, Connections, PoolContext, PoolLedger, ProvenSize, PullDeadlines,
    PullTimeout, RateAboveCeiling, StreamMeter, UpstreamPull, UpstreamPullHeader, UpstreamRefused,
    aligned_wire_len, exchange, progress, served_wire_len, stall_sample_period, verify_response,
};

/// A node's verified answer that did not verify: its signature, its echoed
/// fields, its size proof, or its coverage. A discovery open tags its
/// verification fault with this, so the caller can tell a node that answered
/// wrongly from one that did not answer.
#[derive(Debug, Clone, Copy)]
pub struct UnverifiedAnswer;

impl std::fmt::Display for UnverifiedAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the node's answer did not verify")
    }
}

impl std::error::Error for UnverifiedAnswer {}

/// One `cdn/client/v1` stream with its verified answer and no voucher sent.
///
/// The node holds the stream open, and pins its blob, until the first voucher
/// or until the stream ends. It does no fill work and reads no pool before that
/// voucher, so a stream the caller drops costs the node nothing. Exactly one
/// of three things ends it: [`Self::into_pull`] adopts it as a paid pull,
/// [`Self::cancel`] resets it, or a drop finishes it. None of them closes a
/// shared connection.
pub struct OpenedStream {
    pub(crate) conn: Connection,
    pub(crate) owner: ConnOwner,
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
    pub(crate) resp: StreamResponse,
    pub(crate) resp_ext: StreamResponseExt,
    /// The blob size the answer proves; `None` for a refusal.
    pub(crate) proven_size: Option<ProvenSize>,
    pub(crate) hash: [u8; 32],
    pub(crate) byte_offset: u64,
    pub(crate) byte_len: u64,
    /// The pool the request named.
    pub(crate) pool_id: B256,
    /// The key the request bound: the lane signer every voucher on the
    /// stream must sign under.
    pub(crate) signer: Address,
    /// The connection's path RTT, measured after the answer by a discovery
    /// open; `None` for a pull's open, which does not wait for it.
    pub(crate) rtt: Option<PathRtt>,
    /// When the answer arrived.
    pub(crate) answered_at: tokio::time::Instant,
}

impl std::fmt::Debug for OpenedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedStream")
            .field("peer", &self.conn.remote_id())
            .field("hash", &blake3::Hash::from_bytes(self.hash))
            .field("ok", &self.resp.body.ok)
            .field("rate_per_mb", &self.resp.body.rate_per_mb)
            .field("proven_size", &self.proven_size)
            .finish_non_exhaustive()
    }
}

impl OpenedStream {
    /// The node that answered.
    #[must_use]
    pub fn peer(&self) -> iroh::PublicKey {
        self.conn.remote_id()
    }

    /// The node's signed answer, verified against its operator address.
    #[must_use]
    pub const fn response(&self) -> &StreamResponse {
        &self.resp
    }

    /// The answer's unsigned extension: the size proof, the refusal class,
    /// the coverage hint.
    #[must_use]
    pub const fn ext(&self) -> &StreamResponseExt {
        &self.resp_ext
    }

    /// Whether the node serves the span: a signed `ok`.
    #[must_use]
    pub const fn served(&self) -> bool {
        self.resp.body.ok
    }

    /// The per-MiB rate the node signed.
    #[must_use]
    pub const fn rate_per_mb(&self) -> u64 {
        self.resp.body.rate_per_mb
    }

    /// The blob size the answer proves; `None` for a refusal.
    #[must_use]
    pub const fn proven_size(&self) -> Option<ProvenSize> {
        self.proven_size
    }

    /// The discovery blocks the node says it holds or can serve. A hint:
    /// unsigned, and checked only against the signed `ok` for the span.
    #[must_use]
    pub const fn coverage(&self) -> &Coverage {
        &self.resp_ext.coverage
    }

    /// The span the request asked for: `(byte_offset, byte_len)`, a `byte_len`
    /// of `0` running to the end.
    #[must_use]
    pub const fn span(&self) -> (u64, u64) {
        (self.byte_offset, self.byte_len)
    }

    /// The connection's path RTT in milliseconds, and whether a direct path
    /// reported it; `None` when the open did not measure it.
    #[must_use]
    pub fn rtt(&self) -> Option<(f64, bool)> {
        self.rtt.map(|rtt| (rtt.ms(), rtt.direct))
    }

    /// When the answer arrived. A node drops a stream that sees no voucher
    /// for a while, so an adopter reads the stream's age from here.
    #[must_use]
    pub const fn answered_at(&self) -> tokio::time::Instant {
        self.answered_at
    }

    /// Whether `ctx` can pay this stream: the request named `ctx`'s pool and
    /// bound `ctx`'s signer, and the node holds the stream for that lane alone.
    #[must_use]
    pub fn paid_by(&self, ctx: &PoolContext) -> bool {
        self.pool_id == ctx.pool_id && self.signer == ctx.client_signer.address()
    }

    /// Reset the stream: `send.reset(0)` and `recv.stop(0)`. The node ends it
    /// unclaimed and releases its pin. A one-shot connection closes with it;
    /// a shared one stays open for its other streams.
    pub fn cancel(mut self) {
        if self.owner.owns() {
            self.conn.close(0u32.into(), b"open-cancelled");
        } else {
            let _ = self.send.reset(0u32.into());
            let _ = self.recv.stop(0u32.into());
        }
    }

    /// Adopt the stream as a paid pull on `ctx`'s lane, paying through
    /// `ledger`. The answer was verified when the stream opened; this checks
    /// what the payer decides: the refusal, the rate ceiling
    /// `max_rate_per_mb` (`0` for none), and the cap `max_blob_size_bytes`
    /// (`0` for none) on the stream's wire. Nothing is paid before the checks
    /// pass. `deadlines` sets the pull's throughput floor.
    ///
    /// # Errors
    ///
    /// - [`UpstreamRefused`] for a signed refusal;
    /// - [`RateAboveCeiling`] for a quote above `max_rate_per_mb`;
    /// - [`crate::ResumeOffsetPastEnd`] or a range-alignment fault for a span
    ///   past the proven size;
    /// - [`BlobTooLarge`] for a stream whose wire runs past the cap;
    /// - a plain error when `ctx` cannot pay the stream ([`Self::paid_by`]).
    pub fn into_pull(
        self,
        ctx: &PoolContext,
        ledger: Arc<PoolLedger>,
        max_blob_size_bytes: u64,
        max_rate_per_mb: u64,
        deadlines: PullDeadlines,
    ) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
        anyhow::ensure!(
            self.paid_by(ctx),
            "the stream was opened for pool {} and signer {}, not the lane's pool {} and \
             signer {}",
            self.pool_id,
            self.signer,
            ctx.pool_id,
            ctx.client_signer.address()
        );
        let Self {
            conn,
            owner,
            send,
            recv,
            resp,
            resp_ext,
            proven_size,
            hash,
            byte_offset,
            byte_len,
            ..
        } = self;
        let Some(proven) = proven_size.filter(|_| resp.body.ok) else {
            return Err(UpstreamRefused::open(resp, &resp_ext));
        };
        // A quote above the payer's ceiling is refused before the first paid
        // interval. `0` = no ceiling.
        if max_rate_per_mb > 0 && resp.body.rate_per_mb > max_rate_per_mb {
            return Err(RateAboveCeiling::over_ceiling(
                resp.body.rate_per_mb,
                max_rate_per_mb,
            ));
        }
        // The node accepted at a rate this pull pays: its signed answer is
        // receipt evidence for the lane (#2599).
        if let Some(evidence) = ledger.evidence() {
            evidence.note_response(&resp);
        }
        let rate_per_mb = resp.body.rate_per_mb;
        // Wire-byte bound (bao-encoded size of the aligned range), not content
        // bytes: the window path forwards this stream verbatim and pays the
        // upstream in wire bytes (ADR 038 §Payment metering).
        let total_bytes = proven.get();
        let expected_wire_bytes = served_wire_len(byte_offset, byte_len, total_bytes)?;
        // Per-stream bound on received bytes (#1895), as WIRE so `next_chunk`
        // can enforce it without decoding: the wire size of the cap's worth of
        // content from this offset. The cap bounds the bytes held for one blob,
        // so one stream never carries more content than the cap, wherever in
        // the blob it starts. The caller's driver meters the bytes the store
        // holds across streams (`driver::fill_gap`). `0` = unlimited. The empty
        // blob carries no content, so a cap has nothing to bound.
        let max_received_wire = if max_blob_size_bytes == 0 || total_bytes == 0 {
            0
        } else {
            aligned_wire_len(byte_offset, max_blob_size_bytes, total_bytes)?
        };
        // The size is proven, so a stream whose wire runs past the cap can
        // never complete. Refuse it before paying anything.
        if max_received_wire > 0 && expected_wire_bytes > max_received_wire {
            return Err(anyhow::Error::new(BlobTooLarge {
                reached: expected_wire_bytes,
                ceiling: max_received_wire,
            }));
        }
        let header = UpstreamPullHeader {
            total_bytes,
            size_proof: resp_ext.size_proof().cloned(),
            rate_per_mb,
            interval_bytes: CHUNK_BYTES,
            origin_attestation: resp_ext
                .origin_attestation
                .clone()
                .or_else(|| decdn_protocol::OriginAttestation::from_response(&resp)),
        };
        let window = deadlines.window;
        let progress_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let floor = progress::ThroughputFloor::new(
            progress::FloorConfig {
                window,
                floor_bps: deadlines.floor_bps,
            },
            Arc::clone(&progress_counter),
            tokio::time::Instant::now(),
        );
        let mut sampler = tokio::time::interval(stall_sample_period(window));
        sampler.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ledger.note_rate(rate_per_mb);
        let pull = UpstreamPull {
            window,
            progress_counter,
            floor,
            sampler,
            conn,
            owner,
            send,
            recv,
            ctx: ctx.clone(),
            ledger,
            hash,
            byte_offset,
            byte_len,
            rate_per_mb,
            meter: StreamMeter::default(),
            expected_wire_bytes,
            max_received_wire,
            cumulative: 0,
            verified: 0,
            paid: 0,
            node_failed: false,
            settled: false,
            ended: false,
            returned_at: None,
            opened_at: tokio::time::Instant::now(),
        };
        Ok((header, pull))
    }
}

/// What one open asks for: the blob, the span, the payer, and how long the
/// open may take.
#[derive(Clone, Copy)]
pub(crate) struct OpenArgs<'a> {
    pub(crate) ctx: &'a PoolContext,
    pub(crate) slash_domain: &'a Eip712Domain,
    /// The operator address the answer's `slash_sig` must recover to.
    pub(crate) expected_signer: Address,
    pub(crate) hash: [u8; 32],
    pub(crate) namespace_id: [u8; 32],
    pub(crate) byte_offset: u64,
    pub(crate) byte_len: u64,
    pub(crate) timestamp_us: u64,
    /// The wall-clock bound on the connect and the exchange.
    pub(crate) open: Duration,
    /// Whether a node that misses the blob fills it before it answers.
    pub(crate) pull_through: bool,
}

/// Open one stream on `source` and verify its answer ([`verify_response`]).
/// A verification fault is tagged [`UnverifiedAnswer`] when `tag_unverified`
/// is set. `measure_rtt` waits after the answer for the connection's path RTT
/// ([`path_rtt`]).
pub(crate) async fn open_answer(
    source: ConnSource<'_>,
    owner: ConnOwner,
    args: OpenArgs<'_>,
    tag_unverified: bool,
    measure_rtt: bool,
) -> anyhow::Result<OpenedStream> {
    let (conn, send, recv, resp, resp_ext) = exchange(source, &args).await?;
    let verified = verify_response(
        &resp,
        &resp_ext,
        args.slash_domain,
        args.expected_signer,
        args.hash,
        args.timestamp_us,
        args.byte_offset,
        args.byte_len,
    );
    let proven_size = match verified {
        Ok(proven) => proven,
        Err(err) if tag_unverified && !crate::is_range_past_end(&err) => {
            return Err(err.context(UnverifiedAnswer));
        }
        Err(err) => return Err(err),
    };
    let answered_at = tokio::time::Instant::now();
    let rtt = if measure_rtt {
        path_rtt(&conn, DIRECT_PATH_GRACE).await
    } else {
        None
    };
    Ok(OpenedStream {
        conn,
        owner,
        send,
        recv,
        resp,
        resp_ext,
        proven_size,
        hash: args.hash,
        byte_offset: args.byte_offset,
        byte_len: args.byte_len,
        pool_id: args.ctx.pool_id,
        signer: args.ctx.client_signer.address(),
        rtt,
        answered_at,
    })
}

/// Open one stream on `target`'s connection in `conns`, dialling one when the
/// target has none. `args.open` bounds the dial, and again the exchange that
/// follows it.
///
/// The stream holds the connection while it lives. A transport fault on the
/// open (a connection reset, a closed connection, an idle timeout, an open
/// that falls under its deadline) unpins the connection from the map, so the
/// next open dials again. Any other failure (a refusal, a bad answer) leaves
/// it pinned.
///
/// A connection that was pinned before this open and is closed under it is a
/// stale pin: the target reaped it idle, or its budget ran out. The open then
/// redials once and opens again. Nothing is paid before the answer, so the
/// retry costs nothing. A connection this open dialled itself that fails is
/// the target's failure, and is not retried.
pub(crate) async fn open_shared(
    conns: &Connections,
    target: &EndpointAddr,
    args: OpenArgs<'_>,
    tag_unverified: bool,
    measure_rtt: bool,
) -> anyhow::Result<OpenedStream> {
    debug_assert_eq!(
        conns.alpn(),
        decdn_protocol::ALPN_CLIENT,
        "a stream opens on a cdn/client/v1 map"
    );
    let lease = lease_conn(conns, target, args.open).await?;
    let pinned = lease.is_pinned();
    let warm = lease.into_warm();
    let err = match open_on_warm(conns, &warm, args, tag_unverified, measure_rtt).await {
        Ok(opened) => return Ok(opened),
        Err(err) => err,
    };
    if !(pinned && warm.is_closed()) {
        return Err(err);
    }
    tracing::debug!(
        peer = %target.id,
        close = ?warm.connection().close_reason(),
        error = %err,
        "pooled connection closed under the open; redialling once"
    );
    let fresh = lease_conn(conns, target, args.open).await?.into_warm();
    open_on_warm(conns, &fresh, args, tag_unverified, measure_rtt)
        .await
        .with_context(|| format!("after a redial; the pinned connection failed: {err:#}"))
}

/// `target`'s connection in `conns`, dialled when it has none, under `open`.
async fn lease_conn(
    conns: &Connections,
    target: &EndpointAddr,
    open: Duration,
) -> anyhow::Result<crate::connection::Lease> {
    tokio::time::timeout(open, conns.get_or_dial(target))
        .await
        .map_err(|_| anyhow::Error::new(PullTimeout { after: open }))?
}

/// Open one stream on `warm`, which the stream holds and unpins from `conns`
/// on a transport fault.
async fn open_on_warm(
    conns: &Connections,
    warm: &Arc<crate::WarmConnection>,
    args: OpenArgs<'_>,
    tag_unverified: bool,
    measure_rtt: bool,
) -> anyhow::Result<OpenedStream> {
    let owner = ConnOwner::Shared {
        warm: Arc::clone(warm),
        map: conns.clone(),
    };
    let opened = open_answer(
        ConnSource::Reuse(warm.connection()),
        owner.clone(),
        args,
        tag_unverified,
        measure_rtt,
    )
    .await;
    if let Err(err) = &opened {
        owner.unpin_on_fault(err).await;
    }
    opened
}

/// Open `hash` on `target` for discovery: one stream on the target's shared
/// connection in `conns`, its answer verified, and the connection's path RTT
/// measured after it. No voucher is sent: the caller adopts the stream
/// ([`OpenedStream::into_pull`]) or cancels it ([`OpenedStream::cancel`]).
///
/// The request carries `ctx`'s pool, binding and capability like any open,
/// and asks for `span` (`(byte_offset, byte_len)`, a `byte_len` of `0` to the
/// end). With `pull_through` unset, a node that misses the blob answers a
/// signed `NotFound` at once and does no upstream work; with it set, the node
/// runs its miss path and admits the payer before it fills.
///
/// The RTT waits up to 500 ms after the answer for hole punching to select a
/// direct path, and keeps the selected path's estimate when none comes. It is
/// the transport's estimate, never the time to the answer, which includes any
/// work the node did before it answered.
///
/// `open` bounds the dial, and again the exchange.
///
/// # Errors
///
/// A transport fault or a timeout ([`PullTimeout`]); a rate-limit shed
/// ([`crate::UpstreamRateLimited`]) or a timestamp refusal
/// ([`crate::UpstreamTimestampRejected`]); an answer that does not verify,
/// tagged [`UnverifiedAnswer`]; or a span past the proven size
/// ([`crate::ResumeOffsetPastEnd`]).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    name = "discovery_open",
    skip_all,
    fields(
        otel.kind = "client",
        peer = %target.id,
        hash = %decdn_protocol::ContentHash::from_bytes(hash),
        pool_id = %ctx.pool_id,
        byte_offset = span.0,
        byte_len = span.1,
        pull_through = pull_through,
    )
)]
pub async fn discovery_open(
    conns: &Connections,
    target: &EndpointAddr,
    expected_signer: Address,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    span: (u64, u64),
    pull_through: bool,
    open: Duration,
) -> anyhow::Result<OpenedStream> {
    let args = OpenArgs {
        ctx,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset: span.0,
        byte_len: span.1,
        timestamp_us: crate::targets::micros_now(),
        open,
        pull_through,
    };
    open_shared(conns, target, args, true, true).await
}

#[cfg(test)]
pub(crate) mod fake_node;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
