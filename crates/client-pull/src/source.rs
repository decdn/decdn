//! The two *sourcing* axes of the gap-driven, range-minimized pull driver
//! (#1608): [`BlobSource`] — a dumb producer of raw interleaved bao bytes for a
//! contiguous range — and [`Funder`] — the injected pool top-up seam.
//!
//! # Why "dumb"
//!
//! A [`BlobSource`] never decodes and never verifies. It opens a pull over the
//! raw bao encoding of one [`decdn_bao_range::AlignedRange`] and yields the wire
//! bytes on demand; the STORE's ingest decoder (rooted at the blob hash `H`)
//! verifies each chunk group exactly once, as the bytes arrive. This keeps a
//! source — `PeerSource` (paid `cdn/client/v1`) or a
//! future `BackendSource` (origin re-encode) — free of bao logic, and matches the
//! "admit verifies once" contract of [`decdn_bao_range::RangedStore`].
//!
//! The reader a source yields is a [`BaoRangeReader`]: an
//! [`iroh_io::AsyncStreamReader`] that also preserves the pull's typed faults via
//! [`crate::sink::StashedFault`], so a stalled or refusing peer is surfaced to the
//! reputation layer rather than collapsed into an anonymous decode error.
//!
//! # The `Funder` seam
//!
//! A mid-fetch top-up goes through the injected [`Funder`] trait so the driver
//! and `PeerSource` stay chain-handle-agnostic: each deployment supplies its own
//! implementation (the CLI's `CliFunder`, the node's `NodeFunder`) rather than
//! the driver naming a contract instance directly. The reactive-top-up budget is
//! deployment-specific and travels on the funder ([`Funder::max_topups`] — CLI 3,
//! node 1).

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use decdn_bao_range::AlignedRange;
use decdn_incentive::DepositOutcome;
use iroh::{Endpoint, EndpointAddr};

use crate::connection::WarmConnection;
use crate::sink::{PullReader, StashedFault};
use crate::{
    DialObserver, PoolContext, PoolLedger, PullDeadlines, UpstreamPull, UpstreamPullHeader,
    VoucherProgress,
};

/// A boxed, `Send` future returned by the async trait methods in this module —
/// the same boxed-future async-trait shape [`decdn_bao_range::RangedStore`] and
/// `cache::Origin` use, so a [`Funder`] can be held as `&dyn Funder`.
pub type SourceFuture<'a, T> =
    core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// A reader over the raw interleaved bao encoding of one range.
///
/// The store's ingest decoder pulls bytes on demand through
/// [`iroh_io::AsyncStreamReader`]; a typed peer fault (stall, refusal, voucher
/// rejection) is preserved through [`StashedFault`] so it beats the decoder's
/// generic "bytes stopped arriving". Blanket-implemented, so any reader that is
/// both is a `BaoRangeReader` with no extra code.
pub trait BaoRangeReader: iroh_io::AsyncStreamReader + StashedFault + Send {}

impl<T> BaoRangeReader for T where T: iroh_io::AsyncStreamReader + StashedFault + Send {}

/// A source of raw, unverified interleaved bao bytes for contiguous ranges of one
/// blob. Dumb: it produces bytes (and, for paid sources, pays) — it never decodes
/// or verifies.
///
/// One `BlobSource` serves one gap-driven fetch. The driver calls [`open`] once
/// per gap (a contiguous [`AlignedRange`] from
/// [`missing_ranges`](decdn_bao_range::RangedStore::missing_ranges)), streams the
/// reader into the store's ingest decoder, then calls [`finish`] to drain the
/// pull to completion and recover the acked voucher watermark to persist.
///
/// [`open`]: BlobSource::open
/// [`finish`]: BlobSource::finish
pub trait BlobSource: Send + Sync {
    /// The reader this source yields — raw bao bytes plus preserved typed faults.
    type Reader: BaoRangeReader;

    /// Open a pull over the raw bao encoding of `range` of the blob `hash`.
    ///
    /// Returns the upstream [`UpstreamPullHeader`] (the committed `total_bytes`
    /// and the quoted `rate_per_mb` / `interval_bytes` the driver prices the next
    /// voucher from) alongside the live [`Self::Reader`]. An unpaid source reports
    /// a zero rate/interval; `total_bytes` is always authoritative.
    ///
    /// # Errors
    ///
    /// The handshake/response faults of the underlying pull (connect/transport, a
    /// refused or zero-rate response, an over-ceiling rate or size, a resume
    /// offset past the blob end) — the same set `open_progressive_pull` raises.
    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)>;

    /// Consume a fully-decoded gap's reader: drain the pull to its stream end,
    /// enforce wire-byte completeness, and return the acked voucher watermark for
    /// the driver to persist. An unpaid source returns
    /// [`VoucherProgress::default`].
    ///
    /// # Errors
    ///
    /// A short delivery (fewer wire bytes than promised before the stream end) or
    /// a transport/protocol error while draining — the faults
    /// [`crate::UpstreamPull::finish`] raises.
    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress>;

    /// The buyer's received-byte ceiling (#1895): the driver aborts with
    /// [`crate::BlobTooLarge`] once the content BLAKE3-verified into the store
    /// crosses this, enforcing the size cap on the bytes that ACTUALLY arrive rather
    /// than on the peer's unverified signed `total_bytes`. `0` = unlimited — the
    /// default, and correct for an own-origin source whose engine store already caps
    /// on stored bytes.
    fn max_blob_size_bytes(&self) -> u64 {
        0
    }
}

/// The store/sink capability the gap-driven [`crate::drive`] needs beyond
/// [`decdn_bao_range::RangedStore`]'s queries: ingest one gap's raw bao. The
/// client backend writes `.partial`/`.obao4`; a node backend (B2) admits to
/// the cache and tees to its downstream client. Kept a generic method (not
/// `dyn`) so an impl can stream any [`BaoRangeReader`]; `drive` is already
/// fully generic.
///
/// The returned future is intentionally NOT `Send`-bounded, unlike
/// [`SourceFuture`]. [`iroh_io::AsyncStreamReader`]'s methods are
/// return-position-impl-trait-in-trait with no `Send` bound on the trait
/// itself, so a method generic over `R: BaoRangeReader` (as this one must be,
/// to stream ANY reader an [`IngestStore`] impl is handed) can never prove its
/// decode-loop future is `Send` for an arbitrary `R` — only a concrete,
/// non-generic instantiation could. `drive`/`fill_gap` only ever `.await` this
/// future in place (never spawn it across a task boundary), so dropping `Send`
/// here is behavior-preserving.
pub trait IngestStore: decdn_bao_range::RangedStore {
    /// Decode-and-admit the raw bao bytes in `reader` as `range`'s content,
    /// verifying against the store's rooted hash as it streams. Returns the
    /// drained `reader` (its typed fault, if any, surfaces via
    /// [`StashedFault`] on the caller's copy) so the
    /// source can [`finish`](BlobSource::finish) the pull.
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<R>> + 'a>>
    where
        R: BaoRangeReader + 'a;

    /// Persist the store's current in-memory present-range snapshot to its
    /// durable record. The single-writer flush point (spec §5.5): several
    /// `ingest_stream` calls can run concurrently on one store (the
    /// multi-source scheduler), so no checkpoint writes the record — callers
    /// flush it explicitly instead. `drive` calls this once after its gap loop,
    /// before `finalize`, so the single-source path keeps its resume durability
    /// without per-checkpoint fsyncs.
    ///
    /// The snapshot is taken when this is called; the returned future writes
    /// it. An implementation that touches the disk does so off the runtime
    /// workers, because the paid pulls that share the runtime pay from their
    /// decode loops.
    ///
    /// # Errors
    ///
    /// Any I/O failure persisting the record.
    fn flush_present_record(&self) -> SourceFuture<'_, ()>;
}

/// The injected pool top-up seam. Wraps the deployment's funding path — the
/// CLI's `CliFunder` and the node's `NodeFunder`, both driving
/// `PoolOpener::top_up_pool_by` over their own chain handle — so the driver and
/// [`BlobSource`] never name a contract instance.
///
/// A top-up is only ever attempted after a [`crate::pacer::Pacer`] returns
/// [`crate::pacer::PaceDecision::TopUp`] — i.e. a genuine, ledger-corroborated
/// mid-fetch exhaustion — so [`max_topups`](Funder::max_topups) is the sole bound
/// on how many times one fetch will escrow more USDC.
pub trait Funder: Send + Sync {
    /// How many reactive top-ups this deployment allows for one fetch. CLI = 3
    /// ([`crate::MAX_TOPUP_ATTEMPTS`]), node = 1. The driver copies this into the
    /// pacer's [`PaceState::max_topups`](crate::pacer::PaceState::max_topups).
    fn max_topups(&self) -> u32;

    /// Add `additional` micro-USDC to the channel on-chain and credit the local record.
    ///
    /// Returns the [`DepositOutcome`] of crediting the row: `Added(new_total)` on
    /// the clean path — the driver updates its channel deposit from it — or the
    /// escrowed-but-untracked variants (`UnknownPool` / `PoolMismatch`),
    /// which the driver treats as terminal (the USDC is on-chain but the local
    /// record is gone; reconcile against the tx).
    ///
    /// # Errors
    ///
    /// If the on-chain `topUp` fails to submit, reverts, or its receipt is not
    /// obtained — the funds did not move. Also if a mined `topUp` cannot be
    /// credited locally, in which case the funds **are** escrowed and the error
    /// names the tx (see `client_pull::buyer_pool::escrowed_but_untracked`).
    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome>;
}

/// Current unix time in microseconds (the requester-echoed
/// [`decdn_protocol::client::StreamRequest::timestamp_us`]). Mirrors the CLI's
/// `micros_now` (`crates/cli/src/commands/fetch.rs`) and the node's `now_micros`
/// (`crates/node/src/node_origin/mod.rs`) — each caller of `open_progressive_pull`
/// owns its own copy rather than sharing one across crate boundaries, and
/// `PeerSource` needs the same one-liner here.
fn micros_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX)
}

/// The paid `BlobSource` (#1608): wraps
/// [`crate::open_progressive_pull`] behind [`BlobSource`], scoping every
/// [`open`](BlobSource::open) to exactly the requested [`AlignedRange`] via the
/// `byte_len`-bounded pull: the wire `StreamRequest` carries the field and the
/// OPEN-side plumbing scopes each pull to the requested range.
///
/// One `PeerSource` serves one gap-driven fetch against one upstream peer — it
/// holds the pull context (`endpoint`, `target`, `ctx`, `ledger`, …) but not a
/// [`Funder`]: reactive top-up is the driver's concern (it holds the `Funder`
/// separately and calls it directly), not the source's.
///
/// Borrows its `endpoint`/`slash_domain` (the driver, which owns these for the
/// whole fetch, outlives every `open`/`finish` call), but holds the channel
/// context behind a SHARED `Arc<Mutex<PoolContext>>` rather than a `&'a`
/// borrow. That shared handle is what resolves the #1608 borrow conflict: the
/// driver mutates the context (crediting a mid-fetch top-up's new deposit)
/// while this source also reads it to open each pull. A `&'a PoolContext`
/// borrow would freeze it for the whole fetch and forbid the driver's `&mut`;
/// the `Arc<Mutex<..>>` lets both see one state. `open` locks it only to CLONE
/// the context out (single-writer per fetch — the driver never opens a pull
/// concurrently with a top-up), then drops the guard before awaiting, so no
/// lock is ever held across an `.await`. Concurrent opens (a range set driven
/// several gaps at a time) each take their own snapshot; a snapshot that misses a
/// top-up landing at the same moment only under-states the deposit, and the
/// driver's post-top-up settle wait already covers a refusal on that.
///
/// By default every open dials its own connection. A source built
/// [`with_warm_connection`](Self::with_warm_connection) dials once and opens
/// every pull as a new stream on that connection instead (#2119).
pub struct PeerSource<'a> {
    endpoint: &'a Endpoint,
    target: EndpointAddr,
    ctx: Arc<Mutex<PoolContext>>,
    ledger: Arc<PoolLedger>,
    slash_domain: &'a Eip712Domain,
    expected_signer: Address,
    namespace_id: [u8; 32],
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    /// Notified with a weak handle to every connection this source dials, set by a
    /// caller that must observe those connections reach drained before it drops the
    /// runtime their QUIC drivers live on (`decdn-node`'s per-serve pull legs).
    ///
    /// It sits HERE rather than in the caller because a dial that fails its
    /// handshake never yields a reader — and those are exactly the opens whose
    /// connection is left live on the pull runtime, so a caller-side wrapper around
    /// the returned reader cannot see them.
    on_connect: Option<&'a DialObserver<'a>>,
    /// The one connection every open reuses, when the source keeps one warm.
    /// `None` dials per open. The inner `None` is a warm source that has not
    /// dialled yet, or whose connection closed and is dialled again on the next
    /// open.
    warm: Option<tokio::sync::Mutex<Option<Arc<WarmConnection>>>>,
}

impl std::fmt::Debug for PeerSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `PoolContext` (a signing key) and `Eip712Domain` are not `Debug`,
        // so this prints only the non-sensitive routing/policy fields.
        f.debug_struct("PeerSource")
            .field("target", &self.target)
            .field("expected_signer", &self.expected_signer)
            .field("namespace_id", &self.namespace_id)
            .field("max_blob_size_bytes", &self.max_blob_size_bytes)
            .field("max_rate_per_mb", &self.max_rate_per_mb)
            .field("deadlines", &self.deadlines)
            .field("warm", &self.warm.is_some())
            .finish_non_exhaustive()
    }
}

impl<'a> PeerSource<'a> {
    /// Build a source for one gap-driven fetch against `target`, paid out of
    /// `ledger` over `ctx`'s channel. `namespace_id`, `max_rate_per_mb`, and
    /// `deadlines` are the same buyer-side policy knobs
    /// [`crate::open_progressive_pull`] takes directly — see its docs.
    /// `max_blob_size_bytes` is the received-byte ceiling (#1895) the driver reads
    /// back via [`BlobSource::max_blob_size_bytes`] to abort a fill that crosses it;
    /// `0` = unlimited.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        endpoint: &'a Endpoint,
        target: EndpointAddr,
        ctx: Arc<Mutex<PoolContext>>,
        ledger: Arc<PoolLedger>,
        slash_domain: &'a Eip712Domain,
        expected_signer: Address,
        namespace_id: [u8; 32],
        max_blob_size_bytes: u64,
        max_rate_per_mb: u64,
        deadlines: PullDeadlines,
    ) -> Self {
        Self {
            endpoint,
            target,
            ctx,
            ledger,
            slash_domain,
            expected_signer,
            namespace_id,
            max_blob_size_bytes,
            max_rate_per_mb,
            deadlines,
            on_connect: None,
            warm: None,
        }
    }

    /// Observe every connection this source dials.
    ///
    /// For a caller that drops the runtime the pull ran on: see [`DialObserver`].
    /// A source without one behaves identically and costs nothing.
    #[must_use]
    pub const fn with_dial_observer(mut self, on_connect: &'a DialObserver<'a>) -> Self {
        self.on_connect = Some(on_connect);
        self
    }

    /// Keep one connection to the target warm and open every pull on it.
    ///
    /// A fetch that opens many small legs against one provider (a range-dedup
    /// entry's boundary groups) otherwise pays a dial, a QUIC handshake and a
    /// teardown per leg (#2119). Each pull is still its own stream with its own
    /// signed request and response, so nothing about payment changes. A
    /// connection that closes (an idle timeout while the fetch waits on a
    /// sibling, a peer close) is dialled again on the next open, and an open that
    /// fails because the connection closed under it is retried once on a fresh
    /// one; an open sends no voucher, so the retry pays nothing. The connection
    /// closes when the source drops.
    ///
    /// Dials through a warm connection are not reported to a
    /// [`with_dial_observer`](Self::with_dial_observer) observer; a caller that
    /// needs one keeps the per-open dial.
    #[must_use]
    pub fn with_warm_connection(mut self) -> Self {
        self.warm = Some(tokio::sync::Mutex::new(None));
        self
    }

    /// Open the whole blob (`byte_len == 0`, "to end") from offset 0.
    ///
    /// For a caller that must read the signed `total_bytes` before it can size
    /// the store a drive fills. Hand the result to a [`PrimedSource`] primed at
    /// `align_range(0, 0, total_bytes)` and the drive's first leg adopts this
    /// pull rather than opening the same range a second time (#2063).
    ///
    /// # Errors
    ///
    /// The same faults as [`BlobSource::open`].
    pub fn open_whole(&self, hash: [u8; 32]) -> SourceFuture<'_, (UpstreamPullHeader, PullReader)> {
        Box::pin(async move {
            let pull = self.open_pull(hash, 0, 0).await?;
            Ok((pull.0, PullReader::new(pull.1)))
        })
    }

    /// Open `[byte_offset, +byte_len)` of `hash` (`byte_len == 0` = to end), on
    /// the warm connection when the source keeps one.
    async fn open_pull(
        &self,
        hash: [u8; 32],
        byte_offset: u64,
        byte_len: u64,
    ) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
        // Snapshot the shared context, then drop the guard before the await —
        // `open_progressive_pull` needs `&PoolContext` for its whole call, and no
        // std `Mutex` guard may be held across an `.await`.
        let ctx = {
            self.ctx
                .lock()
                .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                .clone()
        };
        let Some(slot) = &self.warm else {
            return crate::open_progressive_pull(
                self.endpoint,
                self.target.clone(),
                &ctx,
                Arc::clone(&self.ledger),
                self.slash_domain,
                self.expected_signer,
                hash,
                self.namespace_id,
                byte_offset,
                micros_now(),
                self.max_blob_size_bytes,
                self.max_rate_per_mb,
                self.deadlines,
                byte_len,
                self.on_connect,
            )
            .await;
        };
        let conn = self.warm_connection(slot).await?;
        match self.open_on(&conn, &ctx, hash, byte_offset, byte_len).await {
            Err(_) if conn.is_closed() => {
                let conn = self.warm_connection(slot).await?;
                self.open_on(&conn, &ctx, hash, byte_offset, byte_len).await
            }
            opened => opened,
        }
    }

    /// The warm connection, dialled when there is none yet or the last one
    /// closed. The slot's lock is held across the dial, so concurrent opens share
    /// one dial rather than racing several.
    async fn warm_connection(
        &self,
        slot: &tokio::sync::Mutex<Option<Arc<WarmConnection>>>,
    ) -> anyhow::Result<Arc<WarmConnection>> {
        let mut held = slot.lock().await;
        if let Some(conn) = held.as_ref().filter(|c| !c.is_closed()) {
            return Ok(Arc::clone(conn));
        }
        let conn = Arc::new(
            WarmConnection::connect(self.endpoint, self.target.clone(), self.deadlines.open())
                .await?,
        );
        *held = Some(Arc::clone(&conn));
        Ok(conn)
    }

    /// Open one pull as a new stream on `conn`.
    async fn open_on(
        &self,
        conn: &WarmConnection,
        ctx: &PoolContext,
        hash: [u8; 32],
        byte_offset: u64,
        byte_len: u64,
    ) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
        crate::open_progressive_pull_on(
            conn,
            ctx,
            Arc::clone(&self.ledger),
            self.slash_domain,
            self.expected_signer,
            hash,
            self.namespace_id,
            byte_offset,
            micros_now(),
            self.max_blob_size_bytes,
            self.max_rate_per_mb,
            self.deadlines,
            byte_len,
            None,
        )
        .await
    }
}

impl BlobSource for PeerSource<'_> {
    type Reader = PullReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(async move {
            let (header, pull) = self
                .open_pull(hash, range.fetch_start(), range.fetch_len())
                .await?;
            Ok((header, PullReader::new(pull)))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move { reader.into_inner().finish().await })
    }

    fn max_blob_size_bytes(&self) -> u64 {
        self.max_blob_size_bytes
    }
}

/// How long a primed pull may wait for its adopting open. A pull opened ahead of
/// its drive is only worth adopting straight away: an idle one is a stream the
/// peer is holding bytes on, and a peer that sees no voucher for long enough
/// drops it. A primed pull older than this is dropped and the open goes to the
/// wrapped source.
const PRIMED_MAX_IDLE: Duration = Duration::from_secs(2);

/// A pull opened ahead of the drive, waiting for the open it answers.
struct Primed<R> {
    hash: [u8; 32],
    range: AlignedRange,
    header: UpstreamPullHeader,
    reader: R,
    at: tokio::time::Instant,
}

/// A [`BlobSource`] that hands one already-open pull to the drive (#2063).
///
/// A caller that opens a pull before the drive (to read the signed
/// `total_bytes` the store is sized from, or to learn whether the peer serves
/// at all) would otherwise drop it and let the drive open the same range again.
/// The node treats every open as real: it signs, claims a fill, and starts an
/// origin draw, so the thrown-away open costs a duplicate draw and delays the
/// real one. [`prime`](Self::prime) parks the live pull here instead, and the
/// drive's first [`open`](BlobSource::open) of exactly that hash and range takes
/// it. Any other open, and an open that finds the primed pull older than two
/// seconds, goes to the wrapped source.
///
/// Adoption is by exact [`AlignedRange`] equality, never by containment: a
/// pull's [`finish`](BlobSource::finish) drains and pays to its stream end, so a
/// longer pull in place of a shorter leg would pay for bytes the leg never asked
/// for. Call [`clear`](Self::clear) once the drive returns, so a pull no open
/// took is closed at once rather than left idle.
pub struct PrimedSource<S: BlobSource> {
    inner: S,
    primed: Mutex<Option<Primed<S::Reader>>>,
}

impl<S: BlobSource> std::fmt::Debug for PrimedSource<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let primed = self.primed.lock().is_ok_and(|p| p.is_some());
        f.debug_struct("PrimedSource")
            .field("primed", &primed)
            .finish_non_exhaustive()
    }
}

impl<S: BlobSource> PrimedSource<S> {
    /// Wrap `inner` with nothing primed.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self {
            inner,
            primed: Mutex::new(None),
        }
    }

    /// Park a live pull of `range` of `hash` for the next open of exactly that
    /// range. Replaces (and so closes) any pull still parked.
    pub fn prime(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
        header: UpstreamPullHeader,
        reader: S::Reader,
    ) {
        let parked = Primed {
            hash,
            range,
            header,
            reader,
            at: tokio::time::Instant::now(),
        };
        if let Ok(mut slot) = self.primed.lock() {
            *slot = Some(parked);
        }
    }

    /// Close a parked pull that no open took.
    pub fn clear(&self) {
        let parked = self.primed.lock().ok().and_then(|mut slot| slot.take());
        drop(parked);
    }

    /// The wrapped source.
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Take the parked pull if it answers an open of `range` of `hash`. A stale
    /// parked pull is dropped here whatever the open asks for; a fresh one for
    /// another range stays parked.
    fn take(
        &self,
        hash: [u8; 32],
        range: &AlignedRange,
    ) -> Option<(UpstreamPullHeader, S::Reader)> {
        let mut slot = self.primed.lock().ok()?;
        let parked = slot.take()?;
        if parked.at.elapsed() > PRIMED_MAX_IDLE {
            return None;
        }
        if parked.hash == hash && parked.range == *range {
            return Some((parked.header, parked.reader));
        }
        *slot = Some(parked);
        None
    }
}

impl<S: BlobSource> BlobSource for PrimedSource<S> {
    type Reader = S::Reader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        match self.take(hash, &range) {
            Some(adopted) => Box::pin(async move { Ok(adopted) }),
            None => self.inner.open(hash, range),
        }
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.finish(reader)
    }

    fn max_blob_size_bytes(&self) -> u64 {
        self.inner.max_blob_size_bytes()
    }
}

// ---------------------------------------------------------------------------
// Test doubles: a scripted BlobSource + a fake Funder for the driver tests.
// Feature-gated so a production caller cannot name them, but compiled outside
// `cfg(test)` under `test-util`, so they must stay anti-panic clean.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-util"))]
mod doubles {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use alloy::primitives::U256;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, encode_verified_range};
    use decdn_incentive::DepositOutcome;

    use super::{SourceFuture, StashedFault};
    use crate::{UpstreamPullHeader, VoucherProgress};

    /// Fixed quote a [`ScriptedSource`] reports so the driver can price vouchers
    /// deterministically in tests.
    const SCRIPTED_RATE_PER_MB: u64 = 1;
    const SCRIPTED_INTERVAL_BYTES: u64 = 1024 * 1024;

    /// Builds a typed fault to park mid-range. Boxed so a source can be re-opened
    /// (an `anyhow::Error` is not `Clone`, so it is regenerated per open).
    type FaultFn = Arc<dyn Fn() -> anyhow::Error + Send + Sync>;

    /// A scripted [`BlobSource`](super::BlobSource) that yields the real bao wire
    /// for any requested range of a fixed blob, and can truncate a range's wire
    /// and park a typed fault to simulate a mid-range peer failure.
    #[derive(Clone)]
    pub struct ScriptedSource {
        root: [u8; 32],
        blob: Bytes,
        outboard: Bytes,
        fault: Option<(usize, FaultFn)>,
        /// Every range this source was `open`ed for, in call order, as
        /// `(fetch_start, fetch_len)`. Shared behind an `Arc<Mutex<..>>` so a
        /// clone handed to the driver records into the same log the test
        /// inspects — the ledger the #1608 money assertion reads to prove the
        /// driver pulled ONLY the gaps of `missing_ranges` and never a held
        /// range.
        opened: Arc<Mutex<Vec<(u64, u64)>>>,
        /// Total WIRE bytes this source's readers actually delivered (summed
        /// across every reader). Unlike [`opened`](Self::opened) — which records
        /// the requested `fetch_len` at `open` time, before a byte streams —
        /// this counts bytes that truly left the source, so it is the honest
        /// proxy for "bytes fetched and paid for". A leg cancelled mid-stream
        /// stops incrementing this the instant its reader is dropped, which is
        /// exactly what lets the multi-source no-double-pay assertion see that a
        /// stolen tail was fetched by ONE source, not two.
        delivered: Arc<AtomicU64>,
        /// One-time stall injected on the FIRST read of every reader this source
        /// yields. Models a slow-to-start peer; a fast peer (no stall) then
        /// reliably finishes its own segment and steals the slow peer's tail,
        /// forcing the steal path deterministically without wall-clock racing on
        /// per-byte timing.
        first_read_stall: Option<Duration>,
        /// Delay injected inside `finish`, AFTER the range's bytes are fully
        /// delivered but BEFORE `fill_gap` returns. Holds the completed range in
        /// the scheduler's `in_flight` (delivered, not yet cleared) so a peer's
        /// steal of an already-present range is deterministic — the exact
        /// completed-but-uncleared window the present-bytes backstop must close.
        finish_stall: Option<Duration>,
        /// Wedge every reader after it has delivered `n` wire bytes: it sleeps
        /// for the given duration instead of yielding the next chunk. Models a
        /// source that opens, delivers a prefix, then stops making progress
        /// WITHOUT erroring — the only shape that reaches the scheduler's stall
        /// watchdog. A source that returns `Err` takes the fault path instead,
        /// so `with_fault_after` cannot stand in for this.
        stall_after: Option<(u64, Duration)>,
        /// Optional ledger to advance on a clean `finish`, modelling payment: a
        /// real pull pays vouchers for the WIRE bytes it drains, and the driver's
        /// completion is PAID-frontier based (`content_paid_frontier`), so a double
        /// that never advanced `committed` would leave the driver's paid frontier at
        /// zero and it would never complete. `None` keeps the pre-payment "unpaid
        /// double" behaviour for tests that do not drive `fill_gap` to completion.
        ledger: Option<Arc<crate::PoolLedger>>,
        /// Legs opened and not yet finished. A faulted leg is never finished, so
        /// this reads true only on a run with no faults.
        in_flight: Arc<AtomicU64>,
        /// The most legs [`in_flight`](Self::in_flight) ever held at once.
        peak_in_flight: Arc<AtomicU64>,
    }

    impl std::fmt::Debug for ScriptedSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ScriptedSource")
                .field("root", &blake3::Hash::from_bytes(self.root))
                .field("total_bytes", &self.total_bytes())
                .field("fault_after", &self.fault.as_ref().map(|(after, _)| *after))
                .field("opened_ranges", &self.opened_ranges())
                .finish_non_exhaustive()
        }
    }

    impl ScriptedSource {
        /// Build a source over `blob`, computing its root and outboard once.
        ///
        /// # Errors
        ///
        /// If the blob length does not fit the bao tree math (only on absurdly
        /// large inputs a test never uses).
        pub fn new(blob: impl Into<Bytes>) -> anyhow::Result<Self> {
            let blob = blob.into();
            let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
            Ok(Self {
                root: *ob.root.as_bytes(),
                blob,
                outboard: ob.data.into(),
                fault: None,
                opened: Arc::new(Mutex::new(Vec::new())),
                delivered: Arc::new(AtomicU64::new(0)),
                first_read_stall: None,
                finish_stall: None,
                stall_after: None,
                ledger: None,
                in_flight: Arc::new(AtomicU64::new(0)),
                peak_in_flight: Arc::new(AtomicU64::new(0)),
            })
        }

        /// The most legs this source ever had open and unfinished at once. A
        /// driver that runs its gaps one at a time reads `1`.
        #[must_use]
        pub fn peak_in_flight(&self) -> u64 {
            self.peak_in_flight.load(Ordering::SeqCst)
        }

        /// Delay every `finish` by `stall` after its range is fully delivered,
        /// holding the completed range in the scheduler's `in_flight` so a peer's
        /// steal of an already-present range fires deterministically (see
        /// `finish_stall`).
        #[must_use]
        pub const fn slow_finish(mut self, stall: Duration) -> Self {
            self.finish_stall = Some(stall);
            self
        }

        /// Wedge every reader once it has delivered `after_bytes`: the next read
        /// sleeps for `stall` rather than returning bytes, so the source stops
        /// making verified progress without ever erroring (see
        /// [`stall_after`](Self::stall_after)). Pair a `stall` well above the
        /// scheduler's `unit_deadline` with a checkpoint-crossing `after_bytes`
        /// to trip the stall watchdog deterministically.
        #[must_use]
        pub const fn stall_after(mut self, after_bytes: u64, stall: Duration) -> Self {
            self.stall_after = Some((after_bytes, stall));
            self
        }

        /// Inject a one-time `stall` on the first read of every reader this
        /// source yields, so a competing fast source finishes first and steals
        /// this one's tail — the deterministic trigger the no-double-pay test
        /// needs.
        #[must_use]
        pub const fn slow_to_start(mut self, stall: Duration) -> Self {
            self.first_read_stall = Some(stall);
            self
        }

        /// Total WIRE bytes actually delivered across every reader (see
        /// `delivered`). The honest "fetched and paid" proxy
        /// the no-double-pay assertion reads.
        #[must_use]
        pub fn delivered_bytes(&self) -> u64 {
            self.delivered.load(Ordering::SeqCst)
        }

        /// Model payment: on every clean `finish`, advance `ledger`'s committed
        /// watermark by the leg's drained WIRE bytes (at `SCRIPTED_RATE_PER_MB`),
        /// exactly as a real pull's acked vouchers do. Required by any test that
        /// drives a gap to completion, since the driver's `Done` is paid-frontier
        /// based. Pass the SAME `Arc<PoolLedger>` the driver is handed.
        #[must_use]
        pub fn paying(mut self, ledger: Arc<crate::PoolLedger>) -> Self {
            self.ledger = Some(ledger);
            self
        }

        /// The blob's BLAKE3 root — the `hash` the driver opens against.
        #[must_use]
        pub const fn root(&self) -> [u8; 32] {
            self.root
        }

        /// Total blob length.
        #[must_use]
        pub const fn total_bytes(&self) -> u64 {
            self.blob.len() as u64
        }

        /// Every range `open` was called for, in call order, as
        /// `(fetch_start, fetch_len)`. The #1608 money assertion checks this
        /// equals exactly the contiguous gaps of `missing_ranges` — no held
        /// range is ever opened, so no held byte is ever re-pulled or re-paid.
        #[must_use]
        pub fn opened_ranges(&self) -> Vec<(u64, u64)> {
            self.opened.lock().map(|o| o.clone()).unwrap_or_default()
        }

        /// Total content bytes opened across every `open` call (the sum of each
        /// opened range's `fetch_len`). Equals the gap bytes, NOT the whole blob,
        /// when the driver skips held ranges.
        #[must_use]
        pub fn opened_bytes(&self) -> u64 {
            self.opened
                .lock()
                .map_or(0, |o| o.iter().map(|(_, len)| *len).sum())
        }

        /// After `wire_bytes` of a range's wire, truncate it and park the fault
        /// `make` produces — the exact shape a stalled/refusing peer leaves.
        #[must_use]
        pub fn with_fault_after(
            mut self,
            wire_bytes: usize,
            make: impl Fn() -> anyhow::Error + Send + Sync + 'static,
        ) -> Self {
            self.fault = Some((wire_bytes, Arc::new(make)));
            self
        }

        /// The header-less bao wire for `range` (content plus interleaved proof,
        /// the same bytes `UpstreamPull::next_chunk` yields).
        fn wire_for(&self, range: &AlignedRange) -> anyhow::Result<Bytes> {
            let s = usize::try_from(range.fetch_start())?;
            let e = usize::try_from(range.fetch_end())?;
            let data = self
                .blob
                .get(s..e)
                .ok_or_else(|| anyhow::anyhow!("scripted range out of bounds"))?;
            let combined = encode_verified_range(self.root, range, data, self.outboard.clone())?;
            // Strip the 8-byte little-endian size header: the wire the pull yields
            // is header-less (the node frames the tree itself).
            let wire = combined
                .get(8..)
                .ok_or_else(|| anyhow::anyhow!("combined wire shorter than its 8-byte header"))?;
            Ok(Bytes::copy_from_slice(wire))
        }
    }

    impl super::BlobSource for ScriptedSource {
        type Reader = ScriptedReader;

        fn open(
            &self,
            hash: [u8; 32],
            range: AlignedRange,
        ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
            Box::pin(async move {
                if hash != self.root {
                    anyhow::bail!("scripted source opened for a foreign hash");
                }
                if let Ok(mut log) = self.opened.lock() {
                    log.push((range.fetch_start(), range.fetch_len()));
                }
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
                let mut wire = self.wire_for(&range)?;
                let mut fault = None;
                if let Some((after, make)) = &self.fault
                    && *after < wire.len()
                {
                    wire = wire.slice(..*after);
                    fault = Some(make());
                }
                let header = UpstreamPullHeader {
                    total_bytes: self.total_bytes(),
                    rate_per_mb: SCRIPTED_RATE_PER_MB,
                    interval_bytes: SCRIPTED_INTERVAL_BYTES,
                    // No real network round trip in this scripted double.
                    ttfb_ms: 0.0,
                };
                // Wire bytes this leg will drain (post-fault-truncation). `finish`
                // is reached only on a CLEAN drain (the driver skips it on an
                // `ingest_stream` fault), so on the paying path this is the full
                // range's wire — the exact delta an accepted voucher would cover.
                let wire_len = wire.len() as u64;
                Ok((
                    header,
                    ScriptedReader {
                        wire,
                        fault,
                        wire_len,
                        delivered: Arc::clone(&self.delivered),
                        first_read_stall: self.first_read_stall,
                        stall_after: self.stall_after,
                        read_so_far: 0,
                    },
                ))
            })
        }

        fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            Box::pin(async move {
                // Hold the completed-but-unpaid range open (delivered, `fill_gap`
                // not yet returned) so a peer can steal it while it is present.
                if let Some(stall) = self.finish_stall {
                    tokio::time::sleep(stall).await;
                }
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                let Some(ledger) = &self.ledger else {
                    // Unpaid double: no channel, nothing to drain, no watermark.
                    return Ok(VoucherProgress::default());
                };
                // Model the committed voucher for this leg's wire: a successful
                // issue commits optimistically (implicit acceptance, ADR 005),
                // advancing `committed.bytes` by the drained WIRE bytes so the
                // driver's paid frontier tracks payment.
                if reader.wire_len > 0 {
                    ledger
                        .issue(
                            reader.wire_len,
                            SCRIPTED_RATE_PER_MB,
                            crate::EpochAction::Keep,
                            |_next, _chain| async { Ok(()) },
                        )
                        .await?;
                }
                Ok(VoucherProgress::from_cumulative(
                    ledger.committed(),
                    U256::ZERO,
                ))
            })
        }
    }

    /// The reader a [`ScriptedSource`] yields: a fixed wire buffer, optionally
    /// holding a parked typed fault. Mirrors the production `PullReader`'s two
    /// behaviours the decode loop depends on — feed bytes, then reveal the fault.
    #[derive(Debug)]
    pub struct ScriptedReader {
        wire: Bytes,
        fault: Option<anyhow::Error>,
        /// The wire byte count this reader was handed (before consumption), used by
        /// `ScriptedSource`'s [`BlobSource::finish`](crate::BlobSource::finish) to advance a paying ledger by
        /// this leg's spend.
        wire_len: u64,
        /// Shared with the parent [`ScriptedSource`]: bumped by the bytes each
        /// `read_bytes` actually yields, so a mid-stream drop stops counting the
        /// instant it happens.
        delivered: Arc<AtomicU64>,
        /// A one-time stall consumed on the first `read_bytes` (see
        /// [`ScriptedSource::slow_to_start`]); `None` after it fires once.
        first_read_stall: Option<Duration>,
        /// Wedge this reader once it has delivered the byte threshold (see
        /// [`ScriptedSource::stall_after`]); `None` after it fires once.
        stall_after: Option<(u64, Duration)>,
        /// Wire bytes this reader has yielded so far, against `stall_after`'s
        /// threshold.
        read_so_far: u64,
    }

    impl iroh_io::AsyncStreamReader for ScriptedReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
            // A slow-to-start peer: sleep once before the first byte so a fast
            // peer reliably wins the race, finishes its own segment, and steals
            // this reader's tail — the deterministic steal trigger. Cancellation
            // drops this future while it sleeps, delivering nothing on this leg.
            if let Some(stall) = self.first_read_stall.take() {
                tokio::time::sleep(stall).await;
            }
            // A source that delivered a prefix and then wedged: it holds the
            // connection open, returns no error, and simply stops. Only the
            // progress-relative watchdog can end this.
            if let Some((after, stall)) = self.stall_after
                && self.read_so_far >= after
            {
                self.stall_after = None;
                tokio::time::sleep(stall).await;
            }
            // Model a real network read's yield point. A synchronous in-memory
            // reader never pends, so under the cooperative single-thread runtime
            // the first-polled multi-source worker would drain every segment
            // before a peer worker is ever polled — starving the fan-out. One
            // yield per read lets concurrent workers interleave, exactly as I/O
            // waits would. Behavior-neutral for the single-source driver tests
            // (a yield only reschedules the same task).
            tokio::task::yield_now().await;
            let take = self.wire.len().min(len);
            let chunk = self.wire.split_to(take);
            self.delivered
                .fetch_add(chunk.len() as u64, Ordering::SeqCst);
            self.read_so_far = self.read_so_far.saturating_add(chunk.len() as u64);
            Ok(chunk)
        }

        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            if self.wire.len() < L {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "scripted reader exhausted before a fixed-size bao read",
                ));
            }
            let got = self.wire.split_to(L);
            let mut out = [0u8; L];
            out.copy_from_slice(&got);
            Ok(out)
        }
    }

    impl StashedFault for ScriptedReader {
        fn take_fault(&mut self) -> Option<anyhow::Error> {
            self.fault.take()
        }
    }

    /// A fake [`Funder`](super::Funder): records each requested top-up amount and
    /// returns a scripted [`DepositOutcome`], so the driver's top-up branch is
    /// testable without a chain handle.
    #[derive(Debug)]
    pub struct FakeFunder {
        max_topups: u32,
        outcome: DepositOutcome,
        calls: Mutex<Vec<U256>>,
    }

    impl FakeFunder {
        /// A funder allowing `max_topups` top-ups, each returning `outcome`.
        #[must_use]
        pub const fn new(max_topups: u32, outcome: DepositOutcome) -> Self {
            Self {
                max_topups,
                outcome,
                calls: Mutex::new(Vec::new()),
            }
        }

        /// The `additional` amounts passed to [`Funder::top_up`](super::Funder::top_up),
        /// in call order.
        #[must_use]
        pub fn calls(&self) -> Vec<U256> {
            self.calls.lock().map(|c| c.clone()).unwrap_or_default()
        }
    }

    impl super::Funder for FakeFunder {
        fn max_topups(&self) -> u32 {
            self.max_topups
        }

        fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
            Box::pin(async move {
                if let Ok(mut calls) = self.calls.lock() {
                    calls.push(additional);
                }
                Ok(self.outcome)
            })
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub use doubles::{FakeFunder, ScriptedReader, ScriptedSource};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::cast_possible_truncation)] // tests
mod tests {
    use super::{BlobSource, Funder};
    use crate::sink::StashedFault;
    use alloy::primitives::U256;
    use decdn_bao_range::align_range;
    use decdn_incentive::DepositOutcome;
    use iroh_io::AsyncStreamReader;

    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// The scripted source yields wire that the store's decoder can verify, and
    /// reports the blob's true `total_bytes`.
    #[tokio::test]
    async fn scripted_source_opens_a_verifiable_range() -> anyhow::Result<()> {
        let data = blob(200 * 1024 + 7);
        let source = super::ScriptedSource::new(data.clone())?;
        let range = align_range(0, 0, data.len() as u64)?;
        let (header, mut reader) = source.open(source.root(), range).await?;
        assert_eq!(header.total_bytes, data.len() as u64);
        let first = reader.read_bytes(64).await?;
        assert!(!first.is_empty(), "a non-empty blob must yield wire bytes");
        assert!(
            reader.take_fault().is_none(),
            "no fault was scripted, so none should be parked"
        );
        Ok(())
    }

    /// A scripted mid-range fault is parked on the reader for the decode loop to
    /// surface, exactly as a real stalled peer would.
    #[tokio::test]
    async fn scripted_source_parks_a_mid_range_fault() -> anyhow::Result<()> {
        let data = blob(200 * 1024 + 7);
        let source = super::ScriptedSource::new(data.clone())?
            .with_fault_after(4096, || anyhow::anyhow!("scripted stall"));
        let range = align_range(0, 0, data.len() as u64)?;
        let (_header, mut reader) = source.open(source.root(), range).await?;
        // Drain the truncated wire.
        while !reader.read_bytes(64 * 1024).await?.is_empty() {}
        assert!(
            reader.take_fault().is_some(),
            "the scripted fault must be parked once the truncated wire is drained"
        );
        Ok(())
    }

    /// `PeerSource` implements `BlobSource` — checked at compile time rather than
    /// exercised end-to-end, since a real run needs a live `Endpoint`/connection.
    /// The driver tests exercise the trait's behavior against `ScriptedSource`;
    /// the loopback suites (`open_progressive_pull` under the `stream_fetch*`
    /// wrappers, the `byte_len` plumbing above) cover `PeerSource`'s own
    /// building blocks. `'static` is just a concrete lifetime to instantiate the
    /// generic type parameter with — no value is constructed.
    #[test]
    fn peer_source_is_a_blob_source() {
        fn assert_impl<T: BlobSource>() {}
        assert_impl::<super::PeerSource<'static>>();
    }

    /// The fake funder records amounts and echoes its scripted outcome.
    #[tokio::test]
    async fn fake_funder_records_and_returns() -> anyhow::Result<()> {
        let funder = super::FakeFunder::new(3, DepositOutcome::Added(U256::from(100u64)));
        assert_eq!(funder.max_topups(), 3);
        let out = funder.top_up(U256::from(40u64)).await?;
        assert_eq!(out, DepositOutcome::Added(U256::from(100u64)));
        assert_eq!(funder.calls(), vec![U256::from(40u64)]);
        Ok(())
    }
}
