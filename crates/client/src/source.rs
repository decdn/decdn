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

use crate::sink::{PullReader, StashedFault};
use crate::{
    PoolContext, PoolLedger, PullDeadlines, UpstreamPull, UpstreamPullHeader, VoucherProgress,
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
/// client backend writes `.partial`; a node backend admits to the cache and
/// tees to its downstream client. Kept a generic method (not
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
    ///
    /// `claimed_total` is the blob size the leg's sender signs in its header.
    /// The sender serves `range` clamped to that size and encodes it under
    /// that size's tree, so a store keyed by offset verifies the leg under
    /// it.
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<R>> + 'a>>
    where
        R: BaoRangeReader + 'a;

    /// Persist the store's current in-memory present-range snapshot to its
    /// durable record. The single-writer flush point: several
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

    /// The size a verified final chunk proved, if any leg has proved one. A
    /// store keyed by a fixed size knows that size from the start, so the
    /// default is [`total_bytes`](decdn_bao_range::RangedStore::total_bytes).
    /// [`crate::acquire`] completes once a size is proven and every byte below
    /// it is present.
    fn proven(&self) -> Option<u64> {
        Some(self.total_bytes())
    }

    /// Move the planner's bound ([`total_bytes`](decdn_bao_range::RangedStore::total_bytes))
    /// to `bound`. [`crate::acquire`] grows the bound this way while no size is
    /// proven. A store keyed by a fixed size keeps it, so the default does
    /// nothing.
    fn set_bound(&self, bound: u64) {
        let _ = bound;
    }
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
    /// names the tx (see `decdn_client::buyer_pool::escrowed_but_untracked`).
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
/// the context out, then drops the guard before awaiting, so no lock is ever
/// held across an `.await`. Concurrent lanes pay from one pool, so one lane
/// can open while another lane tops the pool up, and an open's snapshot can
/// predate a deposit that is about to land: it under-states the deposit, never
/// over-states it. The lane that topped up waits out the node's view of the
/// new deposit; a lane refused on the stale view takes its own fund-and-retry
/// path, and the pool's top-up lock ([`crate::SharedPool::topup_lock`]) makes
/// it re-read the raised deposit instead of escrowing again.
///
/// Every open dials its own connection.
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
    /// The runtime every dial of this source runs on, so each connection's QUIC
    /// driver lives there rather than on the runtime the pull runs on. `None`
    /// dials on the caller's runtime. See
    /// [`with_dial_runtime`](Self::with_dial_runtime).
    dial_runtime: Option<tokio::runtime::Handle>,
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
            dial_runtime: None,
        }
    }

    /// Run every dial of this source on `runtime`.
    ///
    /// A dial spawns the connection's QUIC driver on the runtime that runs it, and
    /// the driver is what carries the connection through its close to QUIC's
    /// draining state. A caller that runs a pull on a runtime it drops afterwards
    /// (`decdn-node`'s per-serve pull legs) passes a runtime that outlives its
    /// endpoint here. Otherwise a connection the pull dialled that has not reached
    /// draining when the pull's runtime drops loses its driver, stays in the
    /// endpoint's active set, and `Endpoint::close` waits for it forever.
    /// The dial and the connection's driver move to `runtime`; the pull still
    /// opens and polls its streams from the caller's runtime.
    #[must_use]
    pub fn with_dial_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.dial_runtime = Some(runtime);
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

    /// Open `[byte_offset, +byte_len)` of `hash` (`byte_len == 0` = to end) on
    /// a connection of its own.
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
        crate::open_progressive_pull(
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
            self.dial_runtime.as_ref(),
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
            // One line per paid leg, so a stalled leg can be matched to the
            // node's own serve of the same range (#2211).
            tracing::debug!(
                peer = %self.target.id,
                hash = %blake3::Hash::from_bytes(hash).to_hex(),
                byte_offset = range.fetch_start(),
                byte_len = range.fetch_len(),
                "opening a paid leg"
            );
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
/// wrapped source. The age runs from when the pull opened, which the caller
/// passes to [`PrimedSource::prime`].
pub const PRIMED_MAX_IDLE: Duration = Duration::from_secs(2);

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
/// it. Any other open, and an open that finds the primed pull opened longer than
/// [`PRIMED_MAX_IDLE`] ago, goes to the wrapped source.
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
        let primed = self.slot().is_some();
        f.debug_struct("PrimedSource")
            .field("primed", &primed)
            .finish_non_exhaustive()
    }
}

impl<S: BlobSource> PrimedSource<S> {
    /// The parked-pull slot. The slot is a plain swap with no invariant a panic
    /// could break, so a poisoned lock is used as is.
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Primed<S::Reader>>> {
        self.primed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Wrap `inner` with nothing primed.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self {
            inner,
            primed: Mutex::new(None),
        }
    }

    /// Park a live pull of `range` of `hash`, opened at `opened_at`, for the next
    /// open of exactly that range. Replaces (and so closes) any pull still
    /// parked. The pull's age runs from `opened_at`, not from this call, so a
    /// caller that holds the pull a while before parking it cannot hand the
    /// drive a pull the peer has stopped holding.
    pub fn prime(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
        header: UpstreamPullHeader,
        reader: S::Reader,
        opened_at: tokio::time::Instant,
    ) {
        let parked = Primed {
            hash,
            range,
            header,
            reader,
            at: opened_at,
        };
        let replaced = self.slot().replace(parked);
        if replaced.is_some() {
            tracing::debug!("a primed pull no open took is replaced and closed");
        }
    }

    /// Close a parked pull that no open took.
    pub fn clear(&self) {
        if self.slot().take().is_some() {
            tracing::debug!("a primed pull no open took is closed");
        }
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
        let mut slot = self.slot();
        let parked = slot.take()?;
        if parked.at.elapsed() > PRIMED_MAX_IDLE {
            tracing::debug!("a primed pull went stale before its open and is closed");
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
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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

    /// A healthy buyer context paying `provider`, with `deposit` on the pool.
    #[cfg(test)]
    pub(crate) fn ctx_with(provider: u8, deposit: U256) -> crate::PoolContext {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        crate::PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(provider),
            deposit,
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    /// Builds a typed fault to park mid-range. Boxed so a source can be re-opened
    /// (an `anyhow::Error` is not `Clone`, so it is regenerated per open).
    type FaultFn = Arc<dyn Fn() -> anyhow::Error + Send + Sync>;

    /// One leg's `(fetch_start, opened_at, finished_at)` on the runtime's
    /// clock; `finished_at` is `None` for a leg that faulted or was dropped.
    type LegTimes = (u64, tokio::time::Instant, Option<tokio::time::Instant>);

    /// A scripted [`BlobSource`](super::BlobSource) that yields the real bao wire
    /// for any requested range of a fixed blob, and can truncate a range's wire
    /// and park a typed fault to simulate a mid-range peer failure.
    #[derive(Clone)]
    pub struct ScriptedSource {
        root: [u8; 32],
        blob: Bytes,
        outboard: Bytes,
        fault: Option<(usize, FaultFn)>,
        /// When set, `fault` fires on the readers that reach it while this
        /// count is above zero, and never again. Shared by clones.
        faults_left: Option<Arc<AtomicU32>>,
        /// When set, the size every header signs in place of the blob's own.
        signed_size: Option<u64>,
        /// Refuse every open whose 0-based index (in call order) lies in the
        /// range, with the fault its `FaultFn` produces, before any byte flows:
        /// a node that refuses one more stream while it serves the others. A
        /// refused open is still recorded in `opened`.
        refuse_open: Option<(std::ops::Range<usize>, FaultFn)>,
        /// When each `open` started and each clean `finish` ended, on the
        /// runtime's clock, as `(fetch_start, opened_at, finished_at)`. A leg
        /// that faulted or was dropped has no `finished_at`. Shared by clones.
        timeline: Arc<Mutex<Vec<LegTimes>>>,
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
        /// When set, every reader waits on its first read until the gate reads
        /// `true`: a source held back until the test releases it.
        gate: Option<tokio::sync::watch::Receiver<bool>>,
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
                faults_left: None,
                signed_size: None,
                refuse_open: None,
                timeline: Arc::new(Mutex::new(Vec::new())),
                opened: Arc::new(Mutex::new(Vec::new())),
                delivered: Arc::new(AtomicU64::new(0)),
                gate: None,
                first_read_stall: None,
                finish_stall: None,
                stall_after: None,
                ledger: None,
            })
        }

        #[cfg(test)]
        /// Delay every `finish` by `stall` after its range is fully delivered,
        /// holding the completed range in the scheduler's `in_flight` so a peer's
        /// steal of an already-present range fires deterministically (see
        /// `finish_stall`).
        #[must_use]
        pub(crate) const fn slow_finish(mut self, stall: Duration) -> Self {
            self.finish_stall = Some(stall);
            self
        }

        #[cfg(test)]
        /// Wedge every reader once it has delivered `after_bytes`: the next read
        /// sleeps for `stall` rather than returning bytes, so the source stops
        /// making verified progress without ever erroring (see
        /// [`stall_after`](Self::stall_after)). Pair a `stall` well above the
        /// scheduler's lane watchdog (`LANE_WATCHDOG`) with a nonzero
        /// `after_bytes` to trip the watchdog deterministically.
        #[must_use]
        pub(crate) const fn stall_after(mut self, after_bytes: u64, stall: Duration) -> Self {
            self.stall_after = Some((after_bytes, stall));
            self
        }

        #[cfg(test)]
        /// Hold every reader's first read until `gate` reads `true`, so a test
        /// decides when this source starts to deliver.
        #[must_use]
        pub(crate) fn gated_on(mut self, gate: tokio::sync::watch::Receiver<bool>) -> Self {
            self.gate = Some(gate);
            self
        }

        #[cfg(test)]
        /// Inject a one-time `stall` on the first read of every reader this
        /// source yields, so a competing fast source finishes first and steals
        /// this one's tail — the deterministic trigger the no-double-pay test
        /// needs.
        #[must_use]
        pub(crate) const fn slow_to_start(mut self, stall: Duration) -> Self {
            self.first_read_stall = Some(stall);
            self
        }

        #[cfg(test)]
        /// Total WIRE bytes actually delivered across every reader (see
        /// `delivered`). The honest "fetched and paid" proxy
        /// the no-double-pay assertion reads.
        #[must_use]
        pub(crate) fn delivered_bytes(&self) -> u64 {
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
        pub(crate) fn opened_ranges(&self) -> Vec<(u64, u64)> {
            self.opened.lock().map(|o| o.clone()).unwrap_or_default()
        }

        #[cfg(test)]
        /// Each leg's `(fetch_start, opened_at, finished_at)`, in open order
        /// (see `timeline`).
        #[must_use]
        pub(crate) fn timeline(&self) -> Vec<LegTimes> {
            self.timeline.lock().map(|t| t.clone()).unwrap_or_default()
        }

        #[cfg(test)]
        /// Total content bytes opened across every `open` call (the sum of each
        /// opened range's `fetch_len`). Equals the gap bytes, NOT the whole blob,
        /// when the driver skips held ranges.
        #[must_use]
        pub(crate) fn opened_bytes(&self) -> u64 {
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

        /// [`Self::with_fault_after`], but only the first reader that reaches
        /// `wire_bytes` faults: a peer that blips once and then recovers.
        #[must_use]
        pub fn fault_once_after(
            self,
            wire_bytes: usize,
            make: impl Fn() -> anyhow::Error + Send + Sync + 'static,
        ) -> Self {
            self.fault_times_after(1, wire_bytes, make)
        }

        /// [`Self::with_fault_after`], but only the first `times` readers that
        /// reach `wire_bytes` fault: a peer that refuses a few times and then
        /// serves.
        #[must_use]
        pub fn fault_times_after(
            self,
            times: u32,
            wire_bytes: usize,
            make: impl Fn() -> anyhow::Error + Send + Sync + 'static,
        ) -> Self {
            let mut this = self.with_fault_after(wire_bytes, make);
            this.faults_left = Some(Arc::new(AtomicU32::new(times)));
            this
        }

        /// Sign `total_bytes` in every header in place of the blob's own size:
        /// a peer that reports a wrong size. Its wire stays the blob's own, so
        /// a leg that holds the final chunk fails to verify under the signed
        /// size.
        #[must_use]
        pub const fn signing_size(mut self, total_bytes: u64) -> Self {
            self.signed_size = Some(total_bytes);
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
                let index = self.opened.lock().map_or(0, |mut log| {
                    log.push((range.fetch_start(), range.fetch_len()));
                    log.len().saturating_sub(1)
                });
                let leg = self.timeline.lock().ok().map(|mut t| {
                    t.push((range.fetch_start(), tokio::time::Instant::now(), None));
                    t.len().saturating_sub(1)
                });
                if let Some((refused, make)) = &self.refuse_open
                    && refused.contains(&index)
                {
                    return Err(make());
                }
                // Serve the request clamped to this blob's end, as a node
                // does: the requester's range may be aligned under another
                // size than this blob's.
                let served = decdn_bao_range::align_range_clamped(
                    range.fetch_start(),
                    range.fetch_len(),
                    self.total_bytes(),
                )?;
                let mut wire = self.wire_for(&served)?;
                let mut fault = None;
                if let Some((after, make)) = &self.fault
                    && *after < wire.len()
                    && self.faults_left.as_ref().is_none_or(|left| {
                        left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                            .is_ok()
                    })
                {
                    wire = wire.slice(..*after);
                    fault = Some(make());
                }
                let header = UpstreamPullHeader {
                    total_bytes: self.signed_size.unwrap_or_else(|| self.total_bytes()),
                    rate_per_mb: SCRIPTED_RATE_PER_MB,
                    interval_bytes: SCRIPTED_INTERVAL_BYTES,
                    // No real network round trip in this scripted double.
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
                        leg,
                        wire_len,
                        delivered: Arc::clone(&self.delivered),
                        gate: self.gate.clone(),
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
                if let Some(leg) = reader.leg
                    && let Ok(mut timeline) = self.timeline.lock()
                    && let Some(entry) = timeline.get_mut(leg)
                {
                    entry.2 = Some(tokio::time::Instant::now());
                }
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
        /// This leg's index in its source's timeline, stamped when it finishes.
        leg: Option<usize>,
        /// Waited on before the first read (see `ScriptedSource::gated_on`);
        /// `None` once passed.
        gate: Option<tokio::sync::watch::Receiver<bool>>,
        /// The wire byte count this reader was handed (before consumption), used by
        /// `ScriptedSource`'s [`BlobSource::finish`](crate::BlobSource::finish) to advance a paying ledger by
        /// this leg's spend.
        wire_len: u64,
        /// Shared with the parent [`ScriptedSource`]: bumped by the bytes each
        /// `read_bytes` actually yields, so a mid-stream drop stops counting the
        /// instant it happens.
        delivered: Arc<AtomicU64>,
        /// A one-time stall consumed on the first `read_bytes` (see
        /// `ScriptedSource::slow_to_start`); `None` after it fires once.
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
            if let Some(mut gate) = self.gate.take() {
                // A closed gate channel reads as open.
                let _ = gate.wait_for(|open| *open).await;
            }
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
pub(crate) use doubles::ctx_with;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::cast_possible_truncation)] // tests
mod tests {
    use super::{BlobSource, Funder};
    use crate::sink::StashedFault;
    use alloy::primitives::U256;
    use decdn_bao_range::{AlignedRange, align_range};
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
        assert!(
            drain(&source, &range).await.is_err(),
            "the scripted fault must be parked once the truncated wire is drained"
        );
        Ok(())
    }

    /// Open `range` on `src` and read its wire to the end. Returns the wire
    /// bytes read, or the fault the reader parked.
    async fn drain(src: &super::ScriptedSource, range: &AlignedRange) -> anyhow::Result<u64> {
        let (_header, mut reader) = src.open(src.root(), range.clone()).await?;
        let mut read = 0u64;
        loop {
            let chunk = reader.read_bytes(64 * 1024).await?;
            if chunk.is_empty() {
                break;
            }
            read += chunk.len() as u64;
        }
        reader.take_fault().map_or(Ok(read), Err)
    }

    #[tokio::test]
    async fn fault_once_after_fires_on_the_first_reader_only() -> anyhow::Result<()> {
        let src = super::ScriptedSource::new(vec![3u8; 64 * 1024])?
            .fault_once_after(4096, || anyhow::anyhow!("scripted reset"));
        let range = align_range(0, 64 * 1024, 64 * 1024)?;
        let first = drain(&src, &range).await;
        assert!(first.is_err(), "the first reader faults");
        let second = drain(&src, &range).await;
        assert!(second.is_ok(), "the second reader delivers");
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
