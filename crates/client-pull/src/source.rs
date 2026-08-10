//! The two *sourcing* axes of the gap-driven, range-minimized pull driver
//! (#1608): [`BlobSource`] — a dumb producer of raw interleaved bao bytes for a
//! contiguous range — and [`Funder`] — the injected channel top-up seam.
//!
//! # Why "dumb"
//!
//! A [`BlobSource`] never decodes and never verifies. It opens a pull over the
//! raw bao encoding of one [`decdn_bao_range::AlignedRange`] and yields the wire
//! bytes on demand; the STORE's ingest decoder (rooted at the blob hash `H`)
//! verifies each chunk group exactly once, exactly as `sink::decode_to_sink` does
//! today. This keeps a source — `PeerSource` (paid `cdn/client/v1`, A2) or a
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
//! There is no `trait Funder` in the pre-#1608 code: a mid-fetch top-up is the
//! free function [`crate::buyer_channel::top_up`], called by hand in each caller
//! (the CLI's `fetch_blob_streaming`, the node's resume loop). [`Funder`] lifts
//! that into an injected trait so the driver and `PeerSource` stay
//! chain-handle-agnostic. The reactive-top-up budget is deployment-specific and
//! travels on the funder ([`Funder::max_topups`] — CLI 3, node 1).

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use decdn_bao_range::AlignedRange;
use decdn_incentive::DepositOutcome;
use iroh::{Endpoint, EndpointAddr};

use crate::sink::{PullReader, StashedFault};
use crate::{ChannelContext, ChannelLedger, PullDeadlines, UpstreamPullHeader, VoucherProgress};

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
}

/// The injected channel top-up seam. Wraps the deployment's funding path
/// ([`crate::buyer_channel::top_up`] over the CLI's chain handle; the node's
/// `top_up_channel` later) so the driver and [`BlobSource`] never name a contract
/// instance.
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
    /// escrowed-but-untracked variants (`UnknownChannel` / `ChannelMismatch`),
    /// which the driver treats as terminal (the USDC is on-chain but the local
    /// record is gone; reconcile against the tx).
    ///
    /// # Errors
    ///
    /// If the on-chain `topUp` fails to submit, reverts, or its receipt is not
    /// obtained — the funds did not move.
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

/// The paid `BlobSource` (#1608 Phase A, A2): wraps
/// [`crate::open_progressive_pull`] behind [`BlobSource`], scoping every
/// [`open`](BlobSource::open) to exactly the requested [`AlignedRange`] via the
/// `byte_len`-bounded pull (the wire `StreamRequest` already carried the field;
/// only the OPEN-side plumbing was whole-tail before this).
///
/// One `PeerSource` serves one gap-driven fetch against one upstream peer — it
/// holds the pull context (`endpoint`, `target`, `ctx`, `ledger`, …) but not a
/// [`Funder`]: reactive top-up is the driver's concern (it holds the `Funder`
/// separately and calls it directly), not the source's.
///
/// Borrows its `endpoint`/`slash_domain` (the driver, which owns these for the
/// whole fetch, outlives every `open`/`finish` call), but holds the channel
/// context behind a SHARED `Arc<Mutex<ChannelContext>>` rather than a `&'a`
/// borrow. That shared handle is what resolves the #1608 borrow conflict: the
/// driver mutates the context (crediting a mid-fetch top-up's new deposit)
/// while this source also reads it to open each pull. A `&'a ChannelContext`
/// borrow would freeze it for the whole fetch and forbid the driver's `&mut`;
/// the `Arc<Mutex<..>>` lets both see one state. `open` locks it only to CLONE
/// the context out (single-writer per fetch — the driver never opens a pull
/// concurrently with a top-up), then drops the guard before awaiting, so no
/// lock is ever held across an `.await`.
pub struct PeerSource<'a> {
    endpoint: &'a Endpoint,
    target: EndpointAddr,
    ctx: Arc<Mutex<ChannelContext>>,
    ledger: Arc<ChannelLedger>,
    slash_domain: &'a Eip712Domain,
    expected_signer: Address,
    namespace_id: [u8; 32],
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
}

impl std::fmt::Debug for PeerSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `ChannelContext` (a signing key) and `Eip712Domain` are not `Debug`,
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
    /// `ledger` over `ctx`'s channel. `namespace_id`, `max_blob_size_bytes`,
    /// `max_rate_per_mb`, and `deadlines` are the same buyer-side policy knobs
    /// [`crate::open_progressive_pull`] takes directly — see its docs.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        endpoint: &'a Endpoint,
        target: EndpointAddr,
        ctx: Arc<Mutex<ChannelContext>>,
        ledger: Arc<ChannelLedger>,
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
        }
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
            // Snapshot the shared context (single-writer per fetch, so this can
            // never race a top-up), then drop the guard before the await —
            // `open_progressive_pull` needs `&ChannelContext` for its whole call,
            // and no std `Mutex` guard may be held across an `.await`.
            let ctx = {
                self.ctx
                    .lock()
                    .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                    .clone()
            };
            let (header, pull) = crate::open_progressive_pull(
                self.endpoint,
                self.target.clone(),
                &ctx,
                Arc::clone(&self.ledger),
                self.slash_domain,
                self.expected_signer,
                hash,
                self.namespace_id,
                range.fetch_start(),
                micros_now(),
                self.max_blob_size_bytes,
                self.max_rate_per_mb,
                self.deadlines,
                range.fetch_len(),
            )
            .await?;
            Ok((header, PullReader::new(pull)))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move { reader.into_inner().finish().await })
    }
}

// ---------------------------------------------------------------------------
// Test doubles: a scripted BlobSource + a fake Funder for the A4 driver tests.
// Feature-gated so a production caller cannot name them, but compiled outside
// `cfg(test)` under `test-util`, so they must stay anti-panic clean.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-util"))]
mod doubles {
    use std::sync::{Arc, Mutex};

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
        /// Optional ledger to advance on a clean `finish`, modelling payment: a
        /// real pull pays vouchers for the WIRE bytes it drains, and the driver's
        /// completion is PAID-frontier based (`content_paid_frontier`), so a double
        /// that never advanced `committed` would leave the driver's paid frontier at
        /// zero and it would never complete. `None` keeps the pre-payment "unpaid
        /// double" behaviour for tests that do not drive `fill_gap` to completion.
        ledger: Option<Arc<crate::ChannelLedger>>,
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
                ledger: None,
            })
        }

        /// Model payment: on every clean `finish`, advance `ledger`'s committed
        /// watermark by the leg's drained WIRE bytes (at [`SCRIPTED_RATE_PER_MB`]),
        /// exactly as a real pull's acked vouchers do. Required by any test that
        /// drives a gap to completion, since the driver's `Done` is paid-frontier
        /// based. Pass the SAME `Arc<ChannelLedger>` the driver is handed.
        #[must_use]
        pub fn paying(mut self, ledger: Arc<crate::ChannelLedger>) -> Self {
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
                    },
                ))
            })
        }

        fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            Box::pin(async move {
                let Some(ledger) = &self.ledger else {
                    // Unpaid double: no channel, nothing to drain, no watermark.
                    return Ok(VoucherProgress::default());
                };
                // Model the acked voucher for this leg's wire: issue then resolve,
                // advancing `committed.bytes` by the drained WIRE bytes so the
                // driver's paid frontier tracks payment (a real pull does this via
                // the receive loop's per-interval vouchers + acks).
                if reader.wire_len > 0 {
                    ledger
                        .issue(reader.wire_len, SCRIPTED_RATE_PER_MB, |_next| async {
                            Ok(())
                        })
                        .await?;
                    let _ = ledger.resolve_ack();
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
        /// [`ScriptedSource::finish`] to advance a paying ledger by this leg's spend.
        wire_len: u64,
    }

    impl iroh_io::AsyncStreamReader for ScriptedReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
            let take = self.wire.len().min(len);
            Ok(self.wire.split_to(take))
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
    /// A4's driver tests exercise the trait's behavior against `ScriptedSource`;
    /// the loopback pull tests in `lib.rs`/`sink.rs` (`open_progressive_pull`,
    /// `decode_to_sink`, the `byte_len` plumbing above) cover `PeerSource`'s own
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
