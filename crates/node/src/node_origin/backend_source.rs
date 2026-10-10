//! `BackendSource` — the node's UNPAID [`decdn_client::BlobSource`] over its
//! own configured origin (fs/http/s3), for the own-origin serve-miss.
//!
//! The peer serve-miss path pulls a cold blob from another node over a paid
//! `cdn/client/v1` lane ([`decdn_client::PeerSource`]). The own-origin
//! serve-miss flow is the LOCAL twin: the bytes are already reachable through this
//! node's own origin, so
//! there is no counterparty, no lane, and no payment — but the driver, sink,
//! and serve leg are reused verbatim. This source is what lets that happen: it
//! streams the header-less interleaved bao wire for one [`AlignedRange`] straight
//! out of [`CacheEngine::origin_range_wire`], so the existing `NodeAdmitStore`
//! sink verifies-and-stores it against the root `H` exactly as it does a peer
//! pull's wire. The wire is produced window by window, so a leg holds
//! `O(window + outboard)` bytes whatever its range (#2065).
//!
//! # Why the origin bytes are a LOCAL fault, not upstream corruption
//!
//! By the time this source runs, the node has already signed a `StreamResponse`
//! committing to serve the blob under `H` to a paying client.
//! [`CacheEngine::origin_range_wire`] therefore treats a range that fails bao
//! verification as a HARD [`decdn_cache::CacheError::VerifyFailed`] — a
//! corrupt/misconfigured OWN origin — rather than degrading it (there is no
//! upstream to blame and no fallback that still honours `H`). This source
//! propagates that fault out of [`BlobSource::open`] when it is found up front,
//! and parks it on the reader ([`StashedFault`]) when it is found mid-stream;
//! nothing here scores a provider, because there is no provider.
//!
//! # The self-payment counter (THE CRUX)
//!
//! [`decdn_client::drive`]'s per-gap completion is PAID-frontier gated: a gap
//! is `Done` only once the ledger's committed `bytes` reach the gap end. An unpaid
//! source whose `finish` never advanced a ledger would leave that frontier at zero
//! and the gap loop would re-draw forever. So this source carries a fresh, LOCAL
//! bookkeeping [`PoolLedger`] (`self_pay`) and, on [`BlobSource::finish`],
//! advances it by exactly the leg's drained WIRE bytes at rate 0 — `committed.bytes`
//! moves so the frontier reaches the gap end, while `committed.amount` stays 0 so
//! nothing resembling a payment or a deposit-exhaustion is ever computed. This is
//! NOT payment: no lane, no voucher, no chain, no counterparty — only the
//! driver's internal completion counter, exactly the `ScriptedSource::paying`
//! precedent with a zero rate.

use std::sync::Arc;

use alloy::primitives::U256;
use bytes::Bytes;
use decdn_bao_range::AlignedRange;
use decdn_cache::{CacheEngine, CacheError, CacheResult, Hash, OriginRangeWire};
use decdn_client::sink::StashedFault;
use decdn_client::source::SourceFuture;
use decdn_client::{BlobSource, PoolLedger, UpstreamPullHeader, VoucherProgress};
use iroh_io::AsyncStreamReader;

/// The node's unpaid [`BlobSource`] for the own-origin serve-miss flow.
///
/// One `BackendSource` serves one gap-driven fetch of the blob `root` (a
/// `total_bytes`-byte blob) out of `engine`'s configured origins. `self_pay` is a
/// LOCAL completion counter only — see the module docs and
/// [`BlobSource::finish`].
pub(crate) struct BackendSource {
    engine: CacheEngine,
    root: [u8; 32],
    total_bytes: u64,
    /// Local completion bookkeeping ONLY — no lane, no chain, no counterparty.
    self_pay: Arc<PoolLedger>,
}

impl BackendSource {
    /// Build an unpaid own-origin source for `root` (a `total_bytes`-byte blob)
    /// over `engine`, whose `self_pay` ledger the caller also drives the
    /// completion frontier off (the SAME `Arc` the driver is handed). See the
    /// module docs for why the ledger is local bookkeeping, not payment.
    pub(crate) const fn new(
        engine: CacheEngine,
        root: [u8; 32],
        total_bytes: u64,
        self_pay: Arc<PoolLedger>,
    ) -> Self {
        Self {
            engine,
            root,
            total_bytes,
            self_pay,
        }
    }

    /// The LOCAL completion-counter ledger this source advances on
    /// [`BlobSource::finish`]. The pull leg MUST hand this SAME `Arc` to
    /// [`decdn_client::drive`] as its `ledger`, so the paid-frontier the gap
    /// loop reads for completion is the one `finish` moves — the whole point of THE
    /// CRUX in the module docs. Returned as a fresh handle onto the shared ledger,
    /// never a second ledger.
    pub(crate) fn ledger(&self) -> Arc<PoolLedger> {
        Arc::clone(&self.self_pay)
    }
}

impl BlobSource for BackendSource {
    type Reader = BackendReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(async move {
            // This source is pinned to one blob: a mismatched `hash` is a
            // driver/orchestration bug, not an origin condition.
            if hash != self.root {
                anyhow::bail!(
                    "backend source for {} opened for a foreign hash {}",
                    Hash::from(self.root),
                    Hash::from(hash),
                );
            }
            // Stream + verify + encode the range out of our own origin. A fault
            // found up front (a wrong-length outboard or first window, a transport
            // fault, a read past its time budget) surfaces here via `?`; a clean
            // decline (no origin serves the outboard or the range) is `None`.
            let Some(wire) = self
                .engine
                .origin_range_wire(Hash::from(hash), &range)
                .await?
            else {
                anyhow::bail!(
                    "no configured origin serves the outboard or the range for {}",
                    Hash::from(hash)
                );
            };
            let header = UpstreamPullHeader {
                total_bytes: self.total_bytes,
                rate_per_mb: 0,
                interval_bytes: 0,
                // A local origin re-encode, not a network round trip.
            };
            Ok((header, BackendReader::new(wire, range.wire_len())))
        })
    }

    fn stop(&self, _reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        // Only a steal stops a leg early, and the node's pull leg drives one
        // source with no steal. A caller that asks otherwise is this
        // process's bug, never the origin's.
        Box::pin(async {
            Err(anyhow::anyhow!("a backend leg runs every range to its end")
                .context(decdn_client::LocalPullFault))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            // Advance the LOCAL completion counter by this leg's wire length at
            // rate 0, so `committed.bytes` reaches the gap end (the driver's
            // paid-frontier completion) while `committed.amount` stays 0 — the
            // `ScriptedSource::finish` paying arm, with a zero rate. This is not
            // payment; see the module docs. The driver calls `finish` only after
            // the sink decoded the whole range, so the reader drained exactly
            // `expected_wire_len` bytes.
            debug_assert!(
                reader.pending.is_empty(),
                "finish on a backend reader with undrained wire"
            );
            if reader.expected_wire_len > 0 {
                self.self_pay
                    .issue(
                        reader.expected_wire_len,
                        0,
                        // No chain on the unpaid leg: `Keep` on a lane that has
                        // opened none commits a sealed section and opens nothing.
                        decdn_client::EpochAction::Keep,
                        |_next, _chain| async { Ok(()) },
                    )
                    .await?;
            }
            Ok(VoucherProgress::from_cumulative(
                self.self_pay.committed(),
                U256::ZERO,
            ))
        })
    }
}

/// The chunk source a [`BackendReader`] drains: in production the
/// [`OriginRangeWire`], whose chunks end in one terminal `Err` on a fault.
pub(crate) trait WireChunks: Send {
    /// The next chunk, a terminal `Err`, or `None` at a clean end.
    fn next_chunk(&mut self) -> impl Future<Output = Option<CacheResult<Bytes>>> + Send;
}

impl WireChunks for OriginRangeWire {
    fn next_chunk(&mut self) -> impl Future<Output = Option<CacheResult<Bytes>>> + Send {
        Self::next_chunk(self)
    }
}

/// The reader a [`BackendSource`] yields: the header-less bao wire streamed out
/// of an [`OriginRangeWire`]. A fault the encode stopped on — a window that fails
/// verification against `H`, or an origin that stops serving mid-stream — fails
/// the read and is kept for [`StashedFault::take_fault`], so the sink reports
/// that typed [`CacheError`] rather than a bare truncation.
pub(crate) struct BackendReader<W = OriginRangeWire> {
    wire: W,
    /// The unread rest of the last chunk the wire yielded.
    pending: Bytes,
    /// The terminal fault the wire ended on, until the sink takes it.
    fault: Option<CacheError>,
    /// The range's exact header-less wire byte count
    /// ([`AlignedRange::wire_len`]), used by [`BlobSource::finish`] to advance
    /// the local completion counter.
    expected_wire_len: u64,
}

impl<W: WireChunks> BackendReader<W> {
    const fn new(wire: W, expected_wire_len: u64) -> Self {
        Self {
            wire,
            pending: Bytes::new(),
            fault: None,
            expected_wire_len,
        }
    }

    /// Refill `pending` from the wire when it is empty. `Ok(false)` at a clean
    /// end; `Err` once the wire has ended on a fault (kept for `take_fault`).
    async fn refill(&mut self) -> std::io::Result<bool> {
        while self.pending.is_empty() {
            match self.wire.next_chunk().await {
                Some(Ok(chunk)) => self.pending = chunk,
                Some(Err(fault)) => {
                    let msg = fault.display_chain().to_string();
                    self.fault = Some(fault);
                    return Err(std::io::Error::other(msg));
                }
                None if self.fault.is_some() => {
                    return Err(std::io::Error::other(
                        "own origin range wire ended on a fault",
                    ));
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }
}

impl<W: WireChunks> AsyncStreamReader for BackendReader<W> {
    /// Reads `len` bytes, or fewer only at the wire's end: the bao decoder
    /// reads each leaf with one `read_bytes_exact`, so a short read here would
    /// fail it even though the rest of the leaf is in the next chunk. A read
    /// inside one chunk is a zero-copy slice; only a read that spans chunks is
    /// copied.
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        if len == 0 || !self.refill().await? {
            return Ok(Bytes::new());
        }
        if self.pending.len() >= len {
            return Ok(self.pending.split_to(len));
        }
        let mut out = bytes::BytesMut::new();
        while out.len() < len {
            if !self.refill().await? {
                break;
            }
            let take = self.pending.len().min(len - out.len());
            out.extend_from_slice(&self.pending.split_to(take));
        }
        Ok(out.freeze())
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        let mut out = [0u8; L];
        let mut filled = 0usize;
        while filled < L {
            if !self.refill().await? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "backend reader exhausted before a fixed-size bao read",
                ));
            }
            let take = self.pending.len().min(L - filled);
            let got = self.pending.split_to(take);
            let dst = out
                .get_mut(filled..filled.saturating_add(take))
                .ok_or_else(|| std::io::Error::other("backend reader fixed-size read overran"))?;
            dst.copy_from_slice(&got);
            filled = filled.saturating_add(take);
        }
        Ok(out)
    }
}

impl<W: WireChunks> StashedFault for BackendReader<W> {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.fault.take().map(anyhow::Error::from)
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "tests"
)]
#[cfg(test)]
mod tests;
