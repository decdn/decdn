//! Per-channel voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`ChannelLedger`] serializes voucher *issuance* across concurrent streams on one
//! payment channel — the lock spans compute → sign → send, but NOT the ack wait, so
//! several vouchers can be outstanding (unacked) at once and parallel streams to one
//! provider no longer serialize their payments behind each other's round trips (#1484).

use std::collections::VecDeque;
use std::future::Future;

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;
use decdn_protocol::client::WatermarkBundle;
use tokio::sync::Mutex;

/// A channel's cumulative voucher state: the absolute totals carried by the most
/// recent voucher. All three advance monotonically over the channel's lifetime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cumulative {
    /// Last voucher nonce (next voucher uses `nonce + 1`). `ZERO` for a fresh channel.
    pub nonce: U256,
    /// Cumulative channel bytes paid for.
    pub bytes: U256,
    /// Cumulative channel amount paid (token base units).
    pub amount: U256,
}

impl From<&WatermarkBundle> for Cumulative {
    /// Decode a wallet-less resume bundle's big-endian `uint256` totals
    /// (issue #1481) into the same shape [`ChannelLedger`] tracks. Infallible
    /// — `U256::from_be_bytes` cannot fail on a fixed 32-byte array — so a
    /// caller can re-seed directly from a bundle without a `Result`.
    fn from(bundle: &WatermarkBundle) -> Self {
        Self {
            nonce: U256::from_be_bytes(bundle.nonce),
            bytes: U256::from_be_bytes(bundle.bytes_delivered),
            amount: U256::from_be_bytes(bundle.amount),
        }
    }
}

/// Compute the next voucher's absolute totals from the live cumulative and the
/// `delta_bytes` newly delivered (on any stream) since the last voucher. The
/// amount delta is `ceil(delta_bytes * rate_per_mb / 1 MiB)` so each voucher's
/// own delta covers its own bytes at the advertised rate (the node checks deltas).
#[must_use]
fn next_voucher(cur: &Cumulative, delta_bytes: u64, rate_per_mb: u64) -> Cumulative {
    let amount_delta = U256::from(delta_bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    Cumulative {
        nonce: cur.nonce.saturating_add(U256::from(1u64)),
        bytes: cur.bytes.saturating_add(U256::from(delta_bytes)),
        amount: cur.amount.saturating_add(amount_delta),
    }
}

/// The committed watermark plus the pipeline of vouchers still in flight, under a
/// single lock so a reader never catches `committed` advanced without the matching
/// pop (or the reverse). Both are read from a `Drop` impl, so the lock is a
/// `std::sync::Mutex` — see [`ChannelLedger::committed`].
#[derive(Debug)]
struct Pipeline {
    /// The highest voucher the upstream has acked. Monotonic; advanced only in
    /// [`ChannelLedger::resolve_ack`]. `committed` alone is not the record of what
    /// we might owe — a voucher on the wire whose ack we have not read yet is also
    /// money the upstream may already hold (ADR 003: it persists before it acks),
    /// which is what `outstanding` tracks.
    committed: Cumulative,
    /// Vouchers SENT but not yet resolved — neither acked nor rejected — in ascending
    /// nonce order (`push_back` on issue, `pop_front` on the FIFO ack/reject). Their
    /// fate is UNKNOWN, exactly the losing window [`ChannelLedger::settlement`] closes:
    /// a pull dropped inside the ack wait leaves the upstream holding nonce N while
    /// `committed` still says N-1. Settle low and the next reuse re-signs N, the
    /// upstream rejects `StaleNonce` (terminal), and the channel is wedged with its
    /// deposit escrowed until expiry (the desync in `send_voucher`, tracked in #1122).
    /// Pipelining (#1484) widens this window from one voucher to however many are in
    /// flight rather than introducing a new state; the count is bounded by the blob's
    /// interval budget (`max_blob_size / interval`), not a separate ceiling.
    outstanding: VecDeque<Cumulative>,
}

/// One channel's live voucher ledger, shared by every concurrent stream on that
/// channel. Voucher *issuance* is serialized through the async `issuance` mutex —
/// held across compute → sign → send so vouchers reach the node in strict nonce
/// order — but NOT across the ack wait. The ack is read off the critical path by
/// the receive loop and fed back through [`Self::resolve_ack`] /
/// [`Self::resolve_reject`], so several vouchers can be outstanding at once and
/// parallel streams to one provider no longer serialize their payments (#1484).
#[derive(Debug)]
pub struct ChannelLedger {
    /// Serializes issuance (compute the next nonce → sign → send). Its guarded value
    /// is `()`: the ordering it enforces lives in `pipeline`, and this is only the
    /// token that makes the compute-and-send critical section mutually exclusive
    /// across concurrent issuers. Held across an `await` (the send), so it is a
    /// `tokio::sync::Mutex`, never the sync one below.
    issuance: Mutex<()>,
    /// The committed watermark + in-flight pipeline. A `std::sync::Mutex` because it
    /// is read from `Drop` (which cannot await): a pull can end by being DROPPED, not
    /// only by returning, and this is the record of money already spent (#1145
    /// review). The buffered node pull runs with `hard_cap: None` (#1134), so what
    /// ends a slow-but-progressing transfer is always external — the foreground
    /// `outer_pull_deadline`, or the serve future being dropped (client disconnect,
    /// shutdown) — which DROPS the future. Held for a few instructions at a time and never
    /// across an await, so it cannot deadlock with the issuance lock.
    pipeline: std::sync::Mutex<Pipeline>,
}

impl ChannelLedger {
    /// Build a ledger seeded from the channel's persisted cumulative state (the
    /// last voucher acked on earlier streams/invocations). Pass `Cumulative::default()`
    /// for a brand-new channel.
    #[must_use]
    pub fn new(seed: Cumulative) -> Self {
        Self {
            issuance: Mutex::new(()),
            pipeline: std::sync::Mutex::new(Pipeline {
                committed: seed,
                outstanding: VecDeque::new(),
            }),
        }
    }

    /// Lock the pipeline, recovering the inner value on poison. A poisoned lock means
    /// a prior holder panicked mid-write; the guarded value is money, so recover it
    /// rather than propagate — refusing to read here would throw away the very
    /// watermark the mirror exists to save.
    fn pipeline(&self) -> std::sync::MutexGuard<'_, Pipeline> {
        self.pipeline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Read the committed (acked) cumulative WITHOUT awaiting — the drop-safe reader,
    /// and the only one a `Drop` impl can call. The ledger advances `committed` only
    /// after an ack, so this is exactly the last acked cumulative: both the drop-path
    /// watermark and the value a *completed* buffered pull persists.
    #[must_use]
    pub fn committed(&self) -> Cumulative {
        self.pipeline().committed
    }

    /// The cumulative a cancelled pull must PERSIST — the drop-path counterpart of
    /// [`Self::committed`], and what a `Drop` guard should actually write (#1122).
    ///
    /// It is [`Self::committed`], except when vouchers are still on the wire with
    /// their fate unresolved, in which case it is the HIGHEST of them. Settling high
    /// is the safe direction, and it is not merely safe but *correct*:
    ///
    /// - Every outstanding voucher pays for bytes the upstream ALREADY DELIVERED to
    ///   us — that is why it was issued. Honouring them is paying what we owe.
    /// - If the upstream did commit them (the likely case: it persists before it
    ///   acks), settling low re-signs a spent nonce and wedges the channel permanently.
    /// - If it did not, we have merely skipped a nonce. The serve side tolerates a
    ///   nonce GAP (it meters `voucher_nonce_gap` and accepts); it does not tolerate a
    ///   regression. The two errors are not symmetric, so we take the survivable one.
    ///
    /// With pipelining the set can hold several vouchers; the highest covers them all
    /// (cumulative totals), so returning it is the settle-high answer for the whole
    /// set. A voucher the upstream explicitly REJECTED is never here — [`Self::resolve_reject`]
    /// removes it — so this cannot inflate our cumulative for bytes the upstream
    /// declined to be paid for. An ambiguous failure (a stall, a reset) is the
    /// opposite: it leaves every voucher armed, because the upstream may hold them and
    /// settling low would strand the deposit.
    #[must_use]
    pub fn settlement(&self) -> Cumulative {
        let pipeline = self.pipeline();
        match pipeline.outstanding.back() {
            // `>` on the nonce, not just "present": the outstanding set is ascending
            // and every entry exceeds `committed` by construction, but comparing keeps
            // the watermark from ever regressing even if that invariant were violated.
            Some(highest) if highest.nonce > pipeline.committed.nonce => *highest,
            _ => pipeline.committed,
        }
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher and
    /// SEND it — without waiting for the ack (#1484). `exchange` does only the signing
    /// and the send; the `VoucherAck` is read off the critical path by the receive
    /// loop and fed back through [`Self::resolve_ack`] / [`Self::resolve_reject`].
    ///
    /// Holds the issuance lock across compute → arm → `exchange`, so concurrent
    /// issuers on the shared ledger sign strictly increasing nonces even though their
    /// byte transfers and ack waits overlap. The voucher is ARMED (added to the
    /// in-flight set) *before* `exchange`, so a future dropped inside the send still
    /// has [`Self::settlement`] report what we owe.
    ///
    /// The committed watermark is NOT advanced here — only [`Self::resolve_ack`] does
    /// that, once the ack comes back. An `exchange` error leaves the voucher armed
    /// (its fate is ambiguous: a partial write may have reached the upstream, which
    /// persists before it acks), so `settlement` settles high.
    ///
    /// The outstanding set is deliberately not capped here. It cannot grow without limit:
    /// the receive loop stops issuing once `expected_wire_bytes` have arrived, and that
    /// total is capped at the open stage against `max_blob_size`, so the set is bounded
    /// by `max_blob_size / interval` (a handful of small `Cumulative`s). Nor does an
    /// unacked pipeline risk money — every voucher is cumulative over bytes ALREADY
    /// delivered, so the client never pays ahead of what it received. A separate
    /// client-side ceiling would only have to be kept above the operator-tunable
    /// serve-side credit window (#1477) or it would abort honest transfers — a coupling
    /// worth avoiding. A provider that delivers but never acks is caught by the pull's
    /// `stall` timeout, not by a voucher count.
    ///
    /// `exchange` receives the next [`Cumulative`] (the values to sign and send); the
    /// caller owns signing + framing so this module stays free of EIP-712 / wire types.
    pub async fn issue<F, Fut>(
        &self,
        delta_bytes: u64,
        rate_per_mb: u64,
        exchange: F,
    ) -> anyhow::Result<Cumulative>
    where
        F: FnOnce(Cumulative) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        // Serialize issuance. Held across the send below, but released before any ack
        // is read — that release is the whole point of #1484.
        let _issuing = self.issuance.lock().await;
        // Compute the next voucher from the current in-flight frontier and arm it, all
        // under one pipeline lock so the basis we build on cannot shift between reading
        // it and pushing. Build on the SETTLEMENT frontier (highest armed, else
        // committed), not `committed` alone: a voucher already on the wire must be
        // exceeded, or we re-sign a nonce the upstream may hold and collide on
        // `StaleNonce`.
        let next = {
            let mut pipeline = self.pipeline();
            let frontier = match pipeline.outstanding.back() {
                Some(highest) if highest.nonce > pipeline.committed.nonce => *highest,
                _ => pipeline.committed,
            };
            let next = next_voucher(&frontier, delta_bytes, rate_per_mb);
            pipeline.outstanding.push_back(next);
            next
        };
        // Send only. On ANY error the voucher stays armed: a send failure is as
        // ambiguous as a drop (a partial write may have landed, and the upstream
        // persists before it acks), so `settlement` must settle high. A rejection is
        // NOT an issuance outcome any more — it arrives later as a `VoucherAck`-slot
        // message and is disarmed via `resolve_reject`.
        exchange(next).await?;
        Ok(next)
    }

    /// Wallet-less self-heal (issue #1481): overwrite the committed watermark to
    /// `cum` — typically [`Cumulative::from`] a [`WatermarkBundle`] the node
    /// attached to a gated `StaleNonce` / `AmountRegression` / `BytesRegression` /
    /// `InsufficientDeposit` rejection — and clear the WHOLE outstanding pipeline,
    /// since the node has just told us its authoritative watermark and every
    /// voucher we optimistically had in flight (#1484) is superseded by it: the one
    /// it rejected it does not hold, and any others past that nonce it never
    /// accepted either. The next [`Self::issue`] on this ledger builds on `cum` and
    /// signs `cum.nonce + 1`, matching what the node will actually accept next.
    ///
    /// This is a hard overwrite, not a monotonic bump: the whole point is that the
    /// caller's prior local state was wrong (a wallet-less client has no reliable
    /// on-chain source for its watermark until settlement), so the bundle —
    /// signer-verified by the node before it was sent, and re-verified against the
    /// client's own key by `resumable_watermark` before it reaches here — is
    /// authoritative. Callers MUST only pass a cumulative sourced from such a
    /// bundle, never a value the caller invented.
    ///
    /// Synchronous: the pipeline is behind a `std::sync::Mutex` (#1484), so unlike
    /// the pre-pipelining ledger this needs no `await`.
    /// Returns `false` — leaving the ledger untouched — if `cum` does not ADVANCE
    /// past the committed watermark. Reseeding is for healing a watermark that has
    /// fallen BEHIND what the node holds; a bundle at or behind `committed` proves
    /// nothing and applying it would REGRESS the watermark, so the retry would
    /// re-sign a spent nonce and strand the channel — the #1122 wedge this ledger
    /// exists to prevent. Guarded here rather than only at the call sites for the
    /// same reason [`Self::resolve_ack`] guards: monotonicity is the ledger's
    /// invariant to keep, not its callers'.
    #[must_use]
    pub fn reseed(&self, cum: Cumulative) -> bool {
        let mut pipeline = self.pipeline();
        if cum.nonce <= pipeline.committed.nonce {
            return false;
        }
        pipeline.committed = cum;
        pipeline.outstanding.clear();
        true
    }

    /// Resolve the oldest outstanding voucher as ACKED: advance the committed
    /// watermark to it and drop it from the in-flight set. Called by the receive loop
    /// when a `VoucherAck` arrives. Acks are FIFO (the node applies vouchers in the
    /// strict nonce order it received them and acks each in turn), so the oldest
    /// outstanding voucher is the one being acked.
    ///
    /// Returns `false` if nothing was outstanding — a spurious ack the caller should
    /// treat as a protocol violation; this never advances `committed`.
    pub fn resolve_ack(&self) -> bool {
        let mut pipeline = self.pipeline();
        match pipeline.outstanding.pop_front() {
            Some(acked) => {
                // Monotonic by construction: the front is the lowest outstanding nonce
                // and every outstanding nonce exceeds `committed`. Guard anyway so a
                // reordering bug can never regress the watermark.
                if acked.nonce > pipeline.committed.nonce {
                    pipeline.committed = acked;
                }
                true
            }
            None => false,
        }
    }

    /// Resolve the oldest outstanding voucher as explicitly REJECTED: drop it from the
    /// in-flight set WITHOUT advancing `committed`. Called by the receive loop when a
    /// `StreamError::VoucherRejected` arrives. A rejection is the upstream declaring it
    /// never took the voucher, so — unlike an ambiguous failure — it must not be
    /// settled optimistically (that would inflate our cumulative for bytes the upstream
    /// refused to be paid for). Returns `false` if nothing was outstanding.
    pub fn resolve_reject(&self) -> bool {
        self.pipeline().outstanding.pop_front().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PullStalled, UpstreamVoucherRejected};
    use decdn_protocol::client::VoucherRejectReason;
    use std::sync::Arc;
    use std::time::Duration;

    /// Issue and immediately ack — the synchronous shape the old blocking loop had,
    /// now expressed as issue-then-`resolve_ack`. Keeps the exact-and-monotonic
    /// guarantee: 50 concurrent issuers produce nonces 1..=50 with no gaps.
    #[tokio::test]
    async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));
        let mut handles = Vec::new();
        for _ in 0..50u32 {
            let l = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                // Fake exchange: yield once (to interleave) then "send". Ack right
                // after so the pipeline never fills — this test is about nonce
                // exactness, not the bound.
                let sent = l
                    .issue(100, 10, |_signed| async {
                        tokio::task::yield_now().await;
                        Ok(())
                    })
                    .await;
                l.resolve_ack();
                sent
            }));
        }

        let mut nonces = Vec::new();
        for h in handles {
            nonces.push(h.await??.nonce);
        }
        nonces.sort_unstable();
        let expected: Vec<U256> = (1..=50u64).map(U256::from).collect();
        assert_eq!(nonces, expected);

        // Every voucher acked ⇒ committed carries all 50, and never more bytes than
        // the 100-per-voucher deltas we actually issued (never pay for unreceived bytes).
        let committed = ledger.committed();
        assert_eq!(committed.nonce, U256::from(50u64));
        assert_eq!(committed.bytes, U256::from(5000u64));
        assert!(ledger.settlement().nonce <= U256::from(50u64));
        Ok(())
    }

    #[tokio::test]
    async fn failed_send_does_not_commit() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        let result = ledger
            .issue(100, 10, |_signed| async { anyhow::bail!("send lost") })
            .await;
        assert!(result.is_err(), "a failed send must surface the error");
        // Committed unmoved: a send that never acked never advances committed.
        assert_eq!(ledger.committed(), Cumulative::default());
        // But it settles HIGH — the send is ambiguous, so the voucher stays armed.
        assert_eq!(ledger.settlement().nonce, U256::from(1u64));
        Ok(())
    }

    /// The window this exists to close (#1122): the upstream persists a voucher and only
    /// THEN acks it (ADR 003), so a pull dropped inside the ack wait leaves the upstream
    /// holding a voucher we have no record of. Settle low and the next reuse re-signs a
    /// spent nonce, the upstream rejects `StaleNonce` (terminal), and the channel is
    /// wedged with its deposit escrowed until expiry.
    ///
    /// `tokio::time::timeout` is the mechanism, not a stand-in for one: it drops the
    /// inner future on elapse, which is exactly what `outer_pull_deadline`, the warm's
    /// hard cap, and the shutdown token each do to a live pull.
    #[tokio::test]
    async fn a_pull_dropped_inside_the_send_settles_at_the_voucher_it_sent() {
        let ledger = ChannelLedger::new(Cumulative::default());
        let dropped = tokio::time::timeout(
            Duration::from_millis(20),
            // The voucher is armed and on the wire; the send future never resolves.
            // This future is dropped mid-await, as production's is.
            ledger.issue(100, 10, |_next| {
                std::future::pending::<anyhow::Result<()>>()
            }),
        )
        .await;
        assert!(dropped.is_err(), "the send must still be in flight");

        // `committed` is untouched — correctly, nothing was acked.
        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "an unacked voucher must never advance the committed watermark"
        );
        // But what we must PERSIST is the voucher we sent: the upstream may well hold it.
        let settled = ledger.settlement();
        assert_eq!(settled.nonce, U256::from(1u64), "settle at the sent nonce");
        assert_eq!(settled.bytes, U256::from(100u64));
        assert_eq!(settled.amount, U256::from(1u64));
    }

    /// A voucher the upstream explicitly REJECTED was never committed upstream, so
    /// settling at it would inflate our cumulative for bytes the upstream refused to be
    /// paid for. `resolve_reject` drops it WITHOUT advancing `committed`, so — with
    /// nothing else outstanding — settlement falls back to committed.
    #[tokio::test]
    async fn a_rejected_voucher_is_not_settled_optimistically() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        // Issue + successful send: the voucher is armed, awaiting its ack.
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        // The upstream rejects it (arrived on the ack slot as VoucherRejected).
        assert!(ledger.resolve_reject(), "the armed voucher is dropped");
        assert_eq!(
            ledger.settlement(),
            Cumulative::default(),
            "an explicitly rejected voucher must not advance what we persist"
        );
        Ok(())
    }

    /// Issue #1481: a wallet-less client cannot reconstruct its watermark from chain, so a
    /// gated `StaleNonce` rejection carries the node's true watermark back in a
    /// `WatermarkBundle`. `Cumulative::from` must decode it losslessly (the boundary values
    /// that would expose a truncation).
    #[test]
    fn cumulative_from_bundle_is_lossless() {
        let bundle = WatermarkBundle {
            amount: U256::MAX.to_be_bytes(),
            nonce: U256::from(7u64).to_be_bytes(),
            bytes_delivered: U256::from(1_048_576u64).to_be_bytes(),
            last_signature: vec![0xABu8; 65],
        };
        let cum = Cumulative::from(&bundle);
        assert_eq!(cum.amount, U256::MAX);
        assert_eq!(cum.nonce, U256::from(7u64));
        assert_eq!(cum.bytes, U256::from(1_048_576u64));
    }

    /// The self-heal itself, in the optimistic/pipelined world (#1484): a `StaleNonce`
    /// rejection with an authenticated bundle is not a dead end. `reseed` overwrites the
    /// committed watermark to the node's true state AND clears the WHOLE outstanding
    /// pipeline — even when several vouchers were optimistically in flight when the reject
    /// landed — so the next `issue` builds on the bundle and signs `bundle.nonce + 1`, not a
    /// collision with what the node already holds and not a repeat of the stale local value.
    ///
    /// The bundle is carried to the caller on the typed `UpstreamVoucherRejected` (see
    /// `resolve_voucher_slot` in `lib.rs`) and authenticated against the client's own key by
    /// `resumable_watermark` before it reaches `reseed`; those steps are exercised by the
    /// `resumable_watermark_*` tests in `lib.rs` and the over-the-wire self-heal test in
    /// `node_origin_pull.rs`. This test isolates the ledger's own job: reseed-clears-all +
    /// next-nonce.
    #[tokio::test]
    async fn a_stale_nonce_rejection_with_a_bundle_self_heals() -> anyhow::Result<()> {
        // The caller's local ledger thinks it is at nonce 1 (e.g. a wallet-less delegate
        // that never persisted the true watermark across a restart), but the node's true
        // watermark — echoed back on the gated reject — is nonce 5.
        let ledger = ChannelLedger::new(Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(1000u64),
            amount: U256::from(10u64),
        });
        let bundle = WatermarkBundle {
            amount: U256::from(50u64).to_be_bytes(),
            nonce: U256::from(5u64).to_be_bytes(),
            bytes_delivered: U256::from(5000u64).to_be_bytes(),
            last_signature: vec![0xCDu8; 65],
        };

        // Two vouchers optimistically in flight when the reject lands (pipelined, #1484):
        // nonces 2 and 3 armed atop the stale local baseline.
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert_eq!(
            ledger.settlement().nonce,
            U256::from(3u64),
            "two vouchers are outstanding before the reject"
        );

        // Self-heal: re-seed to the node's authenticated watermark. reseed clears the whole
        // outstanding pipeline, so settlement drops to the bundle's nonce — proving both
        // armed vouchers were discarded wholesale, not one at a time.
        assert!(
            ledger.reseed(Cumulative::from(&bundle)),
            "a bundle ahead of committed must be applied"
        );
        assert_eq!(
            ledger.settlement().nonce,
            U256::from(5u64),
            "reseed cleared all outstanding and reset to the bundle watermark"
        );

        let mut signed_nonce = None;
        let issued = ledger
            .issue(100, 10, |next: Cumulative| {
                signed_nonce = Some(next.nonce);
                async { Ok(()) }
            })
            .await?;
        assert_eq!(
            signed_nonce,
            Some(U256::from(6u64)),
            "the next voucher must sign bundle.nonce + 1, not the stale local nonce"
        );
        assert_eq!(issued.nonce, U256::from(6u64));
        assert_eq!(issued.bytes, U256::from(5100u64)); // bundle.bytes_delivered + 100
        Ok(())
    }

    /// The monotonicity guard (#1497 review): a bundle that does NOT advance past
    /// the committed watermark must be refused, leaving the ledger untouched.
    ///
    /// This is not a hypothetical. The node attaches an authenticated bundle to
    /// EVERY watermark-gated rejection once any voucher has been accepted —
    /// including a genuinely exhausted channel, where the bundle simply echoes the
    /// watermark the client already holds. Applying it would REGRESS `committed`,
    /// and the retry would then re-sign a nonce the node has already consumed:
    /// the `StaleNonce` wedge of #1122, caused by the client's own self-heal.
    /// Refusing it also lets the caller surface the real rejection instead of
    /// spending its resume budget re-sending refused vouchers.
    #[tokio::test]
    async fn reseed_refuses_a_bundle_that_does_not_advance_the_watermark() -> anyhow::Result<()> {
        let committed = Cumulative {
            nonce: U256::from(5u64),
            bytes: U256::from(5000u64),
            amount: U256::from(50u64),
        };
        let ledger = ChannelLedger::new(committed);
        // One voucher optimistically in flight, so a wrongly-applied reseed would
        // be observable twice over: regressed committed AND a cleared pipeline.
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;

        // The exhausted-channel echo: same nonce we already hold.
        let echo = Cumulative {
            nonce: U256::from(5u64),
            bytes: U256::from(5000u64),
            amount: U256::from(50u64),
        };
        assert!(
            !ledger.reseed(echo),
            "a bundle at the committed watermark proves nothing and must be refused"
        );
        assert_eq!(
            ledger.settlement().nonce,
            U256::from(6u64),
            "the refused reseed must leave the outstanding pipeline intact"
        );

        // And the strictly-behind case: a stale bundle must not rewind us either.
        let behind = Cumulative {
            nonce: U256::from(2u64),
            bytes: U256::from(2000u64),
            amount: U256::from(20u64),
        };
        assert!(
            !ledger.reseed(behind),
            "a bundle behind committed must be refused"
        );
        assert_eq!(
            ledger.committed().nonce,
            U256::from(5u64),
            "committed must never regress"
        );
        Ok(())
    }

    /// The other half: `InsufficientDeposit` with NO bundle (the node either didn't verify
    /// the signer or there is genuinely nothing to resume from) must not be treated as
    /// self-healable — a caller checking `bundle.is_none()` sees exactly the same "give up
    /// and surface to the app" signal it always did. This is the guard against silently
    /// looping on a channel a wallet-less delegate has no way to top up.
    ///
    /// In the optimistic loop (#1484) a `VoucherRejected` no longer surfaces through
    /// `issue`'s send closure (that path is send-only now, and a closure error is an
    /// *ambiguous* send failure, not a rejection); it arrives on the `VoucherAck` slot and
    /// is typed as `UpstreamVoucherRejected` by `resolve_voucher_slot`. This test therefore
    /// asserts the type-shape contract the buffered resume path (`resumable_watermark`)
    /// depends on directly.
    #[test]
    fn insufficient_deposit_without_a_bundle_is_not_self_healable() -> anyhow::Result<()> {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: None,
        });
        let upstream = err
            .downcast_ref::<UpstreamVoucherRejected>()
            .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
        assert_eq!(upstream.reason, VoucherRejectReason::InsufficientDeposit);
        assert!(
            upstream.bundle.is_none(),
            "no bundle means no self-heal path — the caller must surface a top-up need"
        );
        Ok(())
    }

    /// The load-bearing case (#1145 review): an AMBIGUOUS ack failure — a stall timeout,
    /// a transport reset — is not a rejection. The upstream persists before it acks (ADR
    /// 003), so it may already hold the voucher; settling low here re-signs a spent nonce
    /// on the next reuse and wedges the channel. So an ambiguous failure must leave the
    /// voucher ARMED and settle HIGH, exactly as a drop does.
    ///
    /// In the optimistic loop an ambiguous failure surfaces either as a send error
    /// (below) or as the receive loop giving up without ever calling `resolve_ack` — in
    /// both cases the voucher is never disarmed, so `settlement` reports it.
    #[tokio::test]
    async fn an_ambiguous_failure_settles_high() {
        for make_err in [
            || {
                anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                })
            },
            || anyhow::anyhow!("connection reset by peer"),
        ] {
            let ledger = ChannelLedger::new(Cumulative::default());
            let result = ledger
                .issue(100, 10, move |_next| async move { Err(make_err()) })
                .await;
            assert!(result.is_err(), "the ambiguous send must surface its error");
            assert_eq!(ledger.committed(), Cumulative::default());
            let settled = ledger.settlement();
            assert_eq!(settled.nonce, U256::from(1u64), "settle at the sent nonce");
            assert_eq!(settled.bytes, U256::from(100u64));
        }
    }

    /// The shared-ledger corollary: after a voucher is armed, the NEXT issue on the same
    /// ledger must build on it (sign N+1), not re-sign N — which the upstream may already
    /// hold. `issue` computes from the outstanding frontier, so an armed voucher advances
    /// the basis.
    #[tokio::test]
    async fn a_later_issue_builds_on_an_armed_voucher() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        // First voucher: sent, armed, but its send failed ambiguously (stays armed).
        let stalled = ledger
            .issue(100, 10, |_next| async {
                Err(anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                }))
            })
            .await;
        assert!(stalled.is_err());
        // Second issue must not re-sign nonce 1.
        let mut signed_nonce = None;
        let sent = ledger
            .issue(100, 10, |next: Cumulative| {
                signed_nonce = Some(next.nonce);
                async { Ok(()) }
            })
            .await?;
        assert_eq!(
            signed_nonce,
            Some(U256::from(2u64)),
            "the next voucher must build on the armed nonce, not collide with it"
        );
        assert_eq!(sent.nonce, U256::from(2u64));
        Ok(())
    }

    /// A resolved (acked) voucher leaves nothing in flight — settlement is exactly the
    /// committed watermark, no double-count.
    #[tokio::test]
    async fn a_resolved_exchange_leaves_nothing_in_flight() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert!(ledger.resolve_ack());
        assert_eq!(
            ledger.settlement(),
            ledger.committed(),
            "with nothing on the wire, settlement is exactly the committed watermark"
        );
        assert_eq!(ledger.settlement().nonce, U256::from(1u64));
        Ok(())
    }

    // ---- pipelined (#1484): several vouchers outstanding at once ----

    /// Three vouchers pipelined (sent, none acked yet): the nonces are strictly 1,2,3,
    /// each builds on the last, and `settlement` reports the HIGHEST (settle high across
    /// the whole outstanding set). The client has paid for exactly the delivered deltas.
    #[tokio::test]
    async fn pipelined_issues_are_ordered_and_settle_high() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        for expected in 1..=3u64 {
            let sent = ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
            assert_eq!(sent.nonce, U256::from(expected));
        }
        // None acked ⇒ committed still default, settlement at the highest (nonce 3).
        assert_eq!(ledger.committed(), Cumulative::default());
        let settled = ledger.settlement();
        assert_eq!(
            settled.nonce,
            U256::from(3u64),
            "settle high across the set"
        );
        assert_eq!(settled.bytes, U256::from(300u64), "3 × 100 delivered bytes");
        Ok(())
    }

    /// Acks are FIFO: with three outstanding, each `resolve_ack` advances `committed` by
    /// one nonce from the front while `settlement` stays pinned to the highest armed —
    /// the drop-safe watermark never regresses as acks trickle in.
    #[tokio::test]
    async fn pipelined_acks_advance_committed_fifo() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        for _ in 0..3u64 {
            ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        }
        // Ack 1: committed → nonce 1, settlement still at the highest armed (3).
        assert!(ledger.resolve_ack());
        assert_eq!(ledger.committed().nonce, U256::from(1u64));
        assert_eq!(ledger.settlement().nonce, U256::from(3u64));
        // Ack 2.
        assert!(ledger.resolve_ack());
        assert_eq!(ledger.committed().nonce, U256::from(2u64));
        assert_eq!(ledger.settlement().nonce, U256::from(3u64));
        // Ack 3: pipeline drained, settlement collapses onto committed.
        assert!(ledger.resolve_ack());
        assert_eq!(ledger.committed().nonce, U256::from(3u64));
        assert_eq!(ledger.settlement(), ledger.committed());
        // A further ack is spurious — nothing outstanding.
        assert!(!ledger.resolve_ack(), "spurious ack must report false");
        Ok(())
    }

    /// Partial drain then an ambiguous drop: with vouchers 1,2 acked and 3,4 still on the
    /// wire, a drop must settle at nonce 4 (the highest armed), never regress to the
    /// acked 2. This is the pipelined shape of the #1122 settle-high guarantee.
    #[tokio::test]
    async fn pipelined_ambiguous_drop_settles_at_highest_armed() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        for _ in 0..4u64 {
            ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        }
        assert!(ledger.resolve_ack()); // nonce 1
        assert!(ledger.resolve_ack()); // nonce 2
        // 3 and 4 remain armed; the pull is dropped without their acks.
        assert_eq!(ledger.committed().nonce, U256::from(2u64));
        let settled = ledger.settlement();
        assert_eq!(
            settled.nonce,
            U256::from(4u64),
            "settle at the highest armed"
        );
        assert_eq!(settled.bytes, U256::from(400u64));
        Ok(())
    }

    /// A rejection with LATER vouchers still outstanding: `resolve_reject` drops only the
    /// front (the rejected one, known-not-taken); the later armed vouchers remain and
    /// settlement still settles high on them (they are ambiguous, not rejected).
    #[tokio::test]
    async fn reject_of_front_keeps_later_armed_vouchers() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        for _ in 0..3u64 {
            ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        }
        // Front (nonce 1) rejected; 2 and 3 remain ambiguous.
        assert!(ledger.resolve_reject());
        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "reject never commits"
        );
        assert_eq!(
            ledger.settlement().nonce,
            U256::from(3u64),
            "later armed vouchers still settle high"
        );
        Ok(())
    }

    /// A deep pipeline is fine: the outstanding set has no fixed ceiling (it is bounded
    /// only by the blob's interval count via `max_blob_size`), so many vouchers can be
    /// armed at once and each still builds on the last in strict nonce order. The client
    /// has paid for exactly the delivered deltas, never ahead of them.
    #[tokio::test]
    async fn a_deep_pipeline_stays_ordered_and_exact() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        for expected in 1..=100u64 {
            let sent = ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
            assert_eq!(sent.nonce, U256::from(expected));
        }
        let settled = ledger.settlement();
        assert_eq!(
            settled.nonce,
            U256::from(100u64),
            "settle high across the set"
        );
        assert_eq!(
            settled.bytes,
            U256::from(10_000u64),
            "100 × 100 delivered bytes — never more than was issued"
        );
        Ok(())
    }

    #[test]
    fn nonce_increments_by_one() {
        let cur = Cumulative {
            nonce: U256::from(5u64),
            bytes: U256::ZERO,
            amount: U256::ZERO,
        };
        assert_eq!(next_voucher(&cur, 0, 10).nonce, U256::from(6u64));
    }

    #[test]
    fn bytes_accumulate_by_delta() {
        let cur = Cumulative {
            nonce: U256::ZERO,
            bytes: U256::from(1000u64),
            amount: U256::ZERO,
        };
        assert_eq!(next_voucher(&cur, 500, 10).bytes, U256::from(1500u64));
    }

    #[test]
    fn amount_rounds_up_per_voucher() {
        // 1 byte at rate 10/MiB rounds up to 1 (not 0).
        let cur = Cumulative::default();
        assert_eq!(next_voucher(&cur, 1, 10).amount, U256::from(1u64));
        // The largest sub-MiB delta still rounds up to a full MiB's cost.
        assert_eq!(
            next_voucher(&cur, MB_BYTES - 1, 10).amount,
            U256::from(10u64)
        );
        // A full MiB at rate 10 costs exactly 10.
        assert_eq!(next_voucher(&cur, MB_BYTES, 10).amount, U256::from(10u64));
    }

    #[test]
    fn zero_delta_only_bumps_nonce() {
        let cur = Cumulative {
            nonce: U256::from(2u64),
            bytes: U256::from(7u64),
            amount: U256::from(3u64),
        };
        let next = next_voucher(&cur, 0, 99);
        assert_eq!(next.nonce, U256::from(3u64));
        assert_eq!(next.bytes, U256::from(7u64));
        assert_eq!(next.amount, U256::from(3u64));
    }
}
