//! Per-channel voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`ChannelLedger`] serializes voucher issuance across concurrent streams on one
//! payment channel by holding its lock across each voucher's send→ack.

use std::future::Future;

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;
use tokio::sync::Mutex;

use crate::UpstreamVoucherRejected;

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

/// One channel's live voucher ledger, shared by every concurrent stream on that
/// channel. Voucher issuance is serialized through the inner mutex: the lock is
/// held across the whole compute → sign → send → await-ack → commit cycle, so
/// vouchers reach the node in strict nonce order even while byte transfers run
/// in parallel on other streams.
#[derive(Debug)]
pub struct ChannelLedger {
    cumulative: Mutex<Cumulative>,
    /// A synchronously-readable mirror of the committed watermark, written under
    /// the issuance lock in [`Self::issue`] at the same instant `cumulative` is
    /// committed. The two never disagree about a committed voucher.
    ///
    /// It exists because a pull can end by being DROPPED, not only by returning,
    /// and the watermark is the record of money already spent (#1145 review). The
    /// buffered node pull runs with `hard_cap: None` (#1134), so what ends a
    /// slow-but-progressing transfer is always external — the foreground
    /// `outer_pull_deadline`, the background warm's hard cap, or the shutdown
    /// token — and all three DROP the future. The only thing that runs on that
    /// path is `Drop`, which cannot await, so [`Self::snapshot`] is unreachable
    /// there and a `tokio::sync::Mutex` is the wrong instrument. Hence a
    /// `std::sync::Mutex` a `Drop` impl can actually read.
    ///
    /// Held for a few instructions at a time and never across an await, so it
    /// cannot deadlock with the issuance lock above.
    committed: std::sync::Mutex<Cumulative>,
    /// The voucher currently ON THE WIRE, whose fate we do not know: written under
    /// the issuance lock *before* `exchange`, cleared after it resolves EITHER way.
    ///
    /// `committed` alone is not enough, because the losing window is not the one it
    /// closes. The upstream persists a voucher and only THEN writes `VoucherAck`
    /// (ADR 003; `handlers::client` acks after `apply_voucher`). So a pull that loses
    /// this voucher *inside* the ack wait — the longest await in the streaming loop,
    /// and exactly where a slow peer trips the caller's deadline — leaves the upstream
    /// holding nonce N while `committed` still says N-1. Settle low and the next reuse
    /// re-signs N, the upstream rejects `StaleNonce` (now terminal), and the channel is
    /// wedged with its deposit escrowed until expiry (the desync noted in `self_pay`
    /// and tracked in #1122).
    ///
    /// A voucher is left armed here whenever its fate is UNKNOWN — not only a DROP.
    /// `issue` clears it on the two outcomes that resolve the fate: an `Ok` ack (the
    /// upstream took it; `committed` carries it) and a typed [`UpstreamVoucherRejected`]
    /// (the upstream explicitly declined it, so it was never persisted). EVERY other
    /// resolved error — a stall timeout, a transport reset, a protocol violation — is
    /// as ambiguous as a drop: the upstream may already hold the voucher, so the slot
    /// stays armed and [`Self::settlement`] returns it. Clearing it on those errors is
    /// exactly the settle-low bug above (#1145 review).
    in_flight: std::sync::Mutex<Option<Cumulative>>,
}

impl ChannelLedger {
    /// Build a ledger seeded from the channel's persisted cumulative state (the
    /// last voucher acked on earlier streams/invocations). Pass `Cumulative::default()`
    /// for a brand-new channel.
    #[must_use]
    pub fn new(seed: Cumulative) -> Self {
        Self {
            cumulative: Mutex::new(seed),
            committed: std::sync::Mutex::new(seed),
            in_flight: std::sync::Mutex::new(None),
        }
    }

    /// Read the current cumulative (for persistence after a pull completes).
    pub async fn snapshot(&self) -> Cumulative {
        *self.cumulative.lock().await
    }

    /// Read the committed cumulative WITHOUT awaiting — the drop-safe counterpart
    /// of [`Self::snapshot`], and the only one a `Drop` impl can call.
    ///
    /// A poisoned lock means a prior holder panicked mid-write. Recover the inner
    /// value and return it rather than propagating: the caller is a persist-what-we-
    /// paid path, and refusing to read here would throw away the very watermark the
    /// mirror exists to save.
    #[must_use]
    pub fn committed(&self) -> Cumulative {
        *self
            .committed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The cumulative a cancelled pull must PERSIST — the drop-path counterpart of
    /// [`Self::committed`], and what a `Drop` guard should actually write (#1122).
    ///
    /// It is [`Self::committed`], except when a voucher was on the wire with its fate
    /// unresolved, in which case it is that voucher. Settling high is the safe
    /// direction, and it is not merely safe but *correct*:
    ///
    /// - The in-flight voucher pays for bytes the upstream ALREADY DELIVERED to us —
    ///   that is why it was issued. Honouring it is paying what we owe.
    /// - If the upstream did commit it (the likely case: it persists before it acks),
    ///   settling low re-signs a spent nonce and wedges the channel permanently.
    /// - If it did not, we have merely skipped a nonce. The serve side tolerates a
    ///   nonce GAP (it meters `voucher_nonce_gap` and accepts); it does not tolerate a
    ///   regression. The two errors are not symmetric, so we take the survivable one.
    ///
    /// A voucher the upstream explicitly REJECTED is never in flight here — `issue`
    /// clears it on a typed [`UpstreamVoucherRejected`] — so this cannot inflate our
    /// cumulative for bytes the upstream declined to be paid for. An ambiguous failure
    /// (a stall, a reset) is the opposite: it stays armed, because the upstream may
    /// hold it and settling low would strand the deposit.
    #[must_use]
    pub fn settlement(&self) -> Cumulative {
        let committed = self.committed();
        match *self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            // `>` on the nonce, not `!=`: the only way a resolved-then-superseded value
            // could linger is a bug, and this way it cannot regress the watermark.
            Some(in_flight) if in_flight.nonce > committed.nonce => in_flight,
            _ => committed,
        }
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher.
    ///
    /// Holds the channel lock across `exchange`, which performs the actual I/O
    /// (write the signed voucher, await the node's ack). The committed watermark
    /// advances *only* on a successful ack; a rejection or an ambiguous failure leaves
    /// it unmoved. But an ambiguous failure (unlike an explicit rejection) leaves the
    /// voucher ARMED, so [`Self::settlement`] still reports it — see the `in_flight`
    /// field docs. Returns the committed cumulative.
    ///
    /// `exchange` receives the next [`Cumulative`] (the values the caller must
    /// sign and send); the caller owns signing + framing so this module stays
    /// free of EIP-712 / wire types.
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
        let mut guard = self.cumulative.lock().await;
        // Build on the SETTLEMENT watermark, not the committed one. If a prior concurrent
        // pull on this shared ledger left a voucher armed (an ambiguous failure it settled
        // high), the next voucher must exceed THAT nonce, or we re-sign one the upstream may
        // already hold and collide on `StaleNonce`. A nonce GAP is survivable upstream (it
        // meters `voucher_nonce_gap` and accepts); a regression is not — see `settlement`.
        let next = next_voucher(&self.settlement(), delta_bytes, rate_per_mb);
        // Arm the in-flight slot BEFORE the voucher goes out, so that if this future is
        // dropped inside `exchange` — after the upstream committed, before we read its
        // ack — `settlement()` still knows what we owe. See the field's docs.
        self.set_in_flight(Some(next));
        let outcome = exchange(next).await;
        // Disarm ONLY when the voucher's fate is known: an `Ok` ack (`committed` is about to
        // carry it) or a typed `UpstreamVoucherRejected` (the upstream declared it never took
        // it). Every OTHER resolved error — a stall timeout, a transport reset, a protocol
        // violation — is as ambiguous as a drop (the upstream persists before it acks, ADR
        // 003), so the slot stays armed and `settlement()` settles high. Clearing it here was
        // the settle-low bug (#1145 review): a stalled ack looked exactly like a drop but took
        // this line and stranded the deposit.
        let fate_known = match &outcome {
            Ok(()) => true,
            Err(err) => err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
        };
        if fate_known {
            self.set_in_flight(None);
        }
        outcome?;
        *guard = next;
        // Mirror the commit while still holding the issuance lock, so a reader that
        // takes only the sync lock can never observe a voucher as committed here and
        // not there (or the reverse). `exchange` has returned, so the upstream acked —
        // and ADR 003 has it commit before it acks. The money is spent; from this line
        // on, a drop of the pull cannot lose it.
        *self
            .committed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        Ok(next)
    }

    /// Set the in-flight slot. Poison-tolerant for the same reason [`Self::committed`]
    /// is: the value guards money, and refusing to write it would lose the record.
    fn set_in_flight(&self, value: Option<Cumulative>) {
        *self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PullStalled;
    use decdn_protocol::client::VoucherRejectReason;
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
        use std::sync::Arc;

        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));
        let mut handles = Vec::new();
        // 50 concurrent issuers, each paying for 100 bytes at rate 10.
        for _ in 0..50u32 {
            let l = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                // Fake exchange: yield once (to interleave) then "ack".
                l.issue(100, 10, |_signed| async {
                    tokio::task::yield_now().await;
                    Ok(())
                })
                .await
            }));
        }

        let mut nonces = Vec::new();
        for h in handles {
            nonces.push(h.await??.nonce);
        }
        nonces.sort_unstable();
        // Nonces are exactly 1..=50, no gaps, no duplicates.
        let expected: Vec<U256> = (1..=50u64).map(U256::from).collect();
        assert_eq!(nonces, expected);

        let final_cum = ledger.snapshot().await;
        assert_eq!(final_cum.nonce, U256::from(50u64));
        assert_eq!(final_cum.bytes, U256::from(5000u64)); // 50 * 100
        Ok(())
    }

    #[tokio::test]
    async fn failed_exchange_does_not_commit() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        let result = ledger
            .issue(100, 10, |_signed| async { anyhow::bail!("ack lost") })
            .await;
        assert!(result.is_err(), "a failed exchange must surface the error");
        // Watermark unmoved: a signed-but-unacked voucher never advances state.
        assert_eq!(ledger.snapshot().await, Cumulative::default());
        Ok(())
    }

    /// The window this exists to close (#1122): the upstream persists a voucher and only
    /// THEN acks it (ADR 003), so a pull dropped inside the ack wait leaves the upstream
    /// holding a voucher we have no record of. Settle low and the next reuse re-signs a
    /// spent nonce, the upstream rejects `StaleNonce` (a terminal verdict), and the channel
    /// is wedged with its deposit escrowed until expiry.
    ///
    /// `tokio::time::timeout` is the mechanism, not a stand-in for one: it drops the
    /// inner future on elapse, which is exactly what `outer_pull_deadline`, the warm's
    /// hard cap, and the shutdown token each do to a live pull.
    #[tokio::test]
    async fn a_pull_dropped_inside_the_ack_wait_settles_at_the_voucher_it_sent() {
        let ledger = ChannelLedger::new(Cumulative::default());
        let dropped = tokio::time::timeout(
            Duration::from_millis(20),
            // The voucher is on the wire and the upstream has committed it; the ack
            // never comes back. This future is dropped mid-await, as production's is.
            ledger.issue(100, 10, |_next| {
                std::future::pending::<anyhow::Result<()>>()
            }),
        )
        .await;
        assert!(dropped.is_err(), "the exchange must still be in flight");

        // `committed` is untouched — correctly, nothing was acked.
        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "an unacked voucher must never advance the committed watermark"
        );
        // But what we must PERSIST is the voucher we sent: the upstream may well hold it,
        // and it pays for bytes already delivered to us either way.
        let settled = ledger.settlement();
        assert_eq!(settled.nonce, U256::from(1u64), "settle at the sent nonce");
        assert_eq!(settled.bytes, U256::from(100u64));
        assert_eq!(settled.amount, U256::from(1u64));
    }

    /// The other half, and the reason `settlement` cannot simply be "always the highest
    /// voucher we built": an EXPLICITLY REJECTED voucher was never committed upstream, so
    /// settling at it would inflate our cumulative for bytes the upstream refused to be paid
    /// for. Only the rejection is known-uncommitted — so the discriminator is the TYPED
    /// `UpstreamVoucherRejected`, not any error (an ambiguous stall settles high; see below).
    #[tokio::test]
    async fn a_rejected_voucher_is_not_settled_optimistically() {
        let ledger = ChannelLedger::new(Cumulative::default());
        let result = ledger
            .issue(100, 10, |_next| async {
                Err(anyhow::Error::new(UpstreamVoucherRejected {
                    reason: VoucherRejectReason::StaleNonce,
                }))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(
            ledger.settlement(),
            Cumulative::default(),
            "an explicitly rejected voucher must not advance what we persist"
        );
    }

    /// The load-bearing case (#1145 review): an AMBIGUOUS ack failure — a stall timeout, a
    /// transport reset — is not a rejection. The upstream persists before it acks (ADR 003),
    /// so it may already hold the voucher; settling low here re-signs a spent nonce on the
    /// next reuse and wedges the channel. So an ambiguous failure must settle HIGH, exactly
    /// as a drop does. A `PullStalled` is the production trigger: `self_pay` wraps the ack
    /// read in `tokio::time::timeout(stall, …)`, which RESOLVES the exchange as
    /// `Err(PullStalled)` rather than dropping it.
    ///
    /// This is the test that fails on revert: restore the unconditional `set_in_flight(None)`
    /// and the settlement drops back to `default`.
    #[tokio::test]
    async fn an_ambiguous_ack_failure_settles_high() {
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
            assert!(
                result.is_err(),
                "the ambiguous exchange must surface its error"
            );
            // `committed` is untouched — nothing was acked.
            assert_eq!(ledger.committed(), Cumulative::default());
            // But settlement is the voucher we sent: the upstream may hold it.
            let settled = ledger.settlement();
            assert_eq!(settled.nonce, U256::from(1u64), "settle at the sent nonce");
            assert_eq!(settled.bytes, U256::from(100u64));
        }
    }

    /// The shared-ledger corollary: after an ambiguous failure arms nonce N, the NEXT issue
    /// on the same ledger must sign N+1, not re-sign N (which the upstream may already hold).
    /// `issue` computes the next voucher from `settlement()`, so the armed voucher advances
    /// the basis. Reverting that to `next_voucher(&guard, …)` re-signs N here.
    #[tokio::test]
    async fn a_later_issue_builds_on_an_armed_voucher() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        // First pull stalls: arms nonce 1, settles high.
        let stalled = ledger
            .issue(100, 10, |_next| async {
                Err(anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                }))
            })
            .await;
        assert!(stalled.is_err());
        // Second pull on the same shared ledger must not re-sign nonce 1.
        let mut signed_nonce = None;
        let committed = ledger
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
        assert_eq!(committed.nonce, U256::from(2u64));
        Ok(())
    }

    /// A drop AFTER the ack settles at the acked voucher and no higher — the in-flight
    /// slot must not linger and double-count once the exchange has resolved.
    #[tokio::test]
    async fn a_resolved_exchange_leaves_nothing_in_flight() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert_eq!(
            ledger.settlement(),
            ledger.committed(),
            "with nothing on the wire, settlement is exactly the committed watermark"
        );
        assert_eq!(ledger.settlement().nonce, U256::from(1u64));
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
