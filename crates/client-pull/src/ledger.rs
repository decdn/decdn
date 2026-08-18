//! Per-lane voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`PoolLedger`] serializes voucher *issuance* across concurrent streams that
//! draw on one pool lane. Continued delivery is acceptance (ADR 003/005): a sent
//! voucher commits optimistically — the send itself is the commit — and only an
//! explicit rejection rewinds it. There is no ack to wait for, so the issuance
//! lock spans compute → sign → send and nothing more, and parallel streams to
//! one provider never serialize their payments behind each other's round trips.

use std::future::Future;

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;
use decdn_protocol::client::WatermarkBundle;
use tokio::sync::Mutex;

/// A lane's cumulative voucher state: the absolute totals carried by the most
/// recent voucher. Both advance monotonically over the lane's lifetime. There
/// is no nonce — `amount` is the sole monotone ordering and replay key (ADR 005
/// §Voucher wire format).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cumulative {
    /// Cumulative lane bytes paid for.
    pub bytes: U256,
    /// Cumulative lane amount paid (token base units).
    pub amount: U256,
}

impl From<&WatermarkBundle> for Cumulative {
    /// Decode a wallet-less resume bundle's `u64` totals (issue #1481) into
    /// the same shape [`PoolLedger`] tracks. Infallible — `U256::from`
    /// cannot fail on a `u64` — so a caller can re-seed directly from a
    /// bundle without a `Result`.
    fn from(bundle: &WatermarkBundle) -> Self {
        Self {
            bytes: U256::from(bundle.bytes_delivered),
            amount: U256::from(bundle.amount),
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
        bytes: cur.bytes.saturating_add(U256::from(delta_bytes)),
        amount: cur.amount.saturating_add(amount_delta),
    }
}

/// The committed watermark plus the one-step rewind and the ambiguous in-flight
/// voucher, under a single lock so a reader never catches a half-applied update.
/// All three are read from a `Drop` impl, so the lock is a `std::sync::Mutex` —
/// see [`PoolLedger::committed`].
#[derive(Debug)]
struct Pipeline {
    /// The highest voucher whose send SUCCEEDED — presumed accepted, because
    /// continued delivery IS acceptance (ADR 005: only rejection is signalled).
    /// Advanced optimistically on each successful [`PoolLedger::issue`], rewound
    /// one step by [`PoolLedger::resolve_reject`], and hard-set by
    /// [`PoolLedger::reseed`].
    committed: Cumulative,
    /// The committed watermark BEFORE the most-recent successful voucher, so a
    /// later [`PoolLedger::resolve_reject`] can un-commit exactly that voucher —
    /// the one the node declared it never took. A reject terminates the stream
    /// (`handlers/client/wire.rs::write_reject`), and the self-heal path reseeds
    /// from the authenticated bundle immediately after, so a single-step rewind
    /// covers what the settlement window needs.
    prev: Option<Cumulative>,
    /// A voucher ARMED for send whose send did NOT confirm — an ambiguous error,
    /// or a future dropped mid-await. `committed` never advanced to it, but
    /// [`PoolLedger::settlement`] reports it (settle high — the upstream persists
    /// before it would reject, ADR 003), so a pull dropped inside the send still
    /// persists what the upstream may hold. Cleared on the next successful
    /// [`PoolLedger::issue`] or a [`PoolLedger::reseed`].
    armed: Option<Cumulative>,
}

/// One lane's live voucher ledger, shared by every concurrent stream that draws
/// on it. Voucher *issuance* is serialized through the async `issuance` mutex —
/// held across compute → sign → send so vouchers reach the node in strict
/// cumulative order — and released the instant the send returns, because there
/// is no ack to wait for (implicit acceptance, ADR 005). The committed watermark
/// advances the moment a send succeeds; a mid-stream `VoucherRejected` rewinds
/// it through [`Self::resolve_reject`].
#[derive(Debug)]
pub struct PoolLedger {
    /// Serializes issuance (compute the next cumulative → sign → send). Its
    /// guarded value is `()`: the ordering it enforces lives in `pipeline`, and
    /// this is only the token that makes the compute-and-send critical section
    /// mutually exclusive across concurrent issuers. Held across an `await` (the
    /// send), so it is a `tokio::sync::Mutex`, never the sync one below.
    issuance: Mutex<()>,
    /// The committed watermark + rewind + armed voucher. A `std::sync::Mutex`
    /// because it is read from `Drop` (which cannot await): a pull can end by
    /// being DROPPED, not only by returning, and this is the record of money
    /// already spent (#1145 review). Held for a few instructions at a time and
    /// never across an await, so it cannot deadlock with the issuance lock.
    pipeline: std::sync::Mutex<Pipeline>,
}

impl PoolLedger {
    /// Build a ledger seeded from the lane's persisted cumulative state (the
    /// last voucher issued on earlier streams/invocations). Pass
    /// `Cumulative::default()` for a brand-new lane.
    #[must_use]
    pub fn new(seed: Cumulative) -> Self {
        Self {
            issuance: Mutex::new(()),
            pipeline: std::sync::Mutex::new(Pipeline {
                committed: seed,
                prev: None,
                armed: None,
            }),
        }
    }

    /// Lock the pipeline, recovering the inner value on poison. A poisoned lock
    /// means a prior holder panicked mid-write; the guarded value is money, so
    /// recover it rather than propagate — refusing to read here would throw away
    /// the very watermark the mirror exists to save.
    fn pipeline(&self) -> std::sync::MutexGuard<'_, Pipeline> {
        self.pipeline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Read the committed cumulative WITHOUT awaiting — the drop-safe reader, and
    /// the only one a `Drop` impl can call. It is the highest voucher whose send
    /// succeeded (implicit acceptance), so it is exactly what a *completed*
    /// buffered pull persists.
    #[must_use]
    pub fn committed(&self) -> Cumulative {
        self.pipeline().committed
    }

    /// The cumulative a cancelled pull must PERSIST — the drop-path counterpart of
    /// [`Self::committed`], and what a `Drop` guard should actually write (#1122).
    ///
    /// It is [`Self::committed`], except when a voucher is still armed with its
    /// send unconfirmed, in which case it is the HIGHER of the two by `amount`
    /// (the monotone key). Settling high is the safe direction and it is correct:
    /// an armed voucher pays for bytes the upstream ALREADY DELIVERED, and the
    /// upstream persists a voucher before it would reject it, so an ambiguous
    /// failure may leave the upstream holding it — under-reporting would strand
    /// the deposit. A voucher the upstream explicitly REJECTED is never here —
    /// [`Self::resolve_reject`] clears `armed` and rewinds `committed` — so this
    /// cannot inflate our cumulative for bytes the upstream declined.
    #[must_use]
    pub fn settlement(&self) -> Cumulative {
        let pipeline = self.pipeline();
        match pipeline.armed {
            Some(armed) if armed.amount > pipeline.committed.amount => armed,
            _ => pipeline.committed,
        }
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher
    /// and SEND it. The send IS the commit: on a successful send the committed
    /// watermark advances to the new voucher (continued delivery is acceptance,
    /// ADR 005), and there is no ack to wait for.
    ///
    /// Holds the issuance lock across compute → arm → `exchange`, so concurrent
    /// issuers on the shared ledger sign strictly increasing cumulatives. The
    /// voucher is ARMED (recorded as the in-flight candidate) *before* `exchange`,
    /// so a future dropped inside the send still has [`Self::settlement`] report
    /// what we owe. An `exchange` error leaves the voucher armed (its fate is
    /// ambiguous: a partial write may have reached the upstream), so `settlement`
    /// settles high while `committed` does not advance.
    ///
    /// `exchange` receives the next [`Cumulative`] (the values to sign and send);
    /// the caller owns signing + framing so this module stays free of EIP-712 /
    /// wire types.
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
        // Serialize issuance. Held across the send below.
        let _issuing = self.issuance.lock().await;
        // Compute the next voucher from the current frontier (highest of the
        // committed watermark and any armed voucher) and arm it, all under one
        // pipeline lock so the basis we build on cannot shift between reading it
        // and recording it. Build on the SETTLEMENT frontier, not `committed`
        // alone: a voucher already on the wire must be exceeded, or we re-sign a
        // cumulative the upstream may hold.
        let next = {
            let mut pipeline = self.pipeline();
            let frontier = match pipeline.armed {
                Some(armed) if armed.amount > pipeline.committed.amount => armed,
                _ => pipeline.committed,
            };
            let next = next_voucher(&frontier, delta_bytes, rate_per_mb);
            pipeline.armed = Some(next);
            next
        };
        // Send. On ANY error the voucher stays armed and `committed` does not
        // advance: a send failure is as ambiguous as a drop, so `settlement`
        // settles high. A rejection is NOT an issuance outcome — it arrives later
        // as a `StreamError` message and is disarmed via `resolve_reject`.
        exchange(next).await?;
        // The send succeeded: commit optimistically. Remember the prior committed
        // watermark so a later `resolve_reject` can un-commit exactly this voucher.
        {
            let mut pipeline = self.pipeline();
            pipeline.prev = Some(pipeline.committed);
            pipeline.committed = next;
            pipeline.armed = None;
        }
        Ok(next)
    }

    /// Wallet-less self-heal (issue #1481): overwrite the committed watermark to
    /// `cum` — typically [`Cumulative::from`] a [`WatermarkBundle`] the node
    /// attached to a gated `AmountRegression` / `BytesRegression` / `SpendingCapExhausted`
    /// rejection — and clear the rewind + armed state, since the node has just
    /// told us its authoritative watermark. The next [`Self::issue`] builds on
    /// `cum`, matching what the node will accept next.
    ///
    /// This is a hard overwrite, not a monotonic bump: the caller's prior local
    /// state was wrong (a wallet-less client has no reliable on-chain source for
    /// its watermark until settlement), so the bundle — signer-verified by the
    /// node before it was sent, and re-verified against the client's own key by
    /// `resumable_watermark` before it reaches here — is authoritative. Callers
    /// MUST only pass a cumulative sourced from such a bundle.
    ///
    /// Returns `false` — leaving the ledger untouched — if `cum` does not ADVANCE
    /// past the committed watermark's `amount`. Reseeding heals a watermark that
    /// has fallen BEHIND what the node holds; a bundle at or behind `committed`
    /// proves nothing, and applying it would REGRESS the watermark and re-sign a
    /// spent amount. Guarded here rather than only at the call sites because
    /// monotonicity is the ledger's invariant to keep.
    #[must_use]
    pub fn reseed(&self, cum: Cumulative) -> bool {
        let mut pipeline = self.pipeline();
        if cum.amount <= pipeline.committed.amount {
            return false;
        }
        pipeline.committed = cum;
        pipeline.prev = None;
        pipeline.armed = None;
        true
    }

    /// Resolve the most-recent presumed-accepted voucher as explicitly REJECTED:
    /// clear any armed voucher and rewind `committed` to the state before it,
    /// WITHOUT settling that voucher. Called by the receive loop when a
    /// `StreamError::VoucherRejected` arrives. A rejection is the upstream
    /// declaring it never took the voucher, so — unlike an ambiguous failure — it
    /// must not be settled optimistically (that would inflate our cumulative for
    /// bytes the upstream refused to be paid for).
    ///
    /// Returns `false` if there was nothing to rewind — a spurious rejection.
    ///
    /// Assumes the rejected voucher is the LATEST committed one: the one-step
    /// `committed → prev` rewind is exact for the inline-await receive loop, which
    /// issues at most one armed voucher at a time and learns of its rejection before
    /// issuing the next.
    pub fn resolve_reject(&self) -> bool {
        let mut pipeline = self.pipeline();
        pipeline.armed = None;
        match pipeline.prev.take() {
            Some(prev) => {
                pipeline.committed = prev;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PullStalled, UpstreamVoucherRejected};
    use decdn_protocol::client::VoucherRejectReason;
    use std::sync::Arc;
    use std::time::Duration;

    /// 50 concurrent issuers on one ledger sign strictly increasing cumulatives
    /// with no gaps: each successful send commits, so the committed watermark ends
    /// at the exact sum of the 100-byte deltas and never more.
    #[tokio::test]
    async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let mut handles = Vec::new();
        for _ in 0..50u32 {
            let l = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                l.issue(100, 10, |_signed| async {
                    tokio::task::yield_now().await;
                    Ok(())
                })
                .await
            }));
        }

        let mut byte_totals = Vec::new();
        for h in handles {
            byte_totals.push(h.await??.bytes);
        }
        byte_totals.sort_unstable();
        let expected: Vec<U256> = (1..=50u64).map(|k| U256::from(k * 100)).collect();
        assert_eq!(
            byte_totals, expected,
            "every voucher's cumulative is distinct"
        );

        // Every voucher committed ⇒ committed carries all 50, and never more bytes
        // than the 100-per-voucher deltas we actually issued.
        let committed = ledger.committed();
        assert_eq!(committed.bytes, U256::from(5000u64));
        assert_eq!(committed.amount, U256::from(50u64));
        assert_eq!(ledger.settlement(), committed);
        Ok(())
    }

    #[tokio::test]
    async fn failed_send_does_not_commit() -> anyhow::Result<()> {
        let ledger = PoolLedger::new(Cumulative::default());
        let result = ledger
            .issue(100, 10, |_signed| async { anyhow::bail!("send lost") })
            .await;
        assert!(result.is_err(), "a failed send must surface the error");
        // Committed unmoved: a send that never confirmed never advances committed.
        assert_eq!(ledger.committed(), Cumulative::default());
        // But it settles HIGH — the send is ambiguous, so the voucher stays armed.
        assert_eq!(ledger.settlement().bytes, U256::from(100u64));
        Ok(())
    }

    /// The window this exists to close (#1122): a pull dropped inside the send
    /// leaves the upstream possibly holding a voucher we have no committed record
    /// of. Settle low and the deposit is stranded; settle high and it is honoured.
    #[tokio::test]
    async fn a_pull_dropped_inside_the_send_settles_at_the_voucher_it_sent() {
        let ledger = PoolLedger::new(Cumulative::default());
        let dropped = tokio::time::timeout(
            Duration::from_millis(20),
            ledger.issue(100, 10, |_next| {
                std::future::pending::<anyhow::Result<()>>()
            }),
        )
        .await;
        assert!(dropped.is_err(), "the send must still be in flight");

        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "an unconfirmed voucher must never advance the committed watermark"
        );
        let settled = ledger.settlement();
        assert_eq!(
            settled.bytes,
            U256::from(100u64),
            "settle at the sent voucher"
        );
        assert_eq!(settled.amount, U256::from(1u64));
    }

    /// A voucher the upstream explicitly REJECTED was never taken, so settling at
    /// it would inflate our cumulative. `resolve_reject` rewinds committed WITHOUT
    /// keeping it, so — with nothing else in flight — settlement falls back.
    #[tokio::test]
    async fn a_rejected_voucher_is_not_settled_optimistically() -> anyhow::Result<()> {
        let ledger = PoolLedger::new(Cumulative::default());
        // Issue + successful send: committed advances to the voucher.
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert_eq!(ledger.committed().bytes, U256::from(100u64));
        // The upstream rejects it (arrived as a mid-stream VoucherRejected).
        assert!(ledger.resolve_reject(), "the committed voucher is rewound");
        assert_eq!(
            ledger.settlement(),
            Cumulative::default(),
            "an explicitly rejected voucher must not advance what we persist"
        );
        Ok(())
    }

    /// Issue #1481: a wallet-less client cannot reconstruct its watermark from
    /// chain, so a gated rejection carries the node's true watermark back in a
    /// (nonce-free) `WatermarkBundle`. `Cumulative::from` must decode it losslessly.
    #[test]
    fn cumulative_from_bundle_is_lossless() {
        let bundle = WatermarkBundle {
            amount: u64::MAX,
            bytes_delivered: 1_048_576u64,
            last_signature: vec![0xABu8; 65],
        };
        let cum = Cumulative::from(&bundle);
        assert_eq!(cum.amount, U256::from(u64::MAX));
        assert_eq!(cum.bytes, U256::from(1_048_576u64));
    }

    /// The self-heal itself: an `AmountRegression` rejection with an authenticated
    /// bundle is not a dead end. `reseed` overwrites committed to the node's true
    /// state and clears the armed/rewind state, so the next `issue` builds on the
    /// bundle rather than colliding with what the node already holds.
    #[tokio::test]
    async fn an_amount_regression_rejection_with_a_bundle_self_heals() -> anyhow::Result<()> {
        // The caller's local ledger thinks it is at amount 10 (a wallet-less
        // delegate that never persisted the true watermark), but the node's true
        // watermark — echoed on the gated reject — is amount 50.
        let ledger = PoolLedger::new(Cumulative {
            bytes: U256::from(1000u64),
            amount: U256::from(10u64),
        });
        let bundle = WatermarkBundle {
            amount: 50u64,
            bytes_delivered: 5000u64,
            last_signature: vec![0xCDu8; 65],
        };

        // A voucher armed then ambiguously failed (settle high) before the reject.
        let _ = ledger
            .issue(100, 10, |_next| async { anyhow::bail!("ambiguous") })
            .await;
        assert!(ledger.settlement().amount > U256::from(10u64));

        // Self-heal: re-seed to the node's authenticated watermark.
        assert!(
            ledger.reseed(Cumulative::from(&bundle)),
            "a bundle ahead of committed must be applied"
        );
        assert_eq!(
            ledger.settlement().amount,
            U256::from(50u64),
            "reseed cleared the armed voucher and reset to the bundle watermark"
        );

        let issued = ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert_eq!(issued.bytes, U256::from(5100u64)); // bundle.bytes_delivered + 100
        assert_eq!(issued.amount, U256::from(51u64)); // bundle.amount + ceil(100*10/MiB)
        Ok(())
    }

    /// The monotonicity guard (#1497 review): a bundle that does NOT advance past
    /// the committed watermark must be refused, leaving the ledger untouched. The
    /// node attaches a bundle to EVERY watermark-gated rejection once any voucher
    /// has been accepted — including a genuinely exhausted lane, whose bundle just
    /// echoes the watermark the client already holds.
    #[tokio::test]
    async fn reseed_refuses_a_bundle_that_does_not_advance_the_watermark() -> anyhow::Result<()> {
        let committed = Cumulative {
            bytes: U256::from(5000u64),
            amount: U256::from(50u64),
        };
        let ledger = PoolLedger::new(committed);
        ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        let after_issue = ledger.committed();

        // The exhausted-lane echo: same amount we already hold.
        let echo = Cumulative {
            bytes: after_issue.bytes,
            amount: after_issue.amount,
        };
        assert!(
            !ledger.reseed(echo),
            "a bundle at the committed watermark proves nothing and must be refused"
        );
        // And the strictly-behind case must not rewind us either.
        let behind = Cumulative {
            bytes: U256::from(2000u64),
            amount: U256::from(20u64),
        };
        assert!(
            !ledger.reseed(behind),
            "a bundle behind committed must be refused"
        );
        assert_eq!(
            ledger.committed(),
            after_issue,
            "committed must never regress"
        );
        Ok(())
    }

    /// `SpendingCapExhausted` with NO bundle (a genuinely exhausted capability, nothing to
    /// resume from) must not be treated as self-healable — a caller checking
    /// `bundle.is_none()` sees the "give up / top up" signal. This pins the
    /// type-shape contract the resume path depends on.
    #[test]
    fn cap_exceeded_without_a_bundle_is_not_self_healable() -> anyhow::Result<()> {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        let upstream = err
            .downcast_ref::<UpstreamVoucherRejected>()
            .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
        assert_eq!(upstream.reason, VoucherRejectReason::SpendingCapExhausted);
        assert!(
            upstream.bundle.is_none(),
            "no bundle means no self-heal path — the caller must surface a top-up need"
        );
        Ok(())
    }

    /// An AMBIGUOUS failure — a stall timeout, a transport reset — is not a
    /// rejection: the upstream may already hold the voucher, so it must leave the
    /// voucher ARMED and settle HIGH, exactly as a drop does.
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
            let ledger = PoolLedger::new(Cumulative::default());
            let result = ledger
                .issue(100, 10, move |_next| async move { Err(make_err()) })
                .await;
            assert!(result.is_err(), "the ambiguous send must surface its error");
            assert_eq!(ledger.committed(), Cumulative::default());
            let settled = ledger.settlement();
            assert_eq!(
                settled.bytes,
                U256::from(100u64),
                "settle at the sent voucher"
            );
        }
    }

    /// After a voucher is armed (a failed send), the NEXT issue must build on it,
    /// not re-sign the same cumulative — which the upstream may already hold.
    #[tokio::test]
    async fn a_later_issue_builds_on_an_armed_voucher() -> anyhow::Result<()> {
        let ledger = PoolLedger::new(Cumulative::default());
        let stalled = ledger
            .issue(100, 10, |_next| async {
                Err(anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                }))
            })
            .await;
        assert!(stalled.is_err());
        // Second issue must build on the armed voucher (bytes 200, not 100).
        let sent = ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
        assert_eq!(
            sent.bytes,
            U256::from(200u64),
            "the next voucher must build on the armed voucher, not collide with it"
        );
        Ok(())
    }

    /// A sequence of successful issues stays ordered and exact — the committed
    /// watermark is the running cumulative, never ahead of what was delivered.
    #[tokio::test]
    async fn a_sequence_of_issues_stays_ordered_and_exact() -> anyhow::Result<()> {
        let ledger = PoolLedger::new(Cumulative::default());
        for expected in 1..=100u64 {
            let sent = ledger.issue(100, 10, |_next| async { Ok(()) }).await?;
            assert_eq!(sent.bytes, U256::from(expected * 100));
        }
        let committed = ledger.committed();
        assert_eq!(committed.bytes, U256::from(10_000u64));
        assert_eq!(ledger.settlement(), committed);
        Ok(())
    }

    #[test]
    fn bytes_accumulate_by_delta() {
        let cur = Cumulative {
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
    fn zero_delta_bumps_nothing() {
        let cur = Cumulative {
            bytes: U256::from(7u64),
            amount: U256::from(3u64),
        };
        let next = next_voucher(&cur, 0, 99);
        assert_eq!(next.bytes, U256::from(7u64));
        assert_eq!(next.amount, U256::from(3u64));
    }
}
