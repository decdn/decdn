//! Per-channel voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`ChannelLedger`] serializes voucher issuance across concurrent streams on one
//! payment channel by holding its lock across each voucher's send→ack.

use std::future::Future;

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;
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

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher.
    ///
    /// Holds the channel lock across `exchange`, which performs the actual I/O
    /// (write the signed voucher, await the node's ack). The next cumulative is
    /// computed from the live watermark *before* the I/O and committed *only* if
    /// `exchange` succeeds — a rejected or lost ack leaves the watermark unmoved.
    /// Returns the committed cumulative.
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
        let next = next_voucher(&guard, delta_bytes, rate_per_mb);
        exchange(next).await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
