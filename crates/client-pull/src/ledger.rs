//! Per-channel voucher accounting. [`next_voucher`] is the pure cumulative math;
//! the `ChannelLedger` that serializes voucher issuance across concurrent streams
//! on one payment channel builds on it (added in a later step).

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;

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
pub fn next_voucher(cur: &Cumulative, delta_bytes: u64, rate_per_mb: u64) -> Cumulative {
    let amount_delta = U256::from(delta_bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    Cumulative {
        nonce: cur.nonce.saturating_add(U256::from(1u64)),
        bytes: cur.bytes.saturating_add(U256::from(delta_bytes)),
        amount: cur.amount.saturating_add(amount_delta),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
