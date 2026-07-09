//! Venue-neutral exact-out USDC→TOKEN swap abstraction for the bond funding
//! path (#991). One enum, one impl per DEX venue, chosen by chain config. The
//! caller (setup) approves `max_in` to the router before `swap_exact_out`.

use alloy::primitives::{Address, B256, U256};

// Only referenced from the `#[cfg(test)] Mock` arm below; a plain (non-test)
// build has no arm that calls it, so the import itself is cfg-gated to match.
#[cfg(test)]
use crate::swap_math::max_in_with_slippage;

/// Result of an exact-out quote.
#[derive(Debug, Clone, Copy)]
pub struct Quote {
    /// Amount-in the quoter says buys exactly `amount_out`.
    pub expected_in: U256,
    /// `expected_in` grown by the slippage bound → `amountInMaximum`.
    pub max_in: U256,
    /// Amount-in at pool spot price, for the price-impact gate.
    pub spot_in: U256,
}

/// A configured DEX venue. Real arms are added in Tasks 5–6.
#[derive(Debug)]
pub enum SwapVenue {
    #[cfg(test)]
    Mock(MockVenue),
}

impl SwapVenue {
    /// Quote amount-in for `amount_out`, applying `slippage_bps` to `max_in`.
    // `async` is preserved even though the current (Mock-only) body never
    // `.await`s: real venue arms (Tasks 5-6) call out over the network here,
    // so the signature is part of the trait-shaped contract, not incidental.
    #[allow(clippy::unused_async)]
    pub async fn quote_exact_out(
        &self,
        amount_out: U256,
        slippage_bps: u16,
    ) -> anyhow::Result<Quote> {
        match self {
            #[cfg(test)]
            Self::Mock(m) => Ok(Quote {
                expected_in: m.expected_in,
                max_in: max_in_with_slippage(m.expected_in, slippage_bps),
                spot_in: m.spot_in,
            }),
            #[allow(unreachable_patterns)]
            _ => {
                let _ = (amount_out, slippage_bps);
                anyhow::bail!("no swap venue configured")
            }
        }
    }

    /// Execute the exact-out swap; caller has already approved `max_in`.
    // See `quote_exact_out`: async is future-shaped for Tasks 5-6's real arms.
    #[allow(clippy::unused_async)]
    pub async fn swap_exact_out(
        &self,
        amount_out: U256,
        max_in: U256,
        recipient: Address,
        deadline: U256,
    ) -> anyhow::Result<B256> {
        match self {
            #[cfg(test)]
            Self::Mock(m) => Ok(m.swap_tx),
            #[allow(unreachable_patterns)]
            _ => {
                let _ = (amount_out, max_in, recipient, deadline);
                anyhow::bail!("no swap venue configured")
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockVenue {
    pub expected_in: U256,
    pub spot_in: U256,
    pub swap_tx: B256,
}

#[cfg(test)]
impl MockVenue {
    pub fn default_with_tx(swap_tx: B256) -> Self {
        Self {
            expected_in: U256::from(1u64),
            spot_in: U256::from(1u64),
            swap_tx,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};

    #[tokio::test]
    async fn mock_quote_applies_slippage_and_reports_spot() {
        let v = SwapVenue::Mock(MockVenue {
            expected_in: U256::from(1_000_000u64),
            spot_in: U256::from(990_000u64),
            swap_tx: B256::repeat_byte(0xAB),
        });
        let q = v.quote_exact_out(U256::from(500u64), 300).await.unwrap();
        assert_eq!(q.expected_in, U256::from(1_000_000u64));
        assert_eq!(q.max_in, U256::from(1_030_000u64)); // +3%
        assert_eq!(q.spot_in, U256::from(990_000u64));
    }

    #[tokio::test]
    async fn mock_swap_returns_tx() {
        let v = SwapVenue::Mock(MockVenue::default_with_tx(B256::repeat_byte(0xCD)));
        let tx = v
            .swap_exact_out(
                U256::from(500u64),
                U256::from(1u64),
                Address::ZERO,
                U256::from(0u64),
            )
            .await
            .unwrap();
        assert_eq!(tx, B256::repeat_byte(0xCD));
    }
}
