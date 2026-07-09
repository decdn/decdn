//! Venue-neutral exact-out USDC→TOKEN swap abstraction for the bond funding
//! path (#991). One enum, one impl per DEX venue, chosen by chain config. The
//! caller (setup) approves `max_in` to the router before `swap_exact_out`.

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;

use crate::swap_uniswap::UniswapV3Venue;

// Only referenced from the `#[cfg(test)] Mock` arm below; the real
// `UniswapV3` arm applies slippage inside `UniswapV3Venue::quote_exact_out`
// itself, so this import is cfg-gated to the test-only call site.
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

/// A configured DEX venue. The Balancer arm lands in Task 6.
#[derive(Debug)]
pub enum SwapVenue {
    #[cfg(test)]
    Mock(MockVenue),
    UniswapV3(UniswapV3Venue),
}

impl SwapVenue {
    /// Quote amount-in for `amount_out`, applying `slippage_bps` to `max_in`.
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
            Self::UniswapV3(v) => v.quote_exact_out(amount_out, slippage_bps).await,
        }
    }

    /// Execute the exact-out swap; caller has already approved `max_in`.
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
            Self::UniswapV3(v) => {
                v.swap_exact_out(amount_out, max_in, recipient, deadline)
                    .await
            }
        }
    }
}

/// Mirror of the `cli` crate's `chain_ctx::ResolvedSwap` (flag > config
/// resolved swap coordinates). Duplicated here rather than imported because
/// `cli` depends on `incentive`, not the reverse (see the workspace
/// dependency-flow note in the crate root docs) — `incentive` can't name a
/// type defined in `cli`. Field names/shapes match 1:1 so a future CLI wiring
/// pass (Task 4) can convert directly.
#[derive(Debug, Clone)]
pub struct ResolvedSwap {
    /// Venue selector, e.g. `"uniswap-v3"`.
    pub venue: String,
    /// Router contract address (hex string, unparsed).
    pub router: String,
    /// Quoter contract address (hex string, unparsed).
    pub quoter: String,
    /// USDC token address (hex string, unparsed).
    pub usdc: String,
    /// Uniswap V3 fee tier (e.g. `3000` for 0.3%); required for
    /// `"uniswap-v3"`.
    pub uniswap_fee_tier: Option<u32>,
    /// Balancer pool id (bytes32 hex string); required for `"balancer"`
    /// (Task 6).
    pub balancer_pool_id: Option<String>,
}

/// Construct the configured [`SwapVenue`] from resolved chain config,
/// dispatching on `resolved.venue`. `token` is the TOKEN address being
/// bought (not carried by `ResolvedSwap`, which is USDC-side config only).
pub fn from_config<P: Provider + Clone + 'static>(
    provider: P,
    resolved: &ResolvedSwap,
    token: Address,
) -> anyhow::Result<SwapVenue> {
    match resolved.venue.as_str() {
        "uniswap-v3" => {
            let router: Address = resolved.router.parse().map_err(|e| {
                anyhow::anyhow!(
                    "swap_router_address {:?} is not a valid address: {e}",
                    resolved.router
                )
            })?;
            let quoter: Address = resolved.quoter.parse().map_err(|e| {
                anyhow::anyhow!(
                    "swap_quoter_address {:?} is not a valid address: {e}",
                    resolved.quoter
                )
            })?;
            let usdc: Address = resolved.usdc.parse().map_err(|e| {
                anyhow::anyhow!(
                    "usdc_address {:?} is not a valid address: {e}",
                    resolved.usdc
                )
            })?;
            let fee = resolved.uniswap_fee_tier.ok_or_else(|| {
                anyhow::anyhow!("uniswap_fee_tier required for --swap-venue uniswap-v3")
            })?;
            Ok(SwapVenue::UniswapV3(UniswapV3Venue::new(
                provider, router, quoter, None, usdc, token, fee,
            )))
        }
        other => anyhow::bail!("unknown swap venue {other}"),
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

    /// A provider that never dials out: `from_config` only needs a value
    /// satisfying `Provider + Clone + 'static` to construct the venue, and
    /// `connect_http` builds the HTTP transport lazily (no I/O until a call
    /// is made), so this is safe to use in a unit test.
    fn unconnected_provider() -> impl alloy::providers::Provider + Clone + 'static {
        alloy::providers::ProviderBuilder::new().connect_http("http://127.0.0.1:1".parse().unwrap())
    }

    fn addr_hex(byte: u8) -> String {
        format!("{:#x}", Address::repeat_byte(byte))
    }

    #[test]
    fn from_config_dispatches_uniswap_v3() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: addr_hex(0x11),
            quoter: addr_hex(0x22),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: Some(3000),
            balancer_pool_id: None,
        };
        let venue = from_config(
            unconnected_provider(),
            &resolved,
            Address::repeat_byte(0x44),
        )
        .unwrap();
        assert!(matches!(venue, SwapVenue::UniswapV3(_)));
    }

    #[test]
    fn from_config_requires_fee_tier_for_uniswap_v3() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: addr_hex(0x11),
            quoter: addr_hex(0x22),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: None,
            balancer_pool_id: None,
        };
        let err = from_config(unconnected_provider(), &resolved, Address::ZERO).unwrap_err();
        assert!(err.to_string().contains("uniswap_fee_tier"), "{err}");
    }

    #[test]
    fn from_config_rejects_unknown_venue() {
        let resolved = ResolvedSwap {
            venue: "curve".to_string(),
            router: String::new(),
            quoter: String::new(),
            usdc: String::new(),
            uniswap_fee_tier: None,
            balancer_pool_id: None,
        };
        let err = from_config(unconnected_provider(), &resolved, Address::ZERO).unwrap_err();
        assert!(
            err.to_string().contains("unknown swap venue curve"),
            "{err}"
        );
    }

    #[test]
    fn from_config_rejects_malformed_router_address() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: "not-an-address".to_string(),
            quoter: addr_hex(0x22),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: Some(3000),
            balancer_pool_id: None,
        };
        let err = from_config(unconnected_provider(), &resolved, Address::ZERO).unwrap_err();
        assert!(err.to_string().contains("swap_router_address"), "{err}");
    }
}
