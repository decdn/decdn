//! Venue-neutral exact-out USDC→TOKEN swap abstraction for the bond funding
//! path (#991). One enum, one impl per DEX venue, chosen by chain config. Each
//! venue's `swap_exact_out` owns its own approvals (Uniswap: a direct ERC20
//! allowance to the router; Balancer: the ERC20→Permit2→Router dance), so the
//! caller just quotes then calls `swap_exact_out` — it never pre-approves.

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;

use crate::swap_balancer::BalancerV3Venue;
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

/// A configured DEX venue.
#[derive(Debug)]
pub enum SwapVenue {
    #[cfg(test)]
    Mock(MockVenue),
    UniswapV3(UniswapV3Venue),
    BalancerV3(BalancerV3Venue),
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
            Self::BalancerV3(v) => v.quote_exact_out(amount_out, slippage_bps).await,
        }
    }

    /// Estimated extra native-gas units this venue's swap sequence consumes,
    /// on top of the bond-phase transactions — used to size `setup`'s
    /// native-gas pre-flight. Over-estimates (the safe direction). Uniswap runs
    /// approve + `exactOutputSingle` + reset; Balancer runs ERC20-approve +
    /// Permit2-approve + swap + two allowance resets, so it needs more headroom.
    pub const fn swap_gas_units(&self) -> u64 {
        match self {
            #[cfg(test)]
            Self::Mock(_) => 0,
            Self::UniswapV3(_) => 300_000,
            Self::BalancerV3(_) => 450_000,
        }
    }

    /// Execute the exact-out swap; the venue performs its own approvals.
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
            Self::BalancerV3(v) => {
                v.swap_exact_out(amount_out, max_in, recipient, deadline)
                    .await
            }
        }
    }
}

/// Flag > config resolved swap coordinates (USDC-side config only). This is
/// the canonical type: `cli`'s `chain_ctx::resolve_swap` builds and returns it
/// directly (the CLI imports it rather than defining its own, since `cli`
/// depends on `incentive`, not the reverse — see the workspace dependency-flow
/// note in the crate root docs), and [`from_config`] consumes it.
#[derive(Debug, Clone)]
pub struct ResolvedSwap {
    /// Venue selector, e.g. `"uniswap-v3"`.
    pub venue: String,
    /// Router contract address (hex string, unparsed).
    pub router: String,
    /// Quoter contract address (hex string, unparsed). Uniswap-only:
    /// `"uniswap-v3"` requires it (its separate `QuoterV2`), while
    /// `"balancer-v3"` quotes through the Router itself and leaves this `None`.
    pub quoter: Option<String>,
    /// USDC token address (hex string, unparsed).
    pub usdc: String,
    /// Uniswap V3 fee tier (e.g. `3000` for 0.3%); required for
    /// `"uniswap-v3"`.
    pub uniswap_fee_tier: Option<u32>,
    /// Balancer V3 pool contract address (hex string, unparsed); required for
    /// `"balancer-v3"`. Balancer V3 addresses pools directly, not by bytes32
    /// id — unlike Balancer V2, there is no `poolId` indirection through the
    /// Vault (see `IBalancerV3Router.sol`'s `pool` parameter).
    pub balancer_pool_address: Option<String>,
    /// Uniswap V3 pool address (hex string, unparsed) for the TOKEN/USDC
    /// pair. Optional: when `Some`, lights up the advisory price-impact gate
    /// via `slot0`; when `None`, the gate stays inert (see [`from_config`] and
    /// the `swap_uniswap` module docs).
    pub pool: Option<String>,
}

/// Parse a swap contract address with a labelled error and reject the zero
/// address. `Address::ZERO` parses cleanly but is never a real deployment — it
/// would surface only as an opaque on-chain revert deep in the swap path (a
/// call to the codeless zero address), so reject it here with a clear, labelled
/// error. Mirrors the CLI's `chain_ctx::parse_nonzero_address` guard
/// (#1153/#1213); duplicated because `incentive` cannot depend on the `cli`
/// crate. The "… is not a valid address" wording is preserved so the existing
/// malformed-address tests keep matching.
fn parse_nonzero(value: &str, label: &str) -> anyhow::Result<Address> {
    let addr: Address = value
        .parse()
        .map_err(|e| anyhow::anyhow!("{label} {value:?} is not a valid address: {e}"))?;
    anyhow::ensure!(
        addr != Address::ZERO,
        "{label} must not be the zero address — set it to the deployed contract address"
    );
    Ok(addr)
}

/// Construct the configured [`SwapVenue`] from resolved chain config,
/// dispatching on `resolved.venue`. `token` is the TOKEN address being
/// bought (not carried by `ResolvedSwap`, which is USDC-side config only).
/// `payer` is the account whose USDC funds the swap — the signer behind
/// `provider` — which each venue uses as the allowance owner (Uniswap) and
/// enforced swap recipient (Balancer V3 has no recipient slot).
pub fn from_config<P: Provider + Clone + 'static>(
    provider: P,
    resolved: &ResolvedSwap,
    token: Address,
    payer: Address,
) -> anyhow::Result<SwapVenue> {
    match resolved.venue.as_str() {
        "uniswap-v3" => {
            let router = parse_nonzero(&resolved.router, "swap_router_address")?;
            let quoter_str = resolved.quoter.as_ref().ok_or_else(|| {
                anyhow::anyhow!("swap_quoter_address required for --swap-venue uniswap-v3")
            })?;
            let quoter = parse_nonzero(quoter_str, "swap_quoter_address")?;
            let usdc = parse_nonzero(&resolved.usdc, "usdc_address")?;
            let fee = resolved.uniswap_fee_tier.ok_or_else(|| {
                anyhow::anyhow!("uniswap_fee_tier required for --swap-venue uniswap-v3")
            })?;
            let pool = match &resolved.pool {
                Some(p) => Some(parse_nonzero(p, "swap_pool_address")?),
                None => None,
            };
            Ok(SwapVenue::UniswapV3(UniswapV3Venue::new(
                provider, router, quoter, pool, usdc, token, fee, payer,
            )))
        }
        "balancer-v3" => {
            let router = parse_nonzero(&resolved.router, "swap_router_address")?;
            let usdc = parse_nonzero(&resolved.usdc, "usdc_address")?;
            let pool_str = resolved.balancer_pool_address.as_ref().ok_or_else(|| {
                anyhow::anyhow!("swap_balancer_pool required for --swap-venue balancer-v3")
            })?;
            let pool = parse_nonzero(pool_str, "swap_balancer_pool")?;
            Ok(SwapVenue::BalancerV3(BalancerV3Venue::new(
                provider, router, pool, usdc, token, payer,
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
            quoter: Some(addr_hex(0x22)),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: Some(3000),
            balancer_pool_address: None,
            pool: None,
        };
        let venue = from_config(
            unconnected_provider(),
            &resolved,
            Address::repeat_byte(0x44),
            Address::ZERO,
        )
        .unwrap();
        assert!(matches!(venue, SwapVenue::UniswapV3(_)));
    }

    #[test]
    fn from_config_requires_fee_tier_for_uniswap_v3() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: addr_hex(0x11),
            quoter: Some(addr_hex(0x22)),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: None,
            balancer_pool_address: None,
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::ZERO,
            Address::ZERO,
        )
        .unwrap_err();
        assert!(err.to_string().contains("uniswap_fee_tier"), "{err}");
    }

    #[test]
    fn from_config_requires_quoter_for_uniswap_v3() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: addr_hex(0x11),
            quoter: None,
            usdc: addr_hex(0x33),
            uniswap_fee_tier: Some(3000),
            balancer_pool_address: None,
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::ZERO,
            Address::ZERO,
        )
        .unwrap_err();
        assert!(err.to_string().contains("swap_quoter_address"), "{err}");
    }

    #[test]
    fn from_config_rejects_unknown_venue() {
        let resolved = ResolvedSwap {
            venue: "curve".to_string(),
            router: String::new(),
            quoter: None,
            usdc: String::new(),
            uniswap_fee_tier: None,
            balancer_pool_address: None,
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::ZERO,
            Address::ZERO,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unknown swap venue curve"),
            "{err}"
        );
    }

    #[test]
    fn balancer_from_config_requires_pool_id() {
        let resolved = ResolvedSwap {
            venue: "balancer-v3".to_string(),
            router: addr_hex(0x11),
            quoter: None,
            usdc: addr_hex(0x33),
            uniswap_fee_tier: None,
            balancer_pool_address: None,
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::repeat_byte(0x44),
            Address::ZERO,
        )
        .unwrap_err();
        assert!(err.to_string().contains("swap_balancer_pool"), "{err}");
    }

    #[test]
    fn from_config_dispatches_balancer_v3() {
        let resolved = ResolvedSwap {
            venue: "balancer-v3".to_string(),
            router: addr_hex(0x11),
            quoter: None,
            usdc: addr_hex(0x33),
            uniswap_fee_tier: None,
            balancer_pool_address: Some(addr_hex(0x55)),
            pool: None,
        };
        let venue = from_config(
            unconnected_provider(),
            &resolved,
            Address::repeat_byte(0x44),
            Address::ZERO,
        )
        .unwrap();
        assert!(matches!(venue, SwapVenue::BalancerV3(_)));
    }

    #[test]
    fn from_config_rejects_malformed_balancer_pool_address() {
        let resolved = ResolvedSwap {
            venue: "balancer-v3".to_string(),
            router: addr_hex(0x11),
            quoter: None,
            usdc: addr_hex(0x33),
            uniswap_fee_tier: None,
            balancer_pool_address: Some("not-an-address".to_string()),
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::ZERO,
            Address::ZERO,
        )
        .unwrap_err();
        assert!(err.to_string().contains("swap_balancer_pool"), "{err}");
    }

    #[test]
    fn from_config_rejects_malformed_router_address() {
        let resolved = ResolvedSwap {
            venue: "uniswap-v3".to_string(),
            router: "not-an-address".to_string(),
            quoter: Some(addr_hex(0x22)),
            usdc: addr_hex(0x33),
            uniswap_fee_tier: Some(3000),
            balancer_pool_address: None,
            pool: None,
        };
        let err = from_config(
            unconnected_provider(),
            &resolved,
            Address::ZERO,
            Address::ZERO,
        )
        .unwrap_err();
        assert!(err.to_string().contains("swap_router_address"), "{err}");
    }

    /// `addr_hex(0x00)` is the zero address as a hex string. A zero contract
    /// address parses cleanly but is never a real deployment; `from_config` now
    /// rejects it at parse time via the local `parse_nonzero` guard (#1213)
    /// rather than letting it surface as an opaque on-chain revert deep in the
    /// swap path. Every parsed swap address is covered across both venues.
    fn assert_rejects_zero(resolved: &ResolvedSwap, label: &str) {
        let err = from_config(
            unconnected_provider(),
            resolved,
            Address::repeat_byte(0x44),
            Address::ZERO,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(label), "expected label {label:?}, got: {err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    #[test]
    fn from_config_rejects_zero_uniswap_router() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "uniswap-v3".to_string(),
                router: addr_hex(0x00),
                quoter: Some(addr_hex(0x22)),
                usdc: addr_hex(0x33),
                uniswap_fee_tier: Some(3000),
                balancer_pool_address: None,
                pool: None,
            },
            "swap_router_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_uniswap_quoter() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "uniswap-v3".to_string(),
                router: addr_hex(0x11),
                quoter: Some(addr_hex(0x00)),
                usdc: addr_hex(0x33),
                uniswap_fee_tier: Some(3000),
                balancer_pool_address: None,
                pool: None,
            },
            "swap_quoter_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_uniswap_usdc() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "uniswap-v3".to_string(),
                router: addr_hex(0x11),
                quoter: Some(addr_hex(0x22)),
                usdc: addr_hex(0x00),
                uniswap_fee_tier: Some(3000),
                balancer_pool_address: None,
                pool: None,
            },
            "usdc_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_uniswap_pool() {
        // The pool is optional, but a present zero must still be rejected.
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "uniswap-v3".to_string(),
                router: addr_hex(0x11),
                quoter: Some(addr_hex(0x22)),
                usdc: addr_hex(0x33),
                uniswap_fee_tier: Some(3000),
                balancer_pool_address: None,
                pool: Some(addr_hex(0x00)),
            },
            "swap_pool_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_balancer_router() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "balancer-v3".to_string(),
                router: addr_hex(0x00),
                quoter: None,
                usdc: addr_hex(0x33),
                uniswap_fee_tier: None,
                balancer_pool_address: Some(addr_hex(0x55)),
                pool: None,
            },
            "swap_router_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_balancer_usdc() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "balancer-v3".to_string(),
                router: addr_hex(0x11),
                quoter: None,
                usdc: addr_hex(0x00),
                uniswap_fee_tier: None,
                balancer_pool_address: Some(addr_hex(0x55)),
                pool: None,
            },
            "usdc_address",
        );
    }

    #[test]
    fn from_config_rejects_zero_balancer_pool() {
        assert_rejects_zero(
            &ResolvedSwap {
                venue: "balancer-v3".to_string(),
                router: addr_hex(0x11),
                quoter: None,
                usdc: addr_hex(0x33),
                uniswap_fee_tier: None,
                balancer_pool_address: Some(addr_hex(0x00)),
                pool: None,
            },
            "swap_balancer_pool",
        );
    }
}
