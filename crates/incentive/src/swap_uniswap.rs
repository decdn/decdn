//! Uniswap V3 exact-out swap venue (#991).
//!
//! Buys exactly `amount_out` TOKEN with USDC via `SwapRouter02`'s
//! `exactOutputSingle`, bounding spend with `amountInMaximum` derived from
//! `QuoterV2.quoteExactOutputSingle` + [`crate::swap_math::max_in_with_slippage`].
//!
//! `spot_in` (the price-impact gate's baseline) is advisory only — it never
//! bounds the actual swap, that's `expected_in`/`max_in`. The `--swap-venue`
//! config surface ([`crate::swap_venue::ResolvedSwap`]) carries a dedicated
//! Uniswap TOKEN/USDC `pool` address (`--swap-pool-address`): when it is
//! configured the `slot0` spot read runs and lights up the price-impact gate,
//! and `UniswapV3Venue`'s `pool` field is `Option` so an unset pool degrades
//! `spot_in` to `expected_in` (0 bps impact, gate stays inert) rather than
//! fabricating a number.
//!
//! The `slot0` mid-price is fee-exclusive while `QuoterV2`'s `expected_in`
//! includes the pool fee, so `spot_in` is grossed up by the fee tier
//! ([`crate::swap_math::fee_inclusive_spot_in`]) before the gate compares them
//! — otherwise the fee (e.g. 30 bps on a 0.3% pool) would read as phantom
//! impact and eat the advisory budget before any real depth impact.

// The `sol!`-generated bindings include macro-emitted code that uses
// patterns workspace clippy denies (raw indexing, `unwrap` on infallible
// conversions). These allows scope the relaxation to this module only —
// same posture as `erc20`, `capacity_bond`, `payment_pool`.
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::pub_underscore_fields,
    clippy::missing_docs_in_private_items,
    clippy::too_many_arguments,
    missing_debug_implementations,
    missing_docs,
    non_snake_case,
    non_camel_case_types
)]
mod sol_types {
    alloy::sol! {
        #[sol(rpc)]
        contract SwapRouter02 {
            struct ExactOutputSingleParams {
                address tokenIn; address tokenOut; uint24 fee; address recipient;
                uint256 amountOut; uint256 amountInMaximum; uint160 sqrtPriceLimitX96;
            }
            function exactOutputSingle(ExactOutputSingleParams calldata params)
                external payable returns (uint256 amountIn);
            function multicall(uint256 deadline, bytes[] calldata data)
                external payable returns (bytes[] memory results);
        }
        #[sol(rpc)]
        contract QuoterV2 {
            struct QuoteExactOutputSingleParams {
                address tokenIn; address tokenOut; uint256 amount; uint24 fee;
                uint160 sqrtPriceLimitX96;
            }
            function quoteExactOutputSingle(QuoteExactOutputSingleParams memory params)
                external returns (uint256 amountIn, uint160 sqrtPriceX96After,
                                  uint32 initializedTicksCrossed, uint256 gasEstimate);
        }
        #[sol(rpc)]
        contract UniswapV3Pool { function slot0() external view returns (
            uint160 sqrtPriceX96, int24 tick, uint16 obsIndex, uint16 obsCard,
            uint16 obsCardNext, uint8 feeProtocol, bool unlocked); }
    }
}

pub use sol_types::{QuoterV2, SwapRouter02, UniswapV3Pool};

use alloy::primitives::aliases::U24;
use alloy::primitives::{Address, B256, Bytes, U160, U256, U512};
use alloy::providers::{DynProvider, Provider};
use alloy::sol_types::SolCall;
use anyhow::Context;

use crate::erc20::Erc20;
use crate::swap_math::{fee_inclusive_spot_in, max_in_with_slippage};
use crate::swap_venue::Quote;

/// Build the `exactOutputSingle` params for a USDC→TOKEN exact-out swap.
/// Always quotes at no price limit (`sqrtPriceLimitX96 = 0`) — the actual
/// spend bound is `amountInMaximum`, not a price limit. `fee` saturates into
/// the on-chain `uint24` (fee tiers are small governance-configured constants
/// like 500/3000/10000, so saturation is unreachable in practice; this keeps
/// the builder infallible rather than threading a `Result` through pure param
/// assembly).
pub(crate) fn build_exact_output_params(
    token_in: Address,
    token_out: Address,
    fee: u32,
    recipient: Address,
    amount_out: U256,
    amount_in_maximum: U256,
) -> SwapRouter02::ExactOutputSingleParams {
    SwapRouter02::ExactOutputSingleParams {
        tokenIn: token_in,
        tokenOut: token_out,
        fee: U24::saturating_from(fee),
        recipient,
        amountOut: amount_out,
        amountInMaximum: amount_in_maximum,
        sqrtPriceLimitX96: U160::ZERO,
    }
}

/// Spot amount-in for `amount_out` TOKEN at a pool's current `sqrtPriceX96`,
/// per Uniswap V3's Q96 price representation: `sqrtPriceX96^2 / 2^192` is the
/// price of token0 in terms of token1 (token1 per token0), in raw base
/// units.
///
/// This feeds only the advisory price-impact warning (see module docs) —
/// never the actual bounded spend. The intermediate products (`sqrtPriceX96^2`
/// and `amount_out << 192`) overflow `U256` for realistic inputs — a Uniswap
/// `sqrtPriceX96` runs up to ~2^160 (its square up to ~2^320) and a real bond
/// (`795_000 TOKEN ≈ 2^80` base units) shifted left by 192 exceeds `U256::MAX`
/// — so the math runs in `U512` and narrows only at the end. Narrowing
/// saturates in the astronomically-unlikely case the advisory number exceeds
/// `U256::MAX`, which keeps the function total (no panic) while never fabricating
/// a wrong bound on real spend.
pub(crate) fn spot_in_from_sqrt(
    sqrt_price_x96: U256,
    amount_out: U256,
    usdc_is_token0: bool,
) -> U256 {
    if sqrt_price_x96.is_zero() {
        return U256::ZERO;
    }
    // `sqrt_price_x96 < 2^160`, so the square is `< 2^320` and fits `U512`.
    // `saturating_mul` keeps this total (no overflow panic) even for the
    // unreachable type-max inputs — this is an advisory-only spot estimate.
    let sqrt = U512::from(sqrt_price_x96);
    let price_x192 = sqrt.saturating_mul(sqrt);
    if price_x192.is_zero() {
        return U256::ZERO;
    }
    let spot: U512 = if usdc_is_token0 {
        // price = TOKEN per USDC; USDC_in = amount_out(TOKEN) / price.
        (U512::from(amount_out) << 192) / price_x192
    } else {
        // price = USDC per TOKEN; USDC_in = amount_out(TOKEN) * price.
        U512::from(amount_out).saturating_mul(price_x192) >> 192
    };
    // Narrow back to U256; saturate to `U256::MAX` rather than panic in the
    // unreachable type-max case (see the doc comment).
    spot.saturating_to::<U256>()
}

/// A configured Uniswap V3 exact-out venue: `SwapRouter02` for the swap,
/// `QuoterV2` for the pre-flight quote, and (optionally) the pool contract
/// for the advisory `slot0` spot-price check — see module docs for why
/// `pool` is `Option`.
#[derive(Debug, Clone)]
pub struct UniswapV3Venue {
    provider: DynProvider,
    router: Address,
    quoter: Address,
    pool: Option<Address>,
    usdc: Address,
    token: Address,
    fee: u32,
    payer: Address,
}

impl UniswapV3Venue {
    /// Construct a venue against `provider`. `pool` is `None` when the
    /// current config surface has no Uniswap pool address (the common case
    /// today — see module docs); the price-impact gate simply won't fire.
    /// `payer` is the account whose USDC funds the swap (the signer behind
    /// `provider`); the router pulls `tokenIn` from it, so it is the allowance
    /// owner.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: impl Provider + Clone + 'static,
        router: Address,
        quoter: Address,
        pool: Option<Address>,
        usdc: Address,
        token: Address,
        fee: u32,
        payer: Address,
    ) -> Self {
        Self {
            provider: provider.erased(),
            router,
            quoter,
            pool,
            usdc,
            token,
            fee,
            payer,
        }
    }

    /// Quote amount-in for `amount_out` TOKEN via `QuoterV2`, applying
    /// `slippage_bps` to `max_in`. `spot_in` comes from the pool's `slot0`
    /// when a pool address is configured, else falls back to `expected_in`
    /// (neutral — the price-impact gate never fires on a fallback value).
    pub async fn quote_exact_out(
        &self,
        amount_out: U256,
        slippage_bps: u16,
    ) -> anyhow::Result<Quote> {
        let quoter = QuoterV2::new(self.quoter, &self.provider);
        let params = QuoterV2::QuoteExactOutputSingleParams {
            tokenIn: self.usdc,
            tokenOut: self.token,
            amount: amount_out,
            fee: U24::saturating_from(self.fee),
            sqrtPriceLimitX96: U160::ZERO,
        };
        let result = quoter
            .quoteExactOutputSingle(params)
            .call()
            .await
            .context("QuoterV2.quoteExactOutputSingle failed")?;
        let expected_in = result.amountIn;
        let max_in = max_in_with_slippage(expected_in, slippage_bps);

        let spot_in = match self.pool {
            Some(pool_addr) => {
                let pool = UniswapV3Pool::new(pool_addr, &self.provider);
                let slot0 = pool
                    .slot0()
                    .call()
                    .await
                    .context("UniswapV3Pool.slot0 failed")?;
                let usdc_is_token0 = self.usdc < self.token;
                let mid =
                    spot_in_from_sqrt(U256::from(slot0.sqrtPriceX96), amount_out, usdc_is_token0);
                // `mid` is the fee-exclusive mid-price cost; `expected_in`
                // includes the pool fee. Gross `mid` up by the fee so the
                // mandatory fee tier isn't counted as price impact (see
                // `fee_inclusive_spot_in`).
                fee_inclusive_spot_in(mid, self.fee)
            }
            None => expected_in,
        };

        Ok(Quote {
            expected_in,
            max_in,
            spot_in,
        })
    }

    /// Execute the exact-out swap. This method owns its own approvals:
    /// `SwapRouter02` pulls `tokenIn` via a direct ERC20 allowance, so it
    /// ERC20-approves the router for `max_in` first (idempotent — skipped when
    /// the standing allowance already covers `max_in`, so a `setup` re-run
    /// doesn't re-approve). The venue holds `payer` explicitly (the account the
    /// router pulls USDC from), so it is the allowance owner; `recipient` can be
    /// an arbitrary address — Uniswap's `exactOutputSingle` honors it — so the
    /// two need not coincide. `SwapRouter02.exactOutputSingle` has no per-call
    /// `deadline` parameter (unlike the V1 router), so the swap is submitted
    /// through the router's `multicall(uint256 deadline, bytes[] data)`, which
    /// reverts if the transaction can't land before `deadline` — the same
    /// expiry guarantee the Balancer venue gets natively.
    ///
    /// After the swap attempt — on **every** path (success, on-chain revert, or
    /// send failure) — the router allowance is reset to `0` best-effort, so no
    /// standing USDC approval survives: neither the unused `max_in − actual_in`
    /// remainder after a successful exact-out swap nor the full `max_in` after a
    /// failed one. The reset is captured separately from the swap's own result
    /// and a reset failure is logged, never masking the swap error.
    pub async fn swap_exact_out(
        &self,
        amount_out: U256,
        max_in: U256,
        recipient: Address,
        deadline: U256,
    ) -> anyhow::Result<B256> {
        // Approve the router to pull up to `max_in` USDC (direct ERC20
        // allowance). Idempotent: a sufficient existing allowance is a no-op.
        let usdc = Erc20::new(self.usdc, &self.provider);
        let allowance = usdc
            .allowance(self.payer, self.router)
            .call()
            .await
            .context("failed to read USDC allowance")?;
        if allowance < max_in {
            crate::tx::send_unrecorded(usdc.approve(self.router, max_in), "USDC approve", None)
                .await?;
        }

        let router = SwapRouter02::new(self.router, &self.provider);
        let params = build_exact_output_params(
            self.usdc, self.token, self.fee, recipient, amount_out, max_in,
        );
        // `exactOutputSingle` has no per-call deadline, so wrap it in the
        // router's `multicall(deadline, data)`: the router reverts the whole
        // batch if it can't execute before `deadline`, giving the swap the same
        // expiry guarantee the Balancer venue has natively.
        let inner = SwapRouter02::exactOutputSingleCall { params }.abi_encode();
        // The swap does NOT early-return on failure: the allowance reset must
        // run on every path so no standing USDC allowance survives — neither the
        // `max_in − actual_in` remainder after a successful swap nor the full
        // `max_in` after a reverted/failed one. A reset failure is logged and
        // swallowed so it never masks the swap's own error. `run_then_cleanup`
        // is what makes that sequencing a named, tested property rather than an
        // easily-broken local convention (see its docs).
        crate::tx::run_then_cleanup(
            crate::tx::send_unrecorded(
                router.multicall(deadline, vec![Bytes::from(inner)]),
                "exactOutputSingle (via multicall)",
                None,
            ),
            || self.reset_router_allowance(),
        )
        .await
    }

    /// Best-effort reset of the router's USDC allowance to zero. Warns and
    /// returns on any failure — clearing the leftover allowance must never mask
    /// the swap's own outcome, so this runs after the swap on every path (see
    /// `swap_exact_out`).
    async fn reset_router_allowance(&self) {
        let usdc = Erc20::new(self.usdc, &self.provider);
        let outcome = async {
            crate::tx::send_unrecorded(usdc.approve(self.router, U256::ZERO), "reset", None)
                .await?;
            anyhow::Ok(())
        }
        .await;
        if let Err(e) = outcome {
            tracing::warn!(
                error = %e,
                "USDC router allowance reset failed; a stale router allowance may survive"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    const USDC: Address = Address::repeat_byte(0xA1);
    const TOKEN: Address = Address::repeat_byte(0xB2);
    const RECIPIENT: Address = Address::repeat_byte(0xC3);
    const ROUTER: Address = Address::repeat_byte(0xD4);
    const QUOTER: Address = Address::repeat_byte(0xE5);
    const PAYER: Address = Address::repeat_byte(0xF6);

    /// A provider that never dials out (`127.0.0.1:1` refuses immediately), so
    /// every RPC send fails. Used to exercise the best-effort reset helper's
    /// swallow-and-continue path without any on-chain infra.
    fn unconnected_provider() -> impl Provider + Clone + 'static {
        alloy::providers::ProviderBuilder::new().connect_http("http://127.0.0.1:1".parse().unwrap())
    }

    #[test]
    fn exact_output_params_are_usdc_in_token_out() {
        let p = build_exact_output_params(
            USDC,
            TOKEN,
            3000,
            RECIPIENT,
            U256::from(500u64),
            U256::from(1_030_000u64),
        );
        assert_eq!(p.tokenIn, USDC);
        assert_eq!(p.tokenOut, TOKEN);
        assert_eq!(p.amountOut, U256::from(500u64));
        assert_eq!(p.amountInMaximum, U256::from(1_030_000u64));
        assert_eq!(p.sqrtPriceLimitX96, U256::ZERO.to::<U160>());
    }

    #[test]
    fn exact_output_params_carry_recipient_and_fee() {
        let p = build_exact_output_params(
            USDC,
            TOKEN,
            500,
            RECIPIENT,
            U256::from(1u64),
            U256::from(1u64),
        );
        assert_eq!(p.recipient, RECIPIENT);
        assert_eq!(p.fee, U24::saturating_from(500u32));
    }

    #[test]
    fn spot_in_zero_sqrt_price_is_zero() {
        assert_eq!(
            spot_in_from_sqrt(U256::ZERO, U256::from(500u64), true),
            U256::ZERO
        );
    }

    #[test]
    fn spot_in_unit_price_is_amount_out_either_side() {
        // sqrtPriceX96 = 1 * 2^96 -> price = 1 (1 TOKEN per 1 USDC raw unit).
        let sqrt_price_1x = U256::from(1u8) << 96;
        assert_eq!(
            spot_in_from_sqrt(sqrt_price_1x, U256::from(500u64), true),
            U256::from(500u64)
        );
        assert_eq!(
            spot_in_from_sqrt(sqrt_price_1x, U256::from(500u64), false),
            U256::from(500u64)
        );
    }

    #[test]
    fn spot_in_respects_token_ordering() {
        // sqrtPriceX96 = 2 * 2^96 -> price_x192 = 4 * 2^192 (price = 4).
        let sqrt_price_2x = U256::from(2u8) << 96;

        // usdc_is_token0: price = TOKEN per USDC = 4 -> buying 400 TOKEN
        // costs 400/4 = 100 USDC.
        assert_eq!(
            spot_in_from_sqrt(sqrt_price_2x, U256::from(400u64), true),
            U256::from(100u64)
        );
        // !usdc_is_token0: price = USDC per TOKEN = 4 -> buying 400 TOKEN
        // costs 400*4 = 1600 USDC.
        assert_eq!(
            spot_in_from_sqrt(sqrt_price_2x, U256::from(400u64), false),
            U256::from(1600u64)
        );
    }

    /// A realistic ~10 Gbps-tier bond quantity (`795_000` TOKEN, 18 decimals).
    /// A `U256` square would overflow here and return a fabricated value
    /// (`U256::MAX / price` on the token0 side, or `U256::MAX >> 192` on the
    /// token1 side); the `U512` path returns the mathematically correct spot.
    /// Both token-ordering branches are covered.
    fn ten_gbps_bond() -> U256 {
        // 795_000 * 10^18.
        U256::from(795_000u64) * U256::from(10u64).pow(U256::from(18u64))
    }

    #[test]
    fn spot_in_large_bond_usdc_token0_is_sane_not_saturated() {
        // sqrtPriceX96 = 2 * 2^96 -> price_x192 = 4 * 2^192 (price = 4 TOKEN
        // per USDC). Buying `amount_out` TOKEN costs `amount_out / 4` USDC.
        let sqrt_price_2x = U256::from(2u8) << 96;
        let amount_out = ten_gbps_bond();
        let expected = amount_out / U256::from(4u64); // 198_750 * 10^18
        let got = spot_in_from_sqrt(sqrt_price_2x, amount_out, true);
        assert_eq!(got, expected, "token0 spot must be amount_out / price");
        assert_ne!(got, U256::MAX, "must not saturate to U256::MAX");
        // Sanity: the correct answer is ~1.9875e23, far below U256::MAX and far
        // above the fabricated ~2^62 a saturating `U256` square would produce.
        assert!(got > U256::from(10u64).pow(U256::from(23u64)));
    }

    #[test]
    fn spot_in_large_bond_usdc_token1_is_sane_not_saturated() {
        // Same pool, opposite ordering: price = 4 USDC per TOKEN. Buying
        // `amount_out` TOKEN costs `amount_out * 4` USDC.
        let sqrt_price_2x = U256::from(2u8) << 96;
        let amount_out = ten_gbps_bond();
        let expected = amount_out * U256::from(4u64); // 3_180_000 * 10^18
        let got = spot_in_from_sqrt(sqrt_price_2x, amount_out, false);
        assert_eq!(got, expected, "token1 spot must be amount_out * price");
        assert_ne!(got, U256::MAX, "must not saturate to U256::MAX");
        // Correct answer ~3.18e24; a saturating `U256` square would produce ~2^64.
        assert!(got > U256::from(10u64).pow(U256::from(24u64)));
    }

    // Cleanup coverage note. Running the
    // allowance reset on every path rests on two properties, now covered
    // separately:
    //
    // 1. The reset is *best-effort* — a failed reset must never panic, abort
    //    onboarding, or mask the swap's own error. Unit-tested below against an
    //    unconnected provider (the reset's send fails and is swallowed).
    // 2. It runs on every path, after the swap, without replacing its result.
    //    That is control flow, so it is tested as control flow:
    //    `tx::run_then_cleanup`'s own tests drive both the Ok and Err paths with
    //    plain closures and no RPC at all. Both venues route through it.
    //
    // What remains deferred is the on-chain version — approve succeeds, the swap
    // *reverts on-chain*, and the follow-up reset is then observed against real
    // allowance state. That needs anvil, which this crate has no dev-dep on.
    //
    // `alloy::providers::mock::Asserter` does NOT close that gap, and the reason
    // is worth recording so it is not re-litigated: it answers by QUEUE POSITION,
    // not by method or calldata. That makes read paths trivial to drive (see
    // `cli/src/commands/unbond.rs` `mod plan_computation`) and a *send* path
    // impractical, because one `send()` issues several filler requests whose
    // order we do not control — so a queued response cannot be tied to the call
    // it answers, and the mock cannot report which calls were made. The same
    // blindness is why the sibling `all_target` decision in `unbond.rs` had to be
    // extracted to be testable at all.
    //
    // A test built on it would therefore assert queue arithmetic rather than our
    // sequencing, and would break on any alloy change to the filler stack.

    #[tokio::test]
    async fn reset_router_allowance_swallows_send_failure() {
        // On an unconnected provider the reset transaction can't send; the
        // helper must warn-and-continue (return `()`), never panic — otherwise
        // running it on the swap-failure path would turn a swap error into a
        // panic. Reaching the `.await` completion is the assertion.
        let venue = UniswapV3Venue::new(
            unconnected_provider(),
            ROUTER,
            QUOTER,
            None,
            USDC,
            TOKEN,
            3000,
            PAYER,
        );
        venue.reset_router_allowance().await;
    }
}
