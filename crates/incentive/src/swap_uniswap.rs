//! Uniswap V3 exact-out swap venue (#991).
//!
//! Buys exactly `amount_out` TOKEN with USDC via `SwapRouter02`'s
//! `exactOutputSingle`, bounding spend with `amountInMaximum` derived from
//! `QuoterV2.quoteExactOutputSingle` + [`crate::swap_math::max_in_with_slippage`].
//!
//! `spot_in` (the price-impact gate's baseline) is advisory only — it never
//! bounds the actual swap, that's `expected_in`/`max_in`. The current
//! `--swap-venue` config surface (`chain_ctx::ResolvedSwap` in the `cli`
//! crate) carries a router, quoter, USDC address, and fee tier, but no
//! dedicated Uniswap *pool* address (only `swap_pool_id`, reserved for
//! Balancer's bytes32 pool id — see Task 6). Without a pool address the
//! `slot0` spot check can't run, so `UniswapV3Venue`'s `pool` field is
//! `Option`: `None` degrades `spot_in` to `expected_in` (0 bps impact, gate
//! never fires) rather than fabricating a number. A future config extension
//! can wire a real pool address through to light the gate up.

// The `sol!`-generated bindings include macro-emitted code that uses
// patterns workspace clippy denies (raw indexing, `unwrap` on infallible
// conversions). These allows scope the relaxation to this module only —
// same posture as `erc20`, `capacity_bond`, `payment_channel`.
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
use alloy::primitives::{Address, B256, U160, U256};
use alloy::providers::{DynProvider, Provider};
use anyhow::Context;

use crate::erc20::Erc20;
use crate::swap_math::max_in_with_slippage;
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
/// never the actual bounded spend — so saturating arithmetic on the
/// `sqrtPriceX96` squaring (which can in principle overflow `U256` near
/// Uniswap's `MAX_SQRT_RATIO`, ~2^160) is an acceptable simplification: a
/// saturated result yields an imprecise-but-harmless advisory number, not a
/// panic or a wrong bound on real spend.
pub(crate) fn spot_in_from_sqrt(
    sqrt_price_x96: U256,
    amount_out: U256,
    usdc_is_token0: bool,
) -> U256 {
    if sqrt_price_x96.is_zero() {
        return U256::ZERO;
    }
    let price_x192 = sqrt_price_x96.saturating_mul(sqrt_price_x96);
    if price_x192.is_zero() {
        return U256::ZERO;
    }
    let one_q192 = U256::from(1u8) << 192;
    if usdc_is_token0 {
        // price = TOKEN per USDC; USDC_in = amount_out(TOKEN) / price.
        amount_out.saturating_mul(one_q192) / price_x192
    } else {
        // price = USDC per TOKEN; USDC_in = amount_out(TOKEN) * price.
        amount_out.saturating_mul(price_x192) >> 192
    }
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
                spot_in_from_sqrt(U256::from(slot0.sqrtPriceX96), amount_out, usdc_is_token0)
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
    /// two need not coincide. `SwapRouter02.exactOutputSingle` has no `deadline`
    /// parameter (unlike the V1 router), so `_deadline` is unused here — it
    /// exists only to satisfy `SwapVenue::swap_exact_out`'s venue-neutral
    /// signature (Balancer, added in Task 6, does use one).
    pub async fn swap_exact_out(
        &self,
        amount_out: U256,
        max_in: U256,
        recipient: Address,
        _deadline: U256,
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
            let approve_pending = usdc
                .approve(self.router, max_in)
                .send()
                .await
                .context("USDC approve transaction failed to send")?;
            let approve_receipt = approve_pending
                .get_receipt()
                .await
                .context("USDC approve sent but the receipt could not be fetched")?;
            anyhow::ensure!(
                approve_receipt.status(),
                "USDC approve reverted (tx {})",
                approve_receipt.transaction_hash
            );
        }

        let router = SwapRouter02::new(self.router, &self.provider);
        let params = build_exact_output_params(
            self.usdc, self.token, self.fee, recipient, amount_out, max_in,
        );
        let pending = router
            .exactOutputSingle(params)
            .send()
            .await
            .context("exactOutputSingle transaction failed to send")?;
        let receipt = pending
            .get_receipt()
            .await
            .context("exactOutputSingle sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            receipt.status(),
            "exactOutputSingle reverted (tx {})",
            receipt.transaction_hash
        );
        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    const USDC: Address = Address::repeat_byte(0xA1);
    const TOKEN: Address = Address::repeat_byte(0xB2);
    const RECIPIENT: Address = Address::repeat_byte(0xC3);

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
}
