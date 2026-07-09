//! Balancer V3 exact-out swap venue (#991).
//!
//! Buys exactly `amount_out` TOKEN with USDC via the Balancer V3 `Router`'s
//! `swapSingleTokenExactOut`, bounding spend with `maxAmountIn` derived from
//! the Router's `querySwapSingleTokenExactOut` +
//! [`crate::swap_math::max_in_with_slippage`].
//!
//! Unlike Uniswap's `SwapRouter02`, a Balancer V3 Router pulls `tokenIn` via
//! **Uniswap Permit2** (`permit2.transferFrom`), not a direct ERC20 allowance
//! to the Router — see `contracts/src/BuybackBurnerBalancerV3.sol::_swap`
//! (the reference implementation this module mirrors) and
//! `contracts/src/interfaces/IPermit2.sol`. `swap_exact_out` performs the same
//! two-leg scoped approval: ERC20-approve Permit2 for `max_in`, grant the
//! Router a Permit2 allowance for `max_in`, swap, then reset both legs to `0`
//! so no standing allowance survives — mirroring the reference's confinement
//! invariant (its doc comment: "no standing allowance survives").
//!
//! Permit2 is deployed at the same canonical address on every EVM chain
//! (Nick's-method CREATE2 — see `IPermit2.sol`'s doc comment), so it is
//! hardcoded here rather than threaded through [`crate::swap_venue::ResolvedSwap`].
//!
//! ## Deliberate deviation from the reference: Permit2 `expiration`
//!
//! `BuybackBurnerBalancerV3::_swap` sets `expiration = uint48(block.timestamp)`
//! because its Permit2-approve and Router-swap calls happen **atomically in
//! one transaction** — same `block.timestamp` for both, so an
//! already-"expired-looking" value is fine (Permit2 checks
//! `block.timestamp <= expiration`, and both calls share that timestamp).
//! This venue is an off-chain client issuing **three separate transactions**
//! (ERC20 approve → Permit2 approve → Router swap), so copying
//! `expiration = now` verbatim would create a race: by the time the Router
//! swap transaction lands (even one block later), `block.timestamp` would
//! already exceed the submitted `expiration`, and Permit2 would revert
//! `AllowanceExpired`. Instead, `expiration` is scoped to the caller-supplied
//! `deadline` (the same value bounding the swap itself), which preserves the
//! "expires, does not persist" invariant while giving the transaction
//! sequence room to land on-chain.
//!
//! ## ABI provenance (read before touching this file)
//!
//! `contracts/src/interfaces/IBalancerV3Router.sol` only vendors the
//! **exact-in** entrypoint (`swapSingleTokenExactIn`) because that is the only
//! direction the on-chain buyback path (`BuybackBurnerBalancerV3`) needs. This
//! venue needs the **exact-out** direction instead (the CDN bond-funding flow
//! buys a fixed amount of TOKEN, bounding USDC spend) — no exact-out or query
//! ABI is vendored anywhere in this repo, so there is no repo-local reference
//! to mirror for `swapSingleTokenExactOut` / `querySwapSingleTokenExactOut`.
//! The signatures below are built by mirroring the *shape* of the vendored
//! exact-in interface (swap `exactAmountIn`/`minAmountOut` for
//! `exactAmountOut`/`maxAmountIn`; same `pool`/`tokenIn`/`tokenOut`/
//! `deadline`/`wethIsEth`/`userData` ordering and the query counterpart's
//! `sender` parameter for static-call context) against the public Balancer V3
//! `Router` ABI. **This is the one part of this module not verified against a
//! repo-local reference** — flagged in the task-6 report per the honesty
//! clause.

// The `sol!`-generated bindings include macro-emitted code that uses
// patterns workspace clippy denies (raw indexing, `unwrap` on infallible
// conversions). These allows scope the relaxation to this module only —
// same posture as `erc20`, `capacity_bond`, `swap_uniswap`.
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
        contract BalancerV3Router {
            function swapSingleTokenExactOut(
                address pool,
                address tokenIn,
                address tokenOut,
                uint256 exactAmountOut,
                uint256 maxAmountIn,
                uint256 deadline,
                bool wethIsEth,
                bytes calldata userData
            ) external payable returns (uint256 amountIn);

            function querySwapSingleTokenExactOut(
                address pool,
                address tokenIn,
                address tokenOut,
                uint256 exactAmountOut,
                address sender,
                bytes calldata userData
            ) external returns (uint256 amountIn);
        }
        #[sol(rpc)]
        contract Permit2 {
            function approve(address token, address spender, uint160 amount, uint48 expiration)
                external;
        }
    }
}

pub use sol_types::{BalancerV3Router, Permit2};

use alloy::primitives::aliases::U48;
use alloy::primitives::{Address, B256, Bytes, U160, U256, address};
use alloy::providers::{DynProvider, Provider};
use anyhow::Context;

use crate::erc20::Erc20;
use crate::swap_math::max_in_with_slippage;
use crate::swap_venue::Quote;

/// Canonical Uniswap Permit2 address — identical on every EVM chain via
/// Nick's-method CREATE2 deployment (see `IPermit2.sol`'s doc comment; also
/// vendored in this repo at `contracts/lib/solady/src/utils/SafeTransferLib.sol`).
pub const PERMIT2_ADDRESS: Address = address!("000000000022d473030f116ddee9f6b43ac78ba3");

/// Arguments for `Router.swapSingleTokenExactOut`, assembled as a plain
/// struct purely for testability — the Solidity function takes positional
/// args (Balancer V3 has no equivalent of Uniswap's `ExactOutputSingleParams`
/// struct).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactOutSwapArgs {
    pub pool: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub exact_amount_out: U256,
    pub max_amount_in: U256,
    pub deadline: U256,
    pub weth_is_eth: bool,
    pub user_data: Bytes,
}

/// Build the `swapSingleTokenExactOut` args for a USDC->TOKEN exact-out swap.
/// Always `wethIsEth = false` (deCDN never swaps native ETH) and empty
/// `userData` (no hooks are used — ADR 018 restricts POL pools to "standard
/// V3 Weighted Pools with no hooks").
pub(crate) const fn build_exact_out_swap_args(
    pool: Address,
    token_in: Address,
    token_out: Address,
    exact_amount_out: U256,
    max_amount_in: U256,
    deadline: U256,
) -> ExactOutSwapArgs {
    ExactOutSwapArgs {
        pool,
        token_in,
        token_out,
        exact_amount_out,
        max_amount_in,
        deadline,
        weth_is_eth: false,
        user_data: Bytes::new(),
    }
}

/// A configured Balancer V3 exact-out venue: `Router` for both the query and
/// the swap (Balancer V3 exposes `query...` directly on the Router, unlike
/// Uniswap's separate `QuoterV2`), and the canonical Permit2 contract for the
/// token-pull authorization.
#[derive(Debug, Clone)]
pub struct BalancerV3Venue {
    provider: DynProvider,
    router: Address,
    permit2: Address,
    pool: Address,
    usdc: Address,
    token: Address,
    payer: Address,
}

impl BalancerV3Venue {
    /// Construct a venue against `provider`. `pool` is the Balancer V3 pool
    /// contract address (V3 pools are addressed directly — unlike V2, there
    /// is no bytes32 `poolId` indirection through the Vault). `payer` is the
    /// account whose USDC funds the swap (the signer behind `provider`);
    /// Balancer V3 always sends swap output to that account, so
    /// `swap_exact_out` requires its `recipient` to equal `payer`.
    pub fn new(
        provider: impl Provider + Clone + 'static,
        router: Address,
        pool: Address,
        usdc: Address,
        token: Address,
        payer: Address,
    ) -> Self {
        Self {
            provider: provider.erased(),
            router,
            permit2: PERMIT2_ADDRESS,
            pool,
            usdc,
            token,
            payer,
        }
    }

    /// Quote amount-in for `amount_out` TOKEN via `Router.querySwapSingleTokenExactOut`,
    /// applying `slippage_bps` to `max_in`. Balancer has no cheap slot0-style
    /// spot-price read (unlike a Uniswap V3 pool), so `spot_in` falls back to
    /// `expected_in` — the same neutral fallback `UniswapV3Venue` uses when no
    /// pool is configured (the price-impact gate stays inert; see module docs
    /// on `swap_uniswap`'s `spot_in`). Do not fabricate a spot source.
    pub async fn quote_exact_out(
        &self,
        amount_out: U256,
        slippage_bps: u16,
    ) -> anyhow::Result<Quote> {
        let router = BalancerV3Router::new(self.router, &self.provider);
        // `sender` only affects hookable / per-address-behavior pools; ADR 018
        // restricts the deployed pool to a standard weighted pool with no
        // hooks, so the zero address is a safe query-context placeholder.
        let expected_in = router
            .querySwapSingleTokenExactOut(
                self.pool,
                self.usdc,
                self.token,
                amount_out,
                Address::ZERO,
                Bytes::new(),
            )
            .call()
            .await
            .context("Router.querySwapSingleTokenExactOut failed")?;
        let max_in = max_in_with_slippage(expected_in, slippage_bps);

        Ok(Quote {
            expected_in,
            max_in,
            spot_in: expected_in,
        })
    }

    /// Execute the exact-out swap. Unlike `UniswapV3Venue`, the caller does
    /// **not** pre-approve the router directly: this method performs the full
    /// Permit2 dance itself (mirroring `BuybackBurnerBalancerV3::_swap`) since
    /// a Balancer V3 Router pulls `tokenIn` via Permit2, not a direct ERC20
    /// allowance.
    ///
    /// `recipient` is accepted only to satisfy `SwapVenue::swap_exact_out`'s
    /// venue-neutral signature: unlike Uniswap's `SwapRouter02`, Balancer V3's
    /// `Router.swapSingleTokenExactOut` has no recipient parameter — the swap
    /// output always settles to the Router's caller (`msg.sender`, i.e. the
    /// address behind `provider`'s signer). Callers MUST configure `provider`
    /// with `recipient` as its signer; this venue has no way to redirect
    /// output to a different address.
    pub async fn swap_exact_out(
        &self,
        amount_out: U256,
        max_in: U256,
        recipient: Address,
        deadline: U256,
    ) -> anyhow::Result<B256> {
        // Balancer V3 has no recipient slot: output always settles to the
        // signer (the payer). A `recipient` != payer would silently misroute,
        // so reject it up front rather than paying to the wrong address.
        anyhow::ensure!(
            recipient == self.payer,
            "Balancer V3 sends swap output to the signer; recipient {recipient} must equal the \
             payer {}",
            self.payer
        );

        let usdc = Erc20::new(self.usdc, &self.provider);
        let permit2 = Permit2::new(self.permit2, &self.provider);
        let router = BalancerV3Router::new(self.router, &self.provider);

        // Leg 1: ERC20-approve Permit2 for max_in (mirrors the reference's
        // `usdc.forceApprove(address(permit2), amountIn)`).
        let approve_permit2_pending = usdc
            .approve(self.permit2, max_in)
            .send()
            .await
            .context("USDC approve(Permit2) transaction failed to send")?;
        let approve_permit2_receipt = approve_permit2_pending
            .get_receipt()
            .await
            .context("USDC approve(Permit2) sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            approve_permit2_receipt.status(),
            "USDC approve(Permit2) reverted (tx {})",
            approve_permit2_receipt.transaction_hash
        );

        // Leg 2: grant the Router a Permit2 allowance, scoped to `deadline`
        // (see module docs — deviates from the reference's `block.timestamp`
        // expiration, which is only safe because its two calls are atomic).
        // `max_in`/`deadline` truncation to uint160/uint48 mirrors the
        // reference's `uint160(amountIn)` cast: USDC's 6-decimal total supply
        // and Unix timestamps are both many orders of magnitude below the
        // truncation ceiling, so saturation is unreachable in practice; this
        // keeps the conversion infallible rather than threading a `Result`
        // through what is effectively dead-code error handling.
        let max_in_u160 = U160::saturating_from(max_in);
        let expiration_u48 = U48::saturating_from(deadline);
        let permit2_approve_pending = permit2
            .approve(self.usdc, self.router, max_in_u160, expiration_u48)
            .send()
            .await
            .context("Permit2 approve transaction failed to send")?;
        let permit2_approve_receipt = permit2_approve_pending
            .get_receipt()
            .await
            .context("Permit2 approve sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            permit2_approve_receipt.status(),
            "Permit2 approve reverted (tx {})",
            permit2_approve_receipt.transaction_hash
        );

        let args = build_exact_out_swap_args(
            self.pool, self.usdc, self.token, amount_out, max_in, deadline,
        );
        let pending = router
            .swapSingleTokenExactOut(
                args.pool,
                args.token_in,
                args.token_out,
                args.exact_amount_out,
                args.max_amount_in,
                args.deadline,
                args.weth_is_eth,
                args.user_data,
            )
            .send()
            .await
            .context("swapSingleTokenExactOut transaction failed to send")?;
        let receipt = pending
            .get_receipt()
            .await
            .context("swapSingleTokenExactOut sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            receipt.status(),
            "swapSingleTokenExactOut reverted (tx {})",
            receipt.transaction_hash
        );
        let tx_hash = receipt.transaction_hash;

        // Reset both legs — mirrors the reference's unconditional post-swap
        // reset so no standing allowance survives. Best-effort: the swap has
        // already succeeded (the operator now holds TOKEN), so a failed reset
        // must NOT abort onboarding or be misreported as a swap failure. Each
        // helper warns and continues, so the successful swap tx hash is returned
        // regardless (see module docs for why the non-atomic, multi-transaction
        // error path can't mirror the reference's automatic on-revert rollback).
        self.reset_permit2_allowance().await;
        self.reset_usdc_approval().await;

        Ok(tx_hash)
    }

    /// Best-effort reset of the Router's Permit2 allowance to zero. Warns and
    /// returns on any failure — the swap has already succeeded, so a stale
    /// Router allowance must not abort onboarding (see `swap_exact_out`).
    async fn reset_permit2_allowance(&self) {
        let permit2 = Permit2::new(self.permit2, &self.provider);
        let outcome = async {
            let pending = permit2
                .approve(self.usdc, self.router, U160::ZERO, U48::ZERO)
                .send()
                .await
                .context("failed to send")?;
            let receipt = pending
                .get_receipt()
                .await
                .context("sent but the receipt could not be fetched")?;
            anyhow::ensure!(
                receipt.status(),
                "reverted (tx {})",
                receipt.transaction_hash
            );
            anyhow::Ok(())
        }
        .await;
        if let Err(e) = outcome {
            tracing::warn!(
                error = %e,
                "Permit2 allowance reset failed; swap already succeeded, continuing (a stale \
                 Router allowance may survive)"
            );
        }
    }

    /// Best-effort reset of the USDC→Permit2 ERC20 approval to zero. Warns and
    /// returns on any failure — the swap has already succeeded, so a stale
    /// Permit2 approval must not abort onboarding (see `swap_exact_out`).
    async fn reset_usdc_approval(&self) {
        let usdc = Erc20::new(self.usdc, &self.provider);
        let outcome = async {
            let pending = usdc
                .approve(self.permit2, U256::ZERO)
                .send()
                .await
                .context("failed to send")?;
            let receipt = pending
                .get_receipt()
                .await
                .context("sent but the receipt could not be fetched")?;
            anyhow::ensure!(
                receipt.status(),
                "reverted (tx {})",
                receipt.transaction_hash
            );
            anyhow::Ok(())
        }
        .await;
        if let Err(e) = outcome {
            tracing::warn!(
                error = %e,
                "USDC approve(Permit2) reset failed; swap already succeeded, continuing (a stale \
                 Permit2 allowance may survive)"
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
    const POOL: Address = Address::repeat_byte(0xD4);
    const ROUTER: Address = Address::repeat_byte(0xE5);

    /// A provider that never dials out: the `recipient != payer` guard fires
    /// before any RPC call, so this is safe (see `swap_venue`'s test note).
    fn unconnected_provider() -> impl Provider + Clone + 'static {
        alloy::providers::ProviderBuilder::new().connect_http("http://127.0.0.1:1".parse().unwrap())
    }

    #[tokio::test]
    async fn swap_exact_out_rejects_recipient_ne_payer() {
        let payer = Address::repeat_byte(0x11);
        let recipient = Address::repeat_byte(0x22);
        let venue = BalancerV3Venue::new(unconnected_provider(), ROUTER, POOL, USDC, TOKEN, payer);
        let err = venue
            .swap_exact_out(
                U256::from(1u64),
                U256::from(1u64),
                recipient,
                U256::from(0u64),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must equal the payer"), "{err}");
    }

    #[test]
    fn exact_out_swap_args_are_usdc_in_token_out() {
        let args = build_exact_out_swap_args(
            POOL,
            USDC,
            TOKEN,
            U256::from(500u64),
            U256::from(1_030_000u64),
            U256::from(9_999_999u64),
        );
        assert_eq!(args.pool, POOL);
        assert_eq!(args.token_in, USDC);
        assert_eq!(args.token_out, TOKEN);
        assert_eq!(args.exact_amount_out, U256::from(500u64));
        assert_eq!(args.max_amount_in, U256::from(1_030_000u64));
        assert_eq!(args.deadline, U256::from(9_999_999u64));
        assert!(!args.weth_is_eth);
        assert!(args.user_data.is_empty());
    }

    #[test]
    fn permit2_address_is_canonical() {
        // Canonical Uniswap Permit2 address, identical on every EVM chain.
        assert_eq!(
            PERMIT2_ADDRESS,
            "0x000000000022D473030F116dDEE9F6B43aC78BA3"
                .parse::<Address>()
                .unwrap()
        );
    }
}
