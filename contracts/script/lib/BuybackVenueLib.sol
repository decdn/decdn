// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { BuybackBurnerBalancerV3 } from "../../src/BuybackBurnerBalancerV3.sol";
import { BuybackBurnerUniswapV3 } from "../../src/BuybackBurnerUniswapV3.sol";
import { GuardedBuybackBurner } from "../../src/GuardedBuybackBurner.sol";
import { IBalancerV3Router } from "../../src/interfaces/IBalancerV3Router.sol";
import { IUniswapV3SwapRouter } from "../../src/interfaces/IUniswapV3SwapRouter.sol";

/// @title BuybackVenueLib — the one place buyback venue selection and concrete
///        `GuardedBuybackBurner` construction live.
/// @notice Two entry points construct the buyback burner and must wire an identical
///         one from identical inputs (only the genesis path also *activates* the
///         bucket; the runbook script prints the calldata for governance to do it): the deploy-time genesis path
///         (`BaseProtocolDeploy._activateBuyback`, driven by `DeployProtocol`'s
///         env reader) and the post-deploy operator runbook
///         (`ActivateBuyback.s.sol`). This library is the single source of the
///         steady-state split, the canonical Permit2 address, the venue-string
///         dispatch, and the per-venue `Config` literal, so the two paths cannot
///         drift apart and wire different burners from the same inputs.
///
/// @dev    Deliberately a `library`, not a `Script` base: it touches no
///         cheatcodes, so each caller keeps owning where its inputs come from
///         (env vars for the runbook, the `BuybackActivation` struct for the
///         genesis path) and only the venue semantics are shared. Two caller
///         asymmetries are therefore preserved rather than erased — the genesis
///         path passes the *deployer* as burner admin (so it can `setKeeper`
///         in-script before the governance handoff) while the runbook passes the
///         Timelock, and the genesis path creates + seeds the pool itself while
///         the runbook assumes a live one.
library BuybackVenueLib {
    /// @notice Swap venue the buyback bucket executes against. Both concrete
    ///         burners subclass `GuardedBuybackBurner` and share its MEV-defense
    ///         stack, roles, and `FeeRouter` wiring (ADR 018 § Venue-neutral
    ///         burner selection); only the swap leg differs.
    enum Venue {
        UNISWAP,
        BALANCER
    }

    /// @notice `BUYBACK_VENUE` did not name a supported venue.
    error UnknownBuybackVenue(string venue);
    /// @notice A `Venue` variant reached a dispatch site that does not handle it.
    ///         Solidity has no exhaustive match, so every dispatch on this enum ends in
    ///         an explicit `else revert` carrying this — otherwise adding a variant
    ///         silently reinterprets it as whichever venue the fallthrough arm names,
    ///         and the two entry points this library exists to keep in step would fall
    ///         through to *different* venues.
    error UnknownVenueVariant(uint8 venue);
    /// @notice A burner was about to be constructed with a required field left at
    ///         zero. `field` names it. Lives here rather than on the deploy script
    ///         because BOTH entry points reach the burner through this library —
    ///         `ActivateBuyback` builds its wiring straight from `vm.envAddress`,
    ///         which rejects an *unset* var but happily parses an explicit `0x0`.
    ///         A guard on the deploy script alone leaves the runbook path unprotected,
    ///         which is the two-entry-point drift this library removes.
    error WiringIncomplete(string field);
    /// @notice `maxBuybackAmount_` is zero — a burner receiving the buyback
    ///         bucket's share of revenue that cannot spend it, because
    ///         `amountIn > maxBuybackAmount` reverts every non-zero buyback
    ///         until governance raises the ceiling (48h timelock).
    ///         Reachable from production env as
    ///         `MIN_BUYBACK_AMOUNT=0 MAX_BUYBACK_AMOUNT=0`, the "0 means
    ///         unlimited" misreading. `GuardedBuybackBurner.BuybackBandDead` is
    ///         the authority; this is a pre-broadcast
    ///         fail-fast — see `_requireLiveGuardBand`.
    error GuardBandDead();

    // Steady-state FeeRouter split once buyback is live (ADR 026 § FeeRouter
    // split, ADR 016 § Deployment Order): 60% operator / 30% buyback / 10% treasury.
    uint256 internal constant STEADY_OPERATOR_SHARE = 6000;
    uint256 internal constant STEADY_BUYBACK_SHARE = 3000;
    uint256 internal constant STEADY_TREASURY_SHARE = 1000;

    /// @dev Canonical Uniswap Permit2 (same CREATE2 address on every chain); the
    ///      Balancer V3 Router pulls the swap's input USDC through it.
    address internal constant CANONICAL_PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;

    // The shared MEV-defense guard band is NOT redeclared here. It is
    // `GuardedBuybackBurner.GuardParams` (ADR 018 § Parameter Table) — venue-
    // independent by construction, since it lives on the shared base rather than on
    // either subclass. A local copy would be a fourth name for one concept
    // (base struct -> UniswapV3.Config -> BalancerV3.Config -> here). Not a
    // correctness argument: a new field on the base breaks the named-args literals
    // in both burner constructors, which breaks the `Config` literals below, so it
    // fails to compile either way. The cost of the copy is the extra hop to update
    // and the extra name, which is reason enough not to have one.

    /// @notice Everything `BuybackBurnerBalancerV3`'s constructor needs beyond the
    ///         tokens, admin, and guard band. Deliberately narrower than the
    ///         genesis path's `BalancerVenueParams`: the factory and pool swap fee
    ///         are pool-*creation* inputs the runbook script has no use for.
    struct BalancerWiring {
        address swapRouter;
        address pool;
        address vault;
        address permit2;
    }

    /// @notice Resolve a `BUYBACK_VENUE` string to the enum. Case-sensitive and
    ///         exact — a typo reverts rather than silently defaulting, because the
    ///         venue decides which pool the protocol's revenue is swapped against.
    function parseVenue(string memory venue) internal pure returns (Venue) {
        bytes32 h = keccak256(bytes(venue));
        if (h == keccak256("uniswap")) return Venue.UNISWAP;
        if (h == keccak256("balancer")) return Venue.BALANCER;
        revert UnknownBuybackVenue(venue);
    }

    /// @notice The steady-state `FeeRouter` share vector, in the operator /
    ///         buyback / treasury bucket order `setSharesAndDestinations` expects.
    function steadyShares() internal pure returns (uint256[3] memory) {
        return [STEADY_OPERATOR_SHARE, STEADY_BUYBACK_SHARE, STEADY_TREASURY_SHARE];
    }

    /// @notice Reject an incomplete Balancer wiring or a dead guard band, before any
    ///         deployment. Separated from `deployBalancerBurner` so it can be exercised
    ///         directly: a harness calling the builder would inline both burners'
    ///         creation bytecode and blow past EIP-170.
    ///
    /// @dev    `vault` is the one field with no downstream backstop — a zero vault is
    ///         `BuybackBurnerBalancerV3`'s documented deferred-wiring path, so the
    ///         constructor skips validation and the burner reverts `PoolNotWired` on
    ///         every swap forever. A zero `swapRouter` or `permit2` would hit
    ///         `ZeroAddress` in that constructor anyway; naming them here only makes
    ///         the failure legible.
    function requireBalancerWiring(BalancerWiring memory wiring, GuardedBuybackBurner.GuardParams memory guard)
        internal
        pure
    {
        if (wiring.swapRouter == address(0)) revert WiringIncomplete("balancer.swapRouter");
        if (wiring.pool == address(0)) revert WiringIncomplete("balancer.pool");
        if (wiring.vault == address(0)) revert WiringIncomplete("balancer.vault");
        if (wiring.permit2 == address(0)) revert WiringIncomplete("balancer.permit2");
        _requireLiveGuardBand(guard);
    }

    /// @notice The Uniswap counterpart. Both terms are caught downstream too — a zero
    ///         router by the burner constructor's `ZeroAddress` — so this converts
    ///         opaque reverts into named ones rather than closing a silent hole, and
    ///         applies the same guard-band check.
    function requireUniswapWiring(address swapRouter, address pool, GuardedBuybackBurner.GuardParams memory guard)
        internal
        pure
    {
        if (swapRouter == address(0)) revert WiringIncomplete("uniswap.swapRouter");
        if (pool == address(0)) revert WiringIncomplete("uniswap.pool");
        _requireLiveGuardBand(guard);
    }

    /// @dev Reject a guard band that can never execute. `GuardedBuybackBurner`'s
    ///      constructor reverts `BuybackBandDead` on `max == 0` and is the
    ///      authority; this duplicates the check as a fail-fast at the seam both
    ///      deploy paths share, so a mis-set env var surfaces before the deploy
    ///      transaction is broadcast rather than as a reverted deployment mid-run.
    function _requireLiveGuardBand(GuardedBuybackBurner.GuardParams memory guard) private pure {
        if (guard.maxBuybackAmount_ == 0) revert GuardBandDead();
    }

    /// @notice Deploy the Uniswap V3 burner bound to `pool` and `swapRouter`
    ///         (SwapRouter02). `admin` receives `DEFAULT_ADMIN_ROLE` +
    ///         `GOVERNANCE_ROLE`; the caller is responsible for handing those on.
    function deployUniswapBurner(
        IERC20 usdc,
        ERC20Burnable token,
        address admin,
        address swapRouter,
        address pool,
        GuardedBuybackBurner.GuardParams memory guard
    ) internal returns (GuardedBuybackBurner) {
        requireUniswapWiring(swapRouter, pool, guard);
        return new BuybackBurnerUniswapV3(
            usdc,
            token,
            admin,
            BuybackBurnerUniswapV3.Config({
                swapRouter_: IUniswapV3SwapRouter(swapRouter),
                pool_: pool,
                twapMinWindow_: guard.twapMinWindow_,
                maxBuybackAmount_: guard.maxBuybackAmount_,
                minBuybackAmount_: guard.minBuybackAmount_,
                slippageBps_: guard.slippageBps_,
                epochLiquidityCapFraction_: guard.epochLiquidityCapFraction_
            })
        );
    }

    /// @notice Deploy the Balancer V3 burner bound to `wiring`'s 80/20 pool, Vault,
    ///         Router, and Permit2. `admin` receives `DEFAULT_ADMIN_ROLE` +
    ///         `GOVERNANCE_ROLE`; the caller is responsible for handing those on.
    ///         The constructor validates the pool is registered with `vault` and
    ///         carries live {USDC, TOKEN} legs — but only once BOTH `pool` and `vault`
    ///         are non-zero; a zero either side is the deferred-wiring path, accepted
    ///         here and rejected later at swap time with `PoolNotWired`.
    function deployBalancerBurner(
        IERC20 usdc,
        ERC20Burnable token,
        address admin,
        BalancerWiring memory wiring,
        GuardedBuybackBurner.GuardParams memory guard
    ) internal returns (GuardedBuybackBurner) {
        requireBalancerWiring(wiring, guard);
        return new BuybackBurnerBalancerV3(
            usdc,
            token,
            admin,
            BuybackBurnerBalancerV3.Config({
                swapRouter_: IBalancerV3Router(wiring.swapRouter),
                pool_: wiring.pool,
                vault_: wiring.vault,
                permit2_: wiring.permit2,
                twapMinWindow_: guard.twapMinWindow_,
                maxBuybackAmount_: guard.maxBuybackAmount_,
                minBuybackAmount_: guard.minBuybackAmount_,
                slippageBps_: guard.slippageBps_,
                epochLiquidityCapFraction_: guard.epochLiquidityCapFraction_
            })
        );
    }
}
