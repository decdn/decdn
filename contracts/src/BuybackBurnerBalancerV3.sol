// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

import { BuybackBurner } from "./BuybackBurner.sol";
import { GuardedBuybackBurner } from "./GuardedBuybackBurner.sol";
import { IBalancerV3Router } from "./interfaces/IBalancerV3Router.sol";
import { IBalancerV3Vault } from "./interfaces/IBalancerV3Vault.sol";
import { IBalancerV3WeightedPool } from "./interfaces/IBalancerV3WeightedPool.sol";
import { IPermit2 } from "./interfaces/IPermit2.sol";

/// @title BuybackBurnerBalancerV3
/// @notice Concrete `GuardedBuybackBurner` that binds the live Balancer V3
///         Router and implements the venue hooks behind the shared MEV stack
///         ([ADR 018](../adr/018-liquidity-strategy.md)): a single
///         `swapSingleTokenExactIn` USDC->TOKEN swap with a scoped Permit2
///         approval, the weighted-pool marginal spot feeding the inherited TWAP
///         floor, the live static swap fee, and the in-pool USDC depth feeding
///         the inherited per-epoch cap.
/// @dev    The MEV-defense stack (TWAP accumulator + floor, min/max band,
///         per-epoch cap) lives in `GuardedBuybackBurner`; this contract only
///         provides the Balancer-specific venue wiring and reads.
///
///         TWAP source: Balancer V3 (unlike V2) ships no built-in pool oracle,
///         and TOKEN has no external price feed at launch, so the inherited
///         accumulator samples this contract's `_spotPrice` (the pool's marginal
///         spot from Vault `*Scaled18` balances and pool weights).
contract BuybackBurnerBalancerV3 is GuardedBuybackBurner {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    /// @notice TWAP sub-swap calibration for the off-chain keeper (ADR 018
    ///         § TWAP policy). The contract performs exactly one Router swap per
    ///         `executeBuyback` call — the sub-swap cadence (spacing, count) is
    ///         a keeper concern across multiple calls; these are stored only for
    ///         keeper introspection / event-log correlation.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable subSwapCount;
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable subSwapMinBlockGap;

    /// @notice Uniswap Permit2 — the Balancer V3 Router's token-pull authority.
    ///         A V3 Router pulls `tokenIn` via `permit2.transferFrom`, so the
    ///         swap authorizes it through Permit2 (ERC20-approve Permit2 + a
    ///         scoped Permit2 allowance to the Router), not a direct Vault
    ///         allowance. Canonical on every chain but injected (not hardcoded)
    ///         so tests can substitute a mock; constructor rejects `address(0)`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IPermit2 public immutable permit2;

    // -----------------------------------------------------------------
    // Governance-mutable venue
    // -----------------------------------------------------------------

    /// @notice Balancer V3 Router (the swap call target, distinct from the
    ///         Vault). Set in the constructor and rotatable via `setSwapRouter`.
    IBalancerV3Router public swapRouter;

    /// @notice Balancer V3 Vault address, distinct from the Router. Used for
    ///         pool registration / state reads (e.g. `isPoolRegistered`), not as
    ///         an approval target — under V3 the Router pulls `tokenIn` via
    ///         Permit2 and settles to the Vault. Set via `setVault`.
    address public balancerVault;

    /// @notice Balancer V3 pool contract address for the 80/20 TOKEN/USDC pool.
    address public balancerPool;

    // -----------------------------------------------------------------
    // Events / Errors
    // -----------------------------------------------------------------

    event SwapRouterUpdated(address indexed oldAddr, address indexed newAddr);
    event PoolUpdated(address indexed oldAddr, address indexed newAddr);
    event VaultUpdated(address indexed oldAddr, address indexed newAddr);

    error PoolStateInvalid();
    error PoolNotRegistered(address pool, address vault);

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    /// @param swapRouter_ Balancer V3 Router (call target). Non-zero.
    /// @param pool_ 80/20 TOKEN/USDC weighted pool (may be zero — governance
    ///        wires it later via `setPool`; `executeBuyback` reverts
    ///        `PoolNotWired` until both pool and vault are set).
    /// @param vault_ Balancer V3 Vault (pool registration / state reads, not an
    ///        approval target; may be zero at deploy).
    /// @param permit2_ Canonical Uniswap Permit2 — the Router's token-pull
    ///        authority. Non-zero (constructor reverts `ZeroAddress` otherwise).
    struct Config {
        IBalancerV3Router swapRouter_;
        address pool_;
        address vault_;
        address permit2_;
        uint256 subSwapCount_;
        uint256 subSwapMinBlockGap_;
        uint256 twapMinWindow_;
        uint256 maxBuybackAmount_;
        uint256 minBuybackAmount_;
        uint256 slippageBps_;
        uint256 epochLiquidityCapFraction_;
    }

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, Config memory cfg)
        GuardedBuybackBurner(
            usdc_,
            token_,
            admin,
            GuardParams({
                twapMinWindow_: cfg.twapMinWindow_,
                maxBuybackAmount_: cfg.maxBuybackAmount_,
                minBuybackAmount_: cfg.minBuybackAmount_,
                slippageBps_: cfg.slippageBps_,
                epochLiquidityCapFraction_: cfg.epochLiquidityCapFraction_
            })
        )
    {
        if (address(cfg.swapRouter_) == address(0)) revert ZeroAddress();
        if (cfg.permit2_ == address(0)) revert ZeroAddress();

        swapRouter = cfg.swapRouter_;
        permit2 = IPermit2(cfg.permit2_);
        balancerPool = cfg.pool_;
        balancerVault = cfg.vault_;
        subSwapCount = cfg.subSwapCount_;
        subSwapMinBlockGap = cfg.subSwapMinBlockGap_;

        // Fail-fast on a pool/vault supplied at deploy (issue #968): a `Config`
        // that wires both must reference a Vault-registered pool containing at
        // least the {USDC, TOKEN} legs. A `Config` that defers wiring (pool_ or
        // vault_ == 0) skips this and is rejected later with `PoolNotWired`.
        _validatePoolWiring();
    }

    // -----------------------------------------------------------------
    // Venue hooks
    // -----------------------------------------------------------------

    /// @inheritdoc BuybackBurner
    function _requireWired() internal view override {
        if (balancerPool == address(0) || balancerVault == address(0)) revert PoolNotWired();
    }

    /// @inheritdoc GuardedBuybackBurner
    /// @dev Scoped Permit2 approval (base header invariant #2). A Balancer V3
    ///      Router pulls `tokenIn` via `permit2.transferFrom(this, vault, …)`,
    ///      NOT via a direct ERC20 allowance to the Vault — so authorize the
    ///      spend in two scoped legs: ERC20-approve Permit2 for exactly
    ///      `amountIn`, then grant the Router a Permit2 allowance for exactly
    ///      `amountIn`, with an `uint48(block.timestamp)` expiration. Confinement
    ///      comes from the step-3 reset plus Permit2's amount-decrement on
    ///      transfer — together they drive the allowance to 0 within this tx, so
    ///      no standing allowance survives. `deadline = block.timestamp` gives no
    ///      standing window; front-run protection comes from the inherited
    ///      `minOut`/TWAP floor plus the mandatory private-RPC bundle (ADR 018).
    function _swap(uint256 amountIn, uint256 minOut) internal override returns (uint256 tokenOut) {
        usdc.forceApprove(address(permit2), amountIn);
        // `uint160(amountIn)` cannot truncate: `amountIn` is bounded above by the
        // base-layer `amountIn <= usdc.balanceOf(this)` check (in the
        // `BuybackBurner` parent, so aderyn cannot see the bound across
        // the contract boundary and flags the downcast), and 6-dec USDC total
        // supply is ~30 orders of magnitude below 2^160.
        // forge-lint: disable-next-line(block-timestamp)
        // aderyn-ignore-next-line(unsafe-casting)
        permit2.approve(address(usdc), address(swapRouter), uint160(amountIn), uint48(block.timestamp));

        // forge-lint: disable-next-line(block-timestamp)
        tokenOut = swapRouter.swapSingleTokenExactIn(
            balancerPool, usdc, IERC20(address(token)), amountIn, minOut, block.timestamp, false, ""
        );

        // Reset both legs unconditionally — a Router that pulls less than
        // `amountIn` would otherwise leave a residual standing allowance.
        permit2.approve(address(usdc), address(swapRouter), 0, 0);
        usdc.forceApprove(address(permit2), 0);
    }

    /// @inheritdoc GuardedBuybackBurner
    function _swapFeeE18() internal view override returns (uint256) {
        return IBalancerV3Vault(balancerVault).getStaticSwapFeePercentage(balancerPool);
    }

    /// @inheritdoc GuardedBuybackBurner
    /// @dev Marginal TOKEN-per-USDC spot (1e18 fixed point) for the weighted
    ///      pool: out-per-in = (wIn * balOut) / (balIn * wOut), with in = USDC,
    ///      out = TOKEN. Balances are Vault `*Scaled18`; weights are 1e18.
    function _spotPrice() internal view override returns (uint256) {
        (uint256 balUsdc18, uint256 balToken18, uint256 wUsdc, uint256 wToken) = _poolState();
        // = (wUsdc * balToken18 * WAD) / (balUsdc18 * wToken), nested through
        // `mulDiv` so neither product is formed in plain uint256 (no
        // pre-multiplication overflow for large pools). WAD is applied first to
        // preserve precision before the weight ratio.
        return Math.mulDiv(Math.mulDiv(balToken18, WAD, balUsdc18), wUsdc, wToken);
    }

    /// @inheritdoc GuardedBuybackBurner
    function _usdcDepthRaw() internal view override returns (uint256) {
        (uint256 balUsdc18,,,) = _poolState();
        return balUsdc18 / usdcTo18;
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE, via the 48h timelock)
    // -----------------------------------------------------------------

    function setSwapRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE) {
        if (newRouter == address(0)) revert ZeroAddress();
        address old = address(swapRouter);
        swapRouter = IBalancerV3Router(newRouter);
        emit SwapRouterUpdated(old, newRouter);
    }

    /// @notice Wire/rotate the Balancer pool. Once both pool and vault are set,
    ///         the new pool must be Vault-registered and contain at least the
    ///         {USDC, TOKEN} legs, else this reverts (`PoolNotRegistered` /
    ///         `PoolStateInvalid`) instead of silently accepting a mis-wired or
    ///         substituted pool (issue #968). `newPool == address(0)` stays the
    ///         documented "not wired" state (validation skipped); prefer
    ///         `pause()` to disable without unwiring.
    // slither-disable-next-line missing-zero-check
    function setPool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        address old = balancerPool;
        balancerPool = newPool;
        emit PoolUpdated(old, newPool);
        _validatePoolWiring();
        // A pool rotation changes the spot/depth source: reset the inherited
        // TWAP accumulator and per-epoch cap so they re-derive against the new
        // pool (fail-closed `TwapNotReady` + fresh depth snapshot) rather than
        // carrying stale state from the old pool.
        if (newPool != old) _resetGuardState();
    }

    /// @notice Wire/rotate the Balancer Vault. Symmetric to `setPool`: revalidates
    ///         the (pool, vault) pair so a Vault rotation the current pool is not
    ///         registered with fails fast. Note the new Vault is the unverified
    ///         trust root — `_validatePoolWiring` asks it whether the pool is
    ///         registered AND reads the pool's leg balances from it, so a
    ///         malicious Vault can satisfy both. That residual surface is bounded
    ///         by `GOVERNANCE_ROLE` + the 48h timelock, not by this check.
    // slither-disable-next-line missing-zero-check
    function setVault(address newVault) external onlyRole(GOVERNANCE_ROLE) {
        address old = balancerVault;
        balancerVault = newVault;
        emit VaultUpdated(old, newVault);
        _validatePoolWiring();
        // The Vault is part of the spot/depth source (`_poolState` reads leg
        // balances through it): a Vault rotation resets the inherited TWAP
        // accumulator and per-epoch cap so they re-derive against the new
        // Vault/pool pair rather than carrying stale state from the old Vault.
        if (newVault != old) _resetGuardState();
    }

    // -----------------------------------------------------------------
    // Internal — pool reads
    // -----------------------------------------------------------------

    /// @dev Fail-fast pool-wiring guard, shared by the constructor and the
    ///      `setPool`/`setVault` setters. No-ops while either leg of the
    ///      (pool, vault) pair is unwired (`address(0)`) — that state stays the
    ///      documented "not wired" path rejected later by `PoolNotWired`. Once
    ///      both are set, requires the pool to be registered with the configured
    ///      Vault (`PoolNotRegistered`) and to contain at least the {USDC, TOKEN}
    ///      legs with non-zero balances/weights (`PoolStateInvalid`); additional
    ///      (decoy) legs are tolerated, exactly as at swap time.
    function _validatePoolWiring() internal view {
        address pool = balancerPool;
        address vault = balancerVault;
        if (pool == address(0) || vault == address(0)) return;
        if (!IBalancerV3Vault(vault).isPoolRegistered(pool)) revert PoolNotRegistered(pool, vault);
        // Discards the returned balances/weights — called only for its
        // {USDC, TOKEN}-legs-present-and-non-zero revert (`PoolStateInvalid`).
        // This revert is load-bearing for the guard: `_poolState` MUST revert on
        // an invalid leg set (do not soften it to return zeros).
        _poolState();
    }

    /// @dev Reads the Vault's scaled-18 balances + the pool's normalized weights
    ///      and locates the USDC and TOKEN legs by address (ignoring any extra
    ///      legs). Reverts `PoolStateInvalid` if either leg is absent, zero-
    ///      balance, or zero-weight, or if the Vault's token/balance arrays and
    ///      the pool's weight array disagree in length — relied on by
    ///      `_validatePoolWiring` and the swap path (which is why it MUST revert
    ///      rather than return zeros).
    function _poolState() internal view returns (uint256 balUsdc18, uint256 balToken18, uint256 wUsdc, uint256 wToken) {
        IBalancerV3Vault vault = IBalancerV3Vault(balancerVault);
        IERC20[] memory tokens = vault.getPoolTokens(balancerPool);
        uint256[] memory balances = vault.getCurrentLiveBalances(balancerPool);
        uint256[] memory weights = IBalancerV3WeightedPool(balancerPool).getNormalizedWeights();
        uint256 n = tokens.length;
        if (balances.length != n || weights.length != n) revert PoolStateInvalid();

        bool foundUsdc = false;
        bool foundToken = false;
        for (uint256 i = 0; i < n; ++i) {
            address t = address(tokens[i]);
            if (t == address(usdc)) {
                balUsdc18 = balances[i];
                wUsdc = weights[i];
                foundUsdc = true;
            } else if (t == address(token)) {
                balToken18 = balances[i];
                wToken = weights[i];
                foundToken = true;
            }
        }
        if (!foundUsdc || !foundToken || balUsdc18 == 0 || balToken18 == 0 || wUsdc == 0 || wToken == 0) {
            revert PoolStateInvalid();
        }
    }
}
