// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IERC20Metadata } from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

import { BuybackBurner } from "./BuybackBurner.sol";
import { IBalancerV3Router } from "./interfaces/IBalancerV3Router.sol";
import { IBalancerV3Vault } from "./interfaces/IBalancerV3Vault.sol";
import { IBalancerV3WeightedPool } from "./interfaces/IBalancerV3WeightedPool.sol";

/// @title BuybackBurnerBalancerV3
/// @notice Concrete, deployable `BuybackBurner` that binds the live Balancer V3
///         Router and implements the full on-chain MEV-defense stack from
///         [ADR 018](../adr/018-liquidity-strategy.md): a single
///         `swapSingleTokenExactIn` USDC->TOKEN swap with a scoped Vault
///         approval, a governed `slippageBps`/`minBuybackAmount`/
///         `maxBuybackAmount` band, an on-chain per-epoch USDC liquidity cap,
///         and an on-chain TWAP `minOut` floor.
/// @dev    The base `BuybackBurner.executeBuyback` is the entry point and keeps
///         wrapping this override with its `nonReentrant` guard, actual-TOKEN-
///         balance-delta verification (`SwapReportMismatch`/`SwapNotImplemented`)
///         and the burn. All MEV-stack checks therefore live inside
///         `_performSwap`, ordered checks-effects-interactions: the TWAP floor,
///         the min/max band, and the per-epoch cap accrual all run (and write
///         state) BEFORE the Router call.
///
///         TWAP source: Balancer V3 (unlike V2) ships no built-in pool oracle,
///         and TOKEN has no external price feed at launch. The floor is fed by
///         a self-maintained cumulative-price accumulator (Uniswap-V2
///         `price0CumulativeLast` style) over the pool's marginal spot price,
///         sampled BEFORE each swap and advanceable permissionlessly via
///         `poke()`. It is fail-closed: `executeBuyback` reverts `TwapNotReady`
///         until the accumulator spans `twapMinWindow`. Sparse updates weaken
///         the average, so per ADR 018 this floor is layered under mandatory
///         keeper-side private-RPC routing and the per-epoch liquidity cap; it
///         is necessary, not sufficient, on its own.
contract BuybackBurnerBalancerV3 is BuybackBurner {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Constants / immutables
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant WAD = 1e18;

    /// @notice Length of a buyback epoch for the per-epoch liquidity cap.
    ///         Matches the 7-day, unix-epoch-anchored protocol epoch used by
    ///         `CapacityBond`/`FeeRouter`, so this cap's epoch boundaries align
    ///         with the FeeRouter settlement epoch.
    uint256 internal constant EPOCH_LENGTH = 7 days;

    uint256 internal constant CAP_FRACTION_FLOOR = 100; // 1%
    uint256 internal constant CAP_FRACTION_CEILING = 3000; // 30%

    /// @notice Floor on the constructor `twapMinWindow` so the fail-closed TWAP
    ///         maturity guard cannot be configured toothless (a near-zero window
    ///         would let the floor activate over a one-block span, collapsing the
    ///         time-weighting to manipulable spot).
    uint256 internal constant MIN_TWAP_WINDOW = 30 minutes;

    /// @notice TWAP sub-swap calibration for the off-chain keeper (ADR 018
    ///         § TWAP policy). The contract performs exactly one Router swap per
    ///         `executeBuyback` call — the sub-swap cadence (spacing, count) is
    ///         a keeper concern across multiple calls; these are stored only for
    ///         keeper introspection / event-log correlation.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable subSwapCount;
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable subSwapMinBlockGap;

    /// @notice Minimum span (seconds) the TWAP accumulator must cover before the
    ///         `minOut` floor is trusted; until then `executeBuyback` reverts
    ///         `TwapNotReady` (fail-closed).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable twapMinWindow;

    /// @notice `10 ** (18 - usdcDecimals)` — scales raw USDC amounts up to the
    ///         18-decimal fixed point used by the Vault's `*Scaled18` balances.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable usdcTo18;

    // -----------------------------------------------------------------
    // Governance-mutable venue + parameters
    // -----------------------------------------------------------------

    /// @notice Balancer V3 Router (the swap call target, distinct from the
    ///         Vault that token approvals target). Set in the constructor and
    ///         rotatable by governance via `setSwapRouter`.
    IBalancerV3Router public swapRouter;

    /// @notice Current keeper holding `KEEPER_ROLE`; rotated via `setKeeper`.
    address public keeper;

    uint256 public slippageBps;
    uint256 public minBuybackAmount;
    uint256 public maxBuybackAmount;

    /// @notice Per-epoch liquidity cap as a fraction (bps) of in-pool USDC depth
    ///         snapshotted at epoch start. Bounded `[1%, 30%]` (ADR 018).
    uint256 public epochLiquidityCapFraction;

    // -----------------------------------------------------------------
    // Per-epoch liquidity cap state
    // -----------------------------------------------------------------

    uint64 public currentEpochIndex;
    uint256 public epochStartUsdcDepth;
    uint256 public epochSwappedUsdc;

    // -----------------------------------------------------------------
    // TWAP accumulator state (sliding two-checkpoint window)
    // -----------------------------------------------------------------
    // `internal`: these raw checkpoints are individually meaningless and
    // coherent only as a set (the read divides cumulative-deltas by their
    // matching time-deltas). The curated `twapPrice()` view and the
    // `TwapUpdated` event are the external read surface.

    uint256 internal priceCumulative;
    uint256 internal twapLastUpdate;
    uint256 internal twapLastSpot;
    uint256 internal twapAnchorCumulative;
    uint256 internal twapAnchorTime;
    uint256 internal twapCurrCumulative;
    uint256 internal twapCurrTime;

    // -----------------------------------------------------------------
    // Events / Errors
    // -----------------------------------------------------------------

    event SwapRouterUpdated(address indexed oldAddr, address indexed newAddr);
    event KeeperUpdated(address indexed oldAddr, address indexed newAddr);
    event SlippageUpdated(uint256 oldBps, uint256 newBps);
    event MinBuybackUpdated(uint256 oldAmount, uint256 newAmount);
    event MaxBuybackUpdated(uint256 oldAmount, uint256 newAmount);
    event EpochCapFractionUpdated(uint256 oldBps, uint256 newBps);
    event EpochRolled(uint64 indexed epoch, uint256 usdcDepthSnapshot);
    event TwapUpdated(uint256 priceCumulative, uint256 timestamp);

    error BelowMinBuyback(uint256 amountIn, uint256 floor);
    error AboveMaxBuyback(uint256 amountIn, uint256 ceiling);
    error EpochCapExceeded(uint256 wouldSwap, uint256 cap);
    error MinOutBelowTwapFloor(uint256 minOut, uint256 twapFloor);
    error TwapNotReady();
    error SlippageOutOfBounds(uint256 value, uint256 ceiling);
    error CapFractionOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error TwapWindowTooShort(uint256 value, uint256 floor);
    error BuybackBandInverted(uint256 minAmount, uint256 maxAmount);
    error UnsupportedTokenDecimals(uint8 decimals);
    error PoolStateInvalid();

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    /// @param swapRouter_ Balancer V3 Router (call target). Non-zero.
    /// @param pool_ 80/20 TOKEN/USDC weighted pool (may be zero — governance
    ///        wires it later via `setPool`; base `executeBuyback` reverts
    ///        `PoolNotWired` until both pool and vault are set).
    /// @param vault_ Balancer V3 Vault (approval target; may be zero at deploy).
    struct Config {
        IBalancerV3Router swapRouter_;
        address pool_;
        address vault_;
        uint256 subSwapCount_;
        uint256 subSwapMinBlockGap_;
        uint256 twapMinWindow_;
        uint256 maxBuybackAmount_;
        uint256 minBuybackAmount_;
        uint256 slippageBps_;
        uint256 epochLiquidityCapFraction_;
    }

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, Config memory cfg)
        BuybackBurner(usdc_, token_, admin)
    {
        if (address(cfg.swapRouter_) == address(0)) revert ZeroAddress();
        if (cfg.slippageBps_ >= BPS_DENOMINATOR) revert SlippageOutOfBounds(cfg.slippageBps_, BPS_DENOMINATOR);
        if (
            cfg.epochLiquidityCapFraction_ < CAP_FRACTION_FLOOR || cfg.epochLiquidityCapFraction_ > CAP_FRACTION_CEILING
        ) {
            revert CapFractionOutOfBounds(cfg.epochLiquidityCapFraction_, CAP_FRACTION_FLOOR, CAP_FRACTION_CEILING);
        }
        if (cfg.twapMinWindow_ < MIN_TWAP_WINDOW) revert TwapWindowTooShort(cfg.twapMinWindow_, MIN_TWAP_WINDOW);
        if (cfg.minBuybackAmount_ > cfg.maxBuybackAmount_) {
            revert BuybackBandInverted(cfg.minBuybackAmount_, cfg.maxBuybackAmount_);
        }

        swapRouter = cfg.swapRouter_;
        // Inherited `balancerPool`/`balancerVault` are plain storage; setting
        // them here is equivalent to a post-deploy `setPool`/`setVault`.
        balancerPool = cfg.pool_;
        balancerVault = cfg.vault_;

        subSwapCount = cfg.subSwapCount_;
        subSwapMinBlockGap = cfg.subSwapMinBlockGap_;
        twapMinWindow = cfg.twapMinWindow_;
        maxBuybackAmount = cfg.maxBuybackAmount_;
        minBuybackAmount = cfg.minBuybackAmount_;
        slippageBps = cfg.slippageBps_;
        epochLiquidityCapFraction = cfg.epochLiquidityCapFraction_;

        // Constructor read + immutable write — no reentrancy surface; aderyn's
        // external-call-then-state-write heuristic false-positives in a ctor.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint8 usdcDecimals = IERC20Metadata(address(usdc_)).decimals();
        if (usdcDecimals > 18) revert UnsupportedTokenDecimals(usdcDecimals);
        usdcTo18 = 10 ** (18 - usdcDecimals);
    }

    // -----------------------------------------------------------------
    // Swap override (runs inside base `executeBuyback`)
    // -----------------------------------------------------------------

    /// @inheritdoc BuybackBurner
    function _performSwap(uint256 amountIn, uint256 minOut) internal override returns (uint256 tokenOut) {
        // 1. TWAP floor. Sample spot (pre-swap) and advance the accumulator,
        //    then require the keeper's `minOut` to clear the TWAP-derived,
        //    slippage-discounted floor. Effective minimum = max(floor, minOut).
        _updateTwapAccumulator();
        uint256 floor = _twapFloor(amountIn);
        if (minOut < floor) revert MinOutBelowTwapFloor(minOut, floor);

        // 2. Min/max buyback band (ADR 018 § Parameter Table).
        if (amountIn < minBuybackAmount) revert BelowMinBuyback(amountIn, minBuybackAmount);
        if (amountIn > maxBuybackAmount) revert AboveMaxBuyback(amountIn, maxBuybackAmount);

        // 3. Per-epoch USDC liquidity cap (ADR 018 § TWAP policy — required
        //    on-chain). Roll + re-snapshot depth on a new epoch, then accrue
        //    BEFORE the external swap (effects-before-interactions).
        _accruePerEpochCap(amountIn);

        // 4. Scoped Vault approval (base header invariant #2): approve exactly
        //    `amountIn` to the VAULT (not the Router — the Vault pulls input
        //    tokens), then reset to 0 so no standing allowance survives.
        usdc.forceApprove(balancerVault, amountIn);

        // 5. Single exact-in swap on the Router with the keeper `minOut`.
        //    `deadline = block.timestamp` gives no standing deadline window;
        //    front-run protection comes from the `minOut`/TWAP floor above plus
        //    the mandatory private-RPC bundle (ADR 018), not the deadline.
        //    Approval target is the VAULT (set in step 4) per Balancer V3's
        //    "approve the Vault, call the Router" integration model.
        // forge-lint: disable-next-line(block-timestamp)
        tokenOut = swapRouter.swapSingleTokenExactIn(
            balancerPool, usdc, IERC20(address(token)), amountIn, minOut, block.timestamp, false, ""
        );

        // 6. Reset the scoped approval.
        usdc.forceApprove(balancerVault, 0);
    }

    /// @notice Permissionlessly advance the TWAP accumulator with the current
    ///         pool spot. A buyback never moves its own floor regardless of
    ///         `poke` — the accumulator gives the just-sampled spot zero weight
    ///         in the same call (`elapsed == 0`). `poke` densifies historical
    ///         samples between buybacks so the floor isn't dominated by a single
    ///         stale observation across a long gap.
    function poke() external {
        if (balancerPool == address(0) || balancerVault == address(0)) revert PoolNotWired();
        _updateTwapAccumulator();
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

    /// @notice Rotate the single keeper: revoke `KEEPER_ROLE` from the old
    ///         holder and grant it to `newKeeper`. Multi-keeper setups remain
    ///         possible via the raw `grantRole(KEEPER_ROLE, …)` AccessControl API.
    function setKeeper(address newKeeper) external onlyRole(GOVERNANCE_ROLE) {
        if (newKeeper == address(0)) revert ZeroAddress();
        address old = keeper;
        if (old != address(0)) _revokeRole(KEEPER_ROLE, old);
        keeper = newKeeper;
        _grantRole(KEEPER_ROLE, newKeeper);
        emit KeeperUpdated(old, newKeeper);
    }

    function setSlippageTolerance(uint256 newBps) external onlyRole(GOVERNANCE_ROLE) {
        if (newBps >= BPS_DENOMINATOR) revert SlippageOutOfBounds(newBps, BPS_DENOMINATOR);
        uint256 old = slippageBps;
        slippageBps = newBps;
        emit SlippageUpdated(old, newBps);
    }

    function setMinBuybackAmount(uint256 newAmount) external onlyRole(GOVERNANCE_ROLE) {
        if (newAmount > maxBuybackAmount) revert BuybackBandInverted(newAmount, maxBuybackAmount);
        uint256 old = minBuybackAmount;
        minBuybackAmount = newAmount;
        emit MinBuybackUpdated(old, newAmount);
    }

    function setMaxBuybackAmount(uint256 newAmount) external onlyRole(GOVERNANCE_ROLE) {
        if (newAmount < minBuybackAmount) revert BuybackBandInverted(minBuybackAmount, newAmount);
        uint256 old = maxBuybackAmount;
        maxBuybackAmount = newAmount;
        emit MaxBuybackUpdated(old, newAmount);
    }

    function setEpochLiquidityCapFraction(uint256 newBps) external onlyRole(GOVERNANCE_ROLE) {
        if (newBps < CAP_FRACTION_FLOOR || newBps > CAP_FRACTION_CEILING) {
            revert CapFractionOutOfBounds(newBps, CAP_FRACTION_FLOOR, CAP_FRACTION_CEILING);
        }
        uint256 old = epochLiquidityCapFraction;
        epochLiquidityCapFraction = newBps;
        emit EpochCapFractionUpdated(old, newBps);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice USDC accumulated in this contract awaiting buyback (canonical
    ///         `IBuybackBurner.getAccumulatedFees`, ADR 003).
    function getAccumulatedFees() external view returns (uint256) {
        return usdc.balanceOf(address(this));
    }

    /// @notice Current time-weighted TOKEN-per-USDC price (1e18 fixed point).
    ///         Reverts `TwapNotReady` until the accumulator spans `twapMinWindow`.
    function twapPrice() external view returns (uint256) {
        return _twapPrice();
    }

    // -----------------------------------------------------------------
    // Internal — per-epoch cap
    // -----------------------------------------------------------------

    /// @dev Accrues `amountIn` against the per-epoch USDC cap, snapshotting
    ///      in-pool USDC depth on the first swap of each 7-day epoch.
    ///      SECURITY NOTE: unlike the `minOut` floor, the cap denominator is an
    ///      INSTANTANEOUS depth read (`_usdcDepthRaw`), which a flash-loan can
    ///      transiently shrink to throttle the buyback program for a whole epoch
    ///      (griefing — no fund loss; the cap only bounds throughput). This is an
    ///      accepted residual: the keeper MUST submit buybacks through the
    ///      mandatory private-RPC bundle (ADR 018), so the epoch's first swap —
    ///      which takes the snapshot — is not front-runnable, and governance can
    ///      raise `epochLiquidityCapFraction` or re-trigger after a manipulated
    ///      epoch. State is written before the external swap (CEI); a reverted
    ///      swap rolls back the accrual.
    function _accruePerEpochCap(uint256 amountIn) internal {
        // forge-lint: disable-next-line(block-timestamp)
        uint64 epoch = uint64(block.timestamp / EPOCH_LENGTH);
        if (epoch != currentEpochIndex || epochStartUsdcDepth == 0) {
            currentEpochIndex = epoch;
            epochStartUsdcDepth = _usdcDepthRaw();
            epochSwappedUsdc = 0;
            emit EpochRolled(epoch, epochStartUsdcDepth);
        }
        uint256 cap = Math.mulDiv(epochStartUsdcDepth, epochLiquidityCapFraction, BPS_DENOMINATOR);
        uint256 wouldSwap = epochSwappedUsdc + amountIn;
        if (wouldSwap > cap) revert EpochCapExceeded(wouldSwap, cap);
        epochSwappedUsdc = wouldSwap;
    }

    // -----------------------------------------------------------------
    // Internal — TWAP accumulator
    // -----------------------------------------------------------------

    function _updateTwapAccumulator() internal {
        // forge-lint: disable-next-line(block-timestamp)
        uint256 nowTs = block.timestamp;
        uint256 spot = _spotPrice();

        if (twapLastUpdate == 0) {
            // First observation: initialize both checkpoints to now. The floor
            // stays fail-closed (`TwapNotReady`) until `twapMinWindow` elapses.
            twapLastUpdate = nowTs;
            twapLastSpot = spot;
            twapAnchorTime = nowTs;
            twapCurrTime = nowTs;
            emit TwapUpdated(0, nowTs);
            return;
        }

        uint256 elapsed = nowTs - twapLastUpdate;
        if (elapsed != 0) {
            priceCumulative += twapLastSpot * elapsed;
            twapLastUpdate = nowTs;
            // Slide the window: once the current checkpoint has matured past
            // `twapMinWindow`, promote it to the anchor and re-anchor current to
            // now. Keeps the read span in roughly [window, 2*window].
            if (nowTs - twapCurrTime >= twapMinWindow) {
                twapAnchorCumulative = twapCurrCumulative;
                twapAnchorTime = twapCurrTime;
                twapCurrCumulative = priceCumulative;
                twapCurrTime = nowTs;
            }
        }
        twapLastSpot = spot;
        emit TwapUpdated(priceCumulative, nowTs);
    }

    function _twapPrice() internal view returns (uint256) {
        // forge-lint: disable-next-line(block-timestamp)
        uint256 nowTs = block.timestamp;
        uint256 span = nowTs - twapAnchorTime;
        // `== 0` is the correct "never initialized" sentinel for the lazy-init
        // accumulator; the dangerous-strict-equality detector does not apply.
        // slither-disable-next-line incorrect-equality
        if (twapLastUpdate == 0 || span < twapMinWindow) revert TwapNotReady();
        uint256 cumNow = priceCumulative + twapLastSpot * (nowTs - twapLastUpdate);
        return (cumNow - twapAnchorCumulative) / span;
    }

    /// @dev TWAP-derived minimum TOKEN out for `amountIn` raw USDC. Starts from
    ///      the marginal (linear) price, then subtracts the pool's swap fee and
    ///      the `slippageBps` tolerance. Realized exact-in output is reduced by
    ///      BOTH the swap fee (deterministic, read live from the Vault) AND
    ///      price impact; `slippageBps` covers the latter (`maxBuybackAmount` is
    ///      sized so a single swap stays under `slippageBps` impact, ADR 018),
    ///      and the explicit fee term covers the former. Without the fee term a
    ///      near-`maxBuybackAmount` swap on a 1%-fee pool at the default 2%
    ///      slippage would realize ~97% of marginal, below a fee-free 98% floor,
    ///      and self-revert.
    function _twapFloor(uint256 amountIn) internal view returns (uint256) {
        uint256 price = _twapPrice(); // TOKEN per USDC, 1e18 fixed point
        uint256 amountIn18 = amountIn * usdcTo18;
        uint256 expectedOut = Math.mulDiv(amountIn18, price, WAD);
        // Net the swap fee (1e18-scaled) before applying the slippage tolerance.
        uint256 feeE18 = IBalancerV3Vault(balancerVault).getStaticSwapFeePercentage(balancerPool);
        uint256 afterFee = Math.mulDiv(expectedOut, WAD - feeE18, WAD);
        return Math.mulDiv(afterFee, BPS_DENOMINATOR - slippageBps, BPS_DENOMINATOR);
    }

    // -----------------------------------------------------------------
    // Internal — pool reads
    // -----------------------------------------------------------------

    /// @dev Marginal TOKEN-per-USDC spot (1e18 fixed point) for the weighted
    ///      pool: out-per-in = (wIn * balOut) / (balIn * wOut), with in = USDC,
    ///      out = TOKEN. Balances are Vault `*Scaled18`; weights are 1e18.
    function _spotPrice() internal view returns (uint256) {
        (uint256 balUsdc18, uint256 balToken18, uint256 wUsdc, uint256 wToken) = _poolState();
        // = (wUsdc * balToken18 * WAD) / (balUsdc18 * wToken), nested through
        // `mulDiv` so neither product is formed in plain uint256 (no
        // pre-multiplication overflow for large pools). WAD is applied first to
        // preserve precision before the weight ratio.
        return Math.mulDiv(Math.mulDiv(balToken18, WAD, balUsdc18), wUsdc, wToken);
    }

    function _usdcDepthRaw() internal view returns (uint256) {
        (uint256 balUsdc18,,,) = _poolState();
        return balUsdc18 / usdcTo18;
    }

    /// @dev Reads the Vault's scaled-18 balances + the pool's normalized weights
    ///      and locates the USDC and TOKEN legs by address. Reverts
    ///      `PoolStateInvalid` if either leg is absent or zero-balance.
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
