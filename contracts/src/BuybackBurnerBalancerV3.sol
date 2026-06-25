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
import { IPermit2 } from "./interfaces/IPermit2.sol";

/// @title BuybackBurnerBalancerV3
/// @notice Concrete, deployable `BuybackBurner` that binds the live Balancer V3
///         Router and implements the full on-chain MEV-defense stack from
///         [ADR 018](../adr/018-liquidity-strategy.md): a single
///         `swapSingleTokenExactIn` USDC->TOKEN swap with a scoped Permit2
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

    /// @notice Uniswap Permit2 — the Balancer V3 Router's token-pull authority.
    ///         A V3 Router pulls `tokenIn` via `permit2.transferFrom`, so the
    ///         swap authorizes it through Permit2 (ERC20-approve Permit2 + a
    ///         scoped Permit2 allowance to the Router), not a direct Vault
    ///         allowance. Canonical on every chain but injected (not hardcoded)
    ///         so tests can substitute a mock; constructor rejects `address(0)`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IPermit2 public immutable permit2;

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
    error PoolNotRegistered(address pool, address vault);

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    /// @param swapRouter_ Balancer V3 Router (call target). Non-zero.
    /// @param pool_ 80/20 TOKEN/USDC weighted pool (may be zero — governance
    ///        wires it later via `setPool`; base `executeBuyback` reverts
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
        BuybackBurner(usdc_, token_, admin)
    {
        if (address(cfg.swapRouter_) == address(0)) revert ZeroAddress();
        if (cfg.permit2_ == address(0)) revert ZeroAddress();
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
        permit2 = IPermit2(cfg.permit2_);
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

        // Fail-fast on a pool/vault supplied at deploy (issue #968): a `Config`
        // that wires both must reference a Vault-registered pool containing at
        // least the {USDC, TOKEN} legs. A `Config` that defers wiring (pool_ or
        // vault_ == 0) skips this and is rejected later with `PoolNotWired`.
        _validatePoolWiring();
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

        // 4. Scoped Permit2 approval (base header invariant #2). A Balancer V3
        //    Router pulls `tokenIn` via `permit2.transferFrom(this, vault, …)`,
        //    NOT via a direct ERC20 allowance to the Vault — so authorize the
        //    spend in two scoped legs: ERC20-approve Permit2 for exactly
        //    `amountIn`, then grant the Router a Permit2 allowance for exactly
        //    `amountIn`, with an `uint48(block.timestamp)` expiration (Permit2
        //    reverts a transfer once `block.timestamp > expiration`). The
        //    expiration is a secondary bound only: on an L2 a single timestamp
        //    can span several blocks, so confinement comes from the step-6 reset
        //    plus Permit2's amount-decrement on transfer — together they drive
        //    the allowance to 0 within this transaction, so no standing
        //    allowance survives.
        usdc.forceApprove(address(permit2), amountIn);
        // `uint160(amountIn)` cannot truncate: `amountIn` is bounded above by the
        // base-layer `amountIn <= usdc.balanceOf(this)` check, and 6-dec USDC
        // total supply is ~30 orders of magnitude below 2^160.
        // forge-lint: disable-next-line(block-timestamp)
        permit2.approve(address(usdc), address(swapRouter), uint160(amountIn), uint48(block.timestamp));

        // 5. Single exact-in swap on the Router with the keeper `minOut`.
        //    `deadline = block.timestamp` gives no standing deadline window;
        //    front-run protection comes from the `minOut`/TWAP floor above plus
        //    the mandatory private-RPC bundle (ADR 018), not the deadline.
        // forge-lint: disable-next-line(block-timestamp)
        tokenOut = swapRouter.swapSingleTokenExactIn(
            balancerPool, usdc, IERC20(address(token)), amountIn, minOut, block.timestamp, false, ""
        );

        // 6. Reset both legs of the scoped approval. A successful pull already
        //    decrements both allowances toward 0, but a Router that pulls less
        //    than `amountIn` would leave a residual standing allowance — these
        //    resets close that off unconditionally.
        permit2.approve(address(usdc), address(swapRouter), 0, 0);
        usdc.forceApprove(address(permit2), 0);
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

    /// @inheritdoc BuybackBurner
    /// @dev Wraps the base setter with fail-fast wiring validation (issue #968):
    ///      once both pool and vault are set, the new pool must be Vault-
    ///      registered and contain at least the {USDC, TOKEN} legs, else this
    ///      reverts (`PoolNotRegistered` / `PoolStateInvalid`) instead of
    ///      silently accepting a mis-wired or substituted pool. `newPool ==
    ///      address(0)` stays the documented "not wired" state (validation
    ///      skipped); prefer `pause()` to disable without unwiring.
    function setPool(address newPool) public override onlyRole(GOVERNANCE_ROLE) {
        super.setPool(newPool);
        _validatePoolWiring();
    }

    /// @inheritdoc BuybackBurner
    /// @dev Symmetric to `setPool`: revalidates the (pool, vault) pair so a Vault
    ///      rotation the current pool is not registered with fails fast. Note the
    ///      new Vault is the unverified trust root — `_validatePoolWiring` asks it
    ///      whether the pool is registered AND reads the pool's leg balances from
    ///      it, so a malicious Vault can satisfy both. That residual surface is
    ///      bounded by `GOVERNANCE_ROLE` + the 48h timelock, not by this check.
    function setVault(address newVault) public override onlyRole(GOVERNANCE_ROLE) {
        super.setVault(newVault);
        _validatePoolWiring();
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
        // A degenerate `feeE18 >= WAD` would collapse the floor to 0 (fail-open),
        // so reject it (real Balancer pools cap the swap fee well below 100%).
        uint256 feeE18 = IBalancerV3Vault(balancerVault).getStaticSwapFeePercentage(balancerPool);
        if (feeE18 >= WAD) revert PoolStateInvalid();
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

    /// @dev Fail-fast pool-wiring guard, shared by the constructor and the
    ///      `setPool`/`setVault` overrides. No-ops while either leg of the
    ///      (pool, vault) pair is unwired (`address(0)`) — that state stays the
    ///      documented "not wired" path rejected later by `PoolNotWired`. Once
    ///      both are set, requires the pool to be registered with the configured
    ///      Vault (`PoolNotRegistered`) and to contain at least the {USDC, TOKEN}
    ///      legs with non-zero balances/weights — the latter reusing the same
    ///      `_poolState` invariant the swap path relies on (`PoolStateInvalid`);
    ///      additional (decoy) legs are tolerated, exactly as at swap time.
    ///      Closes the silent mis-wire / pool-substitution gap (issue #968): a
    ///      bad pool is rejected at wiring time, not first surfaced at swap time.
    ///      The Vault is the trust root for both checks and is not itself
    ///      validated (see `setVault`).
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
