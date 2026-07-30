// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IERC20Metadata } from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

import { BuybackBurner } from "./BuybackBurner.sol";

/// @title GuardedBuybackBurner
/// @notice Venue-neutral MEV-defense stack shared by every concrete burner
///         (ADR 018 § TWAP policy): a TWAP-derived `minOut` floor, a governed
///         `slippageBps` / `minBuybackAmount` / `maxBuybackAmount` band, and an
///         on-chain per-epoch USDC liquidity cap. The base `executeBuyback`
///         keeps wrapping this `_performSwap` override with its `nonReentrant`
///         guard, actual-TOKEN-balance-delta verification, and the burn — so
///         all guard checks run, and write state, BEFORE the venue swap
///         (checks-effects-interactions).
/// @dev    The venue is abstracted behind five hooks a concrete subclass binds:
///         `_requireWired` (base), `_spotPrice`, `_usdcDepthRaw`, `_swapFeeE18`,
///         and `_swap`. Nothing here references Balancer or Uniswap; the
///         spot-price source and the swap call are entirely the subclass's.
///
///         TWAP source: a self-maintained cumulative-price accumulator
///         (Uniswap-V2 `price0CumulativeLast` style) over the venue's marginal
///         spot, sampled BEFORE each swap and advanceable permissionlessly via
///         `poke()`. Fail-closed: `executeBuyback` reverts `TwapNotReady` until
///         the accumulator spans `twapMinWindow`. Sparse updates weaken the
///         average, so per ADR 018 this floor is layered under mandatory
///         keeper-side private-RPC routing and the per-epoch cap; it is
///         necessary, not sufficient, on its own.
abstract contract GuardedBuybackBurner is BuybackBurner {
    // -----------------------------------------------------------------
    // Constants
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

    /// @notice Ceiling on `slippageBps` so the TWAP floor cannot be configured
    ///         toothless. `_twapFloor` scales by `(BPS_DENOMINATOR - slippageBps)`,
    ///         so a tolerance near `BPS_DENOMINATOR` collapses the floor toward
    ///         zero and disables the whole MEV stack with no revert and no event —
    ///         the same fail-open shape `_twapFloor` already rejects for the swap
    ///         fee. ADR 018 sets the tolerance at 200 bps; 10% leaves ample room
    ///         for genuine volatility while keeping the guard a guard.
    uint256 internal constant SLIPPAGE_CEILING = 1000; // 10%

    /// @notice Floor on the constructor `twapMinWindow` so the fail-closed TWAP
    ///         maturity guard cannot be configured toothless (a near-zero window
    ///         would let the floor activate over a one-block span, collapsing the
    ///         time-weighting to manipulable spot).
    uint256 internal constant MIN_TWAP_WINDOW = 30 minutes;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    /// @notice Minimum span (seconds) the TWAP accumulator must cover before the
    ///         `minOut` floor is trusted; until then `executeBuyback` reverts
    ///         `TwapNotReady` (fail-closed).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable twapMinWindow;

    /// @notice `10 ** (18 - usdcDecimals)` — scales raw USDC amounts up to the
    ///         18-decimal fixed point the spot-price/TWAP math runs in.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable usdcTo18;

    // -----------------------------------------------------------------
    // Governance-mutable parameters
    // -----------------------------------------------------------------

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

    event KeeperUpdated(address indexed oldAddr, address indexed newAddr);
    event SlippageUpdated(uint256 oldBps, uint256 newBps);
    event MinBuybackUpdated(uint256 oldAmount, uint256 newAmount);
    event MaxBuybackUpdated(uint256 oldAmount, uint256 newAmount);
    event EpochCapFractionUpdated(uint256 oldBps, uint256 newBps);
    event EpochRolled(uint64 indexed epoch, uint256 usdcDepthSnapshot);
    event TwapUpdated(uint256 priceCumulative, uint256 timestamp);
    event GuardStateReset();

    error BelowMinBuyback(uint256 amountIn, uint256 floor);
    error AboveMaxBuyback(uint256 amountIn, uint256 ceiling);
    error EpochCapExceeded(uint256 wouldSwap, uint256 cap);
    error MinOutBelowTwapFloor(uint256 minOut, uint256 twapFloor);
    error TwapNotReady();
    error SlippageOutOfBounds(uint256 value, uint256 ceiling);
    error CapFractionOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error TwapWindowTooShort(uint256 value, uint256 floor);
    error BuybackBandInverted(uint256 minAmount, uint256 maxAmount);
    error BuybackBandDead();
    error UnsupportedTokenDecimals(uint8 decimals);
    error SwapFeeInvalid(uint256 feeE18);

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    /// @param twapMinWindow_ Minimum matured TWAP span before the floor is
    ///        trusted; `>= MIN_TWAP_WINDOW`.
    /// @param maxBuybackAmount_ Upper bound of the per-call USDC band; non-zero
    ///        (a zero ceiling reverts every non-zero buyback, forever).
    /// @param minBuybackAmount_ Lower bound of the per-call USDC band; `<= max`.
    /// @param slippageBps_ TWAP-floor slippage tolerance; `<= 10%`.
    /// @param epochLiquidityCapFraction_ Per-epoch cap fraction; `[1%, 30%]`.
    struct GuardParams {
        uint256 twapMinWindow_;
        uint256 maxBuybackAmount_;
        uint256 minBuybackAmount_;
        uint256 slippageBps_;
        uint256 epochLiquidityCapFraction_;
    }

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, GuardParams memory g)
        BuybackBurner(usdc_, token_, admin)
    {
        if (g.slippageBps_ > SLIPPAGE_CEILING) revert SlippageOutOfBounds(g.slippageBps_, SLIPPAGE_CEILING);
        if (g.epochLiquidityCapFraction_ < CAP_FRACTION_FLOOR || g.epochLiquidityCapFraction_ > CAP_FRACTION_CEILING) {
            revert CapFractionOutOfBounds(g.epochLiquidityCapFraction_, CAP_FRACTION_FLOOR, CAP_FRACTION_CEILING);
        }
        if (g.twapMinWindow_ < MIN_TWAP_WINDOW) revert TwapWindowTooShort(g.twapMinWindow_, MIN_TWAP_WINDOW);
        if (g.maxBuybackAmount_ == 0) revert BuybackBandDead();
        if (g.minBuybackAmount_ > g.maxBuybackAmount_) {
            revert BuybackBandInverted(g.minBuybackAmount_, g.maxBuybackAmount_);
        }

        twapMinWindow = g.twapMinWindow_;
        maxBuybackAmount = g.maxBuybackAmount_;
        minBuybackAmount = g.minBuybackAmount_;
        slippageBps = g.slippageBps_;
        epochLiquidityCapFraction = g.epochLiquidityCapFraction_;

        // Constructor read + immutable write — no reentrancy surface; aderyn's
        // external-call-then-state-write heuristic false-positives in a ctor.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint8 usdcDecimals = IERC20Metadata(address(usdc_)).decimals();
        if (usdcDecimals > 18) revert UnsupportedTokenDecimals(usdcDecimals);
        usdcTo18 = 10 ** (18 - usdcDecimals);
    }

    // -----------------------------------------------------------------
    // Swap orchestration (runs inside base `executeBuyback`)
    // -----------------------------------------------------------------

    /// @inheritdoc BuybackBurner
    /// @dev Ordered checks-effects-interactions: TWAP floor, min/max band, and
    ///      per-epoch cap accrual all run (and write state) BEFORE `_swap`.
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

        // 3. Per-epoch USDC liquidity cap. Roll + re-snapshot depth on a new
        //    epoch, then accrue BEFORE the external swap (effects-before-
        //    interactions: a reverted swap rolls back the accrual).
        _accruePerEpochCap(amountIn);

        // 4. Venue swap. The subclass scopes its own approval and returns the
        //    received TOKEN amount; the base verifies it against the actual
        //    balance delta and burns.
        tokenOut = _swap(amountIn, minOut);
    }

    /// @notice Permissionlessly advance the TWAP accumulator with the current
    ///         venue spot. A buyback never moves its own floor regardless of
    ///         `poke` — the accumulator gives the just-sampled spot zero weight
    ///         in the same call (`elapsed == 0`). `poke` densifies historical
    ///         samples between buybacks so the floor isn't dominated by a single
    ///         stale observation across a long gap.
    function poke() external {
        _requireWired();
        _updateTwapAccumulator();
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE, via the 48h timelock)
    // -----------------------------------------------------------------

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
        if (newBps > SLIPPAGE_CEILING) revert SlippageOutOfBounds(newBps, SLIPPAGE_CEILING);
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
        // A zero ceiling is not an inverted band when the floor is also zero, so
        // `BuybackBandInverted` below would pass it. Reject it here: governance
        // could otherwise walk the band to `0/0` in two calls and wedge the
        // burner permanently, holding the FeeRouter's buyback bucket unspendable.
        if (newAmount == 0) revert BuybackBandDead();
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
    // Venue hooks (bound by concrete subclasses)
    // -----------------------------------------------------------------

    /// @dev Marginal TOKEN-per-USDC spot, 1e18 fixed point. MUST revert (not
    ///      return zero) on an invalid venue state — the TWAP floor relies on it.
    function _spotPrice() internal view virtual returns (uint256);

    /// @dev In-pool USDC depth in raw USDC base units, the per-epoch cap
    ///      denominator.
    function _usdcDepthRaw() internal view virtual returns (uint256);

    /// @dev The venue's swap fee as a 1e18 fixed-point fraction (e.g. a 0.3%
    ///      pool returns `3e15`). Netted out of the TWAP floor before slippage.
    function _swapFeeE18() internal view virtual returns (uint256);

    /// @dev Perform the venue USDC->TOKEN swap of exactly `amountIn` with a
    ///      `minOut` floor, scoping its own approval. The received TOKEN MUST
    ///      land in this contract; returns the received amount.
    function _swap(uint256 amountIn, uint256 minOut) internal virtual returns (uint256 tokenOut);

    // -----------------------------------------------------------------
    // Internal — guard state reset on venue rotation
    // -----------------------------------------------------------------

    /// @dev Clear the venue-derived guard state — the TWAP accumulator and the
    ///      per-epoch cap snapshot — so both re-derive from scratch against a
    ///      freshly wired venue. A concrete burner MUST call this whenever it
    ///      rotates a price/depth source (the pool, or the Balancer Vault): the
    ///      accumulator returns to its fail-closed lazy-init state
    ///      (`twapLastUpdate == 0` ⇒ `_twapPrice` reverts `TwapNotReady` until
    ///      `twapMinWindow` re-matures against the new venue), and zeroing
    ///      `epochStartUsdcDepth`/`currentEpochIndex` forces the next swap to
    ///      re-snapshot depth from the new venue. Without this, a rotation would
    ///      leave the floor and cap derived from the OLD venue's price/depth
    ///      until enough time elapsed — fail-open if the stale floor sits below
    ///      what the new venue warrants.
    function _resetGuardState() internal {
        priceCumulative = 0;
        twapLastUpdate = 0;
        twapLastSpot = 0;
        twapAnchorCumulative = 0;
        twapAnchorTime = 0;
        twapCurrCumulative = 0;
        twapCurrTime = 0;
        currentEpochIndex = 0;
        epochStartUsdcDepth = 0;
        epochSwappedUsdc = 0;
        emit GuardStateReset();
    }

    // -----------------------------------------------------------------
    // Internal — per-epoch cap
    // -----------------------------------------------------------------

    /// @dev Accrues `amountIn` against the per-epoch USDC cap, snapshotting
    ///      in-pool USDC depth on the first swap of each 7-day epoch.
    ///      SECURITY NOTE: the cap denominator is an INSTANTANEOUS depth read
    ///      (`_usdcDepthRaw`), which a flash-loan can transiently shrink to
    ///      throttle the buyback program for a whole epoch (griefing — no fund
    ///      loss; the cap only bounds throughput). Accepted residual: the keeper
    ///      MUST submit buybacks through the mandatory private-RPC bundle (ADR
    ///      018), so the epoch's first swap — which takes the snapshot — is not
    ///      front-runnable, and governance can raise the cap or re-trigger after
    ///      a manipulated epoch. State is written before the external swap (CEI);
    ///      a reverted swap rolls back the accrual.
    function _accruePerEpochCap(uint256 amountIn) internal {
        // forge-lint: disable-next-line(block-timestamp)
        uint64 epoch = uint64(block.timestamp / EPOCH_LENGTH);
        // `epochStartUsdcDepth == 0` is the lazy-init sentinel for the first
        // swap of an epoch — a live, wired pool never has zero USDC depth, so 0
        // unambiguously means "not yet snapshotted". A concrete `_usdcDepthRaw`
        // backed by `balanceOf` (Uniswap venue) taints this into slither's
        // strict-equality detector; the sentinel is intentional, same as the
        // `twapLastUpdate == 0` init guard in `_twapPrice`.
        // slither-disable-next-line incorrect-equality
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
    ///      the marginal (linear) price, then subtracts the venue swap fee and
    ///      the `slippageBps` tolerance. Realized exact-in output is reduced by
    ///      BOTH the swap fee (deterministic, read live) AND price impact;
    ///      `slippageBps` covers the latter (`maxBuybackAmount` is sized so a
    ///      single swap stays under `slippageBps` impact, ADR 018), the explicit
    ///      fee term covers the former.
    function _twapFloor(uint256 amountIn) internal view returns (uint256) {
        uint256 price = _twapPrice(); // TOKEN per USDC, 1e18 fixed point
        uint256 amountIn18 = amountIn * usdcTo18;
        uint256 expectedOut = Math.mulDiv(amountIn18, price, WAD);
        // Net the swap fee (1e18-scaled) before applying the slippage tolerance.
        // A degenerate `feeE18 >= WAD` would collapse the floor to 0 (fail-open),
        // so reject it (real pools cap the swap fee well below 100%).
        uint256 feeE18 = _swapFeeE18();
        if (feeE18 >= WAD) revert SwapFeeInvalid(feeE18);
        uint256 afterFee = Math.mulDiv(expectedOut, WAD - feeE18, WAD);
        return Math.mulDiv(afterFee, BPS_DENOMINATOR - slippageBps, BPS_DENOMINATOR);
    }
}
