// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

/// @title BuybackBurner
/// @notice Receives the 30% USDC bucket from `FeeRouter.routeSettlement`, swaps
///         it for TOKEN, and burns the proceeds (ADR 026 § FeeRouter split →
///         § Slashing and burn). The swap venue is NOT fixed here: this base is
///         venue-neutral and owns only the parts every burner shares — the
///         keeper entry point, the actual-TOKEN-delta verification, the burn,
///         USDC rescue, and pausing. The concrete swap, the pool/router wiring,
///         and any MEV-defense stack live in subclasses (`GuardedBuybackBurner`
///         adds the shared TWAP/band/cap stack; `BuybackBurnerBalancerV3` and
///         `BuybackBurnerUniswapV3` bind a live venue).
/// @dev    `executeBuyback` reverts `PoolNotWired` (via the subclass
///         `_requireWired` hook) until governance wires the venue, and
///         `SwapNotImplemented` if `_performSwap` transfers no TOKEN. Once a
///         subclass override returns a non-zero `tokenOut` backed by a matching
///         balance delta, the burn fires.
///
///         MANDATORY `_performSwap` SUBCLASS INVARIANTS (the deployment-PR
///         auditor MUST verify these before mainnet — the base cannot enforce
///         them because the venue ABI is unknown here):
///           1. Derive the `minOut` floor from an on-chain TWAP/oracle and
///              require the keeper-supplied `minOut >= twapFloor`; the base only
///              rejects `minOut == 0` (no zero-slippage swaps) and bounds
///              `amountIn` by the contract's USDC balance.
///           2. Scope the input-token approval to exactly `amountIn` and reset
///              it to `0` after the swap, so no standing USDC allowance survives
///              the call. The mechanism is venue-specific (a Balancer/Uniswap V3
///              Router pulls via Permit2; another venue may take a direct
///              allowance).
///           3. Optionally cap `amountIn` against a governed per-epoch
///              liquidity budget to limit sandwich exposure on thin pools.
///         `GuardedBuybackBurner` implements all three once for every venue.
abstract contract BuybackBurner is AccessControl, ReentrancyGuard, SunsettingPausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    /// @notice Fixed sink for `rescueUSDC`. Snapshotted at construction from the
    ///         same treasury that `FeeRouter` pays, so a rescue can only ever
    ///         return the buyback bucket here — never to a caller-chosen
    ///         address. A live `FeeRouter.treasury()` read is a circular deploy
    ///         dependency (the router's constructor already needs this burner),
    ///         and the immutable snapshot is stricter still: it cannot be
    ///         redirected even by `FeeRouter.setTreasury`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    address public immutable treasury;

    // -----------------------------------------------------------------
    // Events / Errors
    // -----------------------------------------------------------------

    event BuybackExecuted(uint256 usdcIn, uint256 tokenOut);
    event UsdcRescued(uint256 amount);

    error ZeroAddress();
    error ZeroAmount();
    error ZeroMinOut();
    error AmountExceedsBalance(uint256 amountIn, uint256 balance);
    error PoolNotWired();
    error SwapNotImplemented();
    error SwapReportMismatch(uint256 reported, uint256 actual);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, address treasury_) {
        if (
            address(usdc_) == address(0) || address(token_) == address(0) || admin == address(0)
                || treasury_ == address(0)
        ) {
            revert ZeroAddress();
        }
        usdc = usdc_;
        token = token_;
        treasury = treasury_;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        // Deploy renounces DEFAULT_ADMIN_ROLE, freezing the role table (#2028).
        // PAUSER_ROLE and KEEPER_ROLE stay rotatable under GOVERNANCE_ROLE so a
        // timelocked proposal can rotate a compromised emergency pauser or an
        // operational keeper key without a redeploy.
        _setRoleAdmin(PAUSER_ROLE, GOVERNANCE_ROLE);
        _setRoleAdmin(KEEPER_ROLE, GOVERNANCE_ROLE);
    }

    // -----------------------------------------------------------------
    // Buyback execution
    // -----------------------------------------------------------------

    /// @notice Swap `amountIn` USDC for TOKEN (slippage floor `minOut`), then
    ///         burn the received TOKEN. Reverts `SwapNotImplemented` if the
    ///         subclass `_performSwap` reports a positive `tokenOut` but
    ///         transfers nothing — a success path that emitted `BuybackExecuted`
    ///         with a zero burn would mislead off-chain indexers (I1 fix).
    /// @dev    Subclasses override `_performSwap` with the real venue call; the
    ///         override returns a non-zero `tokenOut`, which makes the burn fire
    ///         and the `SwapNotImplemented` revert unreachable.
    function executeBuyback(uint256 amountIn, uint256 minOut)
        external
        virtual
        nonReentrant
        whenNotPaused
        onlyRole(KEEPER_ROLE)
        returns (uint256 tokenOut)
    {
        if (amountIn == 0) revert ZeroAmount();
        // Reject zero-slippage swaps: a `minOut == 0` keeper call (or one
        // front-run into a thin pool) would accept near-zero TOKEN out and
        // burn dust. The TWAP-derived floor on top of this lives in the
        // guard subclass `_performSwap`.
        if (minOut == 0) revert ZeroMinOut();
        // Venue-wiring readiness is a subclass concern (each venue knows what
        // "wired" means); the hook reverts `PoolNotWired` when not ready.
        _requireWired();
        // Bound the spend by the contract's actual USDC holdings so a keeper
        // cannot request a swap larger than the buyback bucket.
        uint256 usdcBalance = usdc.balanceOf(address(this));
        if (amountIn > usdcBalance) revert AmountExceedsBalance(amountIn, usdcBalance);

        // Verify the subclass's reported `tokenOut` matches the actual
        // balance delta. Closes the trust boundary: a buggy override that
        // returns a smaller amount would otherwise burn less than
        // received, leaving residual TOKEN stranded in this contract.
        uint256 balanceBefore = IERC20(address(token)).balanceOf(address(this));
        tokenOut = _performSwap(amountIn, minOut);
        uint256 actual = IERC20(address(token)).balanceOf(address(this)) - balanceBefore;
        // Derived balance delta, not a token-balance read. Strict equality
        // against 0 is the correct sentinel for "no swap occurred".
        // slither-disable-next-line incorrect-equality
        if (actual == 0) revert SwapNotImplemented();
        if (actual != tokenOut) revert SwapReportMismatch(tokenOut, actual);
        token.burn(actual);
        emit BuybackExecuted(amountIn, actual);
    }

    /// @dev Abstract hook for the concrete venue swap. The subclass performs the
    ///      USDC->TOKEN swap (the TOKEN landing in this contract) and returns the
    ///      post-swap TOKEN amount. Keeping this `virtual` without a body keeps
    ///      the base abstract and makes solc's unreachable-code analysis treat
    ///      the call site as opaque, avoiding the OZ ReentrancyGuard
    ///      `--deny-warnings` trip a return-0 base implementation would cause.
    function _performSwap(uint256 amountIn, uint256 minOut) internal virtual returns (uint256);

    /// @dev Abstract wiring-readiness guard. Subclasses revert `PoolNotWired`
    ///      while their venue (pool/router/vault) is not fully configured, so
    ///      `executeBuyback` never reaches a swap against an unwired venue.
    function _requireWired() internal view virtual;

    // -----------------------------------------------------------------
    // USDC rescue + pausing
    // -----------------------------------------------------------------

    /// @notice Return USDC stranded in this contract to the fixed `treasury` —
    ///         e.g. inflow that accumulated while the pool was unwired, residue
    ///         left by a keeper under-swap, or the full balance before a
    ///         `setBuybackBurner` replacement on `FeeRouter`. Without this,
    ///         replacing the burner would permanently strand the old contract's
    ///         USDC. This is a misconfiguration hatch, not a payout path: it can
    ///         only ever move USDC to the immutable `treasury`, never to a
    ///         caller-supplied address.
    /// @dev Governance-gated; only moves the externally-held USDC bucket, never
    ///      TOKEN (which is always burned, never transferred out).
    function rescueUSDC(uint256 amount) external onlyRole(GOVERNANCE_ROLE) {
        if (amount == 0) revert ZeroAmount();
        emit UsdcRescued(amount);
        usdc.safeTransfer(treasury, amount);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _requirePauseWindowOpen();
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }
}
