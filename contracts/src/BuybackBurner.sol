// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

/// @title BuybackBurner
/// @notice Receives the 30% USDC bucket from `FeeRouter.routeSettlement`, swaps
///         it for TOKEN via the Balancer V3 80/20 pool (ADR 018), and burns
///         the proceeds (ADR 026 § FeeRouter split → § Slashing and burn).
/// @dev    This revision ships the inflow + governance-mutable pool wiring; the
///         concrete Balancer V3 Vault swap ABI integration is deferred to the
///         deployment PR that targets a live pool. `executeBuyback` reverts
///         with `PoolNotWired` until governance calls `setPool` + `setVault`,
///         and with `SwapNotImplemented` until a subclass overrides
///         `_performSwap` with the live Balancer V3 ABI. Once that override
///         is in place, the contract performs a single swap against the
///         Vault then burns the received TOKEN.
///
///         MANDATORY `_performSwap` SUBCLASS INVARIANTS (the deployment-PR
///         auditor MUST verify these before mainnet — the base contract cannot
///         enforce them because the Vault ABI is unknown here):
///           1. Derive the `minOut` floor from an on-chain TWAP/oracle and
///              require the keeper-supplied `minOut >= twapFloor`; the base
///              only rejects `minOut == 0` (no zero-slippage swaps) and bounds
///              `amountIn` by the contract's USDC balance.
///           2. Scope the Vault approval to exactly `amountIn`
///              (`forceApprove(vault, amountIn)`) and reset it to `0` after the
///              swap, so no standing USDC allowance survives the call.
///           3. Optionally cap `amountIn` against a governed per-epoch
///              liquidity budget to limit sandwich exposure on thin pools.
// slither-disable-next-line unimplemented-functions
abstract contract BuybackBurner is AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Immutables + governance-mutable wiring
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    /// @notice Balancer V3 Vault address (Vault pulls input tokens from
    ///         `msg.sender`, distinct from the Router). Set via `setVault`.
    address public balancerVault;

    /// @notice Balancer V3 pool contract address for the 80/20 TOKEN/USDC pool.
    address public balancerPool;

    // -----------------------------------------------------------------
    // Events / Errors
    // -----------------------------------------------------------------

    event BuybackExecuted(uint256 usdcIn, uint256 tokenOut);
    event PoolUpdated(address indexed oldAddr, address indexed newAddr);
    event VaultUpdated(address indexed oldAddr, address indexed newAddr);
    event UsdcRescued(address indexed to, uint256 amount);

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

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin) {
        if (address(usdc_) == address(0) || address(token_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        usdc = usdc_;
        token = token_;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Buyback execution
    // -----------------------------------------------------------------

    /// @notice Swap `amountIn` USDC for TOKEN (slippage floor `minOut`), then
    ///         burn the received TOKEN. Until the Balancer V3 Vault swap ABI
    ///         is bound (separate deployment PR), reverts with
    ///         `SwapNotImplemented` even after `setPool` + `setVault` are
    ///         called. A success path that emitted `BuybackExecuted` with a
    ///         zero burn would mislead off-chain indexers (I1 fix).
    /// @dev    Production deployment subclasses this contract and overrides
    ///         `_performSwap` with the real Vault call; the override returns
    ///         a non-zero `tokenOut`, which makes the burn fire and the
    ///         `SwapNotImplemented` revert unreachable.
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
        // subclass `_performSwap` (see header invariants).
        if (minOut == 0) revert ZeroMinOut();
        if (balancerPool == address(0) || balancerVault == address(0)) revert PoolNotWired();
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

    /// @dev Abstract hook for the live Balancer V3 swap. The deployment PR
    ///      that binds the Vault ABI subclasses `BuybackBurner` and provides
    ///      a concrete `_performSwap` returning the post-swap TOKEN amount.
    ///      Keeping this `virtual` without a body (a) keeps the base
    ///      contract abstract — it cannot be deployed by itself — and
    ///      (b) makes solc's unreachable-code analysis treat the call site
    ///      as opaque, avoiding the OZ ReentrancyGuard `--deny-warnings`
    ///      trip that a return-0 base implementation would cause.
    function _performSwap(uint256 amountIn, uint256 minOut) internal virtual returns (uint256);

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    /// @dev These setters and the constructor are the only writers of
    ///      `balancerPool`/`balancerVault`. The base performs NO pool-integrity
    ///      validation and offers no central post-write hook. A subclass that
    ///      layers a wiring invariant (see `BuybackBurnerBalancerV3`) MUST
    ///      override BOTH `setPool` and `setVault` and re-validate after any
    ///      direct constructor write — every write site carries the obligation
    ///      independently.

    /// @dev `newPool == address(0)` is the documented "not wired" state;
    ///      `executeBuyback` reverts with `PoolNotWired` in that case. Use
    ///      `pause()` for a single-flag disable instead of zeroing the pool.
    // slither-disable-next-line missing-zero-check
    function setPool(address newPool) public virtual onlyRole(GOVERNANCE_ROLE) {
        address old = balancerPool;
        balancerPool = newPool;
        emit PoolUpdated(old, newPool);
    }

    /// @dev `newVault == address(0)` is the documented "not wired" state;
    ///      `executeBuyback` reverts with `PoolNotWired` in that case.
    // slither-disable-next-line missing-zero-check
    function setVault(address newVault) public virtual onlyRole(GOVERNANCE_ROLE) {
        address old = balancerVault;
        balancerVault = newVault;
        emit VaultUpdated(old, newVault);
    }

    /// @notice Recover USDC stranded in this contract — e.g. inflow that
    ///         accumulated while the pool was unwired, residue left by a keeper
    ///         under-swap, or the full balance before a `setBuybackBurner`
    ///         replacement on `FeeRouter`. Without this, replacing the burner
    ///         would permanently strand the old contract's USDC.
    /// @dev Governance-gated; only moves the externally-held USDC bucket, never
    ///      TOKEN (which is always burned, never transferred out).
    function rescueUSDC(address to, uint256 amount) external onlyRole(GOVERNANCE_ROLE) {
        if (to == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        emit UsdcRescued(to, amount);
        usdc.safeTransfer(to, amount);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }
}
