// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

/// @title BuybackBurner
/// @notice Receives the 25% USDC bucket from `FeeRouter.routeSettlement`, swaps
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

    error ZeroAddress();
    error ZeroAmount();
    error PoolNotWired();
    error SwapNotImplemented();

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
        nonReentrant
        whenNotPaused
        onlyRole(KEEPER_ROLE)
        returns (uint256 tokenOut)
    {
        if (amountIn == 0) revert ZeroAmount();
        if (balancerPool == address(0) || balancerVault == address(0)) revert PoolNotWired();

        tokenOut = _performSwap(amountIn, minOut);
        if (tokenOut == 0) revert SwapNotImplemented();
        token.burn(tokenOut);
        emit BuybackExecuted(amountIn, tokenOut);
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

    function setPool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        address old = balancerPool;
        balancerPool = newPool;
        emit PoolUpdated(old, newPool);
    }

    function setVault(address newVault) external onlyRole(GOVERNANCE_ROLE) {
        address old = balancerVault;
        balancerVault = newVault;
        emit VaultUpdated(old, newVault);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }
}
