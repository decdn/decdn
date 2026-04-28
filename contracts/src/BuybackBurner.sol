// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import {
    ReentrancyGuardTransient
} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import { Errors } from "./libraries/Errors.sol";
import { Roles } from "./libraries/Roles.sol";

/// @title BuybackBurner
/// @notice Accumulates USDC protocol fees. In PoC, `executeBuyback` reverts —
///         the buyback-and-burn path activates only in production after the
///         activation criteria in ADR 004 are met. The accumulation entry
///         point is live so the treasury split can be exercised end-to-end.
/// @dev See ADR 004 §Buyback, ADR 016 §4 (USDC flow), ADR 018 §Balancer V3.
///      The Router address is stored but not used in PoC. In production a
///      code swap (no storage migration) enables `executeBuyback` against the
///      Balancer V3 pool — see ADR 018 for the Vault-approval footgun.
contract BuybackBurner is AccessControl, ReentrancyGuardTransient, Pausable {
    using SafeERC20 for IERC20;

    /// @dev Destination for burned TOKEN.
    address public constant BURN_ADDRESS = 0x000000000000000000000000000000000000dEaD;

    /// @dev Upper bound on `slippageToleranceBps` (ADR 009 governable with
    ///      safety bounds). 10% — any higher would be a governance footgun.
    uint256 public constant SLIPPAGE_CEILING_BPS = 1000;
    uint256 public constant BPS_DENOMINATOR = 10_000;

    IERC20 public immutable TOKEN_CONTRACT;
    IERC20 public immutable USDC;

    /// @notice Balancer V3 Router address. Unused in PoC; stored so a
    ///         production upgrade is a code swap, not a storage migration.
    address public immutable BALANCER_V3_ROUTER;

    /// @notice Balancer V3 pool for TOKEN/USDC (ADR 018). Mutable via
    ///         `setPool` for post-deployment configuration.
    address public pool;

    uint256 public minBuybackAmount = 1000e6;
    uint256 public maxBuybackAmount = 100_000e6;
    uint256 public slippageToleranceBps = 200;

    // ---------------------------------------------------------------------
    //  Events
    // ---------------------------------------------------------------------

    event PoolUpdated(address indexed oldPool, address indexed newPool);
    event SlippageToleranceUpdated(uint256 oldBps, uint256 newBps);
    event MinBuybackAmountUpdated(uint256 oldValue, uint256 newValue);
    event MaxBuybackAmountUpdated(uint256 oldValue, uint256 newValue);
    event USDCReceived(address indexed from, uint256 amount);

    // ---------------------------------------------------------------------
    //  Errors
    // ---------------------------------------------------------------------

    /// @dev Thrown by `executeBuyback` in the PoC build. Production replaces
    ///      the body with real swap logic; this error MUST NOT exist there.
    error BuybackDisabled();

    // ---------------------------------------------------------------------
    //  Constructor
    // ---------------------------------------------------------------------

    constructor(
        IERC20 tokenContract,
        IERC20 usdc,
        address balancerV3Router,
        address admin
    ) {
        if (
            address(tokenContract) == address(0) || address(usdc) == address(0)
                || admin == address(0)
        ) {
            revert Errors.ZeroAddress();
        }
        TOKEN_CONTRACT = tokenContract;
        USDC = usdc;
        BALANCER_V3_ROUTER = balancerV3Router; // may be zero on anvil
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        // Governable defaults are declared inline (slippage 2%, min 1k USDC,
        // max 100k USDC). Admin can tune via setters.
    }

    // ---------------------------------------------------------------------
    //  Accumulation (unchanged between PoC and production)
    // ---------------------------------------------------------------------

    /// @notice Pull `amount` USDC from `msg.sender` into the buyback pool.
    function depositUSDC(
        uint256 amount
    ) external nonReentrant whenNotPaused {
        if (amount == 0) revert Errors.ZeroAmount();
        emit USDCReceived(msg.sender, amount);
        USDC.safeTransferFrom(msg.sender, address(this), amount);
    }

    // ---------------------------------------------------------------------
    //  Execution — PoC disabled
    // ---------------------------------------------------------------------

    /// @notice PoC: reverts. Production replaces the body with a Balancer V3
    ///         Router swap + TOKEN burn (ADR 018).
    /// @dev Non-`view` on purpose: production replaces the body with a
    ///      state-mutating Balancer V3 swap. Keeping the signature
    ///      non-`view` in the PoC means the production body swap is a
    ///      pure-function change — no ABI/selector churn, no breaking
    ///      indexers or off-chain callers. Also adds `whenNotPaused`
    ///      for consistency with other fund-moving surfaces.
    function executeBuyback(
        uint256,
        /* amountIn */
        uint256 /* minTokenOut */
    ) external onlyRole(Roles.KEEPER_ROLE) whenNotPaused {
        revert BuybackDisabled();
    }

    // ---------------------------------------------------------------------
    //  Governance-controlled parameters
    // ---------------------------------------------------------------------

    /// @notice Set (or clear) the Balancer V3 pool used for buyback swaps.
    /// @dev Accepts `address(0)` intentionally: pre-activation the pool is
    ///      unset, and governance may un-set it to pause the route without
    ///      pausing accumulation. `executeBuyback` is expected to revert
    ///      when `pool == address(0)` in the production body.
    function setPool(
        address newPool
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        emit PoolUpdated(pool, newPool);
        pool = newPool;
    }

    function setSlippageToleranceBps(
        uint256 bps
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (bps > SLIPPAGE_CEILING_BPS) revert Errors.OutOfBounds();
        emit SlippageToleranceUpdated(slippageToleranceBps, bps);
        slippageToleranceBps = bps;
    }

    function setMinBuybackAmount(
        uint256 amount
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (amount > maxBuybackAmount) revert Errors.OutOfBounds();
        emit MinBuybackAmountUpdated(minBuybackAmount, amount);
        minBuybackAmount = amount;
    }

    function setMaxBuybackAmount(
        uint256 amount
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (amount < minBuybackAmount) revert Errors.OutOfBounds();
        emit MaxBuybackAmountUpdated(maxBuybackAmount, amount);
        maxBuybackAmount = amount;
    }

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
}
