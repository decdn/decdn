// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";

import { IFeeRouter } from "./interfaces/IFeeRouter.sol";
import { ICapacityBondReporter } from "./interfaces/ICapacityBondReporter.sol";

/// @title FeeRouter
/// @notice Three-bucket settlement distributor + canonical served-bytes
///         accountant. Per ADR 026 § FeeRouter split, every settlement
///         transfers `amount` USDC as 60% operator base / 30% buyback /
///         10% treasury in the same transaction. Per ADR 036,
///         `bytesPerEpoch[op][e]` and `totalBytesPerEpoch[e]` are
///         incremented inline on every settlement and consumed by
///         `DecdnGovernor._getVotes` over the trailing `windowEpochs` window.
/// @dev    Bucket shares and dependency addresses are governance-mutable
///         under the `setShares` / `set*` setters with a cross-validation
///         invariant (any non-zero share ⇒ non-zero destination — note the
///         one-way implication: a non-zero destination with a zero share is
///         allowed during launch wiring). Buyback / treasury
///         buckets short-circuit before the `safeTransfer` when their share
///         is 0; the operator share is bounded away from 0 by
///         `OPERATOR_BPS_FLOOR` and absorbs the rounding remainder so dust
///         never gets routed to an inactive bucket's `address(0)` sink.
contract FeeRouter is IFeeRouter, AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;
    using Checkpoints for Checkpoints.Trace208;
    using SafeCast for uint256;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant ROUTER_CALLER_ROLE = keccak256("ROUTER_CALLER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Bucket index constants — see `_shares` layout
    // -----------------------------------------------------------------

    uint256 internal constant BUCKET_OPERATOR = 0;
    uint256 internal constant BUCKET_BUYBACK = 1;
    uint256 internal constant BUCKET_TREASURY = 2;

    uint256 internal constant BPS_DENOMINATOR = 10_000;

    // Per-share bounds (ADR 026 § Governable parameters with safety bounds).
    uint256 internal constant OPERATOR_BPS_FLOOR = 4000;
    uint256 internal constant OPERATOR_BPS_CEILING = 9000;
    uint256 internal constant BUYBACK_BPS_FLOOR = 500;
    uint256 internal constant BUYBACK_BPS_CEILING = 5000;
    uint256 internal constant TREASURY_BPS_CEILING = 3000;

    // Window bounds (ADR 036 § Governable parameters with safety bounds).
    uint64 internal constant WINDOW_EPOCHS_FLOOR = 4;
    uint64 internal constant WINDOW_EPOCHS_CEILING = 26;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondReporter public immutable capacityBond;

    /// @inheritdoc IFeeRouter
    /// @dev Immutable; changing it would shift every stored epoch index.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable override epochLength;

    // -----------------------------------------------------------------
    // Storage — shares + destinations
    // -----------------------------------------------------------------

    /// @dev Layout `[operatorBase, buyback, treasury]`, in bps.
    ///      Sum-to-10_000 invariant enforced by every setter.
    uint256[3] internal _shares;

    address public buybackBurner;
    address public treasury;

    // -----------------------------------------------------------------
    // Storage — bytes accounting (ADR 036)
    // -----------------------------------------------------------------

    mapping(address operator => mapping(uint64 epoch => uint256)) internal _bytesPerEpoch;
    mapping(uint64 epoch => uint256) internal _totalBytesPerEpoch;

    /// @notice Checkpointed `windowEpochs` history (ADR 036 § Governable
    ///         parameters with safety bounds). Read by `DecdnGovernor` via
    ///         `windowEpochsAt(snapshotTp)` so a mid-vote setter change does
    ///         not shift quorum / vote weights for in-flight proposals
    ///         (mirrors the I4 pattern on `DecdnGovernor.voteCapBps`).
    Checkpoints.Trace208 internal _windowEpochsHistory;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Settled(address indexed operator, uint256 bytesDelivered, uint256 amount, uint64 indexed epoch);
    event SharesUpdated(uint256[3] newShares);
    event BuybackBurnerUpdated(address indexed oldAddr, address indexed newAddr);
    event TreasuryUpdated(address indexed oldAddr, address indexed newAddr);
    event WindowEpochsUpdated(uint64 oldValue, uint64 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error SharesDoNotSum(uint256 sum);
    error ShareOutOfBounds(uint256 bucket, uint256 value, uint256 floor, uint256 ceiling);
    error NonZeroShareNeedsDestination(uint256 bucket);
    error WindowOutOfBounds(uint64 value, uint64 floor, uint64 ceiling);
    error ZeroEpochLength();
    error EpochLengthMismatch(uint64 capacityBondEpoch, uint64 routerEpoch);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_              USDC token address.
    /// @param capacityBond_      `CapacityBond` (settlement reporter sink).
    /// @param treasury_          Treasury / `TimelockController` address.
    /// @param epochLength_       Constructor-immutable epoch length (1 week
    ///                           in production). Must be non-zero.
    /// @param windowEpochs_      Initial trailing-window length (default 13;
    ///                           bounded `[4, 26]`).
    /// @param admin              `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder.
    /// @param initialShares      Initial bucket shares `[op, bb, tr]`
    ///                           in bps. Must sum to 10_000 and satisfy the
    ///                           cross-validation invariant against
    ///                           `buybackBurner_`.
    /// @param buybackBurner_     Initial `BuybackBurner` address. May be 0
    ///                           iff `initialShares[BUCKET_BUYBACK] == 0`.
    constructor(
        IERC20 usdc_,
        ICapacityBondReporter capacityBond_,
        address treasury_,
        uint64 epochLength_,
        uint64 windowEpochs_,
        address admin,
        uint256[3] memory initialShares,
        // slither-disable-next-line missing-zero-check
        address buybackBurner_
    ) {
        if (address(usdc_) == address(0) || address(capacityBond_) == address(0) || treasury_ == address(0)) {
            revert ZeroAddress();
        }
        if (admin == address(0)) revert ZeroAddress();
        if (epochLength_ == 0) revert ZeroEpochLength();
        uint64 bondEpoch = capacityBond_.epochLength();
        if (bondEpoch != epochLength_) revert EpochLengthMismatch(bondEpoch, epochLength_);
        _enforceWindowBounds(windowEpochs_);

        usdc = usdc_;
        capacityBond = capacityBond_;
        treasury = treasury_;
        epochLength = epochLength_;
        // Seed the checkpoint at clock()=now so any subsequent
        // `windowEpochsAt(timepoint)` read at a timepoint ≥ deploy returns
        // the constructor-set value. Trace208 stores uint208, the value is
        // bounded to [4, 26] by `_enforceWindowBounds` above so the cast
        // is trivially safe.
        // slither-disable-next-line unused-return
        _windowEpochsHistory.push(_clock(), uint208(uint256(windowEpochs_)));

        // Destinations first, then shares — the cross-validation check inside
        // `_setShares` reads the destination state and requires both to be
        // consistent at the end of construction.
        buybackBurner = buybackBurner_;
        _setShares(initialShares);

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Settlement entrypoint
    // -----------------------------------------------------------------

    /// @inheritdoc IFeeRouter
    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount)
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(ROUTER_CALLER_ROLE)
    {
        if (operator == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();

        // Pull USDC from the caller (PaymentChannel) once, then push out the
        // three legs. Pulls + pushes share the same token, so we avoid the
        // approve-from-channel race by transferring in here. PaymentChannel
        // must `approve(this, amount)` before calling.
        usdc.safeTransferFrom(msg.sender, address(this), amount);

        // forge-lint: disable-next-line(block-timestamp)
        uint64 epoch = uint64(block.timestamp / epochLength);
        if (bytesDelivered != 0) {
            _bytesPerEpoch[operator][epoch] += bytesDelivered;
            _totalBytesPerEpoch[epoch] += bytesDelivered;
        }

        uint256[3] memory s = _shares;

        uint256 buybackShare = (amount * s[BUCKET_BUYBACK]) / BPS_DENOMINATOR;
        uint256 treasuryShare = (amount * s[BUCKET_TREASURY]) / BPS_DENOMINATOR;
        // Operator leg absorbs the rounding remainder: operator share is
        // bounded ≥ `OPERATOR_BPS_FLOOR` (4000 bps) and its destination is the
        // per-call operator argument (always non-zero by the entry guard) — so
        // the dust transfer never targets `address(0)`.
        uint256 opShare = amount - buybackShare - treasuryShare;

        if (opShare != 0) usdc.safeTransfer(operator, opShare);
        if (buybackShare != 0) usdc.safeTransfer(buybackBurner, buybackShare);
        if (treasuryShare != 0) usdc.safeTransfer(treasury, treasuryShare);

        capacityBond.recordSettlement(operator);

        emit Settled(operator, bytesDelivered, amount, epoch);
    }

    // -----------------------------------------------------------------
    // Views — bytes accounting
    // -----------------------------------------------------------------

    /// @inheritdoc IFeeRouter
    function bytesPerEpoch(address operator, uint64 epoch) external view override returns (uint256) {
        return _bytesPerEpoch[operator][epoch];
    }

    /// @inheritdoc IFeeRouter
    function totalBytesPerEpoch(uint64 epoch) external view override returns (uint256) {
        return _totalBytesPerEpoch[epoch];
    }

    /// @inheritdoc IFeeRouter
    function bytesInWindow(address operator, uint64 endEpoch, uint64 n) external view override returns (uint256 sum) {
        if (n == 0) return 0;
        // Clamp the window length to the governable ceiling: no legitimate caller
        // (the Governor passes `windowEpochs() <= WINDOW_EPOCHS_CEILING`) needs a
        // longer span, and an unclamped `n` is an unbounded on-chain loop.
        if (n > WINDOW_EPOCHS_CEILING) n = WINDOW_EPOCHS_CEILING;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _bytesPerEpoch[operator][e];
        }
    }

    /// @inheritdoc IFeeRouter
    function totalBytesInWindow(uint64 endEpoch, uint64 n) external view override returns (uint256 sum) {
        if (n == 0) return 0;
        // Clamp to the governable ceiling — see `bytesInWindow`.
        if (n > WINDOW_EPOCHS_CEILING) n = WINDOW_EPOCHS_CEILING;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _totalBytesPerEpoch[e];
        }
    }

    // -----------------------------------------------------------------
    // Views — shares + destinations
    // -----------------------------------------------------------------

    function getShares() external view returns (uint256[3] memory) {
        return _shares;
    }

    // -----------------------------------------------------------------
    // Governance — shares + destinations + windowEpochs
    // -----------------------------------------------------------------

    function setShares(uint256[3] calldata newShares) external onlyRole(GOVERNANCE_ROLE) {
        _setShares(newShares);
    }

    struct ShareDestinations {
        address buybackBurner;
        address treasury;
    }

    /// @notice Atomic shares + destinations update for governance bucket
    ///         activation. Sets destinations first (zero-share buckets may
    ///         have any address), then shares (cross-validation enforces
    ///         non-zero share ⇒ non-zero destination).
    function setSharesAndDestinations(uint256[3] calldata newShares, ShareDestinations calldata dests)
        external
        onlyRole(GOVERNANCE_ROLE)
    {
        if (dests.treasury == address(0)) revert ZeroAddress();
        _setBuybackBurner(dests.buybackBurner);
        _setTreasury(dests.treasury);
        _setShares(newShares);
    }

    function setBuybackBurner(address newAddr) external onlyRole(GOVERNANCE_ROLE) {
        if (newAddr == address(0) && _shares[BUCKET_BUYBACK] != 0) {
            revert NonZeroShareNeedsDestination(BUCKET_BUYBACK);
        }
        _setBuybackBurner(newAddr);
    }

    function setTreasury(address newAddr) external onlyRole(GOVERNANCE_ROLE) {
        if (newAddr == address(0)) revert ZeroAddress();
        _setTreasury(newAddr);
    }

    function setWindowEpochs(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceWindowBounds(newWindow);
        uint64 old = windowEpochs();
        // slither-disable-next-line unused-return
        _windowEpochsHistory.push(_clock(), uint208(uint256(newWindow)));
        emit WindowEpochsUpdated(old, newWindow);
    }

    /// @inheritdoc IFeeRouter
    function windowEpochs() public view override returns (uint64) {
        return uint64(_windowEpochsHistory.latest());
    }

    /// @inheritdoc IFeeRouter
    function windowEpochsAt(uint48 timepoint) external view override returns (uint64) {
        return uint64(_windowEpochsHistory.upperLookupRecent(timepoint));
    }

    // -----------------------------------------------------------------
    // Pause control
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal — shares + destinations
    // -----------------------------------------------------------------

    function _setShares(uint256[3] memory newShares) internal {
        uint256 sum = newShares[0] + newShares[1] + newShares[2];
        if (sum != BPS_DENOMINATOR) revert SharesDoNotSum(sum);

        if (newShares[BUCKET_OPERATOR] < OPERATOR_BPS_FLOOR || newShares[BUCKET_OPERATOR] > OPERATOR_BPS_CEILING) {
            revert ShareOutOfBounds({
                bucket: BUCKET_OPERATOR,
                value: newShares[BUCKET_OPERATOR],
                floor: OPERATOR_BPS_FLOOR,
                ceiling: OPERATOR_BPS_CEILING
            });
        }
        // Buyback floor only enforced when the bucket is active (non-zero).
        // Setting it to zero is permitted (launch-mode dormancy).
        if (newShares[BUCKET_BUYBACK] != 0) {
            if (newShares[BUCKET_BUYBACK] < BUYBACK_BPS_FLOOR || newShares[BUCKET_BUYBACK] > BUYBACK_BPS_CEILING) {
                revert ShareOutOfBounds({
                    bucket: BUCKET_BUYBACK,
                    value: newShares[BUCKET_BUYBACK],
                    floor: BUYBACK_BPS_FLOOR,
                    ceiling: BUYBACK_BPS_CEILING
                });
            }
        }
        if (newShares[BUCKET_TREASURY] > TREASURY_BPS_CEILING) {
            revert ShareOutOfBounds({
                bucket: BUCKET_TREASURY, value: newShares[BUCKET_TREASURY], floor: 0, ceiling: TREASURY_BPS_CEILING
            });
        }

        // Cross-validation invariant: non-zero share ⇒ non-zero destination.
        if (newShares[BUCKET_BUYBACK] != 0 && buybackBurner == address(0)) {
            revert NonZeroShareNeedsDestination(BUCKET_BUYBACK);
        }
        if (newShares[BUCKET_TREASURY] != 0 && treasury == address(0)) {
            revert NonZeroShareNeedsDestination(BUCKET_TREASURY);
        }

        _shares = newShares;
        emit SharesUpdated(newShares);
    }

    function _setBuybackBurner(address newAddr) internal {
        address old = buybackBurner;
        buybackBurner = newAddr;
        emit BuybackBurnerUpdated(old, newAddr);
    }

    function _setTreasury(address newAddr) internal {
        address old = treasury;
        treasury = newAddr;
        emit TreasuryUpdated(old, newAddr);
    }

    function _enforceWindowBounds(uint64 value) internal pure {
        if (value < WINDOW_EPOCHS_FLOOR || value > WINDOW_EPOCHS_CEILING) {
            revert WindowOutOfBounds(value, WINDOW_EPOCHS_FLOOR, WINDOW_EPOCHS_CEILING);
        }
    }

    /// @dev Matches `DecdnGovernor.clock()` (ERC-6372 timestamp mode) so the
    ///      checkpoint timepoints in this contract and the governor share
    ///      one time axis. Wrapped here to keep the constructor and setter
    ///      readable.
    function _clock() internal view returns (uint48) {
        return Time.timestamp();
    }
}
