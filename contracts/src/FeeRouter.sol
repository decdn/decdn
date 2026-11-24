// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";

import { IFeeRouter } from "./interfaces/IFeeRouter.sol";
import { ICapacityBondCredit } from "./interfaces/ICapacityBondCredit.sol";

/// @title FeeRouter
/// @notice Governance-chain receiver of payment-chain sweeps and the canonical
///         served-bytes accountant (ADR 016 § Contract: FeeRouter, ADR 026 §
///         FeeRouter split, ADR 036). `deposit` is permissionless and knows no
///         bridge: a bridge adapter receives a sweep and deposits the USDC that
///         arrived (ADR 016 § Bridge adapters).
/// @dev    Each deposit splits into a rebate to the operator (`rebateBps`, only
///         while the operator is active) and a treasury remainder, in the same
///         transaction. The weight credit is priced on the treasury leg, never
///         on the rebate, and is clamped to the operator's declared capacity
///         for the arrival epoch.
contract FeeRouter is IFeeRouter, AccessControl, ReentrancyGuard, SunsettingPausable {
    using SafeERC20 for IERC20;
    using Checkpoints for Checkpoints.Trace208;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    /// @notice Holder of every governance setter (the `TimelockController`).
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    /// @notice Holder of `pause` and `unpause` (the emergency multisig).
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;

    /// @dev 1 MiB in bytes, the unit `deliveryFloor` prices.
    uint256 internal constant BYTES_PER_MIB = 1_048_576;

    /// @dev Bytes per second carried by one Mbps (1e6 bits / 8).
    uint256 internal constant BYTES_PER_MBPS_SECOND = 125_000;

    /// @dev Rebate ceiling. The treasury keeps at least 1% of each deposit,
    ///      so weight credit always has a treasury leg to back it and an
    ///      empty window never locks governance out (ADR 036).
    uint256 internal constant MAX_REBATE_BPS = 9900;

    // Window bounds (ADR 036 § Governable parameters with safety bounds).
    uint64 internal constant WINDOW_EPOCHS_FLOOR = 4;
    uint64 internal constant WINDOW_EPOCHS_CEILING = 26;

    // Delivery-floor bounds (ADR 016 § Tunable Economics).
    uint256 internal constant DELIVERY_FLOOR_MIN = 1;
    uint256 internal constant DELIVERY_FLOOR_MAX = 1000;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    /// @notice The settlement token.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    /// @notice Operator registry read for the active gate and the capacity
    ///         clamp.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondCredit public immutable capacityBond;

    /// @inheritdoc IFeeRouter
    /// @dev Immutable; changing it would shift every stored epoch index.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable override epochLength;

    // -----------------------------------------------------------------
    // Storage
    // -----------------------------------------------------------------

    /// @inheritdoc IFeeRouter
    uint256 public override rebateBps;
    /// @inheritdoc IFeeRouter
    address public override treasury;
    /// @inheritdoc IFeeRouter
    uint256 public override deliveryFloor;

    mapping(address operator => mapping(uint64 epoch => uint256)) internal _bytesPerEpoch;
    mapping(uint64 epoch => uint256) internal _totalBytesPerEpoch;

    /// @dev Checkpointed `windowEpochs` history. `DecdnGovernor` reads it at a
    ///      proposal's snapshot, so a setter change never shifts an in-flight
    ///      vote.
    Checkpoints.Trace208 internal _windowEpochsHistory;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    /// @notice One deposit landed. `credit` is the bytes added to
    ///         `bytesPerEpoch[operator][epoch]`; `epoch` is the arrival epoch.
    event Deposited(
        address indexed depositor,
        address indexed operator,
        uint256 amount,
        uint256 servedBytes,
        uint256 credit,
        uint64 indexed epoch
    );
    /// @notice The rebate share changed.
    event SplitUpdated(uint256 rebateBps);
    /// @notice The weight-credit price changed.
    event DeliveryFloorUpdated(uint256 oldValue, uint256 newValue);
    /// @notice The treasury destination changed.
    event TreasuryUpdated(address indexed oldAddr, address indexed newAddr);
    /// @notice The trailing-window length changed.
    event WindowEpochsUpdated(uint64 oldValue, uint64 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    /// @notice A required address is zero.
    error ZeroAddress();
    /// @notice A deposit moved no USDC.
    error ZeroAmount();
    /// @notice `rebateBps` exceeds 9900, which leaves the treasury less than 1%.
    error RebateAboveCeiling(uint256 rebateBps);
    /// @notice `deliveryFloor` is outside [1, 1000].
    error DeliveryFloorOutOfBounds(uint256 value);
    /// @notice `windowEpochs` is outside [4, 26].
    error WindowOutOfBounds(uint64 value, uint64 floor, uint64 ceiling);
    /// @notice The constructor got a zero epoch length.
    error ZeroEpochLength();
    /// @notice The bond and the router disagree on the epoch length.
    error EpochLengthMismatch(uint64 capacityBondEpoch, uint64 routerEpoch);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_          USDC token address.
    /// @param capacityBond_  `CapacityBond`, read on every deposit.
    /// @param treasury_      Treasury (`TimelockController`).
    /// @param epochLength_   Epoch length (1 week). Must equal the bond's.
    /// @param windowEpochs_  Initial trailing-window length (bounded [4, 26]).
    /// @param deliveryFloor_ Initial weight-credit price (bounded [1, 1000]).
    /// @param admin          `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder.
    /// @param rebateBps_     Initial rebate share (at most 9900).
    constructor(
        IERC20 usdc_,
        ICapacityBondCredit capacityBond_,
        address treasury_,
        uint64 epochLength_,
        uint64 windowEpochs_,
        uint256 deliveryFloor_,
        address admin,
        uint256 rebateBps_
    ) {
        if (
            address(usdc_) == address(0) || address(capacityBond_) == address(0) || treasury_ == address(0)
                || admin == address(0)
        ) revert ZeroAddress();
        if (epochLength_ == 0) revert ZeroEpochLength();
        uint64 bondEpoch = capacityBond_.epochLength();
        if (bondEpoch != epochLength_) revert EpochLengthMismatch(bondEpoch, epochLength_);
        _enforceWindowBounds(windowEpochs_);
        _enforceFloorBounds(deliveryFloor_);

        usdc = usdc_;
        capacityBond = capacityBond_;
        epochLength = epochLength_;
        treasury = treasury_;
        deliveryFloor = deliveryFloor_;
        _setSplit(rebateBps_);
        // The window is bounded to [4, 26], so the uint208 cast is exact.
        // slither-disable-next-line unused-return
        _windowEpochsHistory.push(Time.timestamp(), uint208(uint256(windowEpochs_)));

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        // Deploy renounces DEFAULT_ADMIN_ROLE, freezing the role table.
        // PAUSER_ROLE stays rotatable under GOVERNANCE_ROLE so a timelocked
        // proposal can evict a compromised emergency pauser.
        _setRoleAdmin(PAUSER_ROLE, GOVERNANCE_ROLE);
    }

    // -----------------------------------------------------------------
    // Deposit
    // -----------------------------------------------------------------

    /// @inheritdoc IFeeRouter
    function deposit(address operator, uint256 servedBytes, uint256 amount)
        external
        override
        nonReentrant
        whenNotPaused
    {
        if (operator == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();

        usdc.safeTransferFrom(msg.sender, address(this), amount);

        // forge-lint: disable-next-line(block-timestamp)
        uint64 epoch = uint64(block.timestamp / epochLength);
        bool active = capacityBond.isActive(operator);

        uint256 rebate = active ? (amount * rebateBps) / BPS_DENOMINATOR : 0;
        uint256 toTreasury = amount - rebate;

        uint256 credit = 0;
        if (active) credit = _credit(operator, epoch, servedBytes, toTreasury);

        // A rebate the operator cannot receive (a blocked address) joins the
        // treasury leg, so one bad operator address never strands a sweep.
        if (rebate != 0 && !_tryTransfer(operator, rebate)) toTreasury += rebate;
        if (toTreasury != 0) usdc.safeTransfer(treasury, toTreasury);

        emit Deposited(msg.sender, operator, amount, servedBytes, credit, epoch);
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
        // No legitimate caller needs a window above the governable ceiling,
        // and an unclamped `n` is an unbounded loop.
        if (n > WINDOW_EPOCHS_CEILING) n = WINDOW_EPOCHS_CEILING;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _bytesPerEpoch[operator][e];
        }
    }

    /// @inheritdoc IFeeRouter
    function totalBytesInWindow(uint64 endEpoch, uint64 n) external view override returns (uint256 sum) {
        if (n == 0) return 0;
        if (n > WINDOW_EPOCHS_CEILING) n = WINDOW_EPOCHS_CEILING;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _totalBytesPerEpoch[e];
        }
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
    // Governance
    // -----------------------------------------------------------------

    /// @notice Set the rebate share (at most 9900). The treasury takes the
    ///         remainder.
    function setSplit(uint256 newRebateBps) external onlyRole(GOVERNANCE_ROLE) {
        _setSplit(newRebateBps);
    }

    /// @notice Set the treasury destination.
    function setTreasury(address newAddr) external onlyRole(GOVERNANCE_ROLE) {
        if (newAddr == address(0)) revert ZeroAddress();
        address old = treasury;
        treasury = newAddr;
        emit TreasuryUpdated(old, newAddr);
    }

    /// @notice Set the weight-credit price (bounded [1, 1000]).
    function setDeliveryFloor(uint256 newFloor) external onlyRole(GOVERNANCE_ROLE) {
        _enforceFloorBounds(newFloor);
        uint256 old = deliveryFloor;
        deliveryFloor = newFloor;
        emit DeliveryFloorUpdated(old, newFloor);
    }

    /// @notice Set the trailing-window length (bounded [4, 26]).
    function setWindowEpochs(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceWindowBounds(newWindow);
        uint64 old = windowEpochs();
        // slither-disable-next-line unused-return
        _windowEpochsHistory.push(Time.timestamp(), uint208(uint256(newWindow)));
        emit WindowEpochsUpdated(old, newWindow);
    }

    // -----------------------------------------------------------------
    // Pause control
    // -----------------------------------------------------------------

    /// @notice Block `deposit`. Bridge messages stay valid until unpause.
    function pause() external onlyRole(PAUSER_ROLE) {
        _requirePauseWindowOpen();
        _pause();
    }

    /// @notice Lift the pause.
    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    /// @dev The USDC-backed, capacity-clamped credit, written to the operator
    ///      and network counters for `epoch`. `backing` is the treasury leg.
    function _credit(address operator, uint64 epoch, uint256 servedBytes, uint256 backing)
        internal
        returns (uint256 credit)
    {
        credit = Math.min(servedBytes, Math.mulDiv(backing, BYTES_PER_MIB, deliveryFloor));
        uint256 cap = capacityBond.declaredMbps(operator) * uint256(epochLength) * BYTES_PER_MBPS_SECOND;
        uint256 used = _bytesPerEpoch[operator][epoch];
        uint256 room = cap > used ? cap - used : 0;
        if (credit > room) credit = room;
        if (credit != 0) {
            _bytesPerEpoch[operator][epoch] = used + credit;
            _totalBytesPerEpoch[epoch] += credit;
        }
    }

    /// @dev A USDC transfer that reports failure instead of reverting.
    function _tryTransfer(address to, uint256 value) internal returns (bool) {
        // slither-disable-next-line low-level-calls
        (bool ok, bytes memory ret) = address(usdc).call(abi.encodeCall(IERC20.transfer, (to, value)));
        if (!ok) return false;
        return ret.length == 0 || (ret.length >= 32 && abi.decode(ret, (bool)));
    }

    function _setSplit(uint256 newRebateBps) internal {
        if (newRebateBps > MAX_REBATE_BPS) revert RebateAboveCeiling(newRebateBps);
        rebateBps = newRebateBps;
        emit SplitUpdated(newRebateBps);
    }

    function _enforceWindowBounds(uint64 value) internal pure {
        if (value < WINDOW_EPOCHS_FLOOR || value > WINDOW_EPOCHS_CEILING) {
            revert WindowOutOfBounds(value, WINDOW_EPOCHS_FLOOR, WINDOW_EPOCHS_CEILING);
        }
    }

    function _enforceFloorBounds(uint256 value) internal pure {
        if (value < DELIVERY_FLOOR_MIN || value > DELIVERY_FLOOR_MAX) revert DeliveryFloorOutOfBounds(value);
    }
}
