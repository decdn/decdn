// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { ISlashAppeal } from "./interfaces/ISlashAppeal.sol";
import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { ICapacityBondSlashEscrow } from "./interfaces/ICapacityBondSlashEscrow.sol";

/// @title SlashAppeal
/// @notice The slash-appeal state machine (ADR 028 § Contract surface). Under
///         escrow-on-slash the slashed TOKEN is held in `CapacityBond` until an
///         appeal resolves; this contract decides the outcome and instructs
///         `CapacityBond` to either refund the operator (`settleAppealGranted`)
///         or distribute 50/50 (`settleAppealUpheld`). It holds no slash
///         escrow itself — only the per-appeal TOKEN bond.
/// @dev    Flow: `openSlashAppeal` (operator posts bond) → emergency-multisig
///         `fastTrackAppeal` / `rejectAppeal` → Governor `grantAppeal`
///         (operator vindicated) / `upholdAppeal` (slash stands). A
///         permissionless `cleanupExpiredAppeal` resolves appeals the multisig
///         or Governor let lapse. The appeal is keyed by `slashId`; the
///         `CapacityBond` escrow status is the one-shot guard against a second
///         appeal for the same slash (`markAppealOpen` reverts once it is no
///         longer `Escrowed`).
///
///         Naming vs. the retired `SafetyReserve` flow: `grantAppeal` ==
///         operator vindicated (the old `reverseAppeal` — slash undone), and
///         `upholdAppeal` == slash stands (the old `reverseAppeal`-as-failure
///         path). The token flows are the inverse of the old USDC-restitution
///         `ratifyAppeal`; this contract never moves USDC.
contract SlashAppeal is ISlashAppeal, AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant EMERGENCY_MULTISIG_ROLE = keccak256("EMERGENCY_MULTISIG_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Windows + bond bounds (ADR 028 § Hard caps and frequency limits)
    // -----------------------------------------------------------------

    /// @notice Time the emergency multisig has to fast-track or reject an Open
    ///         appeal before it lapses to upheld (slash stands).
    uint256 internal constant APPEAL_REVIEW_WINDOW = 14 days;

    /// @notice Time the Governor has to grant or uphold a FastTracked appeal
    ///         before it lapses operator-favorably (governance inaction is not
    ///         the appellant's fault).
    uint256 internal constant APPEAL_RATIFICATION_WINDOW = 14 days;

    /// @notice One accepted appeal per operator per this window.
    uint256 internal constant APPEAL_FREQUENCY_WINDOW = 365 days;

    uint256 internal constant APPEAL_BOND_FLOOR = 100e18;
    uint256 internal constant APPEAL_BOND_CEILING = 10_000e18;

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    /// @dev TOKEN — appeal bonds are pulled in here and burned / refunded.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    /// @dev `CapacityBond` — read for slash records, called for escrow settle
    ///      via `ICapacityBondSlashEscrow(address(capacityBond))`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBond public immutable capacityBond;

    // -----------------------------------------------------------------
    // Governance-mutable wiring
    // -----------------------------------------------------------------

    /// @notice Destination for the 50% non-burn leg of a failed (upheld) appeal
    ///         bond — funds parties who file successful counter-evidence.
    address public challengerIncentivePool;

    uint256 public appealBond;

    // -----------------------------------------------------------------
    // Appeal state (keyed by slashId — one appeal per slash)
    // -----------------------------------------------------------------

    struct Appeal {
        address appellant;
        address operator;
        uint256 bond;
        bytes32 evidenceBundleHash;
        uint64 openedAt;
        uint64 fastTrackedAt;
        AppealStatus status;
    }

    mapping(uint256 slashId => Appeal) internal _appeals;

    /// @notice Timestamp of the operator's last GRANTED appeal (frequency cap).
    mapping(address operator => uint64) public lastAcceptedAppealAt;

    /// @notice `block.timestamp` at which the current pause began; 0 when not
    ///         paused. The pause duration is not accumulated locally — on
    ///         unpause it is credited to `CapacityBond.pausedTotal` (the single
    ///         combined counter), and the review / ratification window checks
    ///         read that combined value, so a pause on either contract extends
    ///         every window uniformly (ADR 028 §5).
    uint64 internal _pausedAt;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event AppealOpened(
        uint256 indexed slashId,
        address indexed operator,
        address indexed appellant,
        uint256 bond,
        bytes32 evidenceBundleHash
    );
    event AppealFastTracked(uint256 indexed slashId);
    event AppealRejected(uint256 indexed slashId, uint256 bondBurned);
    event AppealGranted(uint256 indexed slashId, address indexed operator, uint256 bondRefunded);
    event AppealUpheld(uint256 indexed slashId, uint256 bondBurned, uint256 toChallengerPool);
    /// @notice `reason` 1 = review-window lapse (upheld); 2 = ratification-window
    ///         lapse (granted, operator-favorable).
    event AppealLapsed(uint256 indexed slashId, uint8 reason);
    event AppealBondUpdated(uint256 oldValue, uint256 newValue);
    event ChallengerIncentivePoolUpdated(address indexed oldAddr, address indexed newAddr);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error UnknownSlash(uint256 slashId);
    error CallerNotOperator(address operator);
    error FrequencyCapHit(uint64 nextAvailableAt);
    error AppealAlreadyExists(uint256 slashId);
    error AppealNotOpen(uint256 slashId);
    error AppealNotFastTracked(uint256 slashId);
    error ReviewWindowOpen(uint64 readyAt);
    error RatificationWindowOpen(uint64 readyAt);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_              TOKEN (must be `ERC20Burnable`).
    /// @param capacityBond_       `CapacityBond` (slash records + escrow hooks).
    /// @param admin               Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE`.
    /// @param emergencyMultisig   Initial `EMERGENCY_MULTISIG_ROLE` (may be 0).
    /// @param appealBond_         Initial appeal bond (bounded `[100, 10_000]e18`).
    constructor(
        ERC20Burnable token_,
        ICapacityBond capacityBond_,
        address admin,
        address emergencyMultisig,
        uint256 appealBond_
    ) {
        if (address(token_) == address(0) || address(capacityBond_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceAppealBondBounds(appealBond_);

        token = token_;
        capacityBond = capacityBond_;
        appealBond = appealBond_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        if (emergencyMultisig != address(0)) {
            _grantRole(EMERGENCY_MULTISIG_ROLE, emergencyMultisig);
        }
    }

    // -----------------------------------------------------------------
    // Appeal lifecycle
    // -----------------------------------------------------------------

    /// @inheritdoc ISlashAppeal
    /// @dev Operator-only: `msg.sender` MUST be the slashed operator (read from
    ///      `CapacityBond.slashRecords`, so it cannot be forged). The appeal
    ///      slot is one-shot per `slashId` (`markAppealOpen` flips the escrow
    ///      out of `Escrowed`), so a permissionless filer would let anyone —
    ///      notably the challenger, who earns 50% of an upheld slash — burn the
    ///      operator's only chance at recourse with a junk appeal. Requiring the
    ///      operator closes that griefing/front-running vector (ADR 028 §1).
    // slither-disable-next-line reentrancy-no-eth,unused-return
    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash) external override nonReentrant whenNotPaused {
        // `slashRecords` is a view on the trusted, immutable `CapacityBond`;
        // the subsequent state writes are guarded by `nonReentrant`.
        // aderyn-ignore-next-line(reentrancy-state-change)
        (address operator,,) = capacityBond.slashRecords(slashId);
        if (operator == address(0)) revert UnknownSlash(slashId);
        if (msg.sender != operator) revert CallerNotOperator(operator);
        if (_appeals[slashId].status != AppealStatus.None) revert AppealAlreadyExists(slashId);

        uint64 last = lastAcceptedAppealAt[operator];
        if (last != 0) {
            uint64 nextAvailable = last + uint64(APPEAL_FREQUENCY_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < nextAvailable) revert FrequencyCapHit(nextAvailable);
        }

        // CEI: write appeal state before the bond pull + external escrow lock.
        uint256 bond = appealBond;
        _appeals[slashId] = Appeal({
            appellant: msg.sender,
            operator: operator,
            bond: bond,
            evidenceBundleHash: evidenceBundleHash,
            openedAt: uint64(block.timestamp),
            fastTrackedAt: 0,
            status: AppealStatus.Open
        });

        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), bond);
        // Locks the escrow + enforces the filing-window deadline on CapacityBond.
        ICapacityBondSlashEscrow(address(capacityBond)).markAppealOpen(slashId);

        emit AppealOpened(slashId, operator, msg.sender, bond, evidenceBundleHash);
    }

    /// @inheritdoc ISlashAppeal
    function fastTrackAppeal(uint256 slashId) external override whenNotPaused onlyRole(EMERGENCY_MULTISIG_ROLE) {
        Appeal storage a = _appeals[slashId];
        if (a.status != AppealStatus.Open) revert AppealNotOpen(slashId);
        a.status = AppealStatus.FastTracked;
        a.fastTrackedAt = uint64(block.timestamp);
        emit AppealFastTracked(slashId);
    }

    /// @inheritdoc ISlashAppeal
    /// @dev Multisig rejection at intake (appeal fails before any interim
    ///      relief): burn the full bond and uphold the slash. Restricted to
    ///      `Open` appeals — once an appeal is `FastTracked`, only the Governor
    ///      may resolve it (`grantAppeal` / `upholdAppeal`), preserving the
    ///      two-stage governance process. A fast-tracked appeal that fails goes
    ///      through `upholdAppeal` (50/50 bond split), not `rejectAppeal`.
    function rejectAppeal(uint256 slashId)
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(EMERGENCY_MULTISIG_ROLE)
    {
        Appeal storage a = _appeals[slashId];
        if (a.status != AppealStatus.Open) revert AppealNotOpen(slashId);
        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Resolved;

        ICapacityBondSlashEscrow(address(capacityBond)).settleAppealUpheld(slashId);
        if (bondBurned != 0) token.burn(bondBurned);
        emit AppealRejected(slashId, bondBurned);
    }

    /// @inheritdoc ISlashAppeal
    /// @dev Governor grants the appeal — the operator was wrongly slashed.
    ///      Refund the bond, stamp the frequency cap, and instruct CapacityBond
    ///      to refund the escrowed TOKEN + clear the slash zero-out.
    function grantAppeal(uint256 slashId) external override nonReentrant whenNotPaused onlyRole(GOVERNANCE_ROLE) {
        Appeal storage a = _appeals[slashId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(slashId);
        uint256 bondRefund = a.bond;
        address appellant = a.appellant;
        address operator = a.operator;
        a.bond = 0;
        a.status = AppealStatus.Resolved;
        lastAcceptedAppealAt[operator] = uint64(block.timestamp);

        ICapacityBondSlashEscrow(address(capacityBond)).settleAppealGranted(slashId);
        if (bondRefund != 0) IERC20(address(token)).safeTransfer(appellant, bondRefund);
        emit AppealGranted(slashId, operator, bondRefund);
    }

    /// @inheritdoc ISlashAppeal
    /// @dev Governor upholds the slash — the fast-tracked appeal fails.
    ///      Distribute the escrow 50/50 and split the bond 50% burn / 50% to the
    ///      challenger-incentive pool. If the pool is unwired (`address(0)`),
    ///      degrade gracefully to a 100% bond burn — matching `rejectAppeal` —
    ///      rather than reverting; otherwise a misconfiguration would block the
    ///      uphold and let `cleanupExpiredAppeal` flip it to an operator-
    ///      favorable grant after the ratification window (ADR 028 §3).
    function upholdAppeal(uint256 slashId) external override nonReentrant whenNotPaused onlyRole(GOVERNANCE_ROLE) {
        Appeal storage a = _appeals[slashId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(slashId);
        uint256 bondTotal = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Resolved;

        ICapacityBondSlashEscrow(address(capacityBond)).settleAppealUpheld(slashId);

        address pool = challengerIncentivePool;
        uint256 toPool = pool == address(0) ? 0 : bondTotal / 2;
        uint256 toBurn = bondTotal - toPool;
        if (toPool != 0) {
            // Degrade to burn if the pool transfer fails — a reverting or
            // blocklisting `challengerIncentivePool` must not be able to block
            // the uphold and let `cleanupExpiredAppeal` flip it to an
            // operator-favorable grant after the ratification window (ADR 028
            // §3). Mirrors the `address(0)` degrade above, defensively.
            try IERC20(address(token)).transfer(pool, toPool) returns (bool ok) {
                if (!ok) {
                    toBurn += toPool;
                    toPool = 0;
                }
            } catch {
                toBurn += toPool;
                toPool = 0;
            }
        }
        if (toBurn != 0) token.burn(toBurn);
        emit AppealUpheld(slashId, toBurn, toPool);
    }

    /// @inheritdoc ISlashAppeal
    /// @dev Permissionless lapse handler. Open + review-window elapsed → uphold
    ///      (burn bond). FastTracked + ratification-window elapsed → grant
    ///      (refund bond — governance inactivity is not the appellant's fault).
    function cleanupExpiredAppeal(uint256 slashId) external override nonReentrant whenNotPaused {
        Appeal storage a = _appeals[slashId];
        uint64 combinedPaused = ICapacityBondSlashEscrow(address(capacityBond)).pausedTotal();
        if (a.status == AppealStatus.Open) {
            // Review window extended by the combined paused duration (this
            // contract's pauses + CapacityBond's) so a pause on either never
            // silently consumes the multisig's window (ADR 028 §5).
            uint64 readyAt = a.openedAt + uint64(APPEAL_REVIEW_WINDOW) + combinedPaused;
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert ReviewWindowOpen(readyAt);
            uint256 bondBurned = a.bond;
            a.bond = 0;
            a.status = AppealStatus.Resolved;
            ICapacityBondSlashEscrow(address(capacityBond)).settleAppealUpheld(slashId);
            if (bondBurned != 0) token.burn(bondBurned);
            emit AppealLapsed(slashId, 1);
            return;
        }
        if (a.status == AppealStatus.FastTracked) {
            // Ratification window extended by the combined paused duration.
            uint64 readyAt = a.fastTrackedAt + uint64(APPEAL_RATIFICATION_WINDOW) + combinedPaused;
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert RatificationWindowOpen(readyAt);
            uint256 bondRefund = a.bond;
            address appellant = a.appellant;
            address operator = a.operator;
            a.bond = 0;
            a.status = AppealStatus.Resolved;
            lastAcceptedAppealAt[operator] = uint64(block.timestamp);
            ICapacityBondSlashEscrow(address(capacityBond)).settleAppealGranted(slashId);
            if (bondRefund != 0) IERC20(address(token)).safeTransfer(appellant, bondRefund);
            emit AppealLapsed(slashId, 2);
            return;
        }
        revert AppealNotOpen(slashId);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    function getAppeal(uint256 slashId) external view returns (Appeal memory) {
        return _appeals[slashId];
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setAppealBond(uint256 newBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceAppealBondBounds(newBond);
        uint256 old = appealBond;
        appealBond = newBond;
        emit AppealBondUpdated(old, newBond);
    }

    function setChallengerIncentivePool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        if (newPool == address(0)) revert ZeroAddress();
        address old = challengerIncentivePool;
        challengerIncentivePool = newPool;
        emit ChallengerIncentivePoolUpdated(old, newPool);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    /// @dev Stamp the pause start so `_unpause` can credit the duration to the
    ///      combined `CapacityBond.pausedTotal` (ADR 028 §5 window extension).
    function _pause() internal override {
        // forge-lint: disable-next-line(block-timestamp)
        _pausedAt = uint64(block.timestamp);
        super._pause();
    }

    /// @dev Credit the just-ended pause interval to the combined counter on
    ///      `CapacityBond` so the slash *filing* window (enforced there) extends
    ///      by the time appeals could not be filed, not just the local windows.
    function _unpause() internal override {
        // forge-lint: disable-next-line(block-timestamp)
        uint64 delta = uint64(block.timestamp) - _pausedAt;
        _pausedAt = 0;
        super._unpause();
        if (delta != 0) ICapacityBondSlashEscrow(address(capacityBond)).creditPauseTime(delta);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    function _enforceAppealBondBounds(uint256 value) internal pure {
        if (value < APPEAL_BOND_FLOOR || value > APPEAL_BOND_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: APPEAL_BOND_FLOOR, ceiling: APPEAL_BOND_CEILING });
        }
    }
}
