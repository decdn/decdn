// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { ISafetyReserve } from "./interfaces/ISafetyReserve.sol";

/// @title SafetyReserve
/// @notice The 5% FeeRouter bucket + 30% slash-redirect custodian. Holds USDC
///         (5% router bucket + post-swap proceeds) and TOKEN (slashed redirect
///         awaiting keeper swap). Exposes the canonical payout surface
///         (ADR 033), the slash-inflow accountant (ADR 026 § Slashing and
///         burn), the Balancer V3 80/20 swap path (ADR 018), and the full
///         slash-appeal state machine (ADR 028 § Contract surface, ADR 032).
/// @dev    The slash-appeal flow consumes `ICapacityBond.clearSlashedAtEpoch`
///         on the `reverseAppeal` path; the post-deployment role grant for
///         `APPEAL_REVERSAL_ROLE` on `CapacityBond` is required for that path
///         to function (ADR 016 § Post-Deployment Initialization, step 6).
///
///         Simplifications vs. ADR 028 carried for this revision:
///           - `MAX_APPEAL_RESTITUTION` is a governance-set USDC ceiling
///             (no TWAP-from-pool oracle dependency in this revision).
///           - `PendingClaim` queue is simple FIFO across all reasons (no
///             per-reason rotation).
///           - `reverseAppeal` routes the 50% non-burn share of the bond to
///             a single `challengerIncentivePool` address (governance-set)
///             rather than the counter-bundle filer routing in ADR 028.
contract SafetyReserve is ISafetyReserve, AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant EMERGENCY_MULTISIG_ROLE = keccak256("EMERGENCY_MULTISIG_ROLE");
    bytes32 public constant SLASH_INFLOW_REPORTER_ROLE = keccak256("SLASH_INFLOW_REPORTER_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------

    uint256 internal constant APPEAL_FILING_WINDOW = 30 days;
    uint256 internal constant APPEAL_REVIEW_WINDOW = 14 days;
    uint256 internal constant APPEAL_RATIFICATION_WINDOW = 14 days;
    uint256 internal constant APPEAL_FREQUENCY_WINDOW = 365 days;

    uint256 internal constant APPEAL_BOND_FLOOR = 100e18;
    uint256 internal constant APPEAL_BOND_CEILING = 10_000e18;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    /// @dev TOKEN held for the 30% slash redirect; ERC20Burnable typing lets
    ///      `rejectAppeal` and `reverseAppeal` burn bond legs directly.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBond public immutable capacityBond;

    // -----------------------------------------------------------------
    // Governance-mutable wiring
    // -----------------------------------------------------------------

    address public balancerPool;
    address public challengerIncentivePool;

    uint256 public appealBond;
    uint256 public maxAppealRestitution;

    // -----------------------------------------------------------------
    // Incidents (immutable append-only ledger)
    // -----------------------------------------------------------------

    Incident[] internal _incidents;

    // -----------------------------------------------------------------
    // Pending-claim queue
    // -----------------------------------------------------------------

    struct PendingClaim {
        bytes32 bundle;
        address recipient;
        uint256 usdcAmount;
        uint64 accrualEpoch;
    }

    PendingClaim[] internal _pendingQueue;
    uint256 internal _pendingHead;

    /// @notice Slash inflow attribution — used by appeal-pinning per ADR 028.
    mapping(address operator => uint256 totalReceived) public slashInflowOf;

    // -----------------------------------------------------------------
    // Appeals (ADR 028 § Contract surface)
    // -----------------------------------------------------------------

    struct Appeal {
        uint256 slashId;
        address operator;
        address appellant;
        bytes32 evidenceBundleHash;
        uint256 bond;
        uint256 escrowAmount;
        uint64 openedAt;
        uint64 fastTrackedAt;
        AppealStatus status;
    }

    Appeal[] internal _appeals;

    /// @notice Last successful appeal timestamp per operator
    ///         (frequency cap per ADR 028 — 1 successful appeal per 365 days).
    mapping(address operator => uint64) public lastAcceptedAppealAt;

    /// @notice Sum of provisional USDC escrow lien for all currently
    ///         fast-tracked appeals. Subtracted from `availableUsdc()` so
    ///         core `payout` flows cannot drain reserve out from under a
    ///         pending ratification.
    uint256 public totalEscrowLien;

    /// @notice One-shot guard against multiple appeals for the same slashId.
    ///         Set on the first successful `openSlashAppeal`; never cleared.
    ///         Closes the duplicate-appeal reserve-drain vector: without
    ///         this guard, an attacker could open + fast-track + ratify
    ///         multiple appeals for one slash event and pull
    ///         `maxAppealRestitution` per ratification.
    mapping(uint256 slashId => bool) public slashAppealed;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Paid(uint256 indexed incidentId, bytes32 bundle, address indexed recipient, uint256 usdcAmount);
    event PendingClaimQueued(uint256 indexed claimId, bytes32 bundle, address indexed recipient, uint256 usdcAmount);
    event PendingClaimDisbursed(uint256 indexed incidentId, uint256 indexed claimId);
    /// @notice Emitted from `disbursePending` on a no-op call so keepers can
    ///         distinguish "queue empty" (`reason=0`) from "balance insufficient"
    ///         (`reason=1`).
    event DisburseSkipped(uint8 reason);
    event SlashInflowRecorded(address indexed operator, uint256 amount);
    /// @notice Emitted by `swapAccumulatedTokens` immediately before the
    ///         `SwapNotImplemented` revert so solc classifies the function
    ///         as state-mutating (it would otherwise warn "can be view"). The
    ///         emit is logically rolled back by the revert, so this event is
    ///         not actually observable on-chain in this revision — downstream
    ///         observers should react to the `SwapNotImplemented` error.
    event SwapAttempted(address indexed keeper, uint256 amountIn, uint256 minOut);

    event SlashAppealOpened(
        uint256 indexed appealId,
        uint256 indexed slashId,
        address indexed operator,
        address appellant,
        bytes32 evidenceBundleHash,
        uint256 bond
    );
    event SlashAppealFastTracked(uint256 indexed appealId, uint256 escrowAmount);
    event SlashAppealRejected(uint256 indexed appealId, uint256 bondBurned);
    event SlashAppealRatified(
        uint256 indexed appealId, uint256 incidentId, address recipient, uint256 restitutionAmount
    );
    event SlashAppealReversed(uint256 indexed appealId, uint256 bondBurned, uint256 toChallenger);
    event SlashAppealLapsed(uint256 indexed appealId, uint8 reason);

    event ParameterUpdated(bytes32 indexed key, uint256 oldValue, uint256 newValue);
    event AddressParameterUpdated(bytes32 indexed key, address oldValue, address newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error UnknownSlash(uint256 slashId);
    error FilingWindowClosed(uint64 slashedAt);
    error FrequencyCapHit(uint64 nextAvailableAt);
    error SlashAlreadyAppealed(uint256 slashId);
    error AppealNotOpen(uint256 appealId);
    error AppealNotFastTracked(uint256 appealId);
    error ReviewWindowOpen(uint64 readyAt);
    error RatificationWindowOpen(uint64 readyAt);
    error InsufficientReserve(uint256 available, uint256 requested);
    error RestitutionExceedsCap(uint256 requested, uint256 cap);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error PoolNotWired();
    error ChallengerPoolNotWired();
    error SwapNotImplemented();

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_                  USDC token.
    /// @param token_                 TOKEN (must be `ERC20Burnable`).
    /// @param capacityBond_          `CapacityBond` (read for clearSlashedAtEpoch).
    /// @param admin                  Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE`.
    /// @param emergencyMultisig      Initial `EMERGENCY_MULTISIG_ROLE`.
    /// @param appealBond_            Initial appeal bond (default 1000 TOKEN;
    ///                               bounded `[100, 10_000]`).
    /// @param maxAppealRestitution_  Initial USDC restitution cap.
    constructor(
        IERC20 usdc_,
        ERC20Burnable token_,
        ICapacityBond capacityBond_,
        address admin,
        address emergencyMultisig,
        uint256 appealBond_,
        uint256 maxAppealRestitution_
    ) {
        if (
            address(usdc_) == address(0) || address(token_) == address(0) || address(capacityBond_) == address(0)
                || admin == address(0)
        ) {
            revert ZeroAddress();
        }
        _enforceAppealBondBounds(appealBond_);

        usdc = usdc_;
        token = token_;
        capacityBond = capacityBond_;
        appealBond = appealBond_;
        maxAppealRestitution = maxAppealRestitution_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        if (emergencyMultisig != address(0)) {
            _grantRole(EMERGENCY_MULTISIG_ROLE, emergencyMultisig);
        }
    }

    // -----------------------------------------------------------------
    // Core payout (ADR 033)
    // -----------------------------------------------------------------

    /// @inheritdoc ISafetyReserve
    function payout(bytes32 bundle, address recipient, uint256 usdcAmount)
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(GOVERNANCE_ROLE)
        returns (uint256 incidentId, bool queued, uint256 claimId)
    {
        if (recipient == address(0)) revert ZeroAddress();
        if (usdcAmount == 0) revert ZeroAmount();
        if (availableUsdc() < usdcAmount) {
            // Insufficient liquidity — queue rather than revert. Permissionless
            // `disbursePending` drains insertion-FIFO when balance refills.
            claimId = _pendingQueue.length;
            _pendingQueue.push(
                PendingClaim({
                    bundle: bundle,
                    recipient: recipient,
                    usdcAmount: usdcAmount,
                    accrualEpoch: uint64(block.timestamp / 1 weeks)
                })
            );
            emit PendingClaimQueued(claimId, bundle, recipient, usdcAmount);
            return (0, true, claimId);
        }
        incidentId = _recordIncident(bundle, recipient, usdcAmount, bytes32("payout"));
        usdc.safeTransfer(recipient, usdcAmount);
        emit Paid(incidentId, bundle, recipient, usdcAmount);
        return (incidentId, false, 0);
    }

    /// @inheritdoc ISafetyReserve
    function disbursePending() external override nonReentrant whenNotPaused returns (uint256 incidentId) {
        if (_pendingHead >= _pendingQueue.length) {
            emit DisburseSkipped(0);
            return 0;
        }
        PendingClaim memory head = _pendingQueue[_pendingHead];
        if (availableUsdc() < head.usdcAmount) {
            emit DisburseSkipped(1);
            return 0;
        }
        _pendingHead++;
        incidentId = _recordIncident(head.bundle, head.recipient, head.usdcAmount, bytes32("pending-disburse"));
        usdc.safeTransfer(head.recipient, head.usdcAmount);
        emit PendingClaimDisbursed(incidentId, _pendingHead - 1);
        emit Paid(incidentId, head.bundle, head.recipient, head.usdcAmount);
    }

    /// @notice Head-of-queue inspection for keepers (preview the next claim
    ///         that would disburse).
    /// @return claimId The queue index that would be drained next.
    /// @return required USDC required for the head claim.
    /// @return available Current `availableUsdc()`.
    function nextPendingDisbursable() external view returns (uint256 claimId, uint256 required, uint256 available) {
        if (_pendingHead >= _pendingQueue.length) return (0, 0, availableUsdc());
        PendingClaim memory head = _pendingQueue[_pendingHead];
        return (_pendingHead, head.usdcAmount, availableUsdc());
    }

    /// @inheritdoc ISafetyReserve
    function incidents(uint256 id) external view override returns (Incident memory) {
        return _incidents[id];
    }

    function incidentCount() external view returns (uint256) {
        return _incidents.length;
    }

    function pendingQueueLength() external view returns (uint256) {
        return _pendingQueue.length - _pendingHead;
    }

    /// @notice USDC balance available for new `payout` calls — total balance
    ///         minus the provisional escrow lien for fast-tracked appeals.
    function availableUsdc() public view returns (uint256) {
        uint256 bal = usdc.balanceOf(address(this));
        if (bal <= totalEscrowLien) return 0;
        return bal - totalEscrowLien;
    }

    // -----------------------------------------------------------------
    // Slash inflow / swap (ADR 026, ADR 018)
    // -----------------------------------------------------------------

    /// @inheritdoc ISafetyReserve
    function recordSlashInflow(address operator, uint256 amount)
        external
        override
        onlyRole(SLASH_INFLOW_REPORTER_ROLE)
    {
        if (operator == address(0)) revert ZeroAddress();
        slashInflowOf[operator] += amount;
        emit SlashInflowRecorded(operator, amount);
    }

    /// @inheritdoc ISafetyReserve
    /// @dev Calls the `_doSwap` virtual hook so subclasses with a live
    ///      Balancer V3 Vault integration can override the swap body
    ///      without rewriting the outer access-control frame.
    ///      `nonReentrant` is intentionally absent: the base `_doSwap`
    ///      always reverts before any external call, so the OZ
    ///      `_nonReentrantAfter` would be unreachable (tripping CI
    ///      `--deny-warnings`). Subclasses that override `_doSwap` with a
    ///      real swap MUST re-add the modifier on `swapAccumulatedTokens`
    ///      in the subclass — not enforced by the type system; production
    ///      override checklist item.
    function swapAccumulatedTokens(uint256 amountIn, uint256 minOut)
        external
        virtual
        override
        whenNotPaused
        onlyRole(KEEPER_ROLE)
    {
        if (balancerPool == address(0)) revert PoolNotWired();
        _doSwap(amountIn, minOut);
    }

    /// @dev Virtual swap hook overridden by production subclasses with the
    ///      live Vault ABI binding. Base reverts so a freshly deployed
    ///      SafetyReserve cannot silently no-op a swap. The `emit` before
    ///      the revert lets solc classify the function as state-mutating
    ///      (avoids the "can be view" warning).
    function _doSwap(uint256 amountIn, uint256 minOut) internal virtual {
        emit SwapAttempted(msg.sender, amountIn, minOut);
        revert SwapNotImplemented();
    }

    // -----------------------------------------------------------------
    // Slash appeals (ADR 028 § Contract surface, ADR 032)
    // -----------------------------------------------------------------

    /// @inheritdoc ISafetyReserve
    /// @dev The `operator` is read from `CapacityBond.slashRecords` rather
    ///      than taken from the caller (I2 fix). This closes the unverified-
    ///      operator hole where an appellant could open an appeal naming any
    ///      operator and force the multisig to verify off-chain.
    ///      `slashRecords` is a view on a trusted, immutable contract; the
    ///      third tuple element (`slashAmount`) is intentionally discarded
    ///      (restitution is capped at `maxAppealRestitution` independently).
    // slither-disable-next-line unused-return
    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash)
        external
        override
        nonReentrant
        whenNotPaused
        returns (uint256 appealId)
    {
        // One appeal per slashId — no duplicate appeals can be filed for
        // the same slash event, even after rejection/lapse of a prior one.
        // Closes the duplicate-appeal reserve-drain vector.
        if (slashAppealed[slashId]) revert SlashAlreadyAppealed(slashId);

        // aderyn-ignore-next-line(reentrancy-state-change)
        (address operator, uint64 slashedAt_,) = capacityBond.slashRecords(slashId);
        if (operator == address(0) || slashedAt_ == 0) revert UnknownSlash(slashId);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > uint256(slashedAt_) + APPEAL_FILING_WINDOW) revert FilingWindowClosed(slashedAt_);
        uint64 last = lastAcceptedAppealAt[operator];
        if (last != 0) {
            uint64 nextAvailable = last + uint64(APPEAL_FREQUENCY_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < nextAvailable) revert FrequencyCapHit(nextAvailable);
        }

        // CEI: write the appeal record before the bond pull so the appeal
        // state is final before any external token call (aderyn H-1).
        slashAppealed[slashId] = true;
        appealId = _appeals.length;
        uint256 bondToPull = appealBond;
        _appeals.push(
            Appeal({
                slashId: slashId,
                operator: operator,
                appellant: msg.sender,
                evidenceBundleHash: evidenceBundleHash,
                bond: bondToPull,
                escrowAmount: 0,
                openedAt: uint64(block.timestamp),
                fastTrackedAt: 0,
                status: AppealStatus.Open
            })
        );

        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), bondToPull);
        emit SlashAppealOpened(appealId, slashId, operator, msg.sender, evidenceBundleHash, bondToPull);
    }

    /// @inheritdoc ISafetyReserve
    function fastTrackAppeal(uint256 appealId) external override nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        Appeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open) revert AppealNotOpen(appealId);

        // Provisional restitution escrow at the cap. Solvency invariant
        // enforced atomically: `availableUsdc()` must cover the new lien.
        uint256 escrow = maxAppealRestitution;
        if (availableUsdc() < escrow) revert InsufficientReserve(availableUsdc(), escrow);
        totalEscrowLien += escrow;

        a.escrowAmount = escrow;
        a.fastTrackedAt = uint64(block.timestamp);
        a.status = AppealStatus.FastTracked;

        emit SlashAppealFastTracked(appealId, escrow);
    }

    /// @inheritdoc ISafetyReserve
    function rejectAppeal(uint256 appealId) external override nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        Appeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open && a.status != AppealStatus.FastTracked) revert AppealNotOpen(appealId);

        if (a.status == AppealStatus.FastTracked) {
            totalEscrowLien -= a.escrowAmount;
        }
        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.escrowAmount = 0;
        a.status = AppealStatus.Rejected;

        token.burn(bondBurned);
        emit SlashAppealRejected(appealId, bondBurned);
    }

    /// @inheritdoc ISafetyReserve
    function ratifyAppeal(uint256 appealId) external override nonReentrant onlyRole(GOVERNANCE_ROLE) {
        Appeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(appealId);
        if (a.escrowAmount > maxAppealRestitution) {
            revert RestitutionExceedsCap(a.escrowAmount, maxAppealRestitution);
        }

        uint256 restitution = a.escrowAmount;
        uint256 bondRefund = a.bond;
        address appellant = a.appellant;
        address operator = a.operator;

        // Release escrow lien BEFORE the USDC transfer so `availableUsdc`
        // reads correctly post-payout.
        totalEscrowLien -= restitution;
        a.bond = 0;
        a.escrowAmount = 0;
        a.status = AppealStatus.Ratified;

        lastAcceptedAppealAt[operator] = uint64(block.timestamp);

        // Restitution to the slashed operator via the incidents ledger.
        uint256 incidentId =
            _recordIncident(a.evidenceBundleHash, operator, restitution, bytes32("slash-appeal-ratify"));
        usdc.safeTransfer(operator, restitution);
        IERC20(address(token)).safeTransfer(appellant, bondRefund);

        emit SlashAppealRatified(appealId, incidentId, operator, restitution);
        emit Paid(incidentId, a.evidenceBundleHash, operator, restitution);
    }

    /// @inheritdoc ISafetyReserve
    function reverseAppeal(uint256 appealId) external override nonReentrant onlyRole(GOVERNANCE_ROLE) {
        Appeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(appealId);
        if (challengerIncentivePool == address(0)) revert ChallengerPoolNotWired();

        uint256 escrow = a.escrowAmount;
        uint256 bondTotal = a.bond;

        totalEscrowLien -= escrow;
        a.bond = 0;
        a.escrowAmount = 0;
        a.status = AppealStatus.Reversed;

        // The reverse path clears the slash zero-out so the operator's vote
        // weight recovers per ADR 036 § Slashing zero-out.
        capacityBond.clearSlashedAtEpoch(a.operator);

        // 50% bond burn + 50% to challenger-incentive pool.
        uint256 toChallenger = bondTotal / 2;
        uint256 toBurn = bondTotal - toChallenger;
        if (toBurn != 0) token.burn(toBurn);
        if (toChallenger != 0) {
            IERC20(address(token)).safeTransfer(challengerIncentivePool, toChallenger);
        }

        emit SlashAppealReversed(appealId, toBurn, toChallenger);
    }

    /// @inheritdoc ISafetyReserve
    function cleanupExpiredAppeal(uint256 appealId) external override nonReentrant {
        Appeal storage a = _appeals[appealId];
        // Branch 1: Open at multisig, review window elapsed.
        if (a.status == AppealStatus.Open) {
            uint64 readyAt = a.openedAt + uint64(APPEAL_REVIEW_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert ReviewWindowOpen(readyAt);
            uint256 bondBurned = a.bond;
            a.bond = 0;
            a.status = AppealStatus.Lapsed;
            if (bondBurned != 0) token.burn(bondBurned);
            emit SlashAppealLapsed(appealId, 1);
            return;
        }
        // Branch 2: FastTracked at Governor, ratification window elapsed.
        if (a.status == AppealStatus.FastTracked) {
            uint64 readyAt = a.fastTrackedAt + uint64(APPEAL_RATIFICATION_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert RatificationWindowOpen(readyAt);
            uint256 escrow = a.escrowAmount;
            uint256 bondBurned = a.bond;
            totalEscrowLien -= escrow;
            a.bond = 0;
            a.escrowAmount = 0;
            a.status = AppealStatus.Lapsed;
            if (bondBurned != 0) token.burn(bondBurned);
            emit SlashAppealLapsed(appealId, 2);
            return;
        }
        revert AppealNotOpen(appealId);
    }

    function getAppeal(uint256 appealId) external view returns (Appeal memory) {
        return _appeals[appealId];
    }

    function appealCount() external view returns (uint256) {
        return _appeals.length;
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setBalancerPool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        address old = balancerPool;
        balancerPool = newPool;
        emit AddressParameterUpdated(bytes32("balancerPool"), old, newPool);
    }

    function setChallengerIncentivePool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        address old = challengerIncentivePool;
        challengerIncentivePool = newPool;
        emit AddressParameterUpdated(bytes32("challengerIncentivePool"), old, newPool);
    }

    function setAppealBond(uint256 newBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceAppealBondBounds(newBond);
        uint256 old = appealBond;
        appealBond = newBond;
        emit ParameterUpdated(bytes32("appealBond"), old, newBond);
    }

    function setMaxAppealRestitution(uint256 newCap) external onlyRole(GOVERNANCE_ROLE) {
        uint256 old = maxAppealRestitution;
        maxAppealRestitution = newCap;
        emit ParameterUpdated(bytes32("maxAppealRestitution"), old, newCap);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    function _recordIncident(bytes32 bundle, address recipient, uint256 usdcAmount, bytes32 reason)
        internal
        returns (uint256 incidentId)
    {
        incidentId = _incidents.length;
        _incidents.push(
            Incident({
                bundle: bundle,
                recipient: recipient,
                usdcAmount: usdcAmount,
                paidAt: uint64(block.timestamp),
                paidBy: msg.sender,
                reason: reason
            })
        );
    }

    function _enforceAppealBondBounds(uint256 value) internal pure {
        if (value < APPEAL_BOND_FLOOR || value > APPEAL_BOND_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: APPEAL_BOND_FLOOR, ceiling: APPEAL_BOND_CEILING });
        }
    }
}
