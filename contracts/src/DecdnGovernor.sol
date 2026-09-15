// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Governor } from "@openzeppelin/contracts/governance/Governor.sol";
import { GovernorCountingSimple } from "@openzeppelin/contracts/governance/extensions/GovernorCountingSimple.sol";
import { GovernorTimelockControl } from "@openzeppelin/contracts/governance/extensions/GovernorTimelockControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { IFeeRouter } from "./interfaces/IFeeRouter.sol";

/// @title DecdnGovernor
/// @notice Served-bytes-weighted on-chain governor (ADR 036). Vote weight is
///         derived from `FeeRouter.bytesPerEpoch` summed across epochs, with each
///         epoch's bytes capped at the operator's declared capacity
///         (`CapacityBond.declaredMbpsAtEpoch` × `epochLength` × 125_000), then
///         bounded by the `voteCapBps` share cap and scaled by `age_ramp(firstBondedAt)`.
///         Weight is zeroed if the operator is slashed in-window. Proposals execute
///         through a 48h `TimelockController`.
/// @dev    `VotingEscrow` and `IVotes`/IERC-5805 are intentionally NOT used —
///         voting weight is derived from `FeeRouter` epoch accounting, not
///         from per-account checkpoint structures. See ADR 036 § Formula.
///
///         Vote delegation (ADR 009 / ADR 026, Governor Bravo pattern) is a
///         lightweight registry layered on top: an operator names a `delegatee`
///         (via a direct `delegate` call or an EIP-712 signed `delegateBySig`),
///         and that delegatee may then cast the operator's vote through
///         `castVoteByDelegate`. Only the vote-casting right moves — the bond,
///         NodeId binding, and served-bytes accrual all stay on the operator,
///         and the vote is tallied under (and `hasVoted`-guarded by) the
///         operator's own address, so `_getVotes` and the per-operator cap are
///         untouched. Because the tally is keyed on the operator, an operator's
///         weight can be cast at most once per proposal regardless of who casts
///         or how delegation changes mid-vote, so the delegation relationship
///         needs no snapshotting.
contract DecdnGovernor is Governor, GovernorCountingSimple, GovernorTimelockControl {
    using Checkpoints for Checkpoints.Trace208;
    using SafeCast for uint256;

    // -----------------------------------------------------------------
    // Vote-source wiring (ADR 036)
    // -----------------------------------------------------------------

    IFeeRouter public immutable feeRouter;
    ICapacityBond public immutable capacityBond;

    // -----------------------------------------------------------------
    // Fixed governance schedule (ADR 009 / ADR 026)
    // -----------------------------------------------------------------

    uint256 private constant VOTING_DELAY = 1 days;
    uint256 private constant VOTING_PERIOD = 7 days;
    uint256 private constant QUORUM_NUMERATOR = 4;
    uint256 private constant QUORUM_DENOMINATOR = 100;
    uint256 private constant PROPOSAL_THRESHOLD_NUMERATOR = 1;
    uint256 private constant PROPOSAL_THRESHOLD_DENOMINATOR = 1000;

    // -----------------------------------------------------------------
    // Governable parameters (ADR 036 § Governable parameters with safety bounds)
    // -----------------------------------------------------------------
    //
    // Checkpointed via OZ `Trace208` so a setter change cannot shift vote
    // weights or quorum mid-vote for in-flight proposals (I4 fix). Each
    // setter pushes the new value at `clock()`; `_getVotes`/`quorum`/
    // `proposalThreshold` read at the proposal's snapshot timepoint via
    // `upperLookupRecent`.

    Checkpoints.Trace208 internal _voteCapBpsHistory;
    Checkpoints.Trace208 internal _ageRampMonthsHistory;

    /// @dev Per-operator vote-weight share cap, in bps. Bounded to 1–10%
    ///      (`VOTE_CAP_BPS_FLOOR`–`VOTE_CAP_BPS_CEILING`). The cap launches at
    ///      the 10% ceiling and only ever decreases (see `setVoteCapBps`): a
    ///      higher launch cap keeps quorum reachable while the operator set is
    ///      thin, and every later move decentralizes weight further.
    uint256 internal constant VOTE_CAP_BPS_FLOOR = 100;
    uint256 internal constant VOTE_CAP_BPS_CEILING = 1000;
    uint256 internal constant AGE_RAMP_MONTHS_FLOOR = 1;
    uint256 internal constant AGE_RAMP_MONTHS_CEILING = 24;
    uint256 internal constant SECONDS_PER_MONTH = 30 days;
    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant RAMP_SCALE = 1e18;

    /// @dev Mbps (megabits/s) → bytes/s: `1e6 / 8`. Exact.
    uint256 internal constant BYTES_PER_MBIT_SECOND = 125_000;

    /// @dev Loop bound for the vote window. MUST equal
    ///      `FeeRouter.WINDOW_EPOCHS_CEILING`; `windowEpochsAt` is governance-
    ///      bounded to `[4, 26]`, this is the defensive ceiling on the on-chain
    ///      loop (matches `FeeRouter.bytesInWindow`).
    uint64 internal constant MAX_WINDOW_EPOCHS = 26;

    // -----------------------------------------------------------------
    // Errors / Events
    // -----------------------------------------------------------------

    error ZeroFeeRouter();
    error ZeroCapacityBond();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    /// @notice `setVoteCapBps` rejects any value that does not strictly
    ///         decrease the current cap. The cap is decrease-only.
    error VoteCapNotDecreasing(uint256 newValue, uint256 current);

    event VoteCapBpsUpdated(uint256 oldValue, uint256 newValue);
    event AgeRampMonthsUpdated(uint256 oldValue, uint256 newValue);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    constructor(IFeeRouter feeRouter_, ICapacityBond capacityBond_, TimelockController timelock_)
        Governor("DecdnGovernor")
        GovernorTimelockControl(timelock_)
    {
        if (address(feeRouter_) == address(0)) revert ZeroFeeRouter();
        if (address(capacityBond_) == address(0)) revert ZeroCapacityBond();
        feeRouter = feeRouter_;
        capacityBond = capacityBond_;
        // Seed the checkpoints at clock()=now so any read at a timepoint
        // ≥ deploy time returns the initial values. `Trace208.push` returns
        // (prevValue, newValue); we discard both — initial seeding has no
        // prior value worth recording.
        // slither-disable-start unused-return
        _voteCapBpsHistory.push(clock(), VOTE_CAP_BPS_CEILING.toUint208());
        _ageRampMonthsHistory.push(clock(), 6);
        // slither-disable-end unused-return
    }

    // -----------------------------------------------------------------
    // Governable-parameter getters (current value + snapshot reads)
    // -----------------------------------------------------------------

    function voteCapBps() public view returns (uint256) {
        return _voteCapBpsHistory.latest();
    }

    function voteCapBpsAt(uint48 timepoint) public view returns (uint256) {
        return _voteCapBpsHistory.upperLookupRecent(timepoint);
    }

    function ageRampMonths() public view returns (uint256) {
        return _ageRampMonthsHistory.latest();
    }

    function ageRampMonthsAt(uint48 timepoint) public view returns (uint256) {
        return _ageRampMonthsHistory.upperLookupRecent(timepoint);
    }

    // -----------------------------------------------------------------
    // Clock — ERC-6372 timestamp mode (aligned with FeeRouter epochs)
    // -----------------------------------------------------------------

    function clock() public view override returns (uint48) {
        return Time.timestamp();
    }

    // solhint-disable-next-line func-name-mixedcase
    function CLOCK_MODE() public pure override returns (string memory) {
        return "mode=timestamp";
    }

    // -----------------------------------------------------------------
    // Fixed voting schedule (ADR 009)
    // -----------------------------------------------------------------

    function votingDelay() public pure override returns (uint256) {
        return VOTING_DELAY;
    }

    function votingPeriod() public pure override returns (uint256) {
        return VOTING_PERIOD;
    }

    // -----------------------------------------------------------------
    // Vote source + quorum/threshold (ADR 036 § Formula)
    // -----------------------------------------------------------------

    /// @notice Quorum = 4% of `totalBytesInWindow(endEpoch, windowEpochs)`.
    ///         The denominator is the unramped, uncapped total — a conservative
    ///         upper bound on the true Σ vote weight; documented in
    ///         ADR 036 § Behaviors that follow from the formula. The window ends
    ///         at the last fully-elapsed epoch (`_endEpoch`, #847) so the read is
    ///         immutable at the snapshot timepoint.
    function quorum(uint256 timepoint) public view override returns (uint256) {
        (uint64 endEpoch, bool hasElapsed) = _endEpoch(timepoint);
        if (!hasElapsed) return 0;
        uint64 n = feeRouter.windowEpochsAt(timepoint.toUint48());
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        return (total * QUORUM_NUMERATOR) / QUORUM_DENOMINATOR;
    }

    /// @notice Proposal threshold = 0.1% of `totalBytesInWindow` at `clock() - 1`,
    ///         consistent with the OZ Governor proposer-weight snapshot. Counts
    ///         only fully-elapsed epochs (`_endEpoch`, #847).
    function proposalThreshold() public view override returns (uint256) {
        uint256 snapshot = uint256(clock()) - 1;
        (uint64 endEpoch, bool hasElapsed) = _endEpoch(snapshot);
        if (!hasElapsed) return 0;
        // SafeCast (consistent with the `timepoint.toUint48()` reads in `quorum`
        // / `_cappedServed`). For any reachable timepoint `clock() ≥ 1` (a live
        // chain's `block.timestamp` is never 0), so `snapshot = clock() - 1 ∈
        // [0, uint48.max - 1]` and the cast never reverts; the checked cast is
        // the deliberate backstop if that ever fails (and keeps aderyn's
        // unsafe-cast detector satisfied / the downcast intent explicit).
        uint64 n = feeRouter.windowEpochsAt(snapshot.toUint48());
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        return (total * PROPOSAL_THRESHOLD_NUMERATOR) / PROPOSAL_THRESHOLD_DENOMINATOR;
    }

    /// @dev ADR 036 § Formula. Returns 0 if the operator was slashed inside the
    ///      trailing window; otherwise `min(Σ_e min(bytesPerEpoch(e),
    ///      declaredMbpsAtEpoch(e) × epochLength × 125_000), voteCapBps × total
    ///      / 10_000) × age_ramp / 1e18`.
    function _getVotes(
        address account,
        uint256 timepoint,
        bytes memory /*params*/
    )
        internal
        view
        override
        returns (uint256)
    {
        if (_slashedInWindow(account, timepoint)) return 0;
        uint256 capped = _cappedServed(account, timepoint);
        if (capped == 0) return 0;
        uint256 ramp =
            _ageRampScaled(capacityBond.firstBondedAt(account), timepoint, ageRampMonthsAt(timepoint.toUint48()));
        return (capped * ramp) / RAMP_SCALE;
    }

    function _slashedInWindow(address account, uint256 timepoint) internal view returns (bool) {
        uint64 slashStamp = capacityBond.slashedAtEpoch(account);
        if (slashStamp == 0) return false;
        (uint64 endEpoch, bool hasElapsed) = _endEpoch(timepoint);
        // No fully-elapsed epoch yet ⇒ the byte window is empty, so there is
        // nothing to zero. Keeps the slash window aligned with the byte window.
        if (!hasElapsed) return false;
        // `slashedAtEpoch` returns `actualEpoch + 1` (or 0 if unslashed) so
        // an epoch-0 slash isn't collapsed with the unslashed sentinel.
        uint64 slashed = slashStamp - 1;
        uint64 n = feeRouter.windowEpochsAt(timepoint.toUint48());
        // Clamp identically to `_cappedServed` and `FeeRouter.bytesInWindow` so
        // the slash window can never span more epochs than the byte window it
        // must track, even if `windowEpochsAt` returns an out-of-bound value.
        if (n > MAX_WINDOW_EPOCHS) n = MAX_WINDOW_EPOCHS;
        uint64 windowStart = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        // Upper-bound the slash epoch at `endEpoch` (the last fully-elapsed
        // epoch). A slash that happened AFTER the snapshot timepoint — or in the
        // in-progress epoch — must NOT retroactively zero historical votes for
        // that proposal (#847: the slash window tracks the byte window).
        return slashed >= windowStart && slashed <= endEpoch;
    }

    function _cappedServed(address account, uint256 timepoint) internal view returns (uint256) {
        (uint64 endEpoch, bool hasElapsed) = _endEpoch(timepoint);
        if (!hasElapsed) return 0;
        uint64 n = feeRouter.windowEpochsAt(timepoint.toUint48());
        if (n == 0) return 0;
        if (n > MAX_WINDOW_EPOCHS) n = MAX_WINDOW_EPOCHS;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        uint256 epochSeconds = feeRouter.epochLength();

        uint256 served = 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            uint256 epochBytes = feeRouter.bytesPerEpoch(account, e);
            // Max bytes the tier declared at epoch `e`'s close could deliver.
            // Declared capacity caps delivery-based weight; it never grants it.
            uint256 epochCap = capacityBond.declaredMbpsAtEpoch(account, e) * epochSeconds * BYTES_PER_MBIT_SECOND;
            served += epochBytes < epochCap ? epochBytes : epochCap;
        }

        // Per-operator share cap (ADR 036) reads at the proposal snapshot, not
        // live, so a setter change mid-vote does not shift weights.
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        uint256 capBps = voteCapBpsAt(timepoint.toUint48());
        uint256 cap = (total * capBps) / BPS_DENOMINATOR;
        return served < cap ? served : cap;
    }

    /// @dev Last fully-elapsed epoch as of `timepoint`. Returns
    ///      `hasElapsed = false` when no epoch has completed yet
    ///      (`timepoint < epochLength`), so callers short-circuit to 0 / `false`
    ///      instead of underflowing `uint64`. Counting only elapsed epochs makes
    ///      the served-byte read immutable at the proposal snapshot, since
    ///      `FeeRouter.routeSettlement` only ever writes the current epoch's
    ///      bucket and never mutates an elapsed one (#847).
    function _endEpoch(uint256 timepoint) private view returns (uint64 endEpoch, bool hasElapsed) {
        uint64 cur = uint64(timepoint / feeRouter.epochLength());
        if (cur == 0) return (0, false);
        return (cur - 1, true);
    }

    /// @dev Linear ramp from 0 → 1e18 over `rampMonths × SECONDS_PER_MONTH`
    ///      since `firstBondedAt`. Returns 1e18 (full weight) once tenure
    ///      exceeds the ramp horizon; 0 if `firstBondedAt == 0` (never bonded).
    function _ageRampScaled(uint64 firstBondedAt, uint256 timepoint, uint256 rampMonths)
        internal
        pure
        returns (uint256)
    {
        if (firstBondedAt == 0) return 0;
        if (timepoint <= uint256(firstBondedAt)) return 0;
        uint256 elapsed = timepoint - uint256(firstBondedAt);
        uint256 horizon = rampMonths * SECONDS_PER_MONTH;
        if (elapsed >= horizon) return RAMP_SCALE;
        return (elapsed * RAMP_SCALE) / horizon;
    }

    // -----------------------------------------------------------------
    // Governance-tunable parameters (carry timelock per OZ default flow)
    // -----------------------------------------------------------------

    /// @notice Lower the per-operator vote cap in bps. Must be called through
    ///         the timelock (the governor's executor is itself). The cap is
    ///         decrease-only: it launches at the 10% ceiling and governance can
    ///         move it only down, to the 1% floor. Each move decentralizes vote
    ///         weight; the cap never re-concentrates it. `newValue` must be at
    ///         least `VOTE_CAP_BPS_FLOOR` and strictly below the current cap;
    ///         any increase or no-op reverts. The new value is checkpointed at
    ///         `clock()`; in-flight proposals whose snapshot timepoint is
    ///         earlier read the prior value.
    function setVoteCapBps(uint256 newValue) external onlyGovernance {
        if (newValue < VOTE_CAP_BPS_FLOOR) {
            revert ParamOutOfBounds({ value: newValue, floor: VOTE_CAP_BPS_FLOOR, ceiling: VOTE_CAP_BPS_CEILING });
        }
        uint256 old = voteCapBps();
        if (newValue >= old) revert VoteCapNotDecreasing(newValue, old);
        // slither-disable-next-line unused-return
        _voteCapBpsHistory.push(clock(), newValue.toUint208());
        emit VoteCapBpsUpdated(old, newValue);
    }

    function setAgeRampMonths(uint256 newValue) external onlyGovernance {
        if (newValue < AGE_RAMP_MONTHS_FLOOR || newValue > AGE_RAMP_MONTHS_CEILING) {
            // Positional args here keep this within solhint's 120-char limit
            // and forge-fmt's single-line preference (named args overflow by 1).
            revert ParamOutOfBounds(newValue, AGE_RAMP_MONTHS_FLOOR, AGE_RAMP_MONTHS_CEILING);
        }
        uint256 old = ageRampMonths();
        // slither-disable-next-line unused-return
        _ageRampMonthsHistory.push(clock(), newValue.toUint208());
        emit AgeRampMonthsUpdated(old, newValue);
    }

    // -----------------------------------------------------------------
    // Vote delegation (ADR 009 / ADR 026 — Governor Bravo pattern)
    // -----------------------------------------------------------------
    //
    // A minimal delegation registry: an operator (the delegator) authorises a
    // `delegatee` to cast the operator's vote. The served-bytes weight, the
    // bond, and the NodeId binding all stay on the operator; `_getVotes` is
    // never consulted for the delegatee's own address on the operator's behalf.
    // Instead the delegatee calls `castVoteByDelegate`, which routes through
    // OZ's `_castVote(proposalId, operator, …)` so the weight, the `VoteCast`
    // event, and the `hasVoted` flag are all attributed to the operator. That
    // single `hasVoted[proposalId][operator]` guard makes each operator's
    // weight cast at most once per proposal — whoever casts first (the operator
    // themselves or their current delegatee) wins, and re-delegating mid-vote
    // cannot double-count — so the delegation link is read live and needs no
    // per-timepoint checkpoint.

    /// @dev EIP-712 type hash for a signed delegation. `delegator` is carried
    ///      explicitly (rather than recovered) so contract wallets can delegate
    ///      via EIP-1271, mirroring OZ's `castVoteBySig(…, voter, signature)`.
    bytes32 public constant DELEGATION_TYPEHASH =
        keccak256("Delegation(address delegator,address delegatee,uint256 nonce,uint256 expiry)");

    /// @dev operator ⇒ the address currently authorised to cast its vote.
    ///      `address(0)` means no delegation (only the operator can vote).
    mapping(address => address) private _delegatee;

    /// @dev Replay-protection nonce for `delegateBySig`, kept in a dedicated
    ///      namespace so it never collides with the ballot nonces OZ's
    ///      `Nonces` tracks for `castVoteBySig`.
    mapping(address => uint256) private _delegationNonces;

    event DelegateChanged(address indexed delegator, address indexed fromDelegatee, address indexed toDelegatee);

    error NotDelegatee(address delegator, address caller);
    error DelegationSignatureExpired(uint256 expiry);
    error InvalidDelegationSignature(address delegator);
    error InvalidDelegationNonce(address delegator, uint256 expected, uint256 provided);

    /// @notice The address currently authorised to cast `operator`'s vote, or
    ///         `address(0)` if the operator has not delegated.
    function delegates(address operator) external view returns (address) {
        return _delegatee[operator];
    }

    /// @notice Next unused `delegateBySig` nonce for `operator`.
    function delegationNonces(address operator) external view returns (uint256) {
        return _delegationNonces[operator];
    }

    /// @notice Delegate the caller's vote-casting right to `delegatee`. Pass
    ///         `address(0)` to revoke. Overwrites any prior delegation.
    function delegate(address delegatee) external {
        _delegate(_msgSender(), delegatee);
    }

    /// @notice Delegate `delegator`'s vote-casting right to `delegatee` from an
    ///         off-chain EIP-712 signature, so a relayer can submit it. The
    ///         `expiry` bounds when the signature may be redeemed; once
    ///         redeemed the delegation stands until changed. `nonce` must equal
    ///         the delegator's current `delegationNonces` value and is consumed
    ///         on success (single-use, replay-proof). Supports EIP-1271
    ///         contract signers via `SignatureChecker`.
    function delegateBySig(
        address delegator,
        address delegatee,
        uint256 nonce,
        uint256 expiry,
        bytes calldata signature
    ) external {
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > expiry) revert DelegationSignatureExpired(expiry);
        // Validate the (public) nonce before the signature so a bad/stale nonce
        // fails fast, without paying for the potential EIP-1271 staticcall in
        // `SignatureChecker` — closes a cheap griefing vector.
        uint256 expected = _delegationNonces[delegator];
        if (nonce != expected) revert InvalidDelegationNonce(delegator, expected, nonce);
        bytes32 digest =
            _hashTypedDataV4(keccak256(abi.encode(DELEGATION_TYPEHASH, delegator, delegatee, nonce, expiry)));
        if (!SignatureChecker.isValidSignatureNow(delegator, digest, signature)) {
            revert InvalidDelegationSignature(delegator);
        }
        _delegationNonces[delegator] = expected + 1;
        _delegate(delegator, delegatee);
    }

    /// @notice Cast `delegator`'s vote. Caller must be `delegator`'s current
    ///         delegatee. The vote (weight, `VoteCast` event, `hasVoted`) is
    ///         attributed to `delegator`, not the caller.
    function castVoteByDelegate(uint256 proposalId, address delegator, uint8 support) external returns (uint256) {
        _requireDelegatee(delegator, _msgSender());
        return _castVote(proposalId, delegator, support, "");
    }

    /// @notice `castVoteByDelegate` with a human-readable reason (attributed to
    ///         `delegator` in the emitted `VoteCast`).
    function castVoteByDelegateWithReason(uint256 proposalId, address delegator, uint8 support, string calldata reason)
        external
        returns (uint256)
    {
        _requireDelegatee(delegator, _msgSender());
        return _castVote(proposalId, delegator, support, reason);
    }

    /// @notice Cast the votes of several operators that have all delegated to
    ///         the caller, in one transaction. Reverts wholesale if the caller
    ///         is not the current delegatee of every `delegator` (or if any has
    ///         already voted). The loop is bounded by the caller-supplied array
    ///         and the caller pays its gas, so it carries no griefing surface.
    function castVotesByDelegate(uint256 proposalId, address[] calldata delegators, uint8 support)
        external
        returns (uint256 totalWeight)
    {
        address caller = _msgSender();
        for (uint256 i = 0; i < delegators.length; ++i) {
            _requireDelegatee(delegators[i], caller);
            totalWeight += _castVote(proposalId, delegators[i], support, "");
        }
    }

    function _delegate(address delegator, address delegatee) private {
        address from = _delegatee[delegator];
        _delegatee[delegator] = delegatee;
        emit DelegateChanged(delegator, from, delegatee);
    }

    function _requireDelegatee(address delegator, address caller) private view {
        if (_delegatee[delegator] != caller) revert NotDelegatee(delegator, caller);
    }

    // -----------------------------------------------------------------
    // Timelock plumbing — resolve Governor / GovernorTimelockControl
    // -----------------------------------------------------------------

    function state(uint256 proposalId) public view override(Governor, GovernorTimelockControl) returns (ProposalState) {
        return super.state(proposalId);
    }

    function proposalNeedsQueuing(uint256 proposalId)
        public
        view
        override(Governor, GovernorTimelockControl)
        returns (bool)
    {
        return super.proposalNeedsQueuing(proposalId);
    }

    function _queueOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) returns (uint48) {
        return super._queueOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _executeOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) {
        super._executeOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _cancel(
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) returns (uint256) {
        return super._cancel(targets, values, calldatas, descriptionHash);
    }

    function _executor() internal view override(Governor, GovernorTimelockControl) returns (address) {
        return super._executor();
    }
}
