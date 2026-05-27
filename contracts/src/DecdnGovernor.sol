// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Governor } from "@openzeppelin/contracts/governance/Governor.sol";
import { GovernorCountingSimple } from "@openzeppelin/contracts/governance/extensions/GovernorCountingSimple.sol";
import { GovernorTimelockControl } from "@openzeppelin/contracts/governance/extensions/GovernorTimelockControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { IFeeRouter } from "./interfaces/IFeeRouter.sol";

/// @title DecdnGovernor
/// @notice Served-bytes-weighted on-chain governor (ADR 036). Vote weight is
///         derived from `FeeRouter.bytesInWindow` × `age_ramp(firstBondedAt)`
///         with a per-operator cap and a slash zero-out from
///         `CapacityBond.slashedAtEpoch`. Proposals execute through a 48h
///         `TimelockController`.
/// @dev    `VotingEscrow` and `IVotes`/IERC-5805 are intentionally NOT used —
///         voting weight is derived from `FeeRouter` epoch accounting, not
///         from per-account checkpoint structures. See ADR 036 § Formula.
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

    uint256 internal constant VOTE_CAP_BPS_FLOOR = 100;
    uint256 internal constant VOTE_CAP_BPS_CEILING = 2500;
    uint256 internal constant AGE_RAMP_MONTHS_FLOOR = 1;
    uint256 internal constant AGE_RAMP_MONTHS_CEILING = 24;
    uint256 internal constant SECONDS_PER_MONTH = 30 days;
    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant RAMP_SCALE = 1e18;

    // -----------------------------------------------------------------
    // Errors / Events
    // -----------------------------------------------------------------

    error ZeroFeeRouter();
    error ZeroCapacityBond();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

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
        _voteCapBpsHistory.push(clock(), 500);
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

    /// @notice Quorum = 4% of `totalBytesInWindow(epoch(t), windowEpochs)`.
    ///         The denominator is the unramped, uncapped total — a conservative
    ///         upper bound on the true Σ vote weight; documented in
    ///         ADR 036 § Behaviors that follow from the formula.
    function quorum(uint256 timepoint) public view override returns (uint256) {
        uint64 endEpoch = uint64(timepoint / feeRouter.epochLength());
        uint64 n = feeRouter.windowEpochs();
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        return (total * QUORUM_NUMERATOR) / QUORUM_DENOMINATOR;
    }

    /// @notice Proposal threshold = 0.1% of `totalBytesInWindow` at `clock() - 1`,
    ///         consistent with the OZ Governor proposer-weight snapshot.
    function proposalThreshold() public view override returns (uint256) {
        uint256 snapshot = uint256(clock()) - 1;
        uint64 endEpoch = uint64(snapshot / feeRouter.epochLength());
        uint64 n = feeRouter.windowEpochs();
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        return (total * PROPOSAL_THRESHOLD_NUMERATOR) / PROPOSAL_THRESHOLD_DENOMINATOR;
    }

    /// @dev ADR 036 § Formula. Returns 0 if the operator was slashed inside
    ///      the trailing window; otherwise
    ///      `min(served, voteCapBps × total / 10_000) × age_ramp / 1e18`.
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
        uint64 slashed = capacityBond.slashedAtEpoch(account);
        if (slashed == 0) return false;
        uint64 n = feeRouter.windowEpochs();
        uint64 endEpoch = uint64(timepoint / feeRouter.epochLength());
        uint64 windowStart = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        // Upper-bound the slash epoch at `endEpoch`. A slash that happened
        // AFTER the snapshot timepoint (e.g., between an old proposal's
        // snapshot and "now") must NOT retroactively zero historical votes
        // for that proposal.
        return slashed >= windowStart && slashed <= endEpoch;
    }

    function _cappedServed(address account, uint256 timepoint) internal view returns (uint256) {
        uint64 n = feeRouter.windowEpochs();
        uint64 endEpoch = uint64(timepoint / feeRouter.epochLength());
        uint256 served = feeRouter.bytesInWindow(account, endEpoch, n);
        uint256 total = feeRouter.totalBytesInWindow(endEpoch, n);
        // Read the per-operator cap at the proposal snapshot, not live, so
        // a setter change mid-vote does not shift weights (I4).
        uint256 capBps = voteCapBpsAt(timepoint.toUint48());
        uint256 cap = (total * capBps) / BPS_DENOMINATOR;
        return served < cap ? served : cap;
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

    /// @notice Set the per-operator vote cap in bps. Must be called through
    ///         the timelock (the governor's executor is itself). The new
    ///         value is checkpointed at `clock()`; in-flight proposals whose
    ///         snapshot timepoint is earlier read the prior value (I4 fix).
    function setVoteCapBps(uint256 newValue) external onlyGovernance {
        if (newValue < VOTE_CAP_BPS_FLOOR || newValue > VOTE_CAP_BPS_CEILING) {
            revert ParamOutOfBounds({ value: newValue, floor: VOTE_CAP_BPS_FLOOR, ceiling: VOTE_CAP_BPS_CEILING });
        }
        uint256 old = voteCapBps();
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
