// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title VotingEscrow - linear-decay vote-escrowed TOKEN (veCRV-style)
/// @notice Locks TOKEN for up to 4 years in exchange for a voting weight that
///         decays linearly to zero at unlock time. Provides historical
///         checkpoints - `balanceOfAt(account, ts)` and `totalSupplyAt(ts)` -
///         that are load-bearing for the `FeeRouter` gauge epoch-snapshot
///         (ADR 026 § FeeRouter split) and Governor quorum (ADR 026 §
///         Governance). Spec: ADR 034 § Voting escrow.
///
/// @dev    There is no OpenZeppelin base for this model - `ERC20Votes` is
///         block-number checkpointed with no decay, whereas ve-weight decays
///         continuously with time. The checkpoint engine below is a faithful
///         port of Curve's veCRV `_checkpoint` with one deliberate deviation:
///         historical reads are keyed by **timestamp**, not block number
///         (ADR 034 - ve-weight is a function of time, so block<->time
///         interpolation is unnecessary). OZ is still used for the
///         non-novel parts: `ReentrancyGuard`, `Pausable`, `AccessControl`
///         (pause role), `SafeERC20`.
///
///         Hard invariants (ADR 034 § Voting escrow):
///           - One lock per address.
///           - Non-transferable: no ERC20 surface, no transfer / approve.
///           - No early exit: `withdraw` only after `end`.
///           - ve-locked TOKEN is never slashable (it lives here, not in
///             StakingRegistry).
///           - Unlock times are week-aligned (slopes change on week
///             boundaries - the veCRV pattern that bounds the global
///             checkpoint walk).
///
///         Out of this PR: delegation (`delegate` / `delegateBySig` /
///         `delegates`). ADR 034 reads vote weight via `balanceOfAt` (the
///         locker's own decaying balance) yet also says delegation
///         "reassigns voting weight" - those need a `getVotes`-style
///         delegated-weight accessor to compose, which the ADR omits.
///         Deferred pending a spec clarification; the FeeRouter gauge only
///         needs the per-account `balanceOfAt` shipped here.
contract VotingEscrow is AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    /// @notice Pause / unpause authority (emergency multisig).
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    /// @notice Lock boundaries are aligned to whole weeks (veCRV pattern):
    ///         scheduled slope changes only ever land on week boundaries, so
    ///         the global checkpoint walk advances one week at a time and is
    ///         bounded by `maxLockDuration / WEEK` steps.
    uint256 public constant WEEK = 7 days;

    /// @notice Iteration cap on the global checkpoint week-walk (veCRV uses the
    ///         same 255 bound). `maxLockDuration` (4y ~= 208 weeks) is below
    ///         this, so a checkpoint after a long quiet period still terminates;
    ///         the only cost of hitting the cap is a slightly stale global
    ///         history that the next checkpoint repairs.
    uint256 internal constant MAX_CHECKPOINT_ITERATIONS = 255;

    /// @notice TOKEN being escrowed.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable token;

    /// @notice Minimum lock duration (ADR 034 - 1 week).
    uint256 public immutable minLockDuration;

    /// @notice Maximum lock duration (ADR 034 - 4 years). Also the divisor in
    ///         the slope: `slope = amount / maxLockDuration` ve-units/second.
    uint256 public immutable maxLockDuration;

    struct LockedBalance {
        uint256 amount;
        uint256 end;
    }

    /// @notice A decay checkpoint: ve-weight is `bias - slope * (t - ts)` for
    ///         `t >= ts`, clamped at zero. `slope`/`bias` are `int128` because
    ///         the global aggregate applies negative slope-change deltas.
    struct Point {
        int128 bias;
        int128 slope;
        uint256 ts;
    }

    /// @notice Per-account active lock. `(0, 0)` for never-locked / withdrawn.
    mapping(address account => LockedBalance) public locked;

    /// @notice Global decay checkpoints, oldest first. Binary-searched by
    ///         `totalSupplyAt`.
    Point[] public pointHistory;

    /// @notice Per-account decay checkpoints, oldest first. Binary-searched by
    ///         `balanceOfAt`.
    mapping(address account => Point[]) public userPointHistory;

    /// @notice Aggregate slope decrease scheduled at each week boundary (the
    ///         instant a cohort of locks expires). Drives `totalSupplyAt`.
    mapping(uint256 weekTs => int128 slopeDelta) public slopeChanges;

    event LockCreated(address indexed account, uint256 amount, uint256 unlockTime);
    event LockIncreased(address indexed account, uint256 addedAmount, uint256 newAmount);
    event LockExtended(address indexed account, uint256 oldUnlockTime, uint256 newUnlockTime);
    event Withdrawn(address indexed account, uint256 amount);

    error ZeroAddress();
    error ZeroAmount();
    error LockDurationOutOfRange(uint256 requested, uint256 floor, uint256 ceiling);
    error ExistingLock();
    error NoLock();
    error LockExpired();
    error LockNotExpired(uint256 end);
    error UnlockTimeNotInFuture(uint256 unlockTime, uint256 currentEnd);
    error FutureLookup(uint256 timestamp);

    /// @param token_           TOKEN to escrow.
    /// @param admin            Initial `DEFAULT_ADMIN_ROLE` holder (Timelock at
    ///                         mainnet per ADR 016 § Deployment Order).
    /// @param minLockDuration_ Minimum lock (ADR 034 default 1 week).
    /// @param maxLockDuration_ Maximum lock (ADR 034 default 4 years).
    constructor(IERC20 token_, address admin, uint256 minLockDuration_, uint256 maxLockDuration_) {
        if (address(token_) == address(0) || admin == address(0)) revert ZeroAddress();
        if (minLockDuration_ == 0 || minLockDuration_ > maxLockDuration_) {
            revert LockDurationOutOfRange(minLockDuration_, 1, maxLockDuration_);
        }

        token = token_;
        minLockDuration = minLockDuration_;
        maxLockDuration = maxLockDuration_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);

        // Genesis global checkpoint so `totalSupplyAt` always finds a base
        // point at or before any queried timestamp >= deployment.
        pointHistory.push(Point({ bias: 0, slope: 0, ts: block.timestamp }));
    }

    // -----------------------------------------------------------------
    // Lock lifecycle
    // -----------------------------------------------------------------

    // Two intentional patterns run through the implementation below; the
    // lints are disabled for the body and re-enabled at the end of the
    // contract (both forge-lint and slither).
    //
    //  - block-timestamp: VotingEscrow is fundamentally time-based - locks
    //    expire at absolute timestamps and ve-weight decays continuously with
    //    `block.timestamp`. Validator timestamp skew (consensus drift,
    //    seconds) is immaterial against week-aligned boundaries and multi-year
    //    durations.
    //  - divide-before-multiply: the veCRV slope is an integer ve-units/second
    //    (`slope = amount / maxLockDuration`) stored per checkpoint and reused
    //    for incremental decay (`bias -= slope * dt`). Computing `bias` as
    //    `slope * remaining` (rather than `amount * remaining / maxLockDuration`)
    //    is required so bias and slope stay mutually consistent and the weight
    //    decays to exactly zero at `end`. The precision loss is the documented
    //    veCRV behavior.
    //
    // The strict equality / inequality checks on `LockedBalance.amount` and
    // `.end` below (e.g. `existing.amount == 0`) are lock-presence sentinels,
    // not manipulable-balance comparisons — the same false-positive class as
    // StakingRegistry's `== 0` sentinels — so `incorrect-equality` is disabled
    // for the body too.
    //
    // forge-lint: disable-start(block-timestamp)
    // forge-lint: disable-start(divide-before-multiply)
    // slither-disable-start timestamp
    // slither-disable-start divide-before-multiply
    // slither-disable-start incorrect-equality

    /// @notice Lock `amount` TOKEN until `unlockTime` (week-aligned, rounded
    ///         down). One lock per address.
    function createLock(uint256 amount, uint256 unlockTime) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        LockedBalance memory existing = locked[msg.sender];
        if (existing.amount != 0) revert ExistingLock();

        uint256 weekAligned = (unlockTime / WEEK) * WEEK;
        _requireDurationInRange(weekAligned);

        LockedBalance memory newLock = LockedBalance({ amount: amount, end: weekAligned });
        _commitLock(msg.sender, existing, newLock);

        token.safeTransferFrom(msg.sender, address(this), amount);
        emit LockCreated(msg.sender, amount, weekAligned);
    }

    /// @notice Add `amount` to the caller's existing, non-expired lock; the
    ///         unlock time is unchanged.
    function increaseAmount(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        LockedBalance memory existing = locked[msg.sender];
        if (existing.amount == 0) revert NoLock();
        if (existing.end <= block.timestamp) revert LockExpired();

        LockedBalance memory newLock = LockedBalance({ amount: existing.amount + amount, end: existing.end });
        _commitLock(msg.sender, existing, newLock);

        token.safeTransferFrom(msg.sender, address(this), amount);
        emit LockIncreased(msg.sender, amount, newLock.amount);
    }

    /// @notice Extend the caller's lock to a later week-aligned `unlockTime`.
    function increaseUnlockTime(uint256 unlockTime) external nonReentrant whenNotPaused {
        LockedBalance memory existing = locked[msg.sender];
        if (existing.amount == 0) revert NoLock();
        if (existing.end <= block.timestamp) revert LockExpired();

        uint256 weekAligned = (unlockTime / WEEK) * WEEK;
        if (weekAligned <= existing.end) revert UnlockTimeNotInFuture(weekAligned, existing.end);
        _requireDurationInRange(weekAligned);

        LockedBalance memory newLock = LockedBalance({ amount: existing.amount, end: weekAligned });
        _commitLock(msg.sender, existing, newLock);

        emit LockExtended(msg.sender, existing.end, weekAligned);
    }

    /// @notice Withdraw the full locked balance once the lock has expired.
    ///         Lump-sum only; there is no early-exit path.
    function withdraw() external nonReentrant whenNotPaused {
        LockedBalance memory existing = locked[msg.sender];
        if (existing.amount == 0) revert NoLock();
        if (existing.end > block.timestamp) revert LockNotExpired(existing.end);

        uint256 amount = existing.amount;
        LockedBalance memory empty = LockedBalance({ amount: 0, end: 0 });
        _commitLock(msg.sender, existing, empty);

        token.safeTransfer(msg.sender, amount);
        emit Withdrawn(msg.sender, amount);
    }

    // -----------------------------------------------------------------
    // Views - current
    // -----------------------------------------------------------------

    /// @notice Current voting weight of `account`: `amount × remaining / maxLockDuration`.
    function balanceOf(address account) external view returns (uint256) {
        return balanceOfAt(account, block.timestamp);
    }

    /// @notice Voting weight of `account` at `timestamp`. Binary-searches the
    ///         account's checkpoints for the latest point at or before
    ///         `timestamp`, then applies linear decay. Future lookups revert.
    function balanceOfAt(address account, uint256 timestamp) public view returns (uint256) {
        if (timestamp > block.timestamp) revert FutureLookup(timestamp);
        Point[] storage points = userPointHistory[account];
        uint256 len = points.length;
        if (len == 0) return 0;

        uint256 idx = _findPointIndex(points, timestamp);
        // `_findPointIndex` returns len when `timestamp` precedes the first
        // checkpoint - no weight yet.
        if (idx == len) return 0;

        Point storage point = points[idx];
        int128 bias = point.bias - point.slope * _toInt128(timestamp - point.ts);
        if (bias < 0) return 0;
        return _toUint256(bias);
    }

    /// @notice Current total ve-supply (sum of all live `balanceOf`).
    function totalSupply() external view returns (uint256) {
        return totalSupplyAt(block.timestamp);
    }

    /// @notice Total ve-supply at `timestamp`. Binary-searches the global
    ///         checkpoints, then walks week boundaries from that point to
    ///         `timestamp` applying scheduled slope changes (cohort expiries).
    ///         Future lookups revert.
    function totalSupplyAt(uint256 timestamp) public view returns (uint256) {
        if (timestamp > block.timestamp) revert FutureLookup(timestamp);
        // Genesis point guarantees pointHistory is non-empty.
        uint256 idx = _findGlobalPointIndex(timestamp);
        Point memory point = pointHistory[idx];
        return _supplyAt(point, timestamp);
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
    // Internal - lock commit + checkpoint engine
    // -----------------------------------------------------------------

    function _commitLock(address account, LockedBalance memory oldLock, LockedBalance memory newLock) internal {
        locked[account] = newLock;
        _checkpoint(account, oldLock, newLock);
    }

    /// @dev Records the decay checkpoint for both the per-account history and
    ///      the global aggregate, scheduling the slope changes that fire when
    ///      `oldLock.end` / `newLock.end` are reached. Faithful veCRV port.
    function _checkpoint(address account, LockedBalance memory oldLock, LockedBalance memory newLock) internal {
        Point memory uOld;
        Point memory uNew;
        int128 oldDslope = 0;
        int128 newDslope = 0;

        if (oldLock.end > block.timestamp && oldLock.amount > 0) {
            uOld.slope = _toInt128(oldLock.amount / maxLockDuration);
            uOld.bias = uOld.slope * _toInt128(oldLock.end - block.timestamp);
        }
        if (newLock.end > block.timestamp && newLock.amount > 0) {
            uNew.slope = _toInt128(newLock.amount / maxLockDuration);
            uNew.bias = uNew.slope * _toInt128(newLock.end - block.timestamp);
        }

        oldDslope = slopeChanges[oldLock.end];
        if (newLock.end != 0) {
            newDslope = newLock.end == oldLock.end ? oldDslope : slopeChanges[newLock.end];
        }

        Point memory lastPoint = pointHistory[pointHistory.length - 1];
        uint256 lastCheckpoint = lastPoint.ts;

        // Walk week boundaries from the last global checkpoint to now, baking
        // scheduled slope changes into a fresh global point per crossed week.
        uint256 ti = (lastCheckpoint / WEEK) * WEEK;
        for (uint256 i = 0; i < MAX_CHECKPOINT_ITERATIONS; i++) {
            ti += WEEK;
            int128 dSlope = 0;
            if (ti > block.timestamp) {
                ti = block.timestamp;
            } else {
                dSlope = slopeChanges[ti];
            }
            lastPoint.bias -= lastPoint.slope * _toInt128(ti - lastCheckpoint);
            lastPoint.slope += dSlope;
            if (lastPoint.bias < 0) lastPoint.bias = 0;
            if (lastPoint.slope < 0) lastPoint.slope = 0;
            lastCheckpoint = ti;
            lastPoint.ts = ti;
            if (ti == block.timestamp) {
                break;
            }
            pointHistory.push(lastPoint);
        }

        // Fold the account's slope/bias delta into the now-current global point.
        lastPoint.slope += (uNew.slope - uOld.slope);
        lastPoint.bias += (uNew.bias - uOld.bias);
        if (lastPoint.slope < 0) lastPoint.slope = 0;
        if (lastPoint.bias < 0) lastPoint.bias = 0;
        lastPoint.ts = block.timestamp;
        pointHistory.push(lastPoint);

        // Reschedule the slope changes that fire at the lock end(s).
        if (oldLock.end > block.timestamp) {
            // `oldDslope` already included `uOld.slope`'s removal; undo it,
            // then re-apply if the new lock shares the same end.
            oldDslope += uOld.slope;
            if (newLock.end == oldLock.end) {
                oldDslope -= uNew.slope;
            }
            slopeChanges[oldLock.end] = oldDslope;
        }
        if (newLock.end > block.timestamp && newLock.end > oldLock.end) {
            newDslope -= uNew.slope;
            slopeChanges[newLock.end] = newDslope;
        }

        uNew.ts = block.timestamp;
        userPointHistory[account].push(uNew);
    }

    /// @dev Walk `point` forward week-by-week to `t`, applying scheduled slope
    ///      changes, and return the decayed supply at `t`.
    function _supplyAt(Point memory point, uint256 t) internal view returns (uint256) {
        Point memory lastPoint = point;
        uint256 ti = (lastPoint.ts / WEEK) * WEEK;
        for (uint256 i = 0; i < MAX_CHECKPOINT_ITERATIONS; i++) {
            ti += WEEK;
            int128 dSlope = 0;
            if (ti > t) {
                ti = t;
            } else {
                dSlope = slopeChanges[ti];
            }
            lastPoint.bias -= lastPoint.slope * _toInt128(ti - lastPoint.ts);
            if (ti == t) {
                break;
            }
            lastPoint.slope += dSlope;
            lastPoint.ts = ti;
        }
        if (lastPoint.bias < 0) lastPoint.bias = 0;
        return _toUint256(lastPoint.bias);
    }

    // -----------------------------------------------------------------
    // Internal - binary search
    // -----------------------------------------------------------------

    /// @dev Index of the latest point in `points` with `ts <= timestamp`, or
    ///      `points.length` if `timestamp` precedes the first point.
    function _findPointIndex(Point[] storage points, uint256 timestamp) internal view returns (uint256) {
        uint256 lo = 0;
        uint256 hi = points.length;
        if (hi == 0 || points[0].ts > timestamp) return points.length;
        // Invariant: points[lo].ts <= timestamp. Find the largest such index.
        while (lo + 1 < hi) {
            uint256 mid = (lo + hi) / 2;
            if (points[mid].ts <= timestamp) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /// @dev Index into `pointHistory` of the latest global point with
    ///      `ts <= timestamp`. The genesis point guarantees index 0 qualifies
    ///      for any `timestamp >= deployment`.
    function _findGlobalPointIndex(uint256 timestamp) internal view returns (uint256) {
        uint256 lo = 0;
        uint256 hi = pointHistory.length;
        while (lo + 1 < hi) {
            uint256 mid = (lo + hi) / 2;
            if (pointHistory[mid].ts <= timestamp) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    // -----------------------------------------------------------------
    // Internal - checked casts
    // -----------------------------------------------------------------
    //
    // The veCRV checkpoint math is signed (slopes carry negative
    // slope-change deltas), so unsigned amounts / time deltas cross into
    // int128 and clamped biases cross back. All inputs are bounded - amounts
    // ≤ TOKEN supply (1e27), time deltas ≤ maxLockDuration (~1.3e8 s), and
    // the derived slopes/biases (≤ ~1e27) sit far inside int128 range
    // (~1.7e38). The narrowing-cast lint disables are localized to these two
    // helpers instead of scattering ~28 inline disables across the math.

    /// @dev Cast a bounded unsigned value into int128 (see note above).
    function _toInt128(uint256 x) internal pure returns (int128) {
        // forge-lint: disable-next-line(unsafe-typecast)
        return int128(int256(x));
    }

    /// @dev Cast an already-clamped (>= 0) int128 bias back to uint256.
    function _toUint256(int128 x) internal pure returns (uint256) {
        // forge-lint: disable-next-line(unsafe-typecast)
        return uint256(uint128(x));
    }

    // -----------------------------------------------------------------
    // Internal - validation
    // -----------------------------------------------------------------

    function _requireDurationInRange(uint256 weekAlignedEnd) internal view {
        if (weekAlignedEnd <= block.timestamp) {
            revert LockDurationOutOfRange(0, minLockDuration, maxLockDuration);
        }
        uint256 duration = weekAlignedEnd - block.timestamp;
        if (duration < minLockDuration || duration > maxLockDuration) {
            revert LockDurationOutOfRange(duration, minLockDuration, maxLockDuration);
        }
    }

    // forge-lint: disable-end(block-timestamp)
    // forge-lint: disable-end(divide-before-multiply)
    // slither-disable-end timestamp
    // slither-disable-end divide-before-multiply
    // slither-disable-end incorrect-equality
}
