// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import {
    ReentrancyGuardTransient
} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

import { IContentBlacklist } from "./interfaces/IContentBlacklist.sol";
import { IStakingRegistry } from "./interfaces/IStakingRegistry.sol";
import { Errors } from "./libraries/Errors.sol";
import { Roles } from "./libraries/Roles.sol";

/// @title ContentBlacklist
/// @notice Global hash blacklist and origin-operator ejection for deCDN.
/// @dev See ADR 011 (content takedown) and ADR 016 §3 (cross-contract calls).
///      PoC omits regional bodies. Implements governance (`GOVERNANCE_ROLE`),
///      emergency (`EMERGENCY_ROLE` with 14-day auto-expiry), and
///      governance ratification of emergency entries.
///
///      History is preserved per hash as an append-only list of
///      (addedAt, removedAt, emergencyExpiresAt) intervals. This lets
///      `wasBlacklistedAt(hash, ts)` answer questions about a past moment
///      even after the hash has been re-listed in a later interval —
///      critical for `SlashJudge.submitBlacklistChallenge` which slashes
///      based on probe evidence timestamped in the past.
contract ContentBlacklist is IContentBlacklist, AccessControl, ReentrancyGuardTransient {
    using SafeCast for uint256;

    /// @dev Emergency entries auto-expire after this window unless ratified by
    /// governance (ADR 011).
    uint64 public constant EMERGENCY_EXPIRY = 14 days;

    IStakingRegistry public immutable STAKING_REGISTRY;

    struct Interval {
        uint64 addedAt;
        uint64 removedAt; // 0 while still active (under current interval's rules)
        uint64 emergencyExpiresAt; // 0 for governance entries; non-zero = emergency
    }

    mapping(bytes32 hash => Interval[]) internal _intervals;
    uint256 public override blacklistVersion;

    // ---------------------------------------------------------------------
    //  Events
    // ---------------------------------------------------------------------

    event HashAdded(bytes32 indexed hash, address indexed by, bool emergency, uint64 expiresAt);
    event HashRemoved(bytes32 indexed hash, address indexed by);
    event HashRatified(bytes32 indexed hash, address indexed by);
    event OriginEjected(address indexed operator, address indexed by);

    // ---------------------------------------------------------------------
    //  Errors
    // ---------------------------------------------------------------------

    error AlreadyListed();
    error NotListed();
    error NotEmergencyEntry();

    // ---------------------------------------------------------------------
    //  Constructor
    // ---------------------------------------------------------------------

    constructor(
        IStakingRegistry stakingRegistry,
        address admin
    ) {
        if (address(stakingRegistry) == address(0) || admin == address(0)) {
            revert Errors.ZeroAddress();
        }
        STAKING_REGISTRY = stakingRegistry;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
    }

    // ---------------------------------------------------------------------
    //  Governance path
    // ---------------------------------------------------------------------

    function addHash(
        bytes32 hash
    ) external onlyRole(Roles.GOVERNANCE_ROLE) {
        _addHash(hash, 0);
    }

    function removeHash(
        bytes32 hash
    ) external onlyRole(Roles.GOVERNANCE_ROLE) {
        _removeHash(hash);
    }

    /// @notice Ratify an emergency entry, clearing its expiry and promoting it
    /// to a governance entry. Only works on the CURRENT (most recent) interval
    /// and only while that interval is still active.
    function ratifyEmergency(
        bytes32 hash
    ) external onlyRole(Roles.GOVERNANCE_ROLE) {
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length == 0) revert NotListed();
        Interval storage last = ivs[ivs.length - 1];
        if (!_intervalActive(last)) revert NotListed();
        if (last.emergencyExpiresAt == 0) revert NotEmergencyEntry();
        last.emergencyExpiresAt = 0;
        unchecked {
            ++blacklistVersion;
        }
        emit HashRatified(hash, msg.sender);
    }

    // ---------------------------------------------------------------------
    //  Emergency path
    // ---------------------------------------------------------------------

    function emergencyAdd(
        bytes32 hash
    ) external onlyRole(Roles.EMERGENCY_ROLE) {
        uint64 expiresAt = block.timestamp.toUint64() + EMERGENCY_EXPIRY;
        _addHash(hash, expiresAt);
    }

    // ---------------------------------------------------------------------
    //  Origin ejection (cross-contract)
    // ---------------------------------------------------------------------

    /// @notice Eject an origin operator through the StakingRegistry. Caller
    /// must hold `GOVERNANCE_ROLE` here; this contract must hold
    /// `BLACKLIST_ROLE` on the StakingRegistry (ADR 016 §2).
    function ejectOrigin(
        address operator
    ) external nonReentrant onlyRole(Roles.GOVERNANCE_ROLE) {
        if (operator == address(0)) revert Errors.ZeroAddress();
        emit OriginEjected(operator, msg.sender);
        STAKING_REGISTRY.ejectNode(operator);
    }

    // ---------------------------------------------------------------------
    //  Views (IContentBlacklist)
    // ---------------------------------------------------------------------

    function isBlacklisted(
        bytes32 hash
    ) external view override returns (bool) {
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length == 0) return false;
        return _intervalActive(ivs[ivs.length - 1]);
    }

    /// @notice Returns true if `hash` was blacklisted at time `timestamp`
    /// under any historical interval. Walks the interval list; expected
    /// depth is O(# add/remove cycles), typically 1.
    function wasBlacklistedAt(
        bytes32 hash,
        uint64 timestamp
    ) external view override returns (bool) {
        Interval[] storage ivs = _intervals[hash];
        uint256 len = ivs.length;
        for (uint256 i = 0; i < len; ++i) {
            Interval storage iv = ivs[i];
            if (timestamp < iv.addedAt) continue;
            // Upper bound: the earlier of removedAt (if set) and emergency
            // expiry (if emergency).
            uint64 endsAt = iv.removedAt;
            if (iv.emergencyExpiresAt != 0) {
                if (endsAt == 0 || iv.emergencyExpiresAt < endsAt) endsAt = iv.emergencyExpiresAt;
            }
            if (endsAt == 0 || timestamp < endsAt) return true;
        }
        return false;
    }

    /// @notice Returns the CURRENT (most recent) interval's metadata.
    function getEntry(
        bytes32 hash
    ) external view override returns (Entry memory) {
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length == 0) return Entry({ addedAt: 0, removedAt: 0, exists: false });
        Interval storage last = ivs[ivs.length - 1];
        return Entry({ addedAt: last.addedAt, removedAt: last.removedAt, exists: true });
    }

    /// @notice Returns the emergency expiry timestamp for the CURRENT
    /// interval of `hash` (0 if none or not an emergency entry).
    function emergencyExpiryOf(
        bytes32 hash
    ) external view returns (uint64) {
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length == 0) return 0;
        return ivs[ivs.length - 1].emergencyExpiresAt;
    }

    /// @notice Number of historical intervals recorded for `hash`.
    function intervalCount(
        bytes32 hash
    ) external view returns (uint256) {
        return _intervals[hash].length;
    }

    function intervalAt(
        bytes32 hash,
        uint256 index
    ) external view returns (Interval memory) {
        return _intervals[hash][index];
    }

    // ---------------------------------------------------------------------
    //  Internals
    // ---------------------------------------------------------------------

    function _addHash(
        bytes32 hash,
        uint64 emergencyExpiresAt
    ) internal {
        if (hash == bytes32(0)) revert Errors.ZeroAddress();
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length > 0 && _intervalActive(ivs[ivs.length - 1])) revert AlreadyListed();
        ivs.push(
            Interval({
                addedAt: block.timestamp.toUint64(),
                removedAt: 0,
                emergencyExpiresAt: emergencyExpiresAt
            })
        );
        unchecked {
            ++blacklistVersion;
        }
        emit HashAdded(hash, msg.sender, emergencyExpiresAt != 0, emergencyExpiresAt);
    }

    function _removeHash(
        bytes32 hash
    ) internal {
        Interval[] storage ivs = _intervals[hash];
        if (ivs.length == 0) revert NotListed();
        Interval storage last = ivs[ivs.length - 1];
        if (!_intervalActive(last)) revert NotListed();
        last.removedAt = block.timestamp.toUint64();
        unchecked {
            ++blacklistVersion;
        }
        emit HashRemoved(hash, msg.sender);
    }

    function _intervalActive(
        Interval storage iv
    ) internal view returns (bool) {
        if (iv.removedAt != 0) return false;
        if (iv.emergencyExpiresAt != 0 && block.timestamp >= iv.emergencyExpiresAt) return false;
        return true;
    }
}
