// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ICapacityBond } from "../../src/interfaces/ICapacityBond.sol";

/// @notice Test-only stand-in for `CapacityBond`. Lets tests inject the
///         exact `firstBondedAt` and `slashedAtEpoch` values consumed by
///         `DecdnGovernor._getVotes` per ADR 036.
contract MockCapacityBond is ICapacityBond {
    mapping(address => uint64) public firstBondedAtOf;
    mapping(address => uint64) public slashedAtEpochOf;

    function setFirstBondedAt(address operator, uint64 t) external {
        firstBondedAtOf[operator] = t;
    }

    function setSlashedAtEpoch(address operator, uint64 epoch) external {
        slashedAtEpochOf[operator] = epoch;
    }

    function firstBondedAt(address operator) external view override returns (uint64) {
        return firstBondedAtOf[operator];
    }

    function slashedAtEpoch(address operator) external view override returns (uint64) {
        return slashedAtEpochOf[operator];
    }

    mapping(address => mapping(uint64 => uint256)) public declaredMbpsAtEpochOf;

    function setDeclaredMbpsAtEpoch(address operator, uint64 epoch, uint256 mbps) external {
        declaredMbpsAtEpochOf[operator][epoch] = mbps;
    }

    function declaredMbpsAtEpoch(address operator, uint64 epoch) external view override returns (uint256) {
        return declaredMbpsAtEpochOf[operator][epoch];
    }

    /// @notice Per-slashId record. Tests configure via `setSlashRecord` then
    ///         pass `slashId` to `SlashAppeal.openSlashAppeal`.
    struct SlashRecord {
        address operator;
        uint64 slashedAt;
        uint256 slashAmount;
    }

    mapping(uint256 => SlashRecord) internal _slashRecords;

    function setSlashRecord(uint256 slashId, address op, uint64 slashedAt_, uint256 amount) external {
        _slashRecords[slashId] = SlashRecord(op, slashedAt_, amount);
    }

    function slashRecords(uint256 slashId)
        external
        view
        override
        returns (address operator, uint64 slashedAt_, uint256 slashAmount)
    {
        SlashRecord memory r = _slashRecords[slashId];
        return (r.operator, r.slashedAt, r.slashAmount);
    }
}
