// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ICapacityBond } from "../../src/interfaces/ICapacityBond.sol";
import { SlashRecord, SlashStatus } from "../../src/SlashEscrowLib.sol";

/// @notice Test-only stand-in for `CapacityBond`. Lets tests inject the
///         exact `firstBondedAt` values and slash records consumed by
///         `DecdnGovernor._getVotes` per ADR 036.
contract MockCapacityBond is ICapacityBond {
    mapping(address => uint64) public firstBondedAtOf;

    function setFirstBondedAt(address operator, uint64 t) external {
        firstBondedAtOf[operator] = t;
    }

    function firstBondedAt(address operator) external view override returns (uint64) {
        return firstBondedAtOf[operator];
    }

    mapping(address => mapping(uint64 => uint256)) public declaredMbpsAtEpochOf;

    function setDeclaredMbpsAtEpoch(address operator, uint64 epoch, uint256 mbps) external {
        declaredMbpsAtEpochOf[operator][epoch] = mbps;
    }

    function declaredMbpsAtEpoch(address operator, uint64 epoch) external view override returns (uint256) {
        return declaredMbpsAtEpochOf[operator][epoch];
    }

    /// @notice Per-slashId records and the per-operator slash list, in slash
    ///         order. Tests append with `addSlash` and flip a record's status
    ///         with `setSlashStatus`.
    SlashRecord[] internal _slashRecords;
    mapping(address => uint256[]) internal _operatorSlashIds;

    function addSlash(address operator, uint64 slashedAt, SlashStatus status) external returns (uint256 slashId) {
        slashId = _slashRecords.length;
        _slashRecords.push(
            SlashRecord({
                operator: operator,
                slashedAt: slashedAt,
                status: status,
                offenseType: 0,
                challenger: address(0),
                appealWindowClose: 0,
                slashAmount: 0,
                evidenceHash: bytes32(0)
            })
        );
        _operatorSlashIds[operator].push(slashId);
    }

    function setSlashStatus(uint256 slashId, SlashStatus status) external {
        _slashRecords[slashId].status = status;
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

    function operatorSlashCount(address operator) external view override returns (uint256) {
        return _operatorSlashIds[operator].length;
    }

    function operatorSlashIdAt(address operator, uint256 index) external view override returns (uint256) {
        return _operatorSlashIds[operator][index];
    }

    function getSlashRecord(uint256 slashId) external view override returns (SlashRecord memory) {
        return _slashRecords[slashId];
    }
}
