// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ICapacityBond } from "../../src/interfaces/ICapacityBond.sol";

/// @notice Test-only stand-in for `CapacityBond`. Lets tests inject the
///         exact `firstBondedAt` and `declaredMbpsAtEpoch` values consumed by
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
}
