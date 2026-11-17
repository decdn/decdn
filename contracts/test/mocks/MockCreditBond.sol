// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ICapacityBondCredit } from "../../src/interfaces/ICapacityBondCredit.sol";

/// @notice Test-only `CapacityBond` reads for `FeeRouter.deposit`: the active
///         flag and the declared capacity per operator.
contract MockCreditBond is ICapacityBondCredit {
    uint64 public immutable epochLengthValue;
    mapping(address => bool) public activeOf;
    mapping(address => uint256) public mbpsOf;

    constructor(uint64 epochLengthValue_) {
        epochLengthValue = epochLengthValue_;
    }

    function set(address operator, bool active, uint256 mbps) external {
        activeOf[operator] = active;
        mbpsOf[operator] = mbps;
    }

    function isActive(address operator) external view override returns (bool) {
        return activeOf[operator];
    }

    function declaredMbps(address operator) external view override returns (uint256) {
        return mbpsOf[operator];
    }

    function epochLength() external view override returns (uint64) {
        return epochLengthValue;
    }
}
