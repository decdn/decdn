// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IStakingRegistryEject } from "../../src/interfaces/IStakingRegistryEject.sol";

/// @notice Test stand-in for `StakingRegistry`'s eject surface. Records each
///         ejected operator so tests can assert the cross-call (tests also use
///         `vm.expectCall` for argument-level assertions). NEVER deployed
///         outside `forge test`.
contract MockStakingRegistryEject is IStakingRegistryEject {
    address[] public ejected;

    function ejectNode(address operator) external {
        ejected.push(operator);
    }

    function ejectedCount() external view returns (uint256) {
        return ejected.length;
    }
}
