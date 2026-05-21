// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ISafetyReserve } from "../../src/interfaces/ISafetyReserve.sol";

/// @notice Test-only stand-in for SafetyReserve. Records each call so tests
///         can assert that `StakingRegistry.slash` correctly fired the
///         redirect leg. NEVER deployed outside `forge test`.
contract MockSafetyReserve is ISafetyReserve {
    struct Inflow {
        address operator;
        uint256 amount;
    }

    Inflow[] public inflows;

    function recordSlashInflow(address operator, uint256 amount) external override {
        inflows.push(Inflow({ operator: operator, amount: amount }));
    }

    function inflowCount() external view returns (uint256) {
        return inflows.length;
    }
}
