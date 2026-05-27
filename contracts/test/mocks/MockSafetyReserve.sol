// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ISafetyReserve } from "../../src/interfaces/ISafetyReserve.sol";

/// @notice Test-only stand-in for `SafetyReserve`. Records each
///         `recordSlashInflow` call so tests can assert that
///         `CapacityBond.slash` correctly fired the redirect leg. Every other
///         entrypoint in `ISafetyReserve` reverts so accidental use from a
///         test will fail loudly. NEVER deployed outside `forge test`.
contract MockSafetyReserve is ISafetyReserve {
    struct Inflow {
        address operator;
        uint256 amount;
    }

    Inflow[] public inflows;

    error NotImplementedInMock();

    function recordSlashInflow(address operator, uint256 amount) external override {
        inflows.push(Inflow({ operator: operator, amount: amount }));
    }

    function inflowCount() external view returns (uint256) {
        return inflows.length;
    }

    function payout(bytes32, address, uint256) external pure override returns (uint256, bool, uint256) {
        revert NotImplementedInMock();
    }

    function disbursePending() external pure override returns (uint256) {
        revert NotImplementedInMock();
    }

    function incidents(uint256) external pure override returns (Incident memory) {
        revert NotImplementedInMock();
    }

    function swapAccumulatedTokens(uint256, uint256) external pure override {
        revert NotImplementedInMock();
    }

    function openSlashAppeal(uint256, bytes32) external pure override returns (uint256) {
        revert NotImplementedInMock();
    }

    function fastTrackAppeal(uint256) external pure override {
        revert NotImplementedInMock();
    }

    function rejectAppeal(uint256) external pure override {
        revert NotImplementedInMock();
    }

    function ratifyAppeal(uint256) external pure override {
        revert NotImplementedInMock();
    }

    function reverseAppeal(uint256) external pure override {
        revert NotImplementedInMock();
    }

    function cleanupExpiredAppeal(uint256) external pure override {
        revert NotImplementedInMock();
    }
}
