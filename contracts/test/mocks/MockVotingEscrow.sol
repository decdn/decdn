// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IVotingEscrow } from "../../src/interfaces/IVotingEscrow.sol";

/// @notice Test-only vote source for `DecdnGovernor`. Returns settable, static
///         balances/supply (ignores the timestamp argument) so tests can pin
///         vote weight and ve-supply directly. The real `VotingEscrow` applies
///         linear decay; the governor only consumes whatever the vote source
///         returns, so static values exercise its quorum / threshold / counting
///         logic. NEVER deployed outside `forge test`.
contract MockVotingEscrow is IVotingEscrow {
    mapping(address account => uint256 weight) public balance;
    uint256 public supply;

    // Optional per-timestamp supply override. When set for a timestamp,
    // `totalSupplyAt` returns it; otherwise it falls back to `supply`. Lets a
    // test pin a different supply at a specific snapshot to exercise
    // snapshot-keyed reads (e.g. the proposal threshold) without disturbing the
    // default static behaviour the other tests rely on.
    mapping(uint256 timestamp => uint256 weight) private _supplyAt;
    mapping(uint256 timestamp => bool isSet) private _supplyAtSet;

    function setBalance(address account, uint256 weight) external {
        balance[account] = weight;
    }

    function setSupply(uint256 newSupply) external {
        supply = newSupply;
    }

    function setSupplyAt(uint256 timestamp, uint256 weight) external {
        _supplyAt[timestamp] = weight;
        _supplyAtSet[timestamp] = true;
    }

    function balanceOfAt(address account, uint256) external view returns (uint256) {
        return balance[account];
    }

    function totalSupplyAt(uint256 timestamp) external view returns (uint256) {
        return _supplyAtSet[timestamp] ? _supplyAt[timestamp] : supply;
    }
}
