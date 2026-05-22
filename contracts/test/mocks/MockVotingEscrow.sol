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

    function setBalance(address account, uint256 weight) external {
        balance[account] = weight;
    }

    function setSupply(uint256 newSupply) external {
        supply = newSupply;
    }

    function balanceOfAt(address account, uint256) external view returns (uint256) {
        return balance[account];
    }

    function totalSupplyAt(uint256) external view returns (uint256) {
        return supply;
    }

    function totalSupply() external view returns (uint256) {
        return supply;
    }
}
