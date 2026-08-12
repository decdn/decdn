// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @notice Test-only 6-decimal USDC stand-in with a permissionless `mint`.
///         Unlike `DeployUSDC` (a zero-supply fixed mock used inside in-process
///         deploy tests), this is deployed standalone to a local anvil by the
///         Rust on-chain settlement e2e (`crates/node/tests/anvil_settlement_e2e.rs`,
///         issue #745): the harness mints settlement USDC to the test client so
///         it can fund a `PaymentPool.openPool` deposit. `mint` is open
///         by design — this contract must never ship to a real network.
contract MintableUSDC is ERC20 {
    constructor() ERC20("USD Coin", "USDC") { }

    /// @notice USDC uses 6 decimals; mirror it so deposit/voucher amounts in the
    ///         e2e match production µUSDC math.
    function decimals() public pure override returns (uint8) {
        return 6;
    }

    /// @notice Mint `amount` base units to `to`. Test-only faucet.
    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}
