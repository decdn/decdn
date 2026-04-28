// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Permit } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";

/// @notice 6-decimal ERC20 stand-in for USDC on local networks.
/// @dev Anyone can mint. Only for tests and anvil deploys.
contract MockUSDC is ERC20, ERC20Permit {
    constructor() ERC20("Mock USDC", "USDC") ERC20Permit("Mock USDC") { }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(
        address to,
        uint256 amount
    ) external {
        _mint(to, amount);
    }
}
