// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title IBurnableERC20
/// @notice ERC20 with the OZ `ERC20Burnable` mixin's `burn(uint256)` selector.
/// @dev    Used by `StakingRegistry.slash` to burn the 20% burn leg of the
///         slash split (ADR 026 § Slashing and burn) by calling
///         `token.burn(burnShare)` against TOKEN's `ERC20Burnable` surface.
///         A standalone interface keeps `StakingRegistry` from depending on
///         the concrete `Token` contract directly — easier to mock, and
///         decouples deployment ordering during local testing.
interface IBurnableERC20 is IERC20 {
    function burn(uint256 value) external;
}
