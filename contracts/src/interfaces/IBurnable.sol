// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IBurnable
/// @notice Narrow view of OpenZeppelin's `ERC20Burnable.burn` so fund-holding
///         contracts can actually reduce `totalSupply` instead of sending
///         tokens to a dead address. TOKEN inherits `ERC20Burnable` which
///         exposes this surface; any other burnable ERC20 with the same
///         selector works too.
interface IBurnable {
    /// @notice Destroys `value` tokens from the caller's balance,
    /// decrementing `totalSupply`.
    function burn(
        uint256 value
    ) external;
}
