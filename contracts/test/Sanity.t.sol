// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title SanityTest
/// @notice Smoke test confirming forge + forge-std + OZ v5.1 are wired through
///         the remappings before any deCDN contracts are introduced. Delete
///         once the first real contract from issue #452 lands.
contract SanityTest is Test {
    function test_forgeStdLoaded() public pure {
        assertTrue(true);
    }

    function test_openZeppelinRemappingResolves() public pure {
        // Compile-time check: the import above proves the @openzeppelin remap
        // works; this asserts the canonical ERC20.transfer selector to catch
        // accidental ABI drift if the import ever silently retargets.
        assertEq(IERC20.transfer.selector, bytes4(keccak256("transfer(address,uint256)")));
    }
}
