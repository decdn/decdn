// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { OpenVettingPolicy } from "../src/OpenVettingPolicy.sol";

contract OpenVettingPolicyTest is Test {
    OpenVettingPolicy internal policy;

    function setUp() public {
        policy = new OpenVettingPolicy();
    }

    function test_isVetted_alwaysTrue() public view {
        assertTrue(policy.isVetted(address(0)));
        assertTrue(policy.isVetted(address(0xBEEF)));
        assertTrue(policy.isVetted(address(this)));
    }
}
