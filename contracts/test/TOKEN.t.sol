// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Ownable } from "@openzeppelin/contracts/access/Ownable.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { Errors } from "../src/libraries/Errors.sol";

contract TOKENTest is Test {
    TOKEN internal token;

    address internal owner = makeAddr("owner");
    address internal holder = makeAddr("holder");
    address internal alice = makeAddr("alice");

    uint256 internal constant INITIAL = 1_000_000e18;

    function setUp() public {
        token = new TOKEN(holder, INITIAL, owner);
    }

    function test_Constructor_MintsInitialSupply() public view {
        assertEq(token.totalSupply(), INITIAL);
        assertEq(token.balanceOf(holder), INITIAL);
        assertEq(token.name(), "deCDN");
        assertEq(token.symbol(), "DCDN");
        assertEq(token.decimals(), 18);
        assertEq(token.owner(), owner);
    }

    function test_Constructor_RevertsOnZeroHolder() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        new TOKEN(address(0), INITIAL, owner);
    }

    function test_Constructor_RevertsOnZeroOwner() public {
        // OZ Ownable's own check fires first.
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableInvalidOwner.selector, address(0)));
        new TOKEN(holder, INITIAL, address(0));
    }

    function test_Constructor_AllowsZeroSupply() public {
        TOKEN t = new TOKEN(holder, 0, owner);
        assertEq(t.totalSupply(), 0);
    }

    function test_Mint_OnlyOwner() public {
        vm.prank(owner);
        token.mint(alice, 100e18);
        assertEq(token.balanceOf(alice), 100e18);
        assertEq(token.totalSupply(), INITIAL + 100e18);
    }

    function test_Mint_RevertsWhenNotOwner() public {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vm.prank(alice);
        token.mint(alice, 100e18);
    }

    function test_Mint_RevertsOnZeroAddress() public {
        // Zero-recipient rejection comes from OZ's `_mint` — no custom
        // wrapper check needed.
        vm.expectRevert(abi.encodeWithSignature("ERC20InvalidReceiver(address)", address(0)));
        vm.prank(owner);
        token.mint(address(0), 100e18);
    }

    function test_Mint_RevertsOnZeroAmount() public {
        vm.expectRevert(Errors.ZeroAmount.selector);
        vm.prank(owner);
        token.mint(alice, 0);
    }

    function test_Permit_SignedByHolder() public {
        uint256 privKey = 0xA11CE;
        address signer = vm.addr(privKey);
        vm.prank(holder);
        bool ok = token.transfer(signer, 500e18);
        require(ok, "transfer");

        uint256 deadline = block.timestamp + 1 hours;
        uint256 value = 100e18;
        uint256 nonce = token.nonces(signer);

        bytes32 permitTypehash = keccak256(
            "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"
        );
        bytes32 structHash =
            keccak256(abi.encode(permitTypehash, signer, alice, value, nonce, deadline));
        bytes32 digest =
            keccak256(abi.encodePacked("\x19\x01", token.DOMAIN_SEPARATOR(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(privKey, digest);

        token.permit(signer, alice, value, deadline, v, r, s);
        assertEq(token.allowance(signer, alice), value);
        assertEq(token.nonces(signer), nonce + 1);
    }

    function testFuzz_Mint(
        uint256 amount
    ) public {
        amount = bound(amount, 1, type(uint128).max);
        vm.prank(owner);
        token.mint(alice, amount);
        assertEq(token.balanceOf(alice), amount);
    }
}
