// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { Token } from "../src/Token.sol";

contract TokenTest is Test {
    Token internal token;
    address internal holder;

    function setUp() public {
        holder = makeAddr("holder");
        token = new Token(holder);
    }

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    function test_constructor_mintsFullSupplyToHolder() public view {
        assertEq(token.totalSupply(), 1_000_000_000e18);
        assertEq(token.balanceOf(holder), 1_000_000_000e18);
    }

    function test_constructor_metadata() public view {
        assertEq(token.name(), "deCDN");
        assertEq(token.symbol(), "DCDN");
        assertEq(token.decimals(), 18);
    }

    function test_constructor_revertsOnZeroInitialHolder() public {
        vm.expectRevert(Token.ZeroInitialHolder.selector);
        new Token(address(0));
    }

    function test_constructor_totalSupplyConstantMatches() public view {
        // The compile-time constant must match the minted supply — the
        // audit guarantee "supply is exactly 1B" depends on these being equal.
        assertEq(token.TOTAL_SUPPLY(), token.totalSupply());
        assertEq(token.TOTAL_SUPPLY(), 1_000_000_000e18);
    }

    // -----------------------------------------------------------------
    // No mint path post-genesis — the audit guarantee that supply never
    // grows depends on `mint` not existing as a public function. A
    // low-level call with the canonical `mint(address,uint256)` selector
    // must revert (no fallback), and totalSupply must stay constant.
    // -----------------------------------------------------------------

    function test_noMintFunctionExposed() public {
        (bool ok,) = address(token).call(abi.encodeWithSignature("mint(address,uint256)", holder, 1e18));
        assertFalse(ok, "mint must not exist on Token");
        assertEq(token.totalSupply(), 1_000_000_000e18, "supply unchanged");
    }

    // -----------------------------------------------------------------
    // Burn semantics (the 20% burn leg of slashing depends on this)
    // -----------------------------------------------------------------

    function test_burn_reducesTotalSupplyAndBalance() public {
        uint256 burnAmount = 500e18;
        uint256 supplyBefore = token.totalSupply();

        vm.prank(holder);
        token.burn(burnAmount);

        assertEq(token.totalSupply(), supplyBefore - burnAmount);
        assertEq(token.balanceOf(holder), supplyBefore - burnAmount);
    }

    function testFuzz_burn_reducesSupplyByExactAmount(uint256 amount) public {
        amount = bound(amount, 0, token.balanceOf(holder));
        uint256 supplyBefore = token.totalSupply();

        vm.prank(holder);
        token.burn(amount);

        assertEq(token.totalSupply(), supplyBefore - amount, "supply delta");
        assertEq(token.balanceOf(holder), supplyBefore - amount, "balance delta");
    }

    function test_burnFrom_consumesAllowance() public {
        address spender = makeAddr("spender");
        uint256 burnAmount = 1000e18;

        vm.prank(holder);
        token.approve(spender, burnAmount);

        vm.prank(spender);
        token.burnFrom(holder, burnAmount);

        assertEq(token.totalSupply(), 1_000_000_000e18 - burnAmount);
        assertEq(token.allowance(holder, spender), 0);
    }

    // -----------------------------------------------------------------
    // EIP-2612 Permit (ADR 024 / ERC-4337 + ERC-1271 path)
    // -----------------------------------------------------------------

    function test_permit_setsAllowance() public {
        Vm.Wallet memory owner = vm.createWallet("permit-owner");
        address spender = makeAddr("spender");
        uint256 value = 42e18;
        uint256 deadline = block.timestamp + 1 hours;
        uint256 nonce = token.nonces(owner.addr);

        bytes32 structHash = keccak256(
            abi.encode(
                keccak256("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"),
                owner.addr,
                spender,
                value,
                nonce,
                deadline
            )
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", token.DOMAIN_SEPARATOR(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(owner, digest);

        token.permit(owner.addr, spender, value, deadline, v, r, s);

        assertEq(token.allowance(owner.addr, spender), value);
        assertEq(token.nonces(owner.addr), nonce + 1);
    }

    // -----------------------------------------------------------------
    // ERC20Votes is intentionally not inherited (see Token.sol NatSpec).
    // Locked here so a future contributor doesn't silently re-add it:
    // `delegate`, `getVotes`, `getPastVotes`, `delegates` must not exist
    // on the public surface. Each selector probe via low-level call.
    // -----------------------------------------------------------------

    function test_noVotesSurfaceExposed() public {
        bytes[5] memory probes = [
            abi.encodeWithSignature("delegate(address)", holder),
            abi.encodeWithSignature("getVotes(address)", holder),
            abi.encodeWithSignature("getPastVotes(address,uint256)", holder, 0),
            abi.encodeWithSignature("delegates(address)", holder),
            abi.encodeWithSignature("getPastTotalSupply(uint256)", 0)
        ];
        for (uint256 i = 0; i < probes.length; i++) {
            (bool ok,) = address(token).call(probes[i]);
            assertFalse(ok, "ERC20Votes surface must not exist on Token");
        }
    }
}
