// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";

import { Token } from "../src/Token.sol";
import { TestnetFaucet } from "../testnet/TestnetFaucet.sol";

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

contract TestnetFaucetTest is Test {
    Token internal token;
    TestnetFaucet internal faucet;

    address internal treasury = makeAddr("treasury");
    address internal admin = makeAddr("admin");
    address internal governance = makeAddr("governance");
    address internal pauser = makeAddr("pauser");
    address internal alice = makeAddr("alice");
    address internal bob = makeAddr("bob");

    uint256 internal constant INITIAL_FUNDING = 1_000_000e18;
    uint256 internal constant CLAIM_AMOUNT = 1000e18;
    uint256 internal constant COOLDOWN = 1 days;

    event Claimed(address indexed claimer, uint256 amount);
    event ClaimAmountSet(uint256 oldValue, uint256 newValue);
    event CooldownSet(uint256 oldValue, uint256 newValue);
    event Withdrawn(address indexed to, uint256 amount);

    function setUp() public {
        // Mint TOKEN to the treasury and approve the predicted faucet address
        // before deploy so the constructor can `safeTransferFrom` atomically.
        token = new Token(treasury);
        faucet = _deployFaucet(token, INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN);
        // Move the test forward so first-claim sentinel logic is exercised in
        // a realistic timestamp range (avoids ambiguity around block.timestamp == 0).
        vm.warp(1_700_000_000);
    }

    function _deployFaucet(IERC20 fundingToken, uint256 funding, uint256 amount, uint256 cd)
        internal
        returns (TestnetFaucet)
    {
        address predicted = vm.computeCreateAddress(address(this), vm.getNonce(address(this)));
        vm.prank(treasury);
        fundingToken.approve(predicted, funding);
        TestnetFaucet f = new TestnetFaucet(fundingToken, treasury, funding, amount, cd, admin, governance, pauser);
        assertEq(address(f), predicted, "predicted address must match");
        return f;
    }

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    function test_constructor_setsStateAndRoles() public view {
        assertEq(address(faucet.token()), address(token));
        assertEq(faucet.claimAmount(), CLAIM_AMOUNT);
        assertEq(faucet.cooldown(), COOLDOWN);

        assertTrue(faucet.hasRole(faucet.DEFAULT_ADMIN_ROLE(), admin));
        assertTrue(faucet.hasRole(faucet.GOVERNANCE_ROLE(), governance));
        assertTrue(faucet.hasRole(faucet.PAUSER_ROLE(), pauser));

        assertEq(token.balanceOf(address(faucet)), INITIAL_FUNDING);
        assertEq(token.balanceOf(treasury), 1_000_000_000e18 - INITIAL_FUNDING);
    }

    function test_constructor_allowsZeroCooldown() public {
        // The contract intentionally allows cooldown == 0 (governance may
        // want a no-cooldown burst for a short test). Deploying with cd==0
        // must succeed; this test pins that.
        Token t = new Token(treasury);
        address predicted = vm.computeCreateAddress(address(this), vm.getNonce(address(this)));
        vm.prank(treasury);
        t.approve(predicted, INITIAL_FUNDING);
        TestnetFaucet f = new TestnetFaucet(
            IERC20(address(t)), treasury, INITIAL_FUNDING, CLAIM_AMOUNT, 0, admin, governance, pauser
        );
        assertEq(f.cooldown(), 0);
    }

    function test_constructor_revertsOnZeroTokenAddress() public {
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        new TestnetFaucet(
            IERC20(address(0)), treasury, INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN, admin, governance, pauser
        );
    }

    function test_constructor_revertsOnZeroTreasury() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        new TestnetFaucet(
            IERC20(address(t)), address(0), INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN, admin, governance, pauser
        );
    }

    function test_constructor_revertsOnZeroAdmin() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        new TestnetFaucet(
            IERC20(address(t)), treasury, INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN, address(0), governance, pauser
        );
    }

    function test_constructor_revertsOnZeroGovernance() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        new TestnetFaucet(
            IERC20(address(t)), treasury, INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN, admin, address(0), pauser
        );
    }

    function test_constructor_revertsOnZeroPauser() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        new TestnetFaucet(
            IERC20(address(t)), treasury, INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN, admin, governance, address(0)
        );
    }

    function test_constructor_revertsOnZeroFunding() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAmount.selector);
        new TestnetFaucet(IERC20(address(t)), treasury, 0, CLAIM_AMOUNT, COOLDOWN, admin, governance, pauser);
    }

    function test_constructor_revertsOnZeroClaimAmount() public {
        Token t = new Token(treasury);
        vm.expectRevert(TestnetFaucet.ZeroAmount.selector);
        new TestnetFaucet(IERC20(address(t)), treasury, INITIAL_FUNDING, 0, COOLDOWN, admin, governance, pauser);
    }

    // -----------------------------------------------------------------
    // claim
    // -----------------------------------------------------------------

    function test_claim_happyPath() public {
        vm.expectEmit(true, false, false, true, address(faucet));
        emit Claimed(alice, CLAIM_AMOUNT);

        vm.prank(alice);
        faucet.claim();

        assertEq(token.balanceOf(alice), CLAIM_AMOUNT);
        assertEq(token.balanceOf(address(faucet)), INITIAL_FUNDING - CLAIM_AMOUNT);
        assertEq(faucet.lastClaimedAt(alice), block.timestamp);
    }

    function test_claim_revertsBeforeCooldown() public {
        vm.prank(alice);
        faucet.claim();

        // Advance partway through the cooldown — claim must still revert with
        // remaining == COOLDOWN - elapsed.
        uint256 elapsed = COOLDOWN / 3;
        vm.warp(block.timestamp + elapsed);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(TestnetFaucet.CooldownNotElapsed.selector, COOLDOWN - elapsed));
        faucet.claim();
    }

    function test_claim_succeedsAfterCooldown() public {
        vm.prank(alice);
        faucet.claim();

        vm.warp(block.timestamp + COOLDOWN);

        vm.prank(alice);
        faucet.claim();

        assertEq(token.balanceOf(alice), 2 * CLAIM_AMOUNT);
        assertEq(faucet.lastClaimedAt(alice), block.timestamp);
    }

    function test_claim_perCallerCooldownIndependence() public {
        // Bob's clock is unaffected by Alice's claim.
        vm.prank(alice);
        faucet.claim();

        vm.prank(bob);
        faucet.claim();

        assertEq(token.balanceOf(alice), CLAIM_AMOUNT);
        assertEq(token.balanceOf(bob), CLAIM_AMOUNT);
    }

    function test_claim_revertsOnInsufficientBalance() public {
        // Drain the faucet to below CLAIM_AMOUNT, then assert claim reverts
        // with the exact balance/requested pair encoded.
        uint256 leave = CLAIM_AMOUNT - 1;
        vm.prank(governance);
        faucet.withdraw(treasury, INITIAL_FUNDING - leave);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(TestnetFaucet.InsufficientBalance.selector, leave, CLAIM_AMOUNT));
        faucet.claim();
    }

    function test_claim_revertsWhenPaused() public {
        vm.prank(pauser);
        faucet.pause();

        vm.prank(alice);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        faucet.claim();
    }

    function test_claim_blocksReentrancy() public {
        // Re-deploy faucet over a malicious ERC20 whose `_update` re-enters
        // claim during the transfer. The outer call must bubble OZ's
        // ReentrancyGuardReentrantCall back through SafeERC20.
        MaliciousERC20 mal = new MaliciousERC20();
        mal.mint(treasury, INITIAL_FUNDING);

        TestnetFaucet f = _deployFaucet(IERC20(address(mal)), INITIAL_FUNDING, CLAIM_AMOUNT, COOLDOWN);
        mal.setFaucet(f);
        mal.armAttack();

        vm.prank(alice);
        vm.expectRevert(ReentrancyGuard.ReentrancyGuardReentrantCall.selector);
        f.claim();
    }

    // -----------------------------------------------------------------
    // Cooldown fuzz (#636 explicit acceptance criterion)
    // -----------------------------------------------------------------

    function testFuzz_claim_cooldownBoundary(uint256 delta) public {
        // Bound delta so block.timestamp + delta doesn't overflow but still
        // exercises both sides of the cooldown threshold.
        delta = bound(delta, 0, 30 days);

        vm.prank(alice);
        faucet.claim();
        uint256 firstClaimAt = block.timestamp;

        vm.warp(firstClaimAt + delta);

        if (delta >= COOLDOWN) {
            vm.prank(alice);
            faucet.claim();
            assertEq(faucet.lastClaimedAt(alice), block.timestamp, "lastClaimedAt must advance");
            assertEq(token.balanceOf(alice), 2 * CLAIM_AMOUNT, "two payouts");
        } else {
            vm.prank(alice);
            vm.expectRevert(abi.encodeWithSelector(TestnetFaucet.CooldownNotElapsed.selector, COOLDOWN - delta));
            faucet.claim();
            assertEq(faucet.lastClaimedAt(alice), firstClaimAt, "lastClaimedAt unchanged on revert");
        }
    }

    // -----------------------------------------------------------------
    // timeUntilNext
    // -----------------------------------------------------------------

    function test_timeUntilNext_zeroBeforeFirstClaim() public view {
        assertEq(faucet.timeUntilNext(alice), 0);
    }

    function test_timeUntilNext_tracksCooldownWindow() public {
        vm.prank(alice);
        faucet.claim();

        assertEq(faucet.timeUntilNext(alice), COOLDOWN);

        vm.warp(block.timestamp + COOLDOWN / 4);
        assertEq(faucet.timeUntilNext(alice), COOLDOWN - COOLDOWN / 4);

        vm.warp(block.timestamp + COOLDOWN);
        assertEq(faucet.timeUntilNext(alice), 0);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setClaimAmount_updatesAndEmits() public {
        vm.expectEmit(false, false, false, true, address(faucet));
        emit ClaimAmountSet(CLAIM_AMOUNT, 5e18);

        vm.prank(governance);
        faucet.setClaimAmount(5e18);
        assertEq(faucet.claimAmount(), 5e18);
    }

    function test_setClaimAmount_revertsOnZero() public {
        vm.prank(governance);
        vm.expectRevert(TestnetFaucet.ZeroAmount.selector);
        faucet.setClaimAmount(0);
    }

    function test_setClaimAmount_revertsOnNonGovernance() public {
        bytes32 role = faucet.GOVERNANCE_ROLE();
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, role));
        faucet.setClaimAmount(5e18);
    }

    function test_setCooldown_updatesAndEmits() public {
        vm.expectEmit(false, false, false, true, address(faucet));
        emit CooldownSet(COOLDOWN, 2 days);

        vm.prank(governance);
        faucet.setCooldown(2 days);
        assertEq(faucet.cooldown(), 2 days);
    }

    function test_setCooldown_allowsZero() public {
        vm.prank(governance);
        faucet.setCooldown(0);
        assertEq(faucet.cooldown(), 0);

        // With cooldown == 0, claims should be back-to-back permissible.
        vm.prank(alice);
        faucet.claim();
        vm.prank(alice);
        faucet.claim();
        assertEq(token.balanceOf(alice), 2 * CLAIM_AMOUNT);
    }

    function test_setCooldown_revertsOnNonGovernance() public {
        bytes32 role = faucet.GOVERNANCE_ROLE();
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, role));
        faucet.setCooldown(2 days);
    }

    // -----------------------------------------------------------------
    // withdraw
    // -----------------------------------------------------------------

    function test_withdraw_transfersAndEmits() public {
        uint256 amt = 100e18;
        uint256 treasuryBefore = token.balanceOf(treasury);

        vm.expectEmit(true, false, false, true, address(faucet));
        emit Withdrawn(treasury, amt);

        vm.prank(governance);
        faucet.withdraw(treasury, amt);

        assertEq(token.balanceOf(address(faucet)), INITIAL_FUNDING - amt);
        assertEq(token.balanceOf(treasury), treasuryBefore + amt);
    }

    function test_withdraw_worksWhilePaused() public {
        // withdraw is intentionally pause-independent so governance can drain
        // a paused faucet without unpausing.
        vm.prank(pauser);
        faucet.pause();

        vm.prank(governance);
        faucet.withdraw(treasury, 100e18);

        assertEq(token.balanceOf(address(faucet)), INITIAL_FUNDING - 100e18);
    }

    function test_withdraw_revertsOnZeroTo() public {
        vm.prank(governance);
        vm.expectRevert(TestnetFaucet.ZeroAddress.selector);
        faucet.withdraw(address(0), 100e18);
    }

    function test_withdraw_revertsOnZeroAmount() public {
        vm.prank(governance);
        vm.expectRevert(TestnetFaucet.ZeroAmount.selector);
        faucet.withdraw(treasury, 0);
    }

    function test_withdraw_revertsOnInsufficientBalance() public {
        vm.prank(governance);
        vm.expectRevert(
            abi.encodeWithSelector(TestnetFaucet.InsufficientBalance.selector, INITIAL_FUNDING, INITIAL_FUNDING + 1)
        );
        faucet.withdraw(treasury, INITIAL_FUNDING + 1);
    }

    function test_withdraw_revertsOnNonGovernance() public {
        bytes32 role = faucet.GOVERNANCE_ROLE();
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, role));
        faucet.withdraw(treasury, 100e18);
    }

    // -----------------------------------------------------------------
    // Pause
    // -----------------------------------------------------------------

    function test_pause_blocksClaimUnpauseRestores() public {
        vm.prank(pauser);
        faucet.pause();
        assertTrue(faucet.paused());

        vm.prank(alice);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        faucet.claim();

        vm.prank(pauser);
        faucet.unpause();
        assertFalse(faucet.paused());

        vm.prank(alice);
        faucet.claim();
        assertEq(token.balanceOf(alice), CLAIM_AMOUNT);
    }

    function test_pause_revertsOnNonPauser() public {
        bytes32 role = faucet.PAUSER_ROLE();
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, role));
        faucet.pause();
    }

    function test_unpause_revertsOnNonPauser() public {
        bytes32 role = faucet.PAUSER_ROLE();
        vm.prank(pauser);
        faucet.pause();

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, role));
        faucet.unpause();
    }

    // -----------------------------------------------------------------
    // AccessControl: grant / revoke
    // -----------------------------------------------------------------

    function test_admin_canGrantAndRevokeRoles() public {
        address newPauser = makeAddr("newPauser");
        bytes32 pauserRole = faucet.PAUSER_ROLE();

        vm.startPrank(admin);
        faucet.grantRole(pauserRole, newPauser);
        vm.stopPrank();
        assertTrue(faucet.hasRole(pauserRole, newPauser));

        // New pauser can pause.
        vm.prank(newPauser);
        faucet.pause();

        // Revoke and verify they no longer can.
        vm.startPrank(admin);
        faucet.revokeRole(pauserRole, newPauser);
        vm.stopPrank();

        vm.prank(newPauser);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, newPauser, pauserRole)
        );
        faucet.unpause();
    }

    function test_nonAdmin_cannotGrantRoles() public {
        bytes32 adminRole = faucet.DEFAULT_ADMIN_ROLE();
        bytes32 pauserRole = faucet.PAUSER_ROLE();
        vm.prank(alice);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, adminRole)
        );
        faucet.grantRole(pauserRole, alice);
    }
}

// -----------------------------------------------------------------
// MaliciousERC20: re-enters `claim` during the outer `claim` transfer to
// prove the `nonReentrant` guard. Standard OZ ERC20 with a single hook in
// `_update` that calls back into the faucet exactly once.
// -----------------------------------------------------------------
contract MaliciousERC20 is ERC20 {
    TestnetFaucet public faucet;
    bool public attacking;

    constructor() ERC20("Mal", "MAL") { }

    function setFaucet(TestnetFaucet f) external {
        faucet = f;
    }

    function armAttack() external {
        attacking = true;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function _update(address from, address to, uint256 value) internal override {
        super._update(from, to, value);
        // Only re-enter when the faucet is paying out — i.e. on the
        // `safeTransfer(msg.sender, amount)` inside `claim`.
        if (attacking && from == address(faucet) && address(faucet) != address(0)) {
            attacking = false; // single-shot — otherwise we'd recurse forever pre-guard
            faucet.claim();
        }
    }
}
