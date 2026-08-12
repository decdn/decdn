// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

import { PaymentPool } from "../src/PaymentPool.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { IFeeRouterSettlement } from "../src/interfaces/IFeeRouterSettlement.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @notice Configurable `isActive` registry stand-in. `PaymentPool` never
///         calls it in this task (a pool names no provider), but the
///         constructor still takes a validated non-zero `capacityBond`.
contract MockActiveBond is ICapacityBondActivity {
    mapping(address => bool) internal _active;

    function setActive(address operator, bool a) external {
        _active[operator] = a;
    }

    function isActive(address operator) external view override returns (bool) {
        return _active[operator];
    }
}

/// @notice Records `routeSettlement` calls and pulls USDC exactly like the
///         real `FeeRouter`. Not exercised by this task's `openPool`/`topUp`
///         surface, but ported so the constructor's router conformance probe
///         has a realistic double to construct against.
contract MockSettlementRouter is IFeeRouterSettlement {
    IERC20 public immutable usdc;

    struct RouteCall {
        address operator;
        uint256 bytesDelivered;
        uint256 amount;
    }

    RouteCall[] public calls;

    bool internal _paused;

    constructor(IERC20 usdc_) {
        usdc = usdc_;
    }

    function setPaused(bool p) external {
        _paused = p;
    }

    function paused() external view override returns (bool) {
        return _paused;
    }

    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount) external override {
        require(!_paused, "MockSettlementRouter: paused");
        require(amount != 0, "MockSettlementRouter: zero amount");
        usdc.transferFrom(msg.sender, address(this), amount);
        calls.push(RouteCall(operator, bytesDelivered, amount));
    }

    function callCount() external view returns (uint256) {
        return calls.length;
    }
}

/// @notice Router that pulls one wei LESS than approved. Not exercised until
///         `redeem` lands (Task 2); ported now to keep the double roster
///         aligned with `PaymentChannel.t.sol`.
contract UnderPullRouter is IFeeRouterSettlement {
    IERC20 public immutable usdc;

    constructor(IERC20 usdc_) {
        usdc = usdc_;
    }

    function routeSettlement(address, uint256, uint256 amount) external override {
        require(amount != 0, "UnderPullRouter: zero amount");
        usdc.transferFrom(msg.sender, address(this), amount - 1);
    }

    function paused() external pure override returns (bool) {
        return false;
    }
}

/// @notice Router that implements `routeSettlement` but NOT `paused()`. Used
///         to prove the constructor's conformance probe rejects a
///         non-conforming router loudly rather than bricking on a missing
///         pause view at some later call site.
contract NoPauseRouter {
    function routeSettlement(address, uint256, uint256) external { }
}

/// @notice Router whose `paused()` shares the selector but returns a
///         non-canonical bool word (2). Proves the conformance probe rejects
///         a return the high-level `paused()` call would strict-decode-revert on.
contract NonBoolPauseRouter {
    function routeSettlement(address, uint256, uint256) external { }

    function paused() external pure returns (uint256) {
        return 2;
    }
}

/// @notice Minimal ERC-1271 smart-account wallet. Not exercised by this
///         task's voucher-free surface; ported for parity with the double
///         roster later tasks (`redeem`) will need.
contract MockERC1271Wallet {
    bytes4 internal constant MAGIC = 0x1626ba7e;
    address public immutable owner;

    constructor(address owner_) {
        owner = owner_;
    }

    function isValidSignature(bytes32 hash, bytes calldata signature) external view returns (bytes4) {
        (address recovered, ECDSA.RecoverError err,) = ECDSA.tryRecover(hash, signature);
        return (err == ECDSA.RecoverError.NoError && recovered == owner) ? MAGIC : bytes4(0xffffffff);
    }
}

contract PaymentPoolTest is Test {
    MockUSDC internal usdc;
    MockActiveBond internal bond;
    MockSettlementRouter internal router;
    PaymentPool internal pool;

    address internal owner = address(0xC11E27);
    address internal admin = address(0xA11CE);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);

    uint256 internal constant DISPUTE_WINDOW = 48 hours;
    uint256 internal constant DELIVERY_FLOOR = 1;
    uint256 internal constant MAX_RATE_PER_MB = 1_000_000_000_000;
    uint256 internal constant DEPOSIT = 1000e6;

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered)");
    bytes32 internal constant POOL_OPENED_SIG = keccak256("PoolOpened(bytes32,address,uint256)");

    function setUp() public {
        usdc = new MockUSDC();
        bond = new MockActiveBond();
        router = new MockSettlementRouter(usdc);

        pool = new PaymentPool({
            usdc_: usdc,
            capacityBond_: bond,
            feeRouter_: address(router),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });

        vm.prank(admin);
        pool.grantRole(PAUSER_ROLE, pauser);

        usdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        usdc.approve(address(pool), type(uint256).max);
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _open() internal returns (bytes32 poolId) {
        vm.prank(owner);
        poolId = pool.openPool(DEPOSIT);
    }

    // -----------------------------------------------------------------
    // openPool
    // -----------------------------------------------------------------

    function test_openPool_derivesIdFromOwnerNonceAndIncrements() public {
        bytes32 expected0 = keccak256(abi.encodePacked(owner, uint256(0)));
        bytes32 id0 = _open();
        assertEq(id0, expected0);
        assertEq(pool.ownerPoolNonce(owner), 1);

        bytes32 expected1 = keccak256(abi.encodePacked(owner, uint256(1)));
        bytes32 id1 = _open();
        assertEq(id1, expected1);
        assertEq(pool.ownerPoolNonce(owner), 2);
        assertTrue(id0 != id1);
    }

    function test_openPool_namesNoProvider_noActiveGate() public {
        // No provider is ever named at `openPool`, and no `isActive` gate
        // runs — `bond` stays entirely unconfigured (default `false` for
        // every address) and the pool still opens.
        bytes32 id = _open();
        PaymentPool.Pool memory p = pool.getPool(id);
        assertEq(p.owner, owner);
        assertEq(uint256(p.status), uint256(PaymentPool.Status.Open));
    }

    function test_openPool_revertsOnZeroDeposit() public {
        vm.prank(owner);
        vm.expectRevert(PaymentPool.ZeroAmount.selector);
        pool.openPool(0);
    }

    function test_openPool_acceptsOneBaseUnit() public {
        vm.prank(owner);
        bytes32 id = pool.openPool(1);
        assertEq(pool.getPool(id).deposit, 1, "a one-base-unit deposit must open");
    }

    function test_openPool_revertsWhenPaused() public {
        vm.prank(pauser);
        pool.pause();
        vm.prank(owner);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        pool.openPool(DEPOSIT);
    }

    function test_openPool_creditsReceivedDeltaNotRequested() public {
        FeeOnTransferUSDC feeUsdc = new FeeOnTransferUSDC();
        MockSettlementRouter feeRouter = new MockSettlementRouter(feeUsdc);
        PaymentPool feePool = new PaymentPool({
            usdc_: feeUsdc,
            capacityBond_: bond,
            feeRouter_: address(feeRouter),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });
        feeUsdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        feeUsdc.approve(address(feePool), type(uint256).max);
        feeUsdc.setFeeBps(100);

        uint256 expected = DEPOSIT - (DEPOSIT * 100 / 10_000);
        vm.prank(owner);
        bytes32 id = feePool.openPool(DEPOSIT);

        assertEq(feePool.getPool(id).deposit, expected, "credited the delta");
        assertLt(feePool.getPool(id).deposit, DEPOSIT, "and it is less than requested");
        assertEq(feeUsdc.balanceOf(address(feePool)), expected, "which matches the real balance");
    }

    function test_openPool_revertsWhenTokenConfiscatesWholeDeposit() public {
        FeeOnTransferUSDC feeUsdc = new FeeOnTransferUSDC();
        MockSettlementRouter feeRouter = new MockSettlementRouter(feeUsdc);
        PaymentPool feePool = new PaymentPool({
            usdc_: feeUsdc,
            capacityBond_: bond,
            feeRouter_: address(feeRouter),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });
        feeUsdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        feeUsdc.approve(address(feePool), type(uint256).max);
        feeUsdc.setFeeBps(10_000); // 100% burn

        vm.prank(owner);
        vm.expectRevert(PaymentPool.ZeroAmount.selector);
        feePool.openPool(DEPOSIT);
    }

    // -----------------------------------------------------------------
    // topUp
    // -----------------------------------------------------------------

    function test_topUp_increasesDeposit() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.topUp(id, 500e6);
        assertEq(pool.getPool(id).deposit, DEPOSIT + 500e6);
    }

    function test_topUp_onlyOwner() public {
        bytes32 id = _open();
        vm.prank(stranger);
        vm.expectRevert(PaymentPool.NotPoolOwner.selector);
        pool.topUp(id, 500e6);
    }

    /// @dev `closePool` does not exist until a later task, so this test uses
    ///      `PaymentPoolHarness` (below) to force a pool directly into
    ///      `Closing` and pins `topUp`'s status gate against that state now,
    ///      rather than leaving it unverified until `closePool` lands.
    function test_topUp_revertsWhenClosing() public {
        PaymentPoolHarness harness = new PaymentPoolHarness({
            usdc_: usdc,
            capacityBond_: bond,
            feeRouter_: address(router),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });
        vm.prank(owner);
        usdc.approve(address(harness), type(uint256).max);
        vm.prank(owner);
        bytes32 id = harness.openPool(DEPOSIT);

        harness.forceStatus(id, PaymentPool.Status.Closing);

        vm.prank(owner);
        vm.expectRevert(PaymentPool.PoolNotOpen.selector);
        harness.topUp(id, 1);
    }

    function test_topUp_revertsWhenPaused() public {
        bytes32 id = _open();
        vm.prank(pauser);
        pool.pause();
        vm.prank(owner);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        pool.topUp(id, 500e6);
    }

    function test_topUp_creditsReceivedDelta() public {
        FeeOnTransferUSDC feeUsdc = new FeeOnTransferUSDC();
        MockSettlementRouter feeRouter = new MockSettlementRouter(feeUsdc);
        PaymentPool feePool = new PaymentPool({
            usdc_: feeUsdc,
            capacityBond_: bond,
            feeRouter_: address(feeRouter),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });
        feeUsdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        feeUsdc.approve(address(feePool), type(uint256).max);

        vm.prank(owner);
        bytes32 id = feePool.openPool(DEPOSIT);
        assertEq(feePool.getPool(id).deposit, DEPOSIT, "opened at full value (fee not yet armed)");

        feeUsdc.setFeeBps(100);
        uint256 top = 500e6;
        uint256 credited = top - (top * 100 / 10_000);
        vm.prank(owner);
        feePool.topUp(id, top);

        assertEq(feePool.getPool(id).deposit, DEPOSIT + credited, "top-up credits its delta");
        assertEq(feeUsdc.balanceOf(address(feePool)), DEPOSIT + credited, "and it matches the real balance");
    }

    // -----------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroAddress() public {
        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(IERC20(address(0)), bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, admin);

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(usdc, ICapacityBondActivity(address(0)), address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, admin);

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(usdc, bond, address(0), DISPUTE_WINDOW, DELIVERY_FLOOR, admin);

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, address(0));
    }

    function test_constructor_revertsOnEoaFeeRouter() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterHasNoCode.selector, stranger));
        new PaymentPool(usdc, bond, stranger, DISPUTE_WINDOW, DELIVERY_FLOOR, admin);
    }

    function test_constructor_revertsOnRouterMissingPausedView() public {
        NoPauseRouter bad = new NoPauseRouter();
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterMissingPausedView.selector, address(bad)));
        new PaymentPool(usdc, bond, address(bad), DISPUTE_WINDOW, DELIVERY_FLOOR, admin);

        NonBoolPauseRouter bad2 = new NonBoolPauseRouter();
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterMissingPausedView.selector, address(bad2)));
        new PaymentPool(usdc, bond, address(bad2), DISPUTE_WINDOW, DELIVERY_FLOOR, admin);
    }

    function test_constructor_revertsOnDisputeWindowOutOfBounds() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector, uint256(1 hours), uint256(48 hours), uint256(72 hours)
            )
        );
        new PaymentPool(usdc, bond, address(router), 1 hours, DELIVERY_FLOOR, admin);
    }

    function test_constructor_revertsOnRateFloorAboveWireCap() public {
        uint256 badFloor = MAX_RATE_PER_MB + 1;
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, badFloor));
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, badFloor, admin);
    }

    function test_constructor_revertsOnRateFloorBelowMinimum() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, uint256(0)));
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, 0, admin);
    }

    // -----------------------------------------------------------------
    // EIP-712 typing
    // -----------------------------------------------------------------

    function test_eip712_typehashesMatchAdr003() public view {
        assertEq(pool.CAPABILITY_TYPEHASH(), CAPABILITY_TYPEHASH);
        assertEq(pool.VOUCHER_TYPEHASH(), VOUCHER_TYPEHASH);

        bytes32 expectedDomainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PaymentPool")),
                keccak256(bytes("1")),
                block.chainid,
                address(pool)
            )
        );
        assertEq(pool.DOMAIN_SEPARATOR(), expectedDomainSeparator);
    }
}

/// @notice Test-only subclass exposing a way to force a pool's `status`
///         directly, so `topUp`'s `Closing`/`Closed` gate is testable before
///         `closePool` exists (a later task). Adds no production surface —
///         it lives only in this test file.
contract PaymentPoolHarness is PaymentPool {
    constructor(
        IERC20 usdc_,
        ICapacityBondActivity capacityBond_,
        address feeRouter_,
        uint256 disputeWindow_,
        uint256 deliveryFloor_,
        address admin
    ) PaymentPool(usdc_, capacityBond_, feeRouter_, disputeWindow_, deliveryFloor_, admin) { }

    function forceStatus(bytes32 poolId, Status s) external {
        pools[poolId].status = s;
    }
}

contract FeeOnTransferUSDC is ERC20 {
    uint256 public feeBps;

    constructor() ERC20("Fee USDC", "fUSDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function setFeeBps(uint256 feeBps_) external {
        require(feeBps_ <= 10_000, "fee > 100%");
        feeBps = feeBps_;
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function _update(address from, address to, uint256 value) internal override {
        if (from == address(0) || to == address(0) || feeBps == 0) {
            super._update(from, to, value);
            return;
        }
        uint256 fee = value * feeBps / 10_000;
        super._update(from, to, value - fee);
        if (fee > 0) super._update(from, address(0), fee);
    }
}
