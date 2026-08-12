// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

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

    function totalRoutedPaid() external view returns (uint256 total) {
        for (uint256 i = 0; i < calls.length; i++) {
            total += calls[i].amount;
        }
    }

    function totalBytes() external view returns (uint256 total) {
        for (uint256 i = 0; i < calls.length; i++) {
            total += calls[i].bytesDelivered;
        }
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

    uint256 internal constant OWNER_PK = 0xC11E27;
    uint256 internal constant SIGNER_PK = 0x519E7;
    uint256 internal constant STRANGER_PK = 0xBADBAD;
    address internal owner; // vm.addr(OWNER_PK)
    address internal signer; // vm.addr(SIGNER_PK)
    address internal provider = address(0xB0B);
    address internal admin = address(0xA11CE);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);

    uint256 internal constant DISPUTE_WINDOW = 48 hours;
    uint256 internal constant DELIVERY_FLOOR = 1;
    uint256 internal constant MAX_RATE_PER_MB = 1_000_000_000_000;
    uint256 internal constant DEPOSIT = 1000e6;
    uint256 internal constant BYTES_PER_MB = 1_048_576;
    uint256 internal constant SPENDING_CAP = 500e6;
    uint64 internal expiry; // far-future capability expiry, set in setUp

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered)");
    bytes32 internal constant POOL_OPENED_SIG = keccak256("PoolOpened(bytes32,address,uint256)");

    /// @dev Mirrors `PaymentPool.PoolRedeemed` for `vm.expectEmit`.
    event PoolRedeemed(
        bytes32 indexed poolId,
        address indexed signer,
        address indexed provider,
        uint256 paid,
        uint256 bytesPaid,
        uint256 newPaidCumulative
    );

    function setUp() public {
        owner = vm.addr(OWNER_PK);
        signer = vm.addr(SIGNER_PK);

        usdc = new MockUSDC();
        bond = new MockActiveBond();
        router = new MockSettlementRouter(usdc);
        expiry = uint64(block.timestamp + 365 days);

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

    // -----------------------------------------------------------------
    // redeem — signing helpers
    // -----------------------------------------------------------------

    function _digestFor(address verifyingContract, bytes32 structHash) internal view returns (bytes32) {
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PaymentPool")),
                keccak256(bytes("1")),
                block.chainid,
                verifyingContract
            )
        );
        return keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
    }

    function _signCapabilityFor(
        address verifyingContract,
        bytes32 poolId,
        address signer_,
        uint256 spendingCap,
        uint64 exp,
        uint256 pk
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer_, spendingCap, poolId, exp));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, _digestFor(verifyingContract, structHash));
        return abi.encodePacked(r, s, v);
    }

    function _signVoucherFor(
        address verifyingContract,
        bytes32 poolId,
        address signer_,
        address provider_,
        uint256 amount,
        uint256 bytesDelivered,
        uint256 pk
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(VOUCHER_TYPEHASH, poolId, signer_, provider_, amount, bytesDelivered));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, _digestFor(verifyingContract, structHash));
        return abi.encodePacked(r, s, v);
    }

    /// @dev Owner-signed capability for `signer`, encoded as the redeem
    ///      `capability` calldata tuple `(spendingCap, expiry, ownerSig)`.
    function _cap(bytes32 poolId, uint256 spendingCap, uint64 exp) internal view returns (bytes memory) {
        return
            abi.encode(spendingCap, exp, _signCapabilityFor(address(pool), poolId, signer, spendingCap, exp, OWNER_PK));
    }

    function _voucher(bytes32 poolId, uint256 amount, uint256 bytesDelivered) internal view returns (bytes memory) {
        return _signVoucherFor(address(pool), poolId, signer, provider, amount, bytesDelivered, SIGNER_PK);
    }

    // -----------------------------------------------------------------
    // redeem — registration
    // -----------------------------------------------------------------

    function test_redeem_registersSignerOnFirstUse_thenVoucherOnly() public {
        bytes32 id = _open();

        // First redeem carries the capability; it registers `signer`.
        vm.prank(provider);
        pool.redeem(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        (uint256 cap, uint64 exp, uint256 spent) = pool.authorized(id, signer);
        assertEq(cap, SPENDING_CAP, "cap stored");
        assertEq(uint256(exp), uint256(expiry), "expiry stored");
        assertEq(spent, 300e6, "spent advanced by paid");

        // Second redeem omits the capability (empty bytes) and still works.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");

        (,, uint256 spent2) = pool.authorized(id, signer);
        assertEq(spent2, 500e6, "voucher-only redeem advanced spent");
    }

    function test_redeem_firstRedeem_revertsOnBadCapabilitySig() public {
        bytes32 id = _open();
        // The tuple advertises SPENDING_CAP but the signature is over a
        // different cap → recovery misses the owner.
        bytes memory ownerSig = _signCapabilityFor(address(pool), id, signer, 999e6, expiry, OWNER_PK);
        bytes memory badCap = abi.encode(SPENDING_CAP, expiry, ownerSig);

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidCapabilitySignature.selector);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), badCap);
    }

    function test_redeem_firstRedeem_revertsOnCapabilityForOtherOwner() public {
        bytes32 id = _open();
        // Capability signed by a non-owner key.
        bytes memory ownerSig = _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, STRANGER_PK);
        bytes memory cap = abi.encode(SPENDING_CAP, expiry, ownerSig);

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidCapabilitySignature.selector);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), cap);
    }

    // -----------------------------------------------------------------
    // redeem — cumulative min(desired, capRoom, remaining)
    // -----------------------------------------------------------------

    function test_redeem_paysCumulativeMinusPaid() public {
        bytes32 id = _open();
        // Register with a generous cap so the increment is the only bound.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        assertEq(router.totalRoutedPaid(), 300e6, "first pays full cumulative");

        vm.prank(provider);
        vm.expectEmit(true, true, true, true, address(pool));
        emit PoolRedeemed(id, signer, provider, 200e6, 20_000_000, 500e6);
        pool.redeem(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");

        (uint256 wAmount, uint256 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 500e6, "watermark tracks cumulative paid");
        assertEq(wBytes, 50_000_000);
        assertEq(router.totalRoutedPaid(), 500e6, "second pays only the increment");
    }

    function test_redeem_partialOnDrain_isRetriable() public {
        bytes32 id = _open(); // deposit 1000e6
        // Cap exceeds deposit, so the pool balance is the binding limit.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 1500e6, 1_000_000, _voucher(id, 1500e6, 1_000_000), _cap(id, 2000e6, expiry));

        (uint256 wAmount1,) = pool.watermark(id, signer, provider);
        assertEq(wAmount1, 1000e6, "drained pool pays only remaining deposit");
        assertEq(pool.getPool(id).totalRedeemed, 1000e6);

        // Owner tops up; re-presenting the SAME voucher collects the rest.
        vm.prank(owner);
        pool.topUp(id, 500e6);

        vm.prank(provider);
        pool.redeem(id, signer, provider, 1500e6, 1_000_000, _voucher(id, 1500e6, 1_000_000), "");

        (uint256 wAmount2, uint256 wBytes2) = pool.watermark(id, signer, provider);
        assertEq(wAmount2, 1500e6, "watermark advanced by paid, not cumulative; retry collects rest");
        assertEq(wBytes2, 1_000_000, "paid-proportional bytes total the full delivery once collected");
    }

    function test_redeem_capRoomBounds() public {
        bytes32 id = _open();
        // Cap 500e6 < voucher cumulative 600e6: pays only up to the cap.
        vm.prank(provider);
        pool.redeem(
            id, signer, provider, 600e6, 6_000_000, _voucher(id, 600e6, 6_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        (,, uint256 spent) = pool.authorized(id, signer);
        assertEq(spent, SPENDING_CAP, "paid capped at cap - spent");
        (uint256 wAmount,) = pool.watermark(id, signer, provider);
        assertEq(wAmount, SPENDING_CAP);

        // A further voucher fully over cap pays 0 → NothingToRedeem.
        vm.prank(provider);
        vm.expectRevert(PaymentPool.NothingToRedeem.selector);
        pool.redeem(id, signer, provider, 700e6, 7_000_000, _voucher(id, 700e6, 7_000_000), "");
    }

    function test_redeem_revertsNothingToRedeem_onStaleOrZero() public {
        bytes32 id = _open();
        vm.prank(provider);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        uint256 redeemedBefore = pool.getPool(id).totalRedeemed;

        // Re-present the same cumulative → no increment → NothingToRedeem, no state.
        vm.prank(provider);
        vm.expectRevert(PaymentPool.NothingToRedeem.selector);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), "");

        assertEq(pool.getPool(id).totalRedeemed, redeemedBefore, "stale voucher writes no state");
    }

    function test_redeem_bytesRegressionSettlesMoneyWithZeroBytes() public {
        bytes32 id = _open();
        // Voucher #1 sets the lane watermark at amount=300e6, bytesDelivered=30_000_000.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        (uint256 wAmount1, uint256 wBytes1) = pool.watermark(id, signer, provider);
        assertEq(wAmount1, 300e6);
        assertEq(wBytes1, 30_000_000);
        assertEq(router.totalRoutedPaid(), 300e6);
        assertEq(router.totalBytes(), 30_000_000);

        // Voucher #2 advances the amount (cumulative claim) but regresses
        // bytesDelivered below the watermark. Neither the amount-regression
        // guard nor the rate floor (an upper bound on bytes) catches this
        // shape. The signer already signed the higher `amount`, so the money
        // is owed and must settle; only the served-bytes credit is forfeited
        // for this redemption — it does not revert or panic.
        vm.prank(provider);
        vm.expectEmit(true, true, true, true, address(pool));
        emit PoolRedeemed(id, signer, provider, 100e6, 0, 400e6);
        pool.redeem(id, signer, provider, 400e6, 10_000_000, _voucher(id, 400e6, 10_000_000), "");

        (uint256 wAmount2, uint256 wBytes2) = pool.watermark(id, signer, provider);
        assertEq(wAmount2, 400e6, "amount watermark advances to the new cumulative");
        assertEq(wBytes2, 30_000_000, "bytes watermark holds; the regressed delta credits zero bytes");
        assertEq(router.totalRoutedPaid(), 400e6, "money settles in full despite the bytes regression");
        assertEq(router.totalBytes(), 30_000_000, "no additional bytes are credited for voucher #2");

        // Voucher #3 advances bytesDelivered past the original watermark
        // (30_000_000): byte accounting recovers across the gap left by #2.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 500e6, 45_000_000, _voucher(id, 500e6, 45_000_000), "");

        (uint256 wAmount3, uint256 wBytes3) = pool.watermark(id, signer, provider);
        assertEq(wAmount3, 500e6);
        assertEq(wBytes3, 45_000_000, "byte watermark recovers once bytesDelivered advances again");
        assertEq(router.totalRoutedPaid(), 500e6);
        assertEq(
            router.totalBytes(), 45_000_000, "the full 15_000_000-byte gap since the original watermark is credited"
        );
    }

    // -----------------------------------------------------------------
    // redeem — structural reverts
    // -----------------------------------------------------------------

    function test_redeem_revertsOnWrongProvider() public {
        bytes32 id = _open();
        // `provider != msg.sender` reverts before any registration/return-0 path.
        vm.prank(stranger);
        vm.expectRevert(PaymentPool.NotProvider.selector);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), "");
    }

    function test_redeem_revertsOnBadVoucherSig() public {
        bytes32 id = _open();
        // Voucher signed over a different amount than submitted → recovery misses signer.
        bytes memory sig = _voucher(id, 300e6, 30_000_000);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        pool.redeem(id, signer, provider, 301e6, 30_000_000, sig, _cap(id, SPENDING_CAP, expiry));
    }

    function test_redeem_revertsOnExpiredCapability() public {
        bytes32 id = _open();
        uint64 nearExpiry = uint64(block.timestamp + 1 days);
        // Register + pay while valid.
        vm.prank(provider);
        pool.redeem(
            id,
            signer,
            provider,
            300e6,
            30_000_000,
            _voucher(id, 300e6, 30_000_000),
            abi.encode(
                uint256(1000e6), nearExpiry, _signCapabilityFor(address(pool), id, signer, 1000e6, nearExpiry, OWNER_PK)
            )
        );

        // After expiry a higher voucher is transient-empty → NothingToRedeem.
        vm.warp(block.timestamp + 2 days);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.NothingToRedeem.selector);
        pool.redeem(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");
    }

    function test_redeem_revertsOnClosedPool() public {
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

        harness.forceStatus(id, PaymentPool.Status.Closed);

        bytes memory sig = _signVoucherFor(address(harness), id, signer, provider, 300e6, 30_000_000, SIGNER_PK);
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(harness), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        vm.prank(provider);
        vm.expectRevert(PaymentPool.PoolClosed.selector);
        harness.redeem(id, signer, provider, 300e6, 30_000_000, sig, cap);
    }

    // TODO(task-4): test_redeem_allowedDuringClosingBeforeDeadline needs
    // `closePool` (which sets `disputeDeadline`); added when close/reclaim land.

    // -----------------------------------------------------------------
    // redeem — bytes accounting + routing
    // -----------------------------------------------------------------

    function test_redeem_bytesPaidIsPaidProportional() public {
        bytes32 id = _open();
        // Fully-paid draw: bytesPaid == bytesDelta == bytesDelivered.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 400e6, 40_000_000, _voucher(id, 400e6, 40_000_000), _cap(id, 1000e6, expiry));
        (, uint256 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wBytes, 40_000_000, "fully paid: bytesPaid equals full bytes");

        // Cap-limited partial draw on a fresh pool: bytesPaid == mulDiv(bytesDelta, paid, desired).
        bytes32 id2 = _open();
        uint256 cumulative = 600e6;
        uint256 bytesDelivered = 6_000_000;
        uint256 paid = SPENDING_CAP; // cap-limited
        uint256 expectedBytes = Math.mulDiv(bytesDelivered, paid, cumulative);
        bytes memory sig = _signVoucherFor(address(pool), id2, signer, provider, cumulative, bytesDelivered, SIGNER_PK);
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(pool), id2, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        vm.prank(provider);
        pool.redeem(id2, signer, provider, cumulative, bytesDelivered, sig, cap);
        (, uint256 wBytes2) = pool.watermark(id2, signer, provider);
        assertEq(wBytes2, expectedBytes, "partial pay routes paid-proportional bytes");
    }

    function test_redeem_routesPaidToFeeRouter() public {
        bytes32 id = _open();
        uint256 cumulative = 400e6;
        uint256 bytesDelivered = 40_000_000;
        vm.prank(provider);
        pool.redeem(
            id,
            signer,
            provider,
            cumulative,
            bytesDelivered,
            _voucher(id, cumulative, bytesDelivered),
            _cap(id, 1000e6, expiry)
        );

        assertEq(router.callCount(), 1);
        (address op, uint256 b, uint256 amt) = router.calls(0);
        assertEq(op, provider, "routed to provider");
        assertEq(b, bytesDelivered, "routed full bytes when fully paid");
        assertEq(amt, cumulative, "routed the paid amount");
        assertEq(usdc.balanceOf(address(router)), cumulative, "router received the USDC");
    }

    function test_redeem_zeroesResidualAllowanceAfterUnderPull() public {
        UnderPullRouter under = new UnderPullRouter(usdc);
        PaymentPool p = new PaymentPool({
            usdc_: usdc,
            capacityBond_: bond,
            feeRouter_: address(under),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            admin: admin
        });
        vm.prank(owner);
        usdc.approve(address(p), type(uint256).max);
        vm.prank(owner);
        bytes32 id = p.openPool(DEPOSIT);

        bytes memory sig = _signVoucherFor(address(p), id, signer, provider, 400e6, 40_000_000, SIGNER_PK);
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(p), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        vm.prank(provider);
        p.redeem(id, signer, provider, 400e6, 40_000_000, sig, cap);

        // Router pulled amount-1; the `_route` reset must bring the standing
        // allowance back to zero.
        assertEq(usdc.allowance(address(p), address(under)), 0, "no standing allowance survives an under-pull");
    }

    function test_redeem_revertsWhenRouterPaused_thenSucceedsAfterUnpause() public {
        bytes32 id = _open();

        router.setPaused(true);
        vm.prank(provider);
        vm.expectRevert(bytes("MockSettlementRouter: paused"));
        pool.redeem(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        // The whole tx rolled back: no routing, no registration.
        assertEq(router.callCount(), 0);
        (uint256 cap,,) = pool.authorized(id, signer);
        assertEq(cap, 0, "registration rolled back with the reverted redeem");

        // After unpause the same call (capability included again) redeems cleanly.
        router.setPaused(false);
        vm.prank(provider);
        pool.redeem(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );
        assertEq(router.callCount(), 1);
        assertEq(usdc.balanceOf(address(router)), 300e6);
    }

    // -----------------------------------------------------------------
    // redeem — ERC-1271 signers
    // -----------------------------------------------------------------

    function test_redeem_acceptsErc1271VoucherSigner() public {
        // Signer is a smart-account wallet whose owner key is SIGNER_PK.
        MockERC1271Wallet wallet = new MockERC1271Wallet(signer);
        bytes32 id = _open();

        // Capability names the wallet as the signer; voucher validated via ERC-1271.
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(pool), id, address(wallet), SPENDING_CAP, expiry, OWNER_PK)
        );
        bytes memory sig = _signVoucherFor(address(pool), id, address(wallet), provider, 300e6, 30_000_000, SIGNER_PK);

        vm.prank(provider);
        pool.redeem(id, address(wallet), provider, 300e6, 30_000_000, sig, cap);

        (uint256 wAmount,) = pool.watermark(id, address(wallet), provider);
        assertEq(wAmount, 300e6, "ERC-1271 voucher signer accepted");
    }

    function test_redeem_acceptsErc1271CapabilityOwner() public {
        // Pool owner is a smart-account wallet whose owner key is OWNER_PK.
        MockERC1271Wallet ownerWallet = new MockERC1271Wallet(owner);
        usdc.transfer(address(ownerWallet), 10_000e6);
        vm.prank(address(ownerWallet));
        usdc.approve(address(pool), type(uint256).max);
        vm.prank(address(ownerWallet));
        bytes32 id = pool.openPool(DEPOSIT);

        // Capability signed by the wallet's owner key; verified via ERC-1271 against the wallet.
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        bytes memory sig = _signVoucherFor(address(pool), id, signer, provider, 300e6, 30_000_000, SIGNER_PK);

        vm.prank(provider);
        pool.redeem(id, signer, provider, 300e6, 30_000_000, sig, cap);

        (uint256 cap2, uint64 exp2,) = pool.authorized(id, signer);
        assertEq(cap2, SPENDING_CAP, "ERC-1271 capability owner accepted");
        assertEq(uint256(exp2), uint256(expiry));
    }

    // -----------------------------------------------------------------
    // redeem — rate floor (ADR 003 § Rate-floor enforcement)
    // -----------------------------------------------------------------

    function test_redeem_rateFloor_boundaryExact() public {
        bytes32 id = _open();
        uint256 amount = 100;
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;

        bytes memory sig = _voucher(id, amount, maxBytes);
        bytes memory cap = _cap(id, 1000e6, expiry);
        vm.prank(provider);
        pool.redeem(id, signer, provider, amount, maxBytes, sig, cap);
        (, uint256 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wBytes, maxBytes, "exact boundary passes");

        // One byte past the ceiling reverts on a fresh pool/signer.
        bytes32 id2 = _open();
        bytes memory sig2 = _signVoucherFor(address(pool), id2, signer, provider, amount, maxBytes + 1, SIGNER_PK);
        bytes memory cap2 = abi.encode(
            uint256(1000e6), expiry, _signCapabilityFor(address(pool), id2, signer, 1000e6, expiry, OWNER_PK)
        );
        vm.prank(provider);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentPool.RateFloorViolation.selector, amount, maxBytes + 1, DELIVERY_FLOOR)
        );
        pool.redeem(id2, signer, provider, amount, maxBytes + 1, sig2, cap2);
    }

    function test_redeem_rateFloor_revertsOnInflatedBytes() public {
        bytes32 id = _open();
        // 1 base unit permits at most BYTES_PER_MB bytes; a near-max byte count
        // must revert cleanly (Math.mulDiv ceiling), never arithmetic-panic.
        uint256 huge = type(uint256).max;
        bytes memory sig = _voucher(id, 1, huge);
        bytes memory cap = _cap(id, 1000e6, expiry);
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateFloorViolation.selector, 1, huge, DELIVERY_FLOOR));
        pool.redeem(id, signer, provider, 1, huge, sig, cap);
    }

    function test_redeem_rateFloor_honestPathUnaffected() public {
        bytes32 id = _open();
        // ~38 MB for 390 base units at the $0.01/GB market rate clears the floor by ~10x.
        vm.prank(provider);
        pool.redeem(id, signer, provider, 390, 40_000_000, _voucher(id, 390, 40_000_000), _cap(id, 1000e6, expiry));
        (, uint256 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wBytes, 40_000_000, "honest traffic clears the floor");
    }

    function test_redeem_rateFloor_routedBytesBoundedByPaidAmount() public {
        bytes32 id = _open();
        uint256 amount = 100;
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;
        vm.prank(provider);
        pool.redeem(id, signer, provider, amount, maxBytes, _voucher(id, amount, maxBytes), _cap(id, 1000e6, expiry));

        (, uint256 b, uint256 amt) = router.calls(0);
        assertEq(amt, amount);
        assertEq(b, maxBytes, "routed bytes never exceed amount * BYTES_PER_MB / floor");
    }

    function testFuzz_redeem_rateFloor_revertIffBelowFloor(uint256 amount, uint256 bytesDelivered) public {
        amount = bound(amount, 1, DEPOSIT);
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;
        bytesDelivered = bound(bytesDelivered, 0, 2 * maxBytes);

        bytes32 id = _open();
        bytes memory sig = _voucher(id, amount, bytesDelivered);
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        if (bytesDelivered > maxBytes) {
            vm.expectRevert(
                abi.encodeWithSelector(PaymentPool.RateFloorViolation.selector, amount, bytesDelivered, DELIVERY_FLOOR)
            );
            pool.redeem(id, signer, provider, amount, bytesDelivered, sig, cap);
        } else {
            pool.redeem(id, signer, provider, amount, bytesDelivered, sig, cap);
            (uint256 wAmount,) = pool.watermark(id, signer, provider);
            assertEq(wAmount, amount);
        }
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
