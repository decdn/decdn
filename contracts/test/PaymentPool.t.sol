// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm, VmSafe } from "forge-std/Vm.sol";
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

/// @notice Router that pulls one wei LESS than approved. Exercised by the
///         `redeem` under-pull tests below.
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
    /// Mirrors the deploy script's dormant launch `minDeposit` (bounds
    /// [0, $100]; 0 = any non-zero deposit opens). Tests that exercise an
    /// armed floor raise it via `setMinDeposit` or construct with `ARMED_MIN`.
    uint64 internal constant MIN_DEPOSIT = 0;
    uint64 internal constant ARMED_MIN = 5e6;
    uint64 internal constant MIN_DEPOSIT_CEILING = 100e6;
    uint256 internal constant MAX_RATE_PER_MB = 1000;
    uint64 internal constant DEPOSIT = 1000e6;
    uint256 internal constant BYTES_PER_MB = 1_048_576;
    uint64 internal constant SPENDING_CAP = 500e6;
    uint64 internal expiry; // far-future capability expiry, set in setUp
    /// Vouchers-per-call for the Task 7 marginal-gas benchmark pair
    /// (`test_redeemMany_gas_NVouchers` / `test_redeemMany_gas_NPlus1Vouchers`).
    uint256 internal constant REDEEM_MANY_GAS_BENCHMARK_N = 10;

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );
    /// @dev Mirrors `PaymentPool.CHUNK_BYTES` / `BYTES_PER_MB` (both internal).
    uint256 internal constant CHUNK_BYTES = 1_048_576;
    /// @dev Mirrors the ADR 003 `MAX_CHAIN_LENGTH`: the highest chain index,
    ///      which is the `uint8` domain's own ceiling.
    uint8 internal constant MAX_CHAIN_LENGTH = 255;
    bytes32 internal constant POOL_OPENED_SIG = keccak256("PoolOpened(bytes32,address,uint256)");

    /// @dev Mirrors `PaymentPool.PoolRedeemed` for `vm.expectEmit`.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, PaymentPool.LaneSettled[] lanes);

    /// @dev A one-lane `PoolRedeemed` payload.
    function _settled(address signer_, uint64 newPaidCumulative, uint64 bytesPaid)
        internal
        pure
        returns (PaymentPool.LaneSettled[] memory lanes)
    {
        lanes = new PaymentPool.LaneSettled[](1);
        lanes[0] =
            PaymentPool.LaneSettled({ signer: signer_, newPaidCumulative: newPaidCumulative, bytesPaid: bytesPaid });
    }

    /// @dev Mirrors for `vm.expectEmit` — close/reclaim + governance setters.
    event PoolCloseInitiated(bytes32 indexed poolId, address indexed owner, uint256 disputeDeadline);
    event PoolReclaimed(bytes32 indexed poolId, address indexed owner, uint256 ownerRefund);
    event DisputeWindowUpdated(uint256 oldValue, uint256 newValue);
    event RateBoundsUpdated(uint256 newDeliveryFloor);
    event MinDepositUpdated(uint64 oldValue, uint64 newValue);

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
            minDeposit_: MIN_DEPOSIT,
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
        // `minDeposit == 0` (the dormant launch value) preserves the
        // any-non-zero-deposit-opens behavior.
        vm.prank(owner);
        bytes32 id = pool.openPool(1);
        assertEq(pool.getPool(id).deposit, 1, "a one-base-unit deposit must open");
    }

    function test_openPool_enforcesArmedMinDeposit() public {
        vm.prank(admin);
        pool.setMinDeposit(ARMED_MIN);

        vm.prank(owner);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentPool.BelowMinDeposit.selector, uint256(ARMED_MIN - 1), uint256(ARMED_MIN))
        );
        pool.openPool(ARMED_MIN - 1);

        // Exactly the minimum opens; above it too.
        vm.prank(owner);
        bytes32 id = pool.openPool(ARMED_MIN);
        assertEq(pool.getPool(id).deposit, ARMED_MIN, "exactly minDeposit must open");
        vm.prank(owner);
        pool.openPool(ARMED_MIN + 1);
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
            minDeposit_: MIN_DEPOSIT,
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
            minDeposit_: MIN_DEPOSIT,
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

    function test_openPool_minDepositEnforcedOnReceivedNotRequested() public {
        FeeOnTransferUSDC feeUsdc = new FeeOnTransferUSDC();
        MockSettlementRouter feeRouter = new MockSettlementRouter(feeUsdc);
        PaymentPool feePool = new PaymentPool({
            usdc_: feeUsdc,
            capacityBond_: bond,
            feeRouter_: address(feeRouter),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            minDeposit_: ARMED_MIN,
            admin: admin
        });
        feeUsdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        feeUsdc.approve(address(feePool), type(uint256).max);
        feeUsdc.setFeeBps(100);

        // The requested deposit meets the minimum, but the credited delta
        // falls below it once the transfer fee shaves it — the floor reads
        // `received`, so the open reverts.
        uint256 received = ARMED_MIN - (uint256(ARMED_MIN) * 100 / 10_000);
        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.BelowMinDeposit.selector, received, uint256(ARMED_MIN)));
        feePool.openPool(ARMED_MIN);
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
            minDeposit_: MIN_DEPOSIT,
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
            minDeposit_: MIN_DEPOSIT,
            admin: admin
        });
        feeUsdc.transfer(owner, 100_000e6);
        vm.prank(owner);
        feeUsdc.approve(address(feePool), type(uint256).max);

        vm.prank(owner);
        bytes32 id = feePool.openPool(DEPOSIT);
        assertEq(feePool.getPool(id).deposit, DEPOSIT, "opened at full value (fee not yet armed)");

        feeUsdc.setFeeBps(100);
        uint64 top = 500e6;
        uint64 credited = top - (top * 100 / 10_000);
        vm.prank(owner);
        feePool.topUp(id, top);

        assertEq(feePool.getPool(id).deposit, DEPOSIT + credited, "top-up credits its delta");
        assertEq(feeUsdc.balanceOf(address(feePool)), DEPOSIT + credited, "and it matches the real balance");
    }

    function test_topUp_unaffectedByMinDeposit() public {
        // The Sybil floor prices minting a NEW pool identity; adding to an
        // existing pool stays free of it, down to a single base unit.
        vm.prank(admin);
        pool.setMinDeposit(ARMED_MIN);
        vm.prank(owner);
        bytes32 id = pool.openPool(ARMED_MIN);

        vm.prank(owner);
        pool.topUp(id, 1);
        assertEq(pool.getPool(id).deposit, ARMED_MIN + 1, "a sub-minimum top-up must credit");
    }

    // -----------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroAddress() public {
        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(IERC20(address(0)), bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin);

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(
            usdc, ICapacityBondActivity(address(0)), address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin
        );

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(usdc, bond, address(0), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin);

        vm.expectRevert(PaymentPool.ZeroAddress.selector);
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, address(0));
    }

    function test_constructor_revertsOnEoaFeeRouter() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterHasNoCode.selector, stranger));
        new PaymentPool(usdc, bond, stranger, DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin);
    }

    function test_constructor_revertsOnRouterMissingPausedView() public {
        NoPauseRouter bad = new NoPauseRouter();
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterMissingPausedView.selector, address(bad)));
        new PaymentPool(usdc, bond, address(bad), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin);

        NonBoolPauseRouter bad2 = new NonBoolPauseRouter();
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.FeeRouterMissingPausedView.selector, address(bad2)));
        new PaymentPool(usdc, bond, address(bad2), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT, admin);
    }

    function test_constructor_revertsOnDisputeWindowOutOfBounds() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector, uint256(1 hours), uint256(48 hours), uint256(72 hours)
            )
        );
        new PaymentPool(usdc, bond, address(router), 1 hours, DELIVERY_FLOOR, MIN_DEPOSIT, admin);
    }

    function test_constructor_revertsOnRateFloorAboveWireCap() public {
        uint256 badFloor = MAX_RATE_PER_MB + 1;
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, badFloor));
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, badFloor, MIN_DEPOSIT, admin);
    }

    function test_constructor_revertsOnRateFloorBelowMinimum() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, uint256(0)));
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, 0, MIN_DEPOSIT, admin);
    }

    function test_constructor_revertsOnMinDepositAboveCeiling() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector,
                uint256(MIN_DEPOSIT_CEILING) + 1,
                uint256(0),
                uint256(MIN_DEPOSIT_CEILING)
            )
        );
        new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT_CEILING + 1, admin);
    }

    function test_constructor_armsMinDepositUpToCeiling() public {
        PaymentPool armed =
            new PaymentPool(usdc, bond, address(router), DISPUTE_WINDOW, DELIVERY_FLOOR, MIN_DEPOSIT_CEILING, admin);
        assertEq(armed.minDeposit(), MIN_DEPOSIT_CEILING, "constructor stores the launch minimum");

        vm.prank(owner);
        usdc.approve(address(armed), type(uint256).max);
        vm.prank(owner);
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.BelowMinDeposit.selector, uint256(MIN_DEPOSIT_CEILING) - 1, uint256(MIN_DEPOSIT_CEILING)
            )
        );
        armed.openPool(MIN_DEPOSIT_CEILING - 1);
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
        uint64 spendingCap,
        uint64 exp,
        uint256 pk
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer_, spendingCap, poolId, exp));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, _digestFor(verifyingContract, structHash));
        return abi.encodePacked(r, s, v);
    }

    /// @dev A voucher signature in the EIP-2098 compact form the contract
    ///      takes: `vs` is `s` with the recovery bit folded into its top bit.
    ///      `vm.sign` always yields a low-`s` signature, which is what makes
    ///      that bit free.
    struct Sig {
        bytes32 r;
        bytes32 vs;
    }

    function _signVoucherFor(
        address verifyingContract,
        bytes32 poolId,
        address signer_,
        address provider_,
        uint64 amount,
        uint64 bytesDelivered,
        uint256 pk
    ) internal view returns (Sig memory) {
        return _signVoucherChained(
            verifyingContract, poolId, signer_, provider_, amount, bytesDelivered, bytes32(0), 0, pk
        );
    }

    /// @dev The full seven-field voucher signature, including the two `PayWord`
    ///      fields. `_signVoucherFor` is the sealed case of this — a zero root
    ///      at a zero price — which is what every pre-`PayWord` test in this
    ///      file now exercises.
    function _signVoucherChained(
        address verifyingContract,
        bytes32 poolId,
        address signer_,
        address provider_,
        uint64 amount,
        uint64 bytesDelivered,
        bytes32 chainRoot,
        uint64 chunkPrice,
        uint256 pk
    ) internal view returns (Sig memory) {
        bytes32 digest;
        // Scoped so `structHash` dies before `vm.sign`'s three returns land:
        // this file compiles without the IR pipeline, and nine parameters plus
        // an eight-field `abi.encode` is already at the stack limit.
        {
            bytes32 structHash = keccak256(
                abi.encode(
                    VOUCHER_TYPEHASH, poolId, signer_, provider_, amount, bytesDelivered, chainRoot, uint256(chunkPrice)
                )
            );
            digest = _digestFor(verifyingContract, structHash);
        }
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return Sig({ r: r, vs: bytes32(uint256(s) | (uint256(v - 27) << 255)) });
    }

    /// @dev A [`PaymentPool.LaneVoucher`] from its presented fields plus an
    ///      already-produced signature, so a test can present values that
    ///      differ from the ones it signed over.
    function _laneOf(address signer_, uint64 amount, uint64 bytesDelivered, Sig memory sig)
        internal
        pure
        returns (PaymentPool.LaneVoucher memory)
    {
        return _laneOfChained(signer_, amount, bytesDelivered, sig, bytes32(0), bytes32(0), 0);
    }

    /// @dev A lane presenting a `PayWord` chain: `chainRoot` is what the signer
    ///      committed, `preimage` the released value, and `chainMeter` the
    ///      packed `(chunkPrice, chainIndex)` word. `_laneOf` is the sealed case
    ///      — all three zero — which resolves to exactly `amount` and walks
    ///      nothing.
    function _laneOfChained(
        address signer_,
        uint64 amount,
        uint64 bytesDelivered,
        Sig memory sig,
        bytes32 chainRoot,
        bytes32 preimage,
        uint256 chainMeter
    ) internal pure returns (PaymentPool.LaneVoucher memory) {
        return PaymentPool.LaneVoucher({
            signer: signer_,
            cumulative: amount,
            bytesDelivered: bytesDelivered,
            r: sig.r,
            vs: sig.vs,
            chainRoot: chainRoot,
            preimage: preimage,
            chainMeter: chainMeter
        });
    }

    /// @dev Pack `chunkPrice` (bytes 23..=30) and `chainIndex` (the low byte)
    ///      into the single `chainMeter` word, leaving the reserved upper span
    ///      zero.
    function _meter(uint64 chunkPrice, uint8 chainIndex) internal pure returns (uint256) {
        return (uint256(chunkPrice) << 8) | uint256(chainIndex);
    }

    /// @dev `keccak^(MAX_CHAIN_LENGTH - index)(seed)` — the value a payer
    ///      releases at `index`. At `index == 0` this is the chain root itself,
    ///      which is why a real-root voucher settles its own `cumulative` by
    ///      passing the root as its own preimage.
    function _preimage(bytes32 seed, uint8 index) internal pure returns (bytes32 acc) {
        acc = seed;
        for (uint256 i = 0; i < MAX_CHAIN_LENGTH - uint256(index); i++) {
            acc = keccak256(abi.encodePacked(acc));
        }
    }

    /// @dev `keccak^MAX_CHAIN_LENGTH(seed)` — the head the payer signs into the
    ///      voucher.
    function _chainRoot(bytes32 seed) internal pure returns (bytes32) {
        return _preimage(seed, 0);
    }

    /// @dev Owner-signed capability for `signer`, packed as the
    ///      `(spendingCap, expiry, ownerSig)` triple `_redeemOne` unpacks into
    ///      a `CapabilityReg`.
    function _cap(bytes32 poolId, uint64 spendingCap, uint64 exp) internal view returns (bytes memory) {
        return
            abi.encode(spendingCap, exp, _signCapabilityFor(address(pool), poolId, signer, spendingCap, exp, OWNER_PK));
    }

    function _voucher(bytes32 poolId, uint64 amount, uint64 bytesDelivered) internal view returns (Sig memory) {
        return _signVoucherFor(address(pool), poolId, signer, provider, amount, bytesDelivered, SIGNER_PK);
    }

    /// @dev A one-lane batch. `redeemMany` is the only redemption entry point,
    ///      so a per-lane test states its lane as a one-entry batch; this keeps
    ///      that at one line. The third argument is the payee the voucher
    ///      names — the contract reads the payee off `msg.sender`, so it is
    ///      here only to keep a lane test reading like the voucher it signs.
    ///      Returns `totalPaid`, which is `0` for a transient-empty voucher.
    function _redeemOne(
        bytes32 poolId,
        address signer_,
        address payee,
        uint64 amount,
        uint64 bytesDelivered,
        Sig memory sig,
        bytes memory capability
    ) internal returns (uint256 totalPaid) {
        return _redeemOneOn(pool, poolId, signer_, payee, amount, bytesDelivered, sig, capability);
    }

    /// @dev `_redeemOne` against a pool other than the shared fixture.
    function _redeemOneOn(
        PaymentPool target,
        bytes32 poolId,
        address signer_,
        address,
        uint64 amount,
        uint64 bytesDelivered,
        Sig memory sig,
        bytes memory capability
    ) internal returns (uint256 totalPaid) {
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](capability.length == 0 ? 0 : 1);
        if (capability.length != 0) {
            (uint64 spendingCap, uint64 exp, bytes memory ownerSig) = abi.decode(capability, (uint64, uint64, bytes));
            caps[0] = PaymentPool.CapabilityReg({
                signer: signer_, spendingCap: spendingCap, expiry: exp, ownerSig: ownerSig
            });
        }
        PaymentPool.LaneVoucher[] memory v = new PaymentPool.LaneVoucher[](1);
        v[0] = _laneOf(signer_, amount, bytesDelivered, sig);
        return target.redeemMany(_batch(poolId, caps, v));
    }

    /// @dev A single [`PaymentPool.PoolBatch`], wrapped as the one-element
    ///      array `redeemMany` takes.
    function _batch(bytes32 poolId, PaymentPool.CapabilityReg[] memory caps, PaymentPool.LaneVoucher[] memory vouchers)
        internal
        pure
        returns (PaymentPool.PoolBatch[] memory batches)
    {
        batches = new PaymentPool.PoolBatch[](1);
        batches[0] = PaymentPool.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: vouchers });
    }

    // -----------------------------------------------------------------
    // redeem — registration
    // -----------------------------------------------------------------

    function test_redeem_registersSignerOnFirstUse_thenVoucherOnly() public {
        bytes32 id = _open();

        // First redeem carries the capability; it registers `signer`.
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        (uint64 cap, uint64 exp, uint64 spent) = pool.authorized(id, signer);
        assertEq(cap, SPENDING_CAP, "cap stored");
        assertEq(uint256(exp), uint256(expiry), "expiry stored");
        assertEq(spent, 300e6, "spent advanced by paid");

        // Second redeem omits the capability (empty bytes) and still works.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");

        (,, uint64 spent2) = pool.authorized(id, signer);
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
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), badCap);
    }

    function test_redeem_firstRedeem_revertsOnCapabilityForOtherOwner() public {
        bytes32 id = _open();
        // Capability signed by a non-owner key.
        bytes memory ownerSig = _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, STRANGER_PK);
        bytes memory cap = abi.encode(SPENDING_CAP, expiry, ownerSig);

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidCapabilitySignature.selector);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), cap);
    }

    // -----------------------------------------------------------------
    // redeem — cumulative min(desired, capRoom, remaining)
    // -----------------------------------------------------------------

    function test_redeem_paysCumulativeMinusPaid() public {
        bytes32 id = _open();
        // Register with a generous cap so the increment is the only bound.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        assertEq(router.totalRoutedPaid(), 300e6, "first pays full cumulative");

        vm.prank(provider);
        vm.expectEmit(true, true, true, true, address(pool));
        emit PoolRedeemed(id, provider, _settled(signer, 500e6, 20_000_000));
        _redeemOne(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 500e6, "watermark tracks cumulative paid");
        assertEq(wBytes, 50_000_000);
        assertEq(router.totalRoutedPaid(), 500e6, "second pays only the increment");
    }

    function test_redeem_partialOnDrain_isRetriable() public {
        bytes32 id = _open(); // deposit 1000e6
        // Cap exceeds deposit, so the pool balance is the binding limit.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 1500e6, 1_000_000, _voucher(id, 1500e6, 1_000_000), _cap(id, 2000e6, expiry));

        (uint64 wAmount1,) = pool.watermark(id, signer, provider);
        assertEq(wAmount1, 1000e6, "drained pool pays only remaining deposit");
        assertEq(pool.getPool(id).totalRedeemed, 1000e6);

        // Owner tops up; re-presenting the SAME voucher collects the rest.
        vm.prank(owner);
        pool.topUp(id, 500e6);

        vm.prank(provider);
        _redeemOne(id, signer, provider, 1500e6, 1_000_000, _voucher(id, 1500e6, 1_000_000), "");

        (uint64 wAmount2, uint64 wBytes2) = pool.watermark(id, signer, provider);
        assertEq(wAmount2, 1500e6, "watermark advanced by paid, not cumulative; retry collects rest");
        assertEq(wBytes2, 1_000_000, "paid-proportional bytes total the full delivery once collected");
    }

    function test_redeem_capRoomBounds() public {
        bytes32 id = _open();
        // Cap 500e6 < voucher cumulative 600e6: pays only up to the cap.
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 600e6, 6_000_000, _voucher(id, 600e6, 6_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        (,, uint64 spent) = pool.authorized(id, signer);
        assertEq(spent, SPENDING_CAP, "paid capped at cap - spent");
        (uint64 wAmount,) = pool.watermark(id, signer, provider);
        assertEq(wAmount, SPENDING_CAP);

        // A further voucher fully over cap is transient-empty: it pays 0 and
        // writes nothing, rather than reverting.
        uint256 callsBefore = router.callCount();
        vm.prank(provider);
        uint256 paid = _redeemOne(id, signer, provider, 700e6, 7_000_000, _voucher(id, 700e6, 7_000_000), "");
        assertEq(paid, 0, "cap-reached lane pays nothing");
        assertEq(router.callCount(), callsBefore, "and never reaches the router");
    }

    function test_redeem_staleOrZeroVoucherPaysNothing() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        uint256 redeemedBefore = pool.getPool(id).totalRedeemed;

        // Re-present the same cumulative → no increment → pays 0, no state.
        vm.prank(provider);
        uint256 paid = _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), "");
        assertEq(paid, 0, "a stale voucher pays nothing");

        assertEq(pool.getPool(id).totalRedeemed, redeemedBefore, "stale voucher writes no state");
    }

    function test_redeem_bytesRegressionSettlesMoneyWithZeroBytes() public {
        bytes32 id = _open();
        // Voucher #1 sets the lane watermark at amount=300e6, bytesDelivered=30_000_000.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, 1000e6, expiry));

        (uint64 wAmount1, uint64 wBytes1) = pool.watermark(id, signer, provider);
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
        emit PoolRedeemed(id, provider, _settled(signer, 400e6, 0));
        _redeemOne(id, signer, provider, 400e6, 10_000_000, _voucher(id, 400e6, 10_000_000), "");

        (uint64 wAmount2, uint64 wBytes2) = pool.watermark(id, signer, provider);
        assertEq(wAmount2, 400e6, "amount watermark advances to the new cumulative");
        assertEq(wBytes2, 30_000_000, "bytes watermark holds; the regressed delta credits zero bytes");
        assertEq(router.totalRoutedPaid(), 400e6, "money settles in full despite the bytes regression");
        assertEq(router.totalBytes(), 30_000_000, "no additional bytes are credited for voucher #2");

        // Voucher #3 advances bytesDelivered past the original watermark
        // (30_000_000): byte accounting recovers across the gap left by #2.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 500e6, 45_000_000, _voucher(id, 500e6, 45_000_000), "");

        (uint64 wAmount3, uint64 wBytes3) = pool.watermark(id, signer, provider);
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

    /// A voucher names its payee in the signed payload, and the contract
    /// rebuilds that payload with `msg.sender`. A node presenting another
    /// node's voucher therefore recovers the wrong signer, so the wrong-payee
    /// case and the forged-signature case are one and the same revert — there
    /// is no separate payee field to disagree with.
    function test_redeem_revertsWhenCallerIsNotTheVouchersPayee() public {
        bytes32 id = _open();
        vm.prank(stranger);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );
    }

    function test_redeem_revertsOnBadVoucherSig() public {
        bytes32 id = _open();
        // Voucher signed over a different amount than submitted → recovery misses signer.
        Sig memory sig = _voucher(id, 300e6, 30_000_000);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        _redeemOne(id, signer, provider, 301e6, 30_000_000, sig, _cap(id, SPENDING_CAP, expiry));
    }

    function test_redeem_expiredCapabilityPaysNothing() public {
        bytes32 id = _open();
        uint64 nearExpiry = uint64(block.timestamp + 1 days);
        // Register + pay while valid.
        vm.prank(provider);
        _redeemOne(
            id,
            signer,
            provider,
            300e6,
            30_000_000,
            _voucher(id, 300e6, 30_000_000),
            abi.encode(
                uint64(1000e6), nearExpiry, _signCapabilityFor(address(pool), id, signer, 1000e6, nearExpiry, OWNER_PK)
            )
        );

        // After expiry a higher voucher is transient-empty: pays 0, no revert.
        vm.warp(block.timestamp + 2 days);
        vm.prank(provider);
        uint256 paid = _redeemOne(id, signer, provider, 500e6, 50_000_000, _voucher(id, 500e6, 50_000_000), "");
        assertEq(paid, 0, "an expired capability pays nothing");
    }

    function test_redeem_revertsOnClosedPool() public {
        PaymentPoolHarness harness = new PaymentPoolHarness({
            usdc_: usdc,
            capacityBond_: bond,
            feeRouter_: address(router),
            disputeWindow_: DISPUTE_WINDOW,
            deliveryFloor_: DELIVERY_FLOOR,
            minDeposit_: MIN_DEPOSIT,
            admin: admin
        });
        vm.prank(owner);
        usdc.approve(address(harness), type(uint256).max);
        vm.prank(owner);
        bytes32 id = harness.openPool(DEPOSIT);

        harness.forceStatus(id, PaymentPool.Status.Closed);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(harness), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = _laneOf(
            signer,
            300e6,
            30_000_000,
            _signVoucherFor(address(harness), id, signer, provider, 300e6, 30_000_000, SIGNER_PK)
        );
        vm.prank(provider);
        vm.expectRevert(PaymentPool.PoolClosed.selector);
        harness.redeemMany(_batch(id, caps, vouchers));
    }

    function test_redeem_allowedDuringClosingBeforeDeadline() public {
        bytes32 id = _open();

        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 100e6, 10_000_000, _voucher(id, 100e6, 10_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        vm.prank(owner);
        pool.closePool(id);

        // Still inside the grace window: redeem succeeds.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), "");
        assertEq(pool.getPool(id).totalRedeemed, 200e6);

        // Past the deadline: redeem reverts PoolClosed.
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.PoolClosed.selector);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), "");
    }

    // -----------------------------------------------------------------
    // redeem — bytes accounting + routing
    // -----------------------------------------------------------------

    function test_redeem_bytesPaidIsPaidProportional() public {
        bytes32 id = _open();
        // Fully-paid draw: bytesPaid == bytesDelta == bytesDelivered.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 400e6, 40_000_000, _voucher(id, 400e6, 40_000_000), _cap(id, 1000e6, expiry));
        (, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wBytes, 40_000_000, "fully paid: bytesPaid equals full bytes");

        // Cap-limited partial draw on a fresh pool: bytesPaid == mulDiv(bytesDelta, paid, desired).
        bytes32 id2 = _open();
        uint64 cumulative = 600e6;
        uint64 bytesDelivered = 6_000_000;
        uint64 paid = SPENDING_CAP; // cap-limited
        uint256 expectedBytes = Math.mulDiv(bytesDelivered, paid, cumulative);
        Sig memory sig = _signVoucherFor(address(pool), id2, signer, provider, cumulative, bytesDelivered, SIGNER_PK);
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(pool), id2, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        vm.prank(provider);
        _redeemOne(id2, signer, provider, cumulative, bytesDelivered, sig, cap);
        (, uint64 wBytes2) = pool.watermark(id2, signer, provider);
        assertEq(wBytes2, expectedBytes, "partial pay routes paid-proportional bytes");
    }

    function test_redeem_routesPaidToFeeRouter() public {
        bytes32 id = _open();
        uint64 cumulative = 400e6;
        uint64 bytesDelivered = 40_000_000;
        vm.prank(provider);
        _redeemOne(
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
            minDeposit_: MIN_DEPOSIT,
            admin: admin
        });
        vm.prank(owner);
        usdc.approve(address(p), type(uint256).max);
        vm.prank(owner);
        bytes32 id = p.openPool(DEPOSIT);

        Sig memory sig = _signVoucherFor(address(p), id, signer, provider, 400e6, 40_000_000, SIGNER_PK);
        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(p), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        );
        vm.prank(provider);
        _redeemOneOn(p, id, signer, provider, 400e6, 40_000_000, sig, cap);

        // Router pulled amount-1; the `_route` reset must bring the standing
        // allowance back to zero.
        assertEq(usdc.allowance(address(p), address(under)), 0, "no standing allowance survives an under-pull");
    }

    function test_redeem_revertsWhenRouterPaused_thenSucceedsAfterUnpause() public {
        bytes32 id = _open();

        router.setPaused(true);
        vm.prank(provider);
        vm.expectRevert(bytes("MockSettlementRouter: paused"));
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        // The whole tx rolled back: no routing, no registration.
        assertEq(router.callCount(), 0);
        (uint256 cap,,) = pool.authorized(id, signer);
        assertEq(cap, 0, "registration rolled back with the reverted redeem");

        // After unpause the same call (capability included again) redeems cleanly.
        router.setPaused(false);
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );
        assertEq(router.callCount(), 1);
        assertEq(usdc.balanceOf(address(router)), 300e6);
    }

    // -----------------------------------------------------------------
    // redeem — ERC-1271 signers
    // -----------------------------------------------------------------

    /// A voucher signer must be an EOA. The compact `(r, vs)` pair recovers
    /// through `ecrecover` alone, so a smart account cannot sign vouchers even
    /// when its owner key produced a signature it would honour under ERC-1271:
    /// recovery yields the owner key's address, not the wallet's. This is the
    /// cost of the static calldata layout, and it is paid deliberately — the
    /// narrower lane is what lowers the smallest redeemable lane balance.
    function test_redeem_rejectsErc1271VoucherSigner() public {
        MockERC1271Wallet wallet = new MockERC1271Wallet(signer);
        bytes32 id = _open();

        bytes memory cap = abi.encode(
            SPENDING_CAP, expiry, _signCapabilityFor(address(pool), id, address(wallet), SPENDING_CAP, expiry, OWNER_PK)
        );
        Sig memory sig = _signVoucherFor(address(pool), id, address(wallet), provider, 300e6, 30_000_000, SIGNER_PK);

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        _redeemOne(id, address(wallet), provider, 300e6, 30_000_000, sig, cap);
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
        Sig memory sig = _signVoucherFor(address(pool), id, signer, provider, 300e6, 30_000_000, SIGNER_PK);

        vm.prank(provider);
        _redeemOne(id, signer, provider, 300e6, 30_000_000, sig, cap);

        (uint64 cap2, uint64 exp2,) = pool.authorized(id, signer);
        assertEq(cap2, SPENDING_CAP, "ERC-1271 capability owner accepted");
        assertEq(uint256(exp2), uint256(expiry));
    }

    // -----------------------------------------------------------------
    // redeem — soft rate floor (ADR 003 § Rate-floor enforcement)
    //
    // A sub-floor voucher never reverts. It settles its `cumulative` USDC and
    // credits only `min(bytesDelivered, cumulative * BYTES_PER_MB /
    // deliveryFloor)` bytes, so cheap bytes cannot inflate the served-bytes
    // governance weight (ADR 036) yet the payment still clears.
    // -----------------------------------------------------------------

    function test_redeem_softFloor_creditsFullBytesAtBoundary() public {
        bytes32 id = _open();
        uint64 amount = 100;
        uint64 maxBytes = uint64(uint256(amount) * BYTES_PER_MB / DELIVERY_FLOOR);

        vm.prank(provider);
        _redeemOne(id, signer, provider, amount, maxBytes, _voucher(id, amount, maxBytes), _cap(id, 1000e6, expiry));
        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, amount, "settles the full cumulative at the boundary");
        assertEq(wBytes, maxBytes, "credits every delivered byte at the boundary");
    }

    function test_redeem_softFloor_clampsBytesButSettlesCumulative() public {
        bytes32 id = _open();
        uint64 amount = 100;
        uint64 maxBytes = uint64(uint256(amount) * BYTES_PER_MB / DELIVERY_FLOOR);

        // One byte over the ceiling no longer reverts: the voucher pays its
        // full `amount` and credits exactly the ceiling, not the claim.
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, amount, maxBytes + 1, _voucher(id, amount, maxBytes + 1), _cap(id, 1000e6, expiry)
        );
        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, amount, "pays the full cumulative despite the sub-floor rate");
        assertEq(wBytes, maxBytes, "credited bytes clamped to the floor ceiling");

        (, uint256 routedBytes, uint256 routedAmount) = router.calls(0);
        assertEq(routedAmount, amount, "routes the full cumulative");
        assertEq(routedBytes, maxBytes, "routes only the clamped byte count into the vote-weight counter");
    }

    function test_redeem_softFloor_clampsWildlyInflatedBytes() public {
        bytes32 id = _open();
        // 1 base unit justifies at most BYTES_PER_MB bytes at floor 1. The
        // widest byte count a voucher can carry settles the 1 unit and credits
        // exactly that ceiling — the inflation is neutralised, not rejected.
        uint64 huge = type(uint64).max;
        uint64 ceiling = uint64(uint256(1) * BYTES_PER_MB / DELIVERY_FLOOR);
        vm.prank(provider);
        _redeemOne(id, signer, provider, 1, huge, _voucher(id, 1, huge), _cap(id, 1000e6, expiry));
        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 1, "settles the cumulative");
        assertEq(wBytes, ceiling, "credits only floor-justified bytes");
    }

    function test_redeem_softFloor_honestPathUnaffected() public {
        bytes32 id = _open();
        // ~38 MB for 390 base units at the $0.01/GB market rate clears the floor by ~10x.
        vm.prank(provider);
        _redeemOne(id, signer, provider, 390, 40_000_000, _voucher(id, 390, 40_000_000), _cap(id, 1000e6, expiry));
        (, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wBytes, 40_000_000, "honest traffic credits every delivered byte");
    }

    function test_redeem_softFloor_routedBytesBoundedByCeiling() public {
        bytes32 id = _open();
        uint64 amount = 100;
        uint64 maxBytes = uint64(uint256(amount) * BYTES_PER_MB / DELIVERY_FLOOR);
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, amount, 5 * maxBytes, _voucher(id, amount, 5 * maxBytes), _cap(id, 1000e6, expiry)
        );

        (, uint256 b, uint256 amt) = router.calls(0);
        assertEq(amt, amount);
        assertEq(b, maxBytes, "routed bytes never exceed amount * BYTES_PER_MB / floor");
    }

    /// @dev A voucher that clears the floor when the node accepts it, pushed
    ///      sub-floor by a later governance floor raise, still settles its
    ///      cumulative — it is not stranded — and credits the new, lower
    ///      ceiling. This is the case the hard revert broke: an accepted
    ///      voucher the node cannot un-serve.
    function test_redeem_softFloor_afterFloorRaise_clampsAcceptedVoucher() public {
        bytes32 id = _open();
        uint64 amount = 300;
        uint64 servedBytes = 200_000_000;

        uint256 newFloor = 100;
        vm.prank(admin);
        pool.grantRole(GOVERNANCE_ROLE, admin);
        vm.prank(admin);
        pool.setRateBounds(newFloor);

        uint64 ceiling = uint64(uint256(amount) * BYTES_PER_MB / newFloor);
        assertLt(ceiling, servedBytes, "the raise makes the accepted voucher sub-floor");

        vm.prank(provider);
        _redeemOne(
            id, signer, provider, amount, servedBytes, _voucher(id, amount, servedBytes), _cap(id, 1000e6, expiry)
        );
        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, amount, "accepted voucher still settles after a floor raise");
        assertEq(wBytes, ceiling, "credited bytes clamped to the raised floor");
    }

    /// @dev The motivating fix: one sub-floor lane in a `redeemMany` batch no
    ///      longer reverts and strands every honest lane sharing the call.
    function test_redeemMany_softFloor_subFloorLaneDoesNotPoisonBatch() public {
        bytes32 id = _open();
        (uint256[] memory pks, address[] memory signers) = _primeLanes(id, uint256(keccak256("soft-floor-batch")), 2);

        uint64 amount = 100;
        uint64 maxBytes = uint64(uint256(amount) * BYTES_PER_MB / DELIVERY_FLOOR);

        // Lane 0: honest, well under the ceiling. Lane 1: sub-floor.
        PaymentPool.LaneVoucher[] memory v = new PaymentPool.LaneVoucher[](2);
        v[0] = _laneOf(signers[0], amount, maxBytes / 2, _voucherFor(id, signers[0], amount, maxBytes / 2, pks[0]));
        v[1] =
            _laneOf(signers[1], amount, maxBytes + 1000, _voucherFor(id, signers[1], amount, maxBytes + 1000, pks[1]));

        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, v));

        assertEq(totalPaid, 2 * uint256(amount), "both lanes settle; the sub-floor lane does not revert the batch");

        (, uint64 honestBytes) = pool.watermark(id, signers[0], provider);
        assertEq(honestBytes, maxBytes / 2, "honest lane credits every delivered byte");
        (, uint64 subFloorBytes) = pool.watermark(id, signers[1], provider);
        assertEq(subFloorBytes, maxBytes, "sub-floor lane credited only up to the ceiling");
    }

    function testFuzz_redeem_softFloor_creditsClampedBytes(uint64 amount, uint64 bytesDelivered) public {
        amount = uint64(bound(amount, 1, DEPOSIT));
        uint64 maxBytes = uint64(uint256(amount) * BYTES_PER_MB / DELIVERY_FLOOR);
        // Twice the ceiling for the widest in-bounds `amount` is ~2.1e15, well
        // inside `uint64`, so the fuzzer explores both sides of the floor.
        bytesDelivered = uint64(bound(bytesDelivered, 0, 2 * uint256(maxBytes)));

        bytes32 id = _open();
        Sig memory sig = _voucher(id, amount, bytesDelivered);
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        // Never reverts, whatever the rate: the cumulative always settles.
        _redeemOne(id, signer, provider, amount, bytesDelivered, sig, cap);
        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, amount, "cumulative always settles");
        uint64 expectedBytes = bytesDelivered < maxBytes ? bytesDelivered : maxBytes;
        assertEq(wBytes, expectedBytes, "credited bytes clamped to the floor ceiling");
    }

    // -----------------------------------------------------------------
    // PayWord hash chain (ADR 003 § Hash-chain metering)
    // -----------------------------------------------------------------

    /// @dev The payer's per-lane seed for these tests. Real seeds are derived
    ///      per `(pool, signer, provider)` lane; here one is enough, because
    ///      every test drives a single lane.
    bytes32 internal constant SEED = keccak256("decdn/test/payword/seed");
    /// @dev One chunk of delivery, priced at the expected market rate. Because
    ///      `CHUNK_BYTES == BYTES_PER_MB`, this is also exactly one MB of price.
    uint64 internal constant CHUNK_PRICE = 10;

    /// @dev Redeem one chained lane: sign `(amount, bytes, root, price)`, then
    ///      present it extended by the preimage at `index`.
    /// @dev One chained lane's inputs, as a memory struct rather than a
    ///      parameter list: this file compiles without the IR pipeline, and six
    ///      loose values plus a signature and a batch exceed the stack.
    struct Chained {
        uint64 amount;
        uint64 bytesDelivered;
        bytes32 root;
        uint64 chunkPrice;
        bytes32 preimage;
        uint8 index;
    }

    /// @dev Redeem one chained lane: sign `(amount, bytes, root, price)`, then
    ///      present it extended by the preimage at `index`. Pass an empty
    ///      `cap` once the signer is already registered.
    function _redeemChained(bytes32 id, Chained memory c, bytes memory cap) internal returns (uint256) {
        PaymentPool.LaneVoucher[] memory v = new PaymentPool.LaneVoucher[](1);
        v[0] = _laneOfChained(
            signer,
            c.amount,
            c.bytesDelivered,
            _signVoucherChained(
                address(pool), id, signer, provider, c.amount, c.bytesDelivered, c.root, c.chunkPrice, SIGNER_PK
            ),
            c.root,
            c.preimage,
            _meter(c.chunkPrice, c.index)
        );
        return pool.redeemMany(_batch(id, _capsOf(cap), v));
    }

    /// @dev A [`Chained`] with no anchor bytes — the common shape in these
    ///      tests, where the byte axis is not what is under test.
    function _chained(uint64 amount, bytes32 root, uint64 chunkPrice, bytes32 preimage, uint8 index)
        internal
        pure
        returns (Chained memory)
    {
        return Chained({
            amount: amount, bytesDelivered: 0, root: root, chunkPrice: chunkPrice, preimage: preimage, index: index
        });
    }

    /// @dev The `CapabilityReg[]` a `_cap(...)` blob unpacks into — empty when
    ///      the signer is already registered.
    function _capsOf(bytes memory capability) internal view returns (PaymentPool.CapabilityReg[] memory caps) {
        caps = new PaymentPool.CapabilityReg[](capability.length == 0 ? 0 : 1);
        if (capability.length != 0) {
            (uint64 spendingCap, uint64 exp, bytes memory ownerSig) = abi.decode(capability, (uint64, uint64, bytes));
            caps[0] = PaymentPool.CapabilityReg({
                signer: signer, spendingCap: spendingCap, expiry: exp, ownerSig: ownerSig
            });
        }
    }

    /// @notice The cooperative close. A sealed voucher — zero root, zero index,
    ///         zero preimage, zero price — settles exactly its `cumulative`
    ///         through the ordinary walk, with no branch testing
    ///         `chainRoot == 0` anywhere in the contract.
    function test_redeem_payWord_sealedVoucherSettlesExactlyItsAmount() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 5000,
                bytesDelivered: 5 * uint64(CHUNK_BYTES),
                root: bytes32(0),
                chunkPrice: 0,
                preimage: bytes32(0),
                index: 0
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 5000, "a sealed voucher settles exactly its cumulative");
        assertEq(wBytes, 5 * uint64(CHUNK_BYTES), "and exactly its signed bytes");
    }

    /// @notice Nothing hashes to zero, so a sealed voucher cannot be redeemed
    ///         at any index above 0. This is what makes the zero root a safe
    ///         sentinel rather than a special case.
    function test_redeem_payWord_sealedVoucherRevertsAtAnyIndexAboveZero() public {
        bytes32 id = _open();
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.BadPreimage.selector);
        _redeemChained(id, _chained(5000, bytes32(0), CHUNK_PRICE, _preimage(SEED, 1), 1), cap);
    }

    /// @notice A REAL-root voucher settles its own `cumulative` the same way a
    ///         sealed one does: the node submits the root as its own preimage
    ///         at index 0 and the walk runs zero times. Both shapes reach
    ///         "settle the signed amount" through one check.
    function test_redeem_payWord_realRootAtIndexZeroSettlesExactlyItsAmount() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 5000,
                bytesDelivered: uint64(CHUNK_BYTES),
                root: root,
                chunkPrice: CHUNK_PRICE,
                preimage: root,
                index: 0
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount,) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 5000, "index 0 adds no increment");
    }

    /// @notice One tick pays one `chunkPrice` over the anchor and credits one
    ///         `CHUNK_BYTES` — the whole point of the meter.
    function test_redeem_payWord_oneTickExtendsTheAnchorByOneChunk() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 5000,
                bytesDelivered: uint64(CHUNK_BYTES),
                root: root,
                chunkPrice: CHUNK_PRICE,
                preimage: _preimage(SEED, 1),
                index: 1
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 5000 + CHUNK_PRICE, "one chunk of price over the anchor");
        assertEq(wBytes, 2 * uint64(CHUNK_BYTES), "one chunk of bytes over the anchor");
    }

    /// @notice The full-depth walk: a payer that abandons a stream mid-chain
    ///         leaves the node a claim worth 255 chunks over its anchor, and
    ///         every one of them is provable in a bounded 255 keccaks.
    function test_redeem_payWord_fullDepthWalkAtMaxChainLength() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        uint64 anchorBytes = uint64(CHUNK_BYTES);
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 5000,
                bytesDelivered: anchorBytes,
                root: root,
                chunkPrice: CHUNK_PRICE,
                preimage: _preimage(SEED, MAX_CHAIN_LENGTH),
                index: MAX_CHAIN_LENGTH
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 5000 + uint64(MAX_CHAIN_LENGTH) * CHUNK_PRICE, "255 chunks over the anchor");
        assertEq(wBytes, anchorBytes + uint64(MAX_CHAIN_LENGTH) * uint64(CHUNK_BYTES), "255 chunks of bytes");
    }

    /// @notice There is no index-256 case to test at this layer, and that is
    ///         the point: `chainIndex` is extracted as a `uint8`, so 255 is the
    ///         ceiling by construction. What a caller CAN do is set a byte in
    ///         the packed word's reserved span, and that is refused rather than
    ///         masked away — which is what keeps the span claimable by a later
    ///         field without reinterpreting any voucher signed today.
    function test_redeem_payWord_reservedChainMeterSpanIsRejected() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        Sig memory sig = _signVoucherChained(address(pool), id, signer, provider, 5000, 0, root, CHUNK_PRICE, SIGNER_PK);
        PaymentPool.LaneVoucher[] memory v = new PaymentPool.LaneVoucher[](1);
        // One bit immediately above the price field — the lowest reserved bit.
        v[0] = _laneOfChained(signer, 5000, 0, sig, root, root, _meter(CHUNK_PRICE, 0) | (uint256(1) << 72));

        vm.prank(provider);
        vm.expectRevert(PaymentPool.ChainMeterReservedNonZero.selector);
        pool.redeemMany(_batch(id, _capsOf(_cap(id, DEPOSIT, expiry)), v));
    }

    /// @notice A preimage from another chain is caller error, not transient
    ///         pool state, so it reverts the whole call — unlike a zero-paying
    ///         voucher, which is skipped.
    function test_redeem_payWord_foreignPreimageRevertsTheWholeCall() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.BadPreimage.selector);
        _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(keccak256("other"), 3), 3), cap);
    }

    /// @notice A shallower preimage cannot be passed off as a deeper one:
    ///         reaching the root takes the hashes it takes.
    function test_redeem_payWord_shallowerPreimageCannotClaimADeeperIndex() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.BadPreimage.selector);
        _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(SEED, 4), 5), cap);
    }

    /// @notice Rollover, both ways round. Redeeming the retired chain and then
    ///         the folded voucher collects exactly what redeeming only the
    ///         folded voucher collects — because payment is `claimed - paid`,
    ///         so the second call is already net of the first.
    function test_redeem_payWord_rolloverOldThenNewEqualsNewOnly() public {
        bytes32 rootA = _chainRoot(SEED);
        bytes32 rootB = _chainRoot(keccak256("decdn/test/payword/seed2"));
        uint64 anchor = 5000;
        uint8 reached = 4;
        uint64 folded = anchor + uint64(reached) * CHUNK_PRICE;

        // Path 1: redeem the retired chain at its frontier, then the fold.
        bytes32 idA = _open();
        vm.startPrank(provider);
        _redeemChained(
            idA, _chained(anchor, rootA, CHUNK_PRICE, _preimage(SEED, reached), reached), _cap(idA, DEPOSIT, expiry)
        );
        _redeemChained(idA, _chained(folded, rootB, CHUNK_PRICE, rootB, 0), "");
        vm.stopPrank();
        (uint64 amountA,) = pool.watermark(idA, signer, provider);

        // Path 2: skip the retired chain and redeem only the fold.
        bytes32 idB = _open();
        vm.prank(provider);
        _redeemChained(idB, _chained(folded, rootB, CHUNK_PRICE, rootB, 0), _cap(idB, DEPOSIT, expiry));
        (uint64 amountB,) = pool.watermark(idB, signer, provider);

        assertEq(amountA, folded, "old-then-new lands on the folded total");
        assertEq(amountA, amountB, "both redemption orders collect the same total");
    }

    /// @notice A superseded chain pays 0 and is SKIPPED, not reverted: the
    ///         cumulative resolution makes a stale claim worthless without any
    ///         chain state stored on-chain to detect it.
    function test_redeem_payWord_supersededChainPaysZeroAndIsSkipped() public {
        bytes32 id = _open();
        bytes32 rootA = _chainRoot(SEED);
        bytes32 rootB = _chainRoot(keccak256("decdn/test/payword/seed2"));
        uint8 reached = 4;
        uint64 folded = 5000 + uint64(reached) * CHUNK_PRICE;

        vm.startPrank(provider);
        _redeemChained(id, _chained(folded, rootB, CHUNK_PRICE, rootB, 0), _cap(id, DEPOSIT, expiry));
        // The retired chain, presented afterwards at the frontier it reached.
        uint256 paid = _redeemChained(id, _chained(5000, rootA, CHUNK_PRICE, _preimage(SEED, reached), reached), "");
        vm.stopPrank();

        assertEq(paid, 0, "a superseded chain pays nothing");
        (uint64 wAmount,) = pool.watermark(id, signer, provider);
        assertEq(wAmount, folded, "and moves the watermark nowhere");
    }

    /// @notice The soft floor clamps the CHAIN-EXTENDED pair, not the signed
    ///         half. Bytes proved by preimage carry the same per-byte price
    ///         obligation as bytes proved by signature — otherwise a chain
    ///         could credit 255 chunks of bytes against whatever its anchor
    ///         happened to cost.
    function test_redeem_payWord_floorCeilingRisesWithTheChainExtendedClaim() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        // A 1-base-unit anchor over 1 MB of signed bytes. Read on the SIGNED
        // pair alone, that money justifies only 1 MB at the floor, so a
        // signed-only ceiling would clamp here. The chain adds 2 chunks of
        // price, lifting the claim to 21 and the ceiling to 21 MB, which is
        // comfortably above the 3 MB the extended byte claim asks for — so
        // nothing is clamped and every proved byte is credited.
        uint8 index = 2;
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 1,
                bytesDelivered: uint64(CHUNK_BYTES),
                root: root,
                chunkPrice: CHUNK_PRICE,
                preimage: _preimage(SEED, index),
                index: index
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 1 + uint64(index) * CHUNK_PRICE, "the extended claim settles in full");
        assertEq(wBytes, 3 * uint64(CHUNK_BYTES), "every chain-proved byte is credited");
        assertGt(wBytes, uint64(CHUNK_BYTES), "a signed-only ceiling would have clamped at 1 MB");
    }

    /// @notice And the clamp still bites when the EXTENDED claim cannot justify
    ///         the extended bytes — the ceiling is computed from `claimed`, not
    ///         from the signed `cumulative`.
    function test_redeem_payWord_floorClampBitesOnTheExtendedPair() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        // 1 base unit of anchor over 100 MB of signed bytes, extended by two
        // chunks: claim 21, byte claim 102 MB, ceiling 21 MB. The clamp lands
        // at 21 MB — which is only reachable because the ceiling read the
        // extended claim; the signed `cumulative` of 1 would have allowed 1 MB.
        uint8 index = 2;
        vm.prank(provider);
        _redeemChained(
            id,
            Chained({
                amount: 1,
                bytesDelivered: 100 * uint64(CHUNK_BYTES),
                root: root,
                chunkPrice: CHUNK_PRICE,
                preimage: _preimage(SEED, index),
                index: index
            }),
            _cap(id, DEPOSIT, expiry)
        );

        (uint64 wAmount, uint64 wBytes) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 21, "the extended claim settles in full");
        assertEq(
            uint256(wBytes),
            uint256(wAmount) * BYTES_PER_MB / DELIVERY_FLOOR,
            "credited bytes clamped to the ceiling the EXTENDED claim justifies"
        );
    }

    /// @notice A chain whose extension would push the claim past `uint64` is
    ///         refused rather than silently wrapping. These two values are the
    ///         one pair here derived from calldata rather than read from a
    ///         signed `uint64`, so they are the one pair bounded by a check.
    function test_redeem_payWord_overflowingClaimIsRefused() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        uint64 huge = type(uint64).max;
        bytes memory cap = _cap(id, DEPOSIT, expiry);
        vm.prank(provider);
        vm.expectRevert(PaymentPool.ClaimOverflow.selector);
        _redeemChained(id, _chained(huge, root, huge, _preimage(SEED, 1), 1), cap);
    }

    /// @notice A bad preimage on ONE lane reverts the whole batch, where a
    ///         zero-paying lane is merely skipped. That split is the point: a
    ///         mismatched hash is caller error the node can fix locally, while
    ///         a drained pool is transient state it should retry through.
    function test_redeemMany_payWord_badPreimageLanePoisonsTheBatch() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);

        PaymentPool.LaneVoucher[] memory v = new PaymentPool.LaneVoucher[](2);
        v[0] = _laneOf(signer, 1000, uint64(CHUNK_BYTES), _voucher(id, 1000, uint64(CHUNK_BYTES)));
        v[1] = _laneOfChained(
            signer,
            2000,
            0,
            _signVoucherChained(address(pool), id, signer, provider, 2000, 0, root, CHUNK_PRICE, SIGNER_PK),
            root,
            _preimage(keccak256("other"), 2),
            _meter(CHUNK_PRICE, 2)
        );

        vm.prank(provider);
        vm.expectRevert(PaymentPool.BadPreimage.selector);
        pool.redeemMany(_batch(id, _capsOf(_cap(id, DEPOSIT, expiry)), v));
    }

    /// @notice The chain adds no storage. `chainRoot`, `preimage` and
    ///         `chainMeter` are calldata the contract resolves and discards —
    ///         only the paid watermark persists, which is what lets a lane stay
    ///         one storage slot and needs no anti-replay state of its own.
    function test_redeem_payWord_replayingTheSameProofPaysZero() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        bytes memory cap = _cap(id, DEPOSIT, expiry);

        vm.startPrank(provider);
        _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(SEED, 3), 3), cap);
        uint256 again = _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(SEED, 3), 3), "");
        vm.stopPrank();

        assertEq(again, 0, "re-presenting a fully-paid claim pays nothing");
    }

    /// @notice Presenting a DEEPER preimage on the same anchor collects only
    ///         the difference — the frontier advances, and the money already
    ///         paid is not paid twice.
    function test_redeem_payWord_deeperPreimageCollectsOnlyTheDifference() public {
        bytes32 id = _open();
        bytes32 root = _chainRoot(SEED);
        bytes memory cap = _cap(id, DEPOSIT, expiry);

        vm.startPrank(provider);
        _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(SEED, 3), 3), cap);
        uint256 delta = _redeemChained(id, _chained(5000, root, CHUNK_PRICE, _preimage(SEED, 7), 7), "");
        vm.stopPrank();

        assertEq(delta, 4 * CHUNK_PRICE, "only the four chunks past the paid frontier");
        (uint64 wAmount,) = pool.watermark(id, signer, provider);
        assertEq(wAmount, 5000 + 7 * CHUNK_PRICE, "the lane sits at the deeper frontier");
    }

    // -----------------------------------------------------------------
    // redeemMany — register-batch then redeem-batch (ADR 003 § Batch redemption)
    // -----------------------------------------------------------------

    function test_redeemMany_registersCapabilitiesThenRedeemsVouchers() public {
        bytes32 id = _open();
        address provider2 = address(0xCAFE);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        });

        PaymentPool.LaneVoucher[] memory vouchers1 = new PaymentPool.LaneVoucher[](1);
        vouchers1[0] = _laneOf(signer, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000));

        vm.prank(provider);
        uint256 totalPaid1 = pool.redeemMany(_batch(id, caps, vouchers1));
        assertEq(totalPaid1, 200e6, "capability registers signer; voucher pays in the same call");

        // A second call from the other provider, empty `capabilities` since
        // `signer` is already registered, pays its own lane.
        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers2 = new PaymentPool.LaneVoucher[](1);
        vouchers2[0] = _laneOf(
            signer,
            150e6,
            15_000_000,
            _signVoucherFor(address(pool), id, signer, provider2, 150e6, 15_000_000, SIGNER_PK)
        );

        vm.prank(provider2);
        uint256 totalPaid2 = pool.redeemMany(_batch(id, noCaps, vouchers2));
        assertEq(totalPaid2, 150e6);
        assertEq(totalPaid1 + totalPaid2, 350e6, "totalPaid sums across the two lanes");
    }

    function test_redeemMany_redeemsWithEmptyCapabilitiesWhenAlreadyRegistered() public {
        bytes32 id = _open();
        // A prior single `redeem` registers `signer`.
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = _laneOf(signer, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000));

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, vouchers));
        assertEq(totalPaid, 100e6, "empty capabilities still redeems an already-registered signer");
    }

    function test_redeemMany_skipsUncoveredSignerVoucher() public {
        bytes32 id = _open();
        // `signer` is registered and funded via a normal `redeem`.
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        // `stranger` is never registered and is not in this batch's
        // `capabilities` either; `_applyVoucher` returns 0 for an
        // unregistered signer before it even checks the voucher signature,
        // so a garbage signature here is enough.
        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](2);
        vouchers[0] = _laneOf(stranger, 100e6, 10_000_000, Sig({ r: bytes32(0), vs: bytes32(0) }));
        vouchers[1] = _laneOf(signer, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000));

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, vouchers));
        assertEq(totalPaid, 100e6, "uncovered-signer voucher skips; the covered voucher still pays");
    }

    function test_redeemMany_skipsEmptyLane_doesNotRevert() public {
        bytes32 id = _open();
        // Register + pay once via a single `redeem` so a later replay of the
        // same cumulative is stale (transient-empty, not structural).
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        uint256 signer2Pk = 0xBEEF2;
        address signer2 = vm.addr(signer2Pk);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer2,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer2, SPENDING_CAP, expiry, OWNER_PK)
        });

        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](2);
        // Stale replay of the already-paid cumulative: pays 0, skipped.
        vouchers[0] = _laneOf(signer, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000));
        // Freshly registered signer pays.
        vouchers[1] = _laneOf(
            signer2,
            100e6,
            10_000_000,
            _signVoucherFor(address(pool), id, signer2, provider, 100e6, 10_000_000, signer2Pk)
        );

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, caps, vouchers));
        assertEq(totalPaid, 100e6, "stale lane skipped without reverting; the fresh signer's voucher still pays");
    }

    function test_redeemMany_revertsOnBadVoucherSignature() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        // The signature is over a different amount than the entry presents.
        vouchers[0] = _laneOf(signer, 300e6, 30_000_000, _voucher(id, 299e6, 30_000_000));

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        pool.redeemMany(_batch(id, noCaps, vouchers));
    }

    /// A batch is redeemed for `msg.sender`, and every voucher's signature is
    /// checked against that payee. A node cannot present a batch of another
    /// node's vouchers: each one recovers the wrong signer.
    function test_redeemMany_revertsWhenCallerIsNotTheVouchersPayee() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = _laneOf(signer, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000));

        // The voucher is signed for `provider`, not for this caller.
        vm.prank(stranger);
        vm.expectRevert(PaymentPool.InvalidVoucherSignature.selector);
        pool.redeemMany(_batch(id, noCaps, vouchers));
    }

    function test_redeemMany_revertsOnBadCapabilityOwnerSig() public {
        bytes32 id = _open();

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            // Signed over a different cap than advertised → recovery misses the owner.
            ownerSig: _signCapabilityFor(address(pool), id, signer, 999e6, expiry, OWNER_PK)
        });
        PaymentPool.LaneVoucher[] memory noVouchers = new PaymentPool.LaneVoucher[](0);

        vm.prank(provider);
        vm.expectRevert(PaymentPool.InvalidCapabilitySignature.selector);
        pool.redeemMany(_batch(id, caps, noVouchers));
    }

    function test_redeemMany_registersSignerOncePerNewSigner() public {
        bytes32 id = _open();

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](2);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        // A duplicate entry for the same signer, advertising a different
        // cap/expiry and a signature that would fail if re-verified. The
        // second registration is a no-op, so it is never evaluated.
        caps[1] = PaymentPool.CapabilityReg({ signer: signer, spendingCap: 1, expiry: 1, ownerSig: hex"00" });
        PaymentPool.LaneVoucher[] memory noVouchers = new PaymentPool.LaneVoucher[](0);

        vm.prank(provider);
        pool.redeemMany(_batch(id, caps, noVouchers));

        (uint64 cap, uint64 exp,) = pool.authorized(id, signer);
        assertEq(cap, SPENDING_CAP, "first registration wins");
        assertEq(uint256(exp), uint256(expiry));
    }

    function test_redeemMany_emitsOneEventCarryingEveryPaidLane() public {
        bytes32 id = _open();
        uint256 signer2Pk = 0xBEEF3;
        address signer2 = vm.addr(signer2Pk);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](2);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        caps[1] = PaymentPool.CapabilityReg({
            signer: signer2,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer2, SPENDING_CAP, expiry, OWNER_PK)
        });

        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](2);
        vouchers[0] = _laneOf(signer, 100e6, 10_000_000, _voucher(id, 100e6, 10_000_000));
        vouchers[1] = _laneOf(
            signer2,
            150e6,
            15_000_000,
            _signVoucherFor(address(pool), id, signer2, provider, 150e6, 15_000_000, signer2Pk)
        );

        PaymentPool.LaneSettled[] memory expected = new PaymentPool.LaneSettled[](2);
        expected[0] = PaymentPool.LaneSettled({ signer: signer, newPaidCumulative: 100e6, bytesPaid: 10_000_000 });
        expected[1] = PaymentPool.LaneSettled({ signer: signer2, newPaidCumulative: 150e6, bytesPaid: 15_000_000 });

        vm.prank(provider);
        vm.expectEmit(true, true, true, true, address(pool));
        emit PoolRedeemed(id, provider, expected);
        uint256 totalPaid = pool.redeemMany(_batch(id, caps, vouchers));
        assertEq(totalPaid, 250e6, "one event carries an entry per paid lane; totalPaid sums both");
    }

    /// Every voucher in a batch names the same payee (`provider == msg.sender`
    /// is enforced per entry), so the batch settles through the router exactly
    /// once, carrying the summed amount and the summed paid bytes — not one
    /// `routeSettlement` per voucher.
    /// The pool's remaining deposit is read ONCE per group and drawn down in a
    /// local, so a second voucher on the same pool is bounded by what the first
    /// already took. Without that threading each voucher would re-read a
    /// `totalRedeemed` the group has not written yet, and the two would both be
    /// bounded by the full remaining balance — letting a batch overdraw the pool.
    function test_redeemMany_secondVoucherOnAPoolSeesTheFirstsDraw() public {
        bytes32 id = _open(); // DEPOSIT = 1000e6
        uint256 signer2Pk = 0xD12E57;
        address signer2 = vm.addr(signer2Pk);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](2);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: DEPOSIT,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer, DEPOSIT, expiry, OWNER_PK)
        });
        caps[1] = PaymentPool.CapabilityReg({
            signer: signer2,
            spendingCap: DEPOSIT,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer2, DEPOSIT, expiry, OWNER_PK)
        });

        // Two vouchers wanting 800e6 each against a 1000e6 pool. The first must
        // take its full 800e6; the second must be clamped to the 200e6 left.
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](2);
        vouchers[0] = _laneOf(signer, 800e6, 80_000_000, _voucher(id, 800e6, 80_000_000));
        vouchers[1] = _laneOf(
            signer2,
            800e6,
            80_000_000,
            _signVoucherFor(address(pool), id, signer2, provider, 800e6, 80_000_000, signer2Pk)
        );

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, caps, vouchers));

        assertEq(totalPaid, DEPOSIT, "the batch draws the deposit and not one unit more");
        assertEq(pool.getPool(id).totalRedeemed, DEPOSIT, "totalRedeemed lands in one write at the group's end");
        (uint64 lane1,) = pool.watermark(id, signer, provider);
        (uint64 lane2,) = pool.watermark(id, signer2, provider);
        assertEq(lane1, 800e6, "the first voucher takes its full draw");
        assertEq(lane2, 200e6, "the second is bounded by what is left, and stays retriable for the rest");
        assertEq(usdc.balanceOf(address(router)), DEPOSIT, "the router received exactly the deposit");
    }

    /// Several pools in one call: each group gates and advances its own pool,
    /// and the whole call still settles through the router exactly once.
    function test_redeemMany_spansPoolsAndStillSettlesOnce() public {
        bytes32 idA = _open();
        bytes32 idB = _open();

        PaymentPool.PoolBatch[] memory batches = new PaymentPool.PoolBatch[](2);
        batches[0] = _soloGroup(idA, SIGNER_PK, 100e6, 10_000_000);
        batches[1] = _soloGroup(idB, 0xB0015, 150e6, 15_000_000);

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(batches);

        assertEq(totalPaid, 250e6);
        assertEq(pool.getPool(idA).totalRedeemed, 100e6, "each pool advances its own counter");
        assertEq(pool.getPool(idB).totalRedeemed, 150e6);
        assertEq(router.callCount(), 1, "two pools still settle in one routeSettlement");
        (address op, uint256 b, uint256 amt) = router.calls(0);
        assertEq(op, provider);
        assertEq(amt, 250e6, "the routed amount sums across pools");
        assertEq(b, 25_000_000, "so does the routed byte count");
    }

    /// @dev A one-signer group on `poolId`: the signer's registration plus its
    ///      single voucher. Factored out so a multi-pool test does not carry
    ///      every group's locals in one frame.
    function _soloGroup(bytes32 poolId, uint256 pk, uint64 amount, uint64 bytesDelivered)
        internal
        view
        returns (PaymentPool.PoolBatch memory)
    {
        address s_ = vm.addr(pk);
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: s_,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), poolId, s_, SPENDING_CAP, expiry, OWNER_PK)
        });
        return PaymentPool.PoolBatch({
            poolId: poolId, capabilities: caps, vouchers: _soloVoucher(poolId, pk, amount, bytesDelivered)
        });
    }

    /// @dev The one-voucher half of [`_soloGroup`], split out to keep either
    ///      frame inside the legacy codegen's stack limit.
    function _soloVoucher(bytes32 poolId, uint256 pk, uint64 amount, uint64 bytesDelivered)
        internal
        view
        returns (PaymentPool.LaneVoucher[] memory vouchers)
    {
        address s_ = vm.addr(pk);
        vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = _laneOf(
            s_, amount, bytesDelivered, _signVoucherFor(address(pool), poolId, s_, provider, amount, bytesDelivered, pk)
        );
    }

    /// A closed pool anywhere in the call reverts the whole thing, including
    /// the registrations and draws of pools that came before it.
    function test_redeemMany_aClosedPoolRevertsEveryGroup() public {
        bytes32 idA = _open();
        bytes32 idB = _open();

        vm.prank(owner);
        pool.closePool(idB);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        PaymentPool.CapabilityReg[] memory capsA = new PaymentPool.CapabilityReg[](1);
        capsA[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), idA, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        PaymentPool.LaneVoucher[] memory vouchersA = new PaymentPool.LaneVoucher[](1);
        vouchersA[0] = _laneOf(signer, 100e6, 10_000_000, _voucher(idA, 100e6, 10_000_000));

        PaymentPool.PoolBatch[] memory batches = new PaymentPool.PoolBatch[](2);
        batches[0] = PaymentPool.PoolBatch({ poolId: idA, capabilities: capsA, vouchers: vouchersA });
        batches[1] = PaymentPool.PoolBatch({
            poolId: idB, capabilities: new PaymentPool.CapabilityReg[](0), vouchers: new PaymentPool.LaneVoucher[](0)
        });

        vm.prank(provider);
        vm.expectRevert(PaymentPool.PoolClosed.selector);
        pool.redeemMany(batches);

        assertEq(pool.getPool(idA).totalRedeemed, 0, "the earlier group rolled back too");
        (uint64 cap,,) = pool.authorized(idA, signer);
        assertEq(cap, 0, "and so did its registration");
    }

    function test_redeemMany_settlesOnceForTheWholeBatch() public {
        bytes32 id = _open();
        uint256 signer2Pk = 0xBEEF4;
        address signer2 = vm.addr(signer2Pk);

        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](2);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        caps[1] = PaymentPool.CapabilityReg({
            signer: signer2,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _signCapabilityFor(address(pool), id, signer2, SPENDING_CAP, expiry, OWNER_PK)
        });

        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](2);
        vouchers[0] = _laneOf(signer, 100e6, 10_000_000, _voucher(id, 100e6, 10_000_000));
        vouchers[1] = _laneOf(
            signer2,
            150e6,
            15_000_000,
            _signVoucherFor(address(pool), id, signer2, provider, 150e6, 15_000_000, signer2Pk)
        );

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, caps, vouchers));

        assertEq(totalPaid, 250e6);
        assertEq(router.callCount(), 1, "two paid vouchers settle in one routeSettlement");
        (address op, uint256 b, uint256 amt) = router.calls(0);
        assertEq(op, provider, "the batch's single payee");
        assertEq(amt, 250e6, "the routed amount is the batch total");
        assertEq(b, 25_000_000, "the routed byte count is the batch total");
    }

    /// A batch in which every voucher is transient-empty pays nothing and
    /// touches the router not at all — a zero-amount `routeSettlement` would
    /// revert, so the settlement leg has to be skipped rather than called with 0.
    function test_redeemMany_allEmptyBatchNeverCallsRouter() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000), _cap(id, SPENDING_CAP, expiry)
        );
        uint256 callsBefore = router.callCount();

        // Stale replay of the already-paid cumulative: the only entry, pays 0.
        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = _laneOf(signer, 200e6, 20_000_000, _voucher(id, 200e6, 20_000_000));

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, vouchers));

        assertEq(totalPaid, 0);
        assertEq(router.callCount(), callsBefore, "an all-empty batch never reaches the router");
    }

    function test_redeemMany_bothArraysEmptyReturnsZeroNoRevert() public {
        bytes32 id = _open();
        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);
        PaymentPool.LaneVoucher[] memory noVouchers = new PaymentPool.LaneVoucher[](0);

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, noVouchers));
        assertEq(totalPaid, 0, "nothing structurally wrong with an empty/empty batch");
    }

    /// An empty top-level array is the degenerate case one step further out:
    /// no pool is even named, so nothing is read and nothing settles.
    function test_redeemMany_noBatchesReturnsZeroNoRevert() public {
        vm.prank(provider);
        assertEq(pool.redeemMany(new PaymentPool.PoolBatch[](0)), 0, "an empty call is a no-op");
    }

    // -----------------------------------------------------------------
    // redeemMany — marginal per-voucher gas (Task 7 chunk-size validation)
    // -----------------------------------------------------------------

    /// @dev Registers `count` distinct signer capabilities on `id` ahead of
    ///      time, via their own `redeemMany` call (empty voucher array), so
    ///      a later timed call carries voucher-verify-and-settle cost only —
    ///      not one-time capability registration. Signer keys start at
    ///      `seed + 1` so no lane ever lands on the zero private key.
    function _primeLanes(bytes32 id, uint256 seed, uint256 count)
        internal
        returns (uint256[] memory pks, address[] memory signers)
    {
        pks = new uint256[](count);
        signers = new address[](count);
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](count);
        for (uint256 i = 0; i < count; i++) {
            uint256 pk = seed + i + 1;
            address who = vm.addr(pk);
            pks[i] = pk;
            signers[i] = who;
            caps[i] = PaymentPool.CapabilityReg({
                signer: who,
                spendingCap: SPENDING_CAP,
                expiry: expiry,
                ownerSig: _signCapabilityFor(address(pool), id, who, SPENDING_CAP, expiry, OWNER_PK)
            });
        }
        PaymentPool.LaneVoucher[] memory noVouchers = new PaymentPool.LaneVoucher[](0);
        vm.prank(provider);
        pool.redeemMany(_batch(id, caps, noVouchers));
    }

    /// @dev One voucher per already-registered signer in `signers`, each for
    ///      `amountEach`/`bytesEach` — every lane's first-ever redeem, so
    ///      every watermark write in the timed call is a cold SSTORE.
    function _lanesOf(bytes32 id, uint256[] memory pks, address[] memory signers, uint64 amountEach, uint64 bytesEach)
        internal
        view
        returns (PaymentPool.LaneVoucher[] memory vouchers)
    {
        vouchers = new PaymentPool.LaneVoucher[](signers.length);
        for (uint256 i = 0; i < signers.length; i++) {
            Sig memory sig = _voucherFor(id, signers[i], amountEach, bytesEach, pks[i]);
            vouchers[i] = _laneOf(signers[i], amountEach, bytesEach, sig);
        }
    }

    /// @dev `_signVoucherFor` against `provider` with the payload broken out
    ///      of the call site — keeps `_lanesOf`'s loop body under the stack
    ///      depth `solc` allows without `via_ir`.
    function _voucherFor(bytes32 id, address signer_, uint64 amountEach, uint64 bytesEach, uint256 pk)
        internal
        view
        returns (Sig memory)
    {
        return _signVoucherFor(address(pool), id, signer_, provider, amountEach, bytesEach, pk);
    }

    /// @dev The `test_redeemMany_gas_*` benchmarks pin absolute gas against the
    ///      optimized build. `forge coverage` compiles with the optimizer and
    ///      `via_ir` off, which inflates every measurement tens of thousands of
    ///      gas past the assertion tolerance. Under the coverage context the
    ///      benchmarks skip; the `redeemMany` path they exercise stays covered
    ///      by the functional tests. Callers `return` on a `true` result.
    function _skipGasBenchmarkUnderCoverage() internal returns (bool) {
        if (vm.isContext(VmSafe.ForgeContext.Coverage)) {
            vm.skip(true);
            return true;
        }
        return false;
    }

    /// @dev Opens a fresh pool, primes `count` distinct signer lanes on it,
    ///      redeems one voucher per lane in a single `redeemMany`, and pins
    ///      that call's gas via `snapshotGasLastCall` (surfaced afterward in
    ///      `contracts/.gas-snapshot` / `snapshots/PaymentPool.json` under
    ///      `snapshotName`). Factored out purely to keep each call site's
    ///      stack shallow enough for `solc` without `via_ir`.
    function _redeemFreshLanesAndSnapshotGas(uint256 count, string memory snapshotName) internal returns (uint256) {
        uint64 amount = 1e6;
        uint64 bytesDelivered = 100_000;

        bytes32 id = _open();
        (uint256[] memory pks, address[] memory signers) =
            _primeLanes(id, uint256(keccak256(bytes(snapshotName))), count);
        PaymentPool.LaneVoucher[] memory vouchers = _lanesOf(id, pks, signers, amount, bytesDelivered);
        PaymentPool.CapabilityReg[] memory noCaps = new PaymentPool.CapabilityReg[](0);

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, noCaps, vouchers));
        assertEq(totalPaid, count * uint256(amount), "every primed lane must actually settle");
        // `snapshotGasLastCall` returns the measured gas of the call above (it
        // also mirrors it into the gitignored `snapshots/PaymentPool.json`,
        // which is not readable from tracked state) — capture the return so
        // callers can pin it in a tracked assertion instead of relying on
        // that file.
        return vm.snapshotGasLastCall("PaymentPool", snapshotName);
    }

    /// @notice Task 7: pins the on-chain gas of a `redeemMany` call redeeming
    ///         N already-registered-capability vouchers, to validate
    ///         `DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX` (300,
    ///         `crates/common/src/config/mod.rs`) against the block gas
    ///         limit. Paired with `test_redeemMany_gas_NPlus1Vouchers` below;
    ///         `gas(N+1) - gas(N)` (read from the two pinned
    ///         `contracts/.gas-snapshot` entries after `forge snapshot`) is
    ///         the marginal per-voucher cost.
    ///
    ///         **Why two test functions and not one measuring both calls:**
    ///         `forge` gives every test function its own fresh EVM state, so
    ///         each call below starts from a genuinely cold EIP-2929 access
    ///         list — matching a real standalone on-chain `redeemMany` tx.
    ///         A single test issuing both calls back-to-back was tried and
    ///         rejected: the second call inherits the first's now-*warm*
    ///         router/USDC/allowance slots, making it artificially cheaper
    ///         by tens of thousands of gas — enough to hide the true
    ///         per-voucher marginal, or even flip its sign.
    ///
    ///         Every lane here is that lane's first-ever redeem — a
    ///         cold-storage watermark write per voucher, the worst case a
    ///         chunk of unrelated-client vouchers sees in production — so
    ///         the marginal these two tests pin is a conservative
    ///         (upper-bound) per-voucher cost. Capability registration for
    ///         all N lanes happens in an earlier, unmeasured `redeemMany`
    ///         call within the same test (`_primeLanes`), so this call's gas
    ///         carries voucher-verify-and-settle cost only.
    ///
    ///         The Arbitrum One block gas limit used in this task's
    ///         analysis (~32,000,000) is an external reference recorded from
    ///         prior observation, NOT read live — this test has no RPC
    ///         access. Confirm the current live limit via
    ///         `eth_getBlockByNumber("latest")`'s `gasLimit` on the target
    ///         network before leaning on it for a deploy decision.
    function test_redeemMany_gas_NVouchers() public {
        if (_skipGasBenchmarkUnderCoverage()) return;
        uint256 gasUsed = _redeemFreshLanesAndSnapshotGas(REDEEM_MANY_GAS_BENCHMARK_N, "redeemMany_marginal_N");
        assertLt(gasUsed, 2_000_000, "sanity ceiling on a small fixed-N batch");
        emit log_named_uint("redeemMany gas, N vouchers", gasUsed);
        // Pins the per-call gas cited by `DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX`'s
        // doc comment (crates/common/src/config/mod.rs): N = 515,619 gas.
        // Tolerance covers toolchain/compiler-version gas drift without
        // masking a real regression.
        //
        // Every lane carries the three chain words and runs `_resolveClaim`.
        // These lanes are all SEALED vouchers — zero root, zero preimage, zero
        // meter — so the walk is empty and the three words are pure zero bytes,
        // which is the cooperative path a finalized delivery actually redeems.
        assertApproxEqAbs(gasUsed, 515_619, 5000, "redeemMany gas for N vouchers drifted from the pinned figure");
    }

    /// @notice Companion to `test_redeemMany_gas_NVouchers` — same setup,
    ///         one more voucher. See that test's docstring for why this is a
    ///         separate function rather than a second call inside it.
    function test_redeemMany_gas_NPlus1Vouchers() public {
        if (_skipGasBenchmarkUnderCoverage()) return;
        uint256 gasUsed = _redeemFreshLanesAndSnapshotGas(REDEEM_MANY_GAS_BENCHMARK_N + 1, "redeemMany_marginal_Np1");
        assertLt(gasUsed, 2_000_000, "sanity ceiling on a small fixed-N batch");
        emit log_named_uint("redeemMany gas, N+1 vouchers", gasUsed);
        // Pins the per-call gas cited by `DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX`'s
        // doc comment (crates/common/src/config/mod.rs): N+1 = 550,993 gas.
        // Tolerance covers toolchain/compiler-version gas drift without
        // masking a real regression. See `test_redeemMany_gas_NVouchers` for
        // what the per-lane figure covers.
        assertApproxEqAbs(gasUsed, 550_993, 5000, "redeemMany gas for N+1 vouchers drifted from the pinned figure");
    }

    /// @dev Opens a fresh pool and, unlike `_redeemFreshLanesAndSnapshotGas`,
    ///      does NOT pre-register the lanes' capabilities in a separate call.
    ///      Every signer is brand-new: its `CapabilityReg` rides in the SAME
    ///      measured `redeemMany` as its voucher, one owner-signed
    ///      registration plus one cold voucher-verify-and-settle per lane —
    ///      the realistic shape of this feature's target workload (millions
    ///      of one-time payers, each a new signer registered on its single
    ///      redemption). Pins the call's gas via `snapshotGasLastCall`, same
    ///      as the registered-signer helper above.
    function _redeemFirstTimeLanesAndSnapshotGas(uint256 count, string memory snapshotName) internal returns (uint256) {
        uint64 amount = 1e6;
        uint64 bytesDelivered = 100_000;
        uint256 seed = uint256(keccak256(bytes(snapshotName)));

        bytes32 id = _open();
        uint256[] memory pks = new uint256[](count);
        address[] memory signers = new address[](count);
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](count);
        for (uint256 i = 0; i < count; i++) {
            uint256 pk = seed + i + 1;
            address who = vm.addr(pk);
            pks[i] = pk;
            signers[i] = who;
            caps[i] = PaymentPool.CapabilityReg({
                signer: who,
                spendingCap: SPENDING_CAP,
                expiry: expiry,
                ownerSig: _signCapabilityFor(address(pool), id, who, SPENDING_CAP, expiry, OWNER_PK)
            });
        }
        PaymentPool.LaneVoucher[] memory vouchers = _lanesOf(id, pks, signers, amount, bytesDelivered);

        vm.prank(provider);
        uint256 totalPaid = pool.redeemMany(_batch(id, caps, vouchers));
        assertEq(totalPaid, count * uint256(amount), "every first-time lane must actually settle");
        return vm.snapshotGasLastCall("PaymentPool", snapshotName);
    }

    /// @notice First-time-signer companion to `test_redeemMany_gas_NVouchers`:
    ///         same N, but every lane's `CapabilityReg` rides in THIS
    ///         measured call instead of an earlier unmeasured one — the
    ///         realistic per-lane cost for a one-time payer whose capability
    ///         is registered on its single redemption. See
    ///         `test_redeemMany_gas_NVouchers`'s docstring for why this needs
    ///         its own test function (fresh, cold EVM state) rather than a
    ///         second call sharing a function with its N+1 companion.
    function test_redeemMany_gas_firstTime_NVouchers() public {
        if (_skipGasBenchmarkUnderCoverage()) return;
        uint256 gasUsed = _redeemFirstTimeLanesAndSnapshotGas(REDEEM_MANY_GAS_BENCHMARK_N, "redeemMany_firstTime_N");
        assertLt(gasUsed, 3_000_000, "sanity ceiling on a small fixed-N batch");
        emit log_named_uint("redeemMany gas, first-time N vouchers", gasUsed);
        // Pinned from an actual `forge test -vv` run on this branch. Includes
        // N owner-signed capability registrations plus N cold voucher
        // settlements in one call.
        assertApproxEqAbs(
            gasUsed, 804_352, 8000, "redeemMany gas for first-time N vouchers drifted from the pinned figure"
        );
    }

    /// @notice Companion to `test_redeemMany_gas_firstTime_NVouchers` — same
    ///         setup, one more first-time lane.
    ///         `gas(firstTime N+1) - gas(firstTime N)` is the first-time
    ///         marginal: the per-lane cost of this feature's target
    ///         workload, where a one-time payer's capability registration
    ///         and voucher settlement both land in its single redemption.
    function test_redeemMany_gas_firstTime_NPlus1Vouchers() public {
        if (_skipGasBenchmarkUnderCoverage()) return;
        uint256 gasUsed =
            _redeemFirstTimeLanesAndSnapshotGas(REDEEM_MANY_GAS_BENCHMARK_N + 1, "redeemMany_firstTime_Np1");
        assertLt(gasUsed, 3_000_000, "sanity ceiling on a small fixed-N batch");
        emit log_named_uint("redeemMany gas, first-time N+1 vouchers", gasUsed);
        // Pinned from an actual `forge test -vv` run on this branch.
        assertApproxEqAbs(
            gasUsed, 868_611, 8000, "redeemMany gas for first-time N+1 vouchers drifted from the pinned figure"
        );
    }

    // -----------------------------------------------------------------
    // closePool
    // -----------------------------------------------------------------

    function test_closePool_onlyOwner() public {
        bytes32 id = _open();
        vm.prank(stranger);
        vm.expectRevert(PaymentPool.NotPoolOwner.selector);
        pool.closePool(id);
    }

    function test_closePool_setsClosingAndDeadline() public {
        bytes32 id = _open();
        uint256 expectedDeadline = block.timestamp + DISPUTE_WINDOW;

        vm.expectEmit(true, true, false, true, address(pool));
        emit PoolCloseInitiated(id, owner, expectedDeadline);
        vm.prank(owner);
        pool.closePool(id);

        PaymentPool.Pool memory p = pool.getPool(id);
        assertEq(uint256(p.status), uint256(PaymentPool.Status.Closing));
        assertEq(uint256(p.disputeDeadline), expectedDeadline);
    }

    function test_closePool_revertsWhenNotOpen() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);

        vm.prank(owner);
        vm.expectRevert(PaymentPool.PoolNotOpen.selector);
        pool.closePool(id);
    }

    function test_closePool_movesNoFunds() public {
        bytes32 id = _open();
        uint256 balBefore = usdc.balanceOf(address(pool));

        vm.prank(owner);
        pool.closePool(id);

        assertEq(usdc.balanceOf(address(pool)), balBefore, "closePool moves no funds");
        assertEq(pool.getPool(id).deposit, DEPOSIT, "deposit untouched");
    }

    // -----------------------------------------------------------------
    // reclaim
    // -----------------------------------------------------------------

    function test_reclaim_revertsBeforeDeadline() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);

        vm.expectRevert(PaymentPool.GraceWindowActive.selector);
        pool.reclaim(id);
    }

    function test_reclaim_revertsWhenNotClosing() public {
        bytes32 id = _open();
        vm.expectRevert(PaymentPool.PoolNotClosing.selector);
        pool.reclaim(id);
    }

    function test_reclaim_transfersRemainderAfterDeadline() public {
        bytes32 id = _open();

        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        vm.prank(owner);
        pool.closePool(id);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        uint256 ownerBalBefore = usdc.balanceOf(owner);
        uint256 expectedRefund = DEPOSIT - 300e6;

        pool.reclaim(id);

        assertEq(usdc.balanceOf(owner), ownerBalBefore + expectedRefund);
        assertEq(
            usdc.balanceOf(address(pool)), 0, "the routed leg already left at redeem time; reclaim drains the rest"
        );
    }

    function test_reclaim_callableByAnyoneRefundsOwner() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        uint256 ownerBalBefore = usdc.balanceOf(owner);
        vm.prank(stranger);
        pool.reclaim(id);

        assertEq(usdc.balanceOf(owner), ownerBalBefore + DEPOSIT);
    }

    function test_reclaim_setsClosed() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        pool.reclaim(id);

        assertEq(uint256(pool.getPool(id).status), uint256(PaymentPool.Status.Closed));
    }

    function test_reclaim_routerNotCalled() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        uint256 callsBefore = router.callCount();
        pool.reclaim(id);
        assertEq(router.callCount(), callsBefore, "reclaim never calls the router");
    }

    function test_reclaim_emitsPoolReclaimed() public {
        bytes32 id = _open();
        vm.prank(owner);
        pool.closePool(id);
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        vm.expectEmit(true, true, false, true, address(pool));
        emit PoolReclaimed(id, owner, DEPOSIT);
        pool.reclaim(id);
    }

    // -----------------------------------------------------------------
    // getPools
    // -----------------------------------------------------------------

    function test_getPools_emptyBeforeAnyOpen() public view {
        bytes32[] memory page = pool.getPools(owner, 0, 10);
        assertEq(page.length, 0);
    }

    function test_getPools_recomputesIdsFromNonce_paged() public {
        bytes32 id0 = _open();
        bytes32 id1 = _open();
        bytes32 id2 = _open();

        bytes32[] memory page = pool.getPools(owner, 0, 10);
        assertEq(page.length, 3);
        assertEq(page[0], id0);
        assertEq(page[1], id1);
        assertEq(page[2], id2);

        bytes32[] memory mid = pool.getPools(owner, 1, 1);
        assertEq(mid.length, 1);
        assertEq(mid[0], id1);
    }

    function test_getPools_paginationBoundaries() public {
        _open();
        _open();

        // offset >= len returns empty.
        assertEq(pool.getPools(owner, 2, 10).length, 0);
        assertEq(pool.getPools(owner, 5, 10).length, 0);

        // limit == 0 returns empty.
        assertEq(pool.getPools(owner, 0, 0).length, 0);

        // offset + limit clamped to len, no overflow with a huge limit.
        bytes32[] memory page = pool.getPools(owner, 0, type(uint256).max);
        assertEq(page.length, 2);

        // an owner who never opened a pool gets an empty page, not a revert.
        assertEq(pool.getPools(stranger, 0, 10).length, 0);
    }

    // -----------------------------------------------------------------
    // getAuthorization / getWatermark
    // -----------------------------------------------------------------

    function test_getAuthorization_returnsCapExpirySpent() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        PaymentPool.Authorization memory a = pool.getAuthorization(id, signer);
        assertEq(a.cap, SPENDING_CAP);
        assertEq(uint256(a.expiry), uint256(expiry));
        assertEq(a.spent, 300e6);
    }

    function test_getWatermark_returnsLane() public {
        bytes32 id = _open();
        vm.prank(provider);
        _redeemOne(
            id, signer, provider, 300e6, 30_000_000, _voucher(id, 300e6, 30_000_000), _cap(id, SPENDING_CAP, expiry)
        );

        PaymentPool.Lane memory lane = pool.getWatermark(id, signer, provider);
        assertEq(lane.amount, 300e6);
        assertEq(lane.bytesDelivered, 30_000_000);
    }

    function test_getAuthorizations_batchesRegisteredAndUnregistered() public {
        bytes32 id0 = _open();
        vm.prank(provider);
        _redeemOne(
            id0, signer, provider, 300e6, 30_000_000, _voucher(id0, 300e6, 30_000_000), _cap(id0, SPENDING_CAP, expiry)
        );
        bytes32 id1 = _open(); // signer never registered here

        bytes32[] memory ids = new bytes32[](2);
        ids[0] = id0;
        ids[1] = id1;
        address[] memory signers = new address[](2);
        signers[0] = signer;
        signers[1] = signer;

        PaymentPool.Authorization[] memory auths = pool.getAuthorizations(ids, signers);
        assertEq(auths.length, 2);
        // Registered lane: identical to the single-read view.
        assertEq(auths[0].cap, SPENDING_CAP);
        assertEq(uint256(auths[0].expiry), uint256(expiry));
        assertEq(auths[0].spent, 300e6);
        // Unregistered lane: zero cap (the redeemer's "attach a CapabilityReg" signal).
        assertEq(auths[1].cap, 0);
        assertEq(uint256(auths[1].expiry), 0);
        assertEq(auths[1].spent, 0);
        // Each entry equals the single-read view for the same pair.
        assertEq(auths[0].cap, pool.getAuthorization(id0, signer).cap);
        assertEq(auths[1].cap, pool.getAuthorization(id1, signer).cap);
    }

    function test_getAuthorizations_revertsOnLengthMismatch() public {
        bytes32[] memory ids = new bytes32[](2);
        address[] memory signers = new address[](1);
        vm.expectRevert(PaymentPool.LengthMismatch.selector);
        pool.getAuthorizations(ids, signers);
    }

    // -----------------------------------------------------------------
    // getRateBounds
    // -----------------------------------------------------------------

    function test_getRateBounds_returnsDeliveryFloor() public view {
        assertEq(pool.getRateBounds(), DELIVERY_FLOOR);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setDisputeWindow_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector, uint256(1 hours), uint256(48 hours), uint256(72 hours)
            )
        );
        pool.setDisputeWindow(1 hours);

        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector, uint256(73 hours), uint256(48 hours), uint256(72 hours)
            )
        );
        pool.setDisputeWindow(73 hours);

        vm.expectEmit(false, false, false, true, address(pool));
        emit DisputeWindowUpdated(DISPUTE_WINDOW, 60 hours);
        vm.prank(admin);
        pool.setDisputeWindow(60 hours);
        assertEq(pool.disputeWindow(), 60 hours);
    }

    function test_setDisputeWindow_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        pool.setDisputeWindow(60 hours);
    }

    function test_setRateBounds_enforcesWireCap() public {
        uint256 badFloor = MAX_RATE_PER_MB + 1;
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, badFloor));
        pool.setRateBounds(badFloor);
    }

    function test_setRateBounds_enforcesMin() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.RateBoundsInvalid.selector, uint256(0)));
        pool.setRateBounds(0);
    }

    function test_setRateBounds_belowU64Cap() public pure {
        // The daemon decodes the floor as `u64`; pin that the governable
        // ceiling stays far below `type(uint64).max` so nothing in that gap
        // is silently network-isolating.
        assertLt(MAX_RATE_PER_MB, type(uint64).max);
    }

    function test_setRateBounds_updatesFloor() public {
        vm.expectEmit(false, false, false, true, address(pool));
        emit RateBoundsUpdated(1000);
        vm.prank(admin);
        pool.setRateBounds(1000);
        assertEq(pool.getRateBounds(), 1000);
    }

    function test_setMinDeposit_enforcesCeiling() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentPool.ParamOutOfBounds.selector,
                uint256(MIN_DEPOSIT_CEILING) + 1,
                uint256(0),
                uint256(MIN_DEPOSIT_CEILING)
            )
        );
        pool.setMinDeposit(MIN_DEPOSIT_CEILING + 1);

        vm.prank(admin);
        pool.setMinDeposit(MIN_DEPOSIT_CEILING);
        assertEq(pool.minDeposit(), MIN_DEPOSIT_CEILING, "the ceiling itself is settable");
    }

    function test_setMinDeposit_updatesEmitsAndReturnsToDormant() public {
        vm.expectEmit(false, false, false, true, address(pool));
        emit MinDepositUpdated(0, ARMED_MIN);
        vm.prank(admin);
        pool.setMinDeposit(ARMED_MIN);
        assertEq(pool.minDeposit(), ARMED_MIN);

        // Setting back to 0 returns the knob to dormant: any non-zero
        // deposit opens again.
        vm.expectEmit(false, false, false, true, address(pool));
        emit MinDepositUpdated(ARMED_MIN, 0);
        vm.prank(admin);
        pool.setMinDeposit(0);
        vm.prank(owner);
        bytes32 id = pool.openPool(1);
        assertEq(pool.getPool(id).deposit, 1, "dormant again after reset");
    }

    function test_setMinDeposit_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        pool.setMinDeposit(ARMED_MIN);
    }

    function test_setters_onlyGovernance() public {
        vm.startPrank(stranger);

        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        pool.setRateBounds(1000);

        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        pool.setDisputeWindow(60 hours);

        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        pool.setMinDeposit(ARMED_MIN);

        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Regression guards: the pairwise-channel surface stays removed
    // -----------------------------------------------------------------

    function test_reclaimExpired_removed() public {
        (bool ok,) = address(pool).call(abi.encodeWithSignature("reclaimExpired(bytes32)", bytes32(0)));
        assertFalse(ok, "reclaimExpired must not exist on PaymentPool");
    }

    function test_cooperativeClose_removed() public {
        (bool ok,) = address(pool)
            .call(
                abi.encodeWithSignature(
                    "cooperativeClose(bytes32,uint256,uint256,bytes,bytes)", bytes32(0), 0, 0, "", ""
                )
            );
        assertFalse(ok, "cooperativeClose must not exist on PaymentPool");
    }

    function test_disputeChannel_removed() public {
        (bool ok,) = address(pool)
            .call(
                abi.encodeWithSignature(
                    "disputeChannel(bytes32,uint256,uint256,uint256,bytes)", bytes32(0), 0, 0, 0, ""
                )
            );
        assertFalse(ok, "disputeChannel must not exist on PaymentPool");
    }

    function test_settleChannel_removed() public {
        (bool ok,) = address(pool).call(abi.encodeWithSignature("settleChannel(bytes32)", bytes32(0)));
        assertFalse(ok, "settleChannel must not exist on PaymentPool");
    }

    function test_maxChannelDuration_getterRemoved() public {
        (bool ok,) = address(pool).call(abi.encodeWithSignature("maxChannelDuration()"));
        assertFalse(ok, "maxChannelDuration must not exist on PaymentPool");
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
        uint64 minDeposit_,
        address admin
    ) PaymentPool(usdc_, capacityBond_, feeRouter_, disputeWindow_, deliveryFloor_, minDeposit_, admin) { }

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
