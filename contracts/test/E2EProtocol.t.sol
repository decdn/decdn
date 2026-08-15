// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { PaymentPool } from "../src/PaymentPool.sol";
import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

contract E2EUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @title E2EProtocolTest — full-lifecycle integration over the deployed,
///        governance-handed-off contract surface (ADR 003 § PaymentPool):
///        open a pool → a signer redeems a voucher against a node, self-
///        registering its owner-signed capability on first redemption →
///        settle through `FeeRouter` → 3-bucket distribution.
/// @dev   Runs the real `_runFullDeploy` pipeline (mock ed25519 verifier +
///        mock USDC) so `PaymentPool` is wired exactly as production. The
///        capability + voucher EIP-712 signing mirrors the helpers in
///        `PaymentPool.t.sol`, rebuilt here against the real deployed pool
///        and the real `FeeRouter` rather than the unit-test mocks.
contract E2EProtocolTest is Test, BaseProtocolDeploy {
    Deployment internal d;
    DeployConfig internal cfg;
    E2EUSDC internal usdc;

    uint256 internal constant OWNER_PK = 0xC11E27;
    uint256 internal constant SIGNER_PK = 0x519E7;
    address internal owner;
    address internal signer;
    address internal operator = address(0x0FE1);
    address internal emergencyMultisig = address(0xC0DE);
    address internal tokenHolder = address(0x70);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint64 internal constant DEPOSIT = 1000e6;
    uint64 internal constant SPENDING_CAP = 800e6;
    uint64 internal constant SETTLE_AMOUNT = 800e6;
    uint64 internal constant SETTLE_BYTES = 80_000_000;

    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered)");

    function setUp() public {
        owner = vm.addr(OWNER_PK);
        signer = vm.addr(SIGNER_PK);

        usdc = new E2EUSDC();
        cfg = DeployConfig({
            usdc: usdc,
            ed25519Verifier: new MockEd25519Verifier(),
            deployer: address(this),
            emergencyMultisig: emergencyMultisig,
            initialTokenHolder: tokenHolder,
            initialVetter: address(0),
            timelockDelay: 48 hours,
            // Direct-to-Timelock handoff; the ADR 009 bootstrap phase is opt-in.
            bootstrapMultisig: address(0),
            minBond: MIN_BOND,
            unbondingPeriod: 14 days,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            currentTermsHash: keccak256("decdn operator terms v1"),
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            feeRouterShares: [uint256(9000), uint256(0), uint256(1000)],
            buybackBurner: address(0),
            slashAppealBond: 1000e18
        });
        d = _runFullDeploy(cfg);

        vm.warp(1_000_000);

        // Fund the pool owner with the USDC it will deposit.
        usdc.transfer(owner, 10_000e6);
        vm.prank(owner);
        usdc.approve(address(d.paymentPool), type(uint256).max);
    }

    // -----------------------------------------------------------------
    // Deploy wiring
    // -----------------------------------------------------------------

    function test_deployWiring_paymentPoolWiredAndHandedOff() public view {
        // Post-deployment role grants (ADR 016 § Post-Deployment Initialization).
        assertTrue(d.router.hasRole(d.router.ROUTER_CALLER_ROLE(), address(d.paymentPool)), "paymentPool ROUTER_CALLER");
        assertTrue(d.paymentPool.hasRole(d.paymentPool.PAUSER_ROLE(), emergencyMultisig), "paymentPool PAUSER");

        // Constructor dependency wiring.
        assertEq(d.paymentPool.feeRouter(), address(d.router), "paymentPool.feeRouter");
        assertEq(address(d.paymentPool.capacityBond()), address(d.bond), "paymentPool.capacityBond");

        // Governance handoff: Timelock governs PaymentPool; deployer holds nothing.
        address tl = address(d.timelock);
        assertTrue(d.paymentPool.hasRole(GOVERNANCE_ROLE, tl), "paymentPool gov");
        assertFalse(d.paymentPool.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer), "no paymentPool back door");
    }

    // -----------------------------------------------------------------
    // Full redemption lifecycle
    // -----------------------------------------------------------------

    function test_e2e_openPoolRedeemSettlesThroughFeeRouter() public {
        vm.prank(owner);
        bytes32 poolId = d.paymentPool.openPool(DEPOSIT);

        uint64 expiry = uint64(block.timestamp + 365 days);
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _capabilitySig(poolId, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        (bytes32 vr, bytes32 vvs) = _voucherSig(poolId, signer, operator, SETTLE_AMOUNT, SETTLE_BYTES, SIGNER_PK);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = PaymentPool.LaneVoucher({
            signer: signer, cumulative: SETTLE_AMOUNT, bytesDelivered: SETTLE_BYTES, r: vr, vs: vvs
        });
        PaymentPool.PoolBatch[] memory batches = new PaymentPool.PoolBatch[](1);
        batches[0] = PaymentPool.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: vouchers });

        PaymentPool.LaneSettled[] memory expected = new PaymentPool.LaneSettled[](1);
        expected[0] =
            PaymentPool.LaneSettled({ signer: signer, newPaidCumulative: SETTLE_AMOUNT, bytesPaid: SETTLE_BYTES });
        vm.expectEmit(true, true, true, true, address(d.paymentPool));
        emit PoolRedeemed(poolId, operator, expected);

        vm.prank(operator);
        d.paymentPool.redeemMany(batches);

        // Three-bucket distribution at the configured launch shares (read from
        // cfg, not re-hardcoded, so the assertion tracks the deploy config).
        assertEq(
            usdc.balanceOf(operator),
            SETTLE_AMOUNT * cfg.feeRouterShares[0] / 10_000,
            "operator base share (operator bucket)"
        );
        assertEq(usdc.balanceOf(address(d.timelock)), SETTLE_AMOUNT * cfg.feeRouterShares[2] / 10_000, "treasury share");
        assertEq(usdc.balanceOf(d.router.buybackBurner()), 0, "buyback dormant at launch");

        uint256 epoch = block.timestamp / cfg.feeRouterEpochLength;
        assertEq(d.router.bytesPerEpoch(operator, uint64(epoch)), SETTLE_BYTES, "served bytes stamped");

        // Watermark + authorization advanced.
        PaymentPool.Lane memory lane = d.paymentPool.getWatermark(poolId, signer, operator);
        assertEq(lane.amount, SETTLE_AMOUNT, "watermark cumulative advanced");
        assertEq(lane.bytesDelivered, SETTLE_BYTES, "watermark bytes advanced");
    }

    // -----------------------------------------------------------------
    // EIP-712 signing helpers
    // -----------------------------------------------------------------

    function _domainSeparator(address verifyingContract) internal view returns (bytes32) {
        return keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PaymentPool")),
                keccak256(bytes("1")),
                block.chainid,
                verifyingContract
            )
        );
    }

    function _capabilitySig(bytes32 poolId, address signer_, uint64 spendingCap, uint64 expiry, uint256 pk)
        internal
        view
        returns (bytes memory)
    {
        bytes32 structHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer_, spendingCap, poolId, expiry));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(address(d.paymentPool)), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }

    /// @dev The EIP-2098 compact pair the contract takes: `vs` is `s` with the
    ///      recovery bit folded into its top bit.
    function _voucherSig(
        bytes32 poolId,
        address signer_,
        address provider_,
        uint64 amount,
        uint64 bytesDelivered,
        uint256 pk
    ) internal view returns (bytes32, bytes32) {
        bytes32 structHash = keccak256(abi.encode(VOUCHER_TYPEHASH, poolId, signer_, provider_, amount, bytesDelivered));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(address(d.paymentPool)), structHash));
        (uint8 v, bytes32 r, bytes32 sv) = vm.sign(pk, digest);
        return (r, bytes32(uint256(sv) | (uint256(v - 27) << 255)));
    }

    /// @dev Mirrors `PaymentPool.PoolRedeemed` for `vm.expectEmit`.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, PaymentPool.LaneSettled[] lanes);
}
