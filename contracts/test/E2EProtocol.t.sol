// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { PaymentPool } from "../src/PaymentPool.sol";
import { PaymentPoolBase } from "../src/PaymentPoolBase.sol";
import { ICapacityBondCredit } from "../src/interfaces/ICapacityBondCredit.sol";
import { IMessageTransmitterV2, ITokenMessengerV2 } from "../src/interfaces/ICctpV2.sol";
import { LoopbackCctp } from "../testnet/LoopbackCctp.sol";
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
///        sweep the protocol share through CCTP → `CctpSweepReceiver` deposits
///        it into `FeeRouter` → rebate, treasury, and served-bytes credit.
/// @dev   Runs the real `_runFullDeploy` pipeline (mock ed25519 verifier +
///        mock USDC) with `LoopbackCctp` as the governance chain's CCTP, and a
///        `PaymentPool` on the same chain that sweeps into the deployed
///        receiver. The capability + voucher EIP-712 signing mirrors the
///        helpers in `PaymentPool.t.sol`.
contract E2EProtocolTest is Test, BaseProtocolDeploy {
    Deployment internal d;
    DeployConfig internal cfg;
    E2EUSDC internal usdc;
    LoopbackCctp internal cctp;
    PaymentPool internal pool;

    uint256 internal constant OWNER_PK = 0xC11E27;
    uint256 internal constant SIGNER_PK = 0x519E7;
    address internal owner;
    address internal signer;
    address internal operator = address(0x0FE1);
    address internal emergencyMultisig = address(0xC0DE);
    address internal tokenHolder = address(0x70);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant OPERATOR_BPS = 6000;
    uint64 internal constant DEPOSIT = 1000e6;
    uint64 internal constant SPENDING_CAP = 800e6;
    uint64 internal constant SETTLE_AMOUNT = 800e6;
    uint64 internal constant SETTLE_BYTES = 80_000_000;

    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );

    function setUp() public {
        owner = vm.addr(OWNER_PK);
        signer = vm.addr(SIGNER_PK);

        usdc = new E2EUSDC();
        cctp = new LoopbackCctp(0);
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
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            currentTermsHash: keccak256("decdn operator terms v1"),
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            // Launch split: the operator gets 90% of a redemption end to end
            // (6000 in the pool, then 7500 of the swept 40%), the treasury 10%.
            feeRouterRebateBps: 7500,
            feeRouterDeliveryFloor: 1,
            cctpMessageTransmitter: IMessageTransmitterV2(address(cctp)),
            cctpTokenMessenger: address(cctp)
        });
        d = _runFullDeploy(cfg);

        pool = new PaymentPool({
            usdc_: usdc,
            tokenMessenger_: ITokenMessengerV2(address(cctp)),
            sweepRecipient_: address(d.sweepReceiver),
            sweepDomain_: 0,
            disputeWindow_: 48 hours,
            operatorBps_: OPERATOR_BPS,
            minSweep_: 1,
            minDeposit_: 0,
            pauser_: emergencyMultisig
        });

        vm.warp(1_000_000);

        // Fund the pool owner with the USDC it will deposit.
        usdc.transfer(owner, 10_000e6);
        vm.prank(owner);
        usdc.approve(address(pool), type(uint256).max);
    }

    // -----------------------------------------------------------------
    // Full redemption lifecycle
    // -----------------------------------------------------------------

    function test_e2e_openPoolRedeemSweepSettlesThroughFeeRouter() public {
        vm.prank(owner);
        bytes32 poolId = pool.openPool(DEPOSIT);

        _redeem(poolId);

        uint256 operatorShare = uint256(SETTLE_AMOUNT) * OPERATOR_BPS / 10_000;
        uint256 protocolShare = SETTLE_AMOUNT - operatorShare;
        assertEq(usdc.balanceOf(operator), operatorShare, "same-transaction operator share");
        (uint256 pendingBytes, uint256 pendingUsdc) = pool.pending(operator);
        assertEq(pendingUsdc, protocolShare, "protocol share pending");
        assertEq(pendingBytes, SETTLE_BYTES, "served bytes pending");

        // `FeeRouter.deposit` rebates and credits only an active operator with
        // a declared capacity; stand the operator up as both without a full
        // onboarding. The floor tier's byte ceiling far exceeds SETTLE_BYTES.
        vm.mockCall(address(d.bond), abi.encodeCall(ICapacityBondCredit.isActive, (operator)), abi.encode(true));
        vm.mockCall(
            address(d.bond), abi.encodeCall(ICapacityBondCredit.declaredMbps, (operator)), abi.encode(uint256(10))
        );

        vm.recordLogs();
        address[] memory operators = new address[](1);
        operators[0] = operator;
        pool.sweep(operators);
        d.sweepReceiver.receiveSweep(_lastCctpMessage(), hex"01");

        // Launch split, read from cfg so the assertion tracks the deploy config.
        uint256 rebate = protocolShare * cfg.feeRouterRebateBps / 10_000;
        assertEq(usdc.balanceOf(operator), operatorShare + rebate, "operator share plus rebate");
        assertEq(usdc.balanceOf(address(d.timelock)), protocolShare - rebate, "treasury share");
        assertEq(usdc.balanceOf(address(d.router)), 0, "router holds nothing");

        uint256 epoch = block.timestamp / cfg.feeRouterEpochLength;
        assertEq(d.router.bytesPerEpoch(operator, uint64(epoch)), SETTLE_BYTES, "served bytes credited");

        // Watermark + authorization advanced.
        PaymentPoolBase.Lane memory lane = pool.getWatermark(poolId, signer, operator);
        assertEq(lane.amount, SETTLE_AMOUNT, "watermark cumulative advanced");
        assertEq(lane.bytesDelivered, SETTLE_BYTES, "watermark bytes advanced");
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    /// @dev `operator` redeems one sealed voucher for `SETTLE_AMOUNT` against
    ///      `poolId`, registering the signer's capability in the same batch.
    function _redeem(bytes32 poolId) internal {
        uint64 expiry = uint64(block.timestamp + 365 days);
        PaymentPoolBase.CapabilityReg[] memory caps = new PaymentPoolBase.CapabilityReg[](1);
        caps[0] = PaymentPoolBase.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _capabilitySig(poolId, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        (bytes32 vr, bytes32 vvs) = _voucherSig(poolId, signer, operator, SETTLE_AMOUNT, SETTLE_BYTES, SIGNER_PK);
        PaymentPoolBase.LaneVoucher[] memory vouchers = new PaymentPoolBase.LaneVoucher[](1);
        vouchers[0] = PaymentPoolBase.LaneVoucher({
            signer: signer,
            cumulative: SETTLE_AMOUNT,
            bytesDelivered: SETTLE_BYTES,
            r: vr,
            vs: vvs,
            chainRoot: bytes32(0),
            preimage: bytes32(0),
            chainMeter: 0
        });
        PaymentPoolBase.PoolBatch[] memory batches = new PaymentPoolBase.PoolBatch[](1);
        batches[0] = PaymentPoolBase.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: vouchers });

        PaymentPoolBase.LaneSettled[] memory expected = new PaymentPoolBase.LaneSettled[](1);
        expected[0] =
            PaymentPoolBase.LaneSettled({ signer: signer, newPaidCumulative: SETTLE_AMOUNT, bytesPaid: SETTLE_BYTES });

        vm.expectEmit(true, true, true, true, address(pool));
        emit PoolRedeemed(poolId, operator, expected);

        vm.prank(operator);
        pool.redeemMany(batches);
    }

    /// @dev The burn message of the last `LoopbackCctp.MessageSent` in the
    ///      recorded logs.
    function _lastCctpMessage() internal view returns (bytes memory) {
        Vm.Log[] memory logs = vm.getRecordedLogs();
        bytes32 sig = keccak256("MessageSent(bytes)");
        for (uint256 i = logs.length; i > 0; i--) {
            if (logs[i - 1].emitter == address(cctp) && logs[i - 1].topics[0] == sig) {
                return abi.decode(logs[i - 1].data, (bytes));
            }
        }
        revert("no MessageSent");
    }

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
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(address(pool)), structHash));
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
        // The cooperative close: a sealed voucher (zero root, zero price)
        // settling exactly `amount`, which is the shape a finalized delivery
        // actually redeems (ADR 003 §The chain walk is fallback-only).
        bytes32 structHash = keccak256(
            abi.encode(VOUCHER_TYPEHASH, poolId, signer_, provider_, amount, bytesDelivered, bytes32(0), uint256(0))
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(address(pool)), structHash));
        (uint8 v, bytes32 r, bytes32 sv) = vm.sign(pk, digest);
        return (r, bytes32(uint256(sv) | (uint256(v - 27) << 255)));
    }

    /// @dev Mirrors `PaymentPoolBase.PoolRedeemed` for `vm.expectEmit`.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, PaymentPoolBase.LaneSettled[] lanes);
}
