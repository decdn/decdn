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
    bytes32 internal constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );

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
            signer: signer,
            cumulative: SETTLE_AMOUNT,
            bytesDelivered: SETTLE_BYTES,
            r: vr,
            vs: vvs,
            chainRoot: bytes32(0),
            preimage: bytes32(0),
            chainMeter: 0
        });
        PaymentPool.PoolBatch[] memory batches = new PaymentPool.PoolBatch[](1);
        batches[0] = PaymentPool.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: vouchers });

        PaymentPool.LaneSettled[] memory expected = new PaymentPool.LaneSettled[](1);
        expected[0] =
            PaymentPool.LaneSettled({ signer: signer, newPaidCumulative: SETTLE_AMOUNT, bytesPaid: SETTLE_BYTES });
        // Redemption pays only a staked operator and stamps bytes only for an
        // active one; stand the operator up as both without a full onboarding.
        vm.mockCall(address(d.bond), abi.encodeWithSignature("payeeStatus(address)", operator), abi.encode(true, true));

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
    // Payee gate (#2834): a payee needs stake, not just a binding
    // -----------------------------------------------------------------

    /// @notice A capability holder binds its own address with `bindNodeId`,
    ///         which needs no bond, then signs a voucher for its cap naming
    ///         itself as provider. The redeem reverts: a binding is not stake.
    function test_e2e_redeemMany_boundButUnbondedPayeeReverts() public {
        bytes32 poolId = _openPool();
        _bind(SIGNER_PK, signer, bytes32(uint256(0x5E1F)));
        (bytes32 nodeId, bool active) = d.bond.nodeIdOf(signer);
        assertEq(nodeId, bytes32(uint256(0x5E1F)), "the capability holder holds a binding");
        assertFalse(active, "and no registration");

        PaymentPool.PoolBatch[] memory batches = _settleBatch(poolId, signer);
        vm.prank(signer);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.ProviderNotStaked.selector, signer));
        d.paymentPool.redeemMany(batches);
        assertEq(usdc.balanceOf(signer), 0, "no USDC reaches the self-dealing holder");
    }

    /// @notice A bonded node that queues its whole bond for unbonding still
    ///         redeems the vouchers it earned: unbonding bond is stake.
    function test_e2e_redeemMany_unbondingOperatorStillPaid() public {
        uint256 opPk = 0xB0DED;
        address op = vm.addr(opPk);
        vm.prank(tokenHolder);
        d.token.transfer(op, MIN_BOND);
        vm.startPrank(op);
        d.token.approve(address(d.bond), MIN_BOND);
        d.bond.bond(MIN_BOND);
        vm.stopPrank();
        _bind(opPk, op, bytes32(uint256(0xB0DED)));
        vm.prank(op);
        d.bond.requestUnbond(MIN_BOND);
        assertEq(d.bond.activeBond(op), 0, "the whole bond is unbonding");

        bytes32 poolId = _openPool();
        PaymentPool.PoolBatch[] memory batches = _settleBatch(poolId, op);
        vm.prank(op);
        d.paymentPool.redeemMany(batches);
        assertEq(usdc.balanceOf(op), SETTLE_AMOUNT * cfg.feeRouterShares[0] / 10_000, "operator share paid");
        uint256 epoch = block.timestamp / cfg.feeRouterEpochLength;
        assertEq(d.router.bytesPerEpoch(op, uint64(epoch)), 0, "an inactive payee adds no governance bytes");
    }

    /// @notice An ejected operator is never paid, even with its bond intact.
    function test_e2e_redeemMany_ejectedOperatorReverts() public {
        address op = _registerBonded(0xE7EC7);
        (bool payee,) = d.bond.payeeStatus(op);
        assertTrue(payee, "a registered bonded operator is a payee");

        vm.prank(address(d.blacklist));
        d.bond.ejectNode(op);
        assertEq(d.bond.activeBond(op), MIN_BOND, "ejection leaves the bond in place");

        PaymentPool.PoolBatch[] memory batches = _settleBatch(_openPool(), op);
        vm.prank(op);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.ProviderNotStaked.selector, op));
        d.paymentPool.redeemMany(batches);
    }

    /// @notice A slash below the entry floor that does not eject leaves the
    ///         operator inactive, so it is not paid until it tops up to its
    ///         floor. After the top-up the same batch pays.
    function test_e2e_redeemMany_slashedBelowFloorPaidAfterTopUp() public {
        address op = _registerBonded(0x5A5ED);
        vm.prank(address(d.slashJudge));
        (, uint256 slashed) = d.bond.slash(op, address(0xC4A11), 1, bytes32(0));
        assertFalse(d.bond.ejected(op), "a tier-1 slash does not eject");
        assertFalse(d.bond.isActive(op), "but leaves the operator below its floor");

        PaymentPool.PoolBatch[] memory batches = _settleBatch(_openPool(), op);
        vm.prank(op);
        vm.expectRevert(abi.encodeWithSelector(PaymentPool.ProviderNotStaked.selector, op));
        d.paymentPool.redeemMany(batches);

        vm.prank(tokenHolder);
        d.token.transfer(op, slashed);
        vm.prank(op);
        d.bond.bond(slashed);
        assertTrue(d.bond.isActive(op), "the top-up restores the floor");

        vm.prank(op);
        d.paymentPool.redeemMany(batches);
        assertEq(usdc.balanceOf(op), SETTLE_AMOUNT * cfg.feeRouterShares[0] / 10_000, "paid after the top-up");
    }

    /// @dev Bond `MIN_BOND` and `registerNode` for the key `pk`.
    function _registerBonded(uint256 pk) internal returns (address op) {
        op = vm.addr(pk);
        bytes32 nodeId = bytes32(pk);
        vm.prank(tokenHolder);
        d.token.transfer(op, MIN_BOND);
        vm.startPrank(op);
        d.token.approve(address(d.bond), type(uint256).max);
        d.bond.bond(MIN_BOND);
        vm.stopPrank();
        bytes32 structHash = keccak256(
            abi.encode(d.bond.REGISTER_NODE_TYPEHASH(), nodeId, d.bond.bindingNonce(op), cfg.currentTermsHash)
        );
        bytes memory sig = _bondSig(pk, structHash);
        vm.prank(op);
        d.bond.registerNode(nodeId, hex"", "us-east", cfg.currentTermsHash, sig, hex"01");
        assertTrue(d.bond.isActive(op), "registered and active");
    }

    function _openPool() internal returns (bytes32 poolId) {
        vm.prank(owner);
        poolId = d.paymentPool.openPool(DEPOSIT);
    }

    /// @dev `bindNodeId` with the operator's own EIP-712 consent; the mock
    ///      ed25519 verifier accepts the ownership proof.
    function _bind(uint256 pk, address who, bytes32 nodeId) internal {
        bytes32 structHash = keccak256(abi.encode(d.bond.BIND_NODE_TYPEHASH(), nodeId, d.bond.bindingNonce(who)));
        bytes memory sig = _bondSig(pk, structHash);
        vm.prank(who);
        d.bond.bindNodeId(nodeId, sig, hex"01");
    }

    /// @dev EIP-712 signature over `structHash` in the `CapacityBond` domain.
    function _bondSig(uint256 pk, bytes32 structHash) internal view returns (bytes memory) {
        bytes32 domain = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("CapacityBond")),
                keccak256(bytes("1")),
                block.chainid,
                address(d.bond)
            )
        );
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, keccak256(abi.encodePacked("\x19\x01", domain, structHash)));
        return abi.encodePacked(r, s, v);
    }

    /// @dev One pool group: the signer's capability plus one sealed voucher
    ///      for `SETTLE_AMOUNT` naming `provider_`.
    function _settleBatch(bytes32 poolId, address provider_)
        internal
        view
        returns (PaymentPool.PoolBatch[] memory batches)
    {
        uint64 expiry = uint64(block.timestamp + 365 days);
        PaymentPool.CapabilityReg[] memory caps = new PaymentPool.CapabilityReg[](1);
        caps[0] = PaymentPool.CapabilityReg({
            signer: signer,
            spendingCap: SPENDING_CAP,
            expiry: expiry,
            ownerSig: _capabilitySig(poolId, signer, SPENDING_CAP, expiry, OWNER_PK)
        });
        (bytes32 vr, bytes32 vvs) = _voucherSig(poolId, signer, provider_, SETTLE_AMOUNT, SETTLE_BYTES, SIGNER_PK);
        PaymentPool.LaneVoucher[] memory vouchers = new PaymentPool.LaneVoucher[](1);
        vouchers[0] = PaymentPool.LaneVoucher({
            signer: signer,
            cumulative: SETTLE_AMOUNT,
            bytesDelivered: SETTLE_BYTES,
            r: vr,
            vs: vvs,
            chainRoot: bytes32(0),
            preimage: bytes32(0),
            chainMeter: 0
        });
        batches = new PaymentPool.PoolBatch[](1);
        batches[0] = PaymentPool.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: vouchers });
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
        // The cooperative close: a sealed voucher (zero root, zero price)
        // settling exactly `amount`, which is the shape a finalized delivery
        // actually redeems (ADR 003 §The chain walk is fallback-only).
        bytes32 structHash = keccak256(
            abi.encode(VOUCHER_TYPEHASH, poolId, signer_, provider_, amount, bytesDelivered, bytes32(0), uint256(0))
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(address(d.paymentPool)), structHash));
        (uint8 v, bytes32 r, bytes32 sv) = vm.sign(pk, digest);
        return (r, bytes32(uint256(sv) | (uint256(v - 27) << 255)));
    }

    /// @dev Mirrors `PaymentPool.PoolRedeemed` for `vm.expectEmit`.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, PaymentPool.LaneSettled[] lanes);
}
