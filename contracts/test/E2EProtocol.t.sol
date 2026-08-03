// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
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
///        governance-handed-off contract surface (issue #452 acceptance E2E):
///        register → bond → open channel → settle through FeeRouter → 3-bucket
///        distribution → slash → escrow → auto-ejection → appeal.
/// @dev   Runs the real `_runFullDeploy` pipeline (mock ed25519 verifier + mock
///        USDC) so PaymentChannel/SlashJudge/OriginAssignment are wired exactly as
///        production. The slash is triggered via the deployed `SlashJudge`
///        role-holder (it is the sole `SLASH_ROLE` grantee — the SlashJudge
///        evidence path itself is unit-tested in `SlashJudge.t.sol`); the appeal's
///        governance step is executed as the `TimelockController` (the full
///        proposal path is covered in `GovernanceLifecycle.t.sol`).
contract E2EProtocolTest is Test, BaseProtocolDeploy {
    Deployment internal d;
    DeployConfig internal cfg;
    E2EUSDC internal usdc;

    uint256 internal constant OPERATOR_PK = 0x0FE1;
    uint256 internal constant CLIENT_PK = 0xC11E27;
    address internal operator;
    address internal client;
    address internal challenger = address(0xCA11E2);
    address internal emergencyMultisig = address(0xC0DE);
    address internal tokenHolder = address(0x70);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant DEPOSIT = 1000e6;
    uint256 internal constant SETTLE_AMOUNT = 800e6;
    uint256 internal constant SETTLE_BYTES = 80_000_000;

    bytes32 internal constant NODE_ID = bytes32(uint256(0xD0DE));
    bytes32 internal constant REGISTER_NODE_TYPEHASH =
        keccak256("RegisterNode(bytes32 nodeId,uint64 nonce,bytes32 termsHash)");
    // ADR 019 § Terms Acceptance — non-zero genesis terms hash (CapacityBond
    // rejects the zero sentinel).
    bytes32 internal constant TERMS_HASH = keccak256("decdn operator terms v1");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)");

    function setUp() public {
        operator = vm.addr(OPERATOR_PK);
        client = vm.addr(CLIENT_PK);

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
            currentTermsHash: TERMS_HASH,
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            feeRouterShares: [uint256(9000), uint256(0), uint256(1000)],
            buybackBurner: address(0),
            slashAppealBond: 1000e18
        });
        d = _runFullDeploy(cfg);

        vm.warp(1_000_000);

        // Fund the operator (bond + appeal bond) and the client (USDC deposit).
        vm.prank(tokenHolder);
        d.token.transfer(operator, 200_000e18);
        usdc.transfer(client, 10_000e6);
    }

    // -----------------------------------------------------------------
    // Deploy wiring (the three new contracts)
    // -----------------------------------------------------------------

    function test_deployWiring_newContractsWiredAndHandedOff() public view {
        // Post-deployment role grants (ADR 016 § Post-Deployment Initialization).
        assertTrue(
            d.router.hasRole(d.router.ROUTER_CALLER_ROLE(), address(d.paymentChannel)), "paymentChannel ROUTER_CALLER"
        );
        assertTrue(d.bond.hasRole(d.bond.SLASH_ROLE(), address(d.slashJudge)), "slashJudge SLASH_ROLE");
        assertEq(d.originAssignment.contentBlacklist(), address(d.blacklist), "originAssignment blacklist wired");

        // Constructor dependency wiring.
        assertEq(d.paymentChannel.feeRouter(), address(d.router), "paymentChannel.feeRouter");
        assertEq(address(d.paymentChannel.capacityBond()), address(d.bond), "paymentChannel.capacityBond");
        assertEq(address(d.slashJudge.capacityBond()), address(d.bond), "slashJudge.capacityBond");

        // Governance handoff: Timelock governs the three new contracts; deployer holds nothing.
        address tl = address(d.timelock);
        assertTrue(d.paymentChannel.hasRole(GOVERNANCE_ROLE, tl), "paymentChannel gov");
        assertTrue(d.slashJudge.hasRole(GOVERNANCE_ROLE, tl), "slashJudge gov");
        assertTrue(d.originAssignment.hasRole(GOVERNANCE_ROLE, tl), "originAssignment gov");
        assertFalse(d.paymentChannel.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer), "no paymentChannel back door");
        assertFalse(d.slashJudge.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer), "no slashJudge back door");
        assertFalse(d.originAssignment.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer), "no originAssignment back door");
    }

    // -----------------------------------------------------------------
    // Full lifecycle
    // -----------------------------------------------------------------

    function test_e2e_registerSettleSlashEjectAppeal() public {
        // 1. Bond + register the operator → isActive.
        _bondAndRegister();
        assertTrue(d.bond.isActive(operator), "operator active after register");

        // 2. Open a channel and settle a client voucher through FeeRouter.
        uint256 epoch = _openAndSettleChannel();

        // 3. Three-bucket distribution at the configured launch shares (read from
        //    cfg, not re-hardcoded, so the assertion tracks the deploy config).
        assertEq(
            usdc.balanceOf(operator),
            SETTLE_AMOUNT * cfg.feeRouterShares[0] / 10_000,
            "operator base share (operator bucket)"
        );
        assertEq(usdc.balanceOf(address(d.timelock)), SETTLE_AMOUNT * cfg.feeRouterShares[2] / 10_000, "treasury share");
        assertEq(usdc.balanceOf(d.router.buybackBurner()), 0, "buyback dormant at launch");
        assertEq(d.router.bytesPerEpoch(operator, uint64(epoch)), SETTLE_BYTES, "served bytes stamped");

        // 4. Slash via the wired SLASH_ROLE holder, escrowing each slash. Three
        //    tier slashes (5/15/50%) drop the bond below minBond/2 → auto-eject.
        //    `_slashToEjection` asserts each tier escrows a strictly larger amount.
        uint256 lastSlashId = _slashToEjection();
        assertTrue(d.bond.ejected(operator), "operator auto-ejected");
        assertFalse(d.bond.isActive(operator), "ejected operator inactive");
        uint256 escrowBeforeGrant = d.bond.escrowedTotal();
        assertGt(escrowBeforeGrant, 0, "slash escrowed");
        assertGt(d.bond.slashedAtEpoch(operator), 0, "slash zero-out stamped");

        // 5. Operator appeals the last slash; governance grants → escrow released
        //    for the appealed slash + appeal bond refunded to the appellant. The
        //    zero-out watermark is recomputed over the operator's still-standing
        //    slashes (ADR 036 § Slashing zero-out — multi-slash): the other two
        //    slashes stand, so it must NOT clear (a granted appeal of one slash
        //    must not restore vote weight while others stand — issue #709). The
        //    three slashes here share one epoch (no warp in `_slashToEjection`),
        //    so this asserts persistence; the cross-epoch fall-back to an older
        //    standing slash is covered by the unit tests in CapacityBond.t.sol /
        //    SlashAppeal.t.sol.
        uint256 stampBeforeGrant = d.bond.slashedAtEpoch(operator);
        uint256 operatorTokenBeforeGrant = d.token.balanceOf(operator);
        _appealAndGrant(lastSlashId);
        assertEq(d.bond.slashedAtEpoch(operator), stampBeforeGrant, "zero-out persists while other slashes stand");

        // Only the appealed slash's escrow is released (the other two stand).
        uint256 escrowReleased = escrowBeforeGrant - d.bond.escrowedTotal();
        assertGt(escrowReleased, 0, "escrow released for granted slash");
        // A granted appeal makes the operator whole: the escrowed bond is refunded
        // liquid. The appeal bond round-trips within `_appealAndGrant` (paid at
        // openSlashAppeal, refunded at grantAppeal) — so the net delta is exactly the
        // released escrow. If the grant failed to refund the bond, the operator would
        // be `slashAppealBond` short and this equality would fail.
        assertEq(
            d.token.balanceOf(operator),
            operatorTokenBeforeGrant + escrowReleased,
            "operator refunded escrowed bond; appeal bond round-trips to net zero"
        );
    }

    // Blacklist → CapacityBond ejection. ContentBlacklist holds BLACKLIST_ROLE on
    // the bond (wired in phase 4), so a governance blacklist-add ejects the
    // operator on-chain. Exercises the cross-contract edge behaviorally — a
    // dropped BLACKLIST_ROLE grant would make this revert/no-op rather than eject.
    function test_e2e_blacklistEjectsOperator() public {
        _bondAndRegister();
        assertTrue(d.bond.isActive(operator), "operator active before blacklist");

        // GOVERNANCE_ROLE on ContentBlacklist is the Timelock after handoff.
        vm.prank(address(d.timelock));
        d.blacklist.addOperator(operator);

        assertTrue(d.blacklist.isOperatorBlacklisted(operator), "operator marked blacklisted");
        assertTrue(d.bond.ejected(operator), "operator ejected via blacklist BLACKLIST_ROLE wiring");
        assertFalse(d.bond.isActive(operator), "blacklisted operator inactive");
    }

    // -----------------------------------------------------------------
    // Steps
    // -----------------------------------------------------------------

    function _bondAndRegister() internal {
        vm.startPrank(operator);
        d.token.approve(address(d.bond), type(uint256).max);
        d.bond.bond(MIN_BOND);
        bytes memory bindingSig = _registrationSig(NODE_ID, 0, TERMS_HASH);
        d.bond.registerNode(NODE_ID, hex"01", "US", TERMS_HASH, bindingSig, hex"00");
        vm.stopPrank();
    }

    function _openAndSettleChannel() internal returns (uint256 epoch) {
        vm.startPrank(client);
        usdc.approve(address(d.paymentChannel), type(uint256).max);
        bytes32 channelId = d.paymentChannel.openChannel(operator, DEPOSIT, address(0));
        vm.stopPrank();

        bytes memory voucherSig = _voucherSig(channelId, SETTLE_AMOUNT, 1, SETTLE_BYTES);
        vm.prank(operator);
        d.paymentChannel.closeChannel(channelId, SETTLE_AMOUNT, 1, SETTLE_BYTES, voucherSig);

        vm.warp(block.timestamp + 48 hours);
        epoch = block.timestamp / 7 days;
        d.paymentChannel.settleChannel(channelId);
    }

    function _slashToEjection() internal returns (uint256 lastSlashId) {
        vm.startPrank(address(d.slashJudge));
        uint256 e0 = d.bond.escrowedTotal();
        d.bond.slash(operator, challenger, 0, bytes32(0));
        uint256 e1 = d.bond.escrowedTotal();
        d.bond.slash(operator, challenger, 0, bytes32(0));
        uint256 e2 = d.bond.escrowedTotal();
        (lastSlashId,) = d.bond.slash(operator, challenger, 0, bytes32(0));
        uint256 e3 = d.bond.escrowedTotal();
        vm.stopPrank();

        // Each escalating tier (5% → 15% → 50%) must escrow a strictly larger
        // increment — proves the tier ladder and escrow accounting, not just the
        // terminal ejected state.
        assertGt(e1 - e0, 0, "tier-1 slash escrowed");
        assertGt(e2 - e1, e1 - e0, "tier-2 escrow increment exceeds tier-1");
        assertGt(e3 - e2, e2 - e1, "tier-3 escrow increment exceeds tier-2");
    }

    function _appealAndGrant(uint256 slashId) internal {
        vm.startPrank(operator);
        d.token.approve(address(d.slashAppeal), type(uint256).max);
        d.slashAppeal.openSlashAppeal(slashId, bytes32("evidence"));
        vm.stopPrank();

        // Emergency multisig fast-tracks the appeal into governance review
        // (ADR 028 state machine: Open → FastTracked → Granted/Upheld).
        vm.prank(emergencyMultisig);
        d.slashAppeal.fastTrackAppeal(slashId);

        // Governance (the Timelock) grants the appeal.
        vm.prank(address(d.timelock));
        d.slashAppeal.grantAppeal(slashId);
    }

    // -----------------------------------------------------------------
    // EIP-712 signing helpers
    // -----------------------------------------------------------------

    function _domainSeparator(string memory name, address verifyingContract) internal view returns (bytes32) {
        return keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes(name)),
                keccak256(bytes("1")),
                block.chainid,
                verifyingContract
            )
        );
    }

    function _registrationSig(bytes32 nodeId, uint64 nonce, bytes32 termsHash) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(REGISTER_NODE_TYPEHASH, nodeId, nonce, termsHash));
        bytes32 digest =
            keccak256(abi.encodePacked("\x19\x01", _domainSeparator("CapacityBond", address(d.bond)), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(OPERATOR_PK, digest);
        return abi.encodePacked(r, s, v);
    }

    function _voucherSig(bytes32 channelId, uint256 amount, uint256 nonce, uint256 bytesDelivered)
        internal
        view
        returns (bytes memory)
    {
        bytes32 structHash =
            keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, address(usdc)));
        bytes32 digest = keccak256(
            abi.encodePacked("\x19\x01", _domainSeparator("PaymentChannel", address(d.paymentChannel)), structHash)
        );
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(CLIENT_PK, digest);
        return abi.encodePacked(r, s, v);
    }
}
