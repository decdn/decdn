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
    address internal challengerPool = address(0xCCEE);

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant DEPOSIT = 1000e6;
    uint256 internal constant SETTLE_AMOUNT = 800e6;
    uint256 internal constant SETTLE_BYTES = 80_000_000;

    bytes32 internal constant NODE_ID = bytes32(uint256(0xD0DE));
    bytes32 internal constant BIND_NODE_TYPEHASH = keccak256("BindNodeId(bytes32 nodeId,uint64 nonce)");
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
            challengerIncentivePool: challengerPool,
            timelockDelay: 48 hours,
            minStake: MIN_STAKE,
            unbondingPeriod: 14 days,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            genesisCreditWindow: 30 days,
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            feeRouterShares: [uint256(9000), uint256(0), uint256(1000)],
            buybackBurner: address(0),
            slashAppealBond: 1000e18,
            blacklistAppealBond: 100e18
        });
        d = _runFullDeploy(cfg);

        vm.warp(1_000_000);

        // Fund the operator (stake + appeal bond) and the client (USDC deposit).
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

        // 3. Three-bucket distribution at launch shares (90 / 0 / 10).
        assertEq(usdc.balanceOf(operator), SETTLE_AMOUNT * 9000 / 10_000, "operator base share (90%)");
        assertEq(usdc.balanceOf(address(d.timelock)), SETTLE_AMOUNT * 1000 / 10_000, "treasury share (10%)");
        assertEq(usdc.balanceOf(d.router.buybackBurner()), 0, "buyback dormant at launch");
        assertEq(d.router.bytesPerEpoch(operator, uint64(epoch)), SETTLE_BYTES, "served bytes stamped");

        // 4. Slash via the wired SLASH_ROLE holder, escrowing each slash. Three
        //    tier slashes (5/15/50%) drop the bond below minStake/2 → auto-eject.
        uint256 lastSlashId = _slashToEjection();
        assertTrue(d.bond.ejected(operator), "operator auto-ejected");
        assertFalse(d.bond.isActive(operator), "ejected operator inactive");
        assertGt(d.bond.escrowedTotal(), 0, "slash escrowed");
        assertGt(d.bond.slashedAtEpoch(operator), 0, "slash zero-out stamped");

        // 5. Operator appeals; governance grants → escrow refund + zero-out cleared.
        _appealAndGrant(lastSlashId);
        assertEq(d.bond.slashedAtEpoch(operator), 0, "slash zero-out cleared on grant");
    }

    // -----------------------------------------------------------------
    // Steps
    // -----------------------------------------------------------------

    function _bondAndRegister() internal {
        vm.startPrank(operator);
        d.token.approve(address(d.bond), type(uint256).max);
        d.bond.stake(MIN_STAKE);
        bytes memory bindingSig = _bindingSig(NODE_ID, 0);
        d.bond.registerNode(NODE_ID, hex"01", "US", bindingSig, hex"00");
        vm.stopPrank();
    }

    function _openAndSettleChannel() internal returns (uint256 epoch) {
        vm.startPrank(client);
        usdc.approve(address(d.paymentChannel), type(uint256).max);
        bytes32 channelId = d.paymentChannel.openChannel(operator, DEPOSIT);
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
        d.bond.slash(operator, challenger, 0);
        d.bond.slash(operator, challenger, 0);
        (lastSlashId,) = d.bond.slash(operator, challenger, 0);
        vm.stopPrank();
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

    function _bindingSig(bytes32 nodeId, uint64 nonce) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce));
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
