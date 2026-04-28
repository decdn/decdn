// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { Deploy } from "../../script/Deploy.s.sol";
import { MockUSDC } from "../mocks/MockUSDC.sol";
import { StakingRegistry } from "../../src/StakingRegistry.sol";
import { StablePaymentChannel } from "../../src/StablePaymentChannel.sol";
import { ContentBlacklist } from "../../src/ContentBlacklist.sol";
import { SlashJudge } from "../../src/SlashJudge.sol";
import { BuybackBurner } from "../../src/BuybackBurner.sol";
import { IStakingRegistry } from "../../src/interfaces/IStakingRegistry.sol";
import { Roles } from "../../src/libraries/Roles.sol";

/// @notice Exercises the PoC contract set through a single lifecycle:
///   deploy → register node → open channel → settle with fee → slash → eject.
contract EndToEndTest is Test {
    Deploy.Deployment internal d;

    MockUSDC internal usdc;
    address internal treasury = makeAddr("treasury");
    address internal governor = makeAddr("governor");

    uint256 internal clientPk = 0x1111;
    address internal client;
    uint256 internal operatorPk = 0x2222;
    address internal operator;

    address internal challenger = makeAddr("challenger");

    function setUp() public {
        client = vm.addr(clientPk);
        operator = vm.addr(operatorPk);

        // Deploy the full set as the test contract (treated as `deployer`).
        Deploy deploy = new Deploy();
        usdc = new MockUSDC();
        d = deploy.deployForTest(address(this), treasury, IERC20(address(usdc)), 10_000_000e18);

        // Hand out governance role so we can blacklist hashes later.
        d.contentBlacklist.grantRole(Roles.GOVERNANCE_ROLE, governor);

        // Fund parties.
        d.token.transfer(operator, 100_000e18);
        d.token.transfer(challenger, 10_000e18);
        usdc.mint(client, 10_000e6);

        vm.prank(operator);
        d.token.approve(address(d.stakingRegistry), type(uint256).max);
        vm.prank(challenger);
        d.token.approve(address(d.slashJudge), type(uint256).max);
        vm.prank(client);
        usdc.approve(address(d.paymentChannel), type(uint256).max);
    }

    function test_FullLifecycle() public {
        // -----------------------------------------------------------------
        // 1. Operator stakes and registers.
        // -----------------------------------------------------------------
        vm.prank(operator);
        d.stakingRegistry.stake(10_000e18); // 10x min_stake → discount tier

        bytes32 nodeId = keccak256("operator-ironode");
        bytes memory bindSig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        d.stakingRegistry.registerNode(nodeId, bindSig);

        assertEq(d.stakingRegistry.getStakeMultiple(operator), 10);
        assertEq(
            uint256(d.stakingRegistry.getStakeInfo(operator).state),
            uint256(StakingRegistry.OperatorState.Registered)
        );

        // -----------------------------------------------------------------
        // 2. Client opens channel, client signs voucher, provider settles.
        // -----------------------------------------------------------------
        bytes32 expectedId = d.paymentChannel.nextChannelId(client, operator);
        vm.prank(client);
        bytes32 channelId = d.paymentChannel.openChannel(operator, 1000e6);
        assertEq(expectedId, channelId);

        uint256 claimed = 800e6;
        bytes memory voucher = _signVoucher(clientPk, channelId, claimed, 1);
        vm.prank(operator);
        d.paymentChannel.closeChannel(channelId, claimed, 1, voucher);

        vm.warp(block.timestamp + 48 hours + 1);
        uint256 treasuryBefore = usdc.balanceOf(treasury);
        uint256 opBefore = usdc.balanceOf(operator);
        uint256 clientBefore = usdc.balanceOf(client);
        d.paymentChannel.settleChannel(channelId);

        // Discounted fee (1.5%) because operator holds 10x min stake.
        uint256 fee = (claimed * 150) / 10_000;
        assertEq(usdc.balanceOf(treasury) - treasuryBefore, fee);
        assertEq(usdc.balanceOf(operator) - opBefore, claimed - fee);
        assertEq(usdc.balanceOf(client) - clientBefore, 1000e6 - claimed);

        // -----------------------------------------------------------------
        // 3. Challenger submits a phantom-blob challenge and wins.
        // -----------------------------------------------------------------
        bytes32 PROBE_TH = d.slashJudge.PROBE_RESPONSE_TYPEHASH();
        bytes32 STREAM_TH = d.slashJudge.STREAM_RESPONSE_TYPEHASH();
        bytes32 DOMAIN = d.slashJudge.domainSeparator();

        SlashJudge.ProbeResponse memory probe = SlashJudge.ProbeResponse({
            hash: bytes32(uint256(0xFACE)),
            hasBlob: true,
            ratePerMb: 10,
            timestamp: uint64(block.timestamp)
        });
        SlashJudge.StreamResponse memory sr = SlashJudge.StreamResponse({
            hash: probe.hash,
            ok: false,
            ratePerMb: 10,
            totalBytes: 0,
            channelId: bytes32(uint256(0xFEED)),
            timestamp: uint64(block.timestamp)
        });
        bytes memory pSig = _sign(
            operatorPk,
            DOMAIN,
            keccak256(
                abi.encode(PROBE_TH, probe.hash, probe.hasBlob, probe.ratePerMb, probe.timestamp)
            )
        );
        bytes memory sSig = _sign(
            operatorPk,
            DOMAIN,
            keccak256(
                abi.encode(
                    STREAM_TH,
                    sr.hash,
                    sr.ok,
                    sr.ratePerMb,
                    sr.totalBytes,
                    sr.channelId,
                    sr.timestamp
                )
            )
        );

        uint256 stakeBefore = d.stakingRegistry.getStakeInfo(operator).active;
        vm.prank(challenger);
        uint256 chId = d.slashJudge.submitPhantomChallenge(operator, probe, pSig, sr, sSig);

        vm.warp(block.timestamp + 24 hours + 1);
        uint256 challengerBefore = d.token.balanceOf(challenger);
        d.slashJudge.resolveChallenge(chId);

        uint256 slashAmount = stakeBefore / 10;
        uint256 expected = slashAmount / 2 + 100e18; // 50% reward + bond return
        assertEq(d.token.balanceOf(challenger) - challengerBefore, expected);

        // -----------------------------------------------------------------
        // 4. Governance blacklists a hash and ejects the operator.
        // -----------------------------------------------------------------
        bytes32 h = keccak256("banned-hash");
        vm.prank(governor);
        d.contentBlacklist.addHash(h);
        vm.prank(governor);
        d.contentBlacklist.ejectOrigin(operator);

        StakingRegistry.StakeInfo memory info = d.stakingRegistry.getStakeInfo(operator);
        assertEq(uint256(info.state), uint256(StakingRegistry.OperatorState.Ejected));
        assertEq(info.active, 0);

        // -----------------------------------------------------------------
        // 5. BuybackBurner still accepts accumulation but rejects execution.
        // -----------------------------------------------------------------
        usdc.mint(address(this), 500e6);
        usdc.approve(address(d.buybackBurner), 500e6);
        d.buybackBurner.depositUSDC(500e6);
        assertEq(usdc.balanceOf(address(d.buybackBurner)), 500e6);

        // Grant ourselves KEEPER_ROLE so we can even attempt executeBuyback.
        d.buybackBurner.grantRole(Roles.KEEPER_ROLE, address(this));
        vm.expectRevert(BuybackBurner.BuybackDisabled.selector);
        d.buybackBurner.executeBuyback(500e6, 0);
    }

    // ---------- helpers ----------

    function _signBind(
        uint256 pk,
        bytes32 nodeId,
        uint64 nonce
    ) internal view returns (bytes memory) {
        bytes32 typehash = d.stakingRegistry.BIND_NODE_TYPEHASH();
        bytes32 domain = d.stakingRegistry.domainSeparator();
        return _sign(pk, domain, keccak256(abi.encode(typehash, nodeId, nonce)));
    }

    function _signVoucher(
        uint256 pk,
        bytes32 channelId,
        uint256 amount,
        uint256 nonce
    ) internal view returns (bytes memory) {
        bytes32 typehash = d.paymentChannel.VOUCHER_TYPEHASH();
        bytes32 domain = d.paymentChannel.domainSeparator();
        return _sign(
            pk, domain, keccak256(abi.encode(typehash, channelId, amount, nonce, address(usdc)))
        );
    }

    function _sign(
        uint256 pk,
        bytes32 domain,
        bytes32 structHash
    ) internal pure returns (bytes memory) {
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domain, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }
}
