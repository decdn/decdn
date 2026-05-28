// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { stdJson } from "forge-std/StdJson.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { DeployProtocol } from "../script/DeployProtocol.s.sol";
import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { Token } from "../src/Token.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { SafetyReserve } from "../src/SafetyReserve.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { DecdnGovernor } from "../src/DecdnGovernor.sol";

/// @title DeployProtocolScriptTest — covers the script-shaped behavior of
///        `DeployProtocol.s.sol` itself: env-var → `DeployConfig` plumbing and
///        the `deployments/<chainId>.json` manifest writer. The base-deploy
///        pipeline (phases 1-5) is covered separately in `DeployProtocol.t.sol`
///        and the lifecycle integration in `GovernanceLifecycle.t.sol`.
contract DeployProtocolScriptTest is Test, DeployProtocol {
    using stdJson for string;

    address internal constant TEST_USDC = address(0xa55D);
    address internal constant TEST_ED25519 = address(0xeD25);
    address internal constant TEST_MULTISIG = address(0xC0DE);
    address internal constant TEST_TREASURY = address(0xD75a);
    address internal constant TEST_INITIAL_HOLDER = address(0xbEEF);
    address internal constant TEST_CHALLENGER_POOL = address(0xccEE);

    // Test-only chain ids so manifest writes never collide with a real
    // `deployments/<realChain>.json`. Each test uses a distinct id to avoid
    // cross-test filesystem interference — `vm.serializeX` accumulates per
    // object key across the whole test process, so two tests writing the same
    // path can produce surprising merged content. Cleaned up via `vm.removeFile`.
    uint256 internal constant CHAIN_ID_WRITE = 31_337_694;
    uint256 internal constant CHAIN_ID_REVERT = 31_337_695;
    uint256 internal constant CHAIN_ID_OVERWRITE = 31_337_696;

    function setUp() public {
        vm.setEnv("USDC_ADDRESS", vm.toString(TEST_USDC));
        vm.setEnv("ED25519_VERIFIER_ADDRESS", vm.toString(TEST_ED25519));
        vm.setEnv("EMERGENCY_MULTISIG", vm.toString(TEST_MULTISIG));
        vm.setEnv("TREASURY_ADDRESS", vm.toString(TEST_TREASURY));
        vm.setEnv("INITIAL_TOKEN_HOLDER", vm.toString(TEST_INITIAL_HOLDER));
        vm.setEnv("CHALLENGER_INCENTIVE_POOL", vm.toString(TEST_CHALLENGER_POOL));
        vm.setEnv("FORCE_OVERWRITE_MANIFEST", "false");
    }

    // External wrappers — `_readConfig`/`_writeManifest` are internal, but
    // `vm.expectRevert` only catches reverts at external-call boundaries.
    function externalReadConfig() external view returns (DeployConfig memory) {
        return _readConfig();
    }

    function externalWriteManifest(DeployConfig calldata cfg, Deployment calldata d) external {
        _writeManifest(cfg, d);
    }

    // _readConfig — defaults applied when optional env vars are unset, and
    // required vars round-trip from `vm.envAddress`.
    //
    // The override branch of `vm.envOr` is intentionally not tested here: env
    // vars set via `vm.setEnv` are process-wide and persist across tests, so
    // an override test would race with the parallel-running manifest tests.
    // The override path is a single `vm.envOr` call per field — a forge
    // cheatcode, not our code — so testing the defaults branch is sufficient
    // to prove the plumbing.

    function test_readConfig_appliesDefaultsForOptionalVars() public {
        DeployConfig memory cfg = this.externalReadConfig();
        assertEq(address(cfg.usdc), TEST_USDC, "usdc");
        assertEq(address(cfg.ed25519Verifier), TEST_ED25519, "ed25519");
        assertEq(cfg.emergencyMultisig, TEST_MULTISIG, "multisig");
        assertEq(cfg.treasury, TEST_TREASURY, "treasury");
        assertEq(cfg.initialTokenHolder, TEST_INITIAL_HOLDER, "holder");
        assertEq(cfg.challengerIncentivePool, TEST_CHALLENGER_POOL, "challenger");
        assertEq(cfg.timelockDelay, DEFAULT_TIMELOCK_DELAY, "timelockDelay");
        assertEq(cfg.minStake, DEFAULT_MIN_STAKE, "minStake");
        assertEq(cfg.unbondingPeriod, DEFAULT_UNBONDING_PERIOD, "unbondingPeriod");
        assertEq(cfg.multiaddrUpdateCooldown, DEFAULT_MULTIADDR_UPDATE_COOLDOWN, "multiaddrCooldown");
        assertEq(cfg.maxMultiaddrSize, DEFAULT_MAX_MULTIADDR_SIZE, "maxMultiaddrSize");
        assertEq(cfg.regionStabilityWindow, DEFAULT_REGION_STABILITY_WINDOW, "regionStability");
        assertEq(cfg.genesisCreditWindow, DEFAULT_GENESIS_CREDIT_WINDOW, "genesisCreditWindow");
        assertEq(uint256(cfg.feeRouterEpochLength), uint256(DEFAULT_FEE_ROUTER_EPOCH_LENGTH), "epochLen");
        assertEq(uint256(cfg.feeRouterWindowEpochs), uint256(DEFAULT_FEE_ROUTER_WINDOW_EPOCHS), "windowEpochs");
        assertEq(cfg.safetyAppealBond, DEFAULT_SAFETY_APPEAL_BOND, "safetyAppealBond");
        assertEq(cfg.maxAppealRestitution, DEFAULT_MAX_APPEAL_RESTITUTION, "maxAppealRestitution");
        assertEq(cfg.blacklistAppealBond, DEFAULT_BLACKLIST_APPEAL_BOND, "blacklistAppealBond");

        // Launch shares are hard-coded in the script, not env-driven.
        assertEq(cfg.feeRouterShares[0], LAUNCH_OPERATOR_SHARE, "operator share");
        assertEq(cfg.feeRouterShares[1], 0, "buyback share dormant");
        assertEq(cfg.feeRouterShares[2], LAUNCH_TREASURY_SHARE, "treasury share");
        assertEq(cfg.feeRouterShares[3], LAUNCH_SAFETY_SHARE, "safety share");
        assertEq(cfg.buybackBurner, address(0), "buybackBurner unwired");
    }

    function test_readConfig_deployerIsTxOrigin() public {
        // Default forge sender (different from FORGE_DEFAULT_SENDER which is
        // the one we reject). Reading via the external wrapper makes `tx.origin`
        // = the forge test-runner's tx.origin, which is the DEFAULT_SENDER.
        // We override it via vm.prank to assert the field plumbing.
        address custom = address(0xC0FFEE);
        vm.startPrank(address(this), custom);
        DeployConfig memory cfg = this.externalReadConfig();
        vm.stopPrank();
        assertEq(cfg.deployer, custom, "deployer = tx.origin");
    }

    // -----------------------------------------------------------------
    // _writeManifest — file path, contents, overwrite guard
    // -----------------------------------------------------------------

    function _manifestPath(uint256 chainId) internal pure returns (string memory) {
        return string.concat("./deployments/", vm.toString(chainId), ".json");
    }

    function _cleanupManifest(uint256 chainId) internal {
        string memory path = _manifestPath(chainId);
        if (vm.exists(path)) {
            vm.removeFile(path);
        }
    }

    function _stubDeployment() internal pure returns (Deployment memory d) {
        // Fake addresses — the manifest writer doesn't inspect contract code,
        // only the addresses themselves. Sentinel values per slot make
        // mis-mapped reads obvious in the JSON output.
        d.token = Token(address(0x01));
        d.bond = CapacityBond(address(0x02));
        d.reserve = SafetyReserve(address(0x03));
        d.router = FeeRouter(address(0x04));
        d.blacklist = ContentBlacklist(address(0x05));
        d.registry = PublisherRegistry(address(0x06));
        d.timelock = TimelockController(payable(address(0x07)));
        d.governor = DecdnGovernor(payable(address(0x08)));
    }

    function test_writeManifest_writesAllAddresses() public {
        _cleanupManifest(CHAIN_ID_WRITE);
        DeployConfig memory cfg = this.externalReadConfig();
        Deployment memory d = _stubDeployment();
        vm.chainId(CHAIN_ID_WRITE);

        this.externalWriteManifest(cfg, d);

        string memory json = vm.readFile(_manifestPath(CHAIN_ID_WRITE));
        assertEq(json.readAddress(".contracts.Token"), address(0x01), "Token");
        assertEq(json.readAddress(".contracts.CapacityBond"), address(0x02), "CapacityBond");
        assertEq(json.readAddress(".contracts.SafetyReserve"), address(0x03), "SafetyReserve");
        assertEq(json.readAddress(".contracts.FeeRouter"), address(0x04), "FeeRouter");
        assertEq(json.readAddress(".contracts.ContentBlacklist"), address(0x05), "ContentBlacklist");
        assertEq(json.readAddress(".contracts.PublisherRegistry"), address(0x06), "PublisherRegistry");
        assertEq(json.readAddress(".contracts.TimelockController"), address(0x07), "Timelock");
        assertEq(json.readAddress(".contracts.DecdnGovernor"), address(0x08), "Governor");
        assertEq(json.readAddress(".contracts.BuybackBurner"), address(0), "BuybackBurner unwired");

        assertEq(json.readAddress(".externalDeps.usdc"), TEST_USDC, "usdc");
        assertEq(json.readAddress(".externalDeps.ed25519Verifier"), TEST_ED25519, "ed25519");
        assertEq(json.readAddress(".externalDeps.emergencyMultisig"), TEST_MULTISIG, "multisig");
        assertEq(json.readAddress(".externalDeps.treasury"), TEST_TREASURY, "treasury");

        assertEq(json.readUint(".chainId"), CHAIN_ID_WRITE, "chainId");
        assertEq(json.readUint(".config.timelockDelay"), DEFAULT_TIMELOCK_DELAY, "timelockDelay");
        assertEq(json.readUint(".config.minStake"), DEFAULT_MIN_STAKE, "minStake");

        uint256[] memory shares = json.readUintArray(".config.feeRouterShares");
        assertEq(shares.length, 4, "shares length");
        assertEq(shares[0], LAUNCH_OPERATOR_SHARE);
        assertEq(shares[1], 0);
        assertEq(shares[2], LAUNCH_TREASURY_SHARE);
        assertEq(shares[3], LAUNCH_SAFETY_SHARE);

        _cleanupManifest(CHAIN_ID_WRITE);
    }

    function test_writeManifest_revertsOnExistingFile() public {
        _cleanupManifest(CHAIN_ID_REVERT);
        DeployConfig memory cfg = this.externalReadConfig();
        Deployment memory d = _stubDeployment();
        vm.chainId(CHAIN_ID_REVERT);

        // Seed the manifest file via `vm.writeFile` directly. Using a prior
        // `_writeManifest` (internal or external) call to seed it doesn't
        // work because forge's call-frame filesystem snapshot doesn't include
        // the just-written file when `vm.exists` is queried inside the
        // expectRevert sub-call. `vm.writeFile` from the test-function frame
        // is committed before any sub-call begins.
        vm.writeFile(_manifestPath(CHAIN_ID_REVERT), "{}");

        vm.expectRevert(
            abi.encodeWithSelector(DeployProtocol.ManifestAlreadyExists.selector, _manifestPath(CHAIN_ID_REVERT))
        );
        this.externalWriteManifest(cfg, d);

        _cleanupManifest(CHAIN_ID_REVERT);
    }

    function test_writeManifest_overwritesWhenForced() public {
        _cleanupManifest(CHAIN_ID_OVERWRITE);
        DeployConfig memory cfg = this.externalReadConfig();
        Deployment memory d = _stubDeployment();
        vm.chainId(CHAIN_ID_OVERWRITE);

        // Internal first-write so the file is observable to the second call;
        // see comment in `test_writeManifest_revertsOnExistingFile`.
        _writeManifest(cfg, d);

        vm.setEnv("FORCE_OVERWRITE_MANIFEST", "true");
        // Overwrite with a different deployment to prove the file was rewritten.
        d.token = Token(address(0xAA));
        _writeManifest(cfg, d);

        string memory json = vm.readFile(_manifestPath(CHAIN_ID_OVERWRITE));
        assertEq(json.readAddress(".contracts.Token"), address(0xAA), "Token overwritten");

        vm.setEnv("FORCE_OVERWRITE_MANIFEST", "false");
        _cleanupManifest(CHAIN_ID_OVERWRITE);
    }

    // -----------------------------------------------------------------
    // run() — top-level guard
    // -----------------------------------------------------------------

    function test_run_revertsIfDeployerIsForgeDefaultSender() public {
        // Forge's DEFAULT_SENDER (`0x1804…`) is the tx.origin when `--sender`
        // is omitted. The script must reject this to prevent broadcasting
        // from an EOA the operator did not intentionally choose.
        vm.startPrank(address(this), FORGE_DEFAULT_SENDER);
        vm.expectRevert(
            abi.encodeWithSelector(DeployProtocol.DeployerIsForgeDefaultSender.selector, FORGE_DEFAULT_SENDER)
        );
        this.run();
        vm.stopPrank();
    }
}
