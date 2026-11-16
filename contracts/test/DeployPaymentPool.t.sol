// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { stdJson } from "forge-std/StdJson.sol";

import { DeployPaymentPool } from "../script/DeployPaymentPool.s.sol";
import { PaymentPool } from "../src/PaymentPool.sol";

/// @notice The payment-chain deploy: env plumbing, construction, and the
///         `deployments/<chainId>-payment.json` manifest.
contract DeployPaymentPoolTest is Test, DeployPaymentPool {
    using stdJson for string;

    // `vm.setEnv` is process-wide and suites run in parallel, so every env var
    // this suite shares with `DeployProtocolScriptTest` carries the same value.
    address internal constant TEST_USDC = address(0xa55D);
    address internal constant TEST_MESSENGER = address(0x8FE6);
    address internal constant TEST_RECEIVER = address(0xCAFE);
    address internal constant TEST_PAUSER = address(0xDEAD);
    uint256 internal constant CHAIN_ID = 31_337_698;

    function setUp() public {
        vm.setEnv("USDC_ADDRESS", vm.toString(TEST_USDC));
        vm.setEnv("CCTP_TOKEN_MESSENGER", vm.toString(TEST_MESSENGER));
        vm.setEnv("SWEEP_RECIPIENT", vm.toString(TEST_RECEIVER));
        vm.setEnv("PAUSER", vm.toString(TEST_PAUSER));
        vm.setEnv("FORCE_OVERWRITE_MANIFEST", "false");
    }

    function externalReadConfig() external view returns (PoolConfig memory) {
        return _readConfig();
    }

    function test_readConfig_appliesReferenceDefaults() public view {
        PoolConfig memory cfg = this.externalReadConfig();
        assertEq(address(cfg.usdc), TEST_USDC);
        assertEq(address(cfg.tokenMessenger), TEST_MESSENGER);
        assertEq(cfg.sweepRecipient, TEST_RECEIVER);
        assertEq(cfg.pauser, TEST_PAUSER);
        assertEq(cfg.sweepDomain, 0, "Ethereum domain");
        assertEq(cfg.disputeWindow, DEFAULT_DISPUTE_WINDOW);
        assertEq(cfg.operatorBps, DEFAULT_OPERATOR_BPS);
        assertEq(cfg.minSweep, DEFAULT_MIN_SWEEP);
        assertEq(cfg.minDeposit, DEFAULT_MIN_DEPOSIT);
    }

    function test_deployAndManifest() public {
        PoolConfig memory cfg = this.externalReadConfig();
        PaymentPool pool = deploy(cfg);
        assertEq(pool.operatorBps(), 6000);
        assertEq(pool.sweepRecipient(), TEST_RECEIVER);

        vm.chainId(CHAIN_ID);
        string memory path = _manifestPath();
        if (vm.exists(path)) vm.removeFile(path);
        _writeManifest(cfg, pool, address(0xD0));

        string memory json = vm.readFile(path);
        assertEq(json.readAddress(".contracts.PaymentPool"), address(pool), "pool");
        assertEq(json.readAddress(".externalDeps.sweepRecipient"), TEST_RECEIVER, "recipient");
        assertEq(json.readAddress(".externalDeps.cctpTokenMessenger"), TEST_MESSENGER, "messenger");
        assertEq(json.readUint(".config.operatorBps"), 6000, "operatorBps");
        assertEq(json.readUint(".config.sweepDomain"), 0, "sweepDomain");
        assertEq(json.readUint(".chainId"), CHAIN_ID, "chainId");
        vm.removeFile(path);
    }
}
