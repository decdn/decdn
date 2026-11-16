// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { PaymentPool } from "../src/PaymentPool.sol";
import { ITokenMessengerV2 } from "../src/interfaces/ICctpV2.sol";

/// @title DeployPaymentPool — the payment-chain deploy (ADR 016 § Deployment Tiers)
/// @notice Deploys one CCTP V2 `PaymentPool` on a payment chain and writes
///         `deployments/<chainId>-payment.json`. The pool has no admin and no
///         roles, so there is no handoff: every parameter below is fixed for the
///         contract's life.
///
///         Required env vars:
///           - `USDC_ADDRESS`          — native USDC on this chain
///           - `CCTP_TOKEN_MESSENGER`  — this chain's CCTP V2 `TokenMessengerV2`
///           - `SWEEP_RECIPIENT`       — the governance-chain `CctpSweepReceiver`
///           - `PAUSER`                — the emergency multisig on this chain
///
///         Optional env vars (reference deployment values):
///           - `SWEEP_DOMAIN`          (default 0, Ethereum's CCTP domain)
///           - `DISPUTE_WINDOW`        (default 48h; must exceed this chain's
///                                       censorship window, ADR 003)
///           - `OPERATOR_BPS`          (default 6000)
///           - `MIN_SWEEP`             (default 1_000_000 = 1 USDC; size it so one
///                                       sweep's protocol share covers the
///                                       governance-chain receive gas)
///           - `MIN_DEPOSIT`           (default 0, the dormant Sybil floor)
contract DeployPaymentPool is Script {
    uint256 internal constant DEFAULT_DISPUTE_WINDOW = 48 hours;
    uint256 internal constant DEFAULT_OPERATOR_BPS = 6000;
    uint256 internal constant DEFAULT_MIN_SWEEP = 1_000_000;
    uint256 internal constant DEFAULT_MIN_DEPOSIT = 0;

    /// @dev forge's default `tx.origin` when `--sender` is omitted.
    address internal constant FORGE_DEFAULT_SENDER = 0x1804c8AB1F12E6bbf3894d4083f33e07309d1f38;

    /// @notice The pool parameters, read from env.
    struct PoolConfig {
        IERC20 usdc;
        ITokenMessengerV2 tokenMessenger;
        address sweepRecipient;
        uint32 sweepDomain;
        uint32 disputeWindow;
        uint256 operatorBps;
        uint64 minSweep;
        uint64 minDeposit;
        address pauser;
    }

    /// @notice A manifest for this chain exists and `FORCE_OVERWRITE_MANIFEST`
    ///         is unset.
    error ManifestAlreadyExists(string path);
    /// @notice `--sender` was omitted.
    error DeployerIsForgeDefaultSender(address sender);
    /// @notice A `uint64` or `uint32` env var does not fit its field.
    error ParamOverflows(string field, uint256 provided);

    function run() external returns (PaymentPool pool) {
        PoolConfig memory cfg = _readConfig();
        // solhint-disable-next-line avoid-tx-origin
        address deployer = tx.origin;
        if (deployer == FORGE_DEFAULT_SENDER) revert DeployerIsForgeDefaultSender(deployer);
        _assertManifestWritable();

        vm.startBroadcast(deployer);
        pool = deploy(cfg);
        vm.stopBroadcast();

        _writeManifest(cfg, pool, deployer);
    }

    /// @notice Construct the pool from `cfg`. The constructor rejects zero
    ///         addresses and an `operatorBps` above 10000.
    function deploy(PoolConfig memory cfg) public returns (PaymentPool) {
        return new PaymentPool({
            usdc_: cfg.usdc,
            tokenMessenger_: cfg.tokenMessenger,
            sweepRecipient_: cfg.sweepRecipient,
            sweepDomain_: cfg.sweepDomain,
            disputeWindow_: cfg.disputeWindow,
            operatorBps_: cfg.operatorBps,
            minSweep_: cfg.minSweep,
            minDeposit_: cfg.minDeposit,
            pauser_: cfg.pauser
        });
    }

    function _readConfig() internal view returns (PoolConfig memory cfg) {
        cfg.usdc = IERC20(vm.envAddress("USDC_ADDRESS"));
        cfg.tokenMessenger = ITokenMessengerV2(vm.envAddress("CCTP_TOKEN_MESSENGER"));
        cfg.sweepRecipient = vm.envAddress("SWEEP_RECIPIENT");
        cfg.pauser = vm.envAddress("PAUSER");
        uint256 domain = vm.envOr("SWEEP_DOMAIN", uint256(0));
        if (domain > type(uint32).max) revert ParamOverflows("SWEEP_DOMAIN", domain);
        cfg.sweepDomain = uint32(domain);
        uint256 window = vm.envOr("DISPUTE_WINDOW", DEFAULT_DISPUTE_WINDOW);
        if (window > type(uint32).max) revert ParamOverflows("DISPUTE_WINDOW", window);
        cfg.disputeWindow = uint32(window);
        cfg.operatorBps = vm.envOr("OPERATOR_BPS", DEFAULT_OPERATOR_BPS);
        cfg.minSweep = _u64("MIN_SWEEP", vm.envOr("MIN_SWEEP", DEFAULT_MIN_SWEEP));
        cfg.minDeposit = _u64("MIN_DEPOSIT", vm.envOr("MIN_DEPOSIT", DEFAULT_MIN_DEPOSIT));
    }

    function _u64(string memory field, uint256 value) internal pure returns (uint64) {
        if (value > type(uint64).max) revert ParamOverflows(field, value);
        return uint64(value);
    }

    function _manifestPath() internal view returns (string memory) {
        return string.concat("./deployments/", vm.toString(block.chainid), "-payment.json");
    }

    function _assertManifestWritable() internal view {
        string memory path = _manifestPath();
        if (vm.exists(path) && !vm.envOr("FORCE_OVERWRITE_MANIFEST", false)) {
            revert ManifestAlreadyExists(path);
        }
    }

    function _writeManifest(PoolConfig memory cfg, PaymentPool pool, address deployer) internal {
        string memory contracts = "contracts";
        string memory contractsJson = vm.serializeAddress(contracts, "PaymentPool", address(pool));

        string memory deps = "externalDeps";
        vm.serializeAddress(deps, "cctpTokenMessenger", address(cfg.tokenMessenger));
        vm.serializeAddress(deps, "pauser", cfg.pauser);
        vm.serializeAddress(deps, "sweepRecipient", cfg.sweepRecipient);
        string memory depsJson = vm.serializeAddress(deps, "usdc", address(cfg.usdc));

        string memory params = "config";
        vm.serializeUint(params, "disputeWindow", cfg.disputeWindow);
        vm.serializeUint(params, "minDeposit", cfg.minDeposit);
        vm.serializeUint(params, "minSweep", cfg.minSweep);
        vm.serializeUint(params, "operatorBps", cfg.operatorBps);
        string memory paramsJson = vm.serializeUint(params, "sweepDomain", cfg.sweepDomain);

        string memory root = "manifest";
        vm.serializeUint(root, "chainId", block.chainid);
        vm.serializeUint(root, "deployBlock", block.number);
        vm.serializeAddress(root, "deployer", deployer);
        vm.serializeString(root, "contracts", contractsJson);
        vm.serializeString(root, "externalDeps", depsJson);
        string memory rootJson = vm.serializeString(root, "config", paramsJson);

        vm.writeJson(rootJson, _manifestPath());
    }
}
