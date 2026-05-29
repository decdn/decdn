// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "./BaseProtocolDeploy.s.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";

/// @title DeployProtocol — production v3 contract surface deployer (issue #694)
/// @notice Single `forge script` entry point that deploys every production
///         contract in ADR 016 § Deployment Order and Initialization
///         Dependencies, wires cross-contract roles, and atomically hands off
///         `GOVERNANCE_ROLE` + `DEFAULT_ADMIN_ROLE` from the deployer EOA to a
///         fresh `TimelockController`. On exit the deployer holds NO privileged
///         role on any deployed contract — the post-deploy invariant
///         `_assertNoBackDoors` reverts the run if any step left a back door
///         open.
///
/// @dev    BuybackBurner is NOT deployed here. The contract is abstract pending
///         a concrete Balancer V3 Vault subclass; until that ships, the deploy
///         leaves `FeeRouter.buybackBurner == address(0)` and the burn share
///         at 0 (`feeRouterShares[1] == 0`). Governance later activates the
///         bucket via `FeeRouter.setSharesAndDestinations` once a concrete
///         subclass is deployed — that path runs through the 48h Timelock and
///         is fully observable in advance.
///
/// @dev    The Ed25519 verifier is supplied via `ED25519_VERIFIER_ADDRESS`
///         because no production verifier ships in this repo yet (only the
///         test mock under `test/mocks/`). The operator deploys their chosen
///         implementation (real precompile shim, audited mock, etc.) ahead of
///         time and passes its address here.
///
///         Required env vars:
///           - `USDC_ADDRESS`              — settlement token (e.g. Arbitrum
///                                            Sepolia USDC `0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d`)
///           - `ED25519_VERIFIER_ADDRESS`  — operator-deployed verifier
///           - `EMERGENCY_MULTISIG`        — 3-of-5 multisig per ADR 009
///           - `INITIAL_TOKEN_HOLDER`      — 1B TOKEN recipient at genesis
///           - `CHALLENGER_INCENTIVE_POOL` — SlashAppeal failed-appeal-bond pool
///
///         The FeeRouter treasury bucket is NOT an env var: it is the
///         `TimelockController` this script deploys (step 7 sets it as the
///         FeeRouter treasury destination), so the 10% treasury leg is
///         Timelock-custodied and disbursed only by governance proposal (ADR 016
///         § Deployment Order step 3 / § Contracts Holding Funds). Re-pointing it
///         post-deploy requires a `FeeRouter.setTreasury` proposal through the
///         48h Timelock.
///
///         The FeeRouter epoch length is also NOT an env var: it must equal
///         `CapacityBond.EPOCH_LENGTH` (a compile-time `7 days` constant the
///         FeeRouter constructor cross-checks), so it is read from there rather
///         than a tunable that could only ever be 7 days.
///
///         Optional env vars (defaults from ADR 026 / ADR 028 / ADR 009 / 036):
///           - `TIMELOCK_DELAY`             (default 48h; floor MIN_TIMELOCK_DELAY)
///           - `MIN_STAKE`                  (default 50_000e18)
///           - `UNBONDING_PERIOD`           (default 14 days)
///           - `MULTIADDR_UPDATE_COOLDOWN`  (default 0)
///           - `MAX_MULTIADDR_SIZE`         (default 1024)
///           - `REGION_STABILITY_WINDOW`    (default 7 days)
///           - `GENESIS_CREDIT_WINDOW`      (default 30 days)
///           - `FEE_ROUTER_WINDOW_EPOCHS`   (default 13; bounded [4, 26] per ADR 036)
///           - `SLASH_APPEAL_BOND`          (default 1000e18)
///           - `BLACKLIST_APPEAL_BOND`      (default 100e18)
contract DeployProtocol is BaseProtocolDeploy {
    // Defaults — ADR 026 / 028 / 009 values.
    uint256 internal constant DEFAULT_TIMELOCK_DELAY = 48 hours;
    // Floor below which a Timelock has no meaningful reaction window against a
    // malicious proposal — guards against an accidental `TIMELOCK_DELAY=0` (which
    // OZ's TimelockController silently accepts). Production should use the 48h
    // default per ADR 009; the floor only blocks footgun values.
    uint256 internal constant MIN_TIMELOCK_DELAY = 1 hours;
    uint256 internal constant DEFAULT_MIN_STAKE = 50_000e18;
    uint256 internal constant DEFAULT_UNBONDING_PERIOD = 14 days;
    uint256 internal constant DEFAULT_MULTIADDR_UPDATE_COOLDOWN = 0;
    uint256 internal constant DEFAULT_MAX_MULTIADDR_SIZE = 1024;
    uint256 internal constant DEFAULT_REGION_STABILITY_WINDOW = 7 days;
    uint256 internal constant DEFAULT_GENESIS_CREDIT_WINDOW = 30 days;
    // Fixed, NOT env-tunable: must equal `CapacityBond.EPOCH_LENGTH` (also 7 days).
    // The FeeRouter constructor reverts `EpochLengthMismatch` if they ever drift,
    // so this constant + that cross-check is the single enforced source of truth.
    uint64 internal constant FEE_ROUTER_EPOCH_LENGTH = 7 days;
    uint64 internal constant DEFAULT_FEE_ROUTER_WINDOW_EPOCHS = 13;
    uint256 internal constant DEFAULT_SLASH_APPEAL_BOND = 1000e18;
    uint256 internal constant DEFAULT_BLACKLIST_APPEAL_BOND = 100e18;

    // Launch fee-router shares: 90% operator / 0% buyback / 10% treasury
    // (3-bucket split, ADR 026 § FeeRouter). The buyback bucket is dormant
    // pending a concrete BuybackBurner subclass; its 30% steady-state share is
    // temporarily routed to operators (operator pinned at its 9000-bps ceiling)
    // while treasury holds its 10% target. Both values satisfy
    // `FeeRouter._setShares` bounds: operator ∈ [4000, 9000], buyback ∈ {0} ∪
    // [500, 5000], treasury ∈ [0, 3000].
    uint256 internal constant LAUNCH_OPERATOR_SHARE = 9000;
    uint256 internal constant LAUNCH_TREASURY_SHARE = 1000;

    /// @notice The deploy refuses to overwrite an existing manifest unless
    ///         `FORCE_OVERWRITE_MANIFEST=true`. Prevents the case where a
    ///         re-run on the same chain silently scrambles the address list
    ///         downstream tooling has already cached.
    error ManifestAlreadyExists(string path);

    /// @notice `tx.origin` was the forge-default sender (no `--sender` flag).
    ///         Refuses to deploy because the operator almost certainly did not
    ///         intend to broadcast from forge's deterministic fallback EOA.
    error DeployerIsForgeDefaultSender(address sender);

    /// @notice `TIMELOCK_DELAY` was below `MIN_TIMELOCK_DELAY` — a delay that
    ///         short defeats the governance reaction window (ADR 009).
    error TimelockDelayTooShort(uint256 provided, uint256 floor);

    /// @notice An optional `uint64` parameter env var exceeded `type(uint64).max`
    ///         and would have silently truncated on the narrowing cast.
    error ParamOverflowsUint64(string field, uint256 provided);

    /// @dev forge's default `tx.origin` when `--sender` is omitted. Pinned to
    ///      forge-std v1.x's `DEFAULT_SENDER` constant; bump if forge changes it.
    address internal constant FORGE_DEFAULT_SENDER = 0x1804c8AB1F12E6bbf3894d4083f33e07309d1f38;

    function run() external returns (Deployment memory d) {
        DeployConfig memory cfg = _readConfig();
        if (cfg.deployer == FORGE_DEFAULT_SENDER) revert DeployerIsForgeDefaultSender(cfg.deployer);
        // Fail BEFORE spending gas: a stale manifest with no FORCE_OVERWRITE_MANIFEST
        // must abort here, not after `_runFullDeploy` has already broadcast the
        // irreversible deploy + governance handoff (which would leave the deployed
        // contracts with no recorded address manifest).
        _assertManifestWritable();

        vm.startBroadcast(cfg.deployer);
        d = _runFullDeploy(cfg);
        vm.stopBroadcast();

        _writeManifest(cfg, d);
    }

    function _readConfig() internal view returns (DeployConfig memory cfg) {
        cfg.usdc = IERC20(vm.envAddress("USDC_ADDRESS"));
        cfg.ed25519Verifier = IEd25519Verifier(vm.envAddress("ED25519_VERIFIER_ADDRESS"));
        cfg.emergencyMultisig = vm.envAddress("EMERGENCY_MULTISIG");
        cfg.initialTokenHolder = vm.envAddress("INITIAL_TOKEN_HOLDER");
        cfg.challengerIncentivePool = vm.envAddress("CHALLENGER_INCENTIVE_POOL");
        // `--sender` on the command line becomes `tx.origin` for the script;
        // use that as the deployer so the role grants the constructors emit
        // are attributable to the broadcasting EOA, not this script contract.
        // solhint-disable-next-line avoid-tx-origin
        cfg.deployer = tx.origin;

        cfg.timelockDelay = vm.envOr("TIMELOCK_DELAY", DEFAULT_TIMELOCK_DELAY);
        if (cfg.timelockDelay < MIN_TIMELOCK_DELAY) {
            revert TimelockDelayTooShort(cfg.timelockDelay, MIN_TIMELOCK_DELAY);
        }
        cfg.minStake = vm.envOr("MIN_STAKE", DEFAULT_MIN_STAKE);
        cfg.unbondingPeriod = vm.envOr("UNBONDING_PERIOD", DEFAULT_UNBONDING_PERIOD);
        cfg.multiaddrUpdateCooldown = vm.envOr("MULTIADDR_UPDATE_COOLDOWN", DEFAULT_MULTIADDR_UPDATE_COOLDOWN);
        cfg.maxMultiaddrSize = vm.envOr("MAX_MULTIADDR_SIZE", DEFAULT_MAX_MULTIADDR_SIZE);
        cfg.regionStabilityWindow = vm.envOr("REGION_STABILITY_WINDOW", DEFAULT_REGION_STABILITY_WINDOW);
        cfg.genesisCreditWindow = vm.envOr("GENESIS_CREDIT_WINDOW", DEFAULT_GENESIS_CREDIT_WINDOW);

        // Epoch length is fixed (see FEE_ROUTER_EPOCH_LENGTH) rather than a
        // "tunable" env var that could only ever be 7 days.
        cfg.feeRouterEpochLength = FEE_ROUTER_EPOCH_LENGTH;
        uint256 windowEpochs = vm.envOr("FEE_ROUTER_WINDOW_EPOCHS", uint256(DEFAULT_FEE_ROUTER_WINDOW_EPOCHS));
        if (windowEpochs > type(uint64).max) revert ParamOverflowsUint64("FEE_ROUTER_WINDOW_EPOCHS", windowEpochs);
        cfg.feeRouterWindowEpochs = uint64(windowEpochs);
        cfg.feeRouterShares = [LAUNCH_OPERATOR_SHARE, uint256(0), LAUNCH_TREASURY_SHARE];
        cfg.buybackBurner = address(0);

        cfg.slashAppealBond = vm.envOr("SLASH_APPEAL_BOND", DEFAULT_SLASH_APPEAL_BOND);
        cfg.blacklistAppealBond = vm.envOr("BLACKLIST_APPEAL_BOND", DEFAULT_BLACKLIST_APPEAL_BOND);
    }

    // -----------------------------------------------------------------
    // Manifest — `deployments/<chainId>.json`.
    //
    // Schema is intentionally flat with stable, deterministic keys so downstream
    // tooling can read it without needing a Solidity-side type. `BuybackBurner`
    // is recorded as `address(0)` to signal "unwired at launch" (see header).

    /// @dev Manifest path for the active chain. Single source of truth shared by
    ///      the early writability check (`run`) and the writer.
    function _manifestPath() internal view returns (string memory) {
        return string.concat("./deployments/", vm.toString(block.chainid), ".json");
    }

    /// @dev Reverts if a manifest already exists for this chain and
    ///      `FORCE_OVERWRITE_MANIFEST` is not set. Called both before broadcast
    ///      (fail-fast, no gas spent) and inside `_writeManifest` (so direct
    ///      callers and tests still get the guard).
    function _assertManifestWritable() internal view {
        string memory path = _manifestPath();
        if (vm.exists(path) && !vm.envOr("FORCE_OVERWRITE_MANIFEST", false)) {
            revert ManifestAlreadyExists(path);
        }
    }

    function _writeManifest(DeployConfig memory cfg, Deployment memory d) internal {
        _assertManifestWritable();
        string memory path = _manifestPath();
        string memory contracts = "contracts";
        vm.serializeAddress(contracts, "BuybackBurner", address(0));
        vm.serializeAddress(contracts, "CapacityBond", address(d.bond));
        vm.serializeAddress(contracts, "ContentBlacklist", address(d.blacklist));
        vm.serializeAddress(contracts, "DecdnGovernor", address(d.governor));
        vm.serializeAddress(contracts, "FeeRouter", address(d.router));
        vm.serializeAddress(contracts, "PublisherRegistry", address(d.registry));
        vm.serializeAddress(contracts, "SlashAppeal", address(d.slashAppeal));
        vm.serializeAddress(contracts, "TimelockController", address(d.timelock));
        string memory contractsJson = vm.serializeAddress(contracts, "Token", address(d.token));

        // Treasury is intentionally absent: it is the TimelockController above
        // (Timelock-custodied per ADR 016), recorded under `contracts`, not an
        // external dependency.
        string memory deps = "externalDeps";
        vm.serializeAddress(deps, "ed25519Verifier", address(cfg.ed25519Verifier));
        vm.serializeAddress(deps, "emergencyMultisig", cfg.emergencyMultisig);
        string memory depsJson = vm.serializeAddress(deps, "usdc", address(cfg.usdc));

        string memory params = "config";
        vm.serializeUint(params, "feeRouterEpochLength", cfg.feeRouterEpochLength);
        vm.serializeUint(params, "feeRouterWindowEpochs", cfg.feeRouterWindowEpochs);
        uint256[] memory sharesArr = new uint256[](3);
        sharesArr[0] = cfg.feeRouterShares[0];
        sharesArr[1] = cfg.feeRouterShares[1];
        sharesArr[2] = cfg.feeRouterShares[2];
        vm.serializeUint(params, "feeRouterShares", sharesArr);
        vm.serializeUint(params, "minStake", cfg.minStake);
        string memory paramsJson = vm.serializeUint(params, "timelockDelay", cfg.timelockDelay);

        string memory root = "manifest";
        vm.serializeUint(root, "chainId", block.chainid);
        vm.serializeUint(root, "deployBlock", block.number);
        vm.serializeAddress(root, "deployer", cfg.deployer);
        vm.serializeString(root, "contracts", contractsJson);
        vm.serializeString(root, "externalDeps", depsJson);
        string memory rootJson = vm.serializeString(root, "config", paramsJson);

        vm.writeJson(rootJson, path);
    }
}
