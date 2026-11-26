// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "./BaseProtocolDeploy.s.sol";
import { IMessageTransmitterV2 } from "../src/interfaces/ICctpV2.sol";
import { Ed25519Verifier } from "../src/Ed25519Verifier.sol";

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
/// @dev    This is the governance-chain deploy. Each payment chain deploys its
///         `PaymentPool` with `DeployPaymentPool.s.sol`, pointed at the
///         `CctpSweepReceiver` this script deploys.
///
/// @dev    The production `Ed25519Verifier` (issue #669) is deployed in-script
///         as the first broadcast step — no operator-supplied address. It wraps
///         the audited Smoo.th Crypto Lib EIP-6565 verifier (vendored under
///         `lib/crypto-lib`); forge deploys + links the `SCL_EIP6565` library
///         automatically as part of the broadcast. If a native ed25519
///         precompile ever lands on Arbitrum (RIP-6565), swap the concrete
///         implementation here — a one-line code change gated by an ADR, per
///         the `IEd25519Verifier` swap-out note — rather than re-introducing a
///         deploy-time address knob.
///
///         Required env vars:
///           - `USDC_ADDRESS`              — governance-chain USDC (e.g. Ethereum
///                                            Sepolia USDC `0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238`)
///           - `CCTP_MESSAGE_TRANSMITTER`  — local CCTP V2 `MessageTransmitterV2`
///           - `CCTP_TOKEN_MESSENGER`      — local CCTP V2 `TokenMessengerV2`
///           - `EMERGENCY_MULTISIG`        — 3-of-5 multisig per ADR 009
///           - `INITIAL_TOKEN_HOLDER`      — 1B TOKEN recipient at genesis
///           - `CURRENT_TERMS_HASH`        — genesis terms hash per ADR 019
///                                            § Terms Acceptance; no default, and
///                                            `CapacityBond` rejects the zero sentinel
///
///         The FeeRouter treasury bucket is NOT an env var: it is the
///         `TimelockController` this script deploys (step 6 sets it as the
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
///         Optional env vars (defaults from ADR 026 / ADR 009 / 036):
///           - `BOOTSTRAP_MULTISIG`         (required on Ethereum mainnet, where the
///                                            deploy reverts `BootstrapMultisigRequired`
///                                            without it; on testnets, default unset =
///                                            DAO governance live at deploy). Set it to
///                                            the 5-of-9 bootstrap multisig to launch
///                                            into the ADR 009
///                                            § Bootstrap-multisig phase: the
///                                            multisig becomes the Timelock's sole
///                                            PROPOSER/CANCELLER and `DecdnGovernor`
///                                            gets neither, so every change still
///                                            carries the 48h delay but only the
///                                            multisig can schedule one. The phase
///                                            ends when the multisig schedules the
///                                            `TransitionToGovernor` batch — manual,
///                                            no threshold automation.
///           - `TIMELOCK_DELAY`             (default 48h; floor MIN_TIMELOCK_DELAY)
///           - `MIN_BOND`                   (default 50_000e18)
///           - `MULTIADDR_UPDATE_COOLDOWN`  (default 0)
///           - `MAX_MULTIADDR_SIZE`         (default 1024)
///           - `REGION_STABILITY_WINDOW`    (default 7 days)
///           - `FEE_ROUTER_WINDOW_EPOCHS`   (default 13; bounded [4, 26] per ADR 036)
contract DeployProtocol is BaseProtocolDeploy {
    // Defaults — ADR 026 / 009 values.
    uint256 internal constant DEFAULT_TIMELOCK_DELAY = 48 hours;
    // Floor below which a Timelock has no meaningful reaction window against a
    // malicious proposal — guards against an accidental `TIMELOCK_DELAY=0` (which
    // OZ's TimelockController silently accepts). Production should use the 48h
    // default per ADR 009; the floor only blocks footgun values.
    uint256 internal constant MIN_TIMELOCK_DELAY = 1 hours;
    uint256 internal constant DEFAULT_MIN_BOND = 50_000e18;
    uint256 internal constant DEFAULT_MULTIADDR_UPDATE_COOLDOWN = 0;
    uint256 internal constant DEFAULT_MAX_MULTIADDR_SIZE = 1024;
    uint256 internal constant DEFAULT_REGION_STABILITY_WINDOW = 7 days;
    // Fixed, NOT env-tunable: must equal `CapacityBond.EPOCH_LENGTH` (also 7 days).
    // The FeeRouter constructor reverts `EpochLengthMismatch` if they ever drift,
    // so this constant + that cross-check is the single enforced source of truth.
    uint64 internal constant FEE_ROUTER_EPOCH_LENGTH = 7 days;
    uint64 internal constant DEFAULT_FEE_ROUTER_WINDOW_EPOCHS = 13;

    // Launch FeeRouter split (ADR 016 § Tunable Economics example): with the
    // reference `operatorBps` of 6000 on each payment chain, a 75% rebate gives
    // the operator 90% and the treasury 10% of each redemption.
    uint256 internal constant LAUNCH_REBATE_BPS = 7500;
    // Launch weight-credit price: one treasury-leg USDC base unit per MiB (ADR 016).
    uint256 internal constant LAUNCH_DELIVERY_FLOOR = 1;

    /// @notice The deploy refuses to overwrite an existing manifest unless
    ///         `FORCE_OVERWRITE_MANIFEST=true`. Prevents the case where a
    ///         re-run on the same chain silently scrambles the address list
    ///         downstream tooling has already cached.
    error ManifestAlreadyExists(string path);

    /// @notice `INITIAL_VETTER` was unset (or zero). A deploy with no genesis
    ///         `VETTER_ROLE` holder cannot vet any publisher, so no origin is ever
    ///         seated and no bytes are served — and served-bytes governance
    ///         (ADR 036) cannot start to grant the role after the fact. The
    ///         deploy refuses rather than ship that deadlock.
    error MissingInitialVetter();

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
        // Deploy the production verifier first, inside the broadcast, so it
        // persists on-chain and forge links its `SCL_EIP6565` library. The base
        // pipeline then consumes it like any other dependency (test harnesses
        // inject a mock by populating `cfg.ed25519Verifier` and calling
        // `_runFullDeploy` directly, bypassing this path).
        cfg.ed25519Verifier = new Ed25519Verifier();
        d = _runFullDeploy(cfg);
        vm.stopBroadcast();

        _writeManifest(cfg, d);
    }

    function _readConfig() internal view returns (DeployConfig memory cfg) {
        cfg.usdc = IERC20(vm.envAddress("USDC_ADDRESS"));
        cfg.cctpMessageTransmitter = IMessageTransmitterV2(vm.envAddress("CCTP_MESSAGE_TRANSMITTER"));
        cfg.cctpTokenMessenger = vm.envAddress("CCTP_TOKEN_MESSENGER");
        // `cfg.ed25519Verifier` is intentionally left zero here: `run()` deploys
        // the production verifier inside the broadcast and populates it before
        // `_runFullDeploy`. `_deployTargets`' zero-address guard still protects
        // any caller that forgets to set it.
        cfg.emergencyMultisig = vm.envAddress("EMERGENCY_MULTISIG");
        cfg.initialTokenHolder = vm.envAddress("INITIAL_TOKEN_HOLDER");
        // Genesis ManualVettingPolicy VETTER_ROLE holder (ADR 011). REQUIRED: a
        // deploy with no vetter is a deadlock — no VETTER_ROLE holder means no
        // publisher can be vetted, so no origin can be seated and nothing gets
        // published; and served-bytes governance cannot begin until bytes are
        // served (ADR 036), so there is no one to grant the role after the fact
        // either. Fail fast rather than ship that chicken-and-egg.
        cfg.initialVetter = vm.envOr("INITIAL_VETTER", address(0));
        if (cfg.initialVetter == address(0)) revert MissingInitialVetter();
        // `--sender` on the command line becomes `tx.origin` for the script;
        // use that as the deployer so the role grants the constructors emit
        // are attributable to the broadcasting EOA, not this script contract.
        // solhint-disable-next-line avoid-tx-origin
        cfg.deployer = tx.origin;

        cfg.timelockDelay = vm.envOr("TIMELOCK_DELAY", DEFAULT_TIMELOCK_DELAY);
        if (cfg.timelockDelay < MIN_TIMELOCK_DELAY) {
            revert TimelockDelayTooShort(cfg.timelockDelay, MIN_TIMELOCK_DELAY);
        }
        // ADR 009 § Bootstrap-multisig phase. Unset (the testnet default) = DAO
        // governance live at deploy; `_deployTargets` rejects unset on Ethereum
        // mainnet. Set = seat this multisig as the Timelock's sole proposer and
        // withhold the role from `DecdnGovernor` until the transition batch runs
        // (see `script/TransitionToGovernor.s.sol`, issue #1175).
        cfg.bootstrapMultisig = vm.envOr("BOOTSTRAP_MULTISIG", address(0));
        cfg.minBond = vm.envOr("MIN_BOND", DEFAULT_MIN_BOND);
        cfg.multiaddrUpdateCooldown = vm.envOr("MULTIADDR_UPDATE_COOLDOWN", DEFAULT_MULTIADDR_UPDATE_COOLDOWN);
        cfg.maxMultiaddrSize = vm.envOr("MAX_MULTIADDR_SIZE", DEFAULT_MAX_MULTIADDR_SIZE);
        cfg.regionStabilityWindow = vm.envOr("REGION_STABILITY_WINDOW", DEFAULT_REGION_STABILITY_WINDOW);
        // ADR 019 § Terms Acceptance — genesis operator-terms hash
        // (keccak256 of the shipped TERMS.md). REQUIRED, no default: genesis
        // must commit to a real (possibly draft) terms version, and
        // `CapacityBond`'s constructor rejects the zero sentinel. This forbids
        // an accidental terms-disabled deployment.
        cfg.currentTermsHash = vm.envBytes32("CURRENT_TERMS_HASH");

        // Epoch length is fixed (see FEE_ROUTER_EPOCH_LENGTH) rather than a
        // "tunable" env var that could only ever be 7 days.
        cfg.feeRouterEpochLength = FEE_ROUTER_EPOCH_LENGTH;
        uint256 windowEpochs = vm.envOr("FEE_ROUTER_WINDOW_EPOCHS", uint256(DEFAULT_FEE_ROUTER_WINDOW_EPOCHS));
        if (windowEpochs > type(uint64).max) revert ParamOverflowsUint64("FEE_ROUTER_WINDOW_EPOCHS", windowEpochs);
        cfg.feeRouterWindowEpochs = uint64(windowEpochs);
        cfg.feeRouterRebateBps = LAUNCH_REBATE_BPS;
        cfg.feeRouterDeliveryFloor = LAUNCH_DELIVERY_FLOOR;
    }

    // -----------------------------------------------------------------
    // Manifest — `deployments/<chainId>.json`.
    //
    // Schema is intentionally flat with stable, deterministic keys so downstream
    // tooling can read it without needing a Solidity-side type.

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
        vm.serializeAddress(contracts, "CctpSweepReceiver", address(d.sweepReceiver));
        vm.serializeAddress(contracts, "CapacityBond", address(d.bond));
        vm.serializeAddress(contracts, "ContentBlacklist", address(d.blacklist));
        vm.serializeAddress(contracts, "DecdnGovernor", address(d.governor));
        vm.serializeAddress(contracts, "Ed25519Verifier", address(cfg.ed25519Verifier));
        vm.serializeAddress(contracts, "FeeRouter", address(d.router));
        vm.serializeAddress(contracts, "PublisherRegistry", address(d.registry));
        vm.serializeAddress(contracts, "OriginAssignment", address(d.originAssignment));
        vm.serializeAddress(contracts, "ManualVettingPolicy", address(d.vettingPolicy));
        vm.serializeAddress(contracts, "TimelockController", address(d.timelock));
        string memory contractsJson = vm.serializeAddress(contracts, "Token", address(d.token));

        // Treasury is intentionally absent: it is the TimelockController above
        // (Timelock-custodied per ADR 016), recorded under `contracts`, not an
        // external dependency. The Ed25519Verifier is likewise under `contracts`
        // (this script deploys it — issue #669), not here.
        string memory deps = "externalDeps";
        // `address(0)` on a direct-to-Timelock deploy; otherwise the ADR 009
        // bootstrap multisig holding the Timelock's PROPOSER_ROLE/CANCELLER_ROLE
        // until it transitions. It never holds GOVERNANCE_ROLE on the targets.
        vm.serializeAddress(deps, "bootstrapMultisig", cfg.bootstrapMultisig);
        vm.serializeAddress(deps, "cctpMessageTransmitter", address(cfg.cctpMessageTransmitter));
        vm.serializeAddress(deps, "cctpTokenMessenger", cfg.cctpTokenMessenger);
        vm.serializeAddress(deps, "emergencyMultisig", cfg.emergencyMultisig);
        string memory depsJson = vm.serializeAddress(deps, "usdc", address(cfg.usdc));

        string memory params = "config";
        vm.serializeUint(params, "feeRouterDeliveryFloor", cfg.feeRouterDeliveryFloor);
        vm.serializeUint(params, "feeRouterEpochLength", cfg.feeRouterEpochLength);
        vm.serializeUint(params, "feeRouterRebateBps", cfg.feeRouterRebateBps);
        vm.serializeUint(params, "feeRouterWindowEpochs", cfg.feeRouterWindowEpochs);
        vm.serializeUint(params, "minBond", cfg.minBond);
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
