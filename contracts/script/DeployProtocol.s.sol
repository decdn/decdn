// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "./BaseProtocolDeploy.s.sol";
import { BuybackVenueLib } from "./lib/BuybackVenueLib.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
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
/// @dev    BuybackBurner is dormant by default: the deploy leaves
///         `FeeRouter.buybackBurner == address(0)` and the burn share at 0
///         (`feeRouterShares[1] == 0`). Mainnet activates the bucket later via
///         `FeeRouter.setSharesAndDestinations` through the 48h Timelock (ADR 018
///         § Activation Criteria) — fully observable in advance.
///
///         `ACTIVATE_BUYBACK=true` opts into an OFF-by-default deploy-time genesis
///         activation instead (a testnet / genesis convenience): the script seeds
///         the venue pool, deploys the concrete burner, and flips the FeeRouter to
///         the steady-state `[6000, 3000, 1000]` split in-script — before the
///         governance handoff, because at genesis the served-bytes voting weight
///         that gates the governance path is zero (ADR 036). Venue is selected via
///         `BUYBACK_VENUE`; see `_readBuybackActivation`.
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
///           - `USDC_ADDRESS`              — settlement token (e.g. Arbitrum
///                                            Sepolia USDC `0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d`)
///           - `EMERGENCY_MULTISIG`        — 3-of-5 multisig per ADR 009
///           - `INITIAL_TOKEN_HOLDER`      — 1B TOKEN recipient at genesis
///           - `CURRENT_TERMS_HASH`        — genesis terms hash per ADR 019
///                                            § Terms Acceptance; no default, and
///                                            `CapacityBond` rejects the zero sentinel
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
///           - `BOOTSTRAP_MULTISIG`         (default unset = DAO governance live at
///                                            deploy). Set it to the 5-of-9 bootstrap
///                                            multisig to launch into the ADR 009
///                                            § Bootstrap-multisig phase instead: the
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
///           - `UNBONDING_PERIOD`           (default 14 days)
///           - `MULTIADDR_UPDATE_COOLDOWN`  (default 0)
///           - `MAX_MULTIADDR_SIZE`         (default 1024)
///           - `REGION_STABILITY_WINDOW`    (default 7 days)
///           - `FEE_ROUTER_WINDOW_EPOCHS`   (default 13; bounded [4, 26] per ADR 036)
///           - `SLASH_APPEAL_BOND`          (default 1000e18)
contract DeployProtocol is BaseProtocolDeploy {
    // Defaults — ADR 026 / 028 / 009 values.
    uint256 internal constant DEFAULT_TIMELOCK_DELAY = 48 hours;
    // Floor below which a Timelock has no meaningful reaction window against a
    // malicious proposal — guards against an accidental `TIMELOCK_DELAY=0` (which
    // OZ's TimelockController silently accepts). Production should use the 48h
    // default per ADR 009; the floor only blocks footgun values.
    uint256 internal constant MIN_TIMELOCK_DELAY = 1 hours;
    uint256 internal constant DEFAULT_MIN_BOND = 50_000e18;
    uint256 internal constant DEFAULT_UNBONDING_PERIOD = 14 days;
    uint256 internal constant DEFAULT_MULTIADDR_UPDATE_COOLDOWN = 0;
    uint256 internal constant DEFAULT_MAX_MULTIADDR_SIZE = 1024;
    uint256 internal constant DEFAULT_REGION_STABILITY_WINDOW = 7 days;
    // Fixed, NOT env-tunable: must equal `CapacityBond.EPOCH_LENGTH` (also 7 days).
    // The FeeRouter constructor reverts `EpochLengthMismatch` if they ever drift,
    // so this constant + that cross-check is the single enforced source of truth.
    uint64 internal constant FEE_ROUTER_EPOCH_LENGTH = 7 days;
    uint64 internal constant DEFAULT_FEE_ROUTER_WINDOW_EPOCHS = 13;
    uint256 internal constant DEFAULT_SLASH_APPEAL_BOND = 1000e18;

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
        BuybackActivation memory act = _readBuybackActivation();
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
        d = _runFullDeploy(cfg, act);
        vm.stopBroadcast();

        _writeManifest(cfg, d);
    }

    function _readConfig() internal view returns (DeployConfig memory cfg) {
        cfg.usdc = IERC20(vm.envAddress("USDC_ADDRESS"));
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
        // ADR 009 § Bootstrap-multisig phase. Unset (the default) = DAO governance
        // live at deploy. Set = seat this multisig as the Timelock's sole proposer
        // and withhold the role from `DecdnGovernor` until the transition batch runs
        // (see `script/TransitionToGovernor.s.sol`, issue #1175).
        cfg.bootstrapMultisig = vm.envOr("BOOTSTRAP_MULTISIG", address(0));
        cfg.minBond = vm.envOr("MIN_BOND", DEFAULT_MIN_BOND);
        cfg.unbondingPeriod = vm.envOr("UNBONDING_PERIOD", DEFAULT_UNBONDING_PERIOD);
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
        cfg.feeRouterShares = [LAUNCH_OPERATOR_SHARE, uint256(0), LAUNCH_TREASURY_SHARE];
        cfg.buybackBurner = address(0);

        cfg.slashAppealBond = vm.envOr("SLASH_APPEAL_BOND", DEFAULT_SLASH_APPEAL_BOND);
    }

    // -----------------------------------------------------------------
    // Optional genesis buyback activation (OFF by default).
    //
    // Left unset (`ACTIVATE_BUYBACK` unset/false), the launch is byte-for-byte the
    // dormant deploy: `_readBuybackActivation` returns an all-off struct and
    // `_runFullDeploy` skips the whole path. This is a testnet / genesis
    // convenience; mainnet activates through the ADR 018 governance-gated path.
    //
    //   Required when ON:
    //     - `BUYBACK_KEEPER`             — EOA/bot granted KEEPER_ROLE
    //   Optional (defaults from ADR 018 § Parameter Table):
    //     - `BUYBACK_VENUE`              — uniswap|balancer (default uniswap; the
    //                                      only venue live on Arbitrum Sepolia)
    //     - `TWAP_MIN_WINDOW_SECS`       (default 1800)
    //     - `MAX_BUYBACK_AMOUNT`         (default 10_000e6 USDC; must be non-zero)
    //     - `MIN_BUYBACK_AMOUNT`         (default 100e6 USDC)
    //     - `SLIPPAGE_BPS`               (default 200; ceiling 1000 = 10%)
    //     - `EPOCH_CAP_FRACTION_BPS`     (default 1000 = 10%)
    //   Pool seed (both venues create + seed the pool in-script; the deployer must
    //   hold the seed TOKEN + USDC — set `INITIAL_TOKEN_HOLDER` to the deployer or
    //   fund it). Pick a USDC amount you hold and a target price; the paired TOKEN
    //   seed is derived so the pool initializes at that price (venue weights applied):
    //     - `BUYBACK_USDC_SEED`          (default 100e6 = 100 USDC)
    //     - `BUYBACK_TARGET_PRICE`       (default 10_000 — the price of one TOKEN in
    //                                      USDC base units; 6-dec USDC → $0.01/TOKEN)
    //   Uniswap venue:
    //     - `UNISWAP_SWAP_ROUTER`        — SwapRouter02 (required)
    //     - `UNISWAP_POSITION_MANAGER`   — NonfungiblePositionManager (required)
    //     - `UNISWAP_POOL_FEE`           (default 10000 = 1%)
    //   Balancer venue (80/20 TOKEN/USDC weighted pool):
    //     - `BALANCER_WEIGHTED_POOL_FACTORY` — WeightedPoolFactory (required)
    //     - `BALANCER_ROUTER`, `BALANCER_VAULT` (required)
    //     - `PERMIT2_ADDRESS`            (default canonical Permit2)
    //     - `BALANCER_SWAP_FEE`          (default 1e16 = 1%)
    error PoolFeeOutOfRange(uint256 fee);

    function _readBuybackActivation() internal view returns (BuybackActivation memory act) {
        act.activate = vm.envOr("ACTIVATE_BUYBACK", false);
        if (!act.activate) return act; // all-off; every other field is ignored.

        act.venue = BuybackVenueLib.parseVenue(vm.envOr("BUYBACK_VENUE", string("uniswap")));
        act.keeper = vm.envAddress("BUYBACK_KEEPER"); // required — enforced in _activateBuyback too
        act.guard = GuardedBuybackBurner.GuardParams({
            twapMinWindow_: vm.envOr("TWAP_MIN_WINDOW_SECS", uint256(1800)),
            maxBuybackAmount_: vm.envOr("MAX_BUYBACK_AMOUNT", uint256(10_000e6)),
            minBuybackAmount_: vm.envOr("MIN_BUYBACK_AMOUNT", uint256(100e6)),
            slippageBps_: vm.envOr("SLIPPAGE_BPS", uint256(200)),
            epochLiquidityCapFraction_: vm.envOr("EPOCH_CAP_FRACTION_BPS", uint256(1000))
        });

        // Pool seed: pick a USDC amount you actually hold and a target TOKEN price;
        // the paired TOKEN seed is derived so the pool initializes at that price
        // (no need to hand-compute the ratio, and it differs per venue's weights).
        // `BUYBACK_TARGET_PRICE` is the price of one whole TOKEN in USDC base units
        // (6-dec USDC → `$0.01/TOKEN` is `10_000`).
        uint256 usdcSeed = vm.envOr("BUYBACK_USDC_SEED", uint256(100e6));
        uint256 targetPrice = vm.envOr("BUYBACK_TARGET_PRICE", uint256(10_000)); // $0.01/TOKEN
        // Built through the derivation helper so the seed records the venue it was
        // sized for; pairing it with the other venue mis-anchors the pool silently.
        act.seed = _derivePoolSeed(act.venue, usdcSeed, targetPrice);

        // Exactly one venue sub-struct is populated; the other stays zero, which is
        // what `_assertVenueFieldsScoped` enforces before the burner is deployed.
        if (act.venue == BuybackVenueLib.Venue.UNISWAP) {
            act.uni.swapRouter = vm.envAddress("UNISWAP_SWAP_ROUTER");
            act.uni.positionManager = vm.envAddress("UNISWAP_POSITION_MANAGER");
            // Validate the fee against the supported Uniswap V3 tiers here, before
            // `vm.startBroadcast`, so an unsupported tier aborts with no gas spent
            // rather than reverting `UnsupportedFeeTier` mid-deploy.
            uint256 fee = vm.envOr("UNISWAP_POOL_FEE", uint256(10_000));
            if (fee != 100 && fee != 500 && fee != 3000 && fee != 10_000) revert PoolFeeOutOfRange(fee);
            act.uni.poolFee = uint24(fee);
        } else {
            act.bal.factory = vm.envAddress("BALANCER_WEIGHTED_POOL_FACTORY");
            act.bal.swapFee = vm.envOr("BALANCER_SWAP_FEE", uint256(1e16)); // 1% (ADR 018 pool fee)
            // `pool` stays zero: it does not exist until `_activateBalancer` creates it.
            act.bal.wiring = BuybackVenueLib.BalancerWiring({
                swapRouter: vm.envAddress("BALANCER_ROUTER"),
                pool: address(0),
                vault: vm.envAddress("BALANCER_VAULT"),
                permit2: vm.envOr("PERMIT2_ADDRESS", BuybackVenueLib.CANONICAL_PERMIT2)
            });
        }
    }

    // -----------------------------------------------------------------
    // Manifest — `deployments/<chainId>.json`.
    //
    // Schema is intentionally flat with stable, deterministic keys so downstream
    // tooling can read it without needing a Solidity-side type. `BuybackBurner`
    // is `address(0)` for a dormant launch, or the wired concrete burner when
    // genesis activation ran (see header / `_readBuybackActivation`).

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
        // BuybackBurner is `address(0)` when the launch is dormant (the default);
        // when genesis activation ran it is the wired concrete burner. The activated
        // branch only occurs after a real `_activateBuyback` against a real
        // `FeeRouter`, so read the split straight from it (single source of truth);
        // the dormant branch — the only one the stub-deployment script tests hit —
        // falls back to the launch config so the writer stays stub-safe.
        address bb = address(d.buybackBurner);
        uint256[3] memory liveShares = bb == address(0) ? cfg.feeRouterShares : d.router.getShares();

        string memory contracts = "contracts";
        vm.serializeAddress(contracts, "BuybackBurner", bb);
        vm.serializeAddress(contracts, "CapacityBond", address(d.bond));
        vm.serializeAddress(contracts, "ContentBlacklist", address(d.blacklist));
        vm.serializeAddress(contracts, "DecdnGovernor", address(d.governor));
        vm.serializeAddress(contracts, "Ed25519Verifier", address(cfg.ed25519Verifier));
        vm.serializeAddress(contracts, "FeeRouter", address(d.router));
        vm.serializeAddress(contracts, "PublisherRegistry", address(d.registry));
        vm.serializeAddress(contracts, "SlashAppeal", address(d.slashAppeal));
        vm.serializeAddress(contracts, "OriginAssignment", address(d.originAssignment));
        vm.serializeAddress(contracts, "ManualVettingPolicy", address(d.vettingPolicy));
        vm.serializeAddress(contracts, "PaymentPool", address(d.paymentPool));
        vm.serializeAddress(contracts, "SlashJudge", address(d.slashJudge));
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
        vm.serializeAddress(deps, "emergencyMultisig", cfg.emergencyMultisig);
        string memory depsJson = vm.serializeAddress(deps, "usdc", address(cfg.usdc));

        string memory params = "config";
        vm.serializeUint(params, "feeRouterEpochLength", cfg.feeRouterEpochLength);
        vm.serializeUint(params, "feeRouterWindowEpochs", cfg.feeRouterWindowEpochs);
        uint256[] memory sharesArr = new uint256[](3);
        sharesArr[0] = liveShares[0];
        sharesArr[1] = liveShares[1];
        sharesArr[2] = liveShares[2];
        vm.serializeUint(params, "feeRouterShares", sharesArr);
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
