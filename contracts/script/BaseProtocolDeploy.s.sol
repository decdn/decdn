// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { Token } from "../src/Token.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { SlashAppeal } from "../src/SlashAppeal.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { PaymentChannel } from "../src/PaymentChannel.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { OriginAssignment } from "../src/OriginAssignment.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { IPermit2 } from "../src/interfaces/IPermit2.sol";
import { BuybackVenueLib } from "./lib/BuybackVenueLib.sol";
import { INonfungiblePositionManager } from "./interfaces/IUniswapV3PoolCreation.sol";
import { IBalancerV3RouterInit, IBalancerV3WeightedPoolFactory } from "./interfaces/IBalancerV3PoolCreation.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { ISlashJudgeEvidenceView } from "../src/interfaces/ISlashJudgeEvidenceView.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondEpoch } from "../src/interfaces/ICapacityBondEpoch.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";
import { IContentBlacklistHashView } from "../src/interfaces/IContentBlacklistHashView.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";

/// @title BaseProtocolDeploy
/// @notice Abstract deploy primitive for the v3 contract surface. Performs the
///         six-phase deploy described in ADR 016 § Deployment Order and
///         Initialization Dependencies + § Post-Deployment Initialization, then
///         asserts the deployer EOA holds no role on any target. Inherited by
///         `DeployProtocol.s.sol` (the env-var production script) and
///         `DeployProtocol.t.sol` (the in-process role-topology test) so both
///         share one source of truth.
///
/// @dev    Phases:
///           1. `_deployTimelock`        — TimelockController (deployer as admin;
///                                         empty proposers; open executor).
///                                         ADR 016 § Deployment Order step 3
///                                         (TOKEN/USDC precede it in the ADR, but
///                                         TOKEN is co-deployed in phase 2 and
///                                         USDC is an external input — so the
///                                         Timelock is first among this script's
///                                         deploys) so its address is the
///                                         FeeRouter treasury bucket destination
///                                         (the treasury is Timelock-custodied per
///                                         ADR 016) and the eventual
///                                         DEFAULT_ADMIN_ROLE holder of every target.
///           2. `_deployTargets`         — Token, CapacityBond, SlashAppeal,
///                                         FeeRouter (treasury bucket = the
///                                         TimelockController; buyback bucket
///                                         dormant — see BuybackBurner note below),
///                                         ContentBlacklist, PublisherRegistry,
///                                         PaymentChannel, SlashJudge,
///                                         OriginAssignment. Deployer is admin of
///                                         every AccessControl-bearing target.
///           3. `_deployGovernor`        — DecdnGovernor; grant Timelock's
///                                         PROPOSER + CANCELLER roles to the
///                                         Governor.
///           4. `_wireCrossContractRoles` — peer role grants (slash-appeal driver,
///                                          blacklist ejector,
///                                          emergency multisig, PAUSER_ROLE on every
///                                          Pausable target,
///                                          router-caller → PaymentChannel,
///                                          SLASH_ROLE → SlashJudge) plus the
///                                          deployer-only
///                                          `OriginAssignment.setContentBlacklist`
///                                          setter that MUST run before the
///                                          GOVERNANCE_ROLE handoff because it
///                                          becomes Timelock-gated post-handoff.
///           5. `_handOffGovernance`      — grant-before-revoke loop over every
///                                          target for both GOVERNANCE_ROLE and
///                                          DEFAULT_ADMIN_ROLE, then renounce the
///                                          deployer's admin on the Timelock
///                                          itself. Grant-before-revoke ordering
///                                          is mandatory; reversing it strands
///                                          the contract ungoverned mid-tx.
///           6. `_assertNoBackDoors`      — reverts if deployer still holds
///                                          GOVERNANCE_ROLE or DEFAULT_ADMIN_ROLE
///                                          on any target, if the Timelock does not
///                                          hold both, or if the Timelock's sole
///                                          proposer is not the one this deploy
///                                          mode seats. Runs in-script (not
///                                          just in tests) so a mainnet deploy
///                                          refuses to finish if any handoff
///                                          step failed silently.
///           7. `_assertPeerRolesWired`   — reverts if any phase-4 peer-role grant
///                                          or address binding (slash trigger,
///                                          router-caller, pausers, emergency
///                                          multisig, blacklist binding) did not
///                                          land.
///                                          `grantRole` to a wrong address does not
///                                          revert, so without this a half-wired
///                                          protocol would ship silently.
///
///         Slash restitution is escrow-on-slash inside CapacityBond itself
///         (ADR 026 § Slashing, ADR 028) — there is no standalone reserve
///         contract. `SlashAppeal` drives the appeal state machine and is granted
///         `SLASH_APPEAL_ROLE` on CapacityBond so it can open/settle appeals.
///
///         BuybackBurner is dormant by default: the FeeRouter is constructed with
///         `buybackBurner_=address(0)` and `feeRouterShares[1]=0`, which
///         `FeeRouter._setShares` permits as launch-mode dormancy, and governance
///         activates the bucket later via `FeeRouter.setSharesAndDestinations`.
///         The OPTIONAL genesis path (`BuybackActivation.activate`, OFF by default)
///         instead seeds the venue pool, deploys the concrete burner, and flips the
///         split in-script before the handoff — see `_activateBuyback` and the
///         § Optional genesis buyback activation block below. When off, the deploy
///         is byte-for-byte the dormant launch.
abstract contract BaseProtocolDeploy is Script {
    using SafeERC20 for IERC20;

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant DEFAULT_ADMIN_ROLE = 0x00;

    // -----------------------------------------------------------------
    // Optional genesis buyback activation (ADR 018 § Deploy-time genesis
    // activation, ADR 016 § Optional deploy-time buyback activation).
    //
    // OFF by default: the base deploy always constructs `FeeRouter` dormant
    // (`buybackBurner == address(0)`, shares `[9000, 0, 1000]`) and this whole
    // path is skipped, so an un-activated genesis is byte-for-byte the pre-flag
    // deploy. When ON, `_activateBuyback` runs the same coherent bundle a
    // post-deploy governance activation would (ADR 018 § Activation Criteria) —
    // seed the venue pool, deploy the concrete burner, flip the FeeRouter split
    // to the steady-state `[6000, 3000, 1000]`, wire the keeper — but in-script,
    // while the deployer still holds `GOVERNANCE_ROLE`, because at genesis the
    // served-bytes voting weight that gates that governance call is zero (ADR
    // 036) and no privileged role survives the handoff to bootstrap it.
    //
    // This is a testnet / genesis convenience. Mainnet leaves the flag OFF and
    // activates through the ADR 018 governance-gated path once the pool is
    // seeded, a private-RPC keeper is live, and the per-epoch cap is calibrated.

    /// @dev Largest tick Uniswap V3 supports; the seed position spans the full
    ///      range clamped to the fee tier's tick spacing (constant-product depth).
    int24 internal constant UNIV3_MAX_TICK = 887_272;

    /// @dev Uniswap V3 valid `sqrtPriceX96` bounds (`TickMath.MIN/MAX_SQRT_RATIO`).
    ///      A seed price outside these makes the pool `initialize` revert with an
    ///      opaque error deep inside the NonfungiblePositionManager, so the seed
    ///      helper fail-fasts against them instead.
    uint160 internal constant UNIV3_MIN_SQRT_RATIO = 4_295_128_739;
    uint160 internal constant UNIV3_MAX_SQRT_RATIO = 1_461_446_703_485_210_103_287_273_052_203_988_822_378_723_970_342;

    /// @dev Balancer 80/20 TOKEN/USDC normalized weights (WAD). Single source of
    ///      truth for both the pool creation and the price-implied TOKEN seed sizing.
    uint256 internal constant BAL_TOKEN_WEIGHT = 0.8e18;
    uint256 internal constant BAL_USDC_WEIGHT = 0.2e18;

    /// @notice Pool seed amounts (raw). Both venues create + seed the pool
    ///         in-script; the venue-appropriate ratio differs (the 80/20 Balancer
    ///         pool needs a different TOKEN:USDC ratio than the constant-product
    ///         Uniswap pool to hit the same anchor price), so `_deriveTokenSeed`
    ///         computes `tokenSeed` per venue.
    struct PoolSeed {
        uint256 usdcSeed; // USDC seed (raw, 6-dec)
        uint256 tokenSeed; // TOKEN seed (raw, 18-dec)
    }

    /// @notice Uniswap-venue inputs — the pool is created + seeded in-script.
    ///         Must be entirely zero when `venue != UNISWAP` (see
    ///         `_assertVenueFieldsScoped`).
    struct UniswapVenueParams {
        address swapRouter; // SwapRouter02 (swap + token-pull target)
        address positionManager; // NonfungiblePositionManager (create + seed)
        uint24 poolFee; // fee tier (e.g. 10000 = 1%)
    }

    /// @notice Balancer-venue inputs — the 80/20 pool is created + seeded
    ///         in-script. Must be entirely zero when `venue != BALANCER` (see
    ///         `_assertVenueFieldsScoped`).
    struct BalancerVenueParams {
        address factory; // WeightedPoolFactory (create the 80/20 pool)
        address router; // Balancer V3 Router (seed via Permit2 + swap target)
        address vault; // Balancer V3 Vault (reads + approvals)
        address permit2; // canonical Permit2 the V3 Router pulls through
        uint256 swapFee; // pool swap fee (WAD; 1e16 = 1%)
        uint256 subSwapCount; // keeper TWAP sub-swap count (immutable on the burner)
        uint256 subSwapMinBlockGap; // keeper TWAP sub-swap block gap
    }

    /// @notice Genesis buyback-activation inputs. `activate == false` reproduces
    ///         the dormant launch exactly; all other fields are then ignored.
    ///
    /// @dev    The two venues' inputs live in their own sub-structs rather than
    ///         flattened side by side (issue #1090): a flat struct made
    ///         `balVault`-while-`venue == UNISWAP` a silently-ignored field.
    ///         Solidity has no tagged union, so grouping alone cannot make that
    ///         unrepresentable — `_assertVenueFieldsScoped` supplies the
    ///         enforcement by rejecting any non-zero field on the unselected
    ///         venue's sub-struct.
    struct BuybackActivation {
        bool activate;
        BuybackVenueLib.Venue venue;
        // Keeper granted KEEPER_ROLE in-script (required when `activate`).
        address keeper;
        // Shared MEV-stack guard band (ADR 018 § Parameter Table).
        BuybackVenueLib.GuardParams guard;
        PoolSeed seed;
        UniswapVenueParams uni;
        BalancerVenueParams bal;
    }

    // PaymentChannel launch params (ADR 003 § Initial deployment values). All
    // governance-tunable post-deploy within the contract's safety bounds.
    uint256 internal constant PAYMENT_DISPUTE_WINDOW = 48 hours;
    uint256 internal constant PAYMENT_MAX_CHANNEL_DURATION = 90 days;
    uint256 internal constant PAYMENT_DELIVERY_FLOOR = 1;

    // SlashJudge launch params (ADR 014 § Governable Parameters). `maxEvidenceAge`
    // (5 days) must stay `< unbondingPeriod` (14 days default) — the SlashJudge
    // constructor enforces it.
    uint256 internal constant SLASH_CHALLENGE_BOND = 100e18;
    uint256 internal constant SLASH_MAX_EVIDENCE_AGE_US = 5 days * 1_000_000;

    struct DeployConfig {
        // External dependencies
        IERC20 usdc;
        IEd25519Verifier ed25519Verifier;
        // Roles + recipients. The FeeRouter treasury bucket is not a config
        // field: it is always the TimelockController deployed by this script —
        // the FeeRouter treasury destination (ADR 016 § Deployment Order step 7),
        // which is Timelock-custodied (step 3 / § Contracts Holding Funds).
        address deployer;
        address emergencyMultisig;
        address initialTokenHolder;
        // Governance / Timelock
        uint256 timelockDelay;
        // ADR 009 § Bootstrap-multisig phase. `address(0)` (the default) seats
        // `DecdnGovernor` as the Timelock's proposer, so served-bytes-weighted DAO
        // voting is live from block one. A non-zero address instead seats THIS
        // multisig as the sole proposer and leaves the Governor without the role, so
        // only the multisig can schedule while the operator set is thin. Either way
        // the Timelock holds GOVERNANCE_ROLE and imposes its 48-hour delay. Opt-in
        // because the initial testnet network runs a single controlled EOA and has
        // no multisig to seat (issue #1175).
        address bootstrapMultisig;
        // CapacityBond economic params (ADR 026 defaults)
        uint256 minBond;
        uint256 unbondingPeriod;
        uint256 multiaddrUpdateCooldown;
        uint256 maxMultiaddrSize;
        uint256 regionStabilityWindow;
        // ADR 019 § Terms Acceptance — genesis operator-terms hash
        // (keccak256 of the shipped TERMS.md). Governance-swappable post-deploy
        // via CapacityBond.setCurrentTermsHash.
        bytes32 currentTermsHash;
        // FeeRouter params (ADR 016 / ADR 026). `buybackBurner` may be
        // `address(0)` iff `feeRouterShares[1] == 0` (launch-mode dormancy
        // per ADR 016 § Tunable Economics). The cross-validation is enforced
        // by `FeeRouter._setShares`, not re-asserted here. Buckets are
        // operator / buyback / treasury (3-bucket split, ADR 026 § FeeRouter).
        uint64 feeRouterEpochLength;
        uint64 feeRouterWindowEpochs;
        uint256[3] feeRouterShares;
        address buybackBurner;
        // Appeal-bond param (ADR 028)
        uint256 slashAppealBond;
    }

    struct Deployment {
        Token token;
        CapacityBond bond;
        SlashAppeal slashAppeal;
        FeeRouter router;
        ContentBlacklist blacklist;
        PublisherRegistry registry;
        TimelockController timelock;
        DecdnGovernor governor;
        PaymentChannel paymentChannel;
        SlashJudge slashJudge;
        OriginAssignment originAssignment;
        // Zero unless genesis buyback activation ran (`BuybackActivation.activate`);
        // then it is the concrete `GuardedBuybackBurner` wired into `FeeRouter`.
        GuardedBuybackBurner buybackBurner;
    }

    error ZeroAddress(string field);
    /// @notice Post-deploy invariant — deployer still holds a privileged role
    ///         on `target`. Without this guard, the script could finish with
    ///         the deployer EOA retaining god-mode on production contracts,
    ///         which is exactly the failure mode #694 closes.
    error DeployerStillHoldsRole(address target, bytes32 role);
    /// @notice Post-deploy invariant — the Timelock does NOT hold `role` on
    ///         `target` (or the Timelock does not self-administer). Catches the
    ///         mirror failure of `DeployerStillHoldsRole`: a handoff that revoked
    ///         the deployer but never granted the Timelock would strand the
    ///         contract ungoverned. Asserting both directions makes the in-script
    ///         guard symmetric with the test role matrix.
    error GovernanceNotHandedOff(address target, bytes32 role);
    /// @notice Post-deploy invariant — the Timelock's `PROPOSER_ROLE` is not seated
    ///         as this deploy mode requires. `shouldHold` distinguishes the two
    ///         failures: `true` means `account` was meant to be able to propose and
    ///         cannot; `false` means it can propose and must not — the shape that
    ///         matters, since a bootstrap deploy that left `DecdnGovernor` a proposer
    ///         puts DAO execution one 48-hour delay away while the operator set is
    ///         still too thin for capacity-weighted voting to be safe (ADR 009).
    error ProposerNotSeated(address account, bool shouldHold);
    /// @notice Post-deploy invariant — a phase-4 peer role (e.g. `SLASH_ROLE`,
    ///         `ROUTER_CALLER_ROLE`, `PAUSER_ROLE`) was not actually granted to
    ///         `grantee` on `target`. OZ `grantRole` does not revert when the
    ///         target address is wrong, so a dropped or mis-targeted peer grant is
    ///         otherwise silent — shipping a dead slash trigger, an unroutable
    ///         payment path, or no live pauser.
    error PeerRoleNotWired(address target, bytes32 role, address grantee);
    /// @notice Post-deploy invariant — an address binding (such as
    ///         `CapacityBond.slashJudge` or
    ///         `OriginAssignment.contentBlacklist`) does not match the intended
    ///         deployment target. Catches a setter or constructor binding error
    ///         that would silently leave a security check unwired.
    error BindingNotWired(address target, address expected, address actual);

    /// @notice Genesis buyback activation was requested without a keeper — the
    ///         keeper is mandatory because an activated buyback bucket with no
    ///         `KEEPER_ROLE` holder can never `executeBuyback`, silently queueing
    ///         USDC in the burner forever.
    error MissingBuybackKeeper();
    /// @notice Genesis activation carried a non-zero field on the venue it did NOT
    ///         select — e.g. a `bal.vault` under `venue == UNISWAP`. Under the old
    ///         flat `BuybackActivation` that field was silently dropped, so a
    ///         caller could believe it had wired a Balancer pool and ship a Uniswap
    ///         burner. `venue` is the selected venue; the offending fields are the
    ///         other one's.
    error VenueFieldsCrossWired(BuybackVenueLib.Venue venue);
    /// @notice The venue seed left the pool with no USDC depth — the per-epoch
    ///         cap denominator (`usdc.balanceOf(pool)`) would be zero, so the
    ///         "seed the pool before wiring" guard fails the deploy loudly.
    error PoolNotSeeded(address pool);
    /// @notice The deployer lacks the TOKEN/USDC balance the venue seed pulls.
    error InsufficientSeedBalance(address token, uint256 have, uint256 need);
    /// @notice Fee tier has no known Uniswap V3 tick spacing.
    error UnsupportedFeeTier(uint24 fee);
    /// @notice The derived `sqrtPriceX96` falls outside Uniswap V3's valid
    ///         `[MIN_SQRT_RATIO, MAX_SQRT_RATIO]` range — seed amounts imply a price
    ///         the pool cannot represent. Caught here so the deploy fails with a
    ///         clear error instead of an opaque revert inside pool `initialize`.
    error SqrtPriceOutOfRange(uint256 value);
    /// @notice The genesis-activation target price was zero — the TOKEN seed is
    ///         derived as `usdcSeed / targetPrice`, so a zero divides by zero.
    error TargetPriceZero();
    /// @notice Post-activation invariant — the FeeRouter split is not the expected
    ///         steady-state `[6000, 3000, 1000]` after `setSharesAndDestinations`.
    error BuybackSharesNotActivated(uint256 operator, uint256 buyback, uint256 treasury);

    function _runFullDeploy(DeployConfig memory cfg) internal returns (Deployment memory d) {
        return _runFullDeploy(cfg, _noBuybackActivation());
    }

    /// @dev The genesis-activation-aware pipeline. `_runFullDeploy(cfg)` delegates
    ///      here with activation off, so every existing caller keeps the dormant
    ///      launch unchanged. When `act.activate`, the buyback bundle runs after
    ///      cross-contract wiring (deployer still holds `GOVERNANCE_ROLE`) and
    ///      before the handoff, so the burner is handed to the Timelock alongside
    ///      every other target.
    function _runFullDeploy(DeployConfig memory cfg, BuybackActivation memory act)
        internal
        returns (Deployment memory d)
    {
        TimelockController timelock = _deployTimelock(cfg);
        d = _deployTargets(cfg, timelock);
        _deployGovernor(cfg, d);
        _wireCrossContractRoles(cfg, d);
        if (act.activate) _activateBuyback(cfg, act, d);
        _handOffGovernance(cfg, d);
        _assertNoBackDoors(cfg, d);
        _assertPeerRolesWired(cfg, d);
        if (act.activate) _assertBuybackActivated(cfg, act, d);
    }

    /// @dev The all-off activation the legacy single-arg entry point uses.
    function _noBuybackActivation() internal pure returns (BuybackActivation memory act) {
        // Every field defaults to zero; `activate == false` short-circuits the path.
        return act;
    }

    // Phase 1 — TimelockController. Deployed before the targets so its address
    // is available to FeeRouter as the treasury bucket destination (ADR 016
    // § Deployment Order step 3). Deployer is the initial admin; `proposers` is
    // empty (PROPOSER_ROLE granted to the Governor in phase 3) and `executors`
    // is `[address(0)]` (anyone may execute after the delay).
    function _deployTimelock(DeployConfig memory cfg) internal returns (TimelockController) {
        if (cfg.deployer == address(0)) revert ZeroAddress("deployer");
        address[] memory emptyProposers = new address[](0);
        address[] memory openExecutor = new address[](1);
        openExecutor[0] = address(0);
        return new TimelockController(cfg.timelockDelay, emptyProposers, openExecutor, cfg.deployer);
    }

    // Phase 2 — deploy targets with deployer as admin. `timelock` is the
    // FeeRouter treasury bucket destination (Timelock-custodied per ADR 016).
    function _deployTargets(DeployConfig memory cfg, TimelockController timelock)
        internal
        returns (Deployment memory d)
    {
        d.timelock = timelock;

        // Fail-fast on the five fields whose absence either reverts a
        // constructor with an opaque error (`usdc`, `ed25519Verifier`,
        // `initialTokenHolder`) or silently no-ops a role grant downstream
        // (`emergencyMultisig` skips SlashAppeal's constructor grant; on
        // ContentBlacklist it would grant EMERGENCY_MULTISIG_ROLE to
        // `address(0)`).
        // The treasury is not validated here: it is `address(timelock)`, always
        // non-zero.
        if (address(cfg.usdc) == address(0)) revert ZeroAddress("usdc");
        if (address(cfg.ed25519Verifier) == address(0)) revert ZeroAddress("ed25519Verifier");
        if (cfg.deployer == address(0)) revert ZeroAddress("deployer");
        if (cfg.emergencyMultisig == address(0)) revert ZeroAddress("emergencyMultisig");
        if (cfg.initialTokenHolder == address(0)) revert ZeroAddress("initialTokenHolder");

        d.token = new Token(cfg.initialTokenHolder);

        d.bond = new CapacityBond({
            token_: d.token,
            ed25519Verifier_: cfg.ed25519Verifier,
            admin: cfg.deployer,
            minBond_: cfg.minBond,
            unbondingPeriod_: cfg.unbondingPeriod,
            multiaddrUpdateCooldown_: cfg.multiaddrUpdateCooldown,
            maxMultiaddrSize_: cfg.maxMultiaddrSize,
            regionStabilityWindow_: cfg.regionStabilityWindow,
            currentTermsHash_: cfg.currentTermsHash
        });

        d.slashAppeal = new SlashAppeal({
            token_: d.token,
            capacityBond_: ICapacityBond(address(d.bond)),
            admin: cfg.deployer,
            emergencyMultisig: cfg.emergencyMultisig,
            appealBond_: cfg.slashAppealBond
        });

        d.router = new FeeRouter({
            usdc_: cfg.usdc,
            capacityBond_: ICapacityBondEpoch(address(d.bond)),
            treasury_: address(timelock),
            epochLength_: cfg.feeRouterEpochLength,
            windowEpochs_: cfg.feeRouterWindowEpochs,
            admin: cfg.deployer,
            initialShares: cfg.feeRouterShares,
            buybackBurner_: cfg.buybackBurner
        });

        // PublisherRegistry — consumed by `OriginAssignment` below, which binds
        // it as a constructor immutable. Needs only `admin`.
        d.registry = new PublisherRegistry({ admin: cfg.deployer });

        // `ContentBlacklist` takes no token: it custodies no funds (ADR 016
        // § Contract Inventory) now that the appeal-bond escrow is gone.
        d.blacklist =
            new ContentBlacklist({ capacityBond_: ICapacityBondEjector(address(d.bond)), admin: cfg.deployer });

        // PaymentChannel (ADR 003): USDC settlement gateway. `feeRouter` must be
        // a deployed contract (constructor checks code size) — `d.router` above.
        d.paymentChannel = new PaymentChannel({
            usdc_: cfg.usdc,
            capacityBond_: ICapacityBondActivity(address(d.bond)),
            feeRouter_: address(d.router),
            disputeWindow_: PAYMENT_DISPUTE_WINDOW,
            maxChannelDuration_: PAYMENT_MAX_CHANNEL_DURATION,
            deliveryFloor_: PAYMENT_DELIVERY_FLOOR,
            admin: cfg.deployer
        });

        // SlashJudge (ADR 014): evidence verification + challenge bonds. The
        // constructor enforces `maxEvidenceAge < CapacityBond.unbondingPeriod`.
        d.slashJudge = new SlashJudge({
            capacityBond_: ICapacityBondSlasher(address(d.bond)),
            token_: IERC20(address(d.token)),
            contentBlacklist_: IContentBlacklistHashView(address(d.blacklist)),
            challengeBond_: SLASH_CHALLENGE_BOND,
            maxEvidenceAgeUs_: SLASH_MAX_EVIDENCE_AGE_US,
            admin: cfg.deployer
        });

        // OriginAssignment (ADR 011): deployed with a zero ContentBlacklist binding;
        // `_wireCrossContractRoles` calls `setContentBlacklist` post-deploy (ADR 016
        // § Post-Deployment Initialization step 2).
        d.originAssignment = new OriginAssignment({
            capacityBond_: ICapacityBondActivity(address(d.bond)),
            publisherRegistry_: IPublisherRegistryOwnership(address(d.registry)),
            contentBlacklist_: address(0),
            admin: cfg.deployer
        });
    }

    // Phase 3 — Governor; Timelock proposer/canceller wiring. The Timelock
    // itself is already deployed (phase 1) so its address could seed FeeRouter's
    // treasury bucket.
    //
    // WHO may schedule through the Timelock is the whole of the ADR 009 phase
    // distinction (issue #1175). The Timelock holds GOVERNANCE_ROLE on every target
    // in both modes — so every parameter change carries the standard 48-hour delay
    // either way — and what differs is who can propose one:
    //
    //   - default (`bootstrapMultisig == 0`) — the Governor, so served-bytes-weighted
    //     DAO voting is live from block one.
    //   - bootstrap — the bootstrap multisig, and the Governor gets NOTHING. It is
    //     deployed and its vote-weight sources are wired (ADR 009 § Transition: wired
    //     at deployment, not at transition), but with no PROPOSER_ROLE no DAO proposal
    //     can reach execution while the operator set is too thin for capacity-weighted
    //     voting to be safe.
    function _deployGovernor(DeployConfig memory cfg, Deployment memory d) internal {
        d.governor = new DecdnGovernor(d.router, d.bond, d.timelock);

        address proposer = _initialProposer(cfg, d);
        d.timelock.grantRole(d.timelock.PROPOSER_ROLE(), proposer);
        d.timelock.grantRole(d.timelock.CANCELLER_ROLE(), proposer);
    }

    /// @dev The address seated as the Timelock's sole proposer/canceller at deploy.
    ///      Single source of truth for `_deployGovernor` and `_assertNoBackDoors`
    ///      so the wiring and the invariant cannot disagree about the mode.
    function _initialProposer(DeployConfig memory cfg, Deployment memory d) internal pure returns (address) {
        return cfg.bootstrapMultisig == address(0) ? address(d.governor) : cfg.bootstrapMultisig;
    }

    // Phase 4 — cross-contract peer roles + deployer-only state setters.
    //
    // These calls all require the deployer to still hold `GOVERNANCE_ROLE` or
    // `DEFAULT_ADMIN_ROLE` on the target. They are the last opportunity to
    // configure mutable state before the handoff puts every setter behind the
    // 48h Timelock.
    function _wireCrossContractRoles(DeployConfig memory cfg, Deployment memory d) internal {
        // SlashAppeal drives the escrow-on-slash appeal hooks on CapacityBond
        // (markAppealOpen / settleAppealUpheld / settleAppealGranted) — ADR 028.
        d.bond.grantRole(d.bond.SLASH_APPEAL_ROLE(), address(d.slashAppeal));
        // ContentBlacklist ejects operators via CapacityBond on blacklist add.
        d.bond.grantRole(d.bond.BLACKLIST_ROLE(), address(d.blacklist));

        // EMERGENCY_MULTISIG_ROLE: SlashAppeal already has it from its
        // constructor (when emergencyMultisig != 0); ContentBlacklist has no
        // such constructor path, so grant explicitly.
        d.blacklist.grantRole(d.blacklist.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);

        // PAUSER_ROLE → the emergency multisig on every Pausable target (ADR 016
        // § Role Inventory: `EMERGENCY_ROLE` holds `pause()` on fund-holding /
        // Pausable contracts, a 3-of-5 multisig). No constructor grants
        // PAUSER_ROLE, so without this wiring the system comes up with no live
        // pauser: post-handoff the only PAUSER_ROLE admin is the Timelock, so the
        // first emergency pause would need a 48h-delayed governance proposal —
        // defeating the emergency path. Granted here, before the handoff, so the
        // multisig can pause from block one. (BuybackBurner is also Pausable but
        // is not deployed by this script — see the contract header.)
        d.bond.grantRole(d.bond.PAUSER_ROLE(), cfg.emergencyMultisig);
        d.router.grantRole(d.router.PAUSER_ROLE(), cfg.emergencyMultisig);
        d.slashAppeal.grantRole(d.slashAppeal.PAUSER_ROLE(), cfg.emergencyMultisig);
        d.paymentChannel.grantRole(d.paymentChannel.PAUSER_ROLE(), cfg.emergencyMultisig);
        d.slashJudge.grantRole(d.slashJudge.PAUSER_ROLE(), cfg.emergencyMultisig);

        // PaymentChannel.settleChannel / withdraw call FeeRouter.routeSettlement
        // (ADR 016 § Post-Deployment Init step 4) — without this the settlement
        // path reverts.
        d.router.grantRole(d.router.ROUTER_CALLER_ROLE(), address(d.paymentChannel));
        // SlashJudge is the sole holder of SLASH_ROLE on CapacityBond (step 3) —
        // the only on-chain slash trigger.
        d.bond.grantRole(d.bond.SLASH_ROLE(), address(d.slashJudge));
        // Wire the SlashJudge into CapacityBond so `setUnbondingPeriod` enforces the
        // paired `maxEvidenceAgeUs < unbondingPeriod * 1e6` invariant (ADR 014 § Interaction
        // with unbonding period). Must run before `_handOffGovernance` revokes
        // GOVERNANCE_ROLE from the deployer. SlashJudge's constructor already enforced
        // the other half against the bond's current unbondingPeriod, and
        // `setSlashJudge` re-checks it at wire time, so this cannot revert here.
        d.bond.setSlashJudge(ISlashJudgeEvidenceView(address(d.slashJudge)));
        // Wire the OriginAssignment → ContentBlacklist read direction (step 2);
        // deployer still holds GOVERNANCE_ROLE on OriginAssignment here.
        d.originAssignment.setContentBlacklist(address(d.blacklist));

        _postWiringHook(cfg, d);
    }

    /// @dev Subclasses (e.g. GovernanceLifecycleTest) override this to grant
    ///      test-only roles like ROUTER_CALLER_ROLE before the handoff puts
    ///      the role-grant surface behind the Timelock. No-op in production.
    function _postWiringHook(DeployConfig memory cfg, Deployment memory d) internal virtual { }

    // Phase 5 — atomic role handoff to Timelock.
    //
    // Identical in both ADR 009 phases: the Timelock is always the GOVERNANCE_ROLE
    // and DEFAULT_ADMIN_ROLE holder, so every parameter change carries the standard
    // 48-hour delay whoever proposes it. The bootstrap phase differs only in who may
    // schedule through the Timelock, which `_deployGovernor` decides.
    //
    // Grant-before-revoke ordering is mandatory: revoking GOVERNANCE_ROLE or
    // DEFAULT_ADMIN_ROLE from the deployer before granting it to the Timelock
    // strands the contract (no holder of either role) and locks out every
    // governance setter until a recovery deploy.
    function _handOffGovernance(DeployConfig memory cfg, Deployment memory d) internal {
        address[] memory targets = _allGovernedTargets(d);
        address tl = address(d.timelock);
        // The genesis-activated burner (if any) is inside `targets`: it is deployed
        // with the deployer as admin so `_activateBuyback` can `setKeeper` in-script,
        // and this loop closes that back door alongside every other target.
        for (uint256 i = 0; i < targets.length; i++) {
            IAccessControl target = IAccessControl(targets[i]);
            target.grantRole(GOVERNANCE_ROLE, tl);
            target.grantRole(DEFAULT_ADMIN_ROLE, tl);
            target.revokeRole(GOVERNANCE_ROLE, cfg.deployer);
            target.revokeRole(DEFAULT_ADMIN_ROLE, cfg.deployer);
        }

        // Final step: deployer no longer admins the Timelock itself.
        d.timelock.renounceRole(d.timelock.DEFAULT_ADMIN_ROLE(), cfg.deployer);
    }

    /// @dev Every target whose GOVERNANCE_ROLE + DEFAULT_ADMIN_ROLE the handoff
    ///      moves: the eight fixed governed targets plus the genesis-activated
    ///      burner when one was deployed. Dynamic because the burner is conditional;
    ///      single source of truth for `_handOffGovernance` and `_assertNoBackDoors`
    ///      so a target cannot be handed off by one and missed by the other.
    function _allGovernedTargets(Deployment memory d) internal pure returns (address[] memory) {
        IAccessControl[8] memory fixedTargets = _governedTargets(d);
        bool hasBurner = address(d.buybackBurner) != address(0);
        address[] memory targets = new address[](hasBurner ? 9 : 8);
        for (uint256 i = 0; i < fixedTargets.length; i++) {
            targets[i] = address(fixedTargets[i]);
        }
        if (hasBurner) targets[8] = address(d.buybackBurner);
        return targets;
    }

    // Phase 6 — post-deploy invariant. Symmetric check: the deployer holds
    // neither privileged role on any target (no back door), AND the Timelock
    // holds both on every target plus self-administers (governance is live, not
    // stranded). A handoff that revoked the deployer but skipped a Timelock grant
    // would pass the back-door half yet leave a contract ungoverned.
    function _assertNoBackDoors(DeployConfig memory cfg, Deployment memory d) internal view {
        address[] memory targets = _allGovernedTargets(d);
        address tl = address(d.timelock);

        for (uint256 i = 0; i < targets.length; i++) {
            IAccessControl target = IAccessControl(targets[i]);
            if (target.hasRole(GOVERNANCE_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(targets[i], GOVERNANCE_ROLE);
            }
            if (target.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(targets[i], DEFAULT_ADMIN_ROLE);
            }
            if (!target.hasRole(GOVERNANCE_ROLE, tl)) {
                revert GovernanceNotHandedOff(targets[i], GOVERNANCE_ROLE);
            }
            if (!target.hasRole(DEFAULT_ADMIN_ROLE, tl)) {
                revert GovernanceNotHandedOff(targets[i], DEFAULT_ADMIN_ROLE);
            }
        }

        // Timelock PROPOSER_ROLE seating (ADR 009, issue #1175). The role checks
        // above are mode-independent — they pass identically whether the Governor or
        // the bootstrap multisig can propose — so nothing else in this invariant can
        // catch a bootstrap deploy that left the Governor a proposer, which would put
        // DAO proposals one 48-hour delay from execution against the thin operator set
        // the phase exists to protect. Asserted in both directions so the default
        // deploy equally cannot ship with a Governor that can never propose.
        address proposer = _initialProposer(cfg, d);
        bytes32 proposerRole = d.timelock.PROPOSER_ROLE();
        if (!d.timelock.hasRole(proposerRole, proposer)) revert ProposerNotSeated(proposer, true);
        if (cfg.bootstrapMultisig != address(0) && d.timelock.hasRole(proposerRole, address(d.governor))) {
            revert ProposerNotSeated(address(d.governor), false);
        }

        if (d.timelock.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
            revert DeployerStillHoldsRole(tl, DEFAULT_ADMIN_ROLE);
        }
        // The Timelock must self-administer, or its own role surface is stranded.
        // True in both modes: the bootstrap phase withholds GOVERNANCE_ROLE on the
        // targets from the Timelock, not control of the Timelock itself.
        if (!d.timelock.hasRole(DEFAULT_ADMIN_ROLE, tl)) {
            revert GovernanceNotHandedOff(tl, DEFAULT_ADMIN_ROLE);
        }
    }

    // Phase 7 — post-deploy invariant for the phase-4 peer wiring.
    //
    // `_assertNoBackDoors` covers only the GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE
    // handoff. The peer grants and address bindings wired in `_wireCrossContractRoles`
    // are security-critical (the only slash trigger, the only settlement-routing
    // grant, the live pausers, the blacklist read binding) yet none are reverting:
    // OZ `grantRole` to the wrong address is a silent no-op of the intended
    // property. This phase re-reads each one so a dropped or mis-targeted grant
    // fails the deploy loudly instead of shipping a half-wired protocol.
    function _assertPeerRolesWired(DeployConfig memory cfg, Deployment memory d) internal view {
        // CapacityBond peer roles.
        _requireRole(d.bond, d.bond.SLASH_APPEAL_ROLE(), address(d.slashAppeal));
        _requireRole(d.bond, d.bond.BLACKLIST_ROLE(), address(d.blacklist));
        _requireRole(d.bond, d.bond.SLASH_ROLE(), address(d.slashJudge));
        // FeeRouter settlement-routing grant.
        _requireRole(d.router, d.router.ROUTER_CALLER_ROLE(), address(d.paymentChannel));
        // EMERGENCY_MULTISIG_ROLE on both appeal surfaces.
        _requireRole(d.slashAppeal, d.slashAppeal.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.blacklist, d.blacklist.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);
        // PAUSER_ROLE on every deployed Pausable target (BuybackBurner is not deployed).
        _requireRole(d.bond, d.bond.PAUSER_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.router, d.router.PAUSER_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.slashAppeal, d.slashAppeal.PAUSER_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.paymentChannel, d.paymentChannel.PAUSER_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.slashJudge, d.slashJudge.PAUSER_ROLE(), cfg.emergencyMultisig);

        // Address bindings from deployer-only setters.
        // CapacityBond.slashJudge wires the ADR-014 paired-invariant view used by
        // `setUnbondingPeriod`; an unwired binding leaves the invariant unenforced.
        address boundSlashJudge = address(d.bond.slashJudge());
        if (boundSlashJudge != address(d.slashJudge)) {
            revert BindingNotWired(address(d.bond), address(d.slashJudge), boundSlashJudge);
        }
        address boundBlacklist = d.originAssignment.contentBlacklist();
        if (boundBlacklist != address(d.blacklist)) {
            revert BindingNotWired(address(d.originAssignment), address(d.blacklist), boundBlacklist);
        }
    }

    function _requireRole(IAccessControl target, bytes32 role, address grantee) private view {
        if (!target.hasRole(role, grantee)) revert PeerRoleNotWired(address(target), role, grantee);
    }

    /// @dev The eight GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE-bearing targets handed
    ///      off to the Timelock (router, bond, blacklist, slashAppeal, registry,
    ///      paymentChannel, slashJudge, originAssignment) — single source of truth
    ///      for `_handOffGovernance` and `_assertNoBackDoors` so the governed set
    ///      can't drift between them. The array width MUST equal the number of
    ///      role-bearing `Deployment` members; adding a target requires widening it.
    function _governedTargets(Deployment memory d) internal pure returns (IAccessControl[8] memory) {
        return [
            IAccessControl(address(d.router)),
            d.bond,
            d.blacklist,
            d.slashAppeal,
            d.registry,
            d.paymentChannel,
            d.slashJudge,
            d.originAssignment
        ];
    }

    // -----------------------------------------------------------------
    // Genesis buyback activation (optional — `BuybackActivation.activate`)
    // -----------------------------------------------------------------

    /// @dev Runs the full activation bundle between `_wireCrossContractRoles` and
    ///      `_handOffGovernance`: deploy (and for Uniswap, seed) the venue pool +
    ///      concrete burner (deployer as admin), grant the burner's pauser + keeper,
    ///      then flip the FeeRouter to the steady-state `[6000, 3000, 1000]` split.
    ///      Stores the burner on `d` so the handoff and the post-deploy asserts
    ///      treat it as a governed target.
    function _activateBuyback(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d) internal {
        if (act.keeper == address(0)) revert MissingBuybackKeeper();
        _assertVenueFieldsScoped(act);

        GuardedBuybackBurner burner =
            act.venue == BuybackVenueLib.Venue.UNISWAP ? _activateUniswap(cfg, act, d) : _activateBalancer(cfg, act, d);

        // The burner is Pausable: grant the emergency multisig PAUSER_ROLE while the
        // deployer still admins it, so it ships with a live pauser like every other
        // target (post-handoff this would need a 48h Timelock proposal).
        burner.grantRole(burner.PAUSER_ROLE(), cfg.emergencyMultisig);

        // Wire the single keeper while the deployer holds GOVERNANCE_ROLE on the
        // burner; `setKeeper` is governance-gated, so post-handoff it needs a proposal.
        burner.setKeeper(act.keeper);

        // Flip the FeeRouter split + destinations atomically. The deployer still
        // holds GOVERNANCE_ROLE on the router here (pre-handoff);
        // `setSharesAndDestinations` sets the buyback destination before the non-zero
        // buyback share, satisfying the cross-validation invariant.
        d.router
            .setSharesAndDestinations(
                BuybackVenueLib.steadyShares(),
                FeeRouter.ShareDestinations({ buybackBurner: address(burner), treasury: address(d.timelock) })
            );

        d.buybackBurner = burner;
    }

    /// @dev Reject any non-zero field belonging to the venue `act` did not select.
    ///      `DeployProtocol._readBuybackActivation` only ever populates one branch,
    ///      so this fires exactly on a hand-constructed activation that wired both
    ///      — the failure mode the old flat struct swallowed (issue #1090).
    function _assertVenueFieldsScoped(BuybackActivation memory act) internal pure {
        if (act.venue == BuybackVenueLib.Venue.UNISWAP) {
            BalancerVenueParams memory b = act.bal;
            bool crossWired = b.factory != address(0) || b.router != address(0) || b.vault != address(0)
                || b.permit2 != address(0) || b.swapFee != 0 || b.subSwapCount != 0 || b.subSwapMinBlockGap != 0;
            if (crossWired) revert VenueFieldsCrossWired(act.venue);
        } else {
            UniswapVenueParams memory u = act.uni;
            bool crossWired = u.swapRouter != address(0) || u.positionManager != address(0) || u.poolFee != 0;
            if (crossWired) revert VenueFieldsCrossWired(act.venue);
        }
    }

    /// @dev Uniswap venue: create + seed the TOKEN/USDC V3 pool in-script, then
    ///      deploy the concrete burner bound to it and `SwapRouter02`.
    function _activateUniswap(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d)
        internal
        returns (GuardedBuybackBurner)
    {
        address pool = _createAndSeedUniswapPool(cfg, act, d);
        // "Seed before wire" guard: a live pool must custody USDC, or the burner's
        // per-epoch cap denominator (`usdc.balanceOf(pool)`) is zero.
        if (cfg.usdc.balanceOf(pool) == 0) revert PoolNotSeeded(pool);

        return BuybackVenueLib.deployUniswapBurner(
            cfg.usdc, ERC20Burnable(address(d.token)), cfg.deployer, act.uni.swapRouter, pool, act.guard
        );
    }

    /// @dev Balancer venue: create + seed the 80/20 TOKEN/USDC weighted pool
    ///      in-script via the `WeightedPoolFactory` + Router (Permit2), then deploy
    ///      the concrete burner bound to it, the Vault, and the Router. The burner
    ///      constructor fail-fasts if the pool is not a live, registered USDC/TOKEN
    ///      pair (the seed above makes it one), which is the "seed before wire"
    ///      guard for this venue — the Vault custodies reserves, so there is no
    ///      pool-held USDC balance to check as there is for Uniswap.
    function _activateBalancer(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d)
        internal
        returns (GuardedBuybackBurner)
    {
        address pool = _createAndSeedBalancerPool(cfg, act, d);
        // The Router minted the pool BPT (protocol-owned liquidity) to the
        // broadcasting deployer; hand it to the Timelock and clear the Permit2
        // approvals so the deployer keeps no custody — mirroring the Uniswap path.
        _finalizeBalancerSeed(
            cfg.usdc, IERC20(address(d.token)), pool, act.bal.permit2, cfg.deployer, address(d.timelock)
        );
        return BuybackVenueLib.deployBalancerBurner(
            cfg.usdc,
            ERC20Burnable(address(d.token)),
            cfg.deployer,
            BuybackVenueLib.BalancerWiring({
                swapRouter: act.bal.router,
                pool: pool,
                vault: act.bal.vault,
                permit2: act.bal.permit2,
                subSwapCount: act.bal.subSwapCount,
                subSwapMinBlockGap: act.bal.subSwapMinBlockGap
            }),
            act.guard
        );
    }

    /// @dev Create the 80/20 TOKEN/USDC weighted pool through the live
    ///      `WeightedPoolFactory` and seed it through the Router (Permit2). Tokens
    ///      are sorted ascending (Vault `registerPool` invariant); the weights and
    ///      seed amounts track the sorted order so TOKEN keeps 80% and USDC 20%.
    ///      Pulls both legs from the deployer, so the deployer MUST hold the seed
    ///      TOKEN + USDC (set `INITIAL_TOKEN_HOLDER` to the deployer, or fund it).
    function _createAndSeedBalancerPool(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d)
        internal
        returns (address pool)
    {
        IERC20 usdc = cfg.usdc;
        IERC20 token = IERC20(address(d.token));
        _requireSeedBalance(usdc, cfg.deployer, act.seed.usdcSeed);
        _requireSeedBalance(token, cfg.deployer, act.seed.tokenSeed);

        bool usdcFirst = address(usdc) < address(token);
        // Split the declaration from the allocation: `forge fmt` treats
        // `new T[](n)` as atomic and won't wrap the combined line, which trips
        // solhint's 120-char rule for this long factory type.
        IBalancerV3WeightedPoolFactory.TokenConfig[] memory tokens;
        tokens = new IBalancerV3WeightedPoolFactory.TokenConfig[](2);
        uint256[] memory weights = new uint256[](2);
        {
            IBalancerV3WeightedPoolFactory.TokenConfig memory usdcCfg = IBalancerV3WeightedPoolFactory.TokenConfig({
                token: usdc,
                tokenType: IBalancerV3WeightedPoolFactory.TokenType.STANDARD,
                rateProvider: address(0),
                paysYieldFees: false
            });
            IBalancerV3WeightedPoolFactory.TokenConfig memory tokenCfg = IBalancerV3WeightedPoolFactory.TokenConfig({
                token: token,
                tokenType: IBalancerV3WeightedPoolFactory.TokenType.STANDARD,
                rateProvider: address(0),
                paysYieldFees: false
            });
            tokens[0] = usdcFirst ? usdcCfg : tokenCfg;
            tokens[1] = usdcFirst ? tokenCfg : usdcCfg;
            weights[0] = usdcFirst ? BAL_USDC_WEIGHT : BAL_TOKEN_WEIGHT;
            weights[1] = usdcFirst ? BAL_TOKEN_WEIGHT : BAL_USDC_WEIGHT;
        }

        IBalancerV3WeightedPoolFactory.PoolRoleAccounts memory roles = IBalancerV3WeightedPoolFactory.PoolRoleAccounts({
            pauseManager: address(0), swapFeeManager: address(0), poolCreator: address(0)
        });

        pool = IBalancerV3WeightedPoolFactory(act.bal.factory)
            .create(
                "deCDN 80TOKEN-20USDC",
                "dcdn-8020",
                tokens,
                weights,
                roles,
                act.bal.swapFee,
                address(0), // no hooks
                false, // enableDonation
                false, // disableUnbalancedLiquidity
                keccak256(abi.encodePacked(address(token), address(usdc))) // salt (unique per fresh TOKEN)
            );

        IERC20[] memory initTokens = new IERC20[](2);
        uint256[] memory initAmounts = new uint256[](2);
        initTokens[0] = tokens[0].token;
        initTokens[1] = tokens[1].token;
        initAmounts[0] = usdcFirst ? act.seed.usdcSeed : act.seed.tokenSeed;
        initAmounts[1] = usdcFirst ? act.seed.tokenSeed : act.seed.usdcSeed;

        _permit2Approve(usdc, act, act.seed.usdcSeed);
        _permit2Approve(token, act, act.seed.tokenSeed);

        // slither-disable-next-line unused-return
        IBalancerV3RouterInit(act.bal.router).initialize(pool, initTokens, initAmounts, 0, false, "");
    }

    /// @dev Post-seed cleanup for the Balancer venue: move the freshly-minted
    ///      pool BPT (protocol-owned liquidity) to the Timelock and drop the
    ///      standing Permit2 ERC20 allowances, so the deployer retains no
    ///      custody or approval. Extracted to keep `_createAndSeedBalancerPool`
    ///      under the stack-depth limit.
    function _finalizeBalancerSeed(
        IERC20 usdc,
        IERC20 token,
        address pool,
        address permit2,
        address deployer,
        address timelock
    ) internal {
        IERC20(pool).safeTransfer(timelock, IERC20(pool).balanceOf(deployer));
        usdc.forceApprove(permit2, 0);
        token.forceApprove(permit2, 0);
    }

    /// @dev The two-step Permit2 grant the V3 Router requires to pull `amount` of
    ///      `erc20` from the deployer: ERC20-approve Permit2, then set the Permit2
    ///      allowance for the Router.
    function _permit2Approve(IERC20 erc20, BuybackActivation memory act, uint256 amount) internal {
        erc20.forceApprove(act.bal.permit2, type(uint256).max);
        IPermit2(act.bal.permit2)
            .approve(address(erc20), act.bal.router, uint160(amount), uint48(block.timestamp + 1 days));
    }

    /// @dev Create the TOKEN/USDC Uniswap V3 pool (idempotent) at the seed-implied
    ///      price and seed a single full-range position, custodied by the Timelock
    ///      (POL). Pulls both legs from the deployer, so the deployer MUST hold the
    ///      seed TOKEN + USDC (set `INITIAL_TOKEN_HOLDER` to the deployer, or fund it).
    function _createAndSeedUniswapPool(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d)
        internal
        returns (address pool)
    {
        IERC20 usdc = cfg.usdc;
        IERC20 token = IERC20(address(d.token));

        // Sort the legs; align amounts with the sorted order. `sqrtPriceX96`
        // encodes token1-per-token0 in raw units, matching the seed ratio, so a
        // full-range position deploys both legs without a residual.
        (address token0, address token1, uint256 amount0, uint256 amount1) = address(usdc) < address(token)
            ? (address(usdc), address(token), act.seed.usdcSeed, act.seed.tokenSeed)
            : (address(token), address(usdc), act.seed.tokenSeed, act.seed.usdcSeed);

        _requireSeedBalance(usdc, cfg.deployer, act.seed.usdcSeed);
        _requireSeedBalance(token, cfg.deployer, act.seed.tokenSeed);

        uint160 sqrtPriceX96 = _sqrtPriceX96(amount1, amount0);

        INonfungiblePositionManager npm = INonfungiblePositionManager(act.uni.positionManager);
        pool = npm.createAndInitializePoolIfNecessary(token0, token1, act.uni.poolFee, sqrtPriceX96);

        // Scoped approvals for the seed pull (the manager pulls via a standard ERC20
        // allowance); `forceApprove` handles non-standard return-less USDC.
        IERC20(token0).forceApprove(address(npm), amount0);
        IERC20(token1).forceApprove(address(npm), amount1);

        int24 tickUpper = _fullRangeTick(act.uni.poolFee);
        npm.mint(
            INonfungiblePositionManager.MintParams({
                token0: token0,
                token1: token1,
                fee: act.uni.poolFee,
                tickLower: -tickUpper,
                tickUpper: tickUpper,
                amount0Desired: amount0,
                amount1Desired: amount1,
                // Genesis seed into a fresh pool at a price we set — no adversarial
                // slippage to guard, and any unconsumed leg returns to the deployer.
                amount0Min: 0,
                amount1Min: 0,
                recipient: address(d.timelock),
                // Buffer, not `block.timestamp`: under `forge script` the deadline is
                // encoded during the execution phase, but the tx mines a later block
                // (on-chain simulation / broadcast) whose timestamp would already be
                // past a zero-margin deadline — `Transaction too old`. The seed has no
                // slippage to guard (amounts-min are 0), so a wide window is safe.
                deadline: block.timestamp + 1 hours
            })
        );

        // Reset any residual allowance the manager did not consume.
        IERC20(token0).forceApprove(address(npm), 0);
        IERC20(token1).forceApprove(address(npm), 0);
    }

    function _requireSeedBalance(IERC20 t, address holder, uint256 need) internal view {
        uint256 have = t.balanceOf(holder);
        if (have < need) revert InsufficientSeedBalance(address(t), have, need);
    }

    /// @dev `sqrtPriceX96 = sqrt(amount1 / amount0) * 2**96`, computed with
    ///      `mulDiv` (512-bit intermediate) so `amount1 * 2**192` cannot overflow
    ///      before the division. Reverts if the price falls outside Uniswap V3's
    ///      valid `[MIN_SQRT_RATIO, MAX_SQRT_RATIO]` band — a fail-fast in place of
    ///      the opaque revert the pool `initialize` would otherwise throw.
    /// @dev The TOKEN seed that pairs `usdcSeed` at `targetPrice` — the price of one
    ///      whole TOKEN in USDC base units (USDC is 6-dec, so `$0.01/TOKEN` is
    ///      `10_000`). The USDC decimals cancel (`usdcSeed` and `targetPrice` share
    ///      them), so the result is only scaled by TOKEN's 18 decimals:
    ///        tokenSeed = (wTOKEN / wUSDC) · usdcSeed · 1e18 / targetPrice
    ///      where the value-weight ratio is 1:1 for the 50/50 constant-product
    ///      Uniswap pool and 4:1 (80/20) for the Balancer weighted pool. Feeding the
    ///      seeds in this proportion makes the pool initialize at exactly `targetPrice`.
    function _deriveTokenSeed(BuybackVenueLib.Venue venue, uint256 usdcSeed, uint256 targetPrice)
        internal
        pure
        returns (uint256)
    {
        if (targetPrice == 0) revert TargetPriceZero();
        (uint256 wToken, uint256 wUsdc) =
            venue == BuybackVenueLib.Venue.BALANCER ? (BAL_TOKEN_WEIGHT, BAL_USDC_WEIGHT) : (uint256(1), uint256(1));
        return Math.mulDiv(Math.mulDiv(usdcSeed, wToken, wUsdc), 1e18, targetPrice);
    }

    function _sqrtPriceX96(uint256 amount1, uint256 amount0) internal pure returns (uint160) {
        uint256 ratioX192 = Math.mulDiv(amount1, uint256(1) << 192, amount0);
        uint256 s = Math.sqrt(ratioX192);
        if (s < UNIV3_MIN_SQRT_RATIO || s > UNIV3_MAX_SQRT_RATIO) revert SqrtPriceOutOfRange(s);
        return uint160(s);
    }

    /// @dev Full-range upper tick clamped to the fee tier's spacing (lower is the
    ///      negation). Constant-product depth across the whole curve, so a freshly
    ///      seeded pool always quotes a spot for the burner's TWAP.
    function _fullRangeTick(uint24 fee) internal pure returns (int24) {
        int24 spacing = _tickSpacing(fee);
        // Divide-then-multiply is the intent: round the max tick DOWN to the
        // nearest spacing multiple (Uniswap requires ticks land on the spacing).
        // forge-lint: disable-next-line(divide-before-multiply)
        return (UNIV3_MAX_TICK / spacing) * spacing;
    }

    function _tickSpacing(uint24 fee) internal pure returns (int24) {
        if (fee == 100) return 1;
        if (fee == 500) return 10;
        if (fee == 3000) return 60;
        if (fee == 10_000) return 200;
        revert UnsupportedFeeTier(fee);
    }

    /// @dev Post-deploy invariant for the genesis activation: the FeeRouter runs
    ///      the steady-state split against the wired burner, the keeper holds
    ///      KEEPER_ROLE, and the emergency multisig can pause the burner. Mirrors
    ///      `_assertPeerRolesWired` for the conditionally-deployed burner.
    function _assertBuybackActivated(DeployConfig memory cfg, BuybackActivation memory act, Deployment memory d)
        internal
        view
    {
        GuardedBuybackBurner burner = d.buybackBurner;
        if (d.router.buybackBurner() != address(burner)) {
            revert BindingNotWired(address(d.router), address(burner), d.router.buybackBurner());
        }
        uint256[3] memory shares = d.router.getShares();
        uint256[3] memory expected = BuybackVenueLib.steadyShares();
        if (shares[0] != expected[0] || shares[1] != expected[1] || shares[2] != expected[2]) {
            revert BuybackSharesNotActivated(shares[0], shares[1], shares[2]);
        }
        _requireRole(burner, burner.KEEPER_ROLE(), act.keeper);
        _requireRole(burner, burner.PAUSER_ROLE(), cfg.emergencyMultisig);
    }
}
