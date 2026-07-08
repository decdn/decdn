// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
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
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { ISlashJudgeEvidenceView } from "../src/interfaces/ISlashJudgeEvidenceView.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondReporter } from "../src/interfaces/ICapacityBondReporter.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";
import { IContentBlacklistHashView } from "../src/interfaces/IContentBlacklistHashView.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";
import { IPublisherRegistryStanding } from "../src/interfaces/IPublisherRegistryStanding.sol";

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
///           4. `_wireCrossContractRoles` — peer role grants (settlement reporter,
///                                          slash-appeal driver, blacklist ejector,
///                                          emergency multisig, PAUSER_ROLE on every
///                                          Pausable target,
///                                          router-caller → PaymentChannel,
///                                          SLASH_ROLE → SlashJudge) plus the
///                                          deployer-only `setChallengerIncentivePool`
///                                          and `OriginAssignment.setContentBlacklist`
///                                          setters that MUST run before the
///                                          GOVERNANCE_ROLE handoff because they
///                                          become Timelock-gated post-handoff.
///           5. `_handOffGovernance`      — grant-before-revoke loop over every
///                                          target for both GOVERNANCE_ROLE and
///                                          DEFAULT_ADMIN_ROLE, then renounce the
///                                          deployer's admin on the Timelock
///                                          itself. Grant-before-revoke ordering
///                                          is mandatory; reversing it strands
///                                          the contract ungoverned mid-tx.
///           6. `_assertNoBackDoors`      — reverts if deployer still holds
///                                          GOVERNANCE_ROLE or DEFAULT_ADMIN_ROLE
///                                          on any target. Runs in-script (not
///                                          just in tests) so a mainnet deploy
///                                          refuses to finish if any handoff
///                                          step failed silently.
///           7. `_assertPeerRolesWired`   — reverts if any phase-4 peer-role grant
///                                          or address binding (slash trigger,
///                                          router-caller, pausers, emergency
///                                          multisig, blacklist
///                                          binding, challenger pool) did not land.
///                                          `grantRole` to a wrong address does not
///                                          revert, so without this a half-wired
///                                          protocol would ship silently.
///
///         Slash restitution is escrow-on-slash inside CapacityBond itself
///         (ADR 026 § Slashing, ADR 028) — there is no standalone reserve
///         contract. `SlashAppeal` drives the appeal state machine and is granted
///         `SLASH_APPEAL_ROLE` on CapacityBond so it can open/settle appeals.
///
///         BuybackBurner is deliberately NOT deployed here. The contract is
///         abstract pending a concrete Balancer V3 Vault subclass; until that
///         ships, the FeeRouter is constructed with `buybackBurner_=address(0)`
///         and `feeRouterShares[1]=0`, which `FeeRouter._setShares` explicitly
///         permits as launch-mode dormancy. Governance activates the bucket
///         later via `FeeRouter.setSharesAndDestinations` once a concrete
///         subclass is deployed.
abstract contract BaseProtocolDeploy is Script {
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant DEFAULT_ADMIN_ROLE = 0x00;

    // PaymentChannel launch params (ADR 003 § Initial deployment values). All
    // governance-tunable post-deploy within the contract's safety bounds.
    uint256 internal constant PAYMENT_DISPUTE_WINDOW = 48 hours;
    uint256 internal constant PAYMENT_MAX_CHANNEL_DURATION = 90 days;
    uint256 internal constant PAYMENT_DELIVERY_FLOOR = 1;
    uint256 internal constant PAYMENT_DELIVERY_CEILING = 1000;

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
        address challengerIncentivePool;
        // Governance / Timelock
        uint256 timelockDelay;
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
        // Appeal-bond params (ADR 028)
        uint256 slashAppealBond;
        uint256 blacklistAppealBond;
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
    /// @notice Post-deploy invariant — a phase-4 peer role (e.g. `SLASH_ROLE`,
    ///         `ROUTER_CALLER_ROLE`, `PAUSER_ROLE`) was not actually granted to
    ///         `grantee` on `target`. OZ `grantRole` does not revert when the
    ///         target address is wrong, so a dropped or mis-targeted peer grant is
    ///         otherwise silent — shipping a dead slash trigger, an unroutable
    ///         payment path, or no live pauser.
    error PeerRoleNotWired(address target, bytes32 role, address grantee);
    /// @notice Post-deploy invariant — a deployer-only address binding set in
    ///         phase 4 (`OriginAssignment.contentBlacklist`, the slash-appeal
    ///         challenger pool) does not point where wiring intended. Catches a
    ///         skipped setter that would silently leave a security check unwired.
    error BindingNotWired(address target, address expected, address actual);

    function _runFullDeploy(DeployConfig memory cfg) internal returns (Deployment memory d) {
        TimelockController timelock = _deployTimelock(cfg);
        d = _deployTargets(cfg, timelock);
        _deployGovernor(cfg, d);
        _wireCrossContractRoles(cfg, d);
        _handOffGovernance(cfg, d);
        _assertNoBackDoors(cfg, d);
        _assertPeerRolesWired(cfg, d);
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

        // Fail-fast on the six fields whose absence either reverts a
        // constructor with an opaque error (`usdc`, `ed25519Verifier`,
        // `initialTokenHolder`) or silently no-ops a role grant downstream
        // (`emergencyMultisig` skips SlashAppeal's constructor grant;
        // `challengerIncentivePool` would brick `setChallengerIncentivePool`;
        // ContentBlacklist would grant EMERGENCY_MULTISIG_ROLE to `address(0)`).
        // The treasury is not validated here: it is `address(timelock)`, always
        // non-zero.
        if (address(cfg.usdc) == address(0)) revert ZeroAddress("usdc");
        if (address(cfg.ed25519Verifier) == address(0)) revert ZeroAddress("ed25519Verifier");
        if (cfg.deployer == address(0)) revert ZeroAddress("deployer");
        if (cfg.emergencyMultisig == address(0)) revert ZeroAddress("emergencyMultisig");
        if (cfg.initialTokenHolder == address(0)) revert ZeroAddress("initialTokenHolder");
        if (cfg.challengerIncentivePool == address(0)) revert ZeroAddress("challengerIncentivePool");

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
            capacityBond_: ICapacityBondReporter(address(d.bond)),
            treasury_: address(timelock),
            epochLength_: cfg.feeRouterEpochLength,
            windowEpochs_: cfg.feeRouterWindowEpochs,
            admin: cfg.deployer,
            initialShares: cfg.feeRouterShares,
            buybackBurner_: cfg.buybackBurner
        });

        // PublisherRegistry deploys BEFORE ContentBlacklist: the blacklist binds
        // it as a constructor immutable for the ADR 031 Publisher standing check
        // (security-critical, cannot be left unset). The registry needs only
        // `admin`, so the ordering is free.
        d.registry = new PublisherRegistry({ admin: cfg.deployer });

        d.blacklist = new ContentBlacklist({
            capacityBond_: ICapacityBondEjector(address(d.bond)),
            token_: d.token,
            publisherRegistry_: IPublisherRegistryStanding(address(d.registry)),
            admin: cfg.deployer,
            appealBond_: cfg.blacklistAppealBond
        });

        // PaymentChannel (ADR 003): USDC settlement gateway. `feeRouter` must be
        // a deployed contract (constructor checks code size) — `d.router` above.
        d.paymentChannel = new PaymentChannel({
            usdc_: cfg.usdc,
            capacityBond_: ICapacityBondActivity(address(d.bond)),
            feeRouter_: address(d.router),
            disputeWindow_: PAYMENT_DISPUTE_WINDOW,
            maxChannelDuration_: PAYMENT_MAX_CHANNEL_DURATION,
            deliveryFloor_: PAYMENT_DELIVERY_FLOOR,
            deliveryCeiling_: PAYMENT_DELIVERY_CEILING,
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
    function _deployGovernor(DeployConfig memory, Deployment memory d) internal {
        d.governor = new DecdnGovernor(d.router, d.bond, d.timelock);

        d.timelock.grantRole(d.timelock.PROPOSER_ROLE(), address(d.governor));
        d.timelock.grantRole(d.timelock.CANCELLER_ROLE(), address(d.governor));
    }

    // Phase 4 — cross-contract peer roles + deployer-only state setters.
    //
    // These calls all require the deployer to still hold `GOVERNANCE_ROLE` or
    // `DEFAULT_ADMIN_ROLE` on the target. They are the last opportunity to
    // configure mutable state before the handoff puts every setter behind the
    // 48h Timelock.
    function _wireCrossContractRoles(DeployConfig memory cfg, Deployment memory d) internal {
        // FeeRouter writes settlement state on CapacityBond.
        d.bond.grantRole(d.bond.SETTLEMENT_REPORTER_ROLE(), address(d.router));
        // SlashAppeal drives the escrow-on-slash appeal hooks on CapacityBond
        // (markAppealOpen / settleAppealUpheld / settleAppealGranted) — ADR 028.
        d.bond.grantRole(d.bond.SLASH_APPEAL_ROLE(), address(d.slashAppeal));
        // ContentBlacklist ejects operators via CapacityBond on blacklist add.
        d.bond.grantRole(d.bond.BLACKLIST_ROLE(), address(d.blacklist));

        // SlashAppeal.upholdAppeal routes the non-burn half of a failed appeal
        // bond to a challenger-incentive pool when one is wired (otherwise that
        // half is also burned — `upholdAppeal` degrades gracefully, it does NOT
        // revert on an unset pool). The `setChallengerIncentivePool` setter,
        // however, reverts on address(0), and post-handoff it is governance-gated,
        // so wiring here is the only path that doesn't require a Timelock proposal
        // to give the slash-appeal lifecycle a live pool.
        d.slashAppeal.setChallengerIncentivePool(cfg.challengerIncentivePool);

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
    // Grant-before-revoke ordering is mandatory: revoking GOVERNANCE_ROLE or
    // DEFAULT_ADMIN_ROLE from the deployer before granting it to the Timelock
    // strands the contract (no holder of either role) and locks out every
    // governance setter until a recovery deploy.
    function _handOffGovernance(DeployConfig memory cfg, Deployment memory d) internal {
        IAccessControl[8] memory targets = _governedTargets(d);
        address tl = address(d.timelock);
        for (uint256 i = 0; i < targets.length; i++) {
            targets[i].grantRole(GOVERNANCE_ROLE, tl);
            targets[i].grantRole(DEFAULT_ADMIN_ROLE, tl);
            targets[i].revokeRole(GOVERNANCE_ROLE, cfg.deployer);
            targets[i].revokeRole(DEFAULT_ADMIN_ROLE, cfg.deployer);
        }
        // Final step: deployer no longer admins the Timelock itself.
        d.timelock.renounceRole(d.timelock.DEFAULT_ADMIN_ROLE(), cfg.deployer);
    }

    // Phase 6 — post-deploy invariant. Symmetric check: the deployer holds
    // neither privileged role on any target (no back door), AND the Timelock
    // holds both on every target plus self-administers (governance is live, not
    // stranded). A handoff that revoked the deployer but skipped a Timelock grant
    // would pass the back-door half yet leave a contract ungoverned.
    function _assertNoBackDoors(DeployConfig memory cfg, Deployment memory d) internal view {
        IAccessControl[8] memory targets = _governedTargets(d);
        address tl = address(d.timelock);
        for (uint256 i = 0; i < targets.length; i++) {
            address target = address(targets[i]);
            if (targets[i].hasRole(GOVERNANCE_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(target, GOVERNANCE_ROLE);
            }
            if (targets[i].hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(target, DEFAULT_ADMIN_ROLE);
            }
            if (!targets[i].hasRole(GOVERNANCE_ROLE, tl)) {
                revert GovernanceNotHandedOff(target, GOVERNANCE_ROLE);
            }
            if (!targets[i].hasRole(DEFAULT_ADMIN_ROLE, tl)) {
                revert GovernanceNotHandedOff(target, DEFAULT_ADMIN_ROLE);
            }
        }
        if (d.timelock.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
            revert DeployerStillHoldsRole(tl, DEFAULT_ADMIN_ROLE);
        }
        // The Timelock must self-administer, or its own role surface is stranded.
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
        _requireRole(d.bond, d.bond.SETTLEMENT_REPORTER_ROLE(), address(d.router));
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
        // The Publisher standing check is security-critical and the binding is a
        // constructor immutable — verify a constructor-arg mix-up didn't point it
        // at the wrong registry.
        address boundRegistry = address(d.blacklist.publisherRegistry());
        if (boundRegistry != address(d.registry)) {
            revert BindingNotWired(address(d.blacklist), address(d.registry), boundRegistry);
        }
        address boundPool = d.slashAppeal.challengerIncentivePool();
        if (boundPool != cfg.challengerIncentivePool) {
            revert BindingNotWired(address(d.slashAppeal), cfg.challengerIncentivePool, boundPool);
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
}
