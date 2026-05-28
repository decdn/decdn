// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { Token } from "../src/Token.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { SafetyReserve } from "../src/SafetyReserve.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondReporter } from "../src/interfaces/ICapacityBondReporter.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";

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
///           2. `_deployTargets`         — Token, CapacityBond, SafetyReserve,
///                                         FeeRouter (treasury bucket = the
///                                         TimelockController; buyback bucket
///                                         dormant — see BuybackBurner note below),
///                                         ContentBlacklist, PublisherRegistry.
///                                         Deployer is admin of every
///                                         AccessControl-bearing target.
///           3. `_deployGovernor`        — DecdnGovernor; grant Timelock's
///                                         PROPOSER + CANCELLER roles to the
///                                         Governor.
///           4. `_wireCrossContractRoles` — peer role grants (settlement reporter,
///                                          slash-inflow reporter, appeal reversal,
///                                          blacklist, emergency multisig) plus
///                                          deployer-only setters (setSafetyReserve,
///                                          setChallengerIncentivePool) that MUST
///                                          run before the GOVERNANCE_ROLE handoff
///                                          because the same setters become
///                                          Timelock-gated post-handoff.
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
        uint256 minStake;
        uint256 unbondingPeriod;
        uint256 multiaddrUpdateCooldown;
        uint256 maxMultiaddrSize;
        uint256 regionStabilityWindow;
        uint256 genesisCreditWindow;
        // FeeRouter params (ADR 016 / ADR 026). `buybackBurner` may be
        // `address(0)` iff `feeRouterShares[1] == 0` (launch-mode dormancy
        // per ADR 016 § Tunable Economics). The cross-validation is enforced
        // by `FeeRouter._setShares`, not re-asserted here.
        uint64 feeRouterEpochLength;
        uint64 feeRouterWindowEpochs;
        uint256[4] feeRouterShares;
        address buybackBurner;
        // Appeal-bond params (ADR 028)
        uint256 safetyAppealBond;
        uint256 maxAppealRestitution;
        uint256 blacklistAppealBond;
    }

    struct Deployment {
        Token token;
        CapacityBond bond;
        SafetyReserve reserve;
        FeeRouter router;
        ContentBlacklist blacklist;
        PublisherRegistry registry;
        TimelockController timelock;
        DecdnGovernor governor;
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

    function _runFullDeploy(DeployConfig memory cfg) internal returns (Deployment memory d) {
        TimelockController timelock = _deployTimelock(cfg);
        d = _deployTargets(cfg, timelock);
        _deployGovernor(cfg, d);
        _wireCrossContractRoles(cfg, d);
        _handOffGovernance(cfg, d);
        _assertNoBackDoors(cfg, d);
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
        // (`emergencyMultisig` skips SafetyReserve's constructor grant;
        // `challengerIncentivePool` would brick `reverseAppeal`; ContentBlacklist
        // would grant EMERGENCY_MULTISIG_ROLE to `address(0)`). The treasury is
        // not validated here: it is `address(timelock)`, always non-zero.
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
            minStake_: cfg.minStake,
            unbondingPeriod_: cfg.unbondingPeriod,
            multiaddrUpdateCooldown_: cfg.multiaddrUpdateCooldown,
            maxMultiaddrSize_: cfg.maxMultiaddrSize,
            regionStabilityWindow_: cfg.regionStabilityWindow,
            genesisCreditWindow_: cfg.genesisCreditWindow
        });

        d.reserve = new SafetyReserve({
            usdc_: cfg.usdc,
            token_: d.token,
            capacityBond_: ICapacityBond(address(d.bond)),
            admin: cfg.deployer,
            emergencyMultisig: cfg.emergencyMultisig,
            appealBond_: cfg.safetyAppealBond,
            maxAppealRestitution_: cfg.maxAppealRestitution
        });

        d.router = new FeeRouter({
            usdc_: cfg.usdc,
            capacityBond_: ICapacityBondReporter(address(d.bond)),
            treasury_: address(timelock),
            epochLength_: cfg.feeRouterEpochLength,
            windowEpochs_: cfg.feeRouterWindowEpochs,
            admin: cfg.deployer,
            initialShares: cfg.feeRouterShares,
            safetyReserve_: address(d.reserve),
            buybackBurner_: cfg.buybackBurner
        });

        d.blacklist = new ContentBlacklist({
            capacityBond_: ICapacityBondEjector(address(d.bond)),
            token_: d.token,
            admin: cfg.deployer,
            appealBond_: cfg.blacklistAppealBond
        });

        d.registry = new PublisherRegistry({ admin: cfg.deployer });
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
        d.bond.grantRole(d.bond.SETTLEMENT_REPORTER_ROLE(), address(d.router));
        d.reserve.grantRole(d.reserve.SLASH_INFLOW_REPORTER_ROLE(), address(d.bond));
        d.bond.grantRole(d.bond.APPEAL_REVERSAL_ROLE(), address(d.reserve));
        d.bond.grantRole(d.bond.BLACKLIST_ROLE(), address(d.blacklist));

        // CapacityBond.slash() reverts on address(0) safetyReserve. Post-handoff
        // this setter is governance-gated, so wiring here is the only path that
        // doesn't require a Timelock proposal to enable slashing.
        d.bond.setSafetyReserve(ISafetyReserve(address(d.reserve)));

        // SafetyReserve.reverseAppeal() reverts on address(0) challengerIncentivePool.
        d.reserve.setChallengerIncentivePool(cfg.challengerIncentivePool);

        // EMERGENCY_MULTISIG_ROLE: SafetyReserve already has it from its
        // constructor (when emergencyMultisig != 0); ContentBlacklist has no
        // such constructor path, so grant explicitly.
        d.blacklist.grantRole(d.blacklist.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);

        // GENESIS_GRANTOR_ROLE → the Timelock (treasury custodian), which issues
        // Genesis Bond Credits during GENESIS_CREDIT_WINDOW (ADR 016 § Post-
        // Deployment Init step 7). CapacityBond's window clock starts at
        // construction, so granting here — before the handoff — avoids burning
        // ~10 days of a 30-day window on a governance proposal just to enable the
        // grantor. Governance may revoke the role after the window for hygiene.
        d.bond.grantRole(d.bond.GENESIS_GRANTOR_ROLE(), address(d.timelock));

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
        IAccessControl[5] memory targets = _governedTargets(d);
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
        IAccessControl[5] memory targets = _governedTargets(d);
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

    /// @dev The five GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE-bearing targets handed
    ///      off to the Timelock — single source of truth for `_handOffGovernance`
    ///      and `_assertNoBackDoors` so the governed set can't drift between them.
    function _governedTargets(Deployment memory d) internal pure returns (IAccessControl[5] memory) {
        return [IAccessControl(address(d.router)), d.bond, d.blacklist, d.reserve, d.registry];
    }
}
