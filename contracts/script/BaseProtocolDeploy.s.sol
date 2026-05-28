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
///         five-phase deploy described in ADR 016 § Deployment Order and
///         Initialization Dependencies + § Post-Deployment Initialization, then
///         asserts the deployer EOA holds no role on any target. Inherited by
///         `DeployProtocol.s.sol` (the env-var production script) and
///         `DeployProtocol.t.sol` (the in-process role-topology test) so both
///         share one source of truth.
///
/// @dev    Phases:
///           1. `_deployTargets`         — Token, CapacityBond, SafetyReserve,
///                                         FeeRouter (buyback bucket dormant —
///                                         see BuybackBurner note below),
///                                         ContentBlacklist, PublisherRegistry.
///                                         Deployer is admin of every
///                                         AccessControl-bearing target.
///           2. `_deployGovernance`      — TimelockController (deployer as admin;
///                                         empty proposers; open executor) and
///                                         DecdnGovernor; grant Timelock's
///                                         PROPOSER + CANCELLER roles to the
///                                         Governor.
///           3. `_wireCrossContractRoles` — peer role grants (settlement reporter,
///                                          slash-inflow reporter, appeal reversal,
///                                          blacklist, emergency multisig) plus
///                                          deployer-only setters (setSafetyReserve,
///                                          setChallengerIncentivePool) that MUST
///                                          run before the GOVERNANCE_ROLE handoff
///                                          because the same setters become
///                                          Timelock-gated post-handoff.
///           4. `_handOffGovernance`      — grant-before-revoke loop over every
///                                          target for both GOVERNANCE_ROLE and
///                                          DEFAULT_ADMIN_ROLE, then renounce the
///                                          deployer's admin on the Timelock
///                                          itself. Grant-before-revoke ordering
///                                          is mandatory; reversing it strands
///                                          the contract ungoverned mid-tx.
///           5. `_assertNoBackDoors`      — reverts if deployer still holds
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
        // Roles + recipients
        address deployer;
        address treasury;
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

    function _runFullDeploy(DeployConfig memory cfg) internal returns (Deployment memory d) {
        d = _deployTargets(cfg);
        _deployGovernance(cfg, d);
        _wireCrossContractRoles(cfg, d);
        _handOffGovernance(cfg, d);
        _assertNoBackDoors(cfg, d);
    }

    // Phase 1 — deploy targets with deployer as admin.
    function _deployTargets(DeployConfig memory cfg) internal returns (Deployment memory d) {
        // Fail-fast on the seven fields whose absence either reverts a
        // constructor with an opaque error (`usdc`, `ed25519Verifier`,
        // `treasury`, `initialTokenHolder`) or silently no-ops a role grant
        // downstream (`emergencyMultisig` skips SafetyReserve's constructor
        // grant; `challengerIncentivePool` would brick `reverseAppeal`;
        // ContentBlacklist would grant EMERGENCY_MULTISIG_ROLE to `address(0)`).
        if (address(cfg.usdc) == address(0)) revert ZeroAddress("usdc");
        if (address(cfg.ed25519Verifier) == address(0)) revert ZeroAddress("ed25519Verifier");
        if (cfg.deployer == address(0)) revert ZeroAddress("deployer");
        if (cfg.treasury == address(0)) revert ZeroAddress("treasury");
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
            treasury_: cfg.treasury,
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

    // Phase 2 — Timelock + Governor; Timelock proposer/canceller wiring.
    function _deployGovernance(DeployConfig memory cfg, Deployment memory d) internal {
        address[] memory emptyProposers = new address[](0);
        address[] memory openExecutor = new address[](1);
        openExecutor[0] = address(0);
        d.timelock = new TimelockController(cfg.timelockDelay, emptyProposers, openExecutor, cfg.deployer);

        d.governor = new DecdnGovernor(d.router, d.bond, d.timelock);

        d.timelock.grantRole(d.timelock.PROPOSER_ROLE(), address(d.governor));
        d.timelock.grantRole(d.timelock.CANCELLER_ROLE(), address(d.governor));
    }

    // Phase 3 — cross-contract peer roles + deployer-only state setters.
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

        _postWiringHook(cfg, d);
    }

    /// @dev Subclasses (e.g. GovernanceLifecycleTest) override this to grant
    ///      test-only roles like ROUTER_CALLER_ROLE before the handoff puts
    ///      the role-grant surface behind the Timelock. No-op in production.
    function _postWiringHook(DeployConfig memory cfg, Deployment memory d) internal virtual { }

    // Phase 4 — atomic role handoff to Timelock.
    //
    // Grant-before-revoke ordering is mandatory: revoking GOVERNANCE_ROLE or
    // DEFAULT_ADMIN_ROLE from the deployer before granting it to the Timelock
    // strands the contract (no holder of either role) and locks out every
    // governance setter until a recovery deploy.
    function _handOffGovernance(DeployConfig memory cfg, Deployment memory d) internal {
        IAccessControl[5] memory targets =
            [IAccessControl(address(d.router)), d.bond, d.blacklist, d.reserve, d.registry];
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

    // Phase 5 — post-deploy invariant.
    function _assertNoBackDoors(DeployConfig memory cfg, Deployment memory d) internal view {
        IAccessControl[5] memory targets =
            [IAccessControl(address(d.router)), d.bond, d.blacklist, d.reserve, d.registry];
        for (uint256 i = 0; i < targets.length; i++) {
            if (targets[i].hasRole(GOVERNANCE_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(address(targets[i]), GOVERNANCE_ROLE);
            }
            if (targets[i].hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(address(targets[i]), DEFAULT_ADMIN_ROLE);
            }
        }
        if (d.timelock.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
            revert DeployerStillHoldsRole(address(d.timelock), DEFAULT_ADMIN_ROLE);
        }
    }
}
