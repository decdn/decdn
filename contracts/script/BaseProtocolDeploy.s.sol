// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { Token } from "../src/Token.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { CctpSweepReceiver } from "../src/CctpSweepReceiver.sol";
import { IMessageTransmitterV2 } from "../src/interfaces/ICctpV2.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { ICapacityBondCredit } from "../src/interfaces/ICapacityBondCredit.sol";
import { OriginAssignment } from "../src/OriginAssignment.sol";
import { ManualVettingPolicy } from "../src/ManualVettingPolicy.sol";
import { IVettingPolicy } from "../src/interfaces/IVettingPolicy.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";

/// @title BaseProtocolDeploy
/// @notice Abstract deploy primitive for the v3 contract surface. Performs the
///         seven-phase deploy described in ADR 016 § Deployment Order and
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
///           2. `_deployTargets`         — Token, CapacityBond,
///                                         FeeRouter (treasury = the
///                                         TimelockController),
///                                         CctpSweepReceiver, ContentBlacklist,
///                                         PublisherRegistry, ManualVettingPolicy,
///                                         OriginAssignment. Deployer is admin of
///                                         every AccessControl-bearing target.
///                                         This is the governance-chain set; each
///                                         payment chain deploys its PaymentPool
///                                         with `DeployPaymentPool.s.sol`.
///           3. `_deployGovernor`        — DecdnGovernor; grant Timelock's
///                                         PROPOSER + CANCELLER roles to
///                                         `_initialProposer` — the Governor, or
///                                         the bootstrap multisig when one is
///                                         configured (ADR 009).
///           4. `_wireCrossContractRoles` — peer role grants (blacklist ejector,
///                                          emergency multisig, PAUSER_ROLE on every
///                                          Pausable target), run here while the
///                                          deployer still holds the role admin so
///                                          they skip the Timelock delay they would
///                                          otherwise incur post-handoff.
///           5. `_handOffGovernance`      — hand GOVERNANCE_ROLE to the Timelock
///                                          (grant-before-revoke), then renounce
///                                          DEFAULT_ADMIN_ROLE to no one (#2028) so
///                                          the role table freezes, and finally
///                                          renounce the deployer's admin on the
///                                          Timelock itself. Ordering is mandatory:
///                                          the DEFAULT_ADMIN_ROLE renounce is the
///                                          last role op per target, and reversing
///                                          the GOVERNANCE_ROLE grant/revoke strands
///                                          the contract ungoverned mid-tx.
///           6. `_assertNoBackDoors`      — reverts if the deployer still holds
///                                          GOVERNANCE_ROLE on any target, if the
///                                          Timelock does not hold GOVERNANCE_ROLE,
///                                          if anyone still holds DEFAULT_ADMIN_ROLE
///                                          (#2028), or if the Timelock's sole
///                                          proposer is not the one this deploy
///                                          mode seats. Runs in-script (not
///                                          just in tests) so a mainnet deploy
///                                          refuses to finish if any handoff
///                                          step failed silently. It cannot detect
///                                          an arbitrary third proposer — OZ's
///                                          Timelock is not enumerable — only that
///                                          the seated address holds the role and
///                                          the Governor does not.
///           7. `_assertPeerRolesWired`   — reverts if any phase-4 peer-role grant
///                                          or address binding (pausers, emergency
///                                          multisig, blacklist ejector, vetting
///                                          policy, receiver → router) did not
///                                          land.
///                                          `grantRole` to a wrong address does not
///                                          revert, so without this a half-wired
///                                          protocol would ship silently.
abstract contract BaseProtocolDeploy is Script {
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant DEFAULT_ADMIN_ROLE = 0x00;

    /// @dev Ethereum mainnet, the governance chain the emergency-multisig and
    ///      bootstrap-multisig checks apply to.
    uint256 internal constant ETHEREUM_MAINNET_CHAIN_ID = 1;

    struct DeployConfig {
        // External dependencies
        IERC20 usdc;
        IEd25519Verifier ed25519Verifier;
        // Roles + recipients. The FeeRouter treasury bucket is not a config
        // field: it is always the TimelockController deployed by this script —
        // the FeeRouter treasury destination (ADR 016 § Deployment Order step 6),
        // which is Timelock-custodied (step 3 / § Contracts Holding Funds).
        address deployer;
        address emergencyMultisig;
        address initialTokenHolder;
        // ManualVettingPolicy VETTER_ROLE holder at genesis (ADR 011). May be
        // `address(0)`: then no genesis vetter is seated and governance grants
        // VETTER_ROLE post-deploy. On the initial testnet this is the operator
        // that approves publishers without a 48-hour Timelock delay.
        address initialVetter;
        // Governance / Timelock
        uint256 timelockDelay;
        // ADR 009 § Bootstrap-multisig phase. `address(0)` (the default) seats
        // `DecdnGovernor` as the Timelock's proposer, so served-bytes-weighted DAO
        // voting is live from block one. A non-zero address instead seats THIS
        // multisig as the sole proposer and leaves the Governor without the role, so
        // only the multisig can schedule while the operator set is thin. Either way
        // the Timelock holds GOVERNANCE_ROLE and imposes its 48-hour delay. Required
        // on Ethereum mainnet (`BootstrapMultisigRequired`); optional on testnets,
        // because the initial testnet network runs a single controlled EOA and has
        // no multisig to seat (issue #1175).
        address bootstrapMultisig;
        // CapacityBond economic params (ADR 026 defaults)
        uint256 minBond;
        uint256 multiaddrUpdateCooldown;
        uint256 maxMultiaddrSize;
        uint256 regionStabilityWindow;
        // ADR 019 § Terms Acceptance — genesis operator-terms hash
        // (keccak256 of the shipped TERMS.md). Governance-swappable post-deploy
        // via CapacityBond.setCurrentTermsHash.
        bytes32 currentTermsHash;
        // FeeRouter params (ADR 016 § Tunable Economics).
        uint64 feeRouterEpochLength;
        uint64 feeRouterWindowEpochs;
        uint256 feeRouterRebateBps;
        uint256 feeRouterDeliveryFloor;
        // Governance-chain CCTP V2 contracts the `CctpSweepReceiver` binds.
        IMessageTransmitterV2 cctpMessageTransmitter;
        address cctpTokenMessenger;
    }

    struct Deployment {
        Token token;
        CapacityBond bond;
        FeeRouter router;
        ContentBlacklist blacklist;
        PublisherRegistry registry;
        TimelockController timelock;
        DecdnGovernor governor;
        CctpSweepReceiver sweepReceiver;
        OriginAssignment originAssignment;
        ManualVettingPolicy vettingPolicy;
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
    /// @notice Post-deploy invariant (#2028) — `DEFAULT_ADMIN_ROLE` still has a
    ///         holder on `target`. The handoff renounces it to no one so the role
    ///         table freezes; a lingering holder is a master key that could mint a
    ///         fresh `GOVERNANCE_ROLE` and defeat every other hardening.
    error AdminRoleNotRenounced(address target, address holder);
    /// @notice Post-deploy invariant (#2028) — `role`'s admin on `target` is not
    ///         the expected value. The rotatable off-chain-key roles must be
    ///         `GOVERNANCE_ROLE`-administered (so a compromised key can be rotated
    ///         out post-freeze), and the check fails loudly if a constructor
    ///         `_setRoleAdmin` was dropped and the role silently froze instead.
    error RoleAdminNotScoped(address target, bytes32 role, bytes32 wantAdmin, bytes32 gotAdmin);
    /// @notice Post-deploy invariant — `account` was meant to hold a Timelock
    ///         scheduling role and does not. `role` decides the consequence: a missing
    ///         `PROPOSER_ROLE` means nobody can schedule and the protocol ships
    ///         ungovernable; a missing `CANCELLER_ROLE` means the holder cannot pull
    ///         back its own erroneous proposal inside the 48-hour window, for the
    ///         whole bootstrap phase.
    error TimelockRoleMissing(address account, bytes32 role);
    /// @notice Post-deploy invariant — `account` holds a Timelock scheduling role and
    ///         must not. The consequence is the opposite of `TimelockRoleMissing` and
    ///         they are deliberately separate errors rather than one carrying a
    ///         boolean: a deploy aborts mid-broadcast and the revert has to say which
    ///         it was without the operator going to read the source. `role` decides
    ///         the consequence: a bootstrap deploy that left `DecdnGovernor`
    ///         `PROPOSER_ROLE` puts DAO execution one 48-hour delay away against an
    ///         operator set still too thin for capacity-weighted voting to be safe
    ///         (ADR 009), while `CANCELLER_ROLE` would let it cancel the multisig's
    ///         proposals — including the transition batch itself. A deployer holding
    ///         either is a standing single-key power surviving the handoff.
    error TimelockRoleUnexpected(address account, bytes32 role);
    /// @notice `DeployConfig.bootstrapMultisig` is the deployer EOA. That would seat
    ///         a single key as the Timelock's sole proposer for the whole bootstrap
    ///         phase — the standing god-mode the handoff exists to remove — and
    ///         `_assertNoBackDoors` would pass, because it asks whether the deployer
    ///         holds GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE, not whether it can schedule.
    error BootstrapMultisigIsDeployer(address account);
    /// @notice `DeployConfig.bootstrapMultisig` holds no code. ADR 009 § Composition
    ///         specifies a 5-of-9 multisig; a typo'd or dead EOA here becomes the only
    ///         party that can ever schedule, with the Governor unseated and the
    ///         deployer renounced — permanently ungovernable, with no on-chain
    ///         recovery and a green deploy.
    error BootstrapMultisigNotAContract(address account);
    /// @notice On Ethereum mainnet, `DeployConfig.emergencyMultisig` is the deployer
    ///         EOA, so one key would hold every pause and emergency role.
    error EmergencyMultisigIsDeployer(address account);
    /// @notice On Ethereum mainnet, `DeployConfig.emergencyMultisig` holds no code, so
    ///         it cannot be the multisig ADR 009 § Emergency Multisig specifies.
    error EmergencyMultisigNotAContract(address account);
    /// @notice On Ethereum mainnet, `DeployConfig.bootstrapMultisig` is unset. The
    ///         deploy would seat `DecdnGovernor` as proposer at block one, when
    ///         proposalThreshold and quorum are zero against an empty served-bytes
    ///         window, so the first operator with any vote weight could pass a
    ///         proposal alone. ADR 009
    ///         § Bootstrap-multisig phase requires the multisig at launch.
    error BootstrapMultisigRequired();
    /// @notice Post-deploy invariant — a phase-4 peer role (e.g. `BLACKLIST_ROLE`,
    ///         `PAUSER_ROLE`) was not actually granted to `grantee` on `target`.
    ///         OZ `grantRole` does not revert when the target address is wrong, so
    ///         a dropped or mis-targeted peer grant is otherwise silent — shipping
    ///         a dead blacklist ejector or no live pauser.
    error PeerRoleNotWired(address target, bytes32 role, address grantee);
    /// @notice Post-deploy invariant — an address binding (such as
    ///         `OriginAssignment.vettingPolicy` or `CctpSweepReceiver.feeRouter`)
    ///         does not match the intended deployment target. Catches a setter or
    ///         constructor binding error that would silently leave a security
    ///         check unwired.
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
    // empty (PROPOSER_ROLE granted in phase 3, to the Governor or the bootstrap
    // multisig — see `_initialProposer`) and `executors`
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
        // `initialTokenHolder`) or silently mis-wires a role grant downstream
        // (`emergencyMultisig`: ContentBlacklist would grant
        // EMERGENCY_MULTISIG_ROLE to `address(0)`).
        // The treasury is not validated here: it is `address(timelock)`, always
        // non-zero.
        if (address(cfg.usdc) == address(0)) revert ZeroAddress("usdc");
        if (address(cfg.ed25519Verifier) == address(0)) revert ZeroAddress("ed25519Verifier");
        if (cfg.deployer == address(0)) revert ZeroAddress("deployer");
        if (cfg.emergencyMultisig == address(0)) revert ZeroAddress("emergencyMultisig");
        // On mainnet the emergency multisig holds every PAUSER_ROLE and the
        // blacklist emergency role, so it must be a real multisig, never
        // the deployer key. Testnet and e2e deploys seat the deployer on purpose.
        if (block.chainid == ETHEREUM_MAINNET_CHAIN_ID) {
            if (cfg.emergencyMultisig == cfg.deployer) revert EmergencyMultisigIsDeployer(cfg.deployer);
            if (cfg.emergencyMultisig.code.length == 0) revert EmergencyMultisigNotAContract(cfg.emergencyMultisig);
            // Without a bootstrap multisig the Governor is the Timelock's proposer
            // at genesis, while proposalThreshold and quorum are zero against an
            // empty served-bytes window. ADR 009 requires the bootstrap phase at
            // launch.
            if (cfg.bootstrapMultisig == address(0)) revert BootstrapMultisigRequired();
        }
        if (cfg.initialTokenHolder == address(0)) revert ZeroAddress("initialTokenHolder");

        d.token = new Token(cfg.initialTokenHolder);

        d.bond = new CapacityBond({
            token_: d.token,
            ed25519Verifier_: cfg.ed25519Verifier,
            admin: cfg.deployer,
            minBond_: cfg.minBond,
            multiaddrUpdateCooldown_: cfg.multiaddrUpdateCooldown,
            maxMultiaddrSize_: cfg.maxMultiaddrSize,
            regionStabilityWindow_: cfg.regionStabilityWindow,
            currentTermsHash_: cfg.currentTermsHash
        });

        d.router = new FeeRouter({
            usdc_: cfg.usdc,
            capacityBond_: ICapacityBondCredit(address(d.bond)),
            treasury_: address(timelock),
            epochLength_: cfg.feeRouterEpochLength,
            windowEpochs_: cfg.feeRouterWindowEpochs,
            deliveryFloor_: cfg.feeRouterDeliveryFloor,
            admin: cfg.deployer,
            rebateBps_: cfg.feeRouterRebateBps
        });

        // CctpSweepReceiver (ADR 016 § Bridge adapters): the CCTP V2 adapter
        // every payment-chain PaymentPool sweeps to. Stateless, no roles.
        if (address(cfg.cctpMessageTransmitter) == address(0)) revert ZeroAddress("cctpMessageTransmitter");
        if (cfg.cctpTokenMessenger == address(0)) revert ZeroAddress("cctpTokenMessenger");
        d.sweepReceiver = new CctpSweepReceiver({
            usdc_: cfg.usdc,
            messageTransmitter_: cfg.cctpMessageTransmitter,
            tokenMessenger_: cfg.cctpTokenMessenger,
            feeRouter_: IFeeRouter(address(d.router))
        });

        // PublisherRegistry — consumed by `OriginAssignment` below, which binds
        // it as a constructor immutable. Needs only `admin`.
        d.registry = new PublisherRegistry({ admin: cfg.deployer });

        // `ContentBlacklist` takes no token: it custodies no funds (ADR 016
        // § Contract Inventory).
        d.blacklist =
            new ContentBlacklist({ capacityBond_: ICapacityBondEjector(address(d.bond)), admin: cfg.deployer });

        // ManualVettingPolicy (ADR 011): the genesis vetting policy, consumed by
        // `OriginAssignment` below as a constructor arg. Deployer is admin +
        // GOVERNANCE_ROLE (handed to the Timelock in phase 5); `initialVetter`
        // (may be zero) is seeded with VETTER_ROLE, whose admin is GOVERNANCE_ROLE
        // so governance re-points it post-handoff.
        d.vettingPolicy = new ManualVettingPolicy({ admin: cfg.deployer, initialVetter: cfg.initialVetter });

        // OriginAssignment (ADR 011): the seat set is a routing hint, so it takes
        // no ContentBlacklist binding — consumers of `getOrigins` filter against
        // the blacklist themselves. The vetting policy is a constructor
        // immutable-shaped binding (governance swaps it via `setVettingPolicy`),
        // non-zero from genesis.
        d.originAssignment = new OriginAssignment({
            capacityBond_: ICapacityBondActivity(address(d.bond)),
            publisherRegistry_: IPublisherRegistryOwnership(address(d.registry)),
            vettingPolicy_: IVettingPolicy(address(d.vettingPolicy)),
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

        // Validate the bootstrap multisig BEFORE seating it. `_assertNoBackDoors`
        // cannot catch either failure below, because in both cases the address
        // genuinely does hold the role the invariant asks about:
        //
        //   - deployer — seats a standing single-key scheduling power on a Timelock
        //     that holds GOVERNANCE_ROLE over every target, surviving the very
        //     handoff whose purpose is to leave the deployer with nothing. The
        //     deployer's Timelock DEFAULT_ADMIN_ROLE is renounced in phase 5, so
        //     the grant is then irrevocable without the deployer's own cooperation.
        //   - non-contract — a typo'd or dead address becomes the only party that
        //     can ever schedule. The Governor has no proposer role, the deployer has
        //     renounced, and there is no on-chain recovery: permanently ungovernable.
        //
        // Both are plausible slips (`BOOTSTRAP_MULTISIG=$DEPLOYER` in a shell that
        // already exports `DEPLOYER`), and neither has any recovery path, so they
        // are worth a fail-fast rather than an invariant.
        if (cfg.bootstrapMultisig != address(0)) {
            if (cfg.bootstrapMultisig == cfg.deployer) revert BootstrapMultisigIsDeployer(cfg.deployer);
            if (cfg.bootstrapMultisig.code.length == 0) revert BootstrapMultisigNotAContract(cfg.bootstrapMultisig);
        }

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

    /// @dev Assert one Timelock scheduling role is seated as this deploy mode
    ///      requires. Three legs: held by `_initialProposer`; not held by the Governor
    ///      when the Governor is not the expected proposer (i.e. in bootstrap mode);
    ///      and never held by the deployer. Applied to `PROPOSER_ROLE` and
    ///      `CANCELLER_ROLE` alike.
    ///
    ///      This is a denylist of two suspects, not a proof of soleness — OZ's
    ///      `TimelockController` is not `AccessControlEnumerable`, so an arbitrary
    ///      third grantee cannot be detected by reading roles. Closing that would take
    ///      a `RoleGranted` log sweep over the deploy.
    function _assertTimelockRoleSeating(DeployConfig memory cfg, Deployment memory d, bytes32 role) internal view {
        address expected = _initialProposer(cfg, d);
        if (!d.timelock.hasRole(role, expected)) revert TimelockRoleMissing(expected, role);

        // Gated on `expected != governor` rather than restating the mode test. Note
        // this is a readability change and nothing more: `_initialProposer` returns
        // the Governor exactly when `bootstrapMultisig == 0`, so the two predicates
        // agree in every reachable configuration, and on a default deploy this leg is
        // still skipped — correctly, since there the Governor IS the expected
        // proposer. All of the added coverage comes from the deployer leg below.
        if (address(d.governor) != expected && d.timelock.hasRole(role, address(d.governor))) {
            revert TimelockRoleUnexpected(address(d.governor), role);
        }

        // The deployer, unconditionally. `_assertNoBackDoors` checks it for
        // GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE on the targets and DEFAULT_ADMIN_ROLE on
        // the Timelock — never for scheduling power. It admins the Timelock through
        // phases 3-5, so a `_postWiringHook` override or any future code could grant
        // itself one and survive the handoff whose whole point is to leave it nothing.
        if (cfg.deployer != expected && d.timelock.hasRole(role, cfg.deployer)) {
            revert TimelockRoleUnexpected(cfg.deployer, role);
        }
    }

    // Phase 4 — cross-contract peer role grants.
    //
    // These grants all require the deployer to still hold the role's admin on
    // the target, so they run before the phase-5 handoff. After it, a role's
    // fate follows its admin. `BLACKLIST_ROLE` on CapacityBond is admin'd by
    // `DEFAULT_ADMIN_ROLE`, which phase 5 renounces, so that grant freezes.
    // `EMERGENCY_MULTISIG_ROLE` on ContentBlacklist and `PAUSER_ROLE` on
    // CapacityBond and FeeRouter are admin'd by `GOVERNANCE_ROLE`, so they stay
    // rotatable behind the 48h Timelock.
    function _wireCrossContractRoles(DeployConfig memory cfg, Deployment memory d) internal {
        // ContentBlacklist ejects operators via CapacityBond on blacklist add.
        d.bond.grantRole(d.bond.BLACKLIST_ROLE(), address(d.blacklist));

        // EMERGENCY_MULTISIG_ROLE: ContentBlacklist has no constructor grant,
        // so grant explicitly.
        d.blacklist.grantRole(d.blacklist.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);

        // PAUSER_ROLE → the emergency multisig on every Pausable target (ADR 016
        // § Role Inventory: `EMERGENCY_ROLE` holds `pause()` on fund-holding /
        // Pausable contracts, a 3-of-5 multisig). No constructor grants
        // PAUSER_ROLE, so without this wiring the system comes up with no live
        // pauser: post-handoff PAUSER_ROLE is GOVERNANCE_ROLE-administered (#2028),
        // so a first grant would then need a 48h-delayed governance proposal —
        // defeating the emergency path. Granted here, before the handoff (the
        // deployer still holds GOVERNANCE_ROLE, PAUSER_ROLE's admin), so the
        // multisig can pause from block one. Post-freeze a compromised pauser is
        // still rotatable through that same GOVERNANCE_ROLE admin.
        d.bond.grantRole(d.bond.PAUSER_ROLE(), cfg.emergencyMultisig);
        d.router.grantRole(d.router.PAUSER_ROLE(), cfg.emergencyMultisig);

        _postWiringHook(cfg, d);
    }

    /// @dev Subclasses (e.g. GovernanceLifecycleTest) override this to wire
    ///      test-only state while the deployer still holds every role admin,
    ///      before the phase-5 handoff. No-op in production.
    function _postWiringHook(DeployConfig memory cfg, Deployment memory d) internal virtual { }

    // Phase 5 — role handoff to Timelock, then DEFAULT_ADMIN_ROLE renounced (#2028).
    //
    // The Timelock becomes the sole GOVERNANCE_ROLE holder, so every parameter
    // change carries the standard 48-hour delay whoever proposes it. The bootstrap
    // phase differs only in who may schedule through the Timelock, which
    // `_deployGovernor` decides.
    //
    // DEFAULT_ADMIN_ROLE is then renounced to NO ONE (#2028). It is the master key
    // over the role table — a holder could `grantRole(GOVERNANCE_ROLE, anyEOA)` and
    // hand that EOA the whole governance surface with no vote and no Timelock delay,
    // bypassing every other hardening. Renouncing it freezes the table: GOVERNANCE_ROLE
    // and the contract-held peer roles (their admin is the now-unheld DEFAULT_ADMIN_ROLE)
    // can no longer be granted or rotated. The narrow, genuinely-rotatable off-chain-key
    // roles (PAUSER / EMERGENCY_MULTISIG) are GOVERNANCE_ROLE-administered by
    // each constructor, so governance keeps exactly that minimal rotation
    // power and nothing more. Migrating governance to a successor Governor is a
    // redeploy, consistent with the no-proxy philosophy.
    //
    // Ordering is mandatory per target: grant GOVERNANCE_ROLE to the Timelock and
    // revoke it from the deployer BEFORE renouncing DEFAULT_ADMIN_ROLE. Revoking
    // GOVERNANCE_ROLE needs the deployer's still-held admin, and once DEFAULT_ADMIN_ROLE
    // is renounced no admin-gated grant/revoke can succeed — so the renounce is the
    // last role op on each target.
    function _handOffGovernance(DeployConfig memory cfg, Deployment memory d) internal {
        IAccessControl[6] memory targets = _governedTargets(d);
        address tl = address(d.timelock);
        for (uint256 i = 0; i < targets.length; i++) {
            IAccessControl target = targets[i];
            target.grantRole(GOVERNANCE_ROLE, tl);
            target.revokeRole(GOVERNANCE_ROLE, cfg.deployer);
            // Last op on this target — freezes the role table. renounceRole drops
            // the deployer's own DEFAULT_ADMIN_ROLE with no onward grant.
            target.renounceRole(DEFAULT_ADMIN_ROLE, cfg.deployer);
        }

        // Final step: deployer no longer admins the Timelock itself.
        d.timelock.renounceRole(d.timelock.DEFAULT_ADMIN_ROLE(), cfg.deployer);
    }

    // Phase 6 — post-deploy invariant. Three checks per target: GOVERNANCE_ROLE
    // is off the deployer (no back door) and on the Timelock (governance is live,
    // not stranded), and DEFAULT_ADMIN_ROLE is held by no one (#2028 — the role
    // table is frozen, no master key survives). A handoff that revoked the deployer
    // but skipped the Timelock grant would pass the back-door half yet leave a
    // contract ungoverned; one that left DEFAULT_ADMIN_ROLE on the Timelock would
    // leave the capture master key in place.
    function _assertNoBackDoors(DeployConfig memory cfg, Deployment memory d) internal view {
        IAccessControl[6] memory targets = _governedTargets(d);
        address tl = address(d.timelock);

        for (uint256 i = 0; i < targets.length; i++) {
            IAccessControl target = targets[i];
            if (target.hasRole(GOVERNANCE_ROLE, cfg.deployer)) {
                revert DeployerStillHoldsRole(address(target), GOVERNANCE_ROLE);
            }
            if (!target.hasRole(GOVERNANCE_ROLE, tl)) {
                revert GovernanceNotHandedOff(address(target), GOVERNANCE_ROLE);
            }
            // DEFAULT_ADMIN_ROLE is renounced to no one (#2028), so neither the
            // deployer nor the Timelock may hold it — a Timelock holder would still
            // be a master key reachable by a captured governance. OZ AccessControl
            // is not enumerable, so this asserts the only two addresses that could
            // plausibly hold it: the deployer (which held it transiently before the
            // renounce) and the Timelock (never granted it here, checked in case a
            // future handoff edit grants it by mistake). The constructor
            // `_setRoleAdmin` moves live-key roles off it and `_assertPeerRolesWired`
            // confirms those admins landed.
            if (target.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
                revert AdminRoleNotRenounced(address(target), cfg.deployer);
            }
            if (target.hasRole(DEFAULT_ADMIN_ROLE, tl)) {
                revert AdminRoleNotRenounced(address(target), tl);
            }
        }

        // Timelock proposer/canceller seating (ADR 009, issue #1175). The role checks
        // above are mode-independent — they pass identically whether the Governor or
        // the bootstrap multisig can propose — so nothing else in this invariant can
        // catch a bootstrap deploy that left the Governor a proposer, which would put
        // DAO proposals one 48-hour delay from execution against the thin operator set
        // the phase exists to protect. Asserted in both directions so the default
        // deploy equally cannot ship with a Governor that can never propose.
        //
        // CANCELLER_ROLE is checked alongside PROPOSER_ROLE rather than assumed from
        // it. `_deployGovernor` grants them on adjacent lines and `TransitionToGovernor`
        // moves them together, but OZ `grantRole` does not revert on a wrong or dropped
        // target — the same silence `_assertPeerRolesWired` exists to break. A canceller
        // grant that missed the multisig would take away its only way to cancel its own
        // erroneous scheduled proposal inside the 48-hour window, for the whole 6–12
        // month phase, and nothing would surface it until the day it was needed.
        _assertTimelockRoleSeating(cfg, d, d.timelock.PROPOSER_ROLE());
        _assertTimelockRoleSeating(cfg, d, d.timelock.CANCELLER_ROLE());

        if (d.timelock.hasRole(DEFAULT_ADMIN_ROLE, cfg.deployer)) {
            revert DeployerStillHoldsRole(tl, DEFAULT_ADMIN_ROLE);
        }
        // The Timelock must self-administer, or its own role surface is stranded.
        // True in both modes: the bootstrap phase confines who may *schedule* through
        // the Timelock, not what the Timelock holds or its control of its own roles.
        if (!d.timelock.hasRole(DEFAULT_ADMIN_ROLE, tl)) {
            revert GovernanceNotHandedOff(tl, DEFAULT_ADMIN_ROLE);
        }
    }

    // Phase 7 — post-deploy invariant for the phase-4 peer wiring.
    //
    // `_assertNoBackDoors` covers only the GOVERNANCE_ROLE/DEFAULT_ADMIN_ROLE
    // handoff. The peer grants and address bindings wired in `_wireCrossContractRoles`
    // are security-critical (the blacklist ejector, the live pausers, the
    // vetting-policy binding, the receiver's router binding) yet none are reverting:
    // OZ `grantRole` to the wrong address is a silent no-op of the intended
    // property. This phase re-reads each one so a dropped or mis-targeted grant
    // fails the deploy loudly instead of shipping a half-wired protocol.
    function _assertPeerRolesWired(DeployConfig memory cfg, Deployment memory d) internal view {
        // CapacityBond peer role.
        _requireRole(d.bond, d.bond.BLACKLIST_ROLE(), address(d.blacklist));
        // EMERGENCY_MULTISIG_ROLE on the blacklist.
        _requireRole(d.blacklist, d.blacklist.EMERGENCY_MULTISIG_ROLE(), cfg.emergencyMultisig);
        // PAUSER_ROLE on every deployed Pausable target.
        _requireRole(d.bond, d.bond.PAUSER_ROLE(), cfg.emergencyMultisig);
        _requireRole(d.router, d.router.PAUSER_ROLE(), cfg.emergencyMultisig);

        // #2028 — the rotatable off-chain-key roles must be GOVERNANCE_ROLE-administered
        // so a compromised key can be rotated out after DEFAULT_ADMIN_ROLE is renounced.
        // A dropped constructor `_setRoleAdmin` would silently leave the role admined by
        // the now-unheld DEFAULT_ADMIN_ROLE — permanently frozen, unrotatable — and only
        // this read surfaces it. Checked on every deployed Pausable target.
        _requireRoleAdmin(d.bond, d.bond.PAUSER_ROLE(), GOVERNANCE_ROLE);
        _requireRoleAdmin(d.router, d.router.PAUSER_ROLE(), GOVERNANCE_ROLE);
        _requireRoleAdmin(d.blacklist, d.blacklist.EMERGENCY_MULTISIG_ROLE(), GOVERNANCE_ROLE);

        // Address bindings.
        // OriginAssignment.vettingPolicy is a constructor binding, but re-read it
        // so a wrong-wired genesis policy fails the deploy loudly like every other
        // security-critical binding.
        address boundVettingPolicy = address(d.originAssignment.vettingPolicy());
        if (boundVettingPolicy != address(d.vettingPolicy)) {
            revert BindingNotWired(address(d.originAssignment), address(d.vettingPolicy), boundVettingPolicy);
        }
        // Every CCTP sweep lands through the receiver, so it must deposit into
        // this deployment's router.
        address boundRouter = address(d.sweepReceiver.feeRouter());
        if (boundRouter != address(d.router)) {
            revert BindingNotWired(address(d.sweepReceiver), address(d.router), boundRouter);
        }
    }

    function _requireRole(IAccessControl target, bytes32 role, address grantee) private view {
        if (!target.hasRole(role, grantee)) revert PeerRoleNotWired(address(target), role, grantee);
    }

    /// @dev #2028 — assert `role`'s admin on `target` equals `wantAdmin`, so a
    ///      dropped constructor `_setRoleAdmin` (which would leave the role admined
    ///      by the renounced DEFAULT_ADMIN_ROLE, i.e. permanently frozen) fails the
    ///      deploy loudly.
    function _requireRoleAdmin(IAccessControl target, bytes32 role, bytes32 wantAdmin) private view {
        bytes32 got = target.getRoleAdmin(role);
        if (got != wantAdmin) revert RoleAdminNotScoped(address(target), role, wantAdmin, got);
    }

    /// @dev The six role-bearing targets whose handoff grants GOVERNANCE_ROLE to
    ///      the Timelock and renounces DEFAULT_ADMIN_ROLE to no one (router, bond,
    ///      blacklist, registry, originAssignment, vettingPolicy) — single
    ///      source of truth for `_handOffGovernance` and `_assertNoBackDoors` so
    ///      the governed set can't drift between them. The array width MUST equal
    ///      the number of role-bearing `Deployment` members; adding a target
    ///      requires widening it. `ManualVettingPolicy`'s VETTER_ROLE is admin'd
    ///      by its GOVERNANCE_ROLE, so handing off GOVERNANCE_ROLE gives the
    ///      Timelock control of the vetter set too.
    function _governedTargets(Deployment memory d) internal pure returns (IAccessControl[6] memory) {
        IAccessControl router = IAccessControl(address(d.router));
        return [router, d.bond, d.blacklist, d.registry, d.originAssignment, d.vettingPolicy];
    }
}
