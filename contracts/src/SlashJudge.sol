// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { ISlashJudge } from "./interfaces/ISlashJudge.sol";
import { ICapacityBondSlasher } from "./interfaces/ICapacityBondSlasher.sol";
import { ICapacityBondRegionView } from "./interfaces/ICapacityBondRegionView.sol";
import { IContentBlacklistHashView } from "./interfaces/IContentBlacklistHashView.sol";
import { RegionScopeLib } from "./RegionScopeLib.sol";

/// @title SlashJudge
/// @notice On-chain adjudicator for the three signature-dependent slashable
///         offenses (ADR 014): phantom announcement, rate manipulation, and
///         blacklist violation. Challenging is a two-phase commit–reveal flow
///         (ADR 014 § Challenge front-running mitigation, #854): the challenger
///         first `commitChallenge`s an opaque
///         `keccak256(evidenceHash, salt, challenger)`, then reveals via a
///         `submit*Challenge` once the commitment has matured. Each reveal
///         verifies secp256k1 EIP-712 `slash_sig` evidence with `SignatureChecker`
///         (EOA + ERC-1271), confirms the challenged address is a registered
///         operator, enforces the evidence-age window, then calls
///         `CapacityBond.slash` and emits the canonical `Slashed` event with no
///         counter-evidence window. The challenger is recorded by `slash` for the
///         50% finality reward (escrow-on-slash, ADR 026 / ADR 028); binding the
///         challenger into the commitment stops a mempool copy of the reveal from
///         stealing that reward. `SlashJudge` itself only round-trips the
///         challenge bond.
/// @dev    `MAX_EVIDENCE_AGE_US < CapacityBond.unbondingPeriod * 1e6` is enforced on
///         this side (constructor + `setMaxEvidenceAge`). The mirror check on
///         `CapacityBond.setUnbondingPeriod` is enforced via CapacityBond's
///         `slashJudge` reference (wired post-deploy through
///         `CapacityBond.setSlashJudge`), so the paired invariant is now closed
///         on both contracts (#778).
contract SlashJudge is ISlashJudge, AccessControl, ReentrancyGuard, Pausable, EIP712 {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // EIP-712 typing (ADR 014 § EIP-712 Type Definitions)
    // -----------------------------------------------------------------

    bytes32 public constant PROBE_RESPONSE_TYPEHASH =
        keccak256("ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)");

    bytes32 public constant STREAM_RESPONSE_TYPEHASH = keccak256(
        "StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,"
        "bytes32 channelId,uint64 timestampUs,bytes32 redirect)"
    );

    // -----------------------------------------------------------------
    // Constants (ADR 014 § Governable Parameters with Safety Bounds)
    // -----------------------------------------------------------------

    /// @dev 30-second requester-anchored window for the two-message offenses.
    uint64 internal constant SLASH_WINDOW_US = 30_000_000;
    /// @dev Fixed NTP-drift tolerance; not governable (ADR 014).
    uint64 internal constant MAX_FUTURE_SKEW_US = 60_000_000;

    uint256 internal constant MAX_EVIDENCE_AGE_FLOOR_US = 1 days * 1_000_000;
    uint256 internal constant MAX_EVIDENCE_AGE_CEILING_US = 30 days * 1_000_000;

    uint256 internal constant CHALLENGE_BOND_FLOOR = 1e18;
    uint256 internal constant CHALLENGE_BOND_CEILING = 1000e18;

    /// @dev Global blacklist scope key (ADR 014 § Blacklist violation). Regional
    ///      and ripening-prev-region scope is now enforced too (ADR 030
    ///      § Region-stability window) — see `_checkBlacklistedBefore`.
    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    /// @dev Commit–reveal anti-front-running bounds (ADR 014 § Challenge
    ///      front-running mitigation, #854). A reveal (`submit*Challenge`) is
    ///      valid only once its commitment has aged `MIN_REVEAL_DELAY` and before
    ///      it expires at `REVEAL_WINDOW`. The 1-minute maturation is negligible
    ///      against the ≥1-day evidence-age window, so it never stales otherwise
    ///      fresh evidence; `REVEAL_WINDOW` bounds how long a commitment stays
    ///      valid (the normal path reveals well within it). Both are fixed (not
    ///      governable), matching the `MAX_FUTURE_SKEW_US` precedent.
    uint256 internal constant MIN_REVEAL_DELAY = 1 minutes;
    uint256 internal constant REVEAL_WINDOW = 1 days;

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondSlasher public immutable capacityBond;

    /// @dev Same deployed `CapacityBond` as `capacityBond`, typed for the ADR 030
    ///      region-scope read surface. A second narrow immutable (rather than
    ///      widening `ICapacityBondSlasher`, which the SlashJudge mock shares).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondRegionView public immutable capacityBondRegion;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable token;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IContentBlacklistHashView public immutable contentBlacklist;

    uint256 public challengeBond;
    uint256 public maxEvidenceAgeUs;

    /// @notice Set once an `evidenceHash` has resulted in a slash, so the exact
    ///         same signed proof can never be replayed to ratchet an operator's
    ///         `lifetimeOffenseCount`. `CapacityBond.slash` has no evidence-level
    ///         dedup, so this guard lives here.
    mapping(bytes32 => bool) public usedEvidenceHash;

    /// @notice Commit timestamp (unix seconds) for each commit–reveal commitment
    ///         `keccak256(abi.encode(evidenceHash, salt, challenger))`, set by
    ///         `commitChallenge` and cleared on the matching reveal. `0` means "no
    ///         commitment"; a non-zero value older than `REVEAL_WINDOW` is an
    ///         expired commitment that `commitChallenge` may overwrite. Binding the
    ///         challenger into the preimage is what makes the reward un-stealable by
    ///         a mempool copy of the reveal (#854).
    mapping(bytes32 => uint64) public commitments;

    // -----------------------------------------------------------------
    // Decoded evidence structs (ABI layout of the `*ResponseData` args)
    // -----------------------------------------------------------------

    struct ProbeMsg {
        bytes32 hash;
        bool hasBlob;
        uint64 ratePerMb;
        uint64 timestampUs;
    }

    struct StreamMsg {
        bytes32 hash;
        bool ok;
        uint64 ratePerMb;
        uint64 totalBytes;
        bytes32 channelId;
        uint64 timestampUs;
        bytes32 redirect;
    }

    // -----------------------------------------------------------------
    // Events + errors
    // -----------------------------------------------------------------

    event ChallengeBondUpdated(uint256 oldValue, uint256 newValue);
    event MaxEvidenceAgeUpdated(uint256 oldValueUs, uint256 newValueUs);
    /// @notice A commit–reveal commitment was registered (#854). The preimage
    ///         (evidence, salt, challenger) is intentionally not revealed here.
    event ChallengeCommitted(bytes32 indexed commitment);

    error ZeroAddress();
    error NodeNotRegistered(address challengedNode);
    error NodeIdMismatch(bytes32 provided, bytes32 bound);
    error InvalidProbeSignature();
    error InvalidStreamSignature();
    error InvalidResponseSignature();
    error HashMismatch(bytes32 expected, bytes32 actual);
    error NotPhantom();
    error NotRateManipulation();
    error NotBlacklistViolation();
    error TimestampWindowViolated(uint64 probeTsUs, uint64 streamTsUs);
    error EvidenceInFuture(uint64 evidenceTsUs);
    error EvidenceTooOld(uint256 ageUs, uint256 maxAgeUs);
    error HashNotBlacklisted(bytes32 hash);
    error BlacklistAfterResponse(uint256 addedAtUs, uint64 responseTsUs);
    error EvidenceAlreadyUsed(bytes32 evidenceHash);
    error CommitmentExists();
    error NoCommitment();
    error RevealTooEarly(uint256 readyAt);
    error CommitmentExpired(uint256 expiredAt);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error EvidenceAgeExceedsUnbonding(uint256 maxEvidenceAgeUs, uint256 unbondingUs);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param capacityBond_     Operator registry / slash sink.
    /// @param token_            TOKEN used for challenge bonds.
    /// @param contentBlacklist_ Blacklist read source for blacklist challenges.
    /// @param challengeBond_    Initial challenge bond (bounded [1, 1000] TOKEN).
    /// @param maxEvidenceAgeUs_ Initial evidence-age ceiling in microseconds
    ///                          (bounded [1d, 30d]); must be `< unbondingPeriod`.
    /// @param admin             `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder.
    constructor(
        ICapacityBondSlasher capacityBond_,
        IERC20 token_,
        IContentBlacklistHashView contentBlacklist_,
        uint256 challengeBond_,
        uint256 maxEvidenceAgeUs_,
        address admin
    ) EIP712("deCDN SlashJudge", "1") {
        if (
            address(capacityBond_) == address(0) || address(token_) == address(0)
                || address(contentBlacklist_) == address(0) || admin == address(0)
        ) {
            revert ZeroAddress();
        }
        _enforceBondBounds(challengeBond_);
        _enforceEvidenceAgeBounds(maxEvidenceAgeUs_);

        capacityBond = capacityBond_;
        capacityBondRegion = ICapacityBondRegionView(address(capacityBond_));
        token = token_;
        contentBlacklist = contentBlacklist_;
        challengeBond = challengeBond_;

        // `unbondingPeriod()` is a view on the trusted immutable `capacityBond`;
        // reading it during construction cannot reenter, so the following state
        // write is not a reentrancy vector (aderyn reentrancy-state-change FP).
        // aderyn-ignore-next-line(reentrancy-state-change)
        _enforceUnbondingInvariant(maxEvidenceAgeUs_, capacityBond_.unbondingPeriod());
        maxEvidenceAgeUs = maxEvidenceAgeUs_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Challenge submission (ADR 014 § Evidence Verification Per Offense Type)
    // -----------------------------------------------------------------

    /// @inheritdoc ISlashJudge
    function commitChallenge(bytes32 commitment) external override whenNotPaused {
        uint64 existing = commitments[commitment];
        // A still-live commitment may not be overwritten; an expired one (or an
        // empty slot, `existing == 0`) may be (re)committed. This keeps a
        // challenger who went offline or lost a blind race from being permanently
        // wedged on a `(salt, evidence)` pair — including across a pause longer
        // than `REVEAL_WINDOW` — since they can re-commit the same salt once it
        // expires (#854). `uint64(block.timestamp)` is safe past year ~2554.
        if (existing != 0) {
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp <= uint256(existing) + REVEAL_WINDOW) revert CommitmentExists();
        }
        commitments[commitment] = uint64(block.timestamp);
        emit ChallengeCommitted(commitment);
    }

    /// @inheritdoc ISlashJudge
    function submitPhantomChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig,
        bytes32 salt
    ) external override nonReentrant whenNotPaused {
        _checkRegistered(challengedNode, nodeId);
        bytes32 evidenceHash = _verifyPair(
            challengedNode, probeResponseData, probeSlashSig, streamResponseData, streamSlashSig, OffenseType.Phantom
        );
        _resolve(challengedNode, OffenseType.Phantom, evidenceHash, salt);
    }

    /// @inheritdoc ISlashJudge
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig,
        bytes32 salt
    ) external override nonReentrant whenNotPaused {
        _checkRegistered(challengedNode, nodeId);
        bytes32 evidenceHash = _verifyPair(
            challengedNode,
            probeResponseData,
            probeSlashSig,
            streamResponseData,
            streamSlashSig,
            OffenseType.RateManipulation
        );
        _resolve(challengedNode, OffenseType.RateManipulation, evidenceHash, salt);
    }

    /// @inheritdoc ISlashJudge
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,
        bytes calldata slashSig,
        bool isStreamResponse,
        bytes32 salt
    ) external override nonReentrant whenNotPaused {
        _checkRegistered(challengedNode, nodeId);

        bytes32 responseHash;
        bytes32 structHash;
        uint64 responseTsUs;
        bool servedClaim;
        if (isStreamResponse) {
            StreamMsg memory s = abi.decode(responseData, (StreamMsg));
            structHash = _streamStructHash(s);
            responseHash = s.hash;
            responseTsUs = s.timestampUs;
            servedClaim = s.ok;
        } else {
            ProbeMsg memory p = abi.decode(responseData, (ProbeMsg));
            structHash = _probeStructHash(p);
            responseHash = p.hash;
            responseTsUs = p.timestampUs;
            servedClaim = p.hasBlob;
        }

        if (!SignatureChecker.isValidSignatureNow(challengedNode, _hashTypedDataV4(structHash), slashSig)) {
            revert InvalidResponseSignature();
        }
        if (responseHash != blobHash) revert HashMismatch(blobHash, responseHash);
        if (!servedClaim) revert NotBlacklistViolation();
        _checkStaleness(responseTsUs);
        _checkBlacklistedBefore(challengedNode, blobHash, responseTsUs);

        bytes32 evidenceHash = keccak256(abi.encode(uint8(OffenseType.Blacklist), structHash, isStreamResponse));
        _resolve(challengedNode, OffenseType.Blacklist, evidenceHash, salt);
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE — Timelock post-deploy)
    // -----------------------------------------------------------------

    function setChallengeBond(uint256 newBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceBondBounds(newBond);
        uint256 old = challengeBond;
        challengeBond = newBond;
        emit ChallengeBondUpdated(old, newBond);
    }

    /// @notice Update the evidence-age ceiling. Enforces the individual [1d, 30d]
    ///         bound AND the paired `< CapacityBond.unbondingPeriod` invariant
    ///         (ADR 014 § Interaction with unbonding period).
    function setMaxEvidenceAge(uint256 newValueUs) external onlyRole(GOVERNANCE_ROLE) {
        _enforceEvidenceAgeBounds(newValueUs);
        // `unbondingPeriod()` is a view on the trusted immutable `capacityBond` and
        // this setter is GOVERNANCE_ROLE-gated, so the following state write is not
        // a reentrancy vector (aderyn reentrancy-state-change FP).
        // aderyn-ignore-next-line(reentrancy-state-change)
        _enforceUnbondingInvariant(newValueUs, capacityBond.unbondingPeriod());
        uint256 old = maxEvidenceAgeUs;
        maxEvidenceAgeUs = newValueUs;
        emit MaxEvidenceAgeUpdated(old, newValueUs);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal — verification
    // -----------------------------------------------------------------

    /// @dev Shared phantom/rate path: decode + verify both signatures, confirm
    ///      same-hash, the 30s window, and evidence freshness, then apply the
    ///      offense-specific predicate and return the `evidenceHash`. Registration
    ///      is checked by the caller; the predicate and hashing fold in here
    ///      (rather than returning the decoded messages) so the reveal entry
    ///      points stay within the stack limit once `salt` rides along (via_ir is
    ///      off).
    function _verifyPair(
        address challengedNode,
        bytes calldata probeData,
        bytes calldata probeSig,
        bytes calldata streamData,
        bytes calldata streamSig,
        OffenseType offense
    ) internal view returns (bytes32 evidenceHash) {
        ProbeMsg memory p = abi.decode(probeData, (ProbeMsg));
        StreamMsg memory s = abi.decode(streamData, (StreamMsg));
        bytes32 probeHash = _probeStructHash(p);
        bytes32 streamHash = _streamStructHash(s);

        if (!SignatureChecker.isValidSignatureNow(challengedNode, _hashTypedDataV4(probeHash), probeSig)) {
            revert InvalidProbeSignature();
        }
        if (!SignatureChecker.isValidSignatureNow(challengedNode, _hashTypedDataV4(streamHash), streamSig)) {
            revert InvalidStreamSignature();
        }

        if (p.hash != s.hash) revert HashMismatch(p.hash, s.hash);
        // 30s requester-anchored window: stream at/after probe, delta < 30s.
        if (s.timestampUs < p.timestampUs || s.timestampUs - p.timestampUs >= SLASH_WINDOW_US) {
            revert TimestampWindowViolated(p.timestampUs, s.timestampUs);
        }
        _checkStaleness(p.timestampUs);

        if (offense == OffenseType.Phantom) {
            // Phantom = announced the blob then failed to deliver it.
            if (!p.hasBlob || s.ok) revert NotPhantom();
        } else {
            // Rate manipulation = charged a higher stream rate than was probe-quoted.
            if (s.ratePerMb <= p.ratePerMb) revert NotRateManipulation();
        }

        evidenceHash = keccak256(abi.encode(uint8(offense), probeHash, streamHash));
    }

    function _probeStructHash(ProbeMsg memory p) internal pure returns (bytes32) {
        return keccak256(abi.encode(PROBE_RESPONSE_TYPEHASH, p.hash, p.hasBlob, p.ratePerMb, p.timestampUs));
    }

    function _streamStructHash(StreamMsg memory s) internal pure returns (bytes32) {
        return keccak256(
            abi.encode(
                STREAM_RESPONSE_TYPEHASH,
                s.hash,
                s.ok,
                s.ratePerMb,
                s.totalBytes,
                s.channelId,
                s.timestampUs,
                s.redirect
            )
        );
    }

    /// @dev Confirm the challenged address is a registered operator and that the
    ///      challenger-supplied `nodeId` matches its on-chain binding. The binding
    ///      survives deregistration, so a deregistered-but-bonded offender stays
    ///      slashable. The `active` flag is intentionally unused (ADR 014 — the
    ///      `nodeId != 0` binding, not liveness, is the registration gate).
    // slither-disable-next-line unused-return
    function _checkRegistered(address challengedNode, bytes32 nodeId) internal view {
        (bytes32 bound,) = capacityBond.nodeIdOf(challengedNode);
        if (bound == bytes32(0)) revert NodeNotRegistered(challengedNode);
        if (bound != nodeId) revert NodeIdMismatch(nodeId, bound);
    }

    /// @dev Confirm `blobHash` was an enforceable blacklist entry IN SCOPE for
    ///      `operator` (ADR 030 § Region-stability window: global ∪ current-region
    ///      ∪ ripening-prev-region) strictly before the served response. A
    ///      suspended entry (fast-tracked appeal) lifts the serving restriction,
    ///      so it is not slashable — matching `ContentBlacklist._isLive`
    ///      (`addedAt != 0 && !suspended`). The GLOBAL leg preserves the original
    ///      two-error semantics (the richer `BlacklistAfterResponse` when a global
    ///      entry exists but post-dates the response and no regional leg rescues
    ///      it); the regional legs fold their before-response check into a boolean.
    function _checkBlacklistedBefore(address operator, bytes32 blobHash, uint64 responseTsUs) internal view {
        (uint64 gAddedAt, bool gSuspended) = contentBlacklist.getHashEntry(GLOBAL_REGION, blobHash);
        if (gAddedAt != 0 && !gSuspended) {
            // Effective-since (seconds → μs) must precede the served response.
            uint256 gAddedAtUs = uint256(gAddedAt) * 1_000_000;
            if (gAddedAtUs < uint256(responseTsUs)) return; // slashable under global scope
            // Global entry exists but post-dates the response: only blockable if
            // no regional leg independently makes the node slashable.
            if (!_regionalLiveBefore(operator, blobHash, responseTsUs)) {
                revert BlacklistAfterResponse(gAddedAtUs, responseTsUs);
            }
            return;
        }
        // No enforceable global entry — fall back to the ADR 030 regional legs.
        if (!_regionalLiveBefore(operator, blobHash, responseTsUs)) revert HashNotBlacklisted(blobHash);
    }

    /// @dev ADR 030 regional scope: true iff `blobHash` is live-before-response in
    ///      the operator's current region, OR (still inside the ripening window)
    ///      its previous region. Region keys are read from `CapacityBond` and
    ///      packed to `bytes32` to match `ContentBlacklist`'s region keying.
    function _regionalLiveBefore(address operator, bytes32 blobHash, uint64 responseTsUs) private view returns (bool) {
        (
            string memory regionHint,
            string memory regionPrev,
            uint64 regionLastChanged,
            uint64 firstBondedAt,
            uint64 gateActivatedAt,
            uint256 window
        ) = capacityBondRegion.regionScopeData(operator);

        uint64 effective = RegionScopeLib.effectiveSince(regionLastChanged, firstBondedAt, gateActivatedAt);
        // forge-lint: disable-next-line(block-timestamp)
        (bytes32 cur, bytes32 prev, bool prevApplies) = RegionScopeLib.scopedRegions(
            GLOBAL_REGION, regionHint, regionPrev, uint64(block.timestamp), effective, window
        );

        if (cur != bytes32(0) && _liveBefore(cur, blobHash, responseTsUs)) return true;
        if (prevApplies && _liveBefore(prev, blobHash, responseTsUs)) return true;
        return false;
    }

    /// @dev True iff `(region, blobHash)` is a live entry (`addedAt != 0 &&
    ///      !suspended`) whose effective-since (seconds → μs) strictly precedes
    ///      the served response.
    function _liveBefore(bytes32 region, bytes32 blobHash, uint64 responseTsUs) private view returns (bool) {
        (uint64 addedAt, bool suspended) = contentBlacklist.getHashEntry(region, blobHash);
        // Positive form (matches `ContentBlacklist._isLive`): an entry is live iff
        // present (`addedAt != 0`) and not fast-track-suspended.
        return addedAt != 0 && !suspended && uint256(addedAt) * 1_000_000 < uint256(responseTsUs);
    }

    /// @dev Skew-safe evidence-age check (ADR 014 § Evidence staleness).
    function _checkStaleness(uint64 evidenceTsUs) internal view {
        uint256 nowUs = block.timestamp * 1_000_000;
        if (uint256(evidenceTsUs) > nowUs + MAX_FUTURE_SKEW_US) revert EvidenceInFuture(evidenceTsUs);
        uint256 ageUs = uint256(evidenceTsUs) >= nowUs ? 0 : nowUs - uint256(evidenceTsUs);
        if (ageUs >= maxEvidenceAgeUs) revert EvidenceTooOld(ageUs, maxEvidenceAgeUs);
    }

    /// @dev Reveal: reconstruct the caller's commitment, enforce the reveal
    ///      window, then consume the evidence (replay guard), pull the challenge
    ///      bond, slash (records `msg.sender` as the challenger for the 50%
    ///      finality leg), return the bond, and emit `Slashed`. The commitment
    ///      `keccak256(evidenceHash, salt, msg.sender)` binds the challenger, so a
    ///      mempool copy of this reveal (different `msg.sender`) finds no
    ///      commitment and cannot claim the reward (#854).
    function _resolve(address operator, OffenseType offenseType, bytes32 evidenceHash, bytes32 salt) internal {
        bytes32 commitment = keccak256(abi.encode(evidenceHash, salt, msg.sender));
        uint64 committedAt = commitments[commitment];
        // `0` is the unset sentinel for `commitments`; strict equality is correct.
        // slither-disable-next-line incorrect-equality
        if (committedAt == 0) revert NoCommitment();
        uint256 readyAt = uint256(committedAt) + MIN_REVEAL_DELAY;
        uint256 expiresAt = uint256(committedAt) + REVEAL_WINDOW;
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert RevealTooEarly(readyAt);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > expiresAt) revert CommitmentExpired(expiresAt);
        delete commitments[commitment];

        if (usedEvidenceHash[evidenceHash]) revert EvidenceAlreadyUsed(evidenceHash);
        usedEvidenceHash[evidenceHash] = true;

        uint256 bond = challengeBond;
        token.safeTransferFrom(msg.sender, address(this), bond);

        (uint256 slashId, uint256 amount) = capacityBond.slash(operator, msg.sender, uint8(offenseType));

        token.safeTransfer(msg.sender, bond);

        emit Slashed(slashId, operator, offenseType, amount, evidenceHash);
    }

    // -----------------------------------------------------------------
    // Internal — bounds
    // -----------------------------------------------------------------

    function _enforceBondBounds(uint256 bond) internal pure {
        if (bond < CHALLENGE_BOND_FLOOR || bond > CHALLENGE_BOND_CEILING) {
            revert ParamOutOfBounds(bond, CHALLENGE_BOND_FLOOR, CHALLENGE_BOND_CEILING);
        }
    }

    function _enforceEvidenceAgeBounds(uint256 ageUs) internal pure {
        if (ageUs < MAX_EVIDENCE_AGE_FLOOR_US || ageUs > MAX_EVIDENCE_AGE_CEILING_US) {
            revert ParamOutOfBounds(ageUs, MAX_EVIDENCE_AGE_FLOOR_US, MAX_EVIDENCE_AGE_CEILING_US);
        }
    }

    function _enforceUnbondingInvariant(uint256 ageUs, uint256 unbondingSeconds) internal pure {
        uint256 unbondingUs = unbondingSeconds * 1_000_000;
        if (ageUs >= unbondingUs) revert EvidenceAgeExceedsUnbonding(ageUs, unbondingUs);
    }
}
