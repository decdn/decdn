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
import { IContentBlacklistHashView } from "./interfaces/IContentBlacklistHashView.sol";

/// @title SlashJudge
/// @notice On-chain adjudicator for the three signature-dependent slashable
///         offenses (ADR 014): phantom announcement, rate manipulation, and
///         blacklist violation. Each `submit*Challenge` verifies secp256k1
///         EIP-712 `slash_sig` evidence with `SignatureChecker` (EOA + ERC-1271),
///         confirms the challenged address is a registered operator, enforces the
///         evidence-age window, then calls `CapacityBond.slash` and emits the
///         canonical `Slashed` event — all synchronously, with no counter-evidence
///         window. The challenger is recorded by `slash` for the 50% finality
///         reward (escrow-on-slash, ADR 026 / ADR 028); `SlashJudge` itself only
///         round-trips the challenge bond.
/// @dev    `MAX_EVIDENCE_AGE_US < CapacityBond.unbondingPeriod` is enforced on
///         this side (constructor + `setMaxEvidenceAge`). The mirror check on
///         `CapacityBond.setUnbondingPeriod` is NOT present on the deployed
///         `CapacityBond` (it predates `SlashJudge`); closing that half needs a
///         `CapacityBond` change and is tracked outside this PR.
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

    /// @dev Global blacklist scope key (ADR 014 § Blacklist violation — regional
    ///      scope is deferred for the PoC, so all blacklist violations are global).
    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondSlasher public immutable capacityBond;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable token;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IContentBlacklistHashView public immutable contentBlacklist;

    uint256 public challengeBond;
    uint256 public maxEvidenceAgeUs;

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
    error BlacklistAfterResponse(uint64 addedAtUs, uint64 responseTsUs);
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
        token = token_;
        contentBlacklist = contentBlacklist_;
        challengeBond = challengeBond_;

        _enforceUnbondingInvariant(maxEvidenceAgeUs_, capacityBond_.unbondingPeriod());
        maxEvidenceAgeUs = maxEvidenceAgeUs_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Challenge submission (ADR 014 § Evidence Verification Per Offense Type)
    // -----------------------------------------------------------------

    /// @inheritdoc ISlashJudge
    function submitPhantomChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external override nonReentrant whenNotPaused {
        (ProbeMsg memory p, StreamMsg memory s, bytes32 probeHash, bytes32 streamHash) =
            _verifyPair(challengedNode, nodeId, probeResponseData, probeSlashSig, streamResponseData, streamSlashSig);

        // Phantom = announced the blob then failed to deliver it.
        if (!p.hasBlob || s.ok) revert NotPhantom();

        bytes32 evidenceHash = keccak256(abi.encode(uint8(OffenseType.Phantom), probeHash, streamHash));
        _resolve(challengedNode, OffenseType.Phantom, evidenceHash);
    }

    /// @inheritdoc ISlashJudge
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external override nonReentrant whenNotPaused {
        (ProbeMsg memory p, StreamMsg memory s, bytes32 probeHash, bytes32 streamHash) =
            _verifyPair(challengedNode, nodeId, probeResponseData, probeSlashSig, streamResponseData, streamSlashSig);

        // Rate manipulation = charged a higher stream rate than was probe-quoted.
        if (s.ratePerMb <= p.ratePerMb) revert NotRateManipulation();

        bytes32 evidenceHash = keccak256(abi.encode(uint8(OffenseType.RateManipulation), probeHash, streamHash));
        _resolve(challengedNode, OffenseType.RateManipulation, evidenceHash);
    }

    /// @inheritdoc ISlashJudge
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,
        bytes calldata slashSig,
        bool isStreamResponse
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

        (uint64 addedAt,) = contentBlacklist.getHashEntry(GLOBAL_REGION, blobHash);
        if (addedAt == 0) revert HashNotBlacklisted(blobHash);
        // Effective-since (seconds → μs) must precede the served response.
        if (uint256(addedAt) * 1_000_000 >= uint256(responseTsUs)) {
            revert BlacklistAfterResponse(addedAt, responseTsUs);
        }

        bytes32 evidenceHash = keccak256(abi.encode(uint8(OffenseType.Blacklist), structHash, isStreamResponse));
        _resolve(challengedNode, OffenseType.Blacklist, evidenceHash);
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
    ///      registration, same-hash, the 30s window, and evidence freshness.
    ///      Returns the decoded messages and their EIP-712 struct hashes (used by
    ///      the caller for the offense-specific predicate + `evidenceHash`).
    function _verifyPair(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeData,
        bytes calldata probeSig,
        bytes calldata streamData,
        bytes calldata streamSig
    ) internal view returns (ProbeMsg memory p, StreamMsg memory s, bytes32 probeHash, bytes32 streamHash) {
        _checkRegistered(challengedNode, nodeId);

        p = abi.decode(probeData, (ProbeMsg));
        s = abi.decode(streamData, (StreamMsg));
        probeHash = _probeStructHash(p);
        streamHash = _streamStructHash(s);

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
    ///      slashable.
    function _checkRegistered(address challengedNode, bytes32 nodeId) internal view {
        (bytes32 bound,) = capacityBond.nodeIdOf(challengedNode);
        if (bound == bytes32(0)) revert NodeNotRegistered(challengedNode);
        if (bound != nodeId) revert NodeIdMismatch(nodeId, bound);
    }

    /// @dev Skew-safe evidence-age check (ADR 014 § Evidence staleness).
    function _checkStaleness(uint64 evidenceTsUs) internal view {
        uint256 nowUs = block.timestamp * 1_000_000;
        if (uint256(evidenceTsUs) > nowUs + MAX_FUTURE_SKEW_US) revert EvidenceInFuture(evidenceTsUs);
        uint256 ageUs = uint256(evidenceTsUs) >= nowUs ? 0 : nowUs - uint256(evidenceTsUs);
        if (ageUs >= maxEvidenceAgeUs) revert EvidenceTooOld(ageUs, maxEvidenceAgeUs);
    }

    /// @dev Pull the challenge bond, slash (records `msg.sender` as the challenger
    ///      for the 50% finality leg), return the bond, and emit `Slashed`.
    function _resolve(address operator, OffenseType offenseType, bytes32 evidenceHash) internal {
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
