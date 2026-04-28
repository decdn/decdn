// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import {
    ReentrancyGuardTransient
} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

import { IStakingRegistry } from "./interfaces/IStakingRegistry.sol";
import { IContentBlacklist } from "./interfaces/IContentBlacklist.sol";
import { IStablePaymentChannel } from "./interfaces/IStablePaymentChannel.sol";
import { IBurnable } from "./interfaces/IBurnable.sol";
import { Errors } from "./libraries/Errors.sol";

/// @title SlashJudge
/// @notice Four-way challenge arbitration for deCDN operator misbehavior.
/// @dev See ADR 014 (optimistic PoC challenge-response path) and ADR 016 §3.
///      Implements phantom-blob, rate-manipulation, blacklist-violation, and
///      corruption challenges with a counter/delay window governable in
///      [12h, 72h] (PoC default 24h, set by `Deploy.s.sol`).
///      Interactive Merkle proofs for corruption are deferred to production.
///
///      All challenger and counter-evidence signatures use `SignatureChecker`
///      (ADR 024), so ERC-4337 / ERC-1271 smart accounts are supported.
contract SlashJudge is AccessControl, ReentrancyGuardTransient, Pausable, EIP712 {
    using SafeERC20 for IERC20;
    using SafeCast for uint256;

    // ---------------------------------------------------------------------
    //  Constants
    // ---------------------------------------------------------------------

    uint256 public constant CHALLENGE_BOND_FLOOR = 1e18;
    uint256 public constant CHALLENGE_BOND_CEILING = 1000e18;
    uint64 public constant COUNTER_WINDOW_FLOOR = 12 hours;
    uint64 public constant COUNTER_WINDOW_CEILING = 72 hours;
    uint64 public constant MAX_EVIDENCE_AGE = 5 days;

    /// @dev Frozen — append-only. Changing any field breaks every
    ///      previously-signed response in the wild. A schema bump requires
    ///      a new typehash name (e.g. `ProbeResponseV2`) and a v2 verifier.
    bytes32 public constant PROBE_RESPONSE_TYPEHASH =
        keccak256("ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestamp)");
    /// @dev Frozen — append-only; bump name on any schema change.
    bytes32 public constant STREAM_RESPONSE_TYPEHASH = keccak256(
        "StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 channelId,uint64 timestamp)"
    );
    /// @dev Frozen — append-only; bump name on any schema change.
    bytes32 public constant RATE_CHANGE_TYPEHASH = keccak256(
        "RateChange(bytes32 nodeId,uint64 oldRatePerMb,uint64 newRatePerMb,uint64 effectiveAt)"
    );
    /// @dev Frozen — append-only; bump name on any schema change.
    bytes32 public constant DELIVERY_RECEIPT_TYPEHASH = keccak256(
        "DeliveryReceipt(address requester,bytes32 nodeId,bytes32 channelId,bytes32 blobHash,uint64 deliveredAt)"
    );

    // ---------------------------------------------------------------------
    //  Types
    // ---------------------------------------------------------------------

    enum State {
        None,
        Active,
        Countered,
        Resolved
    }

    struct Challenge {
        address challenger;
        address node;
        IStakingRegistry.OffenseType offense;
        State state;
        uint256 bond;
        uint64 submittedAt;
        uint64 counterWindowExpiresAt;
        /// @dev NodeId registered to `node` at submit time. Counter-evidence
        /// signatures must verify against this value so an operator who
        /// controls multiple nodeIds cannot cross-reuse a RateChange/
        /// DeliveryReceipt from one node to counter a challenge against
        /// another.
        bytes32 nodeIdBinding;
        /// @dev Offense-specific binding slot A. Meaning:
        ///  - RateManipulation: keccak256(abi.encode(probeRate, streamRate))
        ///  - Corruption:       streamResp.channelId
        ///  - Phantom / Blacklist: unused (non-counterable, left zero)
        bytes32 evidenceA;
        /// @dev Offense-specific binding slot B. Meaning:
        ///  - RateManipulation: bytes32(uint256(streamResp.timestamp))
        ///  - Corruption:       streamResp.hash (blob hash)
        ///  - Phantom / Blacklist: unused (left zero)
        bytes32 evidenceB;
    }

    struct ProbeResponse {
        bytes32 hash;
        bool hasBlob;
        uint64 ratePerMb;
        uint64 timestamp;
    }

    struct StreamResponse {
        bytes32 hash;
        bool ok;
        uint64 ratePerMb;
        uint64 totalBytes;
        bytes32 channelId;
        uint64 timestamp;
    }

    // ---------------------------------------------------------------------
    //  Storage
    // ---------------------------------------------------------------------

    IStakingRegistry public immutable STAKING_REGISTRY;
    IContentBlacklist public immutable CONTENT_BLACKLIST;
    IERC20 public immutable TOKEN_CONTRACT;
    IStablePaymentChannel public immutable PAYMENT_CHANNEL;

    uint256 public challengeBond;
    uint64 public counterWindow;

    uint256 public nextChallengeId;
    mapping(uint256 id => Challenge) internal _challenges;
    /// @dev Per-(node, challenger) uniqueness — a single challenger may
    /// only hold ONE active challenge against a given node at a time. This
    /// replaces a fixed per-node cap (griefable via sybil self-fill) with a
    /// bond-cost-proportional throttle: sybil-flooding would require one
    /// bonded EOA per slot, and each bond is at risk if the accused
    /// counter-challenges successfully.
    mapping(address node => mapping(address challenger => bool)) internal _hasActive;
    mapping(address node => uint256) public activeChallengeCount;

    // ---------------------------------------------------------------------
    //  Events
    // ---------------------------------------------------------------------

    event ChallengeSubmitted(
        uint256 indexed id,
        address indexed challenger,
        address indexed node,
        IStakingRegistry.OffenseType offense,
        uint64 counterWindowExpiresAt
    );
    event ChallengeCountered(uint256 indexed id, address indexed by);
    event ChallengeResolved(uint256 indexed id, uint256 slashReward);
    event ChallengeBondUpdated(uint256 oldValue, uint256 newValue);
    event CounterWindowUpdated(uint64 oldValue, uint64 newValue);

    // ---------------------------------------------------------------------
    //  Errors
    // ---------------------------------------------------------------------

    error UnknownChallenge();
    error NotActive();
    error NotAccused();
    error CounterWindowClosed();
    error CounterWindowOpen();
    error EvidenceTooOld();
    error EvidenceInTheFuture();
    error OffenseNotCounterable();
    error InvalidChallengePair();
    error HashNotBlacklisted();
    error DuplicateActiveChallenge();
    error SelfChallenge();
    error EvidenceMismatch();
    error NodeIdMismatch();
    error NodeNotRegistered();
    error NotChannelClient();

    // ---------------------------------------------------------------------
    //  Constructor
    // ---------------------------------------------------------------------

    constructor(
        IStakingRegistry stakingRegistry,
        IContentBlacklist contentBlacklist,
        IStablePaymentChannel paymentChannel,
        IERC20 tokenContract,
        address admin,
        uint256 initialBond,
        uint64 initialCounterWindow
    ) EIP712("SlashJudge", "1") {
        if (
            address(stakingRegistry) == address(0) || address(contentBlacklist) == address(0)
                || address(paymentChannel) == address(0) || address(tokenContract) == address(0)
                || admin == address(0)
        ) revert Errors.ZeroAddress();
        if (initialBond < CHALLENGE_BOND_FLOOR || initialBond > CHALLENGE_BOND_CEILING) {
            revert Errors.OutOfBounds();
        }
        if (
            initialCounterWindow < COUNTER_WINDOW_FLOOR
                || initialCounterWindow > COUNTER_WINDOW_CEILING
        ) {
            revert Errors.OutOfBounds();
        }

        STAKING_REGISTRY = stakingRegistry;
        CONTENT_BLACKLIST = contentBlacklist;
        PAYMENT_CHANNEL = paymentChannel;
        TOKEN_CONTRACT = tokenContract;
        challengeBond = initialBond;
        counterWindow = initialCounterWindow;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
    }

    // ---------------------------------------------------------------------
    //  Submit: phantom blob (ADR 014)
    // ---------------------------------------------------------------------

    function submitPhantomChallenge(
        address node,
        ProbeResponse calldata probe,
        bytes calldata probeSig,
        StreamResponse calldata streamResp,
        bytes calldata streamSig
    ) external nonReentrant whenNotPaused returns (uint256 id) {
        if (probe.hash != streamResp.hash) revert InvalidChallengePair();
        if (!probe.hasBlob || streamResp.ok) revert InvalidChallengePair();

        _verifyProbe(node, probe, probeSig);
        _verifyStream(node, streamResp, streamSig);
        _requireFresh(probe.timestamp);
        _requireFresh(streamResp.timestamp);

        // Phantom / Blacklist are non-counterable — no evidence binding
        // fields needed. Leave evidence slots zero.
        return _openChallenge(node, IStakingRegistry.OffenseType.PhantomBlob, 0, 0);
    }

    // ---------------------------------------------------------------------
    //  Submit: rate manipulation (ADR 014)
    // ---------------------------------------------------------------------

    function submitRateChallenge(
        address node,
        ProbeResponse calldata probe,
        bytes calldata probeSig,
        StreamResponse calldata streamResp,
        bytes calldata streamSig
    ) external nonReentrant whenNotPaused returns (uint256 id) {
        if (probe.hash != streamResp.hash) revert InvalidChallengePair();
        if (streamResp.ratePerMb <= probe.ratePerMb) revert InvalidChallengePair();

        _verifyProbe(node, probe, probeSig);
        _verifyStream(node, streamResp, streamSig);
        _requireFresh(probe.timestamp);
        _requireFresh(streamResp.timestamp);

        // Evidence binding: lock in (probeRate, streamRate) and streamTimestamp
        // so `counterRateChallenge` must present a RateChange that matches
        // this exact dispute.
        bytes32 evA = keccak256(abi.encode(probe.ratePerMb, streamResp.ratePerMb));
        bytes32 evB = bytes32(uint256(streamResp.timestamp));
        return _openChallenge(node, IStakingRegistry.OffenseType.RateManipulation, evA, evB);
    }

    // ---------------------------------------------------------------------
    //  Submit: blacklist violation (ADR 014)
    // ---------------------------------------------------------------------

    function submitBlacklistChallenge(
        address node,
        ProbeResponse calldata probe,
        bytes calldata probeSig
    ) external nonReentrant whenNotPaused returns (uint256 id) {
        if (!probe.hasBlob) revert InvalidChallengePair();
        _verifyProbe(node, probe, probeSig);
        _requireFresh(probe.timestamp);

        // Walk the hash's interval history to determine whether the probe
        // timestamp fell within any blacklisted range. Preserves the
        // ability to slash for evidence from a previous blacklisting even
        // after the hash has been removed and/or re-added.
        if (!CONTENT_BLACKLIST.wasBlacklistedAt(probe.hash, probe.timestamp)) {
            revert HashNotBlacklisted();
        }

        return _openChallenge(node, IStakingRegistry.OffenseType.BlacklistViolation, 0, 0);
    }

    // ---------------------------------------------------------------------
    //  Submit: corruption (ADR 014 — optimistic PoC path)
    // ---------------------------------------------------------------------

    /// @dev Restricted to the channel's client because the counter flow
    ///      validates a `DeliveryReceipt` signed by `c.challenger`. If an
    ///      arbitrary third party could submit, the accused node would have
    ///      no defense — the actual requester's receipt wouldn't match
    ///      `c.challenger`. Gating submission to the channel client closes
    ///      that gap while keeping the flow permissionless within the
    ///      legitimate client population.
    function submitCorruptionChallenge(
        address node,
        StreamResponse calldata streamResp,
        bytes calldata streamSig
    ) external nonReentrant whenNotPaused returns (uint256 id) {
        if (!streamResp.ok) revert InvalidChallengePair();
        if (PAYMENT_CHANNEL.channelClient(streamResp.channelId) != msg.sender) {
            revert NotChannelClient();
        }
        _verifyStream(node, streamResp, streamSig);
        _requireFresh(streamResp.timestamp);

        // Evidence binding: capture the exact (channelId, blobHash) pair the
        // challenger claims was corrupted. `counterCorruptionChallenge` must
        // then present a DeliveryReceipt for these specific identifiers.
        return _openChallenge(
            node, IStakingRegistry.OffenseType.Corruption, streamResp.channelId, streamResp.hash
        );
    }

    // ---------------------------------------------------------------------
    //  Counter (rate manipulation & corruption only)
    // ---------------------------------------------------------------------

    /// @notice Rate-manipulation counter: accused node signs a `RateChange`
    /// proving the rate increase took effect before the stream response.
    /// @dev The caller must pass the (oldRate, newRate) pair and the
    /// streamTimestamp that framed the original dispute. Both are
    /// verified against the evidence snapshot captured at submit time, so
    /// the counter cannot succeed with an unrelated RateChange.
    function counterRateChallenge(
        uint256 id,
        uint64 oldRatePerMb,
        uint64 newRatePerMb,
        uint64 streamTimestamp,
        uint64 effectiveAt,
        bytes calldata signature
    ) external nonReentrant whenNotPaused {
        Challenge storage c = _requireActive(id);
        if (c.offense != IStakingRegistry.OffenseType.RateManipulation) {
            revert OffenseNotCounterable();
        }
        if (msg.sender != c.node) revert NotAccused();
        if (block.timestamp >= c.counterWindowExpiresAt) revert CounterWindowClosed();

        // Bind counter to the specific dispute: (oldRate, newRate) and the
        // streamTimestamp must match what the challenger captured at submit.
        if (keccak256(abi.encode(oldRatePerMb, newRatePerMb)) != c.evidenceA) {
            revert EvidenceMismatch();
        }
        if (bytes32(uint256(streamTimestamp)) != c.evidenceB) revert EvidenceMismatch();
        // The rate change must take effect strictly before (or at) the
        // moment the higher rate was billed — otherwise the counter
        // doesn't actually justify the stream rate.
        if (effectiveAt > streamTimestamp) revert EvidenceMismatch();

        // RateChange must be signed for the NodeId captured at submit —
        // not some OTHER nodeId controlled by the same operator.
        bytes32 structHash = keccak256(
            abi.encode(
                RATE_CHANGE_TYPEHASH, c.nodeIdBinding, oldRatePerMb, newRatePerMb, effectiveAt
            )
        );
        if (!SignatureChecker.isValidSignatureNow(c.node, _hashTypedDataV4(structHash), signature))
        {
            revert Errors.InvalidSignature();
        }
        _resolveCountered(id, c);
    }

    /// @notice Corruption counter: accused node presents a delivery receipt
    /// signed by the original challenger (the requester) affirming good delivery.
    /// @dev `channelId` and `blobHash` come from the challenge's evidence
    /// snapshot, so the counter cannot succeed with a receipt for a
    /// different delivery.
    function counterCorruptionChallenge(
        uint256 id,
        uint64 deliveredAt,
        bytes calldata receiptSignature
    ) external nonReentrant whenNotPaused {
        Challenge storage c = _requireActive(id);
        if (c.offense != IStakingRegistry.OffenseType.Corruption) revert OffenseNotCounterable();
        if (msg.sender != c.node) revert NotAccused();
        if (block.timestamp >= c.counterWindowExpiresAt) revert CounterWindowClosed();

        bytes32 channelId = c.evidenceA;
        bytes32 blobHash = c.evidenceB;

        // DeliveryReceipt is signed by the original challenger, not the node,
        // and must reference the EXACT (channelId, blobHash, nodeId) tuple
        // that framed the challenge.
        bytes32 structHash = keccak256(
            abi.encode(
                DELIVERY_RECEIPT_TYPEHASH,
                c.challenger,
                c.nodeIdBinding,
                channelId,
                blobHash,
                deliveredAt
            )
        );
        if (!SignatureChecker.isValidSignatureNow(
                c.challenger, _hashTypedDataV4(structHash), receiptSignature
            )) {
            revert Errors.InvalidSignature();
        }
        _resolveCountered(id, c);
    }

    // ---------------------------------------------------------------------
    //  Resolve
    // ---------------------------------------------------------------------

    function resolveChallenge(
        uint256 id
    ) external nonReentrant whenNotPaused {
        Challenge storage c = _requireActive(id);
        if (block.timestamp < c.counterWindowExpiresAt) revert CounterWindowOpen();

        address challenger = c.challenger;
        address node = c.node;
        IStakingRegistry.OffenseType offense = c.offense;
        uint256 bond = c.bond;

        c.state = State.Resolved;
        _hasActive[node][challenger] = false;
        activeChallengeCount[node] -= 1;

        // Registry sends 50% of `slashed` to this contract (msg.sender) and
        // burns the other 50%. Using the return value avoids the fragile
        // balanceOf-delta pattern that would fold in any stray TOKEN
        // someone transferred directly to the judge.
        uint256 slashed = STAKING_REGISTRY.slash(node, offense);
        uint256 slashReward = slashed / 2;

        emit ChallengeResolved(id, slashReward);

        // Forward slash reward + return bond to original challenger.
        uint256 payout = slashReward + bond;
        if (payout > 0) TOKEN_CONTRACT.safeTransfer(challenger, payout);
    }

    // ---------------------------------------------------------------------
    //  Governance
    // ---------------------------------------------------------------------

    function setChallengeBond(
        uint256 newBond
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newBond < CHALLENGE_BOND_FLOOR || newBond > CHALLENGE_BOND_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit ChallengeBondUpdated(challengeBond, newBond);
        challengeBond = newBond;
    }

    function setCounterWindow(
        uint64 newWindow
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newWindow < COUNTER_WINDOW_FLOOR || newWindow > COUNTER_WINDOW_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit CounterWindowUpdated(counterWindow, newWindow);
        counterWindow = newWindow;
    }

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }

    // ---------------------------------------------------------------------
    //  Views
    // ---------------------------------------------------------------------

    function getChallenge(
        uint256 id
    ) external view returns (Challenge memory) {
        return _challenges[id];
    }

    function hasActiveChallenge(
        address node,
        address challenger
    ) external view returns (bool) {
        return _hasActive[node][challenger];
    }

    function domainSeparator() external view returns (bytes32) {
        return _domainSeparatorV4();
    }

    // ---------------------------------------------------------------------
    //  Internals
    // ---------------------------------------------------------------------

    function _openChallenge(
        address node,
        IStakingRegistry.OffenseType offense,
        bytes32 evidenceA,
        bytes32 evidenceB
    ) internal returns (uint256 id) {
        if (msg.sender == node) revert SelfChallenge();
        if (_hasActive[node][msg.sender]) revert DuplicateActiveChallenge();

        bytes32 nodeId = STAKING_REGISTRY.nodeIdOf(node);
        if (nodeId == bytes32(0)) revert NodeNotRegistered();

        uint256 bond = challengeBond;
        uint64 expires = block.timestamp.toUint64() + counterWindow;

        id = ++nextChallengeId;
        _challenges[id] = Challenge({
            challenger: msg.sender,
            node: node,
            offense: offense,
            state: State.Active,
            bond: bond,
            submittedAt: block.timestamp.toUint64(),
            counterWindowExpiresAt: expires,
            nodeIdBinding: nodeId,
            evidenceA: evidenceA,
            evidenceB: evidenceB
        });
        _hasActive[node][msg.sender] = true;
        activeChallengeCount[node] += 1;

        emit ChallengeSubmitted(id, msg.sender, node, offense, expires);
        TOKEN_CONTRACT.safeTransferFrom(msg.sender, address(this), bond);
    }

    function _resolveCountered(
        uint256 id,
        Challenge storage c
    ) internal {
        address node = c.node;
        address challenger = c.challenger;
        uint256 bond = c.bond;
        c.state = State.Countered;
        _hasActive[node][challenger] = false;
        activeChallengeCount[node] -= 1;

        // Bond forfeit: 50% burn, 50% to accused node.
        uint256 toNode = bond / 2;
        uint256 toBurn = bond - toNode;

        emit ChallengeCountered(id, msg.sender);

        if (toNode > 0) TOKEN_CONTRACT.safeTransfer(node, toNode);
        if (toBurn > 0) IBurnable(address(TOKEN_CONTRACT)).burn(toBurn);
    }

    function _requireActive(
        uint256 id
    ) internal view returns (Challenge storage c) {
        c = _challenges[id];
        if (c.state == State.None) revert UnknownChallenge();
        if (c.state != State.Active) revert NotActive();
    }

    function _requireFresh(
        uint64 ts
    ) internal view {
        if (ts > block.timestamp) revert EvidenceInTheFuture();
        if (block.timestamp - ts > MAX_EVIDENCE_AGE) revert EvidenceTooOld();
    }

    function _verifyProbe(
        address node,
        ProbeResponse calldata probe,
        bytes calldata sig
    ) internal view {
        bytes32 structHash = keccak256(
            abi.encode(
                PROBE_RESPONSE_TYPEHASH, probe.hash, probe.hasBlob, probe.ratePerMb, probe.timestamp
            )
        );
        if (!SignatureChecker.isValidSignatureNow(node, _hashTypedDataV4(structHash), sig)) {
            revert Errors.InvalidSignature();
        }
    }

    function _verifyStream(
        address node,
        StreamResponse calldata sr,
        bytes calldata sig
    ) internal view {
        bytes32 structHash = keccak256(
            abi.encode(
                STREAM_RESPONSE_TYPEHASH,
                sr.hash,
                sr.ok,
                sr.ratePerMb,
                sr.totalBytes,
                sr.channelId,
                sr.timestamp
            )
        );
        if (!SignatureChecker.isValidSignatureNow(node, _hashTypedDataV4(structHash), sig)) {
            revert Errors.InvalidSignature();
        }
    }
}
