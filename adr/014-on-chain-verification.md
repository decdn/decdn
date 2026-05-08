# ADR 014: On-Chain Verification for Slashing Evidence

**Date:** 2026-04-03
**Status:** Draft

## Context

Four slashable offenses require on-chain evidence verification ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)):

1. **Corrupted delivery** — node serves bytes that fail BLAKE3 hash verification
2. **Phantom announcement** — node signs `has_blob: true` then cannot deliver
3. **Rate manipulation** — node advertises one rate in probe, charges higher in stream
4. **Blacklist violation** — node serves a blacklisted hash after the compliance window ([ADR 011](011-content-takedown.md))

A fifth offense, **double settlement** (submitting the same voucher to multiple channels), is production-only and is not covered by on-chain verification in the PoC.

Three of these (phantom, rate, blacklist) require verifying cryptographic signatures from protocol messages. The fourth (corruption) requires adjudicating whether delivered bytes match the claimed BLAKE3 hash. EVM's native `ecrecover` handles secp256k1 (ECDSA) cheaply (~3,000 gas), but BLAKE3 mismatch adjudication has no EVM opcode and submitting full blob data on-chain is gas-prohibitive for any non-trivial blob size. The PoC therefore uses an optimistic bond + counter-evidence scheme for corruption rather than cryptographic mismatch proof; a production Merkle proof design is specified but deferred.

This ADR specifies concrete on-chain mechanisms for both: `ecrecover`-based signature verification for the three signature-dependent offenses, and an optimistic challenge-response for corruption — enabling all four slash evidence paths for the PoC.

## Decision

### 1. Slash Signatures — secp256k1 EIP-712

#### Approach

Each protocol message that participates in slashing (`ProbeResponse`, `StreamResponse`) carries a single message-body signature, `slash_sig`, produced with the node's Ethereum key. Connection-level peer identity is authenticated separately by the iroh QUIC handshake against the registered Ed25519 NodeId; the body signature exists to make message contents portable evidence verifiable both off-chain and on-chain.

The Ethereum key is the same secp256k1 key the node already holds for staking and channel operations: `StakingRegistry.registerNode` ([ADR 001](001-network.md)) atomically binds the operator's Ethereum address to the Ed25519 NodeId, so `ecrecover` on a `slash_sig` followed by a `StakingRegistry.nodeIdOf(recovered)` lookup attributes the message to a specific NodeId. EVM-native verification costs ~3,000 gas, making routine slashing economically viable; an Ed25519 wire signature would have cost ~500k–1M gas via Solidity library and was rejected for this reason.

##### Wire protocol

`slash_sig` is mandatory and non-empty on every `ProbeResponse` and `StreamResponse`. Requesters MUST reject responses with missing or zero-length `slash_sig`. There is no opt-out: every interaction in the paid delivery path is on-chain slashable. The fields covered by `slash_sig` are the same fields that drive the slashing mechanisms in [ADR 005](005-protocol.md):

```
ProbeResponse {has_blob, rate_per_mb, timestamp_us, total_bytes?, slash_sig}
StreamResponse {ok, rate_per_mb, total_bytes, timestamp_us, redirect?, error?, voucher_interval_mb?, slash_sig}
```

- **ProbeResponse slash_sig covers:** `{hash, has_blob, rate_per_mb, timestamp_us}`
- **StreamResponse slash_sig covers:** `{hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}`

`hash` and `channel_id` are request-context fields (from `ProbeRequest` and `StreamRequest` respectively), not transmitted in the response body — implementers must include them when building and verifying the EIP-712 typed data. When `redirect` is absent (the common case), it is encoded as `bytes32(0)`. The signed-field set is the v1 baseline; per [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing), any subsequent change is a Tier 3 ALPN bump.

#### EIP-712 Type Definitions

```solidity
bytes32 constant PROBE_RESPONSE_TYPEHASH = keccak256(
    "ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)"
);

bytes32 constant STREAM_RESPONSE_TYPEHASH = keccak256(
    "StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 channelId,uint64 timestampUs,bytes32 redirect)"
);
```

The `SlashJudge` contract uses its own EIP-712 domain separator, not shared with `StakingRegistry` or `StablePaymentChannel`. This prevents cross-contract signature replay.

```solidity
EIP712Domain({
    name: "deCDN SlashJudge",
    version: "1",
    chainId: <deployment chain>,
    verifyingContract: <SlashJudge address>
})
```

#### On-Chain Verification Flow

1. Challenger submits the serialized message fields and `slash_sig` to `SlashJudge`.
2. The contract reconstructs the EIP-712 typed data hash and calls `SignatureChecker.isValidSignatureNow(challengedNode, hash, slash_sig)` — **~3,000 gas** for EOA nodes, **~15,000 gas** for Safe-based nodes ([ADR 024](024-account-abstraction.md)).
3. The challenger-provided address is looked up in `StakingRegistry` to confirm it maps to a registered node.
4. For offenses requiring two messages (phantom, rate manipulation), the signatures must both validate against the **same** node address.

#### Node Implementation

When constructing a `ProbeResponse` or `StreamResponse`, the node signs the security-relevant fields with its Ethereum private key using EIP-712 typed data and emits the result as `slash_sig`. Peer authentication is handled separately by the iroh QUIC handshake (against the registered Ed25519 NodeId); `slash_sig` is purely a message-body attribution and evidence artifact and is never used for connection establishment.

#### Alternatives Considered

The signature-scheme alternatives table (RIP-7212, Solidity library, ZK, optimistic) is recorded in [`_history/alternatives-pre-launch.md` § ADR 014 — Slash Signature Scheme](_history/alternatives-pre-launch.md#adr-014--slash-signature-scheme).

> **Note:** [ADR 001](001-network.md#nodeid-ownership-verification) uses direct ed25519 verification (Solidity library, ~500k–1M gas) for node registration ownership proof. This is acceptable because registration is a one-time cost per node lifetime, unlike slash evidence which may be submitted frequently.

### 2. BLAKE3 Content Corruption — Optimistic Challenge-Response

#### Problem

When a client detects a BLAKE3 hash mismatch on received bytes, it needs an on-chain path to slash the delivering node. BLAKE3 is not an EVM precompile, so the contract cannot independently verify the mismatch. Submitting full blob data on-chain is gas-prohibitive.

#### Single-Round Optimistic Challenge

The corruption slash path uses a single-round optimistic model. The challenger's signed `StreamResponse` proves the node committed to serving the blob; the bond prevents frivolous claims.

**Challenge submission:**

The challenger calls `SlashJudge.submitCorruptionChallenge()` with:

- `nodeId` — the node's Ed25519 public key (for identification)
- `blobHash` — the BLAKE3 hash of the content that was requested
- `streamResponse` — the serialized `StreamResponse` fields (with `ok: true` for the challenged blob)
- `slashSig` — the secp256k1 `slash_sig` from the `StreamResponse`
- Challenge bond: 100 TOKEN ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn))

The contract verifies:

1. `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, slashSig)` verifies the signature against the provided node address, which must be registered
2. The `StreamResponse` has `ok: true` and its `hash` field matches `blobHash`
3. The challenge bond is transferred and held

**Counter-evidence window: 24 hours.**

The challenged node may call `SlashJudge.counterChallenge(challengeId, evidence)` within 24 hours. Valid counter-evidence is a requester-signed `DeliveryReceipt` from the same requester, proving the node delivered correct bytes for the same blob. `StreamEnd` itself is an unsigned wire message and cannot serve as on-chain evidence.

##### DeliveryReceipt

After a stream completes and the requester's local BLAKE3 verification passes, the requester produces an EIP-712 signed receipt over:

```solidity
bytes32 constant DELIVERY_RECEIPT_TYPEHASH = keccak256(
    "DeliveryReceipt(address requester,bytes32 nodeId,bytes32 channelId,bytes32 blobHash,uint64 deliveredAtUs)"
);
```

- `requester` — the requester's Ethereum address
- `nodeId` — the delivering node's registered identity
- `channelId` — the payment channel used for this stream
- `blobHash` — the BLAKE3 hash of the blob that was delivered and verified
- `deliveredAtUs` — requester-generated microsecond timestamp of delivery completion

The receipt uses the `SlashJudge` EIP-712 domain (same domain separator as slash signatures). The requester signs this only after successful BLAKE3 verification of the full blob. On counter-challenge, the contract verifies the requester's signature and checks that `nodeId`, `channelId`, and `blobHash` match the challenged delivery. **Critically, the contract MUST verify `receipt.requester == challenge.challenger`** (the address that called the original `submit*Challenge` function, stored in the challenge struct). This prevents cross-requester receipt reuse: without this check, a node could serve corrupt data to client A, obtain a valid receipt from client B (to whom it served correct data for the same blob), and use B's receipt to dismiss A's challenge.

**Incentive to sign:** Nodes SHOULD request a `DeliveryReceipt` after successful delivery. A requester that refuses to sign after accepting delivery cannot later submit a corruption challenge for the same blob and channel (the contract checks for contradictory receipts). Nodes MAY deprioritize or refuse future streams to requesters that consistently refuse receipts.

**Resolution:**

- If the node does not counter within 24 hours: `resolveChallenge()` slashes the node per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) and returns the bond to the challenger.
- If the node counters successfully: the challenge is dismissed, and the bond is forfeited (50% burned, 50% to the node per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)).

**Why single-round:** The 100 TOKEN bond makes frivolous challenges expensive (>> the cost of a legitimate slash). A node that actually served corrupt data has no valid counter-evidence to produce. The simplicity of a single-round model keeps contract complexity and audit surface bounded.

#### Future evolution

The cryptographic upgrade path — interactive keccak256 Merkle proofs over 1024-byte chunks — is specified in [ADR 030](030-blake3-merkle-verification.md). The Merkle path coexists with the optimistic path: v1 deliveries (`cdn/client/v1`) keep using `submitCorruptionChallenge` per this section; v2 deliveries (`cdn/client/v2`) use `submitMerkleCorruptionChallenge` per ADR 030. Both feed the unified `OffenseType.Corruption` and the [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) lifetime escalation counter — no new offense type is introduced.

### 3. SlashJudge Contract

A unified contract that adjudicates all four slashable offense types. The contract holds challenge bonds, verifies evidence, manages counter-evidence windows, and calls `StakingRegistry.slash()` on resolution.

#### Interface

##### Encoding convention

The `bytes calldata` arguments named `*ResponseData` in the interface below are **ABI-encoded structs** matching the EIP-712 typed data fields (not postcard wire bytes). The contract ABI-decodes these fields, reconstructs the EIP-712 struct hash, and verifies using `SignatureChecker.isValidSignatureNow` ([ADR 024](024-account-abstraction.md)). This ensures a single canonical encoding for both the contract and off-chain signature construction.

```solidity
interface ISlashJudge {
    /// Offense identifier emitted on every slash. Order is contract-canonical
    /// and append-only — new offense types extend the enum at the end so existing
    /// `slashId` allocations remain stable.
    enum OffenseType { Corruption, Phantom, RateManipulation, Blacklist, ReceiptFraud }

    /// Emitted on every slash resolution that results in a stake reduction.
    /// `slashId` is globally monotonic across all four offense types.
    /// `evidenceHash` is keccak256 over the per-offense canonical preimage, prefixed
    /// by uint8(offenseType) so overlapping-evidence offenses produce distinct hashes
    /// (see "`Slashed` event and `slashId` allocation" sub-section below for the exact
    /// abi.encode(...) per offense type). Signatures are excluded — the typed-data
    /// digests they sign uniquely determine the evidence already.
    /// Consumed by `SafetyReserve.openSlashAppeal(slashId, evidenceBundleHash)` per ADR 028 §6.
    event Slashed(
        uint256 indexed slashId,
        address indexed operator,
        OffenseType offenseType,
        uint256 amount,
        bytes32 evidenceHash
    );

    /// Phantom announcement: node signed has_blob=true then ok=false within 30s.
    /// Emits `Slashed` synchronously on successful verification (no counter-evidence window).
    function submitPhantomChallenge(
        address challengedNode,              // Ethereum address or Safe address of the challenged node
        bytes32 nodeId,
        bytes calldata probeResponseData,   // serialized {hash, has_blob, rate_per_mb, timestamp_us}
        bytes calldata probeSlashSig,        // EIP-712 signature (EOA or ERC-1271)
        bytes calldata streamResponseData,  // serialized {hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}
        bytes calldata streamSlashSig        // EIP-712 signature (EOA or ERC-1271)
    ) external;

    /// Rate manipulation: stream rate > probe rate within 30s window (immediate).
    /// Emits `Slashed` synchronously on successful verification.
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external;

    /// Blacklist violation: serving a blacklisted hash after compliance window.
    /// Emits `Slashed` synchronously on successful verification.
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,   // ProbeResponse (has_blob=true) or StreamResponse (ok=true)
        bytes calldata slashSig,
        bool isStreamResponse          // false = ProbeResponse evidence, true = StreamResponse evidence
    ) external;

    /// Corrupted delivery (v1 / optimistic path): node served bytes failing
    /// BLAKE3 verification on a `cdn/client/v1` delivery. Opens a 24h
    /// counter-evidence window; `Slashed` is emitted from `resolveChallenge`
    /// only on a slash outcome (countered/dismissed challenges do NOT emit).
    /// The cryptographic v2 path lives in `submitMerkleCorruptionChallenge`
    /// per [ADR 030](030-blake3-merkle-verification.md).
    function submitCorruptionChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external;

    /// Corrupted delivery (v2 / Merkle-bisection path): opens an interactive
    /// dispute against a `cdn/client/v2` delivery whose signed `StreamResponse`
    /// includes the keccak256 MMR `merkleRoot` per [ADR 030](030-blake3-merkle-verification.md).
    /// The dispute progresses via `bisectMove` (cheap rounds) and concludes via
    /// `terminalReveal` + `resolveMerkleCorruption` (terminal on-chain BLAKE3
    /// chunk verification). No counter-evidence window — resolution is
    /// cryptographically dispositive. Feeds the unified `OffenseType.Corruption`;
    /// the v1 and v2 entry points share `evidenceHash` preimage rules per §3
    /// (the v2 struct hash differs from v1 by inclusion of `merkleRoot`).
    function submitMerkleCorruptionChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata streamResponseV2Data,
        bytes calldata streamSlashSig
    ) external returns (uint256 disputeId);

    /// v2 bisection step. See ADR 030 §3 for the state machine.
    function bisectMove(
        uint256 disputeId,
        bytes32 baoRootLeft,    bytes32 keccakRootLeft,
        bytes32 baoRootRight,   bytes32 keccakRootRight,
        bool    challengeLeft
    ) external;

    /// v2 terminal reveal at round N+1.
    function terminalReveal(
        uint256 disputeId,
        bytes  calldata chunkBytes,
        bytes32[] calldata baoParentPath,
        bytes  calldata keccakInclusionProof
    ) external;

    /// v2 resolution after terminal reveal or chess-clock timeout.
    function resolveMerkleCorruption(uint256 disputeId) external;

    /// v2 voluntary concede.
    function forfeitDispute(uint256 disputeId) external;

    /// Receipt-summary fraud: operator's `EpochReceiptSummary` overstates
    /// `claimedBytes` or `claimedDistinctClients` relative to the on-chain
    /// MMR. Emits `Slashed` synchronously on successful verification; no
    /// counter-evidence window because the Merkle proof is cryptographically
    /// dispositive. Full evidence format and verification spec live in
    /// [ADR 027 §5](027-distinct-client-receipts.md#5-challenger-role-see-appendix-fraud-detection).
    function submitReceiptFraudChallenge(
        address challengedNode,
        uint64 epochId,
        bytes calldata fraudEvidence    // serialized Merkle inclusion proofs against the on-chain aggregateRoot
    ) external;

    /// Counter-evidence submission (24h window). Applies to Corruption only;
    /// the other four offense types (Phantom, RateManipulation, Blacklist,
    /// ReceiptFraud) resolve synchronously at submit time.
    function counterChallenge(uint256 challengeId, bytes calldata evidence) external;

    /// Resolve after counter-evidence window expires
    function resolveChallenge(uint256 challengeId) external;
}
```

##### Challenge rate limit

`SlashJudge` enforces a maximum number of concurrent active (unresolved) challenges per target node address: `maxActiveChallengesPerNode` (PoC: 10, production safety bound: [1, 50]). New `submitPhantomChallenge`, `submitRateChallenge`, `submitBlacklistChallenge`, `submitCorruptionChallenge`, `submitMerkleCorruptionChallenge` ([ADR 030](030-blake3-merkle-verification.md)), and `submitReceiptFraudChallenge` calls targeting a node at the limit MUST revert. This bounds the defender's concurrent counter-evidence response burden and prevents griefing attacks where a well-funded attacker submits many simultaneous spurious challenges to force operational disruption. The parameter is governable per [ADR 009](009-governance.md).

#### Evidence Verification Per Offense Type

**Phantom announcement:**

1. Challenger provides `challengedNode` address (the node's Ethereum address or Safe address)
2. `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSlashSig)` — must pass
3. `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSlashSig)` — must pass
4. Verify `probeResponse.has_blob == true` and `streamResponse.ok == false`
5. Verify `probeResponse.hash == streamResponse.hash` (same blob)
6. Verify `streamResponse.timestamp_us >= probeResponse.timestamp_us`
7. Verify `streamResponse.timestamp_us - probeResponse.timestamp_us < 30_000_000` (30-second window)
8. Look up `challengedNode` in `StakingRegistry` — must be a registered node

##### Evidence staleness

All challenge types MUST validate evidence age using a skew-safe comparison. Let `nowUs = block.timestamp * 1_000_000` and `evidence.timestamp_us` be the earliest `timestamp_us` from the submitted evidence messages (e.g., `probeResponse.timestamp_us` for phantom/rate challenges, `streamResponse.timestamp_us` for corruption/blacklist challenges that lack a probe). The contract MUST first require `evidence.timestamp_us <= nowUs + MAX_FUTURE_SKEW_US` (rejects far-future timestamps), then compute age without underflow: `ageUs = evidence.timestamp_us >= nowUs ? 0 : nowUs - evidence.timestamp_us`, and finally require `ageUs < MAX_EVIDENCE_AGE_US`. `MAX_EVIDENCE_AGE_US` is a governable parameter on `SlashJudge` (PoC: 5 days = 432,000,000,000 μs; safety bounds: [1 day, 30 days]). `MAX_FUTURE_SKEW_US` is fixed at 60,000,000 μs (60 seconds).

##### Interaction with unbonding period

`MAX_EVIDENCE_AGE_US` MUST be strictly less than the `StakingRegistry.unbondingPeriod` (converted to microseconds). If evidence can be older than the unbonding period, a node could commit an offense, immediately initiate unstaking, and complete withdrawal before the evidence is submitted — avoiding the slash entirely. With current defaults (evidence age: 5 days, unbonding: 14 days per [ADR 030 §7](030-blake3-merkle-verification.md#7-interaction-with-stake-unbonding--unbondingperiod-raised-to-14-d) — raised from the prior 7-day default to cover the worst-case Merkle-bisection lifecycle), this invariant is satisfied with a 9-day margin. The safety bounds ([1 day, 30 days] for evidence age vs [3 days, 30 days] for unbonding per [ADR 009](009-governance.md)) permit governance to violate this invariant — implementations SHOULD enforce `MAX_EVIDENCE_AGE_US < unbondingPeriod` whenever `MAX_EVIDENCE_AGE_US` is configured or updated, including at initialization and in any governance-controlled reconfiguration path. ADR 030 §7 adds a stricter cross-parameter invariant for the Merkle-bisection path that implementations MUST also enforce.

**Rate manipulation:** 1–3. Same `SignatureChecker` verification and identity check as phantom
4. Verify `streamResponse.rate_per_mb > probeResponse.rate_per_mb`
5. Verify `probeResponse.hash == streamResponse.hash` (same blob)
6–8. Same timestamp and registration checks as phantom
9. Slash immediately via `StakingRegistry.slash()` — no counter-evidence window. Two signed messages from the same NodeId disagreeing about that node's own rate within 30 seconds are non-repudiable; the node's last probe-quoted rate is binding for the slashing window. Legitimate rate changes are handled by waiting out the 30-second window before serving a stream at the new rate.

**Blacklist violation:**

1. Challenger provides `challengedNode` address
2. `SignatureChecker.isValidSignatureNow(challengedNode, responseDigest, slashSig)` — must pass
3. Look up `challengedNode` in `StakingRegistry` — must be a registered node
4. Decode `hash` from the response; verify it matches `blobHash`
5. If `ProbeResponse`: verify `has_blob == true`. If `StreamResponse`: verify `ok == true`
6. Query `ContentBlacklist.getEntry(blobHash)` — must exist and `effectiveAt` must be before the response's `timestamp_us`
7. **Regional scope limitation (PoC):** [ADR 011](011-content-takedown.md#slashing) specifies that a node is only slashable for hashes blacklisted in its declared region. However, the node's region is self-reported and not stored on-chain in `StakingRegistry` for the PoC. The `SlashJudge` contract therefore cannot enforce regional scope in the PoC — all blacklist violations are treated as globally scoped. Production should add a `region` field to `NodeInfo` to enable on-chain regional filtering

**Corrupted delivery (PoC):**

1. Challenger provides `challengedNode` address
2. `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSlashSig)` — must pass
3. Look up `challengedNode` in `StakingRegistry` — must be a registered node
4. Verify `streamResponse.ok == true` and `streamResponse.hash == blobHash`
5. Store challenge; start 24-hour counter-evidence window
6. Resolution after window: slash if no valid counter-evidence; dismiss if countered

#### Bond Handling

- Challengers must `TOKEN.approve(slashJudge, bondAmount)` before calling any `submit*Challenge()` function. The contract transfers the bond on submission.
- **Successful challenge:** bond returned to challenger; node slashed via `StakingRegistry.slash()`.
- **Successful counter:** bond forfeited — 50% burned, 50% transferred to the challenged node ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)).
- **Immediate offenses** (phantom, rate manipulation, blacklist): if on-chain verification passes, the slash executes immediately (no counter-evidence window). The node's recourse is to not commit the offense; for rate changes, that means honoring the last probe-quoted rate for the 30-second slashing window before serving streams at a new rate.
- **Deferred offense** (corruption): 24-hour counter-evidence window before resolution. The node may submit a valid delivery receipt proving the bytes it served match the claimed BLAKE3 hash.

#### `Slashed` event and `slashId` allocation

Every slash that reduces operator stake emits `Slashed(slashId, operator, offenseType, amount, evidenceHash)` (see the `ISlashJudge` interface block above). The event is the canonical record of the slash and is the appeal-pinning identifier consumed by [ADR 028 §6](028-slashing-appeals.md#6-contract-surface) `openSlashAppeal(slashId, evidenceBundleHash)` — without it, three of the four ADR 028 appeal categories (phantom, rate, blacklist) cannot be filed.

- **`slashId`** is a globally monotonic `uint256` (single counter across all four offense types, not per-operator and not per-offense-type). It is allocated from a `nextSlashId` storage slot incremented inline in the same transaction as the `StakingRegistry.slash(...)` call. `slashId` values are stable, non-reusable, and non-zero — `slashId == 0` is reserved as the "no slash" sentinel.
- **`offenseType`** is the `OffenseType` enum from the interface above. The order `{Corruption, Phantom, RateManipulation, Blacklist, ReceiptFraud}` is contract-canonical and append-only; existing entries MUST NOT be reordered (consumer contracts — notably `SafetyReserve` — index by ordinal). New offense types extend the enum at the end.
- **`evidenceHash`** is `keccak256` over a per-offense canonical preimage that uniquely identifies the (offenseType, evidence) pair the slash relied on. The preimage uses `abi.encode(...)` (not `abi.encodePacked`) so the field encoding is unambiguous across implementers. Every preimage is prefixed by `uint8(offenseType)` so two distinct offenses against the same operator on overlapping evidence (e.g., a single `(probe, stream)` pair where `streamResponse.ok == false` AND `streamResponse.rate_per_mb > probeResponse.rate_per_mb` triggers both phantom and rate-manipulation) produce distinct `evidenceHash` values, not just distinct `slashId`s. Signatures are **excluded** from the preimage — the §1 EIP-712 typed-data digests they sign already uniquely identify the message contents, so a successful slash trivially fixes the digest set; including the variable-length signature blobs in the hash would create an ambiguity (`abi.encode` vs `abi.encodePacked` field length) without adding evidentiary content. The `blobHash` parameter passed to the immediate-execution `submit*Challenge` paths is similarly excluded — the §3 evidence-verification flow already binds it via the `responseData.hash == blobHash` check, and the EIP-712 `*Response` struct hash commits to `hash` directly. The per-offense preimages are:
  - **Phantom:** `keccak256(abi.encode(uint8(OffenseType.Phantom), probeStructHash, streamStructHash))`.
  - **Rate manipulation:** `keccak256(abi.encode(uint8(OffenseType.RateManipulation), probeStructHash, streamStructHash))`. The `OffenseType` prefix is what distinguishes this preimage from phantom on overlapping evidence.
  - **Blacklist:** `keccak256(abi.encode(uint8(OffenseType.Blacklist), responseStructHash, isStreamResponse))`. The boolean is required because it is a `submitBlacklistChallenge` parameter, not part of any `*Response` struct.
  - **Corruption:** `keccak256(abi.encode(uint8(OffenseType.Corruption), streamStructHash))`. For v1 (`cdn/client/v1`) deliveries `streamStructHash` is computed under `STREAM_RESPONSE_TYPEHASH` (this ADR §1); for v2 (`cdn/client/v2`) deliveries it is computed under `STREAM_RESPONSE_V2_TYPEHASH` per [ADR 030 §1](030-blake3-merkle-verification.md#1-wire-format-merkle_root-in-signed-streamresponse-tier-3--cdnclientv2). The `OffenseType` byte is shared between paths; `evidenceHash` is naturally distinct because the two typed-data definitions differ (v2 includes `merkleRoot`).
  - **Receipt fraud:** `keccak256(abi.encode(uint8(OffenseType.ReceiptFraud), epochId, aggregateRoot))`. `epochId` and `aggregateRoot` come from the operator's `EpochReceiptSummary` for the challenged epoch; the preimage uniquely identifies (operator, epoch) pair fraud while remaining stable across alternate fraud-evidence formats (different challengers may submit different Merkle proofs against the same `aggregateRoot`, all producing the same `evidenceHash`).

  Each `*StructHash` is the EIP-712 struct hash of the corresponding `*Response` per §1 (head-only `bytes32` — `abi.encode` adds no padding to a fixed-width 32-byte value). Appeals reference `evidenceHash` to prove they are challenging the same evidence the slash relied on; ADR 028 §6 `openSlashAppeal(slashId, evidenceBundleHash)` requires `evidenceBundleHash == evidenceHash` of the referenced `Slashed` event.
- **Emission sites:**
  - **Corruption (v1 / optimistic — `cdn/client/v1`)** — emitted from `resolveChallenge` only when the 24h counter-evidence window resolves to a slash. Countered or dismissed challenges do NOT emit (no stake reduction occurred).
  - **Corruption (v2 / Merkle-bisection — `cdn/client/v2`; per [ADR 030](030-blake3-merkle-verification.md))** — emitted from `resolveMerkleCorruption` (or from `forfeitDispute` when the defender concedes) only on the defender-loss branch. Challenger-loss branches (terminal verification proves defender's chunk correct, challenger chess-clock timeout, or challenger forfeit) do NOT emit; the bond is forfeited per the §Bond Handling challenger-loss split with no stake reduction.
  - **Phantom / rate manipulation / blacklist** — emitted from the synchronous `submit*Challenge` paths immediately after the inline `StakingRegistry.slash()` returns. The "`StakingRegistry.slash()` then `emit Slashed`" sequence is contract-enforced atomic (single transaction); a slash without a matching event is impossible.

The companion `SafetyReserve` events (`SlashAppealOpened`, `SlashAppealRatified`, etc.) remain forward-referenced to a future contract-implementation ADR per [ADR 028 §Forward references](028-slashing-appeals.md#forward-references-follow-up-adrs); only the `Slashed` event itself is canonicalised here.

#### Gas Estimates

| Operation | Estimated Gas | Notes |
| --- | --- | --- |
| `submitPhantomChallenge` | ~65k–90k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage + bond transfer + `Slashed` emit on success |
| `submitRateChallenge` | ~65k–90k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage for pending challenge + bond transfer + `Slashed` emit on success |
| `submitBlacklistChallenge` | ~55k–70k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + `ContentBlacklist` lookup + bond transfer + `Slashed` emit on success |
| `submitCorruptionChallenge` | ~50k–65k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + storage for challenge state + bond transfer (no slash yet — emit deferred to `resolveChallenge`) |
| `submitMerkleCorruptionChallenge` ([ADR 030](030-blake3-merkle-verification.md) v2) | ~80k | 1× `SignatureChecker` (v2 typehash, includes `merkleRoot`) + dispute state init + initial bond transfer (no slash; bisection follows). |
| `bisectMove` ([ADR 030](030-blake3-merkle-verification.md) v2; called ~20 times per dispute for a 1 GiB blob, alternating sides) | ~60k | One keccak parent-composition check (`keccak256(0x01 \|\| left \|\| right) == parentKeccak`), store new subtree-root pair (4× `bytes32`), advance chess-clock, transfer per-round bond. Bao subtree-root commitments stored lazily without on-chain composition; verified at `resolveMerkleCorruption`. |
| `terminalReveal` ([ADR 030](030-blake3-merkle-verification.md) v2; called once per side at round N+1) | ~80k | Hash chunk via `keccak256` + verify keccak Merkle path to `R`; store reveal. |
| `resolveMerkleCorruption` ([ADR 030](030-blake3-merkle-verification.md) v2; called once after both reveals or chess-clock timeout) | ~3–4M | Two BLAKE3 chunk hashes (~150k each) + 2N bao parent-mode hashes (~150k each at depth N) to verify both sides' bao chains compose to `X` + `StakingRegistry.slash()` on defender-loss branch + bond ledger settlement + `Slashed` emit + state cleanup. |
| `forfeitDispute` ([ADR 030](030-blake3-merkle-verification.md) v2) | ~50k–90k | Defender-forfeit branch: `StakingRegistry.slash()` + slash-reward transfer + `Slashed` emit (~85k, equivalent to a defender chess-clock timeout). Challenger-forfeit branch: bond forfeit per ADR 014 §3 split (~50k, no slash). |
| **Total per v2 dispute** (1 GiB blob, ~20 bisection rounds, both sides reveal, defender loses) | **~4.5–5.5M** | Spread across ~24 transactions over up to ~12.7 days at the [ADR 030 §3](030-blake3-merkle-verification.md#3-bisection-protocol-state-machine) default chess-clock budget. |
| `counterChallenge` (rate) | ~40k–55k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + timestamp range check + rate match + storage update |
| `counterChallenge` (corruption) | ~40k | Evidence verification + storage update |
| `resolveChallenge` | ~85k | `StakingRegistry.slash()` + bond transfer + state cleanup + `Slashed` emit on slash outcome |
| `Slashed` event emit | ~5k–7k | `nextSlashId++` (cold SLOAD + non-zero→non-zero SSTORE on first emit per tx, ~5k post-EIP-2929) + LOG3 base + 3 stack topics (event signature + 2 indexed) + 96 bytes non-indexed data (~2k); negligible vs the surrounding `StakingRegistry.slash()`. The very first `Slashed` ever emitted on a fresh deployment pays an additional ~17k for the 0→non-zero `nextSlashId` SSTORE. |

Using secp256k1 EIP-712 for `slash_sig` keeps per-signature verification at ~3k gas via `ecrecover`, against the ~500k–1M gas a Solidity Ed25519 library would require — making routine slashing economically viable.

### 4. Integration with Existing Contracts

**StakingRegistry ([ADR 001](001-network.md), [ADR 003](003-payments.md), [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)):**

- Adds `slash(address node, uint8 offenseType) external` callable only by the `SlashJudge` contract address. Implements the escalating schedule from [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) (10% flat for PoC; 5/15/50% with lifetime counter for production). Checks auto-ejection threshold (50% of `minStake`).
- No new fields in `NodeInfo` for PoC — the existing `msg.sender` Ethereum address serves as the slash key.

**StablePaymentChannel ([ADR 003](003-payments.md)):**

- No changes. Slashing and payment channels are independent by design.

**ContentBlacklist ([ADR 011](011-content-takedown.md)):**

- `SlashJudge` calls `ContentBlacklist.isBlacklisted(hash)` and `ContentBlacklist.getEntry(hash)` to verify blacklist status and compliance window timing. No changes to the `ContentBlacklist` interface.

## Consequences

**Positive:**

- All four slashable offenses now have a concrete, gas-efficient on-chain evidence path. Slashing is no longer aspirational.
- `ecrecover` at 3,000 gas per signature is 100–300× cheaper than a Solidity Ed25519 library, making routine slashing economically viable even for small offenses.
- `slash_sig` reuses the existing NodeId-to-Ethereum-address binding in `StakingRegistry` — no new on-chain registration step.
- `slash_sig` is mandatory and non-empty on every `ProbeResponse` and `StreamResponse`. Universal on-chain accountability is the protocol's single stance — there is no opt-out and no validation-mode difference between PoC and production for this field.
- The unified `SlashJudge` contract provides a single audit surface for all slashing logic.
- The PoC corruption path (single-round optimistic) is simple to implement and audit. The production upgrade path (interactive Merkle proof) is designed but deferred.

**Negative:**

- Nodes perform a secp256k1 EIP-712 signature on every `ProbeResponse` and `StreamResponse`, adding ~1ms of computation per message — negligible relative to network RTT, but nonzero.
- The `slash_sig` field adds ~65 bytes per `ProbeResponse` and `StreamResponse`. For probe messages this is meaningful overhead; for stream responses preceding multi-MB deliveries, it is negligible.
- Off-chain verifiers (clients, requesting nodes, third-party fraud detectors) must `ecrecover` and look up `StakingRegistry.nodeIdOf(recovered)` to attribute a message to a NodeId, rather than verifying directly against the iroh key. The binding cache is already maintained by these parties for voucher attribution, so the marginal cost is one extra map lookup per verification.
- The PoC corruption path relies on the bond as the primary deterrent against frivolous challenges, rather than cryptographic proof. A well-funded attacker could submit many spurious challenges (100 TOKEN each) to force nodes into counter-evidence responses. Mitigation: the bond is forfeited on failed challenges, making sustained attacks expensive.
- The production Merkle proof path requires a future mechanism to bind keccak256 Merkle roots to BLAKE3 hashes — deferred to a follow-up ADR.
- Cross-contract replay is prevented by per-contract EIP-712 domains, but implementers must ensure domain separators are correctly configured at deployment.
- The §3 `Slashed` event adds an `OffenseType` enum, a `nextSlashId` storage slot, and the per-offense `evidenceHash` preimage encoding to `SlashJudge`'s audit surface. The increment is small but real: every slash path emits the event atomically with `StakingRegistry.slash()`, and the `OffenseType` ordering is contract-canonical (any reordering requires coordinated migration of `SafetyReserve` per ADR 028 §6).

## ADRs Affected

- **[ADR 002](002-content-addressing.md):** Open question on on-chain verification mechanism → resolved (this ADR).
- **[ADR 003](003-payments.md):** Options A/B/C for corruption evidence → resolved as Option A (optimistic challenge-response).
- **[ADR 005](005-protocol.md):** Signer binding section reframed around `slash_sig` only; the prior Ed25519 message-body signature is removed and `slash_sig` becomes the sole `ProbeResponse`/`StreamResponse` body signature.
- **[ADR 011](011-content-takedown.md):** Ed25519-library assumption for slash evidence → updated to `ecrecover`-based `slash_sig` scheme (this ADR). The blacklist-removal restitution path ([ADR 011 § Slashing](011-content-takedown.md#slashing)) also references the `slashId` from the `Slashed` event (this ADR §3) to pin the original blacklist-violation slash being appealed via [ADR 028](028-slashing-appeals.md).
- **[ADR 027](027-distinct-client-receipts.md):** Extends the keccak256 Merkle-batch pattern from §2 to anchor `DeliveryReceipt` batches per operator per epoch; reuses the `Bond Handling` model for receipt-fraud challenges.
- **[ADR 028](028-slashing-appeals.md):** Consumes `slashId` from the `Slashed` event (this ADR §3) as the appeal-pinning identifier in `openSlashAppeal(slashId, evidenceBundleHash)`. Without `Slashed` emission for the three immediate-execution offenses, three of the four ADR 028 appeal categories cannot be filed.
- **[ADR 030](030-blake3-merkle-verification.md):** Replaces this ADR's §2 "Future evolution" stub with the production design. Adds the v2 entry points (`submitMerkleCorruptionChallenge` + bisection-step interface) under the unified `OffenseType.Corruption`. Raises the default `unbondingPeriod` from 7 d to 14 d to cover the worst-case bisection lifecycle; the §3 "Interaction with unbonding period" invariant continues to hold under the new default.
