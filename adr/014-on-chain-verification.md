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

Three of these (phantom, rate, blacklist) require verifying cryptographic signatures from protocol messages. The fourth (corruption) requires adjudicating whether delivered bytes match the claimed BLAKE3 hash. Neither Ed25519 signature verification nor BLAKE3 mismatch adjudication is natively supported on EVM:

- **Ed25519 signatures** (iroh NodeId keys, used for `ProbeResponse` and `StreamResponse` per [ADR 005](005-protocol.md)) have no EVM precompile. Solidity-based verification costs ~500k–1M gas per signature — economically unviable for routine slashing.
- **BLAKE3 hashes** have no EVM opcode. Submitting full blob data on-chain to prove a mismatch is gas-prohibitive for any non-trivial blob size. The PoC therefore uses an optimistic bond + counter-evidence scheme rather than cryptographic mismatch proof; a production Merkle proof design is specified but deferred.

This ADR specifies concrete on-chain mechanisms for both: `ecrecover`-based signature verification for the three signature-dependent offenses, and an optimistic challenge-response for corruption — enabling all four slash evidence paths for the PoC.

## Decision

### 1. Ed25519 Signature Verification — Dual-Key Slash Signatures

#### Problem

[ADR 005](005-protocol.md) defines `ProbeResponse` and `StreamResponse` signatures using the node's Ed25519 iroh key. On-chain slash evidence requires verifying these signatures, but EVM's native `ecrecover` only handles secp256k1 (ECDSA). An on-chain Ed25519 library is too expensive for routine use.

#### Approach: secp256k1 slash signatures alongside Ed25519 wire signatures

Nodes already register an Ethereum address (secp256k1-derived) alongside their Ed25519 NodeId in `StakingRegistry` ([ADR 003](003-payments.md)). This ADR leverages that existing binding: protocol messages carry a **second signature** using the node's Ethereum key, specifically for on-chain evidence.

**Wire protocol additions.** `ProbeResponse` and `StreamResponse` each carry a `slash_sig` field, mandatory in this protocol version. Any future relaxation allowing `slash_sig` to be omitted from the wire would change field requiredness and therefore requires a Tier 3 / major-version ALPN bump per [ADR 013](013-schema-evolution.md), not a Tier 1 change. Production may instead relax *validation semantics* (e.g., accept zero-length `slash_sig`) while keeping the field always present on the wire:

```
ProbeResponse {has_blob, rate_per_mb, timestamp_us, signature, total_bytes?, slash_sig}
StreamResponse {ok, rate_per_mb, total_bytes, timestamp_us, signature, redirect?, error?, voucher_interval_mb?, slash_sig}
```

`slash_sig` is an EIP-712 secp256k1 signature over the same security-relevant fields already covered by the Ed25519 `signature`:

- **ProbeResponse slash_sig covers:** `{hash, has_blob, rate_per_mb, timestamp_us}`
- **StreamResponse slash_sig covers:** `{hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}`

These match the Ed25519-signed field sets defined in [ADR 005](005-protocol.md). Note that `hash` and `channel_id` are request-context fields (from `ProbeRequest` and `StreamRequest` respectively), not transmitted in the response body — implementers must include them when building and verifying the EIP-712 typed data. When `redirect` is absent (the common case), it is encoded as `bytes32(0)`. In the PoC, `slash_sig` is mandatory — all nodes must include it, and requesters MUST reject responses that omit it. Production may relax validation semantics (e.g., accepting zero-length `slash_sig` to indicate opt-out) while keeping the field present on the wire for postcard compatibility. Requesters cannot submit on-chain slash evidence for messages with empty `slash_sig`; those cases fall back to reputation penalties, consistent with the existing timeout/non-response handling in [ADR 003](003-payments.md). Fully removing `slash_sig` from the wire format would require a Tier 3 / ALPN bump per [ADR 013](013-schema-evolution.md).

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

When constructing a `ProbeResponse` or `StreamResponse`, the node:

1. Signs the security-relevant fields with its Ed25519 iroh key (existing behavior, used for wire authentication).
2. Signs the same fields with its Ethereum private key using EIP-712 typed data (new behavior, used for on-chain evidence).
3. Includes both signatures in the response.

The Ed25519 signature remains the primary authentication mechanism for the QUIC connection. The secp256k1 `slash_sig` is solely an evidence artifact — it is never used for peer authentication or session establishment.

#### Alternatives Considered

The signature-scheme alternatives table (RIP-7212, Solidity library, ZK, optimistic) is recorded in [`_history/alternatives-pre-launch.md` § ADR 014 — Slash Signature Scheme](_history/alternatives-pre-launch.md#adr-014--slash-signature-scheme).

> **Note:** [ADR 001](001-network.md#nodeid-ownership-verification) uses direct ed25519 verification (Solidity library, ~500k–1M gas) for node registration ownership proof. This is acceptable because registration is a one-time cost per node lifetime, unlike slash evidence which may be submitted frequently.

> **PoC vs production:** The PoC makes `slash_sig` mandatory on all `ProbeResponse` and `StreamResponse` messages ([ADR 005](005-protocol.md)), ensuring universal on-chain accountability. Production may relax validation semantics (accepting zero-length `slash_sig` as an opt-out) while keeping the field on the wire for postcard compatibility — fully removing the field would require a Tier 3 / ALPN bump per [ADR 013](013-schema-evolution.md). Requesters SHOULD be able to require a non-empty `slash_sig` as a precondition for proceeding with a stream — nodes that refuse receive a reputation penalty ([ADR 008](008-reputation.md)), creating economic pressure toward inclusion without a hard protocol requirement.

### 2. BLAKE3 Content Corruption — Optimistic Challenge-Response

#### Problem

When a client detects a BLAKE3 hash mismatch on received bytes, it needs an on-chain path to slash the delivering node. BLAKE3 is not an EVM precompile, so the contract cannot independently verify the mismatch. Submitting full blob data on-chain is gas-prohibitive.

#### PoC Approach: Single-Round Optimistic Challenge

For the PoC, the corruption slash path uses a single-round optimistic model. The challenger's signed `StreamResponse` proves the node committed to serving the blob; the bond prevents frivolous claims.

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

**DeliveryReceipt.** After a stream completes and the requester's local BLAKE3 verification passes, the requester produces an EIP-712 signed receipt over:

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

**Why single-round for PoC:** The 100 TOKEN bond makes frivolous challenges expensive (100 TOKEN PoC testnet bond >> the cost of a legitimate slash). A node that actually served corrupt data has no valid counter-evidence to produce. The simplicity of a single-round model reduces contract complexity and audit surface for the PoC.

#### Production Path: Interactive keccak256 Merkle Proof (Future ADR)

For production, the corruption slash path should upgrade to a two-round interactive protocol with cryptographic verification:

1. **Challenger submits** a keccak256 Merkle commitment of the received data, the index of the corrupt chunk, and the corrupt chunk bytes with a Merkle proof.
2. **Contract verifies** the Merkle proof on-chain (cheap: keccak256 is a native EVM opcode at 30 gas + 6 gas per 32-byte word).
3. **Node responds** (24 hours) with the correct chunk at the same index and a Merkle proof against the correct keccak256 Merkle root for the BLAKE3-addressed blob.

**Merkle tree construction:**

- A blob of `N` bytes is divided into `ceil(N / 1024)` chunks of 1024 bytes (last chunk may be shorter). The 1024-byte leaf size matches the iroh-blobs BLAKE3 hash tree leaf size ([architecture.md glossary](architecture.md#glossary)).
- Each leaf is `keccak256(chunk_index || chunk_bytes)` — including the index prevents second-preimage attacks.
- Internal nodes are `keccak256(left || right)`. Standard binary Merkle tree, left-padded with zero-hashes for non-power-of-2 leaf counts.
- A 1 GB blob has ~1M chunks, producing a tree of depth 20. A proof is 20 hashes (640 bytes). On-chain verification: ~20 keccak256 calls ≈ 840 gas for hashing + calldata costs. Total well under 100k gas.

**Binding Merkle root to BLAKE3 hash:** The contract cannot independently verify that a keccak256 Merkle root corresponds to a given BLAKE3 hash (BLAKE3 is not available on-chain). The production protocol addresses this by requiring the node to commit a keccak256 Merkle root for each blob it serves, either at delivery time (included in `StreamResponse` as an additional field) or registered on-chain. This commitment is the subject of a future ADR.

### 3. SlashJudge Contract

A unified contract that adjudicates all four slashable offense types. The contract holds challenge bonds, verifies evidence, manages counter-evidence windows, and calls `StakingRegistry.slash()` on resolution.

#### Interface

**Encoding convention.** The `bytes calldata` arguments named `*ResponseData` in the interface below are **ABI-encoded structs** matching the EIP-712 typed data fields (not postcard wire bytes). The contract ABI-decodes these fields, reconstructs the EIP-712 struct hash, and verifies using `SignatureChecker.isValidSignatureNow` ([ADR 024](024-account-abstraction.md)). This ensures a single canonical encoding for both the contract and off-chain signature construction.

```solidity
interface ISlashJudge {
    /// Phantom announcement: node signed has_blob=true then ok=false within 30s
    function submitPhantomChallenge(
        address challengedNode,              // Ethereum address or Safe address of the challenged node
        bytes32 nodeId,
        bytes calldata probeResponseData,   // serialized {hash, has_blob, rate_per_mb, timestamp_us}
        bytes calldata probeSlashSig,        // EIP-712 signature (EOA or ERC-1271)
        bytes calldata streamResponseData,  // serialized {hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}
        bytes calldata streamSlashSig        // EIP-712 signature (EOA or ERC-1271)
    ) external;

    /// Rate manipulation: stream rate > probe rate within 30s window (immediate)
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external;

    /// Blacklist violation: serving a blacklisted hash after compliance window
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,   // ProbeResponse (has_blob=true) or StreamResponse (ok=true)
        bytes calldata slashSig,
        bool isStreamResponse          // false = ProbeResponse evidence, true = StreamResponse evidence
    ) external;

    /// Corrupted delivery: node served bytes failing BLAKE3 verification
    function submitCorruptionChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external;

    /// Counter-evidence submission (24h window)
    function counterChallenge(uint256 challengeId, bytes calldata evidence) external;

    /// Resolve after counter-evidence window expires
    function resolveChallenge(uint256 challengeId) external;
}
```

**Challenge rate limit.** `SlashJudge` enforces a maximum number of concurrent active (unresolved) challenges per target node address: `maxActiveChallengesPerNode` (PoC: 10, production safety bound: [1, 50]). New `submitPhantomChallenge`, `submitRateChallenge`, `submitBlacklistChallenge`, and `submitCorruptionChallenge` calls targeting a node at the limit MUST revert. This bounds the defender's concurrent counter-evidence response burden and prevents griefing attacks where a well-funded attacker submits many simultaneous spurious challenges to force operational disruption. The parameter is governable per [ADR 009](009-governance.md).

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

**Evidence staleness.** All challenge types MUST validate evidence age using a skew-safe comparison. Let `nowUs = block.timestamp * 1_000_000` and `evidence.timestamp_us` be the earliest `timestamp_us` from the submitted evidence messages (e.g., `probeResponse.timestamp_us` for phantom/rate challenges, `streamResponse.timestamp_us` for corruption/blacklist challenges that lack a probe). The contract MUST first require `evidence.timestamp_us <= nowUs + MAX_FUTURE_SKEW_US` (rejects far-future timestamps), then compute age without underflow: `ageUs = evidence.timestamp_us >= nowUs ? 0 : nowUs - evidence.timestamp_us`, and finally require `ageUs < MAX_EVIDENCE_AGE_US`. `MAX_EVIDENCE_AGE_US` is a governable parameter on `SlashJudge` (PoC: 5 days = 432,000,000,000 μs; safety bounds: [1 day, 30 days]). `MAX_FUTURE_SKEW_US` is fixed at 60,000,000 μs (60 seconds).

**Interaction with unbonding period.** `MAX_EVIDENCE_AGE_US` MUST be strictly less than the `StakingRegistry.unbondingPeriod` (converted to microseconds). If evidence can be older than the unbonding period, a node could commit an offense, immediately initiate unstaking, and complete withdrawal before the evidence is submitted — avoiding the slash entirely. With PoC defaults (evidence age: 5 days, unbonding: 7 days), this invariant is satisfied with a 2-day margin. The safety bounds ([1 day, 30 days] for evidence age vs [3 days, 30 days] for unbonding per [ADR 009](009-governance.md)) permit governance to violate this invariant — implementations SHOULD enforce `MAX_EVIDENCE_AGE_US < unbondingPeriod` whenever `MAX_EVIDENCE_AGE_US` is configured or updated, including at initialization and in any governance-controlled reconfiguration path.

**Rate manipulation:**
1–3. Same `SignatureChecker` verification and identity check as phantom
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

#### Gas Estimates

| Operation | Estimated Gas | Notes |
| --- | --- | --- |
| `submitPhantomChallenge` | ~60k–85k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage + bond transfer |
| `submitRateChallenge` | ~60k–85k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage for pending challenge + bond transfer |
| `submitBlacklistChallenge` | ~50k–65k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + `ContentBlacklist` lookup + bond transfer |
| `submitCorruptionChallenge` | ~50k–65k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + storage for challenge state + bond transfer |
| `counterChallenge` (rate) | ~40k–55k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + timestamp range check + rate match + storage update |
| `counterChallenge` (corruption) | ~40k | Evidence verification + storage update |
| `resolveChallenge` | ~80k | `StakingRegistry.slash()` + bond transfer + state cleanup |

The dual-key approach reduces per-signature verification from ~500k (Ed25519 library) to ~3k (`ecrecover`), making routine slashing economically viable.

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
- The dual-key approach reuses the existing NodeId-to-Ethereum-address binding in `StakingRegistry` — no new on-chain registration step.
- In the PoC, `slash_sig` is mandatory, ensuring all delivery interactions are on-chain slashable. Production may relax validation semantics (accepting zero-length `slash_sig` as opt-out) while keeping the field on the wire; messages with empty `slash_sig` fall back to reputation penalties. Fully removing the field requires a Tier 3 / ALPN bump.
- The unified `SlashJudge` contract provides a single audit surface for all slashing logic.
- The PoC corruption path (single-round optimistic) is simple to implement and audit. The production upgrade path (interactive Merkle proof) is designed but deferred.

**Negative:**

- Nodes must perform two signatures per protocol message (Ed25519 + secp256k1). The secp256k1 signature adds ~1ms of computation per message — negligible relative to network RTT, but nonzero.
- The `slash_sig` field adds ~65 bytes per `ProbeResponse` and `StreamResponse`. For probe messages this is a ~50% size increase; for stream responses preceding multi-MB deliveries, it is negligible.
- The PoC corruption path relies on the bond as the primary deterrent against frivolous challenges, rather than cryptographic proof. A well-funded attacker could submit many spurious challenges (100 TOKEN each) to force nodes into counter-evidence responses. Mitigation: the bond is forfeited on failed challenges, making sustained attacks expensive.
- The production Merkle proof path requires a future mechanism to bind keccak256 Merkle roots to BLAKE3 hashes — deferred to a follow-up ADR.
- Cross-contract replay is prevented by per-contract EIP-712 domains, but implementers must ensure domain separators are correctly configured at deployment.

## ADRs Affected

- **[ADR 002](002-content-addressing.md):** Open question on on-chain verification mechanism → resolved (this ADR).
- **[ADR 003](003-payments.md):** Options A/B/C for corruption evidence → resolved as Option A (optimistic challenge-response).
- **[ADR 005](005-protocol.md):** Signer binding section updated to reference dual signatures; `slash_sig` field added to `ProbeResponse` and `StreamResponse`.
- **[ADR 011](011-content-takedown.md):** Ed25519-library assumption for slash evidence → updated to dual-key `ecrecover` scheme (this ADR).
- **[ADR 027](027-distinct-client-receipts.md):** Extends the keccak256 Merkle-batch pattern from §2 to anchor `DeliveryReceipt` batches per operator per epoch; reuses the `Bond Handling` model for receipt-fraud challenges.
