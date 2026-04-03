# ADR 014: On-Chain Verification for Slashing Evidence

**Date:** 2026-04-03
**Status:** Draft

## Context

Four slashable offenses require on-chain evidence verification ([ADR 004](004-tokenomics.md#slash-amounts-escalating)):

1. **Corrupted delivery** — node serves bytes that fail BLAKE3 hash verification
2. **Phantom announcement** — node signs `has_blob: true` then cannot deliver
3. **Rate manipulation** — node advertises one rate in probe, charges higher in stream
4. **Blacklist violation** — node serves a blacklisted hash after the compliance window ([ADR 011](011-content-takedown.md))

Three of these (phantom, rate, blacklist) require verifying cryptographic signatures from protocol messages. The fourth (corruption) requires adjudicating whether delivered bytes match the claimed BLAKE3 hash. Neither Ed25519 signature verification nor BLAKE3 mismatch adjudication is natively supported on EVM:

- **Ed25519 signatures** (iroh NodeId keys, used for `ProbeResponse` and `StreamResponse` per [ADR 005](005-protocol.md)) have no EVM precompile. Solidity-based verification costs ~500k–1M gas per signature — economically unviable for routine slashing on Arbitrum.
- **BLAKE3 hashes** have no EVM opcode. Submitting full blob data on-chain to prove a mismatch is gas-prohibitive for any non-trivial blob size. The PoC therefore uses an optimistic bond + counter-evidence scheme rather than cryptographic mismatch proof; a production Merkle proof design is specified but deferred.

This ADR specifies concrete on-chain mechanisms for both: `ecrecover`-based signature verification for the three signature-dependent offenses, and an optimistic challenge-response for corruption — enabling all four slash evidence paths for the PoC.

## Decision

### 1. Ed25519 Signature Verification — Dual-Key Slash Signatures

#### Problem

[ADR 005](005-protocol.md) defines `ProbeResponse` and `StreamResponse` signatures using the node's Ed25519 iroh key. On-chain slash evidence requires verifying these signatures, but EVM's native `ecrecover` only handles secp256k1 (ECDSA). An on-chain Ed25519 library is too expensive for routine use.

#### Approach: secp256k1 slash signatures alongside Ed25519 wire signatures

Nodes already register an Ethereum address (secp256k1-derived) alongside their Ed25519 NodeId in `StakingRegistry` ([ADR 003](003-payments.md)). This ADR leverages that existing binding: protocol messages carry an **optional second signature** using the node's Ethereum key, specifically for on-chain evidence.

**Wire protocol additions.** `ProbeResponse` and `StreamResponse` each gain an optional `slash_sig` field:

```
ProbeResponse {has_blob, rate_per_mb, timestamp_us, signature, total_bytes?, slash_sig?}
StreamResponse {ok, rate_per_mb, total_bytes, timestamp_us, signature, redirect?, error?, voucher_interval_mb?, slash_sig?}
```

`slash_sig` is an EIP-712 secp256k1 signature over the same security-relevant fields already covered by the Ed25519 `signature`:

- **ProbeResponse slash_sig covers:** `{hash, has_blob, rate_per_mb, timestamp_us}`
- **StreamResponse slash_sig covers:** `{hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}`

These match the Ed25519-signed field sets defined in [ADR 005](005-protocol.md). Note that `hash` and `channel_id` are request-context fields (from `ProbeRequest` and `StreamRequest` respectively), not transmitted in the response body — implementers must include them when building and verifying the EIP-712 typed data. When `redirect` is absent (the common case), it is encoded as `bytes32(0)`. The `slash_sig` field follows the Tier 1 minor evolution pattern from [ADR 013](013-schema-evolution.md#tier-1--minor-no-coordination) — nodes that have not upgraded omit it. Requesters cannot submit on-chain slash evidence for messages without `slash_sig`; those cases fall back to reputation penalties, consistent with the existing timeout/non-response handling in [ADR 003](003-payments.md).

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
2. The contract reconstructs the EIP-712 typed data hash and calls `ecrecover(hash, slash_sig)` — **3,000 gas**.
3. The recovered address is looked up in `StakingRegistry` to confirm it maps to a registered node.
4. For offenses requiring two messages (phantom, rate manipulation), both must recover to the **same** address.

#### Node Implementation

When constructing a `ProbeResponse` or `StreamResponse`, the node:
1. Signs the security-relevant fields with its Ed25519 iroh key (existing behavior, used for wire authentication).
2. Signs the same fields with its Ethereum private key using EIP-712 typed data (new behavior, used for on-chain evidence).
3. Includes both signatures in the response.

The Ed25519 signature remains the primary authentication mechanism for the QUIC connection. The secp256k1 `slash_sig` is solely an evidence artifact — it is never used for peer authentication or session establishment.

#### Alternatives Considered

| Approach | Gas Cost | PoC Suitability | Why Not |
| --- | --- | --- | --- |
| RIP-7212 Ed25519 precompile | ~3,000 | Not available | Not deployed on Arbitrum Sepolia or One as of 2026-04 |
| Solidity Ed25519 library (e.g., `ed25519-sol`) | ~500k–1M | Too expensive | A single slash verification would cost $0.25–$0.50; two-signature offenses double that |
| ZK proof of Ed25519 signature | ~300k verify | Too complex | Requires a proving circuit, prover infrastructure, and proof generation latency |
| Optimistic (no signature verification) | ~50k | Insufficient security | A node could deny authorship of any message; counter-evidence alone is not enough |
| **Dual-key slash signatures (chosen)** | **~3,000** | **Recommended** | Uses proven `ecrecover`; adds one `Option` field per message; no new infrastructure |

> **Note:** [ADR 001](001-network.md#nodeid-ownership-verification) uses direct ed25519 verification (Solidity library, ~500k–1M gas) for node registration ownership proof. This is acceptable because registration is a one-time cost per node lifetime, unlike slash evidence which may be submitted frequently.

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
- Challenge bond: 100 TOKEN ([ADR 004](004-tokenomics.md#challenge-bond))

The contract verifies:
1. `ecrecover(streamResponse, slashSig)` recovers a registered node address
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

The receipt uses the `SlashJudge` EIP-712 domain (same domain separator as slash signatures). The requester signs this only after successful BLAKE3 verification of the full blob. On counter-challenge, the contract verifies the requester's signature and checks that `nodeId`, `channelId`, and `blobHash` match the challenged delivery.

**Incentive to sign:** Nodes SHOULD request a `DeliveryReceipt` after successful delivery. A requester that refuses to sign after accepting delivery cannot later submit a corruption challenge for the same blob and channel (the contract checks for contradictory receipts). Nodes MAY deprioritize or refuse future streams to requesters that consistently refuse receipts.

**Resolution:**

- If the node does not counter within 24 hours: `resolveChallenge()` slashes the node per [ADR 004](004-tokenomics.md#slash-amounts-escalating) and returns the bond to the challenger.
- If the node counters successfully: the challenge is dismissed, and the bond is forfeited (50% burned, 50% to the node per [ADR 004](004-tokenomics.md#challenge-bond)).

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

**Encoding convention.** The `bytes calldata` arguments named `*ResponseData` in the interface below are **ABI-encoded structs** matching the EIP-712 typed data fields (not postcard wire bytes). The contract ABI-decodes these fields, reconstructs the EIP-712 struct hash, and calls `ecrecover`. This ensures a single canonical encoding for both the contract and off-chain signature construction.

```solidity
interface ISlashJudge {
    /// Phantom announcement: node signed has_blob=true then ok=false within 30s
    function submitPhantomChallenge(
        bytes32 nodeId,
        bytes calldata probeResponseData,   // serialized {hash, has_blob, rate_per_mb, timestamp_us}
        bytes calldata probeSlashSig,        // EIP-712 secp256k1 signature
        bytes calldata streamResponseData,  // serialized {hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}
        bytes calldata streamSlashSig        // EIP-712 secp256k1 signature
    ) external;

    /// Rate manipulation: stream rate > probe rate within 30s
    function submitRateChallenge(
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig
    ) external;

    /// Blacklist violation: serving a blacklisted hash after compliance window
    function submitBlacklistChallenge(
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,   // ProbeResponse (has_blob=true) or StreamResponse (ok=true)
        bytes calldata slashSig,
        bool isStreamResponse          // false = ProbeResponse evidence, true = StreamResponse evidence
    ) external;

    /// Corrupted delivery: node served bytes failing BLAKE3 verification
    function submitCorruptionChallenge(
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

#### Evidence Verification Per Offense Type

**Phantom announcement:**
1. `ecrecover(probeResponseData, probeSlashSig)` → address A
2. `ecrecover(streamResponseData, streamSlashSig)` → address B
3. Verify A == B (same node)
4. Verify `probeResponse.has_blob == true` and `streamResponse.ok == false`
5. Verify `probeResponse.hash == streamResponse.hash` (same blob)
6. Verify `streamResponse.timestamp_us >= probeResponse.timestamp_us`
7. Verify `streamResponse.timestamp_us - probeResponse.timestamp_us < 30_000_000` (30-second window)
8. Look up address A in `StakingRegistry` — must be a registered node

**Rate manipulation:**
1–3. Same address recovery and identity check as phantom
4. Verify `streamResponse.rate_per_mb > probeResponse.rate_per_mb`
5. Verify `probeResponse.hash == streamResponse.hash` (same blob)
6–8. Same timestamp and registration checks as phantom

**Blacklist violation:**
1. `ecrecover(responseData, slashSig)` → address
2. Look up address in `StakingRegistry` — must be a registered node
3. Decode `hash` from the response; verify it matches `blobHash`
4. If `ProbeResponse`: verify `has_blob == true`. If `StreamResponse`: verify `ok == true`
5. Query `ContentBlacklist.getEntry(blobHash)` — must exist and `effectiveAt` must be before the response's `timestamp_us`
6. **Regional scope limitation (PoC):** [ADR 011](011-content-takedown.md#slashing) specifies that a node is only slashable for hashes blacklisted in its declared region. However, the node's region is self-reported and not stored on-chain in `StakingRegistry` for the PoC. The `SlashJudge` contract therefore cannot enforce regional scope in the PoC — all blacklist violations are treated as globally scoped. Production should add a `region` field to `NodeInfo` to enable on-chain regional filtering

**Corrupted delivery (PoC):**
1. `ecrecover(streamResponseData, streamSlashSig)` → address
2. Look up address in `StakingRegistry` — must be a registered node
3. Verify `streamResponse.ok == true` and `streamResponse.hash == blobHash`
4. Store challenge; start 24-hour counter-evidence window
5. Resolution after window: slash if no valid counter-evidence; dismiss if countered

#### Bond Handling

- Challengers must `TOKEN.approve(slashJudge, bondAmount)` before calling any `submit*Challenge()` function. The contract transfers the bond on submission.
- **Successful challenge:** bond returned to challenger; node slashed via `StakingRegistry.slash()`.
- **Successful counter:** bond forfeited — 50% burned, 50% transferred to the challenged node ([ADR 004](004-tokenomics.md#challenge-bond)).
- **Immediate offenses** (phantom, rate, blacklist): if on-chain verification passes, the slash executes immediately (no counter-evidence window). The node's recourse is to not commit the offense.
- **Deferred offenses** (corruption): 24-hour counter-evidence window before resolution.

#### Gas Estimates

| Operation | Estimated Gas | Notes |
| --- | --- | --- |
| `submitPhantomChallenge` | ~60k | 2× `ecrecover` (6k) + calldata + storage + bond transfer |
| `submitRateChallenge` | ~60k | Same as phantom |
| `submitBlacklistChallenge` | ~50k | 1× `ecrecover` (3k) + `ContentBlacklist` lookup + bond transfer |
| `submitCorruptionChallenge` | ~50k | 1× `ecrecover` (3k) + storage for challenge state + bond transfer |
| `counterChallenge` | ~40k | Evidence verification + storage update |
| `resolveChallenge` | ~80k | `StakingRegistry.slash()` + bond transfer + state cleanup |

These estimates replace the `submitFraudProof()` placeholder (~250k gas) in [ADR 004](004-tokenomics.md#gas-cost-breakdown-arbitrum). The dual-key approach reduces per-signature verification from ~500k (Ed25519 library) to ~3k (`ecrecover`), making routine slashing economically viable.

### 4. Integration with Existing Contracts

**StakingRegistry ([ADR 001](001-network.md), [ADR 003](003-payments.md), [ADR 004](004-tokenomics.md)):**
- Adds `slash(address node, uint8 offenseType) external` callable only by the `SlashJudge` contract address. Implements the escalating schedule from [ADR 004](004-tokenomics.md#slash-amounts-escalating) (10% flat for PoC; 5/15/50% with lifetime counter for production). Checks auto-ejection threshold (50% of `minStake`).
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
- The `slash_sig` field is optional (Tier 1 evolution per [ADR 013](013-schema-evolution.md)), so nodes can upgrade incrementally. The network degrades gracefully: unsigned messages fall back to reputation penalties.
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
- **[ADR 004](004-tokenomics.md):** `submitFraudProof()` gas estimate → replaced by per-offense `SlashJudge` estimates.
