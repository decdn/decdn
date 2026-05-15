# ADR 014: On-Chain Verification for Slashing Evidence

**Date:** 2026-04-03
**Status:** Draft

## Context

Three slashable offenses require on-chain evidence verification ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)):

1. **Phantom announcement** — node signs `has_blob: true` then cannot deliver
2. **Rate manipulation** — node advertises one rate in probe, charges higher in stream
3. **Blacklist violation** — node serves a blacklisted hash after the compliance window ([ADR 011](011-content-takedown.md))

A fourth offense, **double settlement** (submitting the same voucher to multiple channels), is production-only and not covered by on-chain verification in the PoC.

Content corruption — a node delivering bytes that don't BLAKE3 to the advertised hash — is not an on-chain offense; it is absorbed at the wire by client-side BLAKE3 verification + post-verification voucher signing per [ADR 003 §Corrupted delivery](003-payments.md#corrupted-delivery).

Phantom, rate, and blacklist all require verifying cryptographic signatures from protocol messages. EVM-native `ecrecover` handles secp256k1 (ECDSA) cheaply (~3,000 gas). This ADR specifies the concrete mechanism: `ecrecover`-based signature verification through a unified `SlashJudge` contract.

## Decision

### 1. Slash Signatures — secp256k1 EIP-712

#### Approach

Each slashing-participating message (`ProbeResponse`, `StreamResponse`) carries a single message-body signature, `slash_sig`, produced with the node's Ethereum key. Connection-level peer identity is authenticated separately by the iroh QUIC handshake against the registered Ed25519 NodeId; the body signature makes message contents portable evidence verifiable both off-chain and on-chain.

The Ethereum key is the same secp256k1 key the node already holds for staking and channels: `StakingRegistry.registerNode` ([ADR 001](001-network.md)) atomically binds the operator's Ethereum address to the Ed25519 NodeId, so `ecrecover` on a `slash_sig` followed by a `StakingRegistry.nodeIdOf(recovered)` lookup attributes the message to a NodeId. EVM-native verification costs ~3,000 gas, making routine slashing economically viable; an Ed25519 wire signature would have cost ~500k–1M gas via Solidity library and was rejected for this reason.

##### Wire protocol

`slash_sig` is mandatory and non-empty on every `ProbeResponse` and `StreamResponse`. Requesters MUST reject responses with missing or zero-length `slash_sig`. There is no opt-out: every interaction in the paid delivery path is on-chain slashable. The fields covered by `slash_sig` are the same fields that drive the slashing mechanisms in [ADR 005](005-protocol.md):

```
ProbeResponse {has_blob, rate_per_mb, timestamp_us, total_bytes?, slash_sig}
StreamResponse {ok, rate_per_mb, total_bytes, timestamp_us, redirect?, error?, voucher_interval_mb?, slash_sig}
```

- **ProbeResponse slash_sig covers:** `{hash, has_blob, rate_per_mb, timestamp_us}`
- **StreamResponse slash_sig covers:** `{hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}`

`hash` and `channel_id` are request-context fields (from `ProbeRequest` and `StreamRequest` respectively), not transmitted in the response body — implementers must include them when building and verifying the EIP-712 typed data. When `redirect` is absent (common case), it is encoded as `bytes32(0)`. The signed-field set is the v1 baseline; per [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing), any subsequent change is a Tier 3 ALPN bump.

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

When constructing a `ProbeResponse` or `StreamResponse`, the node signs the security-relevant fields with its Ethereum private key using EIP-712 typed data and emits the result as `slash_sig`. Peer authentication is handled separately by the iroh QUIC handshake (against the registered Ed25519 NodeId); `slash_sig` is purely message-body attribution/evidence, never used for connection establishment.

#### Alternatives Considered

The signature-scheme alternatives table (RIP-7212, Solidity library, ZK, optimistic) is recorded in [`_history/alternatives-pre-launch.md` § ADR 014 — Slash Signature Scheme](_history/alternatives-pre-launch.md#adr-014--slash-signature-scheme).

> **Note:** [ADR 001](001-network.md#nodeid-ownership-verification) uses direct ed25519 verification (Solidity library, ~500k–1M gas) for node registration ownership proof. This is acceptable because registration is a one-time cost per node lifetime, unlike slash evidence which may be submitted frequently.

### 2. SlashJudge Contract

A unified contract that adjudicates the three signature-dependent offenses (phantom announcement, rate manipulation, blacklist violation). All resolve synchronously at submit time — see §Bond Handling. The contract holds challenge bonds, verifies evidence, and calls `StakingRegistry.slash()` on each successful submission.

#### Interface

##### Encoding convention

The `bytes calldata` arguments named `*ResponseData` in the interface below are **ABI-encoded structs** matching the EIP-712 typed data fields (not postcard wire bytes). The contract ABI-decodes these fields, reconstructs the EIP-712 struct hash, and verifies using `SignatureChecker.isValidSignatureNow` ([ADR 024](024-account-abstraction.md)). This ensures a single canonical encoding for both the contract and off-chain signature construction.

```solidity
interface ISlashJudge {
    enum OffenseType { Phantom, RateManipulation, Blacklist }

    /// Emitted on every slash resolution that reduces stake. `slashId` is globally
    /// monotonic across all offense types. `evidenceHash` is keccak256 over the
    /// per-offense canonical preimage, prefixed by uint8(offenseType) so
    /// overlapping-evidence offenses produce distinct hashes (exact abi.encode(...)
    /// per offense type in the "`Slashed` event and `slashId` allocation" sub-section
    /// below). Signatures excluded — the typed-data digests they sign already
    /// uniquely determine the evidence.
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

    // All three offense types (Phantom, RateManipulation, Blacklist) resolve
    // synchronously at submit time. There is no counter-evidence window —
    // `Slashed` is emitted atomically with `StakingRegistry.slash()` inside
    // each `submit*Challenge` call.
}
```

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

All challenge types MUST validate evidence age using a skew-safe comparison. Let `nowUs = block.timestamp * 1_000_000` and `evidence.timestamp_us` be the earliest `timestamp_us` from the submitted evidence messages (e.g., `probeResponse.timestamp_us` for phantom/rate challenges, `streamResponse.timestamp_us` for blacklist challenges lacking a probe). The contract MUST: first require `evidence.timestamp_us <= nowUs + MAX_FUTURE_SKEW_US` (rejects far-future timestamps); then compute age without underflow as `ageUs = evidence.timestamp_us >= nowUs ? 0 : nowUs - evidence.timestamp_us`; then require `ageUs < MAX_EVIDENCE_AGE_US`. `MAX_EVIDENCE_AGE_US` is a governable parameter on `SlashJudge` (PoC: 5 days = 432,000,000,000 μs; safety bounds: [1 day, 30 days]). `MAX_FUTURE_SKEW_US` is fixed at 60,000,000 μs (60 seconds).

##### Interaction with unbonding period

`MAX_EVIDENCE_AGE_US` MUST be strictly less than `StakingRegistry.unbondingPeriod` (converted to microseconds). Otherwise a node could commit an offense, immediately initiate unstaking, and complete withdrawal before the evidence is submitted — avoiding the slash entirely. PoC defaults (evidence age 5 days, unbonding 7 days) satisfy the invariant with a 2-day margin. The safety bounds ([1 day, 30 days] for evidence age vs [3 days, 30 days] for unbonding per [ADR 009](009-governance.md)) admit configurations that violate this invariant in isolation, so both setters enforce it as a paired cross-parameter check at the contract layer:

- `SlashJudge.setMaxEvidenceAge(uint256 newValueUs)` MUST revert if `newValueUs >= StakingRegistry.unbondingPeriod * 1_000_000` (or the integer-overflow-safe equivalent), in addition to the [1 day, 30 days] individual bound.
- `StakingRegistry.setUnbondingPeriod(uint256 newValueSeconds)` MUST revert if `newValueSeconds * 1_000_000 <= SlashJudge.maxEvidenceAgeUs`, in addition to the [3 days, 30 days] individual bound.

The check applies at initialization too — neither contract may be deployed with an initial pair that violates the invariant. This matches the cross-parameter setter pattern used elsewhere (see [ADR 009 § Governable Parameters with Safety Bounds](009-governance.md#governable-parameters-with-safety-bounds): `OriginAssignment.minRedundancy ≤ maxOriginsPerNamespace`; [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds): the FeeRouter sum-to-100% invariant) — invariants between parameters with a genuine ordering relationship are contract-enforced, not implementation-enforced.

**Rate manipulation:** 1–3. Same `SignatureChecker` verification and identity check as phantom
4. Verify `streamResponse.rate_per_mb > probeResponse.rate_per_mb`
5. Verify `probeResponse.hash == streamResponse.hash` (same blob)
6–8. Same timestamp and registration checks as phantom
9. Slash immediately via `StakingRegistry.slash()` — no counter-evidence window. Two signed messages from the same NodeId disagreeing about that node's own rate within 30 seconds are non-repudiable; the node's last probe-quoted rate is binding for the slashing window. Legitimate rate changes wait out the 30-second window before serving a stream at the new rate.

**Blacklist violation:**

1. Challenger provides `challengedNode` address
2. `SignatureChecker.isValidSignatureNow(challengedNode, responseDigest, slashSig)` — must pass
3. Look up `challengedNode` in `StakingRegistry` — must be a registered node
4. Decode `hash` from the response; verify it matches `blobHash`
5. If `ProbeResponse`: verify `has_blob == true`. If `StreamResponse`: verify `ok == true`
6. Query `ContentBlacklist.getEntry(blobHash)` — must exist and `effectiveAt` must be before the response's `timestamp_us`
7. **Regional scope limitation (PoC):** [ADR 011](011-content-takedown.md#slashing) specifies a node is only slashable for hashes blacklisted in its declared region. But the node's region is self-reported and not stored on-chain in `StakingRegistry` for the PoC, so `SlashJudge` cannot enforce regional scope in the PoC — all blacklist violations are treated as globally scoped. Production should add a `region` field to `NodeInfo` to enable on-chain regional filtering

#### Bond Handling

- Challengers must `TOKEN.approve(slashJudge, bondAmount)` before calling any `submit*Challenge()` function. The contract transfers the bond on submission.
- **Successful challenge:** bond returned to challenger; node slashed via `StakingRegistry.slash()`.
- **All three offenses** (phantom, rate manipulation, blacklist): if on-chain verification passes, the slash executes synchronously at submit time — no counter-evidence window. Each offense's evidence is cryptographically dispositive: phantom and rate manipulation rely on two contradictory signed messages from the same node within 30 s; blacklist relies on a signed response for an already-blacklisted hash. The node's recourse is to not commit the offense; for rate changes, honor the last probe-quoted rate for the 30-second slashing window before serving streams at a new rate.
- **Frivolous-challenge bond loss.** A `submit*Challenge` that fails on-chain verification (signature mismatch, timestamp out of window, hash mismatch, etc.) reverts and the challenger pays only gas; the bond is not transferred for failed verifications. A challenge that *passes* verification always slashes the node — there is no second-stage dispute that could forfeit the bond after-the-fact.

#### `Slashed` event and `slashId` allocation

Every slash that reduces operator stake emits `Slashed(slashId, operator, offenseType, amount, evidenceHash)` (see the `ISlashJudge` interface block above). The event is the canonical slash record and the appeal-pinning identifier consumed by [ADR 028 §6](028-slashing-appeals.md#6-contract-surface) `openSlashAppeal(slashId, evidenceBundleHash)` — without it, no ADR 028 appeal can be filed.

- **`slashId`** is a globally monotonic `uint256` (single counter across all offense types, not per-operator and not per-offense-type), allocated from a `nextSlashId` storage slot incremented inline in the same transaction as the `StakingRegistry.slash(...)` call. `slashId` values are stable, non-reusable, and non-zero — `slashId == 0` is reserved as the "no slash" sentinel.
- **`offenseType`** is the `OffenseType` enum from the interface above.
- **`evidenceHash`** is `keccak256` over a per-offense canonical preimage that uniquely identifies the (offenseType, evidence) pair the slash relied on. The preimage uses `abi.encode(...)` (not `abi.encodePacked`) so field encoding is unambiguous across implementers. Every preimage is prefixed by `uint8(offenseType)` so two distinct offenses against the same operator on overlapping evidence (e.g., a single `(probe, stream)` pair where `streamResponse.ok == false` AND `streamResponse.rate_per_mb > probeResponse.rate_per_mb` triggers both phantom and rate-manipulation) produce distinct `evidenceHash` values, not just distinct `slashId`s. Signatures are **excluded** — the §1 EIP-712 typed-data digests they sign already uniquely identify the message contents, so a successful slash trivially fixes the digest set; including the variable-length signature blobs would add an `abi.encode` vs `abi.encodePacked` field-length ambiguity without adding evidentiary content. The `blobHash` parameter passed to the immediate-execution `submit*Challenge` paths is similarly excluded — the §2 evidence-verification flow already binds it via the `responseData.hash == blobHash` check, and the EIP-712 `*Response` struct hash commits to `hash` directly. The per-offense preimages are:
  - **Phantom:** `keccak256(abi.encode(uint8(OffenseType.Phantom), probeStructHash, streamStructHash))`.
  - **Rate manipulation:** `keccak256(abi.encode(uint8(OffenseType.RateManipulation), probeStructHash, streamStructHash))`. The `OffenseType` prefix is what distinguishes this preimage from phantom on overlapping evidence.
  - **Blacklist:** `keccak256(abi.encode(uint8(OffenseType.Blacklist), responseStructHash, isStreamResponse))`. The boolean is required because it is a `submitBlacklistChallenge` parameter, not part of any `*Response` struct.
  Each `*StructHash` is the EIP-712 struct hash of the corresponding `*Response` per §1 (head-only `bytes32` — `abi.encode` adds no padding to a fixed-width 32-byte value). Appeals reference `evidenceHash` to prove they challenge the same evidence the slash relied on; ADR 028 §6 `openSlashAppeal(slashId, evidenceBundleHash)` requires `evidenceBundleHash == evidenceHash` of the referenced `Slashed` event.
- **Emission sites.** All four offenses are immediate: `Slashed` is emitted from the synchronous `submit*Challenge` paths immediately after the inline `StakingRegistry.slash()` returns. The "`StakingRegistry.slash()` then `emit Slashed`" sequence is contract-enforced atomic (single transaction); a slash without a matching event is impossible.

The companion `SafetyReserve` events (`SlashAppealOpened`, `SlashAppealRatified`, etc.) remain forward-referenced to a future contract-implementation ADR per [ADR 028 § Cross-ADR Impact](028-slashing-appeals.md#cross-adr-impact); only `Slashed` itself is canonicalised here.

#### Gas Estimates

| Operation | Estimated Gas | Notes |
| --- | --- | --- |
| `submitPhantomChallenge` | ~65k–90k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage + bond transfer + `Slashed` emit on success |
| `submitRateChallenge` | ~65k–90k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage for pending challenge + bond transfer + `Slashed` emit on success |
| `submitBlacklistChallenge` | ~55k–70k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + `ContentBlacklist` lookup + bond transfer + `Slashed` emit on success |
| `Slashed` event emit | ~5k–7k | `nextSlashId++` (cold SLOAD + non-zero→non-zero SSTORE on first emit per tx, ~5k post-EIP-2929) + LOG3 base + 3 stack topics (event signature + 2 indexed) + 96 bytes non-indexed data (~2k); negligible vs the surrounding `StakingRegistry.slash()`. The very first `Slashed` ever emitted on a fresh deployment pays an additional ~17k for the 0→non-zero `nextSlashId` SSTORE. |

Using secp256k1 EIP-712 for `slash_sig` keeps per-signature verification at ~3k gas via `ecrecover`, against the ~500k–1M gas a Solidity Ed25519 library would require — making routine slashing economically viable.

#### Governable Parameters with Safety Bounds

| Parameter | Contract | Default | Min | Max | Cross-parameter invariant |
| --- | --- | --- | --- | --- | --- |
| `MAX_EVIDENCE_AGE_US` | `SlashJudge` | 5 days | 1 day | 30 days | `< StakingRegistry.unbondingPeriod` (paired) |
| `MAX_FUTURE_SKEW_US` | `SlashJudge` | 60 s | (fixed) | (fixed) | — |
| Challenge bond | `SlashJudge` | (per [ADR 009](009-governance.md#governable-parameters-with-safety-bounds)) | 1 TOKEN | 1,000 TOKEN | — |

The `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant is paired across two contracts. Setter paths on both `SlashJudge` and `StakingRegistry` enforce the post-update inequality at the contract layer (see [Interaction with unbonding period](#interaction-with-unbonding-period) for the exact revert conditions); violating updates revert atomically with the setter call. `MAX_FUTURE_SKEW_US` is fixed at 60 seconds at deployment and not governable — it absorbs NTP drift between challenger and evidence-signing node and has no economic surface that varies by network conditions. Challenge bond bounds are canonical in [ADR 009](009-governance.md#governable-parameters-with-safety-bounds); listed here for completeness.

### 3. Integration with Existing Contracts

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

**Negative:**

- Nodes perform a secp256k1 EIP-712 signature on every `ProbeResponse` and `StreamResponse`, adding ~1ms of computation per message — negligible relative to network RTT, but nonzero.
- The `slash_sig` field adds ~65 bytes per `ProbeResponse` and `StreamResponse`. For probe messages this is meaningful overhead; for stream responses preceding multi-MB deliveries, it is negligible.
- Off-chain verifiers (clients, requesting nodes, third-party fraud detectors) must `ecrecover` and look up `StakingRegistry.nodeIdOf(recovered)` to attribute a message to a NodeId, rather than verifying directly against the iroh key. These parties already maintain the binding cache for voucher attribution, so the marginal cost is one extra map lookup per verification.
- Cross-contract replay is prevented by per-contract EIP-712 domains, but implementers must configure domain separators correctly at deployment.
- The §2 `Slashed` event adds an `OffenseType` enum, a `nextSlashId` storage slot, and the per-offense `evidenceHash` preimage encoding to `SlashJudge`'s audit surface — small but real: every slash path emits the event atomically with `StakingRegistry.slash()`, and the `OffenseType` ordering is contract-canonical (any reordering requires coordinated migration of `SafetyReserve` per ADR 028 §6).
