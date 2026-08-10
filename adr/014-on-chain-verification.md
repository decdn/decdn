# ADR 014: On-Chain Verification for Slashing Evidence

**Date:** 2026-04-03
**Status:** Draft

## Context

Two slashable offenses require on-chain evidence verification ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)):

1. **Rate manipulation** — node advertises one rate in probe, delivers (`ok: true`) at a higher rate in stream
2. **Blacklist violation** — node serves a blacklisted hash after the compliance window ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting))

Content corruption — a node delivering bytes that don't BLAKE3 to the advertised hash — is not an on-chain offense; it is absorbed at the wire by client-side BLAKE3 verification + post-verification voucher signing per [ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery).

Rate and blacklist both require verifying cryptographic signatures from protocol messages. EVM-native `ecrecover` handles secp256k1 (ECDSA) cheaply (~3,000 gas). This ADR specifies the concrete mechanism: `ecrecover`-based signature verification through a unified `SlashJudge` contract.

## Decision

### Slash Signatures — secp256k1 EIP-712

#### Approach

Each slashing-participating message (`ProbeResponse`, `StreamResponse`) carries a single message-body signature, `slash_sig`, produced with the node's Ethereum key. Connection-level peer identity is authenticated separately by the iroh QUIC handshake against the registered Ed25519 NodeId; the body signature makes message contents portable evidence verifiable both off-chain and on-chain.

The Ethereum key is the same secp256k1 key the node already holds for staking and pools. `CapacityBond.registerNode` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) atomically binds the operator's Ethereum address to the Ed25519 NodeId, so `ecrecover` on a `slash_sig` plus a `CapacityBond.nodeIdOf(recovered)` lookup attributes the message to a NodeId. EVM-native verification costs ~3,000 gas, making routine slashing economically viable; verifying an Ed25519 wire signature in a Solidity library would cost ~500k–1M gas.

##### Wire protocol

`slash_sig` is mandatory and non-empty on every `ProbeResponse` and `StreamResponse`. Requesters MUST reject responses with missing or zero-length `slash_sig`. There is no opt-out: every interaction in the paid delivery path is on-chain slashable. The fields covered by `slash_sig` are the same fields that drive the slashing mechanisms in [ADR 005](005-protocol.md#adr-005-wire-protocol):

```
ProbeResponse {has_blob, rate_per_mb, timestamp_us, total_bytes?, slash_sig}
StreamResponse {ok, rate_per_mb, total_bytes, timestamp_us, redirect?, error?, voucher_interval_mb?, slash_sig}
```

- **ProbeResponse slash_sig covers:** `{hash, has_blob, rate_per_mb, timestamp_us}`
- **StreamResponse slash_sig covers:** `{hash, ok, rate_per_mb, total_bytes, pool_id, timestamp_us, redirect}`

`hash` and `pool_id` are request-context fields (from `ProbeRequest` and `StreamRequest` respectively), not transmitted in the response body — implementers must include them when building and verifying the EIP-712 typed data. When `redirect` is absent (common case), it is encoded as `bytes32(0)`. The signed-field set is the v1 baseline; per [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing), any subsequent change is a Tier 3 ALPN bump.

#### EIP-712 Type Definitions

```solidity
bytes32 constant PROBE_RESPONSE_TYPEHASH = keccak256(
    "ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)"
);

bytes32 constant STREAM_RESPONSE_TYPEHASH = keccak256(
    "StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 poolId,uint64 timestampUs,bytes32 redirect)"
);
```

The `SlashJudge` contract uses its own EIP-712 domain separator, not shared with `CapacityBond` or `PaymentPool`. This prevents cross-contract signature replay.

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
2. The contract reconstructs the EIP-712 typed data hash and calls `SignatureChecker.isValidSignatureNow(challengedNode, hash, slash_sig)` — **~3,000 gas** for EOA nodes, **~15,000 gas** for Safe-based nodes ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)).
3. The challenger-provided address is looked up in `CapacityBond` to confirm it maps to a registered node.
4. For the two-message offense (rate manipulation), the signatures must both validate against the **same** node address.

#### Node Implementation

When constructing a `ProbeResponse` or `StreamResponse`, the node signs the security-relevant fields with its Ethereum private key using EIP-712 typed data and emits the result as `slash_sig`. Peer authentication is separate (the iroh QUIC handshake against the registered Ed25519 NodeId); `slash_sig` is message-body attribution/evidence only, never used for connection establishment.

> **Note:** [ADR 003 § NodeId Ownership Verification](003-payments.md#nodeid-ownership-verification) uses direct ed25519 verification (Solidity library, ~500k–1M gas) for node registration ownership proof. This is acceptable because registration is a one-time cost per node lifetime, unlike slash evidence which may be submitted frequently.

### SlashJudge Contract

A unified contract that adjudicates the two signature-dependent offenses (rate manipulation, blacklist violation). Challenging is a two-phase **commit–reveal** flow (see [§ Challenge front-running mitigation](#challenge-front-running-mitigation-commitreveal)): a `commitChallenge` registers an opaque commitment, and the `submit*Challenge` reveal resolves synchronously once the commitment matures — there is still no counter-evidence window (see § Bond Handling). The contract holds challenge bonds, verifies evidence, and calls `CapacityBond.slash()` on each successful reveal.

#### Interface

##### Encoding convention

The `bytes calldata` arguments named `*ResponseData` in the interface below are **ABI-encoded structs** matching the EIP-712 typed data fields (not postcard wire bytes). The contract ABI-decodes these fields, reconstructs the EIP-712 struct hash, and verifies using `SignatureChecker.isValidSignatureNow` ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)). This ensures a single canonical encoding for both the contract and off-chain signature construction.

```solidity
interface ISlashJudge {
    enum OffenseType { RateManipulation, Blacklist }

    /// Emitted on every slash resolution that reduces stake. `slashId` is globally
    /// monotonic across all offense types. `evidenceHash` is keccak256 over the
    /// per-offense canonical preimage, prefixed by uint8(offenseType) so
    /// overlapping-evidence offenses produce distinct hashes (exact abi.encode(...)
    /// per offense type in the "`Slashed` event and `slashId` allocation" sub-section
    /// below). Signatures excluded — the typed-data digests they sign already
    /// uniquely determine the evidence.
    /// Consumed by `SlashAppeal.openSlashAppeal(slashId, evidenceBundleHash)` per ADR 028 § Contract surface.
    event Slashed(
        uint256 indexed slashId,
        address indexed operator,
        OffenseType offenseType,
        uint256 amount,
        bytes32 evidenceHash
    );

    /// Phase 1 of every challenge: register an opaque commitment
    /// `keccak256(abi.encode(evidenceHash, salt, msg.sender))`. Hides the evidence
    /// and binds the challenger so a mempool copy of the reveal cannot steal the
    /// reward (see § Challenge front-running mitigation). Reverts on a duplicate.
    function commitChallenge(bytes32 commitment) external;

    /// Rate manipulation: stream delivered (`ok == true`) at a rate exceeding
    /// the probe quote, same hash, within the 30s window. A signed refusal is
    /// inert as evidence. Reveals a prior `commitChallenge`; emits `Slashed`
    /// synchronously on success.
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig,
        bytes32 salt
    ) external;

    /// Blacklist violation: serving a blacklisted hash after compliance window.
    /// Reveals a prior `commitChallenge`; emits `Slashed` synchronously on success.
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,   // ProbeResponse (has_blob=true) or StreamResponse (ok=true)
        bytes calldata slashSig,
        bool isStreamResponse,         // false = ProbeResponse evidence, true = StreamResponse evidence
        bytes32 salt                   // reveals the commitChallenge commitment
    ) external;

    // After a matured commitment, both offense types (RateManipulation,
    // Blacklist) resolve synchronously at reveal time. There is
    // no counter-evidence window — `Slashed` is emitted atomically with
    // `CapacityBond.slash()` inside each `submit*Challenge` reveal.
}
```

#### Evidence Verification Per Offense Type

##### Evidence staleness

All challenge types MUST validate evidence age using a skew-safe comparison. Let `nowUs = block.timestamp * 1_000_000` and `evidence.timestamp_us` be the earliest `timestamp_us` from the submitted evidence messages (e.g., `probeResponse.timestamp_us` for rate challenges, `streamResponse.timestamp_us` for blacklist challenges lacking a probe). The contract MUST: first require `evidence.timestamp_us <= nowUs + MAX_FUTURE_SKEW_US` (rejects far-future timestamps); then compute age without underflow as `ageUs = evidence.timestamp_us >= nowUs ? 0 : nowUs - evidence.timestamp_us`; then require `ageUs < MAX_EVIDENCE_AGE_US`. `MAX_EVIDENCE_AGE_US` is a governable parameter on `SlashJudge` (PoC: 5 days = 432,000,000,000 μs; safety bounds: [1 day, 30 days]). `MAX_FUTURE_SKEW_US` is fixed at 60,000,000 μs (60 seconds).

##### Interaction with unbonding period

`MAX_EVIDENCE_AGE_US` MUST be strictly less than `CapacityBond.unbondingPeriod` (converted to microseconds). Otherwise a node could commit an offense, immediately initiate unbonding, and complete withdrawal before the evidence is submitted — avoiding the slash entirely. PoC defaults (evidence age 5 days, unbonding 14 days per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) satisfy the invariant with a 9-day margin. The safety bounds ([1 day, 30 days] for evidence age vs [7 days, 60 days] for unbonding per [ADR 009](009-governance.md#adr-009-governance-model)) admit configurations that violate this invariant in isolation, so both setters enforce it as a paired cross-parameter check at the contract layer:

- `SlashJudge.setMaxEvidenceAge(uint256 newValueUs)` MUST revert if `newValueUs >= CapacityBond.unbondingPeriod * 1_000_000` (or the integer-overflow-safe equivalent), in addition to the [1 day, 30 days] individual bound.
- `CapacityBond.setUnbondingPeriod(uint256 newValueSeconds)` MUST revert if `newValueSeconds * 1_000_000 <= SlashJudge.maxEvidenceAgeUs`, in addition to the [7 days, 60 days] individual bound.

The check applies at initialization too — neither contract may be deployed with an initial pair that violates the invariant. This matches the cross-parameter setter pattern used elsewhere (see [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds): the FeeRouter sum-to-100% invariant) — invariants between parameters with a genuine ordering relationship are contract-enforced, not implementation-enforced.

##### Rate manipulation

1. Challenger provides `challengedNode` address (the node's Ethereum address or Safe address)
2. `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSlashSig)` — must pass
3. `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSlashSig)` — must pass
4. Verify `streamResponse.ok == true` — the stream must be a delivery, not a refusal
5. Verify `streamResponse.rate_per_mb > probeResponse.rate_per_mb`
6. Verify `probeResponse.hash == streamResponse.hash` (same blob)
7. Verify `streamResponse.timestamp_us >= probeResponse.timestamp_us`
8. Verify `streamResponse.timestamp_us - probeResponse.timestamp_us < 30_000_000` (30-second window)
9. Look up `challengedNode` in `CapacityBond` — must be a registered node
10. Slash immediately via `CapacityBond.slash()` — no counter-evidence window. Two signed messages from the same NodeId disagreeing about that node's own rate within 30 seconds are non-repudiable; the node's last probe-quoted rate is binding for the slashing window. Legitimate rate changes wait out the 30-second window before serving a stream at the new rate.

##### Blacklist violation

1. Challenger provides `challengedNode` address
2. `SignatureChecker.isValidSignatureNow(challengedNode, responseDigest, slashSig)` — must pass
3. Look up `challengedNode` in `CapacityBond` — must be a registered node
4. Decode `hash` from the response; verify it matches `blobHash`
5. If `ProbeResponse`: verify `has_blob == true`. If `StreamResponse`: verify `ok == true`
6. **Entry liveness, anchored to the served response.** For any region key, the entry read from `ContentBlacklist.getHashEntry(region, blobHash)` is *enforceable before the response* iff it exists (`addedAt != 0`), is not a lapsed emergency entry, and its `effectiveAt` precedes the response's `timestamp_us`. Each clause is load-bearing:
   - Emergency lapse is evaluated at `block.timestamp`, not at the response. An emergency entry that has since passed its category deadline ([ADR 011 § Compliance Window](011-content-takedown.md#compliance-window)) is unenforceable outright rather than retroactively valid for the interval it was live — the same semantics as an entry governance has removed, which likewise invalidates the slash.
   - The comparison anchor is `effectiveAt`, not `addedAt`. Slashing on `addedAt` would punish a delivery served inside the compliance grace period the node is explicitly granted, for content it had no way to know was prohibited. `effectiveAt` is stored in seconds and `timestamp_us` in microseconds, so the contract scales the former to microseconds before comparing.
7. **Scope resolution — global ∪ regional.** [ADR 011 § Slashing](011-content-takedown.md#slashing) makes a node slashable only for hashes it was in scope to remove, and `SlashJudge` enforces that scope on-chain. The challenge succeeds iff the entry is enforceable-before-response (item 6) under **at least one** of three legs:
   - **Global** — `getHashEntry(GLOBAL_REGION, blobHash)`, where `GLOBAL_REGION` is the `bytes32("GLOBAL")` sentinel. A global entry binds every operator regardless of declared region.
   - **Current region** — the operator's declared `regionHint`.
   - **Previous region** — the operator's `regionPrev`, while the most recent region change has not yet ripened.

   Region is on-chain state, not merely a self-reported wire field: `CapacityBond` stores `NodeInfo.regionHint` (written by `registerNode`, rewritten by `updateRegion`), `regionPrev`, and `regionLastChanged` per operator, plus the governable `REGION_STABILITY_WINDOW`. `SlashJudge` reads all of them through a single narrow view, `ICapacityBondRegionView.regionScopeData(operator)`. The region strings are packed left-aligned into `bytes32` so they compare equal to `ContentBlacklist`'s region keys. A `regionHint` that is empty or equal to the global sentinel yields no regional leg — global scope is the separate leg above, never a regional match.

   The prev-region leg is open while `elapsed < REGION_STABILITY_WINDOW`, where `elapsed` is measured from the operator's `effective` stamp (`regionLastChanged`, set at registration and restamped on each `updateRegion`) to the **served `responseTs`**, floored from microseconds to seconds — not to challenge-submission time ([ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window) item 2 is the canonical specification). A punitive slash asks whether a *past* serve was in scope, so the window shares the response anchor item 6 already uses. A serve that predates the change saturates to `elapsed == 0` and keeps the serve-time region in scope. Anchoring here closes the flip-then-stall evasion: a node cannot shed the prev-region leg by changing region and waiting out the window before the challenge lands. The three legs, the global/empty exclusions, and the `regionPrev != regionHint` dedup are shared with `ContentBlacklist`'s serve-time read path through a common library, so the punitive and the read predicates cannot drift; only the evaluation anchor differs (`responseTs` here, `block.timestamp` there).

   Failure modes are distinguishable. A global entry that exists but is not yet enforceable at the response, with no regional leg independently in scope, reverts `BlacklistAfterResponse(uint256 effectiveAtUs, uint64 responseTsUs)` — both arguments in microseconds, the global entry's `effectiveAt` already scaled up from its stored seconds; nothing in scope at all reverts `HashNotBlacklisted(blobHash)`.

   One residual is accepted rather than closed: a **pre-positioned** misdeclaration — a region declared falsely since registration, before any entry exists — leaves the operator out of scope for its true jurisdiction's region-scoped entries. The scope predicate routes off the *declared* region and by construction never reaches a region the operator never declared. Reactive flipping is foreclosed by the stability window above; the pre-positioned case is backstopped off-protocol by declared-jurisdiction legal exposure and the continuous latency-vs.-claim reputation penalty ([ADR 030 § Misdeclaration is operator legal exposure, not a protocol offense](030-node-region-self-attestation.md#misdeclaration-is-operator-legal-exposure-not-a-protocol-offense)).

#### Bond Handling

- Challengers must `TOKEN.approve(slashJudge, bondAmount)` before calling any `submit*Challenge()` reveal. The contract transfers the bond on the reveal (not on `commitChallenge`, which moves no funds — see [§ Challenge front-running mitigation](#challenge-front-running-mitigation-commitreveal)).
- **Successful challenge:** bond returned to challenger; node slashed via `CapacityBond.slash()`.
- **Both offenses** (rate manipulation, blacklist): if on-chain verification passes, the slash executes synchronously at reveal time (inside the `submit*Challenge` call, after a prior `commitChallenge`) — no counter-evidence window. Each offense's evidence is cryptographically dispositive: rate manipulation relies on two signed messages from the same node — a probe quote and a higher-rate delivery — within 30 s; blacklist relies on a signed response for an already-blacklisted hash. The node's recourse is to not commit the offense (for rate changes, honor the last probe-quoted rate for the 30-second slashing window per the rate-manipulation flow above).
- **Frivolous-challenge bond loss.** A `submit*Challenge` that fails on-chain verification (signature mismatch, timestamp out of window, hash mismatch, etc.) reverts and the challenger pays only gas; the bond is not transferred for failed verifications. A challenge that *passes* verification always slashes the node — there is no second-stage dispute that could forfeit the bond after-the-fact.

#### Challenge front-running mitigation (commit–reveal)

**Vector (#854).** Each `submit*Challenge` discloses the full slashable evidence (`probeResponseData`, `streamResponseData`, the operator's `slash_sig`s) in its calldata. If that were the only step, a mempool watcher could copy a pending honest challenge, resubmit it with their own address as `msg.sender`, and become the recorded challenger — capturing the 50% finality reward ([§ Integration](#integration-with-existing-contracts); distributed by `finalizeUnappealedSlash` per [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation)). The challenge bond round-trips, so only the *reward* is at risk, but that reward is the entire incentive for off-path witnesses, so leaving it MEV-extractable hollows out the enforcement layer. `usedEvidenceHash` only prevents a double-slash; the reward slot is otherwise strict first-lander.

**Mechanism.** Every challenge is a two-phase commit–reveal:

1. **Commit** — `commitChallenge(commitment)` stores `commitment = keccak256(abi.encode(evidenceHash, salt, msg.sender))` against `block.timestamp`. The commitment is opaque (reveals neither the target nor the evidence) and **binds the challenger's address**. It moves no funds and reverts on a duplicate.
2. **Reveal** — `submit*Challenge(…, salt)` runs the existing verification, recomputes `evidenceHash`, reconstructs `keccak256(abi.encode(evidenceHash, salt, msg.sender))`, and requires a stored commitment that has aged ≥ `MIN_REVEAL_DELAY` and not expired past `REVEAL_WINDOW`. The commitment is deleted on success, then the slash proceeds as before.

**Why it closes the vector.** Front-running the commit reveals nothing (it is a blind hash). At reveal the evidence becomes public, but the commitment binds `msg.sender`, so a copycat must submit under their own address — for which no commitment exists (`NoCommitment`). They cannot create one in time: `evidenceHash` is unknown until the reveal, and a fresh commit cannot be revealed until `MIN_REVEAL_DELAY` has elapsed, by which point the honest reveal has already landed (the sequencer orders it first) and consumed the evidence — so the copycat's later reveal hits `EvidenceAlreadyUsed`. The challenger binding plus the maturation delay are what close the vector; `usedEvidenceHash` is the double-slash backstop, not the primary defense. Two *independent* honest witnesses who each blind-committed still race fairly at reveal — first reveal wins the reward, the second hits `EvidenceAlreadyUsed` — which is the intended discovery race, not theft.

**L2 context and residuals.** The initial deployment targets Arbitrum, whose centralized first-come-first-served sequencer with no public mempool already blunts classical gas-priority front-running; commit–reveal additionally hardens the reward against Timeboost express-lane ordering, sequencer collusion, and any future move to decentralized sequencing, so the enforcement incentive does not rest on a sequencer-trust assumption. Accepted residuals: `commitChallenge` is permissionless and unbonded, so commit spam is possible but self-limiting (each commit costs the spammer one SSTORE and locks no protocol funds); and an honest challenge now costs two transactions plus a short maturation delay.

**Parameters.** `MIN_REVEAL_DELAY` (1 minute) and `REVEAL_WINDOW` (1 day) are fixed, not governable — matching the `MAX_FUTURE_SKEW_US` precedent. The 1-minute maturation is negligible against the ≥ 1-day evidence-age window, so it never stales otherwise-fresh evidence; `REVEAL_WINDOW` only bounds how long a single commitment stays valid, and an expired commitment can simply be re-committed (`commitChallenge` overwrites a slot older than `REVEAL_WINDOW`), so a stalled or pause-interrupted challenger is never permanently wedged on a `(salt, evidence)` pair. Promotion to governable is deferred unless operational experience demands it.

#### `Slashed` event and `slashId` allocation

Every slash that reduces operator stake emits `Slashed(slashId, operator, offenseType, amount, evidenceHash)` (see the `ISlashJudge` interface block above). The event is the canonical slash record and the appeal-pinning identifier consumed by [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface) `openSlashAppeal(slashId, evidenceBundleHash)` — without it, no [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation) appeal can be filed.

- **`slashId`** is a globally monotonic `uint256` (single counter across all offense types, not per-operator and not per-offense-type), allocated from a `nextSlashId` storage slot incremented inline in the same transaction as the `CapacityBond.slash(...)` call. `slashId` values are stable, non-reusable, and non-zero — `slashId == 0` is reserved as the "no slash" sentinel.
- **`offenseType`** is the `OffenseType` enum from the interface above.
- **`evidenceHash`** is `keccak256` over a per-offense canonical preimage that uniquely identifies the (offenseType, evidence) pair the slash relied on. The preimage uses `abi.encode(...)` (not `abi.encodePacked`) so field encoding is unambiguous across implementers. Every preimage is prefixed by `uint8(offenseType)` so two distinct offenses against the same operator on overlapping evidence produce distinct `evidenceHash` values, not just distinct `slashId`s. Signatures are **excluded** — the [§ Slash Signatures — secp256k1 EIP-712](#slash-signatures--secp256k1-eip-712) EIP-712 typed-data digests they sign already uniquely identify the message contents, so a successful slash trivially fixes the digest set; including the variable-length signature blobs would add an `abi.encode` vs `abi.encodePacked` field-length ambiguity without adding evidentiary content. The `blobHash` parameter passed to the immediate-execution `submit*Challenge` paths is similarly excluded — the [§ SlashJudge Contract](#slashjudge-contract) evidence-verification flow already binds it via the `responseData.hash == blobHash` check, and the EIP-712 `*Response` struct hash commits to `hash` directly. The per-offense preimages are:
  - **Rate manipulation:** `keccak256(abi.encode(uint8(OffenseType.RateManipulation), probeStructHash, streamStructHash))`. The `OffenseType` prefix keeps this preimage distinct from a blacklist preimage built over the same response on overlapping evidence.
  - **Blacklist:** `keccak256(abi.encode(uint8(OffenseType.Blacklist), responseStructHash, isStreamResponse))`. The boolean is required because it is a `submitBlacklistChallenge` parameter, not part of any `*Response` struct.
  Each `*StructHash` is the EIP-712 struct hash of the corresponding `*Response` per [§ Slash Signatures — secp256k1 EIP-712](#slash-signatures--secp256k1-eip-712) (head-only `bytes32` — `abi.encode` adds no padding to a fixed-width 32-byte value). Appeals reference `evidenceHash` to prove they challenge the same evidence the slash relied on; [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface) `openSlashAppeal(slashId, evidenceBundleHash)` requires `evidenceBundleHash == evidenceHash` of the referenced `Slashed` event.
- **Emission sites.** Both offenses are immediate: `Slashed` is emitted from the synchronous `submit*Challenge` paths immediately after the inline `CapacityBond.slash()` returns. The "`CapacityBond.slash()` then `emit Slashed`" sequence is contract-enforced atomic (single transaction); a slash without a matching event is impossible.

The companion `SlashAppeal` events (`AppealOpened`, `AppealGranted`, etc.) are specified in [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface); only `Slashed` itself is canonicalised here.

#### Gas Estimates

| Operation | Estimated Gas | Notes |
| --- | --- | --- |
| `submitRateChallenge` | ~65k–90k | 2× `SignatureChecker` (6k EOA / ~30k Safe) + calldata + storage for pending challenge + bond transfer + `Slashed` emit on success |
| `submitBlacklistChallenge` | ~75k–95k | 1× `SignatureChecker` (3k EOA / ~15k Safe) + up to three `ContentBlacklist` entry reads (global, current region, unripened previous region) + one `CapacityBond` region-scope read (~15k: two `string` fields plus four timestamps) + bond transfer + `Slashed` emit on success. The scope resolution in [§ Blacklist violation](#blacklist-violation) item 7 makes this the more read-heavy of the two challenge paths, though it still verifies only one signature. A challenge that resolves on the global leg short-circuits before the region reads. |
| `Slashed` event emit | ~5k–7k | `nextSlashId++` (cold SLOAD + non-zero→non-zero SSTORE on first emit per tx, ~5k post-EIP-2929) + LOG3 base + 3 stack topics (event signature + 2 indexed) + 96 bytes non-indexed data (~2k); negligible vs the surrounding `CapacityBond.slash()`. The very first `Slashed` ever emitted on a fresh deployment pays an additional ~17k for the 0→non-zero `nextSlashId` SSTORE. |

#### Governable Parameters with Safety Bounds

| Parameter | Contract | Default | Min | Max | Cross-parameter invariant |
| --- | --- | --- | --- | --- | --- |
| `MAX_EVIDENCE_AGE_US` | `SlashJudge` | 5 days | 1 day | 30 days | `< CapacityBond.unbondingPeriod` (paired) |
| `MAX_FUTURE_SKEW_US` | `SlashJudge` | 60 s | (fixed) | (fixed) | — |
| `MIN_REVEAL_DELAY` | `SlashJudge` | 60 s | (fixed) | (fixed) | commit–reveal maturation (#854) |
| `REVEAL_WINDOW` | `SlashJudge` | 1 day | (fixed) | (fixed) | commit–reveal expiry (#854) |
| Challenge bond | `SlashJudge` | (per [ADR 009](009-governance.md#governable-parameters-with-safety-bounds)) | 1 TOKEN | 1,000 TOKEN | — |

The `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant is paired across two contracts. Setter paths on both `SlashJudge` and `CapacityBond` enforce the post-update inequality at the contract layer (see [Interaction with unbonding period](#interaction-with-unbonding-period) for the exact revert conditions); violating updates revert atomically with the setter call. `MAX_FUTURE_SKEW_US` is fixed at 60 seconds at deployment and not governable — it absorbs NTP drift between challenger and evidence-signing node and has no economic surface that varies by network conditions. Challenge bond bounds are canonical in [ADR 009](009-governance.md#governable-parameters-with-safety-bounds); listed here for completeness.

### Integration with Existing Contracts

**CapacityBond ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 003](003-payments.md#adr-003-payment-model), [ADR 026 § Operator economics](026-tokenomics.md#operator-economics)):**

- Adds `slash(address operator, address challenger, uint8 offenseType) external returns (uint256 slashId, uint256 slashAmount)` callable only by the `SlashJudge` contract address (`SLASH_ROLE`). Implements the escalating schedule from [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) — 5% / 15% / 50% indexed by a `uint32` monotonic lifetime offense counter. Under escrow-on-slash the slashed amount is moved into a per-`slashId` escrow held by `CapacityBond` — nothing is transferred or burned inline. The escrow is distributed (50% challenger / 50% burn) by `finalizeUnappealedSlash` after the filing window, or resolved by the `SlashAppeal` settle hooks. Checks auto-ejection threshold (50% of the minimum bond for the operator's declared tier per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)) on the post-slash bonded balance; the returned `slashAmount` is the canonical figure consumed by the `Slashed` event on `SlashJudge`.
- The slash key is the operator's Ethereum address, bound to the NodeId by `registerNode`; no additional identity field is required for attribution.
- Exposes the region-scope read surface consumed by blacklist challenges (see [§ Blacklist violation](#blacklist-violation) item 7): `NodeInfo.regionHint`, `regionPrev`, `regionLastChanged`, and the governable `REGION_STABILITY_WINDOW`, read together through `ICapacityBondRegionView.regionScopeData(operator)`. A single narrow view keeps the scope predicate off `CapacityBond`'s own runtime-size budget.

**PaymentPool ([ADR 003](003-payments.md#adr-003-payment-model)):**

- No changes. Slashing and payment pools are independent by design.

**ContentBlacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)):**

- `SlashJudge` calls `ContentBlacklist.getHashEntry(region, blobHash)` once per scope leg — global, current region, and, while unripened, previous region (see [§ Blacklist violation](#blacklist-violation) item 7) — to verify blacklist status and compliance-window timing against the served response. The call goes through a narrow consumer-side view interface exposing that one function; no changes to the `ContentBlacklist` interface.

## Consequences

### Positive

- Both slashable offenses now have a concrete, gas-efficient on-chain evidence path. Slashing is no longer aspirational.
- `ecrecover` at 3,000 gas per signature is 100–300× cheaper than a Solidity Ed25519 library, making routine slashing economically viable even for small offenses.
- `slash_sig` reuses the existing NodeId-to-Ethereum-address binding in `CapacityBond` — no new on-chain registration step.
- `slash_sig` is mandatory and non-empty on every `ProbeResponse` and `StreamResponse`. Universal on-chain accountability is the protocol's single stance — there is no opt-out and no validation-mode difference between PoC and production for this field.
- The unified `SlashJudge` contract provides a single audit surface for all slashing logic.
- The [§ Challenge front-running mitigation](#challenge-front-running-mitigation-commitreveal) commit–reveal makes the 50% challenger reward un-front-runnable: the incentive for off-path witnesses survives independently of any transaction-ordering trust assumption (Arbitrum's sequencer today, decentralized sequencing or Timeboost later).

### Negative

- Nodes perform a secp256k1 EIP-712 signature on every `ProbeResponse` and `StreamResponse`, adding ~1ms of computation per message — negligible relative to network RTT, but nonzero.
- The `slash_sig` field adds ~65 bytes per `ProbeResponse` and `StreamResponse`. For probe messages this is meaningful overhead; for stream responses preceding multi-MB deliveries, it is negligible.
- Off-chain verifiers (clients, requesting nodes, third-party fraud detectors) must `ecrecover` and look up `CapacityBond.nodeIdOf(recovered)` to attribute a message to a NodeId, rather than verifying directly against the iroh key. These parties already maintain the binding cache for voucher attribution, so the marginal cost is one extra map lookup per verification.
- Cross-contract replay is prevented by per-contract EIP-712 domains, but implementers must configure domain separators correctly at deployment.
- The [§ SlashJudge Contract](#slashjudge-contract) `Slashed` event adds an `OffenseType` enum, a `nextSlashId` storage slot, and the per-offense `evidenceHash` preimage encoding to `SlashJudge`'s audit surface — small but real: every slash path emits the event atomically with `CapacityBond.slash()`, and the `OffenseType` ordering is contract-canonical. The ordinal is durable in four places — the persisted `SlashEscrowLib.SlashRecord.offenseType`, the two non-indexed `Slashed` events (whose `topic0` is unchanged by a reorder, so historical logs silently re-decode), the `evidenceHash` preimage that keys `usedEvidenceHash`, and transitively the `commitments` mapping — so any reordering after deployment is a data migration, not an edit. `SlashAppeal` is offense-agnostic and is *not* affected: it reads only the operator from `slashRecords` ([ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)). The ordinals are pinned by `InterfaceFreeze.t.sol`, since selectors alone cannot detect a reorder.
- The [§ Challenge front-running mitigation](#challenge-front-running-mitigation-commitreveal) commit–reveal makes every honest challenge two transactions (`commitChallenge` then `submit*Challenge`) separated by `MIN_REVEAL_DELAY`, adds a `commitments` mapping to `SlashJudge`'s storage and audit surface, and introduces an unbonded permissionless commit whose only abuse is gas-bounded storage spam. This is the accepted cost of removing the reward-MEV surface; the slash semantics, evidence checks, and `Slashed` record are unchanged.
