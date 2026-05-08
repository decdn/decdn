# ADR 030: Production BLAKE3 Corruption Verification (Single-Shot Merkle Proof)

**Date:** 2026-05-08
**Status:** Draft
**Touches:** [ADR 002](002-content-addressing.md), [ADR 005](005-protocol.md), [ADR 013](013-schema-evolution.md), [ADR 014](014-on-chain-verification.md), [ADR 016](016-contract-interactions.md), [ADR 027](027-distinct-client-receipts.md), [ADR 028](028-slashing-appeals.md)
**Required for:** Cryptographic (non-economic) corruption-slash adjudication

## Context

[ADR 014 §2](014-on-chain-verification.md#2-blake3-content-corruption--optimistic-challenge-response) specifies a single-round **optimistic** challenge-response for BLAKE3 content corruption: the challenger posts the node's signed `StreamResponse` plus a 100 TOKEN bond, and the node has 24 h to counter with a requester-signed `DeliveryReceipt`. The mechanism is sound under bond economics, but the deterrent is *economic*, not *cryptographic* — two known weaknesses follow:

- **Bond-economics dependency.** At low TOKEN price the 100 TOKEN griefing cost is small relative to the operational cost the node pays to defend. A cryptographic proof of corruption removes this dependency on TOKEN price.
- **Counter-evidence ambiguity.** A `DeliveryReceipt` proves "the requester accepted these bytes" — not "these bytes match the BLAKE3 hash". Real disputes can hinge on whether the requester verified before signing, whether the bytes were the *same* bytes the challenger received, etc. A cryptographic proof of corruption eliminates the judgement call.

ADR 014's earlier draft sketched a production upgrade — interactive keccak256 Merkle proofs over 1024-byte chunks — but the sketch was incomplete. The unanswered question, from the original §2:

> Binding Merkle root to BLAKE3 hash: The contract cannot independently verify that a keccak256 Merkle root corresponds to a given BLAKE3 hash (BLAKE3 is not available on-chain). … This commitment is the subject of a future ADR.

The half-design was dropped from ADR 014 in PR #386 so that ADR doesn't carry sketches of unbuilt designs. Issue [#387](https://github.com/decdn/decdn/issues/387) tracks the missing follow-up. ADR 027's §Decision opening (line ~24), §1 receipt-format table (line ~35), and §4 on-chain-anchoring intro (line ~116) had three previously-dangling forward-references to a "production path" anchor; they are repointed to ADR 027's own canonical MMR construction by this PR (since ADR 027's MMR is the right primitive for receipt batching but a different primitive — a plain binary keccak Merkle tree — is the right primitive for ADR 030's per-blob commitment, see §1). ADR 002 / ADR 003 also forward-reference this ADR as the resolution to the open production-verification question.

Issue #387 lists five questions the new ADR must answer:

1. **Where does the keccak256 Merkle root live?** Signed wire field, on-chain registry pre-registered by the publisher, or computed lazily by an indexer.
2. **How is the root bound to the BLAKE3 hash?**
3. **Tree shape.** 1024-byte leaves vs alternatives.
4. **Two-round protocol mechanics.** Challenger commitment, node counter-proof, resolution rules.
5. **Migration / coexistence with the optimistic path.** Both at once? Optimistic deprecated?

## Decision

The protocol introduces a **single-shot cryptographic corruption proof**: the challenger submits, in one transaction, a position `i` plus two cryptographic inclusion proofs — one for the chunk at position `i` under the node's signed keccak256 Merkle root `R` (which commits to the bytes the node actually delivered), and one for the canonical chunk at position `i` under the BLAKE3 blob hash `X` (which commits to the bytes the node *claimed* to deliver). If both inclusion proofs verify and the two chunks differ, corruption is proven; the contract slashes via `StakingRegistry.slash()` atomically. If any verification fails, the challenger is frivolous and forfeits the bond per the [ADR 014 §3 split](014-on-chain-verification.md#3-slashjudge-contract).

The dispute is the cryptographic upgrade replacing the "bond + 24 h counter window" of ADR 014 §2. The optimistic path remains in service for `cdn/client/v1`; the cryptographic path lives behind `cdn/client/v2`.

### 1. Wire format: `merkle_root` in signed `StreamResponse` (Tier 3 → `cdn/client/v2`)

A keccak256 chunk-tree root is a new **signed** field on `StreamResponse`. Per [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing), changing the signed-field set is a Tier 3 (major) ALPN bump:

```text
cdn/client/v1  StreamResponse signed fields: {hash, ok, rate_per_mb, total_bytes,
                                               channel_id, timestamp_us, redirect}
cdn/client/v2  StreamResponse signed fields: {hash, ok, rate_per_mb, total_bytes,
                                               channel_id, timestamp_us, redirect,
                                               merkle_root}
```

`merkle_root: bytes32` is the root of a **plain binary keccak256 Merkle tree** over the 1024-byte chunks of the bytes the node is about to deliver in this stream. Construction:

- **Leaf:** `keccak256(0x00 || chunk_i)` for chunk index `i ∈ [0, num_chunks)`. Each `chunk_i` is exactly 1024 bytes except the final chunk, which is the tail (≤ 1024 bytes).
- **Internal node:** `keccak256(0x01 || left || right)`. Left and right are the 32-byte child subtree roots.
- **Padding for non-power-of-2 chunk counts:** the leaf array is right-padded with `bytes32(0)` up to the next power of two. The padding sentinel is distinguishable from any honest leaf (since `keccak256(0x00 || any_chunk)` is overwhelmingly unlikely to equal `bytes32(0)`). Internal nodes whose subtree contains only padded leaves are computed via the standard rule `keccak256(0x01 || bytes32(0) || bytes32(0))`. Inclusion proofs traversing such positions verify normally; a chunk position `≥ num_chunks` is not a valid leaf for inclusion proofs (the contract rejects revealing such positions in §2).
- **No bagging.** Unlike [ADR 027 §4](027-distinct-client-receipts.md#aggregator-implementation--mmr-accumulator)'s MMR (which uses `0x02`-tagged peak-bagging because receipts arrive over time and need streaming append), ADR 030's `merkle_root` is computed once per blob at delivery time. The full chunk sequence is known at signing time, so a plain binary tree is the right primitive — and an inclusion proof is a sibling-hash path of length `ceil(log2(padded_num_chunks))`. The `0x00`/`0x01` leaf/internal-node domain tags are intentionally shared with ADR 027's MMR for cross-construction tag-domain consistency. The two roots are distinguishable on chain because ADR 027's `aggregateRoot` always traces through a `0x02`-tagged bagging fold (see [ADR 027 §4 Root extraction](027-distinct-client-receipts.md#aggregator-implementation--mmr-accumulator)) while ADR 030's `merkle_root` never does — only the *root* shapes differ, not the leaf encoding (both schemes leaf-tag with `0x00`).

#### Computation by the serving node

The serving node — *not* the publisher, not an indexer — computes `merkle_root` from the chunks it received and is about to deliver. For an origin-backed node, this is computed once at write time and persisted alongside the blob. For a cache node, it is computed streamingly during the cache-miss pull (one `keccak256` append per 1024-byte bao chunk arrival). iroh-blobs surfaces bao chunks at the leaf level, so the streaming computation is incremental. A node that received corrupt bytes from a peer would already have rejected those bytes at BLAKE3 verification before signing any `StreamResponse` — so honest nodes' `merkle_root` is well-defined and unique per blob hash X (BLAKE3 collision-resistance ⇒ at most one R consistent with X).

#### EIP-712 type for v2

```solidity
bytes32 constant STREAM_RESPONSE_V2_TYPEHASH = keccak256(
    "StreamResponseV2(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,"
    "bytes32 channelId,uint64 timestampUs,bytes32 redirect,bytes32 merkleRoot)"
);
```

The v2 typehash and the v1 `STREAM_RESPONSE_TYPEHASH` ([ADR 014 §1](014-on-chain-verification.md#eip-712-type-definitions)) coexist on `SlashJudge` — challenges from v1 deliveries verify against v1 typehash; v2 deliveries against v2. Both share the SlashJudge EIP-712 domain separator. The contract picks the typehash by routing on the entry point used (`submitCorruptionChallenge` vs `submitMerkleCorruptionChallenge` — see §3).

#### v1 / v2 ALPN coexistence

QUIC ALPN negotiation handles version selection per [ADR 013 § ALPN Version Negotiation](013-schema-evolution.md#alpn-version-negotiation). A node implementing v2 advertises both `cdn/client/v2` and `cdn/client/v1`; old clients fall back to v1, new clients prefer v2. There is no flag-day deprecation of v1 — see §4.

### 2. Single-shot corruption proof

The contract cannot directly verify `BLAKE3(bytes) == hash` for the full blob (gas-prohibitive at any non-trivial size, no EVM precompile). It *can* verify a single 1024-byte BLAKE3 chunk hash and walk a bounded bao parent path. The v2 dispute is structured to settle in one transaction over a single chunk position.

#### The two cryptographic commitments

Every corruption claim is grounded in two binary trees over the *same* underlying chunk sequence (1024-byte chunks; tail chunk may be shorter):

- **BLAKE3 bao tree** with root = `streamResponse.hash` (= the blob hash X). Leaves are 1024-byte BLAKE3 chunk hashes; internal nodes are BLAKE3 in keyed/parent mode (the bao spec). Inclusion proofs are *bao parent paths*: a leaf chunk plus the sibling-hash sequence up to the root.
- **Plain binary keccak256 Merkle tree** with root = `streamResponse.merkle_root` (= R). Construction defined in §1 above; same 1024-byte leaf size and same chunk-position semantics as the bao tree. Inclusion proofs are sibling-hash paths from leaf to root.

For honest delivery, both trees commit to the same chunks at the same positions, and `BLAKE3(chunks) = X`. For corrupt delivery, the node delivered chunks that do *not* hash to X under BLAKE3, but the node nonetheless signs a `merkle_root` R over those (corrupt) chunks. R cryptographically pins the node to a specific chunk sequence; the contract's job during a dispute is to verify that some chunk position has `chunk_i_under_R ≠ chunk_i_under_X-canonical`.

The two trees share a leaf-position semantic: `position_i` under R refers to the same chunk slot as `position_i` under bao tree of X. A corrupt delivery has at least one position where the trees commit to different chunks; the challenger names any such position and proves the disagreement directly.

#### Protocol

The challenger submits, in one transaction:

```text
inputs to submitMerkleCorruptionChallenge:
  challengedNode        — operator address being slashed if corruption is proven
  nodeId                — node identity ed25519 key (per ADR 019)
  streamResponseV2Data  — ABI-encoded v2 fields (hash, ok, ratePerMb, totalBytes,
                          channelId, timestampUs, redirect, merkleRoot)
  streamSlashSig        — node's EIP-712 signature over the v2 typehash digest
  position              — chunk index i, 0 ≤ i < num_chunks
  chunkDelivered        — bytes the node delivered at position i
                          (1024 bytes for i < num_chunks - 1, tail-length for i = num_chunks - 1)
  keccakPath            — bytes32[] of length ceil(log2(padded_num_chunks))
  chunkCanonical        — canonical chunk at position i under X
                          (same length rule as chunkDelivered)
  baoPath               — bytes32[] equal in length to position i's bao-tree depth
  initialBond           — TOKEN bond, per §5
```

The contract executes atomically:

1. **Recover and verify signature.** Decode `streamResponseV2Data` to recover `(X, ok, ratePerMb, totalBytes, channelId, timestampUs, redirect, R)`. Compute `numChunks = ceil(totalBytes / 1024)`. Verify `streamSlashSig` over the v2 EIP-712 typehash digest binds to the operator key registered against `challengedNode` ([ADR 019 § Node identity](019-node-onboarding.md)). Verify `position < numChunks` and that `chunkDelivered.length` and `chunkCanonical.length` both equal the expected length for `position` (1024 except tail).
2. **Verify R-side inclusion.** Compute `leafR = keccak256(0x00 || chunkDelivered)`. Walk `keccakPath` from `leafR` upward — at each step apply `keccak256(0x01 || left || right)` with the sibling placed left or right by the `position` bit at that depth, padded against `bytes32(0)` siblings for ranges past `numChunks`. The final value MUST equal `R`. Failure ⇒ challenger is frivolous; bond forfeited per [ADR 014 §3 split](014-on-chain-verification.md#3-slashjudge-contract) (50% burned, 50% to defender).
3. **Verify X-side inclusion.** Compute `leafX = blake3_chunk_hash(chunkCanonical)` (BLAKE3 chunk-mode flags, 7-round, per the bao spec). Walk `baoPath` from `leafX` upward — at each step apply BLAKE3 parent-mode hashing with the sibling placed left or right by the `position` bit at that depth, accounting for the bao tree's variable-depth handling of non-power-of-2 chunk counts. The final value MUST equal `X`. Failure ⇒ frivolous (same outcome as §2-step 2).
4. **Verify the inequality.** `chunkDelivered != chunkCanonical`. Equality ⇒ no corruption at position `i` — challenger frivolous (same outcome).
5. **Slash.** Call `StakingRegistry.slash(challengedNode, OffenseType.Corruption)` and emit `Slashed(slashId, operator, OffenseType.Corruption, amount, evidenceHash)` atomically. The challenger receives the [ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract) challenger share (default 50% of slash) and their initial bond is returned.

The `evidenceHash` preimage for v2 corruption is `keccak256(abi.encode(uint8(OffenseType.Corruption), streamStructHashV2))` — **the same preimage rule as ADR 014 §3 with a v2 struct hash**. Because v2's struct hash differs from v1's (extra `merkleRoot` field), the `evidenceHash` is naturally distinct. No new `OffenseType` enum entry is introduced — see §4.

#### Soundness

The protocol is sound by direct cryptographic argument, with no interactive or game-theoretic component:

- The R-side inclusion proof pins `chunkDelivered` as the chunk at position `i` in the bytes the node committed to via the signed `merkle_root`. A pre-image other than the actual delivered chunk requires a keccak256 collision.
- The X-side inclusion proof pins `chunkCanonical` as the chunk at position `i` under the BLAKE3 hash X. A pre-image other than the canonical chunk requires a BLAKE3 collision.
- A defender that delivered honestly has `chunkDelivered = chunkCanonical` at every position; the inequality check fails, the dispute resolves frivolous, and no slash occurs.
- A defender that delivered corruptly has at least one position `i` with `chunkDelivered != chunkCanonical`; the challenger names any such position and the inequality check passes, proving corruption.

The challenger's job off-chain is to find any one such position. Since the challenger has the delivered bytes (received over `cdn/client/v2`) and can fetch the canonical bytes from any other peer or the origin, a byte-by-byte comparison locates a corrupt position in O(num_chunks) time. Multi-corruption patterns do not change the analysis — the challenger needs *any one* corrupt position, not all of them.

#### Gas cost

| Step | On-chain work | Estimated gas |
|---|---|---|
| Signature recovery + length checks | ABI-decode v2 struct, EIP-712 typehash + ecrecover, validate `position` and chunk lengths | ~80k |
| R-side keccak Merkle path | Hash `chunkDelivered` once + `ceil(log2(padded_num_chunks))` keccak256 hashes (~3k each) | ~80k for `numChunks ≤ 2²⁰` |
| X-side bao parent path | One BLAKE3 chunk hash (~150k) + up to N BLAKE3 parent-mode hashes (~150k each) where N is the leaf's depth in the bao tree | ~3.1 M for N=20 (1 GiB blob) |
| Inequality check + slash + emit | Compare chunks, call `StakingRegistry.slash()`, settle bond ledger, emit `Slashed` | ~250k |
| **Total per dispute** (1 GiB blob, N=20 levels: 1 transaction) | | **~3.5 M gas** |

At Arbitrum One ~0.1 gwei effective L2 fee, **~$0.35 per dispute**. The on-chain BLAKE3 step dominates audit complexity and gas cost; library selection and audit are gating prerequisites (see §Forward references issue 1).

### 3. Contract surface: `SlashJudge.submitMerkleCorruptionChallenge`

A new entry point on `SlashJudge` parallel to the existing `submitCorruptionChallenge`. Both routes feed the unified `OffenseType.Corruption` through `StakingRegistry.slash()`.

```solidity
interface ISlashJudge {
    // ... existing entry points unchanged ...

    /// Submit a v2 Merkle corruption proof in a single transaction. Verifies
    /// the v2 StreamResponse signature, the keccak Merkle inclusion proof of
    /// the delivered chunk against R, the bao parent-path inclusion proof of
    /// the canonical chunk against X, and the inequality. On success: slashes
    /// challengedNode via StakingRegistry.slash(OffenseType.Corruption) and
    /// emits Slashed atomically. On any verification or inequality failure:
    /// the bond is forfeited per the ADR 014 §3 split (50% burned, 50% to
    /// defender) and ChallengeFrivolous is emitted.
    function submitMerkleCorruptionChallenge(
        address  challengedNode,
        bytes32  nodeId,
        bytes    calldata streamResponseV2Data,  // ABI-encoded v2 fields incl. merkleRoot
        bytes    calldata streamSlashSig,
        uint64   position,
        bytes    calldata chunkDelivered,        // length per §2 step 1
        bytes32[] calldata keccakPath,           // length ceil(log2(padded_num_chunks))
        bytes    calldata chunkCanonical,        // same length as chunkDelivered
        bytes32[] calldata baoPath               // length = bao depth at position
    ) external returns (uint256 slashId);

    /// Emitted when submitMerkleCorruptionChallenge resolves frivolous.
    /// On the corruption-proven path, ADR 014 §3's existing Slashed event
    /// is the on-chain record; no separate event is emitted by this entry point.
    event ChallengeFrivolous(
        address  indexed challenger,
        address  indexed challengedNode,
        bytes32  blobHash,
        uint64   position,
        uint256  forfeitedBond
    );
}
```

`StakingRegistry`, `SafetyReserve`, and `OffenseType` are unchanged. ADR 016 §3 SlashJudge call-graph row gains the new entry point; ADR 016 §1 contract inventory has no change (still `SlashJudge` with the same constructor parameters; the `challengeBond` and `counterEvidenceWindow` constructor parameters apply to the optimistic path only and are unaffected).

The `maxActiveChallengesPerNode` cap ([ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract)) is no longer load-bearing for v2 disputes — they resolve atomically in one transaction and retain no state. It still applies as a per-tx-burst rate limit shared with v1 (PoC default 10).

Because v2 challenges resolve atomically in one transaction, no per-dispute state is retained on `SlashJudge` between blocks. There is no chess clock, no per-round bond ledger, no bisection state machine, and no `forfeitDispute` / `bisectMove` / `terminalReveal` / `resolveMerkleCorruption` entry points.

### 4. Migration and coexistence with the optimistic path

- **`cdn/client/v1` keeps the optimistic path.** Existing v1 deliveries continue to use `submitCorruptionChallenge` / `counterChallenge` / `resolveChallenge` per ADR 014 §2. v1 nodes need no changes.
- **`cdn/client/v2` mandates `merkle_root` in `StreamResponse`.** Disputes against v2 deliveries use `submitMerkleCorruptionChallenge` (cryptographic, atomic). Calling `submitCorruptionChallenge` with a v2 struct must revert: the v1 typehash will not match.
- **Both paths feed unified `OffenseType.Corruption`.** No new enum entry — the [ADR 014 §3 append-only invariant](014-on-chain-verification.md#3-slashjudge-contract) is preserved. A node that earns one optimistic-slash on v1 and one Merkle-slash on v2 escalates to the 15% tier on the second offense, not 5% twice. Reputation impact ([ADR 008 §3](008-reputation.md)) is identical between paths.
- **No flag-day deprecation.** v1 stays in service indefinitely; sunset is a future ADR (separate decision tied to v2 adoption metrics). The cost is double-audit-surface for the lifetime of v1 — see §Risks.
- **Reputation gauge unchanged.** ADR 008 §3 BLAKE3-correctness signal (40% weight) is binary on `OffenseType.Corruption`; it does not distinguish v1 vs v2 path.

### 5. Bond economics and griefing analysis

#### Initial bond

Reuse the ADR 014 §2 challenge bond: **100 TOKEN**, governance-tunable `[10, 1000]` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics). No premium — the v2 dispute resolves atomically and is *cheaper* to run honestly than the optimistic path's 24 h counter window, so a higher bond is unjustified.

The bond is forfeited atomically on any verification failure (v2 step 2, 3, or 4) per the ADR 014 §3 split (50% burned, 50% to defender). On the corruption-proven branch the bond is returned to the challenger and the challenger receives the [ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract) challenger share of the slash (default 50%).

#### `maxActiveChallengesPerNode` cap

Inherited from [ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract): **10 concurrent challenges per challenged node** (PoC default). For v2 the cap functions as a per-tx-burst rate limit only — there is no in-flight state to retain. v1 and v2 share this cap.

#### Griefing

- **Frivolous challenges.** A challenger who submits with `chunkDelivered == chunkCanonical` at the named position, a malformed inclusion proof, or a bad signature, loses the bond per the §5 forfeiture rule. Attack cost is the bond (default 100 TOKEN), which dominates the contract's gas cost (~$0.35 at Arbitrum One). Griefing is bounded by attacker bond loss × `maxActiveChallengesPerNode`.
- **Vacation griefing eliminated.** Unlike a multi-day interactive dispute, there is no defendant move to time out and no chess clock. The defender does not need to be online or run watchtower infrastructure to be defended against frivolous challenges — frivolous challenges resolve frivolous on-chain without defendant action.
- **Multi-position corruption.** A defender who corrupts many positions does not fan out into many separate slashable offenses — `OffenseType.Corruption` is per-delivery, not per-chunk. One v2 dispute per delivery, regardless of corruption count.

### 6. Interaction with stake unbonding

No `unbondingPeriod` change is required. The pre-existing invariant from [ADR 014 §3 Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period) — `MAX_EVIDENCE_AGE_US < unbondingPeriod` — already holds at the prior 7-day default (5 d evidence age vs 7 d unbonding; margin: 2 days). The v2 dispute resolves atomically within the evidence-age window; no further cross-parameter constraint is introduced.

## Consequences

### Positive

- **Cryptographic dispatch on corruption.** No more reliance on bond economics or counter-evidence ambiguity for production-grade slashing. A node that delivered corrupt bytes loses the dispute deterministically. A node that delivered honestly cannot be slashed by any frivolous challenge (the inclusion proofs cannot be forged).
- **Atomic resolution.** One transaction, no chess clock, no multi-day dispute lifecycle. No watchtower-bot infrastructure required for operators to be defended against frivolous claims.
- **Shared keccak Merkle tag domain with ADR 027.** Leaf and internal-node domain tags (`0x00` / `0x01`) match ADR 027 §4's MMR — auditors review one tag-domain scheme, not two. The structures themselves differ (plain binary tree here vs MMR with bagging there) because the use cases differ (commit-once-per-blob vs streaming-append-receipts).
- **No new offense type.** `OffenseType.Corruption` stays unified; the append-only enum invariant from ADR 014 §3 is preserved. Reputation impact, slashing schedule, appeal flow are identical between v1 and v2 paths.
- **Failed challenges still slash the loser.** DoS via repeated frivolous challenges is bounded by attacker bond loss × max-concurrent-disputes cap.
- **Gauge / receipts toolchain partial reuse.** The keccak primitive (chunk leaf hash, internal-node hash) is shared with the ADR 027 MMR machinery operators already run for receipts; the tree-shape composition is different (plain binary here, MMR there) but is straightforward.

### Negative

- **Tier 3 wire-format break.** v2 ALPN bump is coordinated rollout per [ADR 013 § Three Tiers of Evolution](013-schema-evolution.md#tier-3--major-alpn-version-bump). Old peers continue serving v1 indefinitely; double-audit surface is the cost.
- **Per-dispute terminal-round gas ~3–4 M.** Acceptable on Arbitrum at ~$0.35/dispute, but the on-chain BLAKE3 step is the highest-cost component and dominates audit complexity.
- **Cache-pull computation.** Cache nodes must keccak-Merkle-tree the chunks streamingly during pull (one keccak leaf hash per 1024-byte bao chunk arrival, plus internal-node hashes as siblings become available). For non-power-of-2 chunk counts a small set of zero-padded leaves are appended at the right end before signing. At ~30 MB/s pull throughput this is < 1% of CPU per stream — negligible but non-zero.

### Risks

- **On-chain BLAKE3 audit surface.** Few production-grade Solidity BLAKE3 implementations exist; G-function constants, 7-round (not 10) chunk-leaf mode, bao parent-mode flag bits, and chunk-vs-parent block-byte differences all need audit. Library selection is a hard prerequisite — see §Forward references issue 1.
- **Double-audit surface for v1 + v2.** The optimistic path stays in service indefinitely. Two corruption code paths means two audit budgets. Sunset deferral is intentional, but the cost is real.
- **Challenger must obtain canonical bytes off-chain.** The v2 dispute requires the challenger to submit `chunkCanonical` plus a bao parent path. Both are derivable from any honest copy of the canonical bytes (origin server, another cache node, or any peer that has ingested the blob). No new trust assumption is introduced — the challenger already needs canonical bytes to detect corruption — but the challenger's tooling must integrate at least one canonical-source fallback.

## Alternatives Considered

- **Interactive bisection over the bao tree.** Rejected. The bisection design considered earlier in this ADR's drafting collapsed under analysis: the proposed "agreement → defender wins" termination rule is unsound (a corrupt defender posts canonical bao subtrees at level 0, both halves match the challenger's canonical postings, the rule triggers, and the defender wins immediately — no terminal step is ever reached). Eager per-round BLAKE3 verification doesn't fix this — both parties' postings collapse to canonical X subtrees by collision-resistance, leaving no signal for the bisection to follow. A challenger-driven (Optimism BoLD) variant maps onto a single-shot proof for this use case (the challenger always knows a corrupt position from off-chain byte comparison; bisection has no intermediate disagreement to narrow on). Single-shot reveal is the right primitive; interactive bisection adds chess-clock infrastructure, vacation-griefing risk, and a multi-day dispute lifecycle without buying any soundness or cost benefit.

- **Option B — publisher pre-registers `(blob_hash, merkle_root)` in a `BlobRegistry` contract at ingest.** Rejected: ADR 002's content-claim model has no canonical publisher per blob hash. Multi-claim semantics ([ADR 002 § Multi-claim semantics](002-content-addressing.md#multi-claim-semantics)) explicitly allow any number of independent namespaces to claim the same hash. There is no single party to register R; pre-registration would require coordination that does not exist. Also fails for cache nodes — the cache-miss pull computes R on the fly, decoupled from any publisher registration.

- **Option C — lazy indexer-computed root, posted on first challenge.** Rejected: introduces a new trusted role outside the protocol (the indexer), and ADR 022's discovery model treats the on-chain origin directory as the *deterministic* fallback, not an indexer. Indexer trust model is exactly what the protocol is designed to avoid.

- **Option D — ZK proof of `BLAKE3(B) == X AND keccak256-binary-Merkle(B) == R`.** Rejected for v1: a Groth16/PLONK BLAKE3 circuit is a substantial engineering investment, no audited implementation exists in the public ecosystem at the time of writing, and the single-shot inclusion-proof path is sufficient at expected dispute volume. Worth filing as a future-work seam — when an audited BLAKE3 ZK circuit exists (industry effort tracking), revisit. The single-shot path can coexist with a ZK fast-path: parties can voluntarily settle disputes by submitting a single ZK proof in lieu of the inclusion proofs, falling back to per-chunk reveal on prover failure.

- **Single-shot on-chain BLAKE3-of-whole-blob.** Rejected: gas-prohibitive at any non-trivial blob size (~150 k gas/chunk × millions of chunks for a multi-GB blob = unboundedly expensive). The whole point of the per-chunk inclusion proof is to amortize on-chain BLAKE3 cost to one chunk.

## ADRs Affected

- **[ADR 002](002-content-addressing.md):** Inline forward-reference to "production upgrade path" (lines 113, 118) repointed at this ADR. The §Open Question on production verification is resolved here.
- **[ADR 003](003-payments.md):** Inline forward-reference at §Corrupted delivery (line ~222) and §Negative consequences (line ~154) updated to mention this ADR alongside ADR 014 §2 — both paths are referenced.
- **[ADR 005](005-protocol.md):** §`cdn/client/v1` paid delivery protocol gains a note that `cdn/client/v2` extends the signed-field set with `merkle_root: bytes32` per this ADR. v1 wire descriptions are unchanged — ADR 005 stays the v1 baseline.
- **[ADR 013](013-schema-evolution.md):** §Signed Field Freezing table gains a v2 row for `StreamResponse` listing the v2 signed-field set with `merkle_root`. This ADR is cited as the Tier 3 trigger event.
- **[ADR 014](014-on-chain-verification.md):** §2 "Future evolution" stub is replaced with a one-line cross-reference to this ADR (the optimistic path remains documented in the rest of §2). §3 ISlashJudge interface block extends with `submitMerkleCorruptionChallenge`. §3 gas-estimates table gains a row for the v2 dispute. ADRs Affected gains an ADR 030 entry.
- **[ADR 016](016-contract-interactions.md):** §3 SlashJudge function table appends `submitMerkleCorruptionChallenge` with the same `nonReentrant`, checks-effects-interactions, TOKEN bond-deposit guards. §1 Contract Inventory row 14 (SlashJudge constructor parameters) is unchanged — `challengeBond` and `counterEvidenceWindow` cover both paths' bond mechanics.
- **[ADR 027](027-distinct-client-receipts.md):** Three previously-broken `#production-path-interactive-keccak256-merkle-proof-future-adr` anchor links (in §Decision opening line ~24, §1 receipt-format table line ~35, §4 on-chain-anchoring intro line ~116) repointed to ADR 027 §4's own `#aggregator-implementation--mmr-accumulator` anchor — keeping ADR 027 as the canonical home of the MMR construction. ADR 030 uses a different but tag-domain-compatible primitive (plain binary keccak Merkle tree, leaves `keccak256(0x00 || chunk)`, internal nodes `keccak256(0x01 || left || right)`, no bagging — see §1 rationale).
- **[ADR 028](028-slashing-appeals.md):** §1 confirmation that Merkle-corruption slashes (v2 path) have identical appealability to optimistic-corruption slashes (v1 path), since both feed the unified `OffenseType.Corruption`. No scope change.
- **[`adr/architecture.md`](architecture.md):** Chapter 5 — Verification & enforcement: insert this ADR after ADR 014 in the chapter listing. Numeric ADR index gains a one-line entry. ADR 014's one-line summary is amended to mention "production path in ADR 030".
- **[`adr/README.md`](README.md):** Reading-order pandoc command updated to include `030-blake3-merkle-verification.md` between `014-...` and `008-...` in the verification chapter sequence.

## Forward references (follow-up issues)

Per issue [#387](https://github.com/decdn/decdn/issues/387) "Definition of done":

1. **`solidity-blake3` library selection and audit.** Survey existing implementations (Sovrun, mempirate / blake3-solidity, others). Benchmark on Arbitrum One under realistic L2 fee conditions. Decide vendor-vs-fork-and-audit. Acceptance criteria: an audited library with verified ~150 k gas/chunk benchmark on testnet. **Prerequisite for issue 2.**
2. **`SlashJudge.submitMerkleCorruptionChallenge` Solidity implementation.** v2 EIP-712 signature recovery, keccak Merkle inclusion-proof verification, BLAKE3 chunk-mode + bao parent-path verification, atomic `Slashed` / `ChallengeFrivolous` event emission, and bond settlement per the ADR 014 §3 split. Updates `ISlashJudge` in [ADR 016 §3](016-contract-interactions.md#3-cross-contract-call-graph). Depends on issue 1.
3. **`cdn/client/v2` ALPN handler.** Rust crate work in `crates/protocol` (Tier 3 message types: v2 `StreamResponse` with `merkle_root`), `crates/node` (handler dispatch on negotiated ALPN), `crates/cache` (streaming bao chunk → keccak binary Merkle tree computation during pull, with right-padding to next power of 2 before signing). Operator runbook update via `appendix-operator-upgrade-path.md`.
