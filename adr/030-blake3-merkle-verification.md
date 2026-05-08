# ADR 030: Production BLAKE3 Corruption Verification (Interactive Merkle Bisection)

**Date:** 2026-05-08
**Status:** Draft
**Touches:** [ADR 002](002-content-addressing.md), [ADR 005](005-protocol.md), [ADR 013](013-schema-evolution.md), [ADR 014](014-on-chain-verification.md), [ADR 016](016-contract-interactions.md), [ADR 026](026-gauge-boost-tokenomics.md), [ADR 027](027-distinct-client-receipts.md), [ADR 028](028-slashing-appeals.md)
**Required for:** Cryptographic (non-economic) corruption-slash adjudication

## Context

[ADR 014 §2](014-on-chain-verification.md#2-blake3-content-corruption--optimistic-challenge-response) specifies a single-round **optimistic** challenge-response for BLAKE3 content corruption: the challenger posts the node's signed `StreamResponse` plus a 100 TOKEN bond, and the node has 24 h to counter with a requester-signed `DeliveryReceipt`. The mechanism is sound under bond economics, but the deterrent is *economic*, not *cryptographic* — two known weaknesses follow:

- **Bond-economics dependency.** At low TOKEN price the 100 TOKEN griefing cost is small relative to the operational cost the node pays to defend. A cryptographic proof of corruption removes this dependency on TOKEN price.
- **Counter-evidence ambiguity.** A `DeliveryReceipt` proves "the requester accepted these bytes" — not "these bytes match the BLAKE3 hash". Real disputes can hinge on whether the requester verified before signing, whether the bytes were the *same* bytes the challenger received, etc. A cryptographic proof of corruption eliminates the judgement call.

ADR 014's earlier draft sketched a production upgrade — interactive keccak256 Merkle proofs over 1024-byte chunks — but the sketch was incomplete. The unanswered question, from the original §2:

> Binding Merkle root to BLAKE3 hash: The contract cannot independently verify that a keccak256 Merkle root corresponds to a given BLAKE3 hash (BLAKE3 is not available on-chain). … This commitment is the subject of a future ADR.

The half-design was dropped from ADR 014 in PR #386 so that ADR doesn't carry sketches of unbuilt designs. Issue [#387](https://github.com/decdn/decdn/issues/387) tracks the missing follow-up. ADR 027 §1 and §4 had previously dangling forward-references to a "production path" anchor; they are repointed to ADR 027's own canonical MMR construction by this PR (since ADR 027's MMR is the right primitive for receipt batching but a different primitive — a plain binary keccak Merkle tree — is the right primitive for ADR 030's per-blob commitment, see §1). ADR 002 / ADR 003 also forward-reference this ADR as the resolution to the open production-verification question.

Issue #387 lists five questions the new ADR must answer:

1. **Where does the keccak256 Merkle root live?** Signed wire field, on-chain registry pre-registered by the publisher, or computed lazily by an indexer.
2. **How is the root bound to the BLAKE3 hash?**
3. **Tree shape.** 1024-byte leaves vs alternatives.
4. **Two-round protocol mechanics.** Challenger commitment, node counter-proof, resolution rules.
5. **Migration / coexistence with the optimistic path.** Both at once? Optimistic deprecated?

## Decision

The protocol introduces an **interactive bisection-based corruption proof** layered on top of two cryptographic commitments: the BLAKE3 bao tree (intrinsic to the blob's content addressing) and a plain binary keccak256 Merkle tree over 1024-byte chunks committed by the serving node at delivery time. Disputes bisect down to a single 1024-byte chunk position; the on-chain terminal step verifies *one* BLAKE3 chunk hash and *one* bao parent path against the blob hash, and *one* keccak inclusion proof against the node's committed root. Both proofs must pass; the loser is slashed under the existing `OffenseType.Corruption` schedule.

The dispute is the cryptographic upgrade replacing the "bond + 24 h counter window" of ADR 014 §2. The optimistic path remains in service for `cdn/client/v1`; the production path lives behind `cdn/client/v2`.

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

- **Leaf:** `keccak256(0x00 || chunk_i)` for chunk index `i ∈ [0, num_chunks)`. Each `chunk_i` is exactly 1024 bytes except the final chunk, which is the tail (≤ 1024 bytes — see §3 `terminalReveal`).
- **Internal node:** `keccak256(0x01 || left || right)`. Left and right are the 32-byte child subtree roots.
- **Padding for non-power-of-2 chunk counts:** the leaf array is padded with `bytes32(0)` at the right end up to the next power of two. The padding sentinel is distinguishable from any honest leaf (since `keccak256(0x00 || any_chunk)` is overwhelmingly unlikely to equal `bytes32(0)`).
- **No bagging.** Unlike [ADR 027 §4](027-distinct-client-receipts.md#aggregator-implementation--mmr-accumulator)'s MMR (which uses `0x02`-tagged peak-bagging because receipts arrive over time and need streaming append), ADR 030's `merkle_root` is computed once per blob at delivery time. The full chunk sequence is known at signing time, so a plain binary tree is the right primitive — and it bisects cleanly under simple `keccak256(0x01 || left || right)` parent composition (see §2). The `0x00`/`0x01` leaf/internal-node domain tags are intentionally shared with ADR 027's MMR for cross-construction tag-domain consistency; the absence of `0x02` makes plain-binary leaves and MMR leaves distinguishable on the wire.

#### Computation by the serving node

The serving node — *not* the publisher, not an indexer — computes `merkle_root` from the chunks it received and is about to deliver. For an origin-backed node, this is computed once at write time and persisted alongside the blob. For a cache node, it is computed streamingly during the cache-miss pull (one `keccak256` append per 1024-byte bao chunk arrival). iroh-blobs surfaces bao chunks at the leaf level, so the streaming computation is incremental. A node that received corrupt bytes from a peer would already have rejected those bytes at BLAKE3 verification before signing any `StreamResponse` — so honest nodes' `merkle_root` is well-defined and unique per blob hash X (BLAKE3 collision-resistance ⇒ at most one R consistent with X).

#### EIP-712 type for v2

```solidity
bytes32 constant STREAM_RESPONSE_V2_TYPEHASH = keccak256(
    "StreamResponseV2(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,"
    "bytes32 channelId,uint64 timestampUs,bytes32 redirect,bytes32 merkleRoot)"
);
```

The v2 typehash and the v1 `STREAM_RESPONSE_TYPEHASH` ([ADR 014 §1](014-on-chain-verification.md#eip-712-type-definitions)) coexist on `SlashJudge` — challenges from v1 deliveries verify against v1 typehash; v2 deliveries against v2. Both share the SlashJudge EIP-712 domain separator. The contract picks the typehash by routing on the entry point used (`submitCorruptionChallenge` vs `submitMerkleCorruptionChallenge` — see §4).

#### v1 / v2 ALPN coexistence

QUIC ALPN negotiation handles version selection per [ADR 013 § ALPN Version Negotiation](013-schema-evolution.md#alpn-version-negotiation). A node implementing v2 advertises both `cdn/client/v2` and `cdn/client/v1`; old clients fall back to v1, new clients prefer v2. There is no flag-day deprecation of v1 — see §5.

### 2. Root-to-BLAKE3 binding: bisection over the bao tree

The contract cannot directly verify `BLAKE3(bytes) == hash` for the full blob (gas-prohibitive at any non-trivial size, no EVM precompile). It *can* verify a single 1024-byte BLAKE3 chunk hash and walk a bounded bao parent path. The dispute is structured to drive a disagreement down to exactly one chunk, then verify on-chain.

#### The two cryptographic commitments

Every disputed delivery is committed in two binary trees over the *same* underlying chunk sequence (1024-byte chunks; tail chunk may be shorter):

- **BLAKE3 bao tree** with root = `streamResponse.hash` (= the blob hash X). Leaves are 1024-byte BLAKE3 chunk hashes; internal nodes are BLAKE3 in keyed/parent mode (the bao spec). Inclusion proofs are *bao parent paths*: a leaf chunk plus the sibling-hash sequence up to the root.
- **Plain binary keccak256 Merkle tree** with root = `streamResponse.merkle_root` (= R). Construction defined in §1 above; same 1024-byte leaf size and same chunk-position semantics as the bao tree. Inclusion proofs are sibling-hash paths from leaf to root.

For honest delivery, both trees commit to the same chunks at the same positions, and `BLAKE3(chunks) = X`. For corrupt delivery, the node delivered chunks that do *not* hash to X under BLAKE3, but the node nonetheless signs a `merkle_root` R over those (corrupt) chunks. R cryptographically pins the node to a specific chunk sequence; the contract's job during dispute is to *prove* that some chunk position has `chunk_i_under_R ≠ chunk_i_under_X-canonical`.

The two trees share a leaf-position semantic: `position_i` under R refers to the same chunk slot as `position_i` under bao tree of X. Disagreement is therefore localised to a single position once bisection completes.

#### The bisection game (FaultDisputeGame pattern)

Modeled on the FaultDisputeGame pattern from Optimism / Arbitrum BoLD. The game narrows the disputed chunk range to one position via bisection, then verifies on-chain that the parties' claimed chunks at that position differ AND that one party's claim is consistent with X while the other's is consistent with R.

```text
state: R = claimed merkle_root, X = claimed blob_hash, num_chunks = ceil(total_bytes / 1024)
       rangeStart, rangeEnd  (current disputed chunk-position range)
       parentKeccak           (claimed keccak subtree root for the current range, under R)
       parentBaoChallenger    (challenger's claimed bao subtree root for current range, under X)
       parentBaoDefender      (defender's claimed bao subtree root for current range, under X)

round 0  — Challenger opens dispute. Posts streamResponseV2 + slash_sig + initial bond.
           Contract verifies signature, recovers (X, R, num_chunks).
           Initial state: rangeStart=0, rangeEnd=padded_num_chunks, parentKeccak=R,
           parentBaoChallenger=X, parentBaoDefender=X.
           Challenger asserts: ∃ position i with chunk_i_under_R ≠ chunk_i_under_X.

rounds 1..N where N = ceil(log2(num_chunks)):
  The MOVING SIDE (alternating per ADR 014 §3 chess-clock semantics; challenger
  moves first) posts (left_keccak, right_keccak, left_bao, right_bao) for the
  current range:
    • Contract verifies left_keccak and right_keccak compose to parentKeccak via
      keccak256(0x01 || left_keccak || right_keccak) == parentKeccak.
      This is a hard check — failure ⇒ moving side loses immediately.
    • Contract does NOT verify bao composition on-chain (would require BLAKE3
      parent-mode hash per round, ~150k gas each — expensive). The bao subtree-
      root claims are stored "lazily"; their honesty is verified at the terminal
      step via a cross-side parent-path check against X.

  At the END of each round (after both sides have moved at least once at this
  level), the contract picks the half to recurse into:
    • If both sides' (left_bao_C, right_bao_C) tuples agree with (left_bao_D,
      right_bao_D), there is no bao disagreement at this level — challenger has
      no remaining claim. Defender wins.
    • Otherwise, the contract recurses into the half whose bao subtree roots
      disagreed. parentKeccak, parentBaoChallenger, parentBaoDefender are
      updated to the chosen half's left or right values.

  After N rounds, rangeStart + 1 == rangeEnd (one chunk position).

terminal round N+1 — both sides reveal their claimed chunk bytes at position i,
   plus a bao parent path of length N to X and a keccak Merkle path of length
   ceil(log2(padded_num_chunks)) to R.
  The contract:
   • verifies challenger's chunk c_C composes via keccak256(0x00 || c_C) +
     keccak Merkle path to R; if fail, challenger loses immediately.
   • verifies defender's chunk c_D similarly to R; if fail, defender loses
     immediately.
   • verifies challenger's chunk c_C composes via BLAKE3 chunk hash + bao
     parent path to X; if fail, challenger's bao chain was a lie at some
     intermediate round, challenger loses immediately.
   • verifies defender's chunk c_D similarly to X; if fail, defender's bao
     chain was a lie OR defender's chunk doesn't BLAKE3 to canonical X-leaf
     — defender loses (corruption proven OR fraud-proof loss, same outcome).
   • If both verifications pass, the contract compares c_C and c_D:
       - c_C == c_D: no actual chunk-level disagreement. Challenger
         frivolous, loses bond.
       - c_C != c_D AND c_D verifies to BOTH R and X: contradicts BLAKE3
         collision-resistance — should not occur in a real dispute. Treat as
         frivolous; both terminal proofs were produced honestly so neither side
         cheated procedurally. Defender wins (challenger had no real claim).
       - Cannot occur otherwise: c_D verifies to R but not to X means
         defender's bytes don't BLAKE3 to X — corruption proven. Defender
         loses (slashed).
```

**Why bao-side lazy verification is sound.** A defender who lies about an intermediate bao subtree root must eventually post a bao parent path at the terminal step that composes to X. The lie cannot be sustained — at some level the chain of intermediate commitments will fail to compose to X via BLAKE3 parent-mode hashing of the actual chunk. The lie is detected at the terminal step rather than at the round it was made; the only cost is one extra round of game tree (the lying side could have terminated earlier by being honest). This is the same pattern used in optimistic-rollup fault-proof games where intermediate state-trie commitments are verified only at the final step.

The chess-clock (§3) bounds total dispute lifecycle. Per-round contract-state cost is bounded — each round writes a small constant number of `bytes32` to storage (subtree-root pairs + clock state).

#### Gas cost breakdown

The bisection is deliberately split into **cheap intermediate rounds** (keccak side only — bao composition deferred to terminal) and **one expensive terminal round** (BLAKE3 chunk hash + bao parent path + keccak Merkle path):

| Round | On-chain work | Estimated gas |
|---|---|---|
| Round 0 (`submitMerkleCorruptionChallenge`) | Verify v2 typehash digest, recover signer, record state, transfer initial bond | ~80k |
| Rounds 1..N (`bisectMove`, ~20 rounds for 1 GiB blob) | One keccak parent-composition check (`keccak256(0x01 \|\| left \|\| right) == parentKeccak`) + store new subtree-root pair + advance chess-clock + per-round bond transfer | ~60k each |
| Terminal round N+1 reveal (`terminalReveal`) | Hash chunk via keccak256 + verify keccak Merkle path to R (~50k); store reveal | ~80k each side |
| Terminal round N+1 resolution (`resolveMerkleCorruption`) | Two BLAKE3 chunk hashes (~150k each), 2N bao parent-mode hashes (~150k each at level depth N), state cleanup, `StakingRegistry.slash()` if defender loses, bond ledger settlement, `Slashed` emit | ~3–4M |
| Total per dispute (1 GiB blob, N=20) | One open + 20 bisect + 2 reveals + 1 resolve | **~4.5–5.5M gas** |

At Arbitrum One ~0.1 gwei effective L2 fee, **~$0.30 per dispute**. Spread across ~24 transactions over up to ~12.7 days at the §3 default chess-clock budget. Cheap relative to the slash recovered (default 5% of 50k TOKEN minimum stake = 2.5k TOKEN; 50% to challenger).

The terminal-round BLAKE3 audit surface is non-trivial — see §6 Risks. There is exactly one production-grade Solidity BLAKE3 library in the public ecosystem at the time of writing; library selection and audit are gating prerequisites (see §Forward references).

### 3. Bisection protocol state machine

#### State stored per dispute

```solidity
struct MerkleCorruptionDispute {
    address challenger;
    address challengedNode;
    bytes32 blobHash;             // X — from streamResponseV2
    bytes32 merkleRoot;           // R — from streamResponseV2
    uint64  totalBytes;           // from streamResponseV2; determines num_chunks and N
    uint64  numChunks;            // ceil(totalBytes / 1024)
    uint8   round;                // 0 = open, 1..N = bisecting, N+1 = terminal reveal/resolve
    uint64  rangeStart;           // chunk index, narrows each round
    uint64  rangeEnd;             // exclusive
    bytes32 parentKeccak;         // current-range keccak Merkle subtree root, under R.
                                  // Initialised to R; updated to chosen half on each recursion.
                                  // Verified via parent-composition each round.
    bytes32 parentBaoChallenger;  // challenger's claimed current-range bao subtree root, under X.
                                  // Initialised to X. Verified lazily — only at the terminal step.
    bytes32 parentBaoDefender;    // defender's claimed current-range bao subtree root, under X.
                                  // Initialised to X. Verified lazily.
    bytes32 pendingLeftKeccak;    // moving side's posted left-half keccak subtree root for this round
    bytes32 pendingRightKeccak;   // moving side's posted right-half keccak subtree root
    bytes32 pendingLeftBao;       // moving side's posted left-half bao subtree root (lazy)
    bytes32 pendingRightBao;      // moving side's posted right-half bao subtree root (lazy)
    uint8   currentSide;          // 0 = challenger to move, 1 = defender to move
    uint64  challengerClockUs;    // remaining chess-clock per side
    uint64  defenderClockUs;
    uint64  lastMoveUs;           // for clock decrement
    bytes32 challengerChunkHash;  // keccak(0x00 || c_C) at terminal — empty until terminalReveal
    bytes32 defenderChunkHash;    // keccak(0x00 || c_D) at terminal
    uint256 challengerBond;       // initial bond (default 100 TOKEN, governance-tunable
                                  // per ADR 026 §11) plus per-round bonds accumulated during bisection
    uint256 defenderBond;         // per-round bonds (defender posts symmetrically from round 1;
                                  // default 10 TOKEN/round, governance-tunable per ADR 026 §11)
}
```

#### Chess-clock

Each side has an independent clock that runs only during their move. Default per-side per-move budget: **4 h** (chosen to satisfy the §7 cross-parameter invariant at the default `unbondingPeriod` of 14 d — a clock chosen for an automated watchtower-bot operator, see §6 vacation griefing mitigation; humans relying on alerts may need to widen the clock and `unbondingPeriod` together via governance). For a 1 GiB blob (`max_rounds = 20`) this gives a per-side total of `4 h × 20 = 80 h ≈ 3.3 days`; worst-case wall-clock duration of the bisection (both sides max out their per-move budgets, which alternate on the chess clock) is `2 × 4 h × 20 ≈ 6.7 days`. Adding the 5-day `MAX_EVIDENCE_AGE_US` and a 1-day margin yields the worst-case end-to-end dispute lifecycle of **~12.7 days**, which fits the §7 `unbondingPeriod` of 14 d.

When a side's clock reaches zero before they post their next move, they default-loss; the contract resolves the dispute with that side as the loser.

The per-round chess-clock and `max_rounds` are governance-tunable; safety bounds `[1 h, 24 h]` per side per move and `[1, 30]` rounds. The §7 cross-parameter invariant (the canonical statement; this section's defaults satisfy it) is enforced at every governance update of any input parameter.

#### Open-commit, no commit-reveal

There is no hidden information in this game — both parties already know the bytes. Open commit (post claimed roots in plaintext each round) is simpler, cheaper, and security-equivalent to commit-reveal in zero-information games. Implementations MUST NOT introduce a reveal phase.

#### Termination conditions

A dispute resolves when any of the following holds; resolution executes synchronously:

- **Default-on-timeout.** The current side's clock reaches zero. Loser is the timed-out side.
- **Parent-composition failure.** A bisecting party posts `(bao_left, keccak_left, bao_right, keccak_right)` that do not compose to the prior round's `(bao_root, keccak_root)` via the bao parent rule (X-side) and keccak parent rule (R-side). Loser is the offending side.
- **Terminal verification.** At round N+1 both sides reveal chunks + proofs. Outcome per §2 terminal-round table.
- **Voluntary forfeit.** A party may call `forfeitDispute(disputeId)` to concede; treated identically to a timeout loss.

Resolution: contract calls `StakingRegistry.slash(loser, uint8(OffenseType.Corruption))` if the loser is the defender. If the loser is the challenger, no `slash()` call — the bond is forfeited per the ADR 014 §3 split (50% burn, 50% to defender). Either way, `Slashed(slashId, operator, OffenseType.Corruption, amount, evidenceHash)` is emitted on the slash branch only — same emission rule as ADR 014 §3 corruption.

The `evidenceHash` preimage for v2 corruption is `keccak256(abi.encode(uint8(OffenseType.Corruption), streamStructHashV2))` — **the same preimage rule as ADR 014 §3 with a v2 struct hash**. Because v2's struct hash differs from v1's (extra `merkleRoot` field), the `evidenceHash` is naturally distinct. No new `OffenseType` enum entry is introduced — see §5.

### 4. Contract surface: `SlashJudge.submitMerkleCorruptionChallenge`

A new entry point on `SlashJudge` parallel to the existing `submitCorruptionChallenge`. Both routes feed the unified `OffenseType.Corruption` through `StakingRegistry.slash()`.

```solidity
interface ISlashJudge {
    // ... existing entry points unchanged ...

    /// Open a Merkle-bisection corruption dispute. Verifies v2 StreamResponse
    /// signature (v2 typehash, includes merkle_root), transfers initial bond,
    /// initializes dispute state. Subsequent moves use bisectMove / terminalReveal.
    function submitMerkleCorruptionChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata streamResponseV2Data,  // ABI-encoded v2 fields incl. merkleRoot
        bytes calldata streamSlashSig
    ) external returns (uint256 disputeId);

    /// Bisecting move. The caller (current chess-clock side, see §3) asserts
    /// (left, right) subtree-root pairs for both the keccak side (verified
    /// on-chain via parent-composition against the prior parentKeccak) and the
    /// bao side (stored lazily; verified at the terminal step). After both
    /// sides have moved at this level, the contract recurses into the half the
    /// parties' bao tuples disagreed on; the keccak commitments propagate
    /// alongside via the chosen half. Decrements chess-clock, transfers
    /// per-round bond, advances state.
    function bisectMove(
        uint256 disputeId,
        bytes32 keccakLeft, bytes32 keccakRight,
        bytes32 baoLeft,    bytes32 baoRight
    ) external;

    /// Terminal reveal at round N+1. Each side posts its claimed chunk bytes at
    /// the disputed position plus (a) a bao parent path of length N to
    /// claimed_blob_hash X, and (b) a keccak Merkle path of length
    /// ceil(log2(padded_num_chunks)) to claimed merkle_root R. `chunkBytes` is
    /// 1024 bytes for all chunks except the tail chunk (position
    /// `numChunks - 1`), which is `totalBytes - (numChunks - 1) * 1024` bytes
    /// when `totalBytes % 1024 != 0`. Once both sides have revealed (or the
    /// second side times out their chess-clock), resolveMerkleCorruption
    /// is callable.
    function terminalReveal(
        uint256 disputeId,
        bytes  calldata chunkBytes,         // ≤ 1024 bytes — must equal the tail length
                                            // for the final chunk position
        bytes32[] calldata baoParentPath,   // length N (depth of the bao tree)
        bytes32[] calldata keccakMerklePath // length ceil(log2(padded_num_chunks))
    ) external;

    /// Resolve after terminal reveal or after a side's chess-clock reached zero.
    /// Slashes the loser via StakingRegistry.slash() if the loser is the
    /// defender; emits Slashed atomically on the slash branch.
    function resolveMerkleCorruption(uint256 disputeId) external;

    /// Voluntary concede. If the conceding party is the defender, the
    /// contract calls StakingRegistry.slash(defender, OffenseType.Corruption)
    /// before refunding the challenger's bonds — equivalent to a defender
    /// timeout. If the conceding party is the challenger, no slash occurs;
    /// the bond is forfeited per the ADR 014 §3 challenger-loss split
    /// (50% burned, 50% to defender).
    function forfeitDispute(uint256 disputeId) external;

    /// Emitted on every successful submitMerkleCorruptionChallenge so
    /// operators (and watchtower bots, see §6) can subscribe to disputes
    /// against them.
    event MerkleDisputeOpened(
        uint256 indexed disputeId,
        address indexed challenger,
        address indexed challengedNode,
        bytes32 blobHash,
        bytes32 merkleRoot,
        uint64  totalBytes
    );

    /// Emitted on every bisectMove and terminalReveal so off-chain
    /// dispute-runners can track game state without re-deriving it from
    /// transaction calldata.
    event MerkleDisputeProgress(
        uint256 indexed disputeId,
        uint8   round,
        address indexed mover,
        uint64  rangeStart,
        uint64  rangeEnd
    );
}
```

The `maxActiveChallengesPerNode` cap ([ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract)) applies — Merkle disputes count against the same cap as optimistic challenges. Per-node concurrent disputes ≤ 10 (PoC default).

`StakingRegistry`, `SafetyReserve`, and `OffenseType` are unchanged. ADR 016 §3 SlashJudge call-graph row gains the new entry point; ADR 016 §1 contract inventory has no change (still `SlashJudge` with the same constructor parameters; the `challengeBond` and `counterEvidenceWindow` constructor parameters apply to the optimistic path only and are unaffected).

### 5. Migration and coexistence with the optimistic path

- **`cdn/client/v1` keeps the optimistic path.** Existing v1 deliveries continue to use `submitCorruptionChallenge` / `counterChallenge` / `resolveChallenge` per ADR 014 §2. v1 nodes need no changes.
- **`cdn/client/v2` mandates `merkle_root` in `StreamResponse`.** Disputes against v2 deliveries use `submitMerkleCorruptionChallenge` (cryptographic, no counter window). Calling `submitCorruptionChallenge` with a v2 struct must revert: the v1 typehash will not match.
- **Both paths feed unified `OffenseType.Corruption`.** No new enum entry — the [ADR 014 §3 append-only invariant](014-on-chain-verification.md#3-slashjudge-contract) is preserved. A node that earns one optimistic-slash on v1 and one Merkle-slash on v2 escalates to the 15% tier on the second offense, not 5% twice. Reputation impact ([ADR 008 §3](008-reputation.md)) is identical between paths.
- **No flag-day deprecation.** v1 stays in service indefinitely; sunset is a future ADR (separate decision tied to v2 adoption metrics). The cost is double-audit-surface for the lifetime of v1 — see §Risks.
- **Reputation gauge unchanged.** ADR 008 §3 BLAKE3-correctness signal (40% weight) is binary on `OffenseType.Corruption`; it does not distinguish v1 vs v2 path.

### 6. Bond economics and griefing analysis

#### Initial bond

Reuse the ADR 014 §2 challenge bond: **100 TOKEN**, governance-tunable `[10, 1000]` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics). No premium — Merkle bisection is *cheaper* to run honestly than the optimistic path's 24 h counter window, so a higher bond is unjustified.

#### Per-round bonds

Each side posts a small per-round bond (default **10 TOKEN** per round, governance-tunable `[1, 100]`) to discourage frivolous bisection that exhausts the opposing side's chess-clock. Per-round bonds are forfeited on default-loss (timeout, parent-composition failure) and returned on win.

Worst-case attacker bond outlay against an honest defender for a single dispute: 100 + 20 × 10 = **300 TOKEN** at default bound depth. Worst-case defender outlay symmetrically. The slash on the losing side dominates these (default 5% × 50k stake = 2.5k TOKEN minimum), so per-round bonds are deterrent supplements, not the primary deterrent.

#### `maxActiveChallengesPerNode` cap

Inherited from [ADR 014 §3](014-on-chain-verification.md#3-slashjudge-contract): **10 concurrent disputes per challenged node** (PoC default). Disputes from both v1 and v2 paths share this cap. A determined attacker with enough capital could open 10 simultaneous disputes against one node and force them into operational disruption — but at 100 + 200 TOKEN of per-dispute-opening capital × 10 disputes = **3,000 TOKEN locked** with a default 50% loss expectation, attack cost is ~1,500 TOKEN per attempted node disruption. Tunable downward via governance if abuse manifests.

#### Concurrent-dispute storage on `SlashJudge`

Each in-flight dispute stores ~25 `bytes32` words (subtree pairs + clock + bond ledger) plus dispute metadata. At 10 concurrent disputes per node, storage is ~250 SSTORE words. Across N nodes with active disputes, total storage scales linearly. Tight but feasible at PoC scale; tighten the cap if needed at production scale.

#### Vacation griefing

An honest party who fails to post a move within their chess-clock budget (vacation, ISP outage, NTP drift) loses by default. Mitigations:

- **Watchtower / dispute-runner bots** as operator infrastructure. ADR 027's [Appendix: Permissionless Fraud-Detection Layer](appendix-fraud-detection.md) sketches a parallel role; a similar appendix for Merkle-dispute runners is filed as a follow-up (see §Forward references).
- **Tunable per-side clock** via governance; if bots are not yet ubiquitous, raise the per-round budget.
- **Evidence is public.** A dispute opened against a node is observable on L2; mature operators can subscribe to `MerkleDisputeOpened` events and engage automated bots.

### 7. Interaction with stake unbonding — `unbondingPeriod` raised to 14 d

The bisection lifecycle worst case (the chess clock alternates — both sides may consume their full per-move budget across all rounds, so wall-clock duration of the bisection itself is `2 × per_side_per_move × max_rounds`) exceeds the prior 7-day default `unbondingPeriod` ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)). Without intervention, a node could:

1. Commit a corrupt delivery at time T.
2. Be challenged at T+ε (≤ 5 d after, bounded by `MAX_EVIDENCE_AGE_US`).
3. Initiate `deregisterNode` immediately, starting the unbonding clock.
4. Withdraw stake before the bisection has resolved.

The slash would arrive after withdrawal — there is nothing left to slash.

**Decision: raise `unbondingPeriod` default to 14 days.** Enforced as an amendment to ADR 026 §7; `StakingRegistry` constructor parameter changes from `7 days` to `14 days`; bounds in [ADR 009](009-governance.md) remain `[3 days, 30 days]` — unchanged. The new default sits above the §3 default-parameter worst-case dispute lifecycle (~12.7 days) with a small margin.

The existing invariant from [ADR 014 §3 Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period) — `MAX_EVIDENCE_AGE_US < unbondingPeriod` — continues to hold (5 days vs new 14 days; margin: 9 days). The Merkle dispute extends this invariant with a stricter cross-parameter constraint that subsumes it:

> **Cross-parameter invariant.** `(bisection_chess_clock_per_side × 2 × max_rounds) + MAX_EVIDENCE_AGE_US + 1 day_margin ≤ unbondingPeriod`. The `× 2` factor reflects chess-clock semantics (both sides alternate within the round; wall-clock is the sum of both sides' move durations, not just one). At §3 defaults — 4 h per side per move, 20 rounds, 5 d evidence age, 14 d unbonding — this evaluates to `2 × 4 h × 20 + 5 d + 1 d = 6.7 d + 6 d = 12.7 d ≤ 14 d` ✓.

Implementations MUST enforce this constraint at every governance update of any input parameter (per-round clock, `max_rounds`, `MAX_EVIDENCE_AGE_US`, or `unbondingPeriod`); a setter call that violates it must revert. The amendment is captured in §11 of ADR 026 (governable-parameters table) and the `unbondingPeriod` row updated to default 14 d / bounds `[3 d, 30 d]`.

The alternative — adding a `stakeLockedWhileDisputed` flag on `StakingRegistry` — is documented in §Alternatives Considered and is considered if 14-day unbonding harms operator UX more than expected at production scale.

## Consequences

### Positive

- **Cryptographic dispatch on corruption.** No more reliance on bond economics or counter-evidence ambiguity for production-grade slashing. A node that delivered corrupt bytes loses the dispute deterministically.
- **Shared keccak Merkle tag domain with ADR 027.** Leaf and internal-node domain tags (`0x00` / `0x01`) match ADR 027 §4's MMR — auditors review one tag-domain scheme, not two. The structures themselves differ (plain binary tree here vs MMR with bagging there) because the use cases differ (commit-once-per-blob vs streaming-append-receipts).
- **No new offense type.** `OffenseType.Corruption` stays unified; the append-only enum invariant from ADR 014 §3 is preserved. Reputation impact, slashing schedule, appeal flow are identical between v1 and v2 paths.
- **Failed challenges still slash the loser.** DoS via repeated frivolous challenges is bounded by attacker bond loss × max-concurrent-disputes cap.
- **Gauge / receipts toolchain partial reuse.** The keccak primitive (chunk leaf hash, internal-node hash) is shared with the ADR 027 MMR machinery operators already run for receipts; the tree-shape composition is different (plain binary here, MMR there) but is straightforward.

### Negative

- **Tier 3 wire-format break.** v2 ALPN bump is coordinated rollout per [ADR 013 § Three Tiers of Evolution](013-schema-evolution.md#tier-3--major-alpn-version-bump). Old peers continue serving v1 indefinitely; double-audit surface is the cost.
- **Worst-case dispute is ~12.7 days at the §3 defaults** (chess-clock alternates, 4 h × 2 × 20 rounds = 6.7 d bisection + 5 d max evidence age + 1 d margin). Long for an on-chain process. The 4 h per-side per-move chess-clock is tight for human-mediated review and effectively assumes operators run a watchtower-bot mitigation (§6); operators that prefer a more humane clock must coordinate a governance proposal that raises both `unbondingPeriod` and the per-move clock together to keep the §7 invariant satisfied. Vacation griefing is a real concern, mitigated by external infrastructure (watchtower bots).
- **Per-dispute terminal-round gas ~3–4 M.** Acceptable on Arbitrum at ~$0.30/dispute, but the on-chain BLAKE3 step is the highest-cost component and dominates audit complexity.
- **Storage cost on SlashJudge.** ~25 `bytes32` per active dispute × `maxActiveChallengesPerNode` = ~250 words per node. Tight at PoC; tighten the cap at production scale.
- **`unbondingPeriod` doubles to 14 d.** Operator working-capital exposure during exit is twice as long. Minor real cost; acceptable trade for cryptographic slashing.
- **Cache-pull computation.** Cache nodes must keccak-Merkle-tree the chunks streamingly during pull (one keccak leaf hash per 1024-byte bao chunk arrival, plus internal-node hashes as siblings become available). For non-power-of-2 chunk counts a small set of zero-padded leaves are appended at the right end before signing. At ~30 MB/s pull throughput this is < 1% of CPU per stream — negligible but non-zero.

### Risks

- **On-chain BLAKE3 audit surface.** Few production-grade Solidity BLAKE3 implementations exist; G-function constants, 7-round (not 10) chunk-leaf mode, bao parent-mode flag bits, and chunk-vs-parent block-byte differences all need audit. Library selection is a hard prerequisite — see §Forward references issue 1.
- **Vacation griefing.** As above; without operator-side bots a single attacker who knows their target is on vacation can win disputes by timing the open. Mitigation is operator infrastructure, not protocol.
- **Bisection-lifecycle vs. unbonding interaction.** Raising `unbondingPeriod` to 14 d is the chosen fix; the alternative (`stakeLockedWhileDisputed` flag) remains a fallback if 14-day unbonding turns out to harm operator UX more than expected. Re-visit if production data shows unbonding-related operator drop-off.
- **Concurrent-dispute storage at scale.** With `maxActiveChallengesPerNode = 10` and many simultaneously-disputed nodes, total `SlashJudge` storage growth is real. PoC tolerance, production may need cap reduction or storage-pruning policy after dispute resolution.
- **Double-audit surface for v1 + v2.** The optimistic path stays in service indefinitely. Two corruption code paths means two audit budgets. Sunset deferral is intentional, but the cost is real.
- **`unbondingPeriod` and operator economics.** Doubling unbonding affects operator P&L modelling in `finance/notebooks/_shared/params.py` (cross-repo dependency per workspace `CLAUDE.md`). The finance subproject must re-derive operator runway tables when this ADR ships.

## Alternatives Considered

- **Option B — publisher pre-registers `(blob_hash, merkle_root)` in a `BlobRegistry` contract at ingest.** Rejected: ADR 002's content-claim model has no canonical publisher per blob hash. Multi-claim semantics ([ADR 002 § Multi-claim semantics](002-content-addressing.md#multi-claim-semantics)) explicitly allow any number of independent namespaces to claim the same hash. There is no single party to register R; pre-registration would require coordination that does not exist. Also fails for cache nodes — the cache-miss pull computes R on the fly, decoupled from any publisher registration.

- **Option C — lazy indexer-computed root, posted on first challenge.** Rejected: introduces a new trusted role outside the protocol (the indexer), and ADR 022's discovery model treats the on-chain origin directory as the *deterministic* fallback, not an indexer. Indexer trust model is exactly what the protocol is designed to avoid.

- **Option D — ZK proof of `BLAKE3(B) == X AND keccak256-binary-Merkle(B) == R`.** Rejected for v1: a Groth16/PLONK BLAKE3 circuit is a substantial engineering investment, no audited implementation exists in the public ecosystem at the time of writing, and the optimistic + bisection path is sufficient at expected dispute volume. Worth filing as a future-work seam — when an audited BLAKE3 ZK circuit exists (industry effort tracking), revisit. The interactive-bisection path can coexist with a ZK fast-path: parties can voluntarily settle disputes by submitting a single ZK proof in lieu of the bisection game, falling back to bisection on prover failure.

- **Single-shot on-chain BLAKE3-of-whole-blob.** Rejected: gas-prohibitive at any non-trivial blob size (~150 k gas/chunk × millions of chunks for a multi-GB blob = unboundedly expensive). The whole point of the bisection construction is to amortize on-chain BLAKE3 cost to one chunk.

- **`stakeLockedWhileDisputed` flag on `StakingRegistry` instead of extending `unbondingPeriod`.** Rejected for v1: minor `StakingRegistry` surface change (one new bool flag per `NodeInfo` + setter), but a larger semantic surface — every interaction touching unstaking must consult the flag, creating a new failure mode (locked stake during runaway dispute). The 14-day `unbondingPeriod` is simpler and the operator UX cost is bounded. **Reconsider** if production data shows 14-day unbonding causes operator drop-off above acceptable thresholds.

- **Challenger names the disputed chunk index upfront.** Rejected: works only if there is exactly one corrupt chunk. Sophisticated griefing can corrupt multiple chunks in patterns that defeat a "name your chunk" shortcut (e.g., interleaved corruption that the challenger cannot disambiguate without bisecting). Full disagreement-frontier bisection (the FaultDisputeGame pattern) is the audited approach and handles arbitrary corruption shapes uniformly.

## ADRs Affected

- **[ADR 002](002-content-addressing.md):** Inline forward-reference to "production upgrade path using interactive keccak256 Merkle proofs" (lines 113, 118) repointed at this ADR. The §Open Question on production verification is resolved here.
- **[ADR 003](003-payments.md):** Inline forward-reference at §Corrupted delivery (line ~222) and §Negative consequences (line ~154) updated to mention this ADR alongside ADR 014 §2 — both paths are referenced.
- **[ADR 005](005-protocol.md):** §`cdn/client/v1` paid delivery protocol gains a note that `cdn/client/v2` extends the signed-field set with `merkle_root: bytes32` per this ADR. v1 wire descriptions are unchanged — ADR 005 stays the v1 baseline.
- **[ADR 013](013-schema-evolution.md):** §Signed Field Freezing table gains a v2 row for `StreamResponse` listing the v2 signed-field set with `merkle_root`. This ADR is cited as the Tier 3 trigger event.
- **[ADR 014](014-on-chain-verification.md):** §2 "Future evolution" stub is replaced with a one-line cross-reference to this ADR (the optimistic path remains documented in the rest of §2). §3 ISlashJudge interface block extends with `submitMerkleCorruptionChallenge`, `bisectMove`, `terminalReveal`, `resolveMerkleCorruption`, `forfeitDispute`. §3 gas-estimates table gains a row for the v2 dispute flow. ADRs Affected gains an ADR 030 entry.
- **[ADR 016](016-contract-interactions.md):** §3 SlashJudge function table appends `submitMerkleCorruptionChallenge` (and the bisection-step entry points) with the same `nonReentrant`, checks-effects-interactions, TOKEN bond-deposit guards. §1 Contract Inventory row 14 (SlashJudge constructor parameters) is unchanged — `challengeBond` and `counterEvidenceWindow` cover both paths' bond mechanics.
- **[ADR 026](026-gauge-boost-tokenomics.md):** §7 Operator economics — `unbondingPeriod` default raised from 7 days to 14 days. §11 governable parameters table gains an `unbondingPeriod` row with default 1,209,600 s (14 days) / bounds `[3 d, 30 d]` (existing bounds unchanged; default within them). Cross-parameter invariant added (per §7 of this ADR).
- **[ADR 027](027-distinct-client-receipts.md):** Three previously-broken `#production-path-interactive-keccak256-merkle-proof-future-adr` anchor links (lines ~24, ~35, ~116) repointed to ADR 027 §4's own `#aggregator-implementation--mmr-accumulator` anchor — keeping ADR 027 as the canonical home of the MMR construction. ADR 030 uses a different but tag-domain-compatible primitive (plain binary keccak Merkle tree, leaves `keccak256(0x00 || chunk)`, internal nodes `keccak256(0x01 || left || right)`, no bagging — see §1 rationale).
- **[ADR 028](028-slashing-appeals.md):** §1 confirmation that Merkle-corruption slashes (v2 path) have identical appealability to optimistic-corruption slashes (v1 path), since both feed the unified `OffenseType.Corruption`. No scope change.
- **[`adr/architecture.md`](architecture.md):** Chapter 5 — Verification & enforcement: insert this ADR after ADR 014 in the chapter listing. Numeric ADR index gains a one-line entry. ADR 014's one-line summary is amended to mention "production path in ADR 030".
- **[`adr/README.md`](README.md):** Reading-order pandoc command updated to include `030-blake3-merkle-verification.md` between `014-...` and `008-...` in the verification chapter sequence.

## Forward references (follow-up issues)

Per issue [#387](https://github.com/decdn/decdn/issues/387) "Definition of done":

1. **`solidity-blake3` library selection and audit.** Survey existing implementations (Sovrun, mempirate / blake3-solidity, others). Benchmark on Arbitrum One under realistic L2 fee conditions. Decide vendor-vs-fork-and-audit. Acceptance criteria: an audited library with verified ~150 k gas/chunk benchmark on testnet. **Prerequisite for issue 2.**
2. **`SlashJudge.submitMerkleCorruptionChallenge` Solidity implementation.** Bisection state machine, per-round chess-clock, terminal BLAKE3 + bao parent-path verification, keccak binary-Merkle inclusion-proof verification, atomic `Slashed` event emission. Updates `ISlashJudge` in [ADR 016 §3](016-contract-interactions.md#3-cross-contract-call-graph). Depends on issue 1.
3. **`cdn/client/v2` ALPN handler.** Rust crate work in `crates/protocol` (Tier 3 message types: v2 `StreamResponse` with `merkle_root`), `crates/node` (handler dispatch on negotiated ALPN), `crates/cache` (streaming bao chunk → keccak binary Merkle tree computation during pull, with right-padding to next power of 2 before signing). Operator runbook update via `appendix-operator-upgrade-path.md`.
4. **Off-chain dispute-runner reference implementation.** Watchtower bot that monitors `MerkleDisputeOpened` events on `SlashJudge`, plays out the bisection on behalf of an operator over multi-day disputes, posts moves within chess-clock budgets. Mitigates vacation-griefing. Operator infrastructure, parallel to `appendix-fraud-detection.md` challenger toolkit.
