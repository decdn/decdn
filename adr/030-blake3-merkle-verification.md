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

The half-design was dropped from ADR 014 in PR #386 so that ADR doesn't carry sketches of unbuilt designs. Issue [#387](https://github.com/decdn/decdn/issues/387) tracks the missing follow-up. ADR 027 §1 and §4 already forward-reference this ADR for the keccak MMR construction they reuse for receipt batching, and ADR 002 / ADR 003 forward-reference it as the resolution to the open production-verification question.

Issue #387 lists five questions the new ADR must answer:

1. **Where does the keccak256 Merkle root live?** Signed wire field, on-chain registry pre-registered by the publisher, or computed lazily by an indexer.
2. **How is the root bound to the BLAKE3 hash?**
3. **Tree shape.** 1024-byte leaves vs alternatives.
4. **Two-round protocol mechanics.** Challenger commitment, node counter-proof, resolution rules.
5. **Migration / coexistence with the optimistic path.** Both at once? Optimistic deprecated?

## Decision

The protocol introduces an **interactive bisection-based corruption proof** layered on top of two cryptographic commitments: the BLAKE3 bao tree (intrinsic to the blob's content addressing) and a keccak256 Merkle Mountain Range (MMR) over 1024-byte chunks committed by the serving node at delivery time. Disputes bisect down to a single 1024-byte chunk; the on-chain terminal step verifies *one* BLAKE3 chunk hash and *one* bao parent path against the blob hash, and *one* keccak inclusion proof against the node's committed root. Both proofs must pass; the loser is slashed under the existing `OffenseType.Corruption` schedule.

The dispute is the cryptographic upgrade replacing the "bond + 24 h counter window" of ADR 014 §2. The optimistic path remains in service for `cdn/client/v1`; the production path lives behind `cdn/client/v2`.

### 1. Wire format: `merkle_root` in signed `StreamResponse` (Tier 3 → `cdn/client/v2`)

The keccak256 MMR root is a new **signed** field on `StreamResponse`. Per [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing), changing the signed-field set is a Tier 3 (major) ALPN bump:

```text
cdn/client/v1  StreamResponse signed fields: {hash, ok, rate_per_mb, total_bytes,
                                               channel_id, timestamp_us, redirect}
cdn/client/v2  StreamResponse signed fields: {hash, ok, rate_per_mb, total_bytes,
                                               channel_id, timestamp_us, redirect,
                                               merkle_root}
```

`merkle_root: bytes32` is the bagged-peaks root of the keccak256 MMR over the 1024-byte chunks of the bytes the node is about to deliver in this stream. The MMR construction (domain tags `0x00` leaf / `0x01` internal / `0x02` bagging, bagging-fold direction, inclusion-proof shape) is canonical in [ADR 027 §4 Aggregator implementation — MMR accumulator](027-distinct-client-receipts.md#aggregator-implementation--mmr-accumulator) — this ADR reuses it verbatim and does not redefine it.

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

Every disputed delivery is committed in two trees over the *same* underlying bytes:

- **BLAKE3 bao tree** with root = `streamResponse.hash` (= the blob hash X). Leaves are 1024-byte BLAKE3 chunk hashes; internal nodes are BLAKE3 in keyed/parent mode (the bao spec). Inclusion proofs are *bao parent paths*: a leaf chunk plus the sibling-hash sequence up to the root.
- **Keccak256 MMR** with bagged root = `streamResponse.merkle_root` (= R). Construction and inclusion-proof shape are canonical in [ADR 027 §4](027-distinct-client-receipts.md#aggregator-implementation--mmr-accumulator); this ADR reuses it without modification.

For honest delivery, both trees commit to the same 1024-byte chunks. For corrupt delivery, the node delivered chunks that do *not* hash to X under BLAKE3, but the node nonetheless signs a `merkle_root` over those (corrupt) chunks. The signed `merkle_root` cryptographically pins the node to a specific chunk sequence; the contract's job during dispute is to *prove* that some chunk in that sequence does not BLAKE3-verify under the bao tree of X.

#### The bisection game (FaultDisputeGame pattern)

Modeled on the FaultDisputeGame pattern from Optimism / Arbitrum BoLD. The game finds the *first* chunk index where the challenger and defender disagree, then verifies that chunk's BLAKE3 leaf against X.

```text
state: claimed_root (R), claimed_blob_hash (X), num_chunks (= ceil(total_bytes / 1024))

round 0  — Challenger opens dispute. Posts streamResponseV2 + slash_sig + bond.
           Contract verifies signature, recovers (X, R), records num_chunks.
           Challenger asserts: ∃ chunk i with bao(chunk_i) ≠ correct hash for slot i in X.

rounds 1..N where N = ceil(log2(num_chunks)):
  Both parties post their claimed pair of bao-tree subtree-root + keccak-MMR
  subtree-root for the current "left half" and "right half" of the disputed range.
  Contract enforces each commit is consistent with the prior parent commit
  (both halves' roots compose to the parent via bao for X-side and via keccak
  for R-side). The party whose subtree-pair fails the parent-composition check
  loses immediately.
  Where the parties disagree about the (bao_root, keccak_root) tuple of a half,
  the dispute recurses into that half. After N rounds the disputed range is one
  1024-byte chunk.

terminal round N+1:
  Both parties reveal their claimed 1024-byte chunk bytes for the disputed slot
  plus (a) bao parent path to X and (b) keccak MMR inclusion proof to R.
  The contract:
   - hashes each side's chunk via BLAKE3 (one chunk = ~150k gas)
   - verifies the bao parent path against X (per-level BLAKE3 parent-mode hash,
     ~150k gas/level × N levels)
   - verifies the keccak MMR inclusion proof against R (~21 keccak hashes at the
     reference 30k-receipt scale per ADR 027 §4; for blob-chunk MMRs the depth
     scales with num_chunks)
  Outcomes:
   - Defender's chunk passes both proofs and challenger's fails → challenger slashed
     (bond + per-round bonds forfeited; defender unaffected).
   - Defender's chunk fails BLAKE3-against-X → defender slashed under
     OffenseType.Corruption per ADR 014 §3 (5/15/50% schedule, 50/30/20 distribution
     per ADR 026 §8); challenger receives 50% of slashed stake plus bond return.
   - Both fail → both slashed (defender for corruption; challenger for frivolous
     evidence). Real-world this is unlikely.
```

The chess-clock (§3) bounds total dispute lifecycle. Per-round contract-state cost is bounded — each round writes a small constant number of `bytes32` to storage (subtree-root pairs + clock state).

#### Gas cost breakdown

The bisection is deliberately split into **cheap intermediate rounds** (keccak only) and **one expensive terminal round** (BLAKE3 + bao path):

| Round | On-chain work | Estimated gas |
|---|---|---|
| Round 0 (open) | Verify v2 typehash digest, recover signer, record state, transfer bond | ~80k |
| Rounds 1..N (bisect) | Two keccak parent-composition checks, store new subtree-root pair, advance chess-clock | ~50k each |
| Terminal round N+1 | Two BLAKE3 chunk hashes (~150k each), N bao parent hashes (~150k each) at level depth N, two keccak MMR inclusion proofs (~50k total), state cleanup | ~3–4M |
| Total per dispute (1 GiB blob, N=20) | One open + 20 bisect + 1 terminal | **~4.1–5.1M gas** |

At Arbitrum One ~0.1 gwei effective L2 fee, ~$0.30 per dispute. Spread across ~22 transactions over up to ~12.7 days at the §3 default chess-clock budget. Cheap relative to the slash recovered (default 5% of 50k TOKEN minimum stake = 2.5k TOKEN; 50% to challenger).

The terminal-round BLAKE3 audit surface is non-trivial — see §6 Risks. There is exactly one production-grade Solidity BLAKE3 library in the public ecosystem at the time of writing; library selection and audit are gating prerequisites (see §Forward references).

### 3. Bisection protocol state machine

#### State stored per dispute

```solidity
struct MerkleCorruptionDispute {
    address challenger;
    address challengedNode;
    bytes32 blobHash;         // X — from streamResponseV2
    bytes32 merkleRoot;       // R — from streamResponseV2
    uint64  totalBytes;       // from streamResponseV2; determines num_chunks and N
    uint64  numChunks;        // ceil(totalBytes / 1024)
    uint8   round;            // 0 = open, 1..N = bisecting, N+1 = terminal
    uint64  rangeStart;       // chunk index, narrows each round
    uint64  rangeEnd;         // exclusive
    bytes32 challengerBaoRoot; bytes32 challengerKeccakRoot;
    bytes32 defenderBaoRoot;  bytes32 defenderKeccakRoot;
    uint8   currentSide;      // 0 = challenger to move, 1 = defender to move
    uint64  challengerClockUs; uint64 defenderClockUs;  // remaining chess-clock per side
    uint64  lastMoveUs;       // for clock decrement
    uint256 challengerBond;   // initial bond (default 100 TOKEN, governance-tunable
                              // per ADR 026 §11) plus per-round bonds accumulated during bisection
    uint256 defenderBond;     // per-round bonds (defender posts symmetrically from round 1;
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

    /// Bisecting move. The caller asserts the (left, right) subtree-root pair
    /// for the current disputed range. Contract verifies parent-composition
    /// against the prior commitment, advances state to the half the parties
    /// disagree on, decrements chess-clock, transfers per-round bond.
    function bisectMove(
        uint256 disputeId,
        bytes32 baoRootLeft,    bytes32 keccakRootLeft,
        bytes32 baoRootRight,   bytes32 keccakRootRight,
        bool    challengeLeft   // true = the disputed half is the left half; false = right
    ) external;

    /// Terminal reveal at round N+1. Caller posts the disputed chunk bytes plus
    /// (a) bao parent path of length N to claimed_blob_hash, and (b) keccak
    /// MMR inclusion proof to claimed merkle_root. Once both sides have
    /// revealed (or the second side times out), resolveMerkleCorruption is
    /// callable.
    function terminalReveal(
        uint256 disputeId,
        bytes  calldata chunkBytes,        // exactly 1024 bytes
        bytes32[] calldata baoParentPath,
        bytes  calldata keccakInclusionProof
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
- **Single audited Merkle code path.** The keccak MMR construction reuses ADR 027 §4 verbatim (domain tags, bagging fold, inclusion proof shape). Receipt-fraud and corruption disputes share a single audited implementation.
- **No new offense type.** `OffenseType.Corruption` stays unified; the append-only enum invariant from ADR 014 §3 is preserved. Reputation impact, slashing schedule, appeal flow are identical between v1 and v2 paths.
- **Failed challenges still slash the loser.** DoS via repeated frivolous challenges is bounded by attacker bond loss × max-concurrent-disputes cap.
- **Gauge / receipts toolchain reuse.** Operators already running the keccak MMR machinery for ADR 027 receipts have most of the v2 implementation already.

### Negative

- **Tier 3 wire-format break.** v2 ALPN bump is coordinated rollout per [ADR 013 § Three Tiers of Evolution](013-schema-evolution.md#tier-3--major-alpn-version-bump). Old peers continue serving v1 indefinitely; double-audit surface is the cost.
- **Worst-case dispute is ~12.7 days at the §3 defaults** (chess-clock alternates, 4 h × 2 × 20 rounds = 6.7 d bisection + 5 d max evidence age + 1 d margin). Long for an on-chain process. The 4 h per-side per-move chess-clock is tight for human-mediated review and effectively assumes operators run a watchtower-bot mitigation (§6); operators that prefer a more humane clock must coordinate a governance proposal that raises both `unbondingPeriod` and the per-move clock together to keep the §7 invariant satisfied. Vacation griefing is a real concern, mitigated by external infrastructure (watchtower bots).
- **Per-dispute terminal-round gas ~3–4 M.** Acceptable on Arbitrum at ~$0.30/dispute, but the on-chain BLAKE3 step is the highest-cost component and dominates audit complexity.
- **Storage cost on SlashJudge.** ~25 `bytes32` per active dispute × `maxActiveChallengesPerNode` = ~250 words per node. Tight at PoC; tighten the cap at production scale.
- **`unbondingPeriod` doubles to 14 d.** Operator working-capital exposure during exit is twice as long. Minor real cost; acceptable trade for cryptographic slashing.
- **Cache-pull computation.** Cache nodes must keccak-MMR the chunks streamingly during pull (one keccak append per 1024-byte bao chunk arrival). At ~30 MB/s pull throughput this is < 1% of CPU per stream — negligible but non-zero.

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

- **Option D — ZK proof of `BLAKE3(B) == X AND keccak256-MMR(B) == R`.** Rejected for v1: a Groth16/PLONK BLAKE3 circuit is a substantial engineering investment, no audited implementation exists in the public ecosystem at the time of writing, and the optimistic + bisection path is sufficient at expected dispute volume. Worth filing as a future-work seam — when an audited BLAKE3 ZK circuit exists (industry effort tracking), revisit. The interactive-bisection path can coexist with a ZK fast-path: parties can voluntarily settle disputes by submitting a single ZK proof in lieu of the bisection game, falling back to bisection on prover failure.

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
- **[ADR 027](027-distinct-client-receipts.md):** Three previously-broken `#production-path-interactive-keccak256-merkle-proof-future-adr` anchor links (lines ~24, ~35, ~116) repointed to ADR 027 §4's own `#aggregator-implementation--mmr-accumulator` anchor — keeping ADR 027 as the canonical home of the MMR construction. ADR 030 §1 / §2 reference back to that anchor; the construction itself is unchanged.
- **[ADR 028](028-slashing-appeals.md):** §1 confirmation that Merkle-corruption slashes (v2 path) have identical appealability to optimistic-corruption slashes (v1 path), since both feed the unified `OffenseType.Corruption`. No scope change.
- **[`adr/architecture.md`](architecture.md):** Chapter 5 — Verification & enforcement: insert this ADR after ADR 014 in the chapter listing. Numeric ADR index gains a one-line entry. ADR 014's one-line summary is amended to mention "production path in ADR 030".
- **[`adr/README.md`](README.md):** Reading-order pandoc command updated to include `030-blake3-merkle-verification.md` between `014-...` and `008-...` in the verification chapter sequence.

## Forward references (follow-up issues)

Per issue [#387](https://github.com/decdn/decdn/issues/387) "Definition of done":

1. **`solidity-blake3` library selection and audit.** Survey existing implementations (Sovrun, mempirate / blake3-solidity, others). Benchmark on Arbitrum One under realistic L2 fee conditions. Decide vendor-vs-fork-and-audit. Acceptance criteria: an audited library with verified ~150 k gas/chunk benchmark on testnet. **Prerequisite for issue 2.**
2. **`SlashJudge.submitMerkleCorruptionChallenge` Solidity implementation.** Bisection state machine, per-round chess-clock, terminal BLAKE3 + bao parent-path verification, keccak MMR inclusion-proof verification, atomic `Slashed` event emission. Updates `ISlashJudge` in [ADR 016 §3](016-contract-interactions.md#3-cross-contract-call-graph). Depends on issue 1.
3. **`cdn/client/v2` ALPN handler.** Rust crate work in `crates/protocol` (Tier 3 message types: v2 `StreamResponse` with `merkle_root`), `crates/node` (handler dispatch on negotiated ALPN), `crates/cache` (streaming bao→keccak-MMR computation during pull). Operator runbook update via `appendix-operator-upgrade-path.md`.
4. **Off-chain dispute-runner reference implementation.** Watchtower bot that monitors `MerkleDisputeOpened` events on `SlashJudge`, plays out the bisection on behalf of an operator over multi-day disputes, posts moves within chess-clock budgets. Mitigates vacation-griefing. Operator infrastructure, parallel to `appendix-fraud-detection.md` challenger toolkit.
