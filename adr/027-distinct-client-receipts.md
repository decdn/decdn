# ADR 027: Distinct-Client Delivery Receipts

**Date:** 2026-04-25
**Status:** Draft
**Required for:** [ADR 026](026-gauge-boost-tokenomics.md) gauge-pool security
**Touches:** [ADR 003](003-payments.md), [ADR 008](008-reputation.md), [ADR 014](014-on-chain-verification.md), [Appendix: Fraud Detection](appendix-fraud-detection.md)

## Context

[ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) defines the gauge-pool share as a function of `bytes_i` (verified bytes per operator) and `ve_i / total_ve` (the operator's ve-share). The formula bounds the *output* but says nothing about whether the *input* `bytes_i` is honest. The on-chain `claimedBytes` reaching `FeeRouter.routeSettlement` ([ADR 003](003-payments.md)) is signed by *some* address that opened a payment channel — nothing today prevents the operator from running both sides.

### The wash-trading attack

([ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks); design-spec §6.3): an operator opens a channel from a sybil client identity to themselves, self-routes traffic, and signs vouchers inflating `bytesDelivered`. Effective cost is the ~60% router skim on the operator's *own* USDC plus a few cents of L2 gas — roughly a 5–8% net loss in USDC terms, more than offset by gauge-share + ve-appreciation gains at TOKEN price 3–5× genesis. Settlement gas does not scale with claimed bytes. **Neither cost is a deterrent at the intended TOKEN price levels.**

Defense: byte counters that feed the gauge formula must be attested by **distinct, verifiable client identities** — counterparties the operator does not control. The voucher protocol from [ADR 003](003-payments.md) already proves *bytes were paid for*; this ADR adds a parallel artifact that proves *bytes were paid for by independent counterparties* and gates gauge eligibility on that artifact.

This ADR is forward-referenced from [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks) and [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) as priority-1, **not optional for production launch**.

## Decision

The protocol introduces a **DeliveryReceipt** primitive, paired one-to-one with each voucher submitted to `FeeRouter.routeSettlement`. Receipts are EIP-712 typed messages signed by the *requester's* secp256k1 key (the address that funded the payment channel). Gauge-pool eligibility for an operator's epoch is gated on a **distinct-client diversity threshold** verified against the receipts the operator commits to chain.

Receipts are batched into a Merkle tree per operator per epoch; only the root and a small summary are stored on-chain at settlement time, mirroring the keccak256 Merkle pattern in [ADR 014 §2 Production Path](014-on-chain-verification.md). Individual receipts surface only on challenge, processed via the permissionless [`SlashJudge` bond mechanism](014-on-chain-verification.md#bond-handling) and gated by reputation ([ADR 008](008-reputation.md)).

### 1. Receipt format

A `DeliveryReceipt` is an EIP-712 typed-data message signed by the **requester's Ethereum key** — the same address that opened the channel in `StablePaymentChannel` ([ADR 003](003-payments.md)). Receipts are cryptographically bound to a specific voucher within a specific channel; one voucher → one receipt.

| Field | Type | Description |
| --- | --- | --- |
| `channelId` | `bytes32` | The `StablePaymentChannel` channel ID. Matches the `channelId` field in the paired voucher ([ADR 003](003-payments.md) `Voucher` typedef). |
| `voucherNonce` | `uint256` | The nonce of the paired voucher (≥1, monotonically increasing within the channel — see [ADR 003 Voucher Nonce Convention](003-payments.md)). Pairs the receipt to a single off-chain voucher. |
| `bytesClaimed` | `uint256` | Cumulative bytes delivered as of this voucher window. Equals the voucher's `bytesDelivered` field; redundant on purpose, see invariant #2 below. |
| `contentRoot` | `bytes32` | Merkle root over the per-stream content delivered in this voucher window — a keccak256 Merkle tree over 1024-byte chunks identical to the construction in [ADR 014 §2 Production Path](014-on-chain-verification.md#production-path-interactive-keccak256-merkle-proof-future-adr). For sub-MB streams a single leaf hash; for multi-MB streams the root binds every chunk. **Multi-stream channels** (per [ADR 003 Concurrent Streams](003-payments.md#concurrent-streams), where multiple streams share a single channel and a single cumulative voucher): `contentRoot` is the Merkle root of the per-stream chunk-tree roots, ordered by stream-id ascending. The receipt thus commits to all bytes delivered across the voucher's stream set, not just one stream. |
| `clientPubKey` | `address` | Requester's Ethereum address. Identity-diversity gating (§3) is computed across this field. Equals the recovered signer; included in plaintext to make on-chain bucketing trivial for batched verification. |
| `operatorAddress` | `address` | The operator's Ethereum address (`channel.provider`). Receipts are not portable across operators. |
| `epochId` | `uint64` | The `FeeRouter` epoch this receipt is intended to credit — set by the requester at signing time. Receipts are grouped/bucketed by this field when constructing per-epoch roots (see §4 Per-epoch bucketing); a receipt is rejected if its `epochId` does not match the specific epoch root/summary under which it is being committed. This does **not** require all receipts in a physical batch to share the same `epochId`. |
| `timestamp` | `uint64` | Microsecond timestamp from the requester's clock at signing. Skew bounds: `MAX_FUTURE_SKEW_US = 60_000_000` (60 s) prevents future-dated receipts. Staleness is measured against the receipt's `epochId` settlement window, **not** wall-clock at challenge time — a receipt is fresh as long as its `timestamp` falls within the `epochId` epoch (1-week window per [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)) plus the gauge claim window (26 epochs ≈ 6 months). This decouples receipt validity from the 90-day max channel duration in [ADR 003](003-payments.md): a receipt signed early in a long-lived channel is fresh for the epoch it claims, regardless of when the channel itself eventually settles. The narrower [ADR 014 Evidence Staleness](014-on-chain-verification.md) bounds (5 days) apply only to slash-evidence paths, which are out of scope for routine receipt-eligibility checks. |

#### EIP-712 domain

Dedicated `FeeRouter` domain separator over the §1 field set, preventing cross-contract replay against `StablePaymentChannel` voucher signatures or `SlashJudge` evidence. EIP-712's typed-data envelope inherently carries domain-separator + struct-type-hash bytes that callers must validate before recovering — an attacker-supplied byte-string that doesn't match the typehash fails recovery against `clientPubKey` and is rejected before any further parsing, providing the equivalent of a magic header for the on-chain validation path.

**Invariants enforced on challenge (§4, §5):** signature recovers to `clientPubKey == channel.client`; `bytesClaimed` matches the paired voucher's `bytesDelivered` (the receipt and voucher must be two views of the *same* delivery); `operatorAddress == channel.provider`; `timestamp` falls within the receipt's `epochId` epoch ± `MAX_FUTURE_SKEW_US` (each receipt is bucketed by its own `epochId`, so a single batch may span multiple epochs — see §4 Per-epoch bucketing for the long-lived-channel pattern).

#### Pairing with vouchers, not replacing them

[ADR 003](003-payments.md)'s `Voucher` is the on-chain payment instrument and remains the authoritative settlement input for `claimedAmount` and `claimedBytes`. The `DeliveryReceipt` is a separate signature over an overlapping field set (`bytesClaimed` mirrors `bytesDelivered`; `channelId` and `voucherNonce` pin the pairing). A voucher without a matching receipt is fully redeemable for USDC at settlement — only **gauge eligibility for the underlying bytes** depends on the receipt. See §7 for failure semantics.

### 2. Signature scheme

#### secp256k1 / EIP-712

Identical curve and signing scheme as the rest of the deCDN/EVM stack: voucher signatures ([ADR 003 EIP-712 Voucher Signature](003-payments.md)), `slash_sig` ([ADR 014 §1](014-on-chain-verification.md)), `BindNodeId` ([ADR 003 NodeId Binding](003-payments.md)), and `DeliveryReceipt` for corruption challenges ([ADR 014 §2](014-on-chain-verification.md)) — though the latter is a different typedef from the receipt defined here.

#### Verifier

OpenZeppelin `SignatureChecker.isValidSignatureNow` so EOAs (`ecrecover`, ~3k gas) and ERC-1271 smart accounts ([ADR 024](024-account-abstraction.md), ~15k gas for Safe) are both supported with no special-casing in `FeeRouter` or `SlashJudge`. Production clients running smart-account wallets sign receipts via the same path as vouchers.

#### Why secp256k1

Receipts are signed by *requester* EVM keys (channel funder, not operator), and identity diversity is computed in the EVM-address space because channel deposits are USDC. Ed25519 is reserved for wire-level operator authentication ([ADR 005](005-protocol.md)); on-chain Ed25519 verification was already rejected as too expensive ([ADR 014 Alternatives](014-on-chain-verification.md#alternatives-considered)).

### 3. Identity-diversity gating

A "distinct client identity" for gauge eligibility purposes is a `clientPubKey` (Ethereum address) satisfying *all* of:

| Criterion | Default | Bounds (governable per [ADR 009](009-governance.md)) |
| --- | ---: | --- |
| **Funded-channel minimum.** Address has at least one channel with the operator (or any operator) where the lifetime aggregate `deposit` ≥ `MIN_CHANNEL_FUNDING_USDC`. Tracked via the per-client cumulative-deposit counter introduced by this ADR and specified in [ADR 003 — StablePaymentChannel](003-payments.md#stablepaymentchannel) (`lifetimeDepositOf` semantics block) — `StablePaymentChannel.lifetimeDepositOf(client) view returns (uint256)`, monotonic, incremented by the funded amount on every `openChannel` and `topUp`. The counter is only ever incremented (decay/withdraw doesn't reduce it) so the metric tracks cumulative capital ever bonded into the protocol by this client. Adds one SSTORE per `openChannel` / `topUp`. | 10 USDC | `[1, 100]` USDC |
| **Funding age.** First channel deposit by this address occurred at least `MIN_CLIENT_AGE` before the receipt's `timestamp`. | 24 h | `[1 h, 30 d]` |
| **Per-channel cooldown.** A client identity is counted at most once per `IDENTITY_COOLDOWN` window per operator, regardless of how many channels or how many receipts it produces. | 7 d | `[1 d, 30 d]` |
| **Funding-source diversity (advisory at v1 — see note below).** The address's USDC balance for the qualifying channel deposit was not received from `operatorAddress`, the operator's known affiliated addresses (registered per [ADR 008](008-reputation.md)), or any other client identity already counted toward this operator's distinct-client set in the current epoch. **Challengers compute this signal off-chain** by indexing public USDC `Transfer` events from L2 RPC at the time of channel funding; the diversity evidence is supplied alongside the `ChallengeReceiptSummary` submission. On-chain enforcement at challenge time is bounded by the EVM's 256-block `BLOCKHASH` window — a full Merkle proof of the historical funding tx is **not** practical without a dedicated block-hash oracle or storage-proof verifier (`reth`-style execution-state proof against an L1-anchored root, deferred to [ADR 017](017-privacy.md) future work). For now, on-chain challenge resolution accepts the challenger's bonded submission plus the standard counter-evidence window from [ADR 014 Bond Handling](014-on-chain-verification.md); deeper cryptographic proof is a v2 hardening. | — | — |
| **Reputation gate (forward to §6).** If the operator's reputation score is below `medium_rep_threshold`, the funded-channel minimum and funding age are tightened (see §6). | — | — |

##### Note on the advisory funding-source diversity criterion

The funding-source diversity criterion is **advisory at v1** and not cryptographically enforced on-chain. The on-chain enforcement path is bounded by the EVM's 256-block `BLOCKHASH` window; a full Merkle proof of the historical funding transaction requires storage-proof verification (`reth`-style execution-state proofs against an L1-anchored root), which is deferred to [ADR 017](017-privacy.md) future work. Until ADR 017 ships, challengers compute this signal off-chain and submit it bonded, and on-chain dispute resolution accepts the bonded submission plus the standard counter-evidence window per [ADR 014 Bond Handling](014-on-chain-verification.md).

The criterion is retained in v1 as defense-in-depth: it raises the off-chain investigation cost for sophisticated wash-trading rings, and bonded challenges still create economic accountability. **The load-bearing economic deterrent is the abuse-cost floor: `MIN_CHANNEL_FUNDING_USDC × MIN_DISTINCT_CLIENTS_PER_EPOCH` of permanently-parked USDC capital plus a per-sybil rotation cycle of `IDENTITY_COOLDOWN`** (the other three criteria above), which is fully on-chain enforceable and stands independently of this heuristic. At default parameters that is 50 USDC of capital plus a 7-day rotation cycle per operator gauge-share inflated. The §Sizing rationale below sizes the floor without crediting the funding-source signal — launch safety does not depend on it.

When ADR 017 ships, this criterion is upgraded to a cryptographic check (storage-proof verification of historical USDC `Transfer` events) and the advisory tag is struck. Until then, readers should treat any §3 / §5 reference to "funding-source diversity" as advisory.

#### Per-epoch eligibility threshold

An operator is eligible for the gauge pool in epoch `e` only if:

```
distinct_clients_e(operator) ≥ MIN_DISTINCT_CLIENTS_PER_EPOCH
```

Where `distinct_clients_e(operator)` counts the unique `clientPubKey` values appearing in receipts the operator commits to chain for epoch `e`, after applying the cooldown and diversity rules above.

| Parameter | Default | Bounds |
| --- | ---: | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | `[1, 50]` |
| `medium_rep_threshold` | 0.50 (per [ADR 008 §12.1](008-reputation.md#121-receipt-tiers)) | governable within `[0.30, 0.70]` per ADR 008 |

#### Sizing rationale

A single sybil costs the attacker the funded-channel minimum (10 USDC), the funding-age delay (24 h), and the cooldown (7 d). Five distinct sybils per week is 50 USDC of capital permanently parked in payment-channel deposits per operator per week, plus on-chain Tx fees to fund and rotate. Larger thresholds harden against sybils linearly at the cost of cold-start UX; 5 is a reasoned default at tens-of-nodes-scale. Production data should retune this — the parameter is governable and the safety bounds are wide.

#### Cold-start exception

During the first 8 epochs of mainnet (governance-set `gaugeBootstrapEpochs`, default 8, max 26), `MIN_DISTINCT_CLIENTS_PER_EPOCH` is reduced to `1`. The intent is to let the gauge pool distribute meaningfully even when the network has fewer aggregate clients than the steady-state threshold; the wash-trading risk is bounded during this window because externally-funded operator-recruitment programs ([ADR 026 §10](026-gauge-boost-tokenomics.md#10-bootstrap-mechanism-pre-seed-usdc)) dominate early-epoch bytes anyway. Bootstrap-window receipts are still validated; only the *threshold* is lowered.

#### Optional reputation-attested registry

The protocol does not require a centralized identity registry. However, a `ClientIdentityRegistry` view contract may be supplied by reputation attesters ([ADR 008](008-reputation.md)) so well-known stable client identities (e.g., operators of large origin-backed services purchasing CDN bandwidth) can be vouched for and bypass the funding-age gate. The registry is opt-in; clients without a registry entry use the default funded-channel-and-cooldown rules unchanged. Registry entries are themselves subject to the `SlashJudge` challenge path if a challenger has evidence the vouched-for identity is operator-affiliated.

### 4. On-chain anchoring (Merkle-batched)

Per-receipt on-chain storage is uneconomical at scale. A 1 Gbps node produces ~30k receipts/month at the default 1 MB voucher cadence ([ADR 003 Voucher Interval Negotiation](003-payments.md)); 1,000 such operators are 30M receipts/month. Storing one log entry per receipt is comparable in cost to settling all the channels themselves.

The protocol mirrors the **keccak256 Merkle-batch pattern** from [ADR 014 §2 Production Path](014-on-chain-verification.md#production-path-interactive-keccak256-merkle-proof-future-adr): receipts are committed via root, individual receipts surface only on challenge.

#### Per-epoch commitment

Operators commit receipt roots **per epoch, directly to `FeeRouter`**, independent of any channel's settlement state:

```solidity
function commitEpochReceiptRoot(uint64 epochId, bytes32 root) external;
```

The caller is recorded as the credited operator (`operator = msg.sender`); no role gate is required — the operator is self-authorising for their own gauge eligibility, and a third party cannot grief by submitting roots on someone else's behalf. Callable any time during the epoch or during the post-epoch commit window. Each call appends one leaf to the per-(operator, epoch) MMR accumulator (see *Per-epoch summary* below). Each receipt's `epochId` field — signed by the requester at receipt-creation time — buckets that receipt's bytes into the corresponding epoch's `EpochReceiptSummary`. A receipt batch may therefore span multiple epochs; there is no requirement that all receipts in a single physical batch share an `epochId`.

**Decoupling from channel settlement.** Gauge eligibility does **not** flow through `StablePaymentChannel.settleChannel`. The settlement path is purely the USDC payment path; the receipt-root commitment path is separate. This decouples channel settlement cadence from gauge-credit cadence:

- A 90-day channel ([ADR 003 maxChannelDuration](003-payments.md)) accumulates receipts across ~13 epochs. The operator commits per-epoch receipt roots as the channel runs, and gauge credit accrues to each epoch as its `EpochReceiptSummary` is committed at epoch rollover.
- Operators settle channels at whatever cadence minimizes their gas (long-lived channels with periodic top-ups; short-lived channels with frequent close+settle) while still capturing gauge eligibility on the per-epoch cadence the boost mechanism requires.
- An operator who never calls `commitEpochReceiptRoot` for an epoch contributes zero to gauge eligibility for that epoch, regardless of how many channels they settled. This is the explicit opt-out: bytes still earn the 40% direct payout via `routeSettlement`, but they do not credit the 40% gauge bucket.

**Why per-epoch and not per-settlement.** An earlier draft attempted a per-settlement commit path keyed to `StablePaymentChannel.commitReceiptRoot` then read at `settleChannel` time. That design carried a race window: because `settleChannel` is permissionless after the dispute deadline, a third-party settler calling `settleChannel` before the operator committed the root would lock the settlement to `bytes32(0)` and forfeit gauge credit for the bytes settled. The per-epoch path eliminates the race because the commitment is bound to the operator's address and an `epochId`, not to a channel-level state slot that anyone can race to read.

#### Batch shape

A `ReceiptBatch` is a keccak256 Merkle tree over the §1 field set plus `keccak256(signature)` (which canonicalises EOA vs. arbitrary ERC-1271 signatures into a fixed-width leaf). Tree construction matches the [ADR 014](014-on-chain-verification.md) pattern: `keccak256(left || right)` internal nodes, zero-padded for non-power-of-2 leaf counts, index-prefixed leaves to prevent second-preimage. A 30k-receipt monthly batch produces a 15-deep tree (~480 bytes per challenge proof).

#### Per-epoch summary

At epoch rollover, `FeeRouter` aggregates all `commitEpochReceiptRoot` calls observed for an operator during the epoch into a single per-epoch `EpochReceiptSummary`:

| Field | Type | Description |
| --- | --- | --- |
| `operator` | `address` | The operator the summary is for. |
| `epochId` | `uint64` | The epoch this summary covers. |
| `claimedBytes` | `uint256` | Operator-asserted sum of `bytesClaimed` across all receipt leaves committed under this operator's `epochId` MMR; this is the `bytes_i` plugged into the gauge formula. Subject to challenge — the contract has no way to derive this on-chain because `commitEpochReceiptRoot` carries only the root, not bytes, and the receipt-epoch axis (`receipt.epochId`) is not aligned with channel settlement timing (long-lived channels can deliver receipts in epoch *e* but settle the paying voucher many epochs later). Authoritative only after the challenge window closes unchallenged. |
| `claimedDistinctClients` | `uint32` | Operator-asserted count of unique `clientPubKey` across the committed receipt batches. Subject to challenge. |
| `aggregateRoot` | `bytes32` | Bagged-peaks root of the per-(`operator`, `epochId`) MMR (see "Aggregator implementation — MMR accumulator" below). One inclusion proof reaches any individual receipt in two hops: receipt → batch root via §Batch-shape proof, batch root → MMR root via peak-path proof. |

The `EpochReceiptSummary` is committed once per operator per epoch by the operator (or any third party) calling `FeeRouter.commitEpochSummary(operator, epochId, summary)` after the epoch closes and before the gauge claim window opens (claim-window timing per [ADR 026 §2 Epoch mechanics](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)). The contract verifies that `aggregateRoot` is consistent with the per-call commit roots already accumulated in the per-(operator, epoch) MMR. `claimedBytes` and `claimedDistinctClients` are both operator-asserted and taken at face value pending the challenge window — neither is derivable on-chain from `commitEpochReceiptRoot` alone (which carries only roots), and both must be verified post-hoc against the committed receipt leaves under the §Challenge window mechanism below.

The `FeeRouter` here is the same contract that holds the §9 launch-prerequisite state — `gaugeLaunched`, `preLaunchGaugeAccumulator`, `enableGauge()` — pinned in [ADR 026 §2 Pre-receipt-launch gauge accumulation](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553). Receipt-path entry points and the gauge-pause state coexist on a single contract; ADR 026 §2 is the single source of truth for the gauge-pause shape, and this ADR §9 forward-references it.

##### Aggregator implementation — MMR accumulator

> **Motivation.** A naive implementation that stores every commit root and re-hashes them on `commitEpochSummary` is O(N) per epoch in storage reads + hash steps, where N is the operator's per-epoch commit count. At the reference scale (~30k receipts/month split across an operator's commit cadence) this is gas-prohibitive on L2. A rolling Merkle Mountain Range (MMR) achieves O(log N) append + O(log N) inclusion-proof verification with the same security as plain-hash; both produce equivalent inclusion proofs. The cost asymmetry is decisive on L2 fee markets.

**Pinned mechanism.** The `FeeRouter` contract MUST maintain a per-(`operator`, `epochId`) rolling MMR accumulator. Plain-hash storage of every commit root is rejected at the contract level.

```solidity
// Per-(operator, epoch) rolling MMR storage on FeeRouter.
// Domain tags: 0x00 = leaf, 0x01 = internal node (append-time merge), 0x02 = peak-bagging fold.
// peaksKey  = keccak256(abi.encode(operator, epochId, height))
// epochKey  = keccak256(abi.encode(operator, epochId))
mapping(bytes32 => bytes32) internal peaks;
mapping(bytes32 => uint64)  internal leafCount;

/// Appends keccak256(0x00 || root) as a leaf and merges peaks bottom-up via
/// keccak256(0x01 || left || right). O(log N) hashes.
/// Operator is `msg.sender` (see §4 prose above and the ADR 016 §3 cross-contract
/// call-graph row for `commitEpochReceiptRoot`).
function commitEpochReceiptRoot(uint64 epochId, bytes32 root) external;

/// Folds remaining peaks shortest-first: acc starts at the shortest peak; taller peaks
/// accumulate on the LEFT at each step via keccak256(0x02 || peaks[i] || acc), so the
/// tallest peak ends up as the outermost hash. See the bagging algorithm in the prose below.
/// Returned as `aggregateRoot` on commitEpochSummary; stable for the (operator, epochId) pair.
function epochAggregateRoot(address operator, uint64 epochId) public view returns (bytes32);
```

- **Append rule.** Leaves and internal nodes are domain-separated to make leaf-vs-node confusion impossible across both the §Batch-shape tree and the MMR layer:
  - **Leaf:** `keccak256(0x00 || root)` (1-byte tag + the §Batch-shape root).
  - **Internal node (peak merge during append):** `keccak256(0x01 || left || right)` (1-byte tag + child-pair concatenation, where `left` is the peak being merged into and `right` is the newly-promoted leaf-derived peak — the same orientation as the §Batch-shape internal-node rule).
  - Peaks merge bottom-up: when an append produces a new peak at height `h` and a peak already exists at height `h`, they merge into a single peak at height `h+1`, repeating until no collision remains.
- **Root extraction.** The MMR's "current root" at `commitEpochSummary` time is the bagged-peaks root. Let `peaks[]` be the array of MMR peaks ordered by peak height with `peaks[0]` the tallest and `peaks[length-1]` the shortest. The fold initialises the accumulator from the shortest peak and incorporates each remaining peak in order of increasing height (walking the array from index `length-2` down to `0`), hashing each peak with the accumulator using domain-tag `0x02`. The tallest peak ends up as the outermost hash:

  ```text
  if peaks.length == 0: return bytes32(0)            // empty MMR
  if peaks.length == 1: return peaks[0]              // single-peak case (one tree, no bagging)
  acc = peaks[peaks.length - 1]                      // start from the shortest peak
  for i = peaks.length - 2 down to 0:                // walk toward the tallest peak
      acc = keccak256(0x02 || peaks[i] || acc)       // higher peak on the LEFT of the hash
  return acc
  ```

  Returned as `aggregateRoot` in the §Per-epoch-summary table above. All three domain tags (`0x00`, `0x01`, `0x02`) are single bytes (Solidity `bytes1`) prepended via `abi.encodePacked(bytes1(0xNN), <fields>)` — no length prefix, no padding. The `0x02` tag distinguishes peak-bagging hashes from leaf hashes (`0x00`) and append-time internal-node hashes (`0x01`); a verifier cannot confuse a bagging hash with a tree-internal hash even on otherwise-colliding inputs.
- **Inclusion proof.** Consumed by §Challenge window. A receipt's proof is a `(leafIndex, siblingPathWithinPeak, peakIndex, peakPath)` tuple where `peakPath` is the array of *other* peaks needed to reproduce the bagging. The verifier (a) replays the leaf-tagged hash up the named peak using `siblingPathWithinPeak`, then (b) reconstructs the bagged root by replaying the `0x02`-tagged folds against `peakPath`. Verification cost is **`peakHeight + (numPeaks − 1)` `keccak256` calls** — bounded above by the within-peak depth plus the bagging-fold depth. At the reference scale (~30k receipts), max peak height is `⌈log₂(30000)⌉ = 15` and `popcount(30000) = 7` peaks ⇒ worst-case ≈ **15 + 6 = 21 hashes** per inclusion proof.
- **Storage retention.** `peaks` and `leafCount` entries for an `(operator, epochId)` pair are retained for the §Challenge window duration (7 days) plus a `MMR_RETENTION_BUFFER` (default **1 day**, governable within `[1 day, 30 days]` per [ADR 009 § Other protocol parameters](009-governance.md#other-protocol-parameters)). After retention expires, the entries are zeroed for refund eligibility — Solidity does not reclaim storage slots; the SSTORE-to-zero produces a refund (capped at 20 % of the surrounding tx gas post-EIP-3529, with Arbitrum's L2 schedule applying its own clamp), and the slots remain allocated indefinitely with zero values. Post-zeroing, the retained `aggregateRoot` on the `EpochReceiptSummary` record continues to identify the epoch's commitments, but leaf-level inclusion proofs are no longer reconstructable on-chain — challengers MUST cache `peaks` off-chain before submitting a `ChallengeReceiptSummary` if there is any risk the §Challenge window resolution will land after the retention window expires.
- **Gas validation.** The MMR-append + byte-counter combined per-`commitEpochReceiptRoot` cost on Arbitrum One fee markets is a pre-mainnet validation requirement per [Appendix: L2 Deployment](appendix-l2-deployment.md).

#### Challenge window

A 7-day window (matches the gauge-claim-window opening) during which any address may submit a `ChallengeReceiptSummary` claim against an operator's summary. Two failure modes are challengeable: (a) `claimedDistinctClients` overstated relative to the unique `clientPubKey` count under the §3 identity-diversity rules; (b) `claimedBytes` overstated relative to the sum of `bytesClaimed` across the receipt leaves committed under the operator's `epochId` MMR. A successful challenge on either field zeros that field for the epoch — gauge eligibility is forfeited in line with §7 (zeroed `claimedBytes` ⇒ `bytes_i = 0` in the gauge formula; zeroed `claimedDistinctClients` ⇒ sub-threshold ⇒ same outcome). An unsuccessful challenge forfeits the challenger's bond per [ADR 014 Bond Handling](014-on-chain-verification.md). Challenge evidence is supplied as Merkle inclusion proofs against the on-chain MMR root: a `claimedBytes` overstatement is proved by opening enough leaves that the partial sum already exceeds the asserted total, or by full enumeration; a `claimedDistinctClients` overstatement is proved by opening leaves whose `clientPubKey` set is smaller than asserted.

### 5. Challenger role (see [Appendix: Fraud Detection](appendix-fraud-detection.md))

The challenge window is permissionless. Any address with sufficient bond capital and the technical capacity to monitor the chain may submit a `ChallengeReceiptSummary`. There is no protocol-defined "validator" role, no on-chain registration, no per-operator subscription, and no fee paid by monitored parties — the challenger's incentive is the bond + reward on a successful challenge.

A challenger investigating an operator's epoch performs:

1. **Signature validity.** For each receipt in the batch, recompute the EIP-712 digest, verify the signature with `SignatureChecker`, and verify all five invariants in §1.
2. **Identity diversity over recent epochs.** Apply the §3 rules — funded-channel minimum, funding-age, per-operator cooldown, funding-source diversity (**advisory at v1** per the §3 note pending [ADR 017](017-privacy.md)) — and compute the actual distinct-client count. Compare with `claimedDistinctClients` in the operator's `EpochReceiptSummary`.
3. **Cross-checks against suspicious clustering.** Heuristics flag ancestry / timing / byte-distribution patterns characteristic of self-routed traffic (e.g., contiguous-EOA "address generator" patterns, common-ancestor funding, jitter-free byte counts). Exact catalog is implementation-defined; what is fixed is the **interface** — a heuristic flag is sufficient grounds to open a `ChallengeReceiptSummary`, but the on-chain dispute resolves on cryptographic evidence (signature validity, channel-funding ancestry traces), never on the heuristic itself.

#### Bond model

Receipt-fraud challenges use the existing `SlashJudge` bond mechanism from [ADR 014 Bond Handling](014-on-chain-verification.md). Successful challenges award the bond back plus a receipt-fraud reward; unsuccessful challenges forfeit the bond per the standard 50%-burn / 50%-to-operator rule. The reward is a parameter on the receipt-fraud challenge handler (deployed alongside `SlashJudge`'s existing phantom / rate / blacklist / corruption types) governable per [ADR 009](009-governance.md); a concrete default and bounds are not yet specified in this ADR and will be pinned with the handler. No new on-chain role is introduced.

#### Separation from the operator

A challenger MUST NOT be operated by the operator they challenge. The operator's affiliated-address registry ([ADR 008](008-reputation.md)) is queried; a challenger address that overlaps with `operatorAddress`'s known affiliates is ineligible. This is enforced on-chain in `SlashJudge` at challenge submission time as part of the receipt-fraud challenge handler. The check defends against a self-griefing pattern where an operator burns its own bond to depress visible challenge volume.

### 6. Reputation gating (forward to [ADR 008](008-reputation.md))

Receipts function without reputation. Reputation hardens them.

Operators below `medium_rep_threshold` (default **0.50**, governable within `[0.30, 0.70]` per [ADR 008 §12.1](008-reputation.md#121-receipt-tiers) — that ADR is the canonical home for the threshold value and bounds) face stricter receipt requirements:

| Parameter | Above threshold | Below threshold |
| --- | --- | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | 10 |
| `MIN_CHANNEL_FUNDING_USDC` | 10 USDC | 25 USDC |
| `MIN_CLIENT_AGE` | 24 h | 72 h |
| Cold-start bootstrap exception | applies | does not apply |

The intent: a high-reputation operator with stable historical traffic has a high cost of false-flagging by an attacker (loss of historical reputation > epoch-level gauge gain), so a thinner identity-diversity threshold is acceptable. A new or recently-slashed operator faces tighter gates, raising the capital cost of wash-trading proportional to the trust deficit.

**Integration is non-blocking.** The receipt protocol does not require reputation to function — `medium_rep_threshold` may be configured to its lower bound (0.30) in early production so almost all operators clear it and get the relaxed thresholds. The protocol upgrades when reputation is online, not when it ships.

#### Forward-compatible

[ADR 008](008-reputation.md) is updated separately to expose `reputationOf(operator)` as an on-chain view that `FeeRouter.commitEpochSummary` reads at commit time.

### 7. Failure semantics

The receipt protocol fails open for **payment** and fails closed for **gauge eligibility**. Distinguishing these two is the central design choice and the section operators most need to read.

| Scenario | Operator's 40% base USDC payout | Gauge-pool eligibility for the epoch | Slashing |
| --- | --- | --- | --- |
| Settlement with valid voucher; operator commits valid epoch receipt root via `commitEpochReceiptRoot` (the happy path). | **Paid same-tx** at settlement. | Counts toward `claimedBytes` and `claimedDistinctClients`. | None. |
| Settlement with valid voucher but operator never calls `commitEpochReceiptRoot` for the epoch (explicit opt-out, or has no signed receipts to commit). | **Paid same-tx** at settlement. | These bytes contribute `0` to `claimedBytes` for gauge purposes; settlement bytes still count for direct-payout proportional accounting. | None. |
| Settlement with valid voucher and a committed epoch root whose batch contains receipts with invalid signatures or invariant violations, **or** an `EpochReceiptSummary` whose `claimedBytes` / `claimedDistinctClients` overstates the truth derivable from the committed leaves. | **Paid same-tx** at settlement. | On a successful challenge, the disputed field is **zeroed** for the epoch — `claimedBytes = 0` collapses `bytes_i` in the gauge formula to zero; `claimedDistinctClients = 0` falls below `MIN_DISTINCT_CLIENTS_PER_EPOCH` and disqualifies the epoch entirely. Either path drives the operator's gauge share for that epoch to zero. The 40% base for any underlying settlement is not clawed back. | Repeated invalid-batch behavior escalates per the [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) lifetime offence counter, on the 5/15/50% slashing schedule. A single bad batch is not a slash — only a *successful challenge with cryptographic evidence of forgery* (e.g., signature recovery yielding an address that never opened a channel) is. |
| Operator delivers bytes but gauge eligibility is forfeited for any reason in this table (zeroed batch, missing summary, sub-threshold distinct clients). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None unless triggered separately. |
| Operator below `MIN_DISTINCT_CLIENTS_PER_EPOCH` after honest validation (low traffic, regional cold-start). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None. |

#### The cashflow invariant

Settlement always pays the operator's 40% base ([ADR 026 §2 Same-transaction guarantees](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553); [ADR 026 §3 Properties](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)). Receipt validity gates only the **40% gauge pool share**, never the **40% base share**. An operator running honest delivery with an immature client base receives full base USDC and zero gauge — they are commodity operators in the [ADR 026 §7 Case A](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) sense. This is the designed incentive pressure, not a punishment.

#### The slashing invariant

Receipt protocol violations are **not by themselves slashable**. A successful Merkle-anchored challenge zeros gauge eligibility for the *current epoch only*. Slashing escalates only via the [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) schedule and only when challenger-submitted evidence proves a deliberate forgery (e.g., a recovered signer that does not correspond to any channel `client` that ever existed, indicating an outright fabricated signature). Routine receipt invalidation — wrong root committed, late batch commit, signature on a closed channel — is corrected by zeroing the epoch and not by slashing.

### 8. Privacy considerations (forward to [ADR 017](017-privacy.md))

Per-receipt client identity is **observable on-chain when challenged**. The Merkle root committed at settlement does not reveal individual client addresses, but the inclusion proof produced during a challenge does — `clientPubKey` is one of the leaf fields. Settlement-time disclosure is minimal (one root); challenge-time disclosure is partial (only the receipts the challenger needed to surface to win). However, challenges are public — anyone watching the L2 chain sees which client addresses funded which operators on which dates.

This is a real privacy regression from the [ADR 003](003-payments.md) baseline, where channel deposits are on-chain (identifying the *funder*) but per-stream byte counts are not. Adding per-stream attestation signed by the funder makes the funder's per-stream activity legible whenever a challenge surfaces them.

**Mitigations within this ADR (no protocol change):** receipts are stored off-chain and surface only on challenge; `contentRoot` is a chunk Merkle root, not a blob hash; per-epoch batching limits each challenge's disclosure to the batch's leaves.

**Future work in [ADR 017](017-privacy.md):** zero-knowledge identity-diversity proofs (replace cleartext leaves with a "distinct count" proof) and stealth-address client identities are the canonical paths. Both are significant work and **not** launch blockers; this ADR ships the cleartext protocol because shipping wash-trading defense late is the worse outcome.

### 9. Implementation sequencing and launch prerequisite

**The protocol can technically launch without distinct-client receipts.** The voucher path in [ADR 003](003-payments.md) is independent of receipts; settlements pass `bytesDelivered` to `FeeRouter` whether a receipt batch is committed or not. Operators receive their 40% base USDC. The 40% gauge pool, the 7% delegator pool, the 5% burn, the 5% treasury, and the 3% safety reserve all accumulate normally.

**But the gauge pool MUST be paused until receipts ship.** From [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks), wash-trading at TOKEN price 3–5× genesis is net-profitable without the receipt gate. Distributing a 40% pool to byte-counters whose inputs are unattestable is an open invitation. Therefore:

> **Launch prerequisite.** The `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and direct-base-payout flows can be deployed without distinct-client receipts. **The 40% gauge boost pool MUST NOT pay out until distinct-client receipts are live and verified at scale.** Until that moment, the 40% gauge bucket accumulates in `FeeRouter.preLaunchGaugeAccumulator[epoch]` per [ADR 026 §2 Pre-receipt-launch gauge accumulation](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553). Cutover is the one-shot `enableGauge()` governance setter; pre-launch epochs become claimable retroactively against the ve-snapshots already taken at each historical epoch boundary, with the 26-epoch claim window starting at `gaugeLaunchEpoch`. The operator-aligned share remains 80% of revenue ([ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)); the gauge half of that share is escrowed, not denied.

This is consistent with [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) — ADR 027 is listed there as "priority-1; not optional for production launch". The wording above pins down what "not optional" means concretely.

#### Sequencing

Format and client/operator libraries → contract paths deployed in parallel-run mode (no enforcement; `gaugeLaunched == false`, gauge bucket escrowing into `preLaunchGaugeAccumulator[epoch]`) → challenger tooling + heuristic library (see [Appendix: Fraud Detection](appendix-fraud-detection.md)) → reputation integration → governance call to `enableGauge()` → cutover (gauge gating on, `MIN_DISTINCT_CLIENTS_PER_EPOCH` active, pre-launch epochs become retroactively claimable). The `enableGauge()` invocation is launch readiness.

## Consequences

### Positive

- **Closes the wash-trading attack surface flagged in [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks).** Self-routed traffic now requires distinct, capital-funded sybil identities, raising the attack's effective cost to `MIN_DISTINCT_CLIENTS_PER_EPOCH × MIN_CHANNEL_FUNDING_USDC` of permanently-parked USDC plus per-sybil setup gas, plus a 7-day rotation cooldown. At default parameters: 50 USDC of permanent capital and a one-week rotation cycle per operator gauge-share inflated.
- **Reuses existing primitives.** secp256k1 / EIP-712 / `SignatureChecker`, the keccak256 Merkle pattern from [ADR 014](014-on-chain-verification.md), the `SlashJudge` bond machinery from [ADR 014 Bond Handling](014-on-chain-verification.md#bond-handling), and the reputation surface from [ADR 008](008-reputation.md). No new cryptography, no new on-chain role, no new contract beyond a `FeeRouter` extension and the `SlashJudge` receipt-fraud challenge handler.
- **Cleanly separates payment from gauge.** The 40% base USDC continues to flow same-tx for every settlement regardless of receipt validity — the cashflow invariant operators rely on is preserved.
- **Composable with reputation.** Operators with strong reputation get cheaper gating; new and recently-slashed operators face tighter gating. Receipt validity is a *layer* over reputation, not a replacement.
- **On-chain footprint is bounded.** One Merkle root per settlement, one summary per operator per epoch. Per-receipt storage is challenge-only.
- **Aligns with the [ADR 014](014-on-chain-verification.md) production direction.** The Merkle-batched anchoring chosen here is the same construction that production-path corruption proofs will use; toolchain reuse across ADRs is direct.

### Negative

- **Cold-start UX cost.** Small clients now sign one extra EIP-712 message per voucher. The signing UX is a single popup if the wallet is unlocked and offers `personal_sign`-compatible EIP-712 (Safe, MetaMask, Rainbow, etc.). For headless clients (CLI, automated workflows) the cost is negligible. The cost is real for first-time browser users; client libraries should auto-batch the receipt-signing prompt with the voucher-signing prompt where the wallet permits.
- **Off-chain validation cost is on the challenger, not on the protocol.** A would-be challenger must validate the operator's batch before submitting `ChallengeReceiptSummary` — at the network mean (1k operators × 30k receipts/month) full coverage is ~30M signature verifications per month. `SignatureChecker` at ~3k gas off-chain (no transaction) is a few seconds of CPU per 100k signatures; scales linearly. A challenger running per-region is comfortably within commodity-VPS budgets. The economics are MEV-style: bond + reward on a successful challenge fund the work; the protocol does not subsidize unsuccessful or speculative coverage. See [Appendix: Fraud Detection](appendix-fraud-detection.md) for the operational shape.
- **Privacy regression from baseline.** Per-receipt client identity is on-chain-observable on challenge. Documented above and forward-referenced to [ADR 017](017-privacy.md). Mitigations exist; they are future work.
- **Operator-side per-epoch commit overhead.** Each `commitEpochReceiptRoot` call costs one MMR-append operation (~10–15k gas amortized at the per-(operator, epoch) MMR depth). Operators choose their commit cadence — once per epoch (cheapest, longest receipt-validation lag) up to once per channel settlement (highest gas, finest-grained gauge accrual). At the chosen L2 ([Appendix: L2 Deployment](appendix-l2-deployment.md)) ~$0.001–0.005 per commit. Total overhead per operator per epoch is bounded by their chosen cadence × the per-call cost.
- **`FeeRouter` MMR storage surface.** §4 Aggregator implementation adds two storage mappings (`peaks` and `leafCount`, each keyed on a per-(`operator`, `epochId`)/per-height hash) plus one external view function (`epochAggregateRoot`). Storage entries are zeroed for refund eligibility after the §Challenge window + `MMR_RETENTION_BUFFER`, but the slots remain allocated; long-term L2 storage growth is bounded by the active-epoch operator count (not the lifetime-epoch count) and by the rotation cadence governance picks for `MMR_RETENTION_BUFFER`. Audit must include the MMR append/extract/proof paths and the retention-buffer governance setter.
- **`commitEpochSummary` is a new keeper-class job.** Per operator, per epoch, somebody must call `commitEpochSummary` after the epoch closes and before the gauge claim window opens. Operators have the strongest incentive (their gauge share depends on it); third parties may be compensated by an operator-paid commit fee. Not a new failure class — it parallels the existing settlement-bot pattern from [ADR 003](003-payments.md) where any address can call `settleChannel`.
- **Bootstrap-window compromise.** The cold-start exception (§3) lowers `MIN_DISTINCT_CLIENTS_PER_EPOCH` to 1 for the first 8 epochs. Wash-trading is harder to detect during the bootstrap window — mitigated by externally-funded operator dominance in early traffic, but not eliminated. Document and monitor; tighten earlier than 8 epochs if bootstrap traffic pattern allows.

### Risks

- **Threshold tuning is empirical.** `MIN_DISTINCT_CLIENTS_PER_EPOCH = 5`, `MIN_CHANNEL_FUNDING_USDC = 10`, and `IDENTITY_COOLDOWN = 7 d` are reasoned defaults. Production data may show that 5 is too sharp a filter for small regional operators, or too lax against sophisticated sybil farms. The parameters are governable; the safety bounds are wide enough for both directions. Monitor and retune in the first 6 months.
- **Funding-source diversity heuristic gameability.** A determined attacker can route sybil funding through public mixers, CEX deposit/withdraw cycles, or third-party DEX swaps to disguise common ancestry. The diversity heuristic catches naive ancestor patterns; sophisticated attackers degrade it. The fundamental cost — funded-channel minimum × distinct-clients-per-epoch × cooldown — remains the binding deterrent regardless of heuristic sophistication.
- **Challenger coverage must be sufficient.** If no challenger investigates an operator's epoch within the 7-day window, an invalid batch is never challenged and `claimedDistinctClients` stands. Mitigated by the bond + reward economics: any obscure operator that pays out gauge inflation is a profitable target for the first challenger that finds them. Failure mode is more likely "an attacker picks an obscure operator with no challenger coverage and abuses it for one epoch" than "the protocol globally fails"; reputation gating (§6) is the secondary defense, and persistent abusers attract escalating attention from challengers chasing the larger reward. See [Appendix: Fraud Detection](appendix-fraud-detection.md).
- **Reputation-receipt feedback loop.** Operators with high reputation get easier receipt thresholds. If reputation is gameable upstream, a sophisticated attacker first farms reputation, then exploits the relaxed threshold. Reputation gameability is [ADR 008](008-reputation.md)'s problem, not this ADR's, but the loop is worth flagging — the receipt protocol is no stronger than the reputation it depends on for §6 gating.
- **Privacy-vs-defense tension.** Aggressive privacy upgrades (zero-knowledge identity-diversity proofs in [ADR 017](017-privacy.md)) complicate the funding-source diversity heuristic — challengers can no longer trace ancestry if identities are private. Tension to resolve in [ADR 017](017-privacy.md) when that path matures; this ADR ships with cleartext receipts.
- **Non-receipt-aware client wallets.** Clients without receipt-aware wallets produce voucher-only deliveries that are settled and paid normally, but their bytes do not credit gauge eligibility. Operators receiving such traffic carry the cost of pure-base economics for it. Acceptable — receipt-aware client libraries ship in the launch SDK; non-receipt-aware traffic is an asymptote that shrinks as the SDK propagates.
