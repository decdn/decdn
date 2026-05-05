# ADR 027: Distinct-Client Delivery Receipts

**Date:** 2026-04-25
**Status:** Draft
**Required for:** [ADR 026](026-gauge-boost-tokenomics.md) gauge-pool security
**Touches:** [ADR 003](003-payments.md), [ADR 007](007-watchtower.md), [ADR 008](008-reputation.md), [ADR 014](014-on-chain-verification.md)

## Context

[ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) defines the gauge-pool share as a function of `bytes_i` (verified bytes per operator) and `ve_i / total_ve` (the operator's ve-share). The formula bounds the *output* but says nothing about whether the *input* `bytes_i` is honest. The on-chain `claimedBytes` reaching `FeeRouter.routeSettlement` ([ADR 003](003-payments.md)) is signed by *some* address that opened a payment channel — nothing today prevents the operator from running both sides.

### The wash-trading attack

([ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks); design-spec §6.3): an operator opens a channel from a sybil client identity to themselves, self-routes traffic, and signs vouchers inflating `bytesDelivered`. Effective cost is the ~60% router skim on the operator's *own* USDC plus a few cents of L2 gas — roughly a 5–8% net loss in USDC terms, more than offset by gauge-share + ve-appreciation gains at TOKEN price 3–5× genesis. Settlement gas does not scale with claimed bytes. **Neither cost is a deterrent at the intended TOKEN price levels.**

Defense: byte counters that feed the gauge formula must be attested by **distinct, verifiable client identities** — counterparties the operator does not control. The voucher protocol from [ADR 003](003-payments.md) already proves *bytes were paid for*; this ADR adds a parallel artifact that proves *bytes were paid for by independent counterparties* and gates gauge eligibility on that artifact.

This ADR is forward-referenced from [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks) and [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) as priority-1, **not optional for production launch**.

## Decision

The protocol introduces a **DeliveryReceipt** primitive, paired one-to-one with each voucher submitted to `FeeRouter.routeSettlement`. Receipts are EIP-712 typed messages signed by the *requester's* secp256k1 key (the address that funded the payment channel). Gauge-pool eligibility for an operator's epoch is gated on a **distinct-client diversity threshold** verified against the receipts the operator commits to chain.

Receipts are batched into a Merkle tree per operator per epoch; only the root and a small summary are stored on-chain at settlement time, mirroring the keccak256 Merkle pattern in [ADR 014 §2 Production Path](014-on-chain-verification.md). Individual receipts surface only on challenge, processed by watchtowers ([ADR 007](007-watchtower.md)) and gated by reputation ([ADR 008](008-reputation.md)).

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
| **Funding-source diversity.** The address's USDC balance for the qualifying channel deposit was not received from `operatorAddress`, the operator's known affiliated addresses (registered per [ADR 008](008-reputation.md)), or any other client identity already counted toward this operator's distinct-client set in the current epoch. **Watchtowers compute this signal off-chain** by indexing public USDC `Transfer` events from L2 RPC at the time of channel funding; the diversity attestation enters the watchtower-signed receipt-validation output (§5). On-chain enforcement at challenge time is bounded by the EVM's 256-block `BLOCKHASH` window — a full Merkle proof of the historical funding tx is **not** practical without a dedicated block-hash oracle or storage-proof verifier (`reth`-style execution-state proof against an L1-anchored root, deferred to [ADR 017](017-privacy.md) future work). For now, on-chain challenge resolution accepts the attesting watchtower's signature plus a corroborating attester (per §5 quorum); deeper cryptographic proof is a v2 hardening. | — | — |
| **Reputation gate (forward to §6).** If the operator's reputation score is below `medium_rep_threshold`, the funded-channel minimum and funding age are tightened (see §6). | — | — |

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

The protocol does not require a centralized identity registry. However, a `ClientIdentityRegistry` view contract may be supplied by reputation attesters ([ADR 008](008-reputation.md)) so well-known stable client identities (e.g., operators of large origin-backed services purchasing CDN bandwidth) can be vouched for and bypass the funding-age gate. The registry is opt-in and watchtower-validated; clients without a registry entry use the default funded-channel-and-cooldown rules unchanged.

### 4. On-chain anchoring (Merkle-batched)

Per-receipt on-chain storage is uneconomical at scale. A 1 Gbps node produces ~30k receipts/month at the default 1 MB voucher cadence ([ADR 003 Voucher Interval Negotiation](003-payments.md)); 1,000 such operators are 30M receipts/month. Storing one log entry per receipt is comparable in cost to settling all the channels themselves.

The protocol mirrors the **keccak256 Merkle-batch pattern** from [ADR 014 §2 Production Path](014-on-chain-verification.md#production-path-interactive-keccak256-merkle-proof-future-adr): receipts are committed via root, individual receipts surface only on challenge.

#### Per-settlement commitment

Operators commit a `bytes32 receiptBatchRoot` against a channel via `StablePaymentChannel.commitReceiptRoot(channelId, receiptBatchRoot)` — provider-only (`msg.sender == channel.provider`), callable any time before settlement. At settlement, `StablePaymentChannel.settleChannel` reads the stored root (defaulting to `bytes32(0)` if the operator opted out) and forwards it to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)`. A zero root signals "this settlement contributes nothing to gauge eligibility" — see §7. The commit / settle split is intentional: `settleChannel` itself remains callable by anyone (preserving the [ADR 003](003-payments.md) anyone-can-settle property), but the gauge-eligibility root is fixed at provider-only commit time, preventing third-party settlers from denying gauge credit by submitting a zero root.

#### Per-epoch bucketing (long-lived channels)

A receipt batch may span multiple epochs: each receipt's `epochId` field (signed by the requester at receipt-creation time) buckets that receipt's bytes into the corresponding epoch's `EpochReceiptSummary`. There is no requirement that all receipts in a single batch share the same `epochId`. This decouples channel settlement cadence from gauge-credit cadence:

- A 90-day channel ([ADR 003 maxChannelDuration](003-payments.md)) accumulates receipts across ~13 epochs. The operator can commit per-epoch receipt roots without waiting for channel settlement, and gauge credit accrues to each epoch when its `EpochReceiptSummary` is committed.
- Per-epoch operator commits use `FeeRouter.commitEpochReceiptRoot(operator, epochId, root)` — directly to the router, independent of any channel's settlement state.
- Channel `settleChannel` then settles USDC payout based on the voucher's `claimedAmount` and `claimedBytes`; `routeSettlement`'s `receiptBatchRoot` argument is now optional metadata for the *current* epoch only, redundant with the per-epoch commits above.

This pattern lets operators settle channels at the cadence that minimizes their gas while still capturing gauge eligibility on the per-epoch cadence the boost mechanism requires.

#### Batch shape

A `ReceiptBatch` is a keccak256 Merkle tree over the §1 field set plus `keccak256(signature)` (which canonicalises EOA vs. arbitrary ERC-1271 signatures into a fixed-width leaf). Tree construction matches the [ADR 014](014-on-chain-verification.md) pattern: `keccak256(left || right)` internal nodes, zero-padded for non-power-of-2 leaf counts, index-prefixed leaves to prevent second-preimage. A 30k-receipt monthly batch produces a 15-deep tree (~480 bytes per challenge proof).

#### Per-epoch summary

At epoch rollover, `FeeRouter` aggregates all `receiptBatchRoot` commitments observed for an operator during the epoch into a single per-epoch `EpochReceiptSummary`:

| Field | Type | Description |
| --- | --- | --- |
| `operator` | `address` | The operator the summary is for. |
| `epochId` | `uint64` | The epoch this summary covers. |
| `claimedBytes` | `uint256` | Sum of `claimedBytes` across all per-epoch receipt commits for this operator within `epochId` (independent of channel settlement timing — see Per-epoch bucketing above); this is the `bytes_i` plugged into the gauge formula. Authoritative input. |
| `claimedDistinctClients` | `uint32` | Operator-asserted count of unique `clientPubKey` across the receipt batches. Subject to challenge. |
| `aggregateRoot` | `bytes32` | Merkle root of the per-settlement `receiptBatchRoot`s for `epochId` (a tree of trees). One inclusion proof reaches any individual receipt in two hops. |

The `EpochReceiptSummary` is committed once per operator per epoch by the operator (or any third party) calling `FeeRouter.commitEpochSummary(operator, epochId, summary)` after the epoch closes and before the gauge claim window opens (claim-window timing per [ADR 026 §2 Epoch mechanics](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)). The contract verifies that `claimedBytes` matches the on-chain accumulated counter for the epoch and that `aggregateRoot` is consistent with the per-settlement roots already stored. `claimedDistinctClients` is taken at face value pending the challenge window.

> **Aggregator implementation — gas-cost note.** A naive implementation that stores every `receiptBatchRoot` and re-hashes them on `commitEpochSummary` is O(N) per epoch in storage reads + hash steps, where N is the operator's per-epoch settlement count. At the reference scale (~30k receipts/month split across O(1k) settlements per operator) this is gas-prohibitive on L2. The expected production implementation maintains a per-(operator, epoch) **rolling Merkle Mountain Range (MMR)** accumulator: each new `commitReceiptRoot` (or per-epoch commit) appends one leaf and updates O(log N) hashes; `commitEpochSummary` reads only the MMR's current root and depth. The MMR vs. plain-hash tradeoff is implementation-detail (more code, smaller gas footprint); both produce equivalent inclusion proofs. This ADR does not pin the choice, but production deployments SHOULD adopt the MMR pattern at scale.

#### Challenge window

A 7-day window (matches the gauge-claim-window opening) during which any address may submit a `ChallengeReceiptSummary` claim against an operator's summary. Watchtowers (§5) typically initiate. A successful challenge zeros `claimedDistinctClients` for the epoch — gauge eligibility is forfeited in line with §7. An unsuccessful challenge forfeits the challenger's bond per [ADR 014 Bond Handling](014-on-chain-verification.md).

### 5. Watchtower role (forward to [ADR 007](007-watchtower.md))

Receipt validation is added as a new **watchtower-class duty** on top of the channel-dispute monitoring already defined in [ADR 007](007-watchtower.md). Watchtowers serving the receipt-validation role are referred to as **receipt attesters** in this ADR; the term is interchangeable with "receipt-validating watchtower".

A receipt attester subscribed to an operator's epoch performs:

1. **Signature validity.** For each receipt in the batch, recompute the EIP-712 digest, verify the signature with `SignatureChecker`, and verify all five invariants in §1.
2. **Identity diversity over recent epochs.** Apply the §3 rules — funded-channel minimum, funding-age, per-operator cooldown, funding-source diversity — and compute the actual distinct-client count. Compare with `claimedDistinctClients` in the operator's `EpochReceiptSummary`.
3. **Cross-checks against suspicious clustering.** Heuristics flag ancestry / timing / byte-distribution patterns characteristic of self-routed traffic (e.g., contiguous-EOA "address generator" patterns, common-ancestor funding, jitter-free byte counts). Exact catalog is implementation-defined; what is fixed is the **interface** — a heuristic flag is sufficient grounds to open a `ChallengeReceiptSummary`, but the on-chain dispute resolves on cryptographic evidence (signature validity, channel-funding ancestry traces), never on the heuristic itself.

#### Bond model

Receipt-fraud challenges use the existing watchtower bond mechanism in [ADR 007](007-watchtower.md). Successful challenges award the bond plus a configurable receipt-fraud reward to the challenger; unsuccessful challenges forfeit the bond per [ADR 014 Bond Handling](014-on-chain-verification.md). Receipt attesters are not a new on-chain role — they are watchtowers running an additional module against the same `WatchtowerEscrow` contract.

#### Separation from the operator

Receipt attesters MUST NOT be operated by the operator they validate. The operator's affiliated-address registry ([ADR 008](008-reputation.md)) is queried; an attester address that overlaps with `operatorAddress`'s known affiliates is ineligible to settle challenges against that operator. This is enforced on-chain in `WatchtowerEscrow` at challenge submission time, mirroring the watchtower-collusion defense already in [ADR 007](007-watchtower.md).

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
| Settlement with valid voucher and valid receipt batch (the happy path). | **Paid same-tx** at settlement. | Counts toward `claimedBytes` and `claimedDistinctClients`. | None. |
| Settlement with valid voucher but **`receiptBatchRoot == 0`** (operator opted out, or has no signed receipts to commit). | **Paid same-tx** at settlement. | These bytes contribute `0` to `claimedBytes` for gauge purposes; settlement bytes still count for direct-payout proportional accounting. | None. |
| Settlement with valid voucher and a `receiptBatchRoot` whose batch contains receipts with invalid signatures or invariant violations. | **Paid same-tx** at settlement. | On a successful challenge, the operator's `claimedDistinctClients` is **zeroed** for the epoch; if zeroing drives the count below `MIN_DISTINCT_CLIENTS_PER_EPOCH`, the operator's gauge share for that epoch is zero. The 40% base for the disputed settlement is not clawed back. | Repeated invalid-batch behavior escalates per the [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) lifetime offence counter, on the 5/15/50% slashing schedule. A single bad batch is not a slash — only a *successful challenge with cryptographic evidence of forgery* (e.g., signature recovery yielding an address that never opened a channel) is. |
| Operator delivers bytes but gauge eligibility is forfeited for any reason in this table (zeroed batch, missing summary, sub-threshold distinct clients). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None unless triggered separately. |
| Operator below `MIN_DISTINCT_CLIENTS_PER_EPOCH` after honest validation (low traffic, regional cold-start). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None. |

#### The cashflow invariant

Settlement always pays the operator's 40% base ([ADR 026 §2 Same-transaction guarantees](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553); [ADR 026 §3 Properties](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)). Receipt validity gates only the **40% gauge pool share**, never the **40% base share**. An operator running honest delivery with an immature client base receives full base USDC and zero gauge — they are commodity operators in the [ADR 026 §7 Case A](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) sense. This is the designed incentive pressure, not a punishment.

#### The slashing invariant

Receipt protocol violations are **not by themselves slashable**. A successful Merkle-anchored challenge zeros gauge eligibility for the *current epoch only*. Slashing escalates only via the [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) schedule and only when watchtower evidence proves a deliberate forgery (e.g., a recovered signer that does not correspond to any channel `client` that ever existed, indicating an outright fabricated signature). Routine receipt invalidation — wrong root committed, late batch commit, signature on a closed channel — is corrected by zeroing the epoch and not by slashing.

### 8. Privacy considerations (forward to [ADR 017](017-privacy.md))

Per-receipt client identity is **observable on-chain when challenged**. The Merkle root committed at settlement does not reveal individual client addresses, but the inclusion proof produced during a challenge does — `clientPubKey` is one of the leaf fields. Settlement-time disclosure is minimal (one root); challenge-time disclosure is partial (only the receipts the challenger needed to surface to win). However, challenges are public — anyone watching the L2 chain sees which client addresses funded which operators on which dates.

This is a real privacy regression from the [ADR 003](003-payments.md) baseline, where channel deposits are on-chain (identifying the *funder*) but per-stream byte counts are not. Adding per-stream attestation signed by the funder makes the funder's per-stream activity legible whenever a challenge surfaces them.

**Mitigations within this ADR (no protocol change):** receipts are stored off-chain and surface only on challenge; `contentRoot` is a chunk Merkle root, not a blob hash; per-epoch batching limits each challenge's disclosure to the batch's leaves.

**Future work in [ADR 017](017-privacy.md):** zero-knowledge identity-diversity proofs (replace cleartext leaves with a "distinct count" proof) and stealth-address client identities are the canonical paths. Both are significant work and **not** launch blockers; this ADR ships the cleartext protocol because shipping wash-trading defense late is the worse outcome.

### 9. Implementation sequencing and launch prerequisite

**The protocol can technically launch without distinct-client receipts.** The voucher path in [ADR 003](003-payments.md) is independent of receipts; settlements pass `bytesDelivered` to `FeeRouter` whether a receipt batch is committed or not. Operators receive their 40% base USDC. The 40% gauge pool, the 7% delegator pool, the 5% burn, the 5% treasury, and the 3% safety reserve all accumulate normally.

**But the gauge pool MUST be paused until receipts ship.** From [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks), wash-trading at TOKEN price 3–5× genesis is net-profitable without the receipt gate. Distributing a 40% pool to byte-counters whose inputs are unattestable is an open invitation. Therefore:

> **Launch prerequisite.** The `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and direct-base-payout flows can be deployed without distinct-client receipts. **The 40% gauge boost pool MUST NOT pay out until distinct-client receipts are live and verified at scale.** Until that moment, the 40% gauge bucket either accumulates in `FeeRouter` (claimable retroactively when receipts ship) or is governance-routed to treasury / SafetyReserve as a temporary measure. The operator-aligned share remains 80% of revenue ([ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)); the gauge half of that share is escrowed, not denied.

This is consistent with [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) — ADR 027 is listed there as "priority-1; not optional for production launch". The wording above pins down what "not optional" means concretely.

#### Sequencing

Format and client/operator libraries → contract paths deployed in parallel-run mode (no enforcement) → watchtower module + heuristic library → reputation integration → cutover (gauge gating on, `MIN_DISTINCT_CLIENTS_PER_EPOCH` active). The cutover is launch readiness.

## Consequences

### Positive

- **Closes the wash-trading attack surface flagged in [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks).** Self-routed traffic now requires distinct, capital-funded sybil identities, raising the attack's effective cost to `MIN_DISTINCT_CLIENTS_PER_EPOCH × MIN_CHANNEL_FUNDING_USDC` of permanently-parked USDC plus per-sybil setup gas, plus a 7-day rotation cooldown. At default parameters: 50 USDC of permanent capital and a one-week rotation cycle per operator gauge-share inflated.
- **Reuses existing primitives.** secp256k1 / EIP-712 / `SignatureChecker`, the keccak256 Merkle pattern from [ADR 014](014-on-chain-verification.md), the watchtower bond machinery from [ADR 007](007-watchtower.md), and the reputation surface from [ADR 008](008-reputation.md). No new cryptography, no new role, no new contract beyond a `FeeRouter` extension.
- **Cleanly separates payment from gauge.** The 40% base USDC continues to flow same-tx for every settlement regardless of receipt validity — the cashflow invariant operators rely on is preserved.
- **Composable with reputation.** Operators with strong reputation get cheaper gating; new and recently-slashed operators face tighter gating. Receipt validity is a *layer* over reputation, not a replacement.
- **On-chain footprint is bounded.** One Merkle root per settlement, one summary per operator per epoch. Per-receipt storage is challenge-only.
- **Aligns with the [ADR 014](014-on-chain-verification.md) production direction.** The Merkle-batched anchoring chosen here is the same construction that production-path corruption proofs will use; toolchain reuse across ADRs is direct.

### Negative

- **Cold-start UX cost.** Small clients now sign one extra EIP-712 message per voucher. The signing UX is a single popup if the wallet is unlocked and offers `personal_sign`-compatible EIP-712 (Safe, MetaMask, Rainbow, etc.). For headless clients (CLI, automated workflows) the cost is negligible. The cost is real for first-time browser users; client libraries should auto-batch the receipt-signing prompt with the voucher-signing prompt where the wallet permits.
- **Off-chain validation cost.** Watchtowers / receipt attesters must validate every operator's epoch they cover — at the network mean (1k operators × 30k receipts/month) this is ~30M signature verifications per month per fully-covering attester. `SignatureChecker` at ~3k gas off-chain (no transaction) is a few seconds of CPU per 100k signatures; scales linearly. A small attester running per-region is comfortably within commodity-VPS budgets. The economic cost is recovered through the watchtower fee model in [ADR 007 §5](007-watchtower.md#5-fee-model) plus the receipt-fraud-bounty addition — see [ADR 007](007-watchtower.md) for the updated fee structure.
- **Privacy regression from baseline.** Per-receipt client identity is on-chain-observable on challenge. Documented above and forward-referenced to [ADR 017](017-privacy.md). Mitigations exist; they are future work.
- **Per-settlement gas cost grows.** `settleChannel` accepts a new `bytes32 receiptBatchRoot` argument and `FeeRouter.routeSettlement` writes one extra storage slot per settlement. Estimated 5–8k additional gas; on the chosen L2 ([Appendix: L2 Deployment](appendix-l2-deployment.md)) approximately $0.005 per settlement. Validate at L2-cost gate.
- **`commitEpochSummary` is a new keeper-class job.** Per operator, per epoch, somebody must call `commitEpochSummary` after the epoch closes and before the gauge claim window opens. Operators have the strongest incentive (their gauge share depends on it); third parties may be compensated by an operator-paid commit fee. Not a new failure class — it parallels the existing settlement-bot pattern from [ADR 003](003-payments.md) where any address can call `settleChannel`.
- **Bootstrap-window compromise.** The cold-start exception (§3) lowers `MIN_DISTINCT_CLIENTS_PER_EPOCH` to 1 for the first 8 epochs. Wash-trading is harder to detect during the bootstrap window — mitigated by externally-funded operator dominance in early traffic, but not eliminated. Document and monitor; tighten earlier than 8 epochs if bootstrap traffic pattern allows.

### Risks

- **Threshold tuning is empirical.** `MIN_DISTINCT_CLIENTS_PER_EPOCH = 5`, `MIN_CHANNEL_FUNDING_USDC = 10`, and `IDENTITY_COOLDOWN = 7 d` are reasoned defaults. Production data may show that 5 is too sharp a filter for small regional operators, or too lax against sophisticated sybil farms. The parameters are governable; the safety bounds are wide enough for both directions. Monitor and retune in the first 6 months.
- **Funding-source diversity heuristic gameability.** A determined attacker can route sybil funding through public mixers, CEX deposit/withdraw cycles, or third-party DEX swaps to disguise common ancestry. The diversity heuristic catches naive ancestor patterns; sophisticated attackers degrade it. The fundamental cost — funded-channel minimum × distinct-clients-per-epoch × cooldown — remains the binding deterrent regardless of heuristic sophistication.
- **Watchtower coverage must be sufficient.** If no attester is monitoring an operator's epoch, an invalid batch is never challenged, and `claimedDistinctClients` stands. Mitigated by the `WatchtowerEscrow` discovery flow ([ADR 007 §4](007-watchtower.md#4-discovery)) plus the receipt-fraud bounty. Failure mode is more likely "an attacker picks an obscure operator with no watchtower coverage and abuses it" than "the protocol globally fails"; reputation gating (§6) is the secondary defense.
- **Reputation-receipt feedback loop.** Operators with high reputation get easier receipt thresholds. If reputation is gameable upstream, a sophisticated attacker first farms reputation, then exploits the relaxed threshold. Reputation gameability is [ADR 008](008-reputation.md)'s problem, not this ADR's, but the loop is worth flagging — the receipt protocol is no stronger than the reputation it depends on for §6 gating.
- **Privacy-vs-defense tension.** Aggressive privacy upgrades (zero-knowledge identity-diversity proofs in [ADR 017](017-privacy.md)) complicate the funding-source diversity heuristic — the watchtower can no longer trace ancestry if identities are private. Tension to resolve in [ADR 017](017-privacy.md) when that path matures; this ADR ships with cleartext receipts.
- **Non-receipt-aware client wallets.** Clients without receipt-aware wallets produce voucher-only deliveries that are settled and paid normally, but their bytes do not credit gauge eligibility. Operators receiving such traffic carry the cost of pure-base economics for it. Acceptable — receipt-aware client libraries ship in the launch SDK; non-receipt-aware traffic is an asymptote that shrinks as the SDK propagates.
