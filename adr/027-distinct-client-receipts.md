# ADR 027: Distinct-Client Delivery Receipts

**Date:** 2026-04-25
**Status:** Draft
**Required for:** [ADR 026](026-gauge-boost-tokenomics.md) gauge-pool security
**Touches:** [ADR 003](003-payments.md), [ADR 007](007-watchtower.md), [ADR 008](008-reputation.md), [ADR 014](014-on-chain-verification.md)

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) §2 introduces a `FeeRouter` that distributes 40% of every settlement into a weekly **gauge boost pool**. Pool share is computed by the Curve-style formula in [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula):

```
working_bytes_i = min(bytes_i, 0.4 × bytes_i + 0.6 × (ve_i / total_ve) × total_bytes)
boost_share_i   = working_bytes_i / Σ working_bytes
```

The formula is bounded by `bytes_i` in both directions (invariant #3 in the gauge-boost design spec §3) — but **the formula says nothing about whether `bytes_i` itself is honest**. The on-chain input is `claimedBytes` from the final voucher submitted to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` ([ADR 003](003-payments.md) settlement flow). Today that field is supplied by the settling operator and signed by *some* address that opened a payment channel against them. Nothing prevents the operator from running both sides.

**The wash-trading attack** ([ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks); design-spec §6 Risks #3; market-dynamics §3; survival-additions §4):

1. Operator `O` controls a sybil client identity `C` (a fresh EOA or smart account).
2. `C` opens a payment channel to `O` with a USDC deposit.
3. `O` self-routes "delivery" traffic — possibly serving real bytes back to itself, possibly not — and `C` signs cumulative vouchers covering whatever `bytesDelivered` figure `O` wants to inflate.
4. `O` settles the channel. The 40% base flows back to `O`'s own wallet; the inflated `bytesDelivered` enters the gauge counter for the epoch.
5. `O`'s effective cost is the router's non-base skim (≈60%) on `O`'s *own* USDC, which `O` already owned, plus a few cents of L2 gas per settlement. That is roughly a 5–8% net round-trip loss in USDC terms — a deterrent only if TOKEN price is flat and ve-yield is small. At TOKEN price 3–5× genesis, gauge share + ve appreciation more than compensates and the attack is *net profitable*.

Settlement gas (~$0.08) is a per-event tax but does not scale with claimed bytes; an operator can pay one gas fee and inflate by terabytes. The router skim is denominated in the same USDC the attacker controls. **Neither cost is an effective deterrent at the intended TOKEN price levels.**

Defense: byte counters that feed the gauge formula must be attested by **distinct, verifiable client identities** — counterparties the operator does not control. The voucher protocol from [ADR 003](003-payments.md) already proves *bytes were paid for*; this ADR adds a parallel artifact that proves *bytes were paid for by independent counterparties* and gates gauge eligibility on that artifact.

This ADR is forward-referenced from [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks) and [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) as priority-1, **not optional for production launch**.

---

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
| `contentRoot` | `bytes32` | Merkle root over the per-stream content delivered in this voucher window — a keccak256 Merkle tree over 1024-byte chunks identical to the construction in [ADR 014 §2 Production Path](014-on-chain-verification.md#production-path-interactive-keccak256-merkle-proof-future-adr). For sub-MB streams a single leaf hash; for multi-MB streams the root binds every chunk. |
| `clientPubKey` | `address` | Requester's Ethereum address. Identity-diversity gating (§3) is computed across this field. Equals the recovered signer; included in plaintext to make on-chain bucketing trivial for batched verification. |
| `operatorAddress` | `address` | The operator's Ethereum address (`channel.provider`). Receipts are not portable across operators. |
| `epochId` | `uint64` | The `FeeRouter` epoch this receipt is intended to credit — set by the requester at signing time. Receipts whose `epochId` does not match the receipt-batch settlement epoch are rejected at root commitment. |
| `timestamp` | `uint64` | Microsecond timestamp from the requester's clock at signing. Skew bounds match [ADR 014 Evidence Staleness](014-on-chain-verification.md): `MAX_FUTURE_SKEW_US = 60_000_000`, `MAX_RECEIPT_AGE_US = 432_000_000_000` (5 days). |

**EIP-712 domain.** Receipts use a dedicated `FeeRouter` domain separator to prevent cross-contract replay against `StablePaymentChannel` voucher signatures or `SlashJudge` evidence:

```solidity
EIP712Domain({
    name: "deCDN FeeRouter",
    version: "1",
    chainId: <deployment chain>,
    verifyingContract: <FeeRouter address>
})

bytes32 constant DELIVERY_RECEIPT_TYPEHASH = keccak256(
    "DeliveryReceipt("
        "bytes32 channelId,"
        "uint256 voucherNonce,"
        "uint256 bytesClaimed,"
        "bytes32 contentRoot,"
        "address clientPubKey,"
        "address operatorAddress,"
        "uint64 epochId,"
        "uint64 timestamp"
    ")"
);
```

**Invariants enforced when a receipt is verified on challenge (§4, §5):**

1. The signature recovers to `clientPubKey` and that address equals `channel.client` of `channelId`.
2. `bytesClaimed` matches the `bytesDelivered` field of the voucher with nonce `voucherNonce` in `channelId` (cross-checked against the on-chain `claimedBytes` for the channel's final voucher, or against any voucher provided as evidence). Receipts that disagree with the paired voucher are invalid — wash-trading defense relies on the receipt and the voucher being two views of the *same* delivery, signed once each.
3. `operatorAddress` matches `channel.provider`.
4. `epochId` matches the operator's settlement epoch the root was committed in.
5. `timestamp` passes the staleness check and is within the receipt's `epochId`'s settlement window.

**Pairing with vouchers, not replacing them.** [ADR 003](003-payments.md)'s `Voucher` is the on-chain payment instrument and remains the authoritative settlement input for `claimedAmount` and `claimedBytes`. The `DeliveryReceipt` is a separate signature over an overlapping field set (`bytesClaimed` mirrors `bytesDelivered`; `channelId` and `voucherNonce` pin the pairing). A voucher without a matching receipt is fully redeemable for USDC at settlement — only **gauge eligibility for the underlying bytes** depends on the receipt. See §7 for failure semantics.

### 2. Signature scheme

**secp256k1 / EIP-712.** Identical curve and signing scheme as the rest of the deCDN/EVM stack: voucher signatures ([ADR 003 EIP-712 Voucher Signature](003-payments.md)), `slash_sig` ([ADR 014 §1](014-on-chain-verification.md)), `BindNodeId` ([ADR 003 NodeId Binding](003-payments.md)), and `DeliveryReceipt` for corruption challenges ([ADR 014 §2](014-on-chain-verification.md)) — though the latter is a different typedef from the receipt defined here.

**Verifier.** OpenZeppelin `SignatureChecker.isValidSignatureNow` so EOAs (`ecrecover`, ~3k gas) and ERC-1271 smart accounts ([ADR 024](024-account-abstraction.md), ~15k gas for Safe) are both supported with no special-casing in `FeeRouter` or `SlashJudge`. Production clients running smart-account wallets sign receipts via the same path as vouchers.

**Why not Ed25519.** Operators' iroh NodeIds are Ed25519, but receipts are signed by *requester* Ethereum keys — receivers, not operators — and identity diversity is computed in the EVM-address space because the channel deposit is itself in the EVM-address space (USDC ERC-20). The protocol already provides Ed25519-only counterparts for wire-level authentication ([ADR 005](005-protocol.md) `signature` field); this ADR uses the secp256k1 lane that already exists for on-chain evidence ([ADR 014 §1](014-on-chain-verification.md) dual-key slash signatures). Mixing curves would add no security and force on-chain Ed25519 verification we already rejected as too expensive (see [ADR 014 Alternatives Considered](014-on-chain-verification.md#alternatives-considered)).

### 3. Identity-diversity gating

A "distinct client identity" for gauge eligibility purposes is a `clientPubKey` (Ethereum address) satisfying *all* of:

| Criterion | Default | Bounds (governable per [ADR 009](009-governance.md)) |
| --- | ---: | --- |
| **Funded-channel minimum.** Address has at least one channel with the operator (or any operator) where the lifetime aggregate `deposit` ≥ `MIN_CHANNEL_FUNDING_USDC`. | 10 USDC | `[1, 100]` USDC |
| **Funding age.** First channel deposit by this address occurred at least `MIN_CLIENT_AGE` before the receipt's `timestamp`. | 24 h | `[1 h, 30 d]` |
| **Per-channel cooldown.** A client identity is counted at most once per `IDENTITY_COOLDOWN` window per operator, regardless of how many channels or how many receipts it produces. | 7 d | `[1 d, 30 d]` |
| **Funding-source diversity.** The address's USDC balance for the qualifying channel deposit was not received from `operatorAddress`, the operator's known affiliated addresses (registered per [ADR 008](008-reputation.md)), or any other client identity already counted toward this operator's distinct-client set in the current epoch. Enforced off-chain by watchtowers; on-chain witness is a Merkle proof of the funding tx provided on challenge. | — | — |
| **Reputation gate (forward to §6).** If the operator's reputation score is below `LOW_REP_THRESHOLD`, the funded-channel minimum and funding age are tightened (see §6). | — | — |

**Per-epoch eligibility threshold.** An operator is eligible for the gauge pool in epoch `e` only if:

```
distinct_clients_e(operator) ≥ MIN_DISTINCT_CLIENTS_PER_EPOCH
```

Where `distinct_clients_e(operator)` counts the unique `clientPubKey` values appearing in receipts the operator commits to chain for epoch `e`, after applying the cooldown and diversity rules above.

| Parameter | Default | Bounds |
| --- | ---: | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | `[1, 50]` |
| `LOW_REP_THRESHOLD` | network 25th percentile | governable |

**Sizing rationale.** A single sybil costs the attacker the funded-channel minimum (10 USDC), the funding-age delay (24 h), and the cooldown (7 d). Five distinct sybils per week is 50 USDC of capital permanently parked in payment-channel deposits per operator per week, plus on-chain Tx fees to fund and rotate. Larger thresholds harden against sybils linearly at the cost of cold-start UX; 5 is a reasoned default for v1, sized for tens-of-nodes-scale. Production data should retune this — the parameter is governable and the safety bounds are wide.

**Cold-start exception.** During the first 8 epochs of mainnet (governance-set `gaugeBootstrapEpochs`, default 8, max 26), `MIN_DISTINCT_CLIENTS_PER_EPOCH` is reduced to `1`. The intent is to let the gauge pool distribute meaningfully even when the network has fewer aggregate clients than the steady-state threshold; the wash-trading risk is bounded during this window because pre-seed USDC ([ADR 026 §10](026-gauge-boost-tokenomics.md#10-bootstrap-mechanism--pre-seed-usdc)) and the Protocol-Owned-Operators program (forward-referenced from [ADR 026](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) as ADR 030) dominate early-epoch bytes anyway. Bootstrap-window receipts are still validated; only the *threshold* is lowered.

**Optional reputation-attested registry.** The protocol does not require a centralized identity registry. However, a `ClientIdentityRegistry` view contract may be supplied by reputation attesters ([ADR 008](008-reputation.md)) so well-known stable client identities (e.g., operators of large origin-backed services purchasing CDN bandwidth) can be vouched for and bypass the funding-age gate. The registry is opt-in and watchtower-validated; clients without a registry entry use the default funded-channel-and-cooldown rules unchanged.

### 4. On-chain anchoring (Merkle-batched)

Per-receipt on-chain storage is uneconomical at scale. A 1 Gbps node produces ~30k receipts/month at the default 1 MB voucher cadence ([ADR 003 Voucher Interval Negotiation](003-payments.md)); 1,000 such operators are 30M receipts/month. Storing one log entry per receipt is comparable in cost to settling all the channels themselves.

The protocol mirrors the **keccak256 Merkle-batch pattern** from [ADR 014 §2 Production Path](014-on-chain-verification.md#production-path-interactive-keccak256-merkle-proof-future-adr): receipts are committed via root, individual receipts surface only on challenge.

**Per-settlement commitment.** `StablePaymentChannel.settleChannel` is extended to accept a `bytes32 receiptBatchRoot` argument, forwarded to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)`. A zero root signals "this settlement contributes nothing to gauge eligibility" — see §7.

**Batch shape.** A `ReceiptBatch` for a single operator within one settlement is a keccak256 Merkle tree over the leaves:

```solidity
leaf_i = keccak256(
    abi.encode(
        DELIVERY_RECEIPT_TYPEHASH,
        receipt_i.channelId,
        receipt_i.voucherNonce,
        receipt_i.bytesClaimed,
        receipt_i.contentRoot,
        receipt_i.clientPubKey,
        receipt_i.operatorAddress,
        receipt_i.epochId,
        receipt_i.timestamp,
        keccak256(receipt_i.signature)   // canonicalises 65-byte EOA / arbitrary ERC-1271
    )
)
```

Tree construction matches the [ADR 014](014-on-chain-verification.md) pattern: `keccak256(left || right)` internal nodes; left-padded with zero hashes for non-power-of-2 leaf counts; index-prefixed leaves prevent second-preimage attacks. A 30k-receipt monthly batch produces a 15-deep tree (~480 bytes per challenge proof).

**Per-epoch summary.** At epoch rollover, `FeeRouter` aggregates all `receiptBatchRoot` commitments observed for an operator during the epoch into a single per-epoch `EpochReceiptSummary`:

| Field | Type | Description |
| --- | --- | --- |
| `operator` | `address` | The operator the summary is for. |
| `epochId` | `uint64` | The epoch this summary covers. |
| `claimedBytes` | `uint256` | Sum of `claimedBytes` from all settlements in `epochId`; this is the `bytes_i` plugged into the gauge formula. Authoritative input. |
| `claimedDistinctClients` | `uint32` | Operator-asserted count of unique `clientPubKey` across the receipt batches. Subject to challenge. |
| `aggregateRoot` | `bytes32` | Merkle root of the per-settlement `receiptBatchRoot`s for `epochId` (a tree of trees). One inclusion proof reaches any individual receipt in two hops. |

The `EpochReceiptSummary` is committed once per operator per epoch by the operator (or any third party) calling `FeeRouter.commitEpochSummary(operator, epochId, summary)` after the epoch closes and before the gauge claim window opens (claim-window timing per [ADR 026 §2 Epoch mechanics](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)). The contract verifies that `claimedBytes` matches the on-chain accumulated counter for the epoch and that `aggregateRoot` is consistent with the per-settlement roots already stored. `claimedDistinctClients` is taken at face value pending the challenge window.

**Challenge window.** A 7-day window (matches the gauge-claim-window opening) during which any address may submit a `ChallengeReceiptSummary` claim against an operator's summary. Watchtowers (§5) typically initiate. A successful challenge zeros `claimedDistinctClients` for the epoch — gauge eligibility is forfeited in line with §7. An unsuccessful challenge forfeits the challenger's bond per [ADR 014 Bond Handling](014-on-chain-verification.md).

### 5. Watchtower role (forward to [ADR 007](007-watchtower.md))

Receipt validation is added as a new **watchtower-class duty** on top of the channel-dispute monitoring already defined in [ADR 007](007-watchtower.md). Watchtowers serving the receipt-validation role are referred to as **receipt attesters** in this ADR; the term is interchangeable with "receipt-validating watchtower".

A receipt attester subscribed to an operator's epoch performs:

1. **Signature validity.** For each receipt in the batch, recompute the EIP-712 digest, verify the signature with `SignatureChecker`, and verify all five invariants in §1.
2. **Identity diversity over recent epochs.** Apply the §3 rules — funded-channel minimum, funding-age, per-operator cooldown, funding-source diversity — and compute the actual distinct-client count. Compare with `claimedDistinctClients` in the operator's `EpochReceiptSummary`.
3. **Cross-checks against suspicious clustering.** Heuristics including:
   - All receipts in an epoch arriving from a contiguous block of EOA addresses ("address generator pattern").
   - Receipts whose `clientPubKey` was funded from a small set of common ancestors.
   - Receipts whose `bytesClaimed` distribution is anomalous relative to the network mean (e.g., uniformly maxed-out at the negotiated voucher interval, with no jitter).
   - Receipts whose `timestamp` clusters tightly within tens of seconds, characteristic of automated wash-routing rather than organic traffic.

   Exact heuristic catalogue is implementation-defined and can evolve; what is fixed is the **interface**: a heuristic flag is sufficient grounds to open a `ChallengeReceiptSummary`, and the on-chain dispute resolves on cryptographic evidence (signature validity, channel-funding ancestry traces) rather than on the heuristic itself. Heuristics are a *prioritisation* signal for the attester, never a slashing input.

**Bond model.** Receipt-fraud challenges use the existing watchtower bond mechanism in [ADR 007](007-watchtower.md). Successful challenges award the bond plus a configurable receipt-fraud reward to the challenger; unsuccessful challenges forfeit the bond per [ADR 014 Bond Handling](014-on-chain-verification.md). Receipt attesters are not a new on-chain role — they are watchtowers running an additional module against the same `WatchtowerEscrow` contract.

**Separation from the operator.** Receipt attesters MUST NOT be operated by the operator they validate. The operator's affiliated-address registry ([ADR 008](008-reputation.md)) is queried; an attester address that overlaps with `operatorAddress`'s known affiliates is ineligible to settle challenges against that operator. This is enforced on-chain in `WatchtowerEscrow` at challenge submission time, mirroring the watchtower-collusion defense already in [ADR 007](007-watchtower.md).

### 6. Reputation gating (forward to [ADR 008](008-reputation.md))

Receipts function without reputation. Reputation hardens them.

Operators below `LOW_REP_THRESHOLD` (default network 25th percentile, governable) face stricter receipt requirements:

| Parameter | Above threshold | Below threshold |
| --- | --- | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | 10 |
| `MIN_CHANNEL_FUNDING_USDC` | 10 USDC | 25 USDC |
| `MIN_CLIENT_AGE` | 24 h | 72 h |
| Cold-start bootstrap exception | applies | does not apply |

The intent: a high-reputation operator with stable historical traffic has a high cost of false-flagging by an attacker (loss of historical reputation > epoch-level gauge gain), so a thinner identity-diversity threshold is acceptable. A new or recently-slashed operator faces tighter gates, raising the capital cost of wash-trading proportional to the trust deficit.

**Integration is non-blocking.** The receipt protocol does not require reputation to function — `LOW_REP_THRESHOLD` may be configured to "always above" in early production, in which case all operators get the relaxed thresholds. The protocol upgrades when reputation is online, not when it ships.

**Forward-compatible.** [ADR 008](008-reputation.md) is updated separately to expose `reputationOf(operator)` as an on-chain view that `FeeRouter.commitEpochSummary` reads at commit time.

### 7. Failure semantics

The receipt protocol fails open for **payment** and fails closed for **gauge eligibility**. Distinguishing these two is the central design choice and the section operators most need to read.

| Scenario | Operator's 40% base USDC payout | Gauge-pool eligibility for the epoch | Slashing |
| --- | --- | --- | --- |
| Settlement with valid voucher and valid receipt batch (the happy path). | **Paid same-tx** at settlement. | Counts toward `claimedBytes` and `claimedDistinctClients`. | None. |
| Settlement with valid voucher but **`receiptBatchRoot == 0`** (operator opted out, or has no signed receipts to commit). | **Paid same-tx** at settlement. | These bytes contribute `0` to `claimedBytes` for gauge purposes; settlement bytes still count for direct-payout proportional accounting. | None. |
| Settlement with valid voucher and a `receiptBatchRoot` whose batch contains receipts with invalid signatures or invariant violations. | **Paid same-tx** at settlement. | On a successful challenge, the operator's `claimedDistinctClients` is **zeroed** for the epoch; if zeroing drives the count below `MIN_DISTINCT_CLIENTS_PER_EPOCH`, the operator's gauge share for that epoch is zero. The 40% base for the disputed settlement is not clawed back. | Repeated invalid-batch behaviour escalates per the [ADR 004](004-tokenomics.md) lifetime offence counter, on the existing 5/15/50% slashing schedule. A single bad batch is not a slash — only a *successful challenge with cryptographic evidence of forgery* (e.g., signature recovery yielding an address that never opened a channel) is. |
| Operator delivers bytes but gauge eligibility is forfeited for any reason in this table (zeroed batch, missing summary, sub-threshold distinct clients). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None unless triggered separately. |
| Operator below `MIN_DISTINCT_CLIENTS_PER_EPOCH` after honest validation (low traffic, regional cold-start). | **Paid same-tx** at settlement. | Zero gauge share for the epoch. | None. |

**The cashflow invariant.** Settlement always pays the operator's 40% base ([ADR 026 §2 Same-transaction guarantees](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553); [ADR 026 §3 Properties](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)). Receipt validity gates only the **40% gauge pool share**, never the **40% base share**. An operator running honest delivery with an immature client base receives full base USDC and zero gauge — they are commodity operators in the [ADR 026 §7 Case A](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) sense. This is the designed incentive pressure, not a punishment.

**The slashing invariant.** Receipt protocol violations are **not by themselves slashable**. A successful Merkle-anchored challenge zeros gauge eligibility for the *current epoch only*. Slashing escalates only via the existing [ADR 004](004-tokenomics.md) schedule and only when watchtower evidence proves a deliberate forgery (e.g., a recovered signer that does not correspond to any channel `client` that ever existed, indicating an outright fabricated signature). Routine receipt invalidation — wrong root committed, late batch commit, signature on a closed channel — is corrected by zeroing the epoch and not by slashing.

### 8. Privacy considerations (forward to [ADR 017](017-privacy.md))

Per-receipt client identity is **observable on-chain when challenged**. The Merkle root committed at settlement does not reveal individual client addresses, but the inclusion proof produced during a challenge does — `clientPubKey` is one of the leaf fields. Settlement-time disclosure is minimal (one root); challenge-time disclosure is partial (only the receipts the challenger needed to surface to win). However, challenges are public — anyone watching the L2 chain sees which client addresses funded which operators on which dates.

This is a real privacy regression from the [ADR 003](003-payments.md) baseline, where channel deposits are on-chain (identifying the *funder*) but per-stream byte counts are not. Adding per-stream attestation signed by the funder makes the funder's per-stream activity legible whenever a challenge surfaces them.

**Mitigations possible within this ADR (do *not* change the protocol):**

- Receipts are batched per epoch — challenges reveal the batch's leaves only, not all of an operator's history.
- The `contentRoot` field is a Merkle root over chunks, not a content hash; it does not reveal which blob the client requested.
- Operators are expected to store receipts off-chain and only surface them on challenge; they are not gossiped or published.

**Mitigations requiring future work, forward-referenced to [ADR 017](017-privacy.md):**

- **Zero-knowledge identity-diversity proofs.** A SNARK / STARK construction proving "this batch contains at least N receipts signed by N distinct addresses, all meeting the §3 gating rules, against operator O" without revealing individual `clientPubKey`s. Would replace the cleartext leaves with a proof and a public "distinct count". Major engineering effort; defer.
- **Stealth-address client identities.** Each per-channel client identity could be a fresh stealth address derived from a master client key. Sidesteps trivial address-graph clustering at the cost of a more complex client wallet UX. Defer.
- **Mixer-funded sybils as anti-defense.** A privacy-improving mixer would also degrade funding-source diversity heuristics (§5). Tension to be resolved in [ADR 017](017-privacy.md).

This ADR ships the cleartext-receipt protocol. Privacy upgrades are a future-work track and **not** a launch blocker — wash-trading defense is the v1 priority, and shipping receipts late is a worse outcome than shipping them with a known privacy regression that [ADR 017](017-privacy.md) addresses on its own timeline.

### 9. Implementation sequencing and v1 prerequisite

**The protocol can technically launch without distinct-client receipts.** The voucher path in [ADR 003](003-payments.md) is independent of receipts; settlements pass `bytesDelivered` to `FeeRouter` whether a receipt batch is committed or not. Operators receive their 40% base USDC. The 40% gauge pool, the 7% delegator pool, the 5% burn, the 5% treasury, and the 3% safety reserve all accumulate normally.

**But the gauge pool MUST be paused until receipts ship.** From [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks), wash-trading at TOKEN price 3–5× genesis is net-profitable without the receipt gate. Distributing a 40% pool to byte-counters whose inputs are unattestable is an open invitation. Therefore:

> **v1 launch prerequisite.** Mainnet may launch the `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and direct-base-payout flows without distinct-client receipts. **The 40% gauge boost pool MUST NOT pay out until distinct-client receipts are live and verified at scale.** Until that moment, the 40% gauge bucket either accumulates in `FeeRouter` (eventually claimable retroactively when receipts ship) or is governance-routed to treasury / SafetyReserve as a temporary measure. The operator-aligned share remains 80% of revenue ([ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)); the gauge half of that share is escrowed, not denied.

This is consistent with [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) — ADR 027 is listed there as "priority-1; not optional for production launch". The wording above pins down what "not optional" means concretely.

**Sequencing within the receipt rollout itself:**

1. Receipt format, signing, EIP-712 domain finalised. Client-side and operator-side libraries shipped.
2. `FeeRouter.commitEpochSummary` and `commitSettlementBatch` paths deployed; receipts collected but no enforcement (parallel-run).
3. Watchtower receipt-validation module shipped ([ADR 007](007-watchtower.md) updated). Heuristic library populated.
4. Reputation integration ([ADR 008](008-reputation.md)) shipped — `reputationOf(operator)` view live.
5. **Gauge gating turned on.** `MIN_DISTINCT_CLIENTS_PER_EPOCH` becomes active. The 40% gauge pool begins paying out per the gated formula. v1 mainnet-readiness reached.

Steps 1–4 may run in parallel-with-mainnet for one or more epochs of testing; step 5 is the cutover.

---

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
- **Per-settlement gas cost grows.** `settleChannel` accepts a new `bytes32 receiptBatchRoot` argument and `FeeRouter.routeSettlement` writes one extra storage slot per settlement. Estimated 5–8k additional gas; on the chosen L2 ([ADR 021](021-l2-chain-selection.md)) approximately $0.005 per settlement. Validate at L2-cost gate.
- **`commitEpochSummary` is a new keeper-class job.** Per operator, per epoch, somebody must call `commitEpochSummary` after the epoch closes and before the gauge claim window opens. Operators have the strongest incentive (their gauge share depends on it); third parties may be compensated by an operator-paid commit fee. Not a new failure class — it parallels the existing settlement-bot pattern from [ADR 003](003-payments.md) where any address can call `settleChannel`.
- **Bootstrap-window compromise.** The cold-start exception (§3) lowers `MIN_DISTINCT_CLIENTS_PER_EPOCH` to 1 for the first 8 epochs. Wash-trading is harder to detect during the bootstrap window — mitigated by pre-seed dominance in early traffic, but not eliminated. Document and monitor; tighten earlier than 8 epochs if bootstrap traffic pattern allows.

### Risks

- **Threshold tuning is empirical.** `MIN_DISTINCT_CLIENTS_PER_EPOCH = 5`, `MIN_CHANNEL_FUNDING_USDC = 10`, and `IDENTITY_COOLDOWN = 7 d` are reasoned defaults. Production data may show that 5 is too sharp a filter for small regional operators, or too lax against sophisticated sybil farms. The parameters are governable; the safety bounds are wide enough for both directions. Monitor and retune in the first 6 months.
- **Funding-source diversity heuristic gameability.** A determined attacker can route sybil funding through public mixers, CEX deposit/withdraw cycles, or third-party DEX swaps to disguise common ancestry. The diversity heuristic catches naive ancestor patterns; sophisticated attackers degrade it. The fundamental cost — funded-channel minimum × distinct-clients-per-epoch × cooldown — remains the binding deterrent regardless of heuristic sophistication.
- **Watchtower coverage must be sufficient.** If no attester is monitoring an operator's epoch, an invalid batch is never challenged, and `claimedDistinctClients` stands. Mitigated by the `WatchtowerEscrow` discovery flow ([ADR 007 §4](007-watchtower.md#4-discovery)) plus the receipt-fraud bounty. Failure mode is more likely "an attacker picks an obscure operator with no watchtower coverage and abuses it" than "the protocol globally fails"; reputation gating (§6) is the secondary defense.
- **Reputation-receipt feedback loop.** Operators with high reputation get easier receipt thresholds. If reputation is gameable upstream, a sophisticated attacker first farms reputation, then exploits the relaxed threshold. Reputation gameability is [ADR 008](008-reputation.md)'s problem, not this ADR's, but the loop is worth flagging — the receipt protocol is no stronger than the reputation it depends on for §6 gating.
- **Privacy-vs-defense tension.** Aggressive privacy upgrades (zero-knowledge identity-diversity proofs in [ADR 017](017-privacy.md)) complicate the funding-source diversity heuristic — the watchtower can no longer trace ancestry if identities are private. Tension to resolve in [ADR 017](017-privacy.md) when that path matures; v1 ships with cleartext receipts.
- **Non-receipt-aware client wallets.** Clients running pre-ADR-027 wallets do not produce receipts. Their voucher-only deliveries are settled and paid for normally, but their bytes do not credit gauge eligibility. Operators receiving traffic from non-receipt-aware clients carry the cost of pure-base economics for that traffic. Acceptable v1 outcome — receipt-aware client libraries are part of the v1 SDK; non-receipt-aware traffic is an asymptote that shrinks as the SDK propagates.

---

## ADRs to update on acceptance

| ADR | What changes |
| --- | --- |
| [ADR 003 — Payments](003-payments.md) | `StablePaymentChannel.settleChannel` accepts `bytes32 receiptBatchRoot`; forwards to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)`. The `Voucher` typedef is unchanged; receipts are a parallel artifact. Voucher protocol description updated to document the *paired-with-receipt* expectation in v1+. |
| [ADR 007 — Watchtower](007-watchtower.md) | New "receipt-attester" role layered on the existing watchtower contract surface. New duty: signature validation, identity-diversity rules, suspicious-clustering heuristics. New on-chain entry: `WatchtowerEscrow.challengeReceiptSummary(operator, epochId, evidence)`. Bond model unchanged; receipt-fraud bounty added to the fee structure. |
| [ADR 008 — Reputation](008-reputation.md) | `reputationOf(operator)` view exposed for `FeeRouter` consumption. Affiliated-address registry surface formalised (already implicit). Tighter receipt thresholds for low-reputation operators documented as a reputation consequence. |
| [ADR 014 — On-chain verification](014-on-chain-verification.md) | The keccak256 Merkle-batch pattern this ADR uses is the same construction documented as the production path for corruption proofs. Cross-link added so toolchain reuse is explicit; no contract changes to `SlashJudge`. |
| [ADR 017 — Privacy](017-privacy.md) | New section flagging per-receipt client-identity disclosure on challenge; zero-knowledge identity-diversity proofs and stealth-address client identities listed as future-work. Tension with funding-source diversity heuristic noted. |
| [ADR 026 — Tokenomics v3](026-gauge-boost-tokenomics.md) | The forward reference to ADR 027 in §Risks and §Forward references is fulfilled. §9 Implementation sequencing's v1 prerequisite ("gauge pool MUST NOT pay out until receipts ship") is the binding rule; ADR 026 already contains the "Strongly recommended for production launch; not optional" wording, which carries through unchanged. |

---
