# ADR 027: Distinct-Client Diversity Gating

**Date:** 2026-04-25
**Status:** Draft
**Required for:** [ADR 026](026-gauge-boost-tokenomics.md) gauge-pool security
**Touches:** [ADR 003](003-payments.md), [ADR 008](008-reputation.md), [ADR 014](014-on-chain-verification.md), [ADR 026](026-gauge-boost-tokenomics.md)

## Context

[ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) defines the gauge-pool share as a function of `bytes_i` (verified bytes per operator) and `ve_i / total_ve` (the operator's ve-share). The formula bounds the *output* but says nothing about whether the *input* `bytes_i` is honest. The on-chain `claimedBytes` reaching `FeeRouter.routeSettlement` ([ADR 003](003-payments.md)) is signed by *some* address that opened a payment channel — nothing today prevents the operator from running both sides.

### The wash-trading attack

([ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks); design-spec §6.3): an operator opens a channel from a sybil client identity to themselves, self-routes traffic, and signs vouchers inflating `bytesDelivered`. Effective cost is the ~60% router skim on the operator's *own* USDC plus a few cents of L2 gas — roughly a 5–8% net loss in USDC terms, more than offset by gauge-share + ve-appreciation gains at TOKEN price 3–5× genesis. Settlement gas does not scale with claimed bytes. **Neither cost is a deterrent at the intended TOKEN price levels.**

Two layered defenses close this surface:

1. **Distinct-client diversity gate.** Gauge eligibility for an epoch requires the operator's settled vouchers in that epoch to come from at least `MIN_DISTINCT_CLIENTS_PER_EPOCH` distinct `channel.client` addresses, each satisfying a funded-channel minimum and funding-age check. Sybils require capital + waiting period.
2. **Per-operator gauge-share cap.** Even an attacker who fabricates enough Sybils to clear the diversity gate cannot capture more than `MAX_GAUGE_SHARE_PER_OPERATOR` (default 5%) of the gauge bucket — see [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap). Bounds the wash-trading payoff regardless of diversity-bypass sophistication.

This ADR specifies the diversity gate. The cap lives in ADR 026 §3 alongside the gauge formula it modifies.

This ADR is forward-referenced from [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks) and [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) as priority-1, **not optional for production launch**.

## Decision

The voucher itself is the diversity-gating primitive. `Voucher` is signed by `channel.client` (the channel funder, set at `openChannel`), commits to `bytesDelivered`, and is settled on-chain via `StablePaymentChannel.settleChannel` → `FeeRouter.routeSettlement`. Every settlement reveals exactly the data needed for diversity counting: who funded the channel, how many bytes the operator delivered, which epoch the bytes credit to.

`FeeRouter` accumulates per-(operator, epoch) state at each `routeSettlement` call: distinct funder count and total bytes. At gauge claim time, the diversity threshold is checked against the on-chain count directly. No separate signed receipt artifact, no operator-asserted summary, no fraud-challenge mechanism.

### 1. FeeRouter per-epoch bookkeeping

The contract maintains three mappings, each keyed by `(operator, epochId)`:

```solidity
// Distinct funder count for the (operator, epoch) — feeds the diversity gate.
mapping(address operator => mapping(uint64 epoch => uint256)) public distinctClientCount;

// Idempotency map: has this client been counted yet for this (operator, epoch)?
mapping(address operator => mapping(uint64 epoch =>
        mapping(address client => bool))) internal _seenClient;

// Total bytes delivered by this operator in this epoch — feeds bytes_i in
// ADR 026 §3 gauge formula.
mapping(address operator => mapping(uint64 epoch => uint256)) public bytesPerEpoch;
```

`routeSettlement` performs the bookkeeping inline:

```solidity
function routeSettlement(
    bytes32 channelId,
    Voucher calldata voucher,
    bytes calldata clientSignature
) external {
    // ... existing voucher verification, USDC routing per ADR 026 §2 ...

    Channel storage ch = channels[channelId];
    address operator = ch.provider;
    address client   = ch.client;
    uint64  epoch    = epochOf(voucher);   // see §2 below

    if (!_seenClient[operator][epoch][client]) {
        _seenClient[operator][epoch][client] = true;
        distinctClientCount[operator][epoch] += 1;
    }
    bytesPerEpoch[operator][epoch] += voucher.bytesDelivered;
}
```

Per-settlement gas overhead: ~25k (2 SLOADs + 1–2 SSTOREs depending on cold/warm and first-time-seen). Negligible against the ~120–180k existing `settleChannel` baseline.

### 2. Epoch attribution

Each voucher carries an explicit `epochId: uint64` field (extending [ADR 003 Voucher](003-payments.md) by one field; this is a Tier 3 schema break per [ADR 013](013-schema-evolution.md) bundled with the launch sequencing in §6 below). The client sets `epochId` at signing time. `FeeRouter` validates:

- `epochId` is a current or recent epoch (within `MAX_EPOCH_LAG` of `currentEpoch()`, default 4 epochs).
- `epochId` does not lie in the future.

A voucher attributed to an old epoch past the lag bound reverts. This bounds storage growth (only recent epochs accept new settlements) and prevents manipulation of long-closed gauge accounting.

For long-lived channels spanning multiple epochs, the client signs separate per-epoch vouchers (incrementing `voucherNonce` across them per [ADR 003 Voucher Nonce Convention](003-payments.md)). The operator submits each at the relevant `settleChannel` call cadence — typically once per epoch boundary per active channel.

### 3. Identity-diversity gating

A "distinct client identity" for gauge eligibility is a `channel.client` address satisfying *all* of:

| Criterion | Default | Bounds (governable per [ADR 009](009-governance.md)) |
| --- | ---: | --- |
| **Funded-channel minimum.** Address has at least one channel where the lifetime aggregate `deposit` ≥ `MIN_CHANNEL_FUNDING_USDC`. Tracked via the per-client cumulative-deposit counter `StablePaymentChannel.lifetimeDepositOf(client) view returns (uint256)` ([ADR 003](003-payments.md)) — monotonic, incremented by funded amount on every `openChannel` and `topUp`, never decreased. Adds one SSTORE per funding event. | 10 USDC | `[1, 100]` USDC |
| **Funding age.** First channel deposit by this address occurred at least `MIN_CLIENT_AGE` before the voucher's `epochId` settlement window. | 24 h | `[1 h, 30 d]` |
| **Per-operator cooldown.** A client identity is counted at most once per `IDENTITY_COOLDOWN` window per operator, regardless of how many channels or vouchers it produces. Enforced by the `_seenClient` mapping above scoped per epoch (default `IDENTITY_COOLDOWN == EPOCH_LENGTH = 7 d`); for tighter or looser cooldowns, governance retunes within the bounds. | 7 d | `[1 d, 30 d]` |
| **Reputation gate (forward to §5).** If the operator's reputation is below `medium_rep_threshold`, the funded-channel minimum and funding age tighten — see §5. | — | — |

The first three criteria are **fully on-chain enforceable** at `routeSettlement` time. The funded-channel minimum and funding age are looked up against `StablePaymentChannel`; the cooldown is the `_seenClient` map. No off-chain heuristics are load-bearing.

#### Per-epoch eligibility threshold

An operator is eligible for the gauge pool in epoch `e` only if:

```
distinctClientCount[operator][e] >= MIN_DISTINCT_CLIENTS_PER_EPOCH
```

| Parameter | Default | Bounds |
| --- | ---: | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | `[1, 50]` |

#### Sizing rationale

A single Sybil costs the attacker the funded-channel minimum (10 USDC, locked for the dispute window after channel close), the funding-age delay (24 h), and the operator-scoped cooldown (one identity per operator per epoch). Five distinct Sybils per operator per epoch is **50 USDC of capital locked for ≥9 days** plus on-chain Tx fees to fund and rotate.

This is a **soft floor**, not a hard one: a determined attacker can absorb 50 USDC × 9 days ≈ $0.12/epoch in opportunity cost. The hard ceiling on attack profitability is the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) (default 5%) — even a successful Sybil bypass cannot capture more than 5% of the gauge bucket per operator. The two defenses compose: the diversity gate raises the per-Sybil work; the cap bounds the per-operator payoff.

#### Cold-start exception

During the first 8 epochs of mainnet (governance-set `gaugeBootstrapEpochs`, default 8, max 26), `MIN_DISTINCT_CLIENTS_PER_EPOCH` is reduced to `1`. The intent is to let the gauge pool distribute meaningfully even when the network has fewer aggregate clients than the steady-state threshold; the per-operator cap remains active throughout, bounding wash-trading risk during the bootstrap window.

### 4. Gauge-claim integration

At gauge claim time (per [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)):

```solidity
require(distinctClientCount[operator][epoch] >= effectiveThreshold(operator, epoch),
        "below distinct-client threshold");
uint256 bytes_i = bytesPerEpoch[operator][epoch];
// Apply ADR 026 §3 working_bytes_i formula and per-operator cap.
```

Where `effectiveThreshold(operator, epoch)` returns the §3 threshold, adjusted for the operator's reputation tier (§5) and the cold-start exception.

There is no per-epoch "summary commitment" the operator makes — the values are read directly from contract state populated by `routeSettlement`. There is nothing the operator can falsely claim and therefore nothing to challenge.

### 5. Reputation gating (forward to [ADR 008](008-reputation.md))

Operators below `medium_rep_threshold` (default **0.50**, governable within `[0.30, 0.70]` per [ADR 008 §12.1](008-reputation.md#121-receipt-tiers)) face stricter gating:

| Parameter | Above threshold | Below threshold |
| --- | --- | --- |
| `MIN_DISTINCT_CLIENTS_PER_EPOCH` | 5 | 10 |
| `MIN_CHANNEL_FUNDING_USDC` | 10 USDC | 25 USDC |
| `MIN_CLIENT_AGE` | 24 h | 72 h |
| Cold-start bootstrap exception | applies | does not apply |

Rationale: a high-reputation operator with stable historical traffic has a higher cost of false-flagging by an attacker (loss of historical reputation > epoch-level gauge gain), so a thinner identity-diversity threshold is acceptable. A new or recently-slashed operator faces tighter gates, raising the capital cost of wash-trading proportional to the trust deficit.

**Integration is non-blocking.** `medium_rep_threshold` may be configured to its lower bound (0.30) in early production so almost all operators clear it and get the relaxed thresholds.

[ADR 008](008-reputation.md) is updated separately to expose `reputationOf(operator)` as an on-chain view that `FeeRouter` reads at gauge-claim time.

### 6. Implementation sequencing and launch prerequisite

**The protocol can technically launch without diversity gating.** The voucher path in [ADR 003](003-payments.md) is independent of `distinctClientCount` enforcement; settlements record the bookkeeping, but if the gate is not yet enforced at gauge-claim time, the gauge bucket can technically distribute on raw `bytesPerEpoch` alone. Operators receive their 40% base USDC in either case.

**But the gauge pool MUST be paused until diversity gating ships.** From [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks), wash-trading at TOKEN price 3–5× genesis is net-profitable without the gate. Therefore:

> **Launch prerequisite.** The `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and direct-base-payout flows can be deployed without the diversity gate active. **The 40% gauge boost pool MUST NOT pay out until diversity gating is live and the per-operator cap from [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) is enforced.** Until that moment, the 40% gauge bucket accumulates in `FeeRouter.preLaunchGaugeAccumulator[epoch]` per [ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation). Cutover is the one-shot `enableGauge()` governance setter; pre-launch epochs become claimable retroactively against the ve-snapshots already taken at each historical epoch boundary, with the 26-epoch claim window starting at `gaugeLaunchEpoch`. The operator-aligned share remains 80% of revenue ([ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)); the gauge half of that share is escrowed, not denied.

This is consistent with [ADR 026 §Forward references](026-gauge-boost-tokenomics.md#forward-references-follow-up-adrs) — ADR 027 is listed there as "priority-1; not optional for production launch".

#### Sequencing

`FeeRouter` per-(operator, epoch) bookkeeping deployed in parallel-run mode (gauge bucket escrowing into `preLaunchGaugeAccumulator[epoch]`, no enforcement) → reputation integration → governance call to `enableGauge()` → cutover (gauge gating on, `MIN_DISTINCT_CLIENTS_PER_EPOCH` and per-operator cap active, pre-launch epochs become retroactively claimable). The `enableGauge()` invocation is launch readiness.

### 7. Privacy considerations

Per-settlement client identity is on-chain by virtue of `channel.client` being a public field on `StablePaymentChannel` — this is no different from the [ADR 003](003-payments.md) baseline. The diversity gate adds **no new on-chain identity disclosure** beyond what channel settlement already reveals: `channelId`, `channel.client`, `channel.provider`, settled bytes, and settled amount.

Future privacy work in [ADR 017](017-privacy.md) — stealth-address client identities, ZK identity-diversity proofs — can replace the cleartext `channel.client` field uniformly across `StablePaymentChannel`, `FeeRouter`, and any consumer; the diversity gate inherits whatever privacy posture the channel layer adopts. This is not launch-blocking.

## Consequences

### Positive

- **Closes the wash-trading attack surface flagged in [ADR 026 §Risks](026-gauge-boost-tokenomics.md#risks).** Two-layer defense: capital-funded Sybil floor (this ADR) + per-operator gauge-share cap ([ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap)). Sophisticated attackers who absorb the diversity floor still face a 5% bucket cap on their wash-trading payoff per operator.
- **Reuses existing primitives only.** No new EIP-712 typedef, no new on-chain role, no new SlashJudge entry point, no new offense type, no MMR machinery, no Merkle-proof verification surface. The diversity gate is two storage maps + an inline check inside `routeSettlement`.
- **Cleanly separates payment from gauge.** The 40% base USDC continues to flow same-tx for every settlement regardless of diversity threshold satisfaction — the cashflow invariant operators rely on is preserved.
- **Composable with reputation.** Operators with strong reputation get cheaper gating; new and recently-slashed operators face tighter gating. Diversity gating is a *layer* over reputation, not a replacement.
- **No privacy regression from baseline.** No per-receipt `clientPubKey` exposure on challenge. The on-chain identity surface is exactly what `StablePaymentChannel` already exposes.
- **No fraud-challenge surface.** The diversity count is contract-computed from settled vouchers; the operator never asserts a summary that could be a lie. Nothing to forge, nothing to challenge.

### Negative

- **Settlement-time epoch attribution.** Bytes are credited to the voucher's `epochId` at `settleChannel` time. For long-lived channels (90-day max), if the operator delays settlement, gauge-claim is also delayed. Mitigations: short voucher cadence (already exists per [ADR 003](003-payments.md)), operator incentive to settle at epoch boundaries to claim gauge promptly. For short-lived or per-epoch channels, this is a non-issue.
- **`Voucher` schema break (Tier 3).** Adds `epochId: uint64` field — coordinated rollout with `enableGauge()` per §6.
- **`FeeRouter` per-settlement gas overhead ~+25k.** 2 SLOADs and 1–2 SSTOREs added to `routeSettlement`. Negligible at L2 fee levels (~$0.001–0.002 per settlement).
- **Bootstrap-window exposure.** During the cold-start window (first 8 epochs), `MIN_DISTINCT_CLIENTS_PER_EPOCH` drops to 1; the per-operator gauge cap remains the binding deterrent. Document and monitor; tighten earlier than 8 epochs if the bootstrap traffic pattern allows.
- **No per-receipt fraud-challenge surface.** Sophisticated wash-trading that bypasses the funded-channel + cooldown + per-operator-cap defenses cannot be caught by any external challenger. The layered defenses are calibrated such that what such a surface would uniquely catch (mid-spectrum sophistication that bypasses on-chain gates but fails ancestry heuristics) is a small slice of the threat surface, and the audit/operational cost of building it would be disproportionate to that slice.

### Risks

- **Threshold tuning is empirical.** `MIN_DISTINCT_CLIENTS_PER_EPOCH = 5`, `MIN_CHANNEL_FUNDING_USDC = 10`, `MAX_GAUGE_SHARE_PER_OPERATOR = 5%` are reasoned defaults. Production data may show that 5 is too sharp a filter for small regional operators, or that the cap needs tightening. Parameters are governable; safety bounds are wide enough for both directions. Monitor and retune in the first 6 months.
- **Reputation feedback loop.** Operators with high reputation get easier diversity thresholds. If reputation is gameable upstream, a sophisticated attacker first farms reputation, then exploits the relaxed threshold. Reputation gameability is [ADR 008](008-reputation.md)'s problem; the loop is worth flagging.
- **Cap residual UX.** When one or more operators hit the 5% cap, the residual bucket rolls over to the next epoch (per [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap)). At extreme operator concentration this means a meaningful share of the gauge bucket is delayed by one epoch. Acceptable cost given the cap is the binding wash-trading deterrent.

## Alternatives Considered

- **Parallel `DeliveryReceipt` primitive with MMR aggregator and on-chain fraud-challenge mechanism.** A receipt typedef signed by the requester per voucher window, batched into a per-(operator, epoch) Merkle Mountain Range, committed to `FeeRouter`, with a 7-day permissionless fraud-challenge window backed by an `OffenseType.ReceiptFraud` on `SlashJudge`. Rejected: a receipt's signed field set would be fully redundant with the voucher's (same signer, same bytes, same channel, same operator), and the MMR + fraud-challenge surface adds ~3 KB of audit code, an extra EIP-712 signature per voucher, a privacy regression on challenge, and a keeper-class summary-commit job — none of which is load-bearing once the diversity count is computed directly from settled vouchers. The per-operator gauge-share cap from [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) bounds the residual attack surface (sophisticated Sybil bypass) at a smaller code cost.

- **Funding-source diversity heuristic enforced on-chain.** A "funding-source diversity" criterion (the address's USDC balance for channel deposits not received from the operator's affiliated addresses), enforced via challenger-bonded submission with off-chain ancestry tracing pending [ADR 017](017-privacy.md) storage-proof verification. Rejected: the criterion is gameable by mixers/CEX cycles/DEX swaps, so the on-chain enforcement surface buys little beyond defense-in-depth. The per-operator gauge-share cap covers the same threat slice with no new audit surface.

- **Drop gauge-boost entirely.** Pure byte-proportional payout, no ve-locker steering. Rejected: gauge-boost is core to the tokenomics — it creates the demand for TOKEN locking that the supply schedule depends on (see [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)).

## ADRs Affected

- **[ADR 003](003-payments.md):** `Voucher` typedef extended with `epochId: uint64` field. Tier 3 schema break per [ADR 013](013-schema-evolution.md), bundled with `enableGauge()` cutover.
- **[ADR 008](008-reputation.md):** `reputationOf(operator)` exposed as an on-chain view consumed by `FeeRouter` at gauge-claim time. The receipt-tier thresholds in §12.1 govern §5 above.
- **[ADR 014](014-on-chain-verification.md):** No interaction. Diversity gating is enforced inside `FeeRouter`, not `SlashJudge`.
- **[ADR 026](026-gauge-boost-tokenomics.md):** §3 gains the per-operator gauge-share cap (`MAX_GAUGE_SHARE_PER_OPERATOR`, default 5%, governable `[1%, 25%]`). §3 gauge-claim now reads `distinctClientCount` and `bytesPerEpoch` from `FeeRouter` directly. §Risks cross-ref repointed at this ADR's §3 + §6 launch prerequisite.
