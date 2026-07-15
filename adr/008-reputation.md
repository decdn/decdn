# ADR 008: Reputation System

**Date:** 2026-03-29
**Status:** Draft

## Context

> **Tokenomics cross-reference.** Per [ADR 026](026-tokenomics.md#adr-026-tokenomics), operator return is entirely USDC-denominated: per-byte settlement (60% operator base at the `FeeRouter`). There is no ongoing TOKEN-denominated service emission; year-1 operator bootstrap is funded by off-chain USDC infrastructure subsidies per [ADR 026 § Bootstrap mechanism — pre-seed USDC](026-tokenomics.md#bootstrap-mechanism--pre-seed-usdc) (not a recurring TOKEN emission). Per-byte USDC is paid by real clients — wash trading does not raise revenue. The on-chain defenses against capacity-inflation are structural: vote weight is sourced from `FeeRouter.bytesInWindow` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) (proven delivered bytes, not declared capacity), and the super-linear capacity-bond curve makes over-declared capacity a dead-capital drag with no governance or revenue upside. Reputation contributes complementary off-chain signal — operator-cluster detection, diversity-of-service signal, governance input — but is not itself an eligibility gate. See [§ Wash-Trading: Reputation as Off-Chain Signal](#wash-trading-reputation-as-off-chain-signal).

The network needs to rank nodes beyond staking alone. Staking provides Sybil resistance but does not measure service quality. Clients need to prefer fast, reliable nodes and avoid slow or unresponsive ones without on-chain proof for every quality metric. A gossip-based reputation system using interaction-weighted scoring fills this gap.

## Decision

### Three-Tier Architecture

| Tier | Mechanism |
|------|-----------|
| Local | Direct observations (delivery speed, uptime, data correctness) |
| Network | Gossip-propagated signed reports via iroh-gossip |
| On-chain | Stake weight for sybil resistance, slashing for provable faults |

### Score Model

- Score range: 0.0 to 1.0 (stored internally as u32, 0 to 1,000,000, for 6-decimal precision)
- Initial score for new nodes: 0.5
- Weight: local observations 70%, network gossip 30%

### Local Score Calculation

After each interaction with a node, the client updates its local score using EWMA:

```
local_score = ewma(local_score, interaction_score, alpha=0.1)
```

Where interaction_score is:

| Metric | Score Contribution | Weight |
|--------|-------------------|--------|
| Delivery speed (bytes/sec vs. expected) | 0.0-1.0 (linear scale) | 40% |
| Data correctness (BLAKE3 verified) | 0.0 or 1.0 (binary) | 40% ¹ |
| Connection success (reachable?) | 0.0 or 1.0 (binary) | 20% |

Formula: `interaction_score = 0.4 * speed_score + 0.4 * correctness + 0.2 * reachability`

> **¹ Why 40% for data correctness — same as delivery speed?** Reputation measures *service quality*, not *honesty*. Data correctness is one quality signal among several; the primary corruption deterrent is **economic** (no payment for failed delivery) **plus traffic loss** (reputation-driven node selection), not on-chain slashing. Content corruption is fully absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md#adr-002-content-addressing) / [ADR 005](005-protocol.md#adr-005-wire-protocol)) — a corrupt window yields no voucher, so the node is unpaid for the bandwidth shipping garbage; see [ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery). Reputation impact compounds on top: a single corruption event (a) zeros the correctness component (−0.4 on `interaction_score`), (b) drops `local_score` via EWMA, (c) degrades node selection via the quadratic reputation penalty (`1/max(reputation, 0.1)²` — [ADR 001](001-network.md#node-selection-algorithm)). Lost revenue plus the traffic-routing penalty make sustained corruption irrational. A higher reputation weight or immediate local blacklist would over-penalize transient bao-pull failures (a brief upstream issue that resolves before retry) that the EWMA already absorbs.

Normalization: `speed_score = min(1.0, actual_bps / expected_bps)` where `expected_bps` is a node-local configurable baseline (default: 10 MiB/s = 10,485,760 bytes/sec).

EWMA with alpha=0.1 means recent interactions matter more but old interactions still contribute.

### Network Score Aggregation

Reports received via iroh-gossip are aggregated using EWMA weighted by reporter credibility:

```
weight_cap = 3.0
raw_weight = effective_settled_value(reporter) / max(1, max_effective_settled_value_observed)
reporter_weight = min(raw_weight, weight_cap)
network_score = ewma(network_score, report.score, alpha=0.05 * reporter_weight)
```

> **EWMA/clamp interaction:** The per-report clamp (±0.05) binds when `reporter_weight × gap > 1`. At the 3× cap the clamp activates for gaps above ~0.33; at weight 2×, above 0.5. For gap ≤ 0.33, the full 0–3× weight range produces proportional EWMA deltas without hitting the clamp. For gaps 0.33–0.5, weights above `1/gap` are clamp-limited but lower weights still differentiate. For gaps > 0.5, all weights above 2× produce identical clamped deltas. The 3× cap keeps the full weight range effective for the common case (small gaps), accepting that large corrections are clamp-governed regardless of reporter credibility.

- `effective_settled_value(reporter)`: the reporter's cumulative settlement value after distinct-counterparty discount and time decay (see [§ Distinct-Counterparty Discount](#distinct-counterparty-discount) and [§ Settled-Value Time Decay](#settled-value-time-decay)). Gross value is computed from all payment channels the reporter has settled on-chain. All channels are denominated in USDC, so gross value equals `total_settled_usdc` with no cross-token normalization. Including both sides credits nodes that pay for cache-miss pulls, not only nodes that receive delivery payment. **Value-weighted, not count-weighted** — this prevents Sybil manipulation via many cheap channels (100 channels of 1 USDC give the same weight as one channel of 100 USDC, making attack cost proportional to desired influence, not channel count).
- `max(1, max_effective_settled_value_observed)`: the `max(1, ...)` guard prevents division by zero at network bootstrap before any channels settle. At bootstrap all reporters have weight 0, so network scores stay at 0.5 until the first channels settle. **Note:** `max_effective_settled_value_observed` is local to each node, so two nodes may compute different weights for the same reporter — network scores are inherently subjective and will not converge to a single global value (accepted property; see Consequences). Because effective values decay over time (per [§ Settled-Value Time Decay](#settled-value-time-decay)), the max observed value drifts downward — recompute periodically (e.g., hourly or on each new `ChannelSettled` event) rather than caching indefinitely.
- `weight_cap`: caps reporter influence at 3× to prevent established high-earning nodes from disproportionately controlling network reputation. The 3× value sits just above the ~2× clamp-saturation threshold, so the full weight range is effective for typical score gaps while the per-report clamp governs extreme divergences. Preserves the anti-Sybil property (influence still scales with capital) while tightening maximum incumbency advantage.
- Alpha is scaled by reporter weight: high-credibility reporters (more settled value) move the score faster

#### Distinct-Counterparty Discount

The `effective_settled_value` computation applies a distinct-counterparty discount to prevent wash trading via self-dealing channels. For each reporter, the indexer tracks two quantities from `ChannelSettled` events:

- `gross_settled_value`: sum of all settlement amounts across channels where the reporter was either client or provider (i.e., the sum before applying diversity discount and time decay)
- `distinct_counterparties`: count of unique counterparty addresses across all settled channels within the last 52 weeks (`settlement_max_age` — see [§ Settled-Value Time Decay](#settled-value-time-decay))

The diversity factor scales the time-decayed settlement sum (see [§ Settled-Value Time Decay](#settled-value-time-decay) for the full formula):

```
diversity_factor = min(distinct_counterparties / min_counterparties, 1.0)
```

Where `min_counterparties = 5` (governance-tunable; hardcoded floor: 2).

**Effect on wash trading:** An attacker cycling funds between two self-owned addresses has `distinct_counterparties = 1`, yielding `diversity_factor = 0.2` — an 80% reduction in effective weight. Full credit needs settlements with 5+ distinct counterparties, each requiring a separate capacity bond (≈50,000 TOKEN at the 1 Gbps entry tier per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) and its own capital cycling fees.

**Counterparty validation:** Only counterparty addresses that had a `CapacityBond` NodeId binding at the time of channel settlement count toward `distinct_counterparties`. Unregistered addresses (pure clients without stake) do not count, since only staked nodes submit gossip reports (per [§ Gossip Protocol](#gossip-protocol)) and counterparty diversity matters only for reporter weight in the network score.

#### Settled-Value Time Decay

Individual settlement contributions decay exponentially with age, forcing an attacker to continuously cycle capital (incurring the `FeeRouter` non-base skim of 40% per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) — 30% burn + 10% treasury — plus per-cycle L2 gas, on their own USDC) to maintain reporter weight:

```
settlement_weight_i = exp(-lambda * age_weeks_i)
effective_settled_value = sum(settled_amount_i * settlement_weight_i) * diversity_factor
```

Where `lambda = 0.1` per week (half-life ≈ 6.9 weeks) and `age_weeks_i` is the time since the `ChannelSettled` event was emitted for channel *i*. Settlements older than `settlement_max_age` (52 weeks) are excluded entirely.

**Decay examples:**

| Settlement age | Weight retained |
|----------------|-----------------|
| 1 week | ~90% |
| 7 weeks | ~50% |
| 20 weeks | ~13.5% |
| 52 weeks | ~0.5% (then excluded) |

**Implementation note:** The computation can be cached per-reporter and updated incrementally on each new `ChannelSettled` event. The 52-week max age bounds the iteration window.

| Parameter | Value |
|-----------|-------|
| `min_counterparties` | 5 (governance-tunable; hardcoded floor: 2) |
| `settlement_decay_lambda` | 0.1 per week (half-life ≈ 6.9 weeks) |
| `settlement_max_age` | 52 weeks (older settlements contribute 0) |
| `weight_cap` | 3.0 (max `reporter_weight` value; bounds the EWMA alpha multiplier) |

### Combined Score

```
final_score = 0.7 * local_score + 0.3 * network_score
```

If a client has no local observations for a node (never interacted), it uses 100% network score.

#### Minimum reporter threshold

A node's network reputation score requires reports from at least 3 distinct staked reporters before departing from the default 0.5 neutral score. This defends against gossip-layer eclipse attacks where an attacker controls all of a victim's gossip peers on `cdn/reputation/v1` and injects fabricated reports. Until the threshold is met, the node is treated as unscored rather than positively or negatively rated.

```mermaid
flowchart TD
    subgraph Local["Local Score (70%)"]
        I[Interaction with node] --> M1["delivery_speed (40%)"]
        I --> M2["data_correct (40%)"]
        I --> M3["connection_success (20%)"]
        M1 --> IS["interaction_score =<br/>0.4*speed + 0.4*correct + 0.2*reachable"]
        M2 --> IS
        M3 --> IS
        IS --> EWMA1["local_score = EWMA(local, interaction, a=0.1)"]
        EWMA1 --> CLAMP1["Per-report clamp: max ±0.05"]
    end

    subgraph Network["Network Score (30%)"]
        GR[Gossip ReputationReport] --> FILT["Filter: exclude settlements<br/>> 52 weeks old"]
        FILT --> DC["diversity_factor =<br/>min(distinct_counterparties / 5, 1.0)"]
        DC --> TD["effective_settled_value =<br/>Σ(amount_i × e^(−0.1 × age_weeks_i)) × diversity_factor"]
        TD --> RW["reporter_weight =<br/>min(effective / max(1, max_observed), 3.0)"]
        RW --> EWMA2["network_score = EWMA(network, report,<br/>a=0.05 * reporter_weight)"]
        EWMA2 --> CLAMP2["Per-report clamp: max ±0.05"]
    end

    CLAMP1 --> FINAL["final_score =<br/>0.7 * local + 0.3 * network"]
    CLAMP2 --> FINAL

    FINAL --> DECAY["Decay toward 0.5<br/>10%/week without data"]
```

### Gossip Protocol

Dedicated gossip topic (`cdn/reputation/v1`) — all nodes subscribe. After interacting with a node, a node broadcasts a signed reputation report:

```rust
struct ReputationReport {
    provider: NodeId,
    reporter: NodeId,
    metrics: ReportMetrics,
    timestamp: u64,
    signature: Signature,
}

struct ReportMetrics {
    delivery_speed: Option<u32>,  // bytes/sec
    uptime_observed: Option<bool>,
    data_correct: Option<bool>,
}
```

Reports only accepted from staked nodes. **Client exclusion:** Only staked nodes may submit gossip reputation reports; clients (unstaked requesters) contribute only via their own local scores (per [§ Local Score Calculation](#local-score-calculation)). This is intentional — without a staking requirement, an attacker could spin up disposable clients to flood the gossip topic with cheap reports, bypassing the economic cost that makes manipulation expensive. **Recency validation:** receivers MUST reject reports where either (a) `current_time - report.timestamp > max_report_age_secs` (too old) or (b) `report.timestamp > current_time + allowed_clock_skew_secs` (too far in the future). Defaults: `max_report_age_secs = 3600` (1 hour), `allowed_clock_skew_secs = 300` (5 minutes). This prevents replay of old reports and reporters using far-future timestamps to extend the replay window. The effective replay window is bounded to `max_report_age_secs + allowed_clock_skew_secs` (~65 minutes). **Gossip deduplication:** As with `NodeAnnounce` messages ([ADR 001, Node Discovery](001-network.md#node-discovery-gossip)), iroh-gossip's transport-layer deduplication (PlumTree seen-message tracking) prevents the same `ReputationReport` being delivered to a node more than once via multiple epidemic paths. Unlike `NodeAnnounce`, no monotonic timestamp check is applied to reputation reports. While EWMA aggregation (per [§ Network Score Aggregation](#network-score-aggregation)) and per-report clamping (per [§ Score Clamping](#score-clamping)) limit the score impact of any single duplicate, the primary defense is the rate limit of 1 report per (reporter, node) pair per hour (per [§ Rate Limiting](#rate-limiting)), which independently rejects replayed reports within the same window. No additional message-id or content-hash seen-set is needed beyond the per-(reporter, node) rate-limit bookkeeping already required by [§ Rate Limiting](#rate-limiting). **Clock sync dependency:** unlike probe/stream timestamps (requester-generated, avoiding clock sync — see [ADR 005](005-protocol.md#adr-005-wire-protocol)), reputation recency depends on loose clock agreement between reporter and receiver. The 1-hour + 5-minute window tolerates typical NTP drift but not completely unsynchronized clocks.

```mermaid
classDiagram
    class ReputationReport {
        +NodeId provider
        +NodeId reporter
        +ReportMetrics metrics
        +u64 timestamp
        +Signature signature
    }

    class ReportMetrics {
        +Option~u32~ delivery_speed
        +Option~bool~ uptime_observed
        +Option~bool~ data_correct
    }

    ReputationReport *-- ReportMetrics
```

### Score Decay (Production Only)

Scores decay toward neutral (0.5) over time without new data. Applied iteratively each week:

```
score_new = score_old + (0.5 - score_old) * decay_rate
```

With `decay_rate = 0.10` (10% per week). Examples:

- Score 1.0: week 1 = 0.95, week 5 = 0.80, week 10 = 0.65, week 20 = 0.53
- Score 0.0: week 1 = 0.05, week 5 = 0.20, week 10 = 0.35, week 20 = 0.47

Scores converge to 0.5 asymptotically, reaching within 0.05 of neutral after ~30 weeks.

| Parameter | Value |
|-----------|-------|
| Decay rate | 10% per week (applied iteratively) |
| Decay starts after | 1 week with no new reports or interactions |
| Minimum score (floor) | 0.0 (selection algorithm clamps at 0.1 — see [ADR 001](001-network.md#node-selection-algorithm)) |

### Score Clamping

A single reputation report (local or network) can move a node's `local_score` or `network_score` by at most 0.05 in either direction. The derived weighted `final_score` (70% local, 30% network) is not separately clamped. This per-report cap prevents one bad interaction from destroying a good node or one fake report from inflating a sybil.

**Interaction with EWMA:** Per-report clamping applies to the delta produced by the EWMA update for `local_score` and `network_score` — compute the EWMA result for the component, then cap the change at ±0.05. For local scores (alpha=0.1), the EWMA itself limits deltas to `0.1 × |interaction_score − local_score|`, so clamping only binds when the score gap exceeds 0.5 (e.g., a node at 0.9 receiving a 0.0 interaction). For network scores, high-weight reporters (alpha up to 0.15) can produce EWMA deltas up to 0.15, making per-report clamping the primary rate limiter — intentional, ensuring no single report, however credible, moves a component score by more than 0.05.

**Selection clamp:** Independently of per-report clamping, the node selection formula in [ADR 001](001-network.md#node-selection-algorithm) clamps reputation to `max(reputation, 0.1)` to avoid division by zero. Nodes with reputation below 0.1 are scored identically (100× penalty vs. a perfect node) — effectively unselectable but not blacklisted.

### Tie-Breaking

When multiple nodes have the same unified selection score (within 1% — see [ADR 001, Node Selection Algorithm](001-network.md#node-selection-algorithm)), select by:

1. Geographic diversity (prefer nodes in regions not already selected)
2. Higher stake (more skin in the game)
3. Random (final tiebreaker)

### Rate Limiting

- Max 1 report per (reporter, node) pair per hour
- Max 10 reports per reporter per hour
- Reports exceeding limits are silently dropped by receiving nodes
- Enforced locally by each node on received gossip messages

### Wash-Trading: Reputation as Off-Chain Signal

Per-byte settlement (60% operator base in USDC) is paid by real clients — wash trading does not raise revenue. The wash-trading defense is structural: inflating *bytes_delivered* via self-routed channels does not raise per-byte settlement revenue (the attacker funds both sides of the wash and pays the FeeRouter's 40% non-base skim — 30% burn + 10% treasury — on every cycle, net negative per cycle). Inflating *declared capacity* without delivering bytes has no governance payoff either, because vote weight is sourced from `FeeRouter.bytesInWindow` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) — proven delivered bytes, not declared capacity — and the super-linear capacity-bond curve makes over-declared capacity dead capital with no governance or revenue upside. The remaining vote-buying attack — operator pays itself for bytes to inflate `FeeRouter.bytesPerEpoch` — is bounded by the per-operator vote cap and the [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying) cost model.

Reputation contributes complementary off-chain signal:

- **Operator-cluster detection.** An operator running a sybil ring of "real-looking" clients to inflate `bytes_delivered` — to buy served-bytes voting weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) — gets reputation-docked via [§ Local Score Calculation](#local-score-calculation) (delivery-speed and correctness failures) and [§ Network Score Aggregation](#network-score-aggregation) (gossip propagation). Low reputation reduces selection probability, lowering natural traffic and forcing more aggressive wash-trading to compensate — at which point the per-cycle USDC cost of the FeeRouter's 40% non-base skim binds harder.
- **Diversity-of-service signal.** Operators with broader real client bases earn more gossip reports (per [§ Distinct-Counterparty Discount](#distinct-counterparty-discount)) than concentrated-traffic operators. This is a soft tokenomics preference for diverse-client operators, expressed via reputation-weighted node selection, not a hard on-chain gate.
- **Governance input.** Reputation distributions over time feed governance decisions on whether to lower the per-operator vote cap, shorten `windowEpochs`, or raise the burn share if persistent wash-trading patterns emerge. Reputation is the canary; the per-operator vote cap is the immediate throttle.

There is no on-chain selection-eligibility gate keyed on reputation. Slashing and vote weight bind equally for high-rep and low-rep operators. Reputation operates in selection probability ([ADR 001](001-network.md#node-selection-algorithm)) and as input to governance-level threshold-tuning decisions, not as a per-epoch gate on settlement or voting weight.

### Regional-Coverage Reputation Signal

The reputation system exposes a per-operator **regional-coverage signal** — a derived metric (not a component of `final_score`) summarizing where an operator's verified deliveries originate geographically. Operators serving high-demand low-coverage regions receive a positive signal; operators serving only oversaturated regions receive a neutral signal.

**This signal is not an input to `final_score` or to any on-chain payout (settlement or governance vote weight) (see [§ Wash-Trading: Reputation as Off-Chain Signal](#wash-trading-reputation-as-off-chain-signal)).** It is an externally-readable per-operator attribute computed from the same gossip reports that drive [§ Local Score Calculation](#local-score-calculation) and [§ Network Score Aggregation](#network-score-aggregation), exposed alongside the main score so downstream programs can consume it without subscribing to anything new.

#### Region taxonomy

Regions are **ISO 3166-1 alpha-2 country codes** (e.g., `DE`, `US`, `JP`), stored as `bytes2` matching the gas-optimization convention in [ADR 011 § Contract: ContentBlacklist](011-content-takedown.md#contract-contentblacklist) and the packing used by [ADR 031 § Storage layout](031-content-blacklist-appeals-contract.md#storage-layout). This is the same choice that already drives `node.region` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)), `BlacklistEntry.region` ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)), and `BlacklistAppeal.region` ([ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface)) — keeping all four surfaces at the same grain so the signal can be meaningfully cross-referenced against blacklist scope and operator declaration.

**Sub-national subdivisions are out of scope** for this ADR. ISO 3166-2 (`US-CA`, `DE-BY`, `CA-QC`, etc.) is the standard for finer granularity, and several compliance regimes — CCPA, Quebec Law 25, German Länder-level overlays — operate at that level. They are excluded here for two reasons:

1. **Grain mismatch with blacklist scope.** [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) regional blacklists, regional governance bodies, and the [ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface) appeal contract are all alpha-2 only. A reputation signal at finer grain than the compliance surface it feeds would silently overstate what it can certify.
2. **Sub-national regimes are not protocol-addressable.** CCPA, Quebec Law 25, etc. are data-controller obligations on the *publisher*, not the *CDN operator*. The protocol-level compliance surface (content takedowns) is country-level by construction.

If a future ADR needs sub-national reputation, all four surfaces — [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) region attestation, [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) blacklist regions, [ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface) appeal regions, and the reputation buckets here — must move together. Per-surface upgrades are forbidden because they would introduce grain mismatch between the signal and what it claims to certify.

#### Storage and aggregation

For each operator a consuming node maintains a sparse map keyed on region:

```rust
struct CoverageBucket {
    score: f32,                  // [0.0, 1.0] — same scale as final_score
    last_interaction_at: u64,    // unix timestamp (seconds) of the most recent update
}

// Per-operator state, computed locally from received ReputationReports.
type RegionalCoverage = HashMap<[u8; 2], CoverageBucket>;
```

Only regions with at least one delivery interaction in the trailing decay window appear in the map — operators serving 5 regions carry 5 entries, not 197.

#### Update rule

On receipt of a `ReputationReport` from reporter `R` about operator `O`:

1. Look up `R.region` from the local peer table (the latest `NodeAnnounce` for `R`'s `NodeId` per [ADR 001 § Node Discovery](001-network.md#node-discovery-gossip)). Reports from reporters with no current `NodeAnnounce` or an unattested region are dropped from regional aggregation only — they still contribute to the main `network_score` per [§ Network Score Aggregation](#network-score-aggregation).
2. If no bucket exists for `R.region`, initialize `bucket.score = 0.5` (neutral) — same convention as the main scores in [§ Local Score Calculation](#local-score-calculation) — and proceed to step 3.
3. Compute `interaction_score` from `report.metrics` using the same formula as [§ Local Score Calculation](#local-score-calculation) (`0.4 × speed_score + 0.4 × correctness + 0.2 × reachability`).
4. Compute `reporter_weight` for `R` using the existing [§ Network Score Aggregation](#network-score-aggregation) machinery: `effective_settled_value(R)` with the [§ Distinct-Counterparty Discount](#distinct-counterparty-discount) distinct-counterparty discount and [§ Settled-Value Time Decay](#settled-value-time-decay) time decay applied, normalized against `max(1, max_effective_settled_value_observed)`, capped at the 3× `weight_cap`. Reporters with `reporter_weight = 0` (fresh-staked, no settled-channel history) have no effect on the bucket.
5. Update the bucket: `bucket.score = ewma(bucket.score, interaction_score, alpha = 0.05 × reporter_weight)` — same weighted-EWMA shape as `network_score` in [§ Network Score Aggregation](#network-score-aggregation). Cap the delta at ±0.05 per the [§ Score Clamping](#score-clamping) per-report clamp.
6. Set `bucket.last_interaction_at = now` (unix seconds).

#### Decay (mirrors §Score Decay)

Each `CoverageBucket` decays toward `0.5` (neutral) at the same per-week rate as the component scores in [§ Score Decay (Production Only)](#score-decay-production-only). Because lookups are lazy and elapsed time is arbitrary, the closed-form is canonical:

```
weeks_elapsed = (now - bucket.last_interaction_at) / SECONDS_PER_WEEK
score_decayed = 0.5 + (bucket.score - 0.5) * (0.9 ^ weeks_elapsed)
```

This is the same `1 - 0.10` per-step factor as [§ Score Decay (Production Only)](#score-decay-production-only) applied for `weeks_elapsed` steps; `weeks_elapsed` MAY be fractional (no quantization to whole weeks). Decay is computed per-bucket at lookup time from `last_interaction_at` — no background sweep. Buckets whose `score_decayed` is within `0.05` of neutral and whose `last_interaction_at` is older than `26 weeks` MAY be evicted from the sparse map as a storage-cleanup pass; the next lookup against an evicted region returns "no signal" rather than "neutral" so consumers can distinguish "never seen" from "decayed to neutral."

#### Consumer access

Downstream operational programs (regional deployment grants, hardware-leasing subsidies, staking-loan approvals) read the coverage map via a local read-only API on the reputation subsystem:

```rust
// "What's this operator's coverage in this region?"
fn coverage(operator: NodeId, region: [u8; 2]) -> Option<f32>;

// "Which regions does this operator serve?"
fn covered_regions(operator: NodeId) -> Vec<([u8; 2], f32)>;
```

No new gossip topic is introduced. The map is per-consumer (each node computes its own view from gossip), matching the subjectivity property in [§ Network Score Aggregation](#network-score-aggregation) — two consumers may compute different regional-coverage maps for the same operator, just as they may compute different `network_score` values.

Eligibility thresholds (e.g., "operator must have coverage ≥ 0.7 in at least 3 regions to qualify for the grant") belong to consuming programs, not this ADR. The reputation system commits only to publishing the signal in a form those programs can read.

Consumers should note the signal trusts the reporter's self-declared `node.region` from `NodeAnnounce`. Region attestation is self-claimed as the production posture per [ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation); the latency-contradiction mitigation in [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) catches gross lies but not adjacent-country or VPN-routed claims. Programs requiring stronger attestation should layer their own filter on top — e.g., only credit reports from reporters whose latency-contradiction rate is below a threshold.

## Consequences

### Positive

- Interaction-weighted scoring makes reputation manipulation expensive — you need real economic activity (settled payment channels), not just stake
- Distinct-counterparty discount and settlement time decay raise the cost of wash trading from a single self-dealing pair to requiring 5+ capacity bonds (each ≥ entry-tier 50K TOKEN per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) plus the per-cycle `FeeRouter` non-base skim (40% per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)) across 5+ counterparties — an order-of-magnitude increase in capital requirements
- Local observations dominate (70%), so a node's own experience always outweighs the crowd
- Score clamping limits the damage from individual malicious reports
- Decay prevents stale high scores from persisting indefinitely
- Regional-coverage signal (per [§ Regional-Coverage Reputation Signal](#regional-coverage-reputation-signal)) is exposed but not coupled to scoring or selection — downstream operational programs own deployment-grant decisions, keeping the reputation system narrowly focused on service-quality measurement

### Negative

- Off-chain reputation is inherently subjective — no single ground truth
- A well-funded attacker can still build real interaction history to manipulate scores, but must maintain settlements with at least 5 distinct counterparties and continuously cycle capital to counteract settlement decay. Cost scales linearly with desired influence and multiplicatively with the counterparty diversity requirement (capital in N distinct channels, each a separate staking deposit, not one recycled pair). Coordinated multi-party wash trading (5+ colluding nodes) remains possible but requires N staking deposits plus cycling fees
- Circular settlement detection (graph-based analysis of settlement flow patterns) is not in this version. A coordinated attacker operating 5+ staked nodes can still build wash-traded reputation, though at substantially higher capital cost. Future iterations may add graph-based detection as an additional layer
- Reporter weight creates a residual incumbency advantage — established nodes with more settled USDC have more influence over network scores. The 3× weight cap bounds this tightly — set just above the ~2× clamp-saturation point so the full range is effective for typical score gaps while limiting maximum influence
- Gossip propagation adds bandwidth overhead: at 1,000 nodes with all reporters at max rate (10 reports/hr), each node receives ~10,000 reports/hr (~2 MB/hr ingress), modest relative to `NodeAnnounce` traffic (~48 MB/hr at 60-second intervals). The strict rate limits (per [§ Rate Limiting](#rate-limiting)) keep reputation gossip well-bounded. See [ADR 001, Gossip Bandwidth Analysis](001-network.md#gossip-bandwidth-analysis) for the combined budget
- The 70/30 local/network split means a client's view of the network is biased toward its own usage patterns
- Coordinated negative gossip reports could push an operator's reputation down unfairly, reducing selection probability. Per-report clamping (per [§ Score Clamping](#score-clamping)) and the 3× reporter-weight cap (per [§ Network Score Aggregation](#network-score-aggregation)) limit the speed and magnitude of such attacks. Because reputation is not a settlement- or vote-weight-eligibility gate (per [§ Wash-Trading: Reputation as Off-Chain Signal](#wash-trading-reputation-as-off-chain-signal)), an unfairly-docked operator still earns per-byte USDC settlement and served-bytes voting weight proportional to their actual settled bytes — they only lose selection probability, a continuous penalty rather than a hard exclusion
