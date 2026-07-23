# ADR 008: Reputation System

**Date:** 2026-03-29
**Status:** Draft

## Context

> **Tokenomics cross-reference.** Per [ADR 026](026-tokenomics.md#adr-026-tokenomics), operator return is entirely USDC-denominated: per-byte settlement (60% operator base at the `FeeRouter`). Reputation is **not** an eligibility gate, a governance input, or a component of settlement or vote weight. The on-chain defenses against capacity-inflation are structural and independent of reputation: vote weight is sourced from `FeeRouter.bytesInWindow` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) (proven delivered bytes, not declared capacity), and the super-linear capacity-bond curve makes over-declared capacity dead capital. Reputation influences only a client's own node-selection probability ([ADR 001](001-network.md#node-selection-algorithm)).

The network needs to rank nodes beyond staking alone. Staking provides Sybil resistance but does not measure service quality. Clients need to prefer fast, reliable nodes and avoid slow or unresponsive ones without on-chain proof for every quality metric.

Reputation is **local-only**: each node scores its peers from its own direct delivery observations, and nothing else. There is no gossip propagation of reputation, no cross-node aggregation, and no network-wide reputation score. A node's ranking of a peer reflects only interactions that node has witnessed itself — the same tit-for-tat principle BitTorrent uses for peer selection, which needs no global reputation to function at scale.

This is deliberate. Reputation is not the Sybil or corruption defense — stake, the capacity bond, mandatory progressive BLAKE3 verification, and no-payment-on-failed-delivery are (see [ADR 002](002-content-addressing.md#adr-002-content-addressing), [ADR 003](003-payments.md#adr-003-payments), [ADR 026](026-tokenomics.md#adr-026-tokenomics)). Reputation only tunes each client's selection probability among otherwise-eligible nodes. Keeping it local avoids the failure modes a gossiped, aggregated score would introduce: herding onto incumbents (a shared score concentrates load and starves cold nodes), an incumbency amplifier (credibility-weighted reporters), and a permanent gossip attack surface (eclipse, coordinated negative reports, replay, clock-sync). A local score has none of these: a peer a node has never used stays neutral and fully selectable, so traffic keeps exploring rather than converging.

## Decision

### Local Scoring

Each node maintains a per-peer score derived solely from its own interactions. No gossip topic, no reporter weighting, no on-chain reputation state.

- Score range: 0.0 to 1.0 (stored internally as u32, 0 to 1,000,000, for 6-decimal precision)
- Initial score for new nodes: 0.5 (neutral)
- A peer the node has never interacted with is **unscored** and treated as neutral (0.5) by the selection algorithm

### Local Score Calculation

After each interaction with a node, the node updates that peer's score using EWMA:

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

> **¹ Why 40% for data correctness — same as delivery speed?** Reputation measures *service quality*, not *honesty*. Data correctness is one quality signal among several; the primary corruption deterrent is **economic** (no payment for failed delivery) **plus traffic loss** (reputation-driven node selection), not on-chain slashing. Content corruption is fully absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md#adr-002-content-addressing) / [ADR 005](005-protocol.md#adr-005-wire-protocol)) — a corrupt window yields no voucher, so the node is unpaid for the bandwidth shipping garbage; see [ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery). Reputation impact compounds on top: a single corruption event (a) zeros the correctness component (−0.4 on `interaction_score`), (b) drops `local_score` via EWMA, (c) degrades node selection via the quadratic reputation penalty (`1/max(reputation, 0.1)²` — [ADR 001](001-network.md#node-selection-algorithm)). Lost revenue plus the traffic-routing penalty make sustained corruption irrational. A higher reputation weight or an immediate local blacklist would over-penalize transient bao-pull failures that the EWMA already absorbs.

Normalization: `speed_score = min(1.0, actual_bps / expected_bps)` where `expected_bps` is a node-local configurable baseline (default: 10 MiB/s = 10,485,760 bytes/sec).

EWMA with alpha=0.1 means recent interactions matter more but old interactions still contribute.

### Score Decay

Scores decay toward neutral (0.5) over time without new data, so a peer that stops being used drifts back to unopinionated rather than holding a stale high or low score. Applied iteratively each week:

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
| Decay starts after | 1 week with no new interactions |
| Minimum score (floor) | 0.0 (selection algorithm clamps at 0.1 — see [ADR 001](001-network.md#node-selection-algorithm)) |

### Score Clamping

A single interaction can move a peer's `local_score` by at most 0.05 in either direction. This per-report cap prevents one bad interaction from destroying a good node's standing.

**Interaction with EWMA:** clamping applies to the delta produced by the EWMA update — compute the EWMA result, then cap the change at ±0.05. With alpha=0.1 the EWMA itself limits deltas to `0.1 × |interaction_score − local_score|`, so the clamp only binds when the score gap exceeds 0.5 (e.g., a node at 0.9 receiving a 0.0 interaction).

**Selection clamp:** independently of per-interaction clamping, the node selection formula in [ADR 001](001-network.md#node-selection-algorithm) clamps reputation to `max(reputation, 0.1)` to avoid division by zero. Nodes with reputation below 0.1 are scored identically (100× penalty vs. a perfect node) — effectively unselectable but not blacklisted.

### Tie-Breaking

When multiple nodes have the same unified selection score (within 1% — see [ADR 001, Node Selection Algorithm](001-network.md#node-selection-algorithm)), select by:

1. Geographic diversity (prefer nodes in regions not already selected)
2. Higher stake (more skin in the game)
3. Random (final tiebreaker)

```mermaid
flowchart TD
    I[Interaction with node] --> M1["delivery_speed (40%)"]
    I --> M2["data_correct (40%)"]
    I --> M3["connection_success (20%)"]
    M1 --> IS["interaction_score =<br/>0.4*speed + 0.4*correct + 0.2*reachable"]
    M2 --> IS
    M3 --> IS
    IS --> EWMA1["local_score = EWMA(local, interaction, a=0.1)"]
    EWMA1 --> CLAMP1["Per-interaction clamp: max ±0.05"]
    CLAMP1 --> DECAY["Decay toward 0.5<br/>10%/week without data"]
    DECAY --> SEL["Node selection<br/>(ADR 001, quadratic penalty)"]
```

## Consequences

### Positive

- A node's ranking of a peer always reflects its own experience — there is no subjective network score, no convergence problem, and no reporter-credibility incumbency advantage.
- No reputation gossip topic, no reporter weighting, no distinct-counterparty / settled-value machinery, no clock-sync dependency, and no eclipse or coordinated-gossip attack surface — none of it exists to attack or maintain.
- Naturally load-spreading: a peer a node has never used stays neutral (0.5) and fully selectable, so traffic keeps exploring new and cold nodes rather than herding onto established ones (the role BitTorrent gives optimistic unchoking).
- Score clamping limits the damage from any single bad interaction; decay prevents stale scores from persisting.
- Reputation is decoupled from settlement and governance entirely — an operator's USDC earnings and served-bytes vote weight ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) never depend on any peer's opinion of it.

### Negative

- A node learns a peer is unreliable only by interacting with it — there is no early warning from other nodes' experience. This cost is bounded: mandatory progressive BLAKE3 verification plus no-payment-on-failed-delivery cap a bad interaction at roughly one unpaid window of wasted ingress, and the peer earns nothing for shipping garbage (see [ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery)).
- A node's view is inherently biased toward its own usage pattern. This is intended — local observation is the signal.
- Network-wide avoidance of a persistently bad actor is not automatic. Provable faults are handled by slashing ([ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals)), not reputation; merely-mediocre nodes are down-weighted independently by each client that encounters them.
