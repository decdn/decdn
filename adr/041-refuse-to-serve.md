# ADR 041: Refuse-to-Serve Economics

**Status:** Accepted

## Context

A node serves a cache miss by buying the bytes upstream and reselling them to
the client. Every settlement, including a node-to-node buy leg, pays the
`FeeRouter` split. The operator nets the operator share of each sale and pays
the full upstream price. A single serve therefore clears only when the upstream
price is far enough below the sell price to cover the skim.

The protocol does not require a node to serve. Refusing is not slashable. A
refused client re-routes to another provider and pays only a latency cost. A
node that serves below margin loses money for no protocol reason. "Serve when it
pays, decline when it does not" is the correct behavior for a trustless market.

The serve path already refuses on several static grounds: an absolute pay
ceiling (`cache.max_rate_per_mb`), client solvency
([ADR 003](003-payments.md#adr-003-payment-model)), per-lane exposure, blob size, and origin health.
None of these compares the buy price against the sell price. That margin
comparison is the gap this ADR fills.

**A buyable route is not guaranteed.** A blob can have no reachable holder, or
only a holder that prices above any margin. The node does not assume a route to
buy from exists. The absence of a profitable route is an ordinary, expected
outcome, not an error.

## Decision

### A node-local economics gate on the buy path

`ServeEconomicsPolicy` is a node-local, config-selectable trait. It is a sibling
to the cache policies of [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies), not part of them. Cache
policy decides what a node keeps; this policy decides what a node buys to serve.
The gate lives in the node buy path, next to route discovery and ranking. The
cache engine holds no price logic.

The policy returns a price, not a verdict:

```rust
pub trait ServeEconomicsPolicy: Send + Sync + std::fmt::Debug {
    /// The most the node pays upstream, per MB, for this blob now.
    /// `None` means no economic ceiling; only the static ceiling then applies.
    /// `Some(v)` bounds the buy; `v` may be zero, which refuses every paid route.
    fn max_buy_per_mb(&self, ctx: &ServeEconomicsCtx) -> Option<u64>;
}
```

Refusal is emergent. The node scans the ranked candidates and buys from the
first whose quoted rate is at or below the ceiling. When candidates exist but
none clears the ceiling, the node returns a below-margin miss. When discovery
finds no candidate at all, the node returns an ordinary empty miss. Both answer
the client with the same `NotFound`; they differ only in a local metric.

### The buy ceiling

Let `sell` be the node's own floor-clamped sell rate, `operator_bps` the
operator share from the fee split, and `heat` the frequency estimate for the
hash. The profit-guarantee price amortizes a buy across expected re-serves:

```
n_hat     = clamp(round(discount * heat), 1, n_max)
amortized = (operator_bps / 10_000) * n_hat * sell
```

A buy at `amortized` is recovered by `n_hat` serves after the fee skim. But
warming is speculative: the node pays up front and recovers only if the blob is
served again. On a flat market — where a peer sells at the same rate the node
sells — `amortized` at a cold `n_hat = 1` sits below the market price, so a node
that bought only at `amortized` could never relay a cold blob, and the mesh could
never warm.

The node therefore buys at the market price while it holds a warming allowance
for the source, and falls back to the profit-guarantee price when the allowance
is spent:

```
max_buy = if warming_allowance(source) > 0 { max(sell, amortized) }
          else                             { amortized }
```

`max(sell, amortized)` lets a cold blob clear at the market price, so warming
works, while a hot blob still commands a premium above market. `discount` derates
the raw estimate; `n_max` caps how much a mis-estimated blob justifies. The
allowance (next section) bounds how much a node speculates before it demands a
proven margin.

### The per-source warming allowance

A node cannot both warm cold content and refuse every speculative buy. Warming is
speculation, and speculation on planted cold blobs is a griefing surface: an
attacker that is the sole source of many distinct one-hit blobs makes a node buy
each at the market price and lose the fee skim on it. The per-blob loss is
symmetric — the attacker burns the same skim — but a well-capitalized attacker
could drain a node's whole deposit at a one-to-one cost, and the deposit is
on-chain visible. A node cannot lose its bond this way, but it can lose its
deposit.

The warming allowance bounds this. Each upstream source, a bonded seller node,
carries an allowance capped at `B`. The allowance refills by serve-vindicated
profit and loss. A speculative buy of a blob from a source — one that clears only
because of the market-price band, not the profit-guarantee price — debits that
source's allowance by the buy cost and tags the blob with its source. Every serve
of a tagged blob credits the source's allowance the realized margin, capped at
`B`. Eviction forgets the tag. A blob re-served two or more times fully refunds
its buy, so an honest source stays warm; a blob served once nets the fee skim as a
loss and stays drained, so a source that keeps selling duds is cut off. A slow
time refill forgives a transient bad patch. While a source's allowance is
positive, the node warms from it at the market price; once the allowance is spent,
the node buys from that source only at the profit-guarantee price.

The allowance is keyed on the source node, never the client. Client identities
are cheap, so a per-client allowance would reset on a fresh client. Source
identities are bond-gated, so a multi-source attack costs one bond per source.
This caps grief to `B` per source per refill window and keeps the operational
deposit untouchable. The allowance is node-local state; it is not a wire or
governance concern.

### Signals

- **Sell rate.** The node's served price, after the on-chain rate-bounds floor
  ([ADR 005](005-protocol.md#adr-005-wire-protocol)).
- **Operator share.** The operator basis points from the `FeeRouter` shares.
  The node seeds the value at startup and refreshes it through the shared
  chain-event poller. The node holds no hardcoded split.
- **Heat.** The frequency estimate from the [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies)
  `FrequencyEstimator`. That estimator observes one sighting per served request
  at the serve chokepoint. Fills and probes emit no sighting. A node therefore
  cannot raise a blob's heat without serving, and paying for, real requests.
- **Candidate prices.** The signed per-MB rate each candidate returns on its
  probe. The node reuses its short-lived probe cache and adds no probe fan-out
  for the gate. A candidate with no fresh cached rate is unqualified for the
  gate and is skipped.

### Composition and enforcement

`max_buy` composes with the static ceiling by the lower-of rule the pull path
already applies: the effective ceiling is the minimum of `cache.max_rate_per_mb`
and `max_buy`, with zero meaning unbounded on each input. The node enforces the
effective ceiling at the point it commits the buy, not only at ranking. A
candidate that quotes a low rate on its probe and then returns a higher rate in
its signed response exceeds the ceiling and the node aborts that leg. The signed
probe rate also makes a low-quote, high-deliver node slashable.

### No route is a valid outcome

When no candidate clears the ceiling, or when discovery returns no holder at
all, the node refuses and closes. The node returns the same `NotFound` the wire
already uses for a miss, so the node's pricing floor does not leak. The node
distinguishes the below-margin case from an empty miss only in a local metric,
never on the wire. The node emits no redirect and no route hint. A redirect is an
attack surface: a node could steer clients to a colluding, expensive peer.
Clients stay trustless of node routing advice and re-route through their own
failover ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality),
[ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1)).

Refusal never denies availability by itself. A refused client still reaches any
holder directly through its own discovery. The node declines an unprofitable
middle hop; it does not gate the data.

### Scope: relayed content only

The gate applies to a foreign-namespace miss, where the node buys bytes from a
peer to relay them. It never applies to a node's own namespace. An origin does
not refuse its own content. The origin's only economic choice for its own
namespace is where to source the bytes — its backend or another node — and that
sourcing choice is a separate concern, outside this decision. A node's decision
to relay foreign namespaces at all is likewise a separate policy, outside this
decision. When a miss resolves through the node's own origin backend, no peer
buy leg exists, so the gate is not consulted.

### Configuration

```toml
[cache.serve_economics]
policy         = "margin"     # "off" | "margin"   (default "margin")
discount       = 0.5          # derates the raw heat estimate; in (0, 1]
n_max          = 64           # ceiling on the expected re-serve count
warming_budget = 5_000_000    # per-source allowance B; $5 in payment base units
warming_refill = 58           # allowance refill rate, base units per second (~$5/day)
```

The per-source ledger is debited the full upstream buy cost on a speculative
pull and credited the realized operator margin on each serve, so a one-hit blob
nets the fee skim as its lasting loss. A `$5` budget therefore covers a large
volume of unrecovered one-hit warming per source before it cuts a source off.

The default is `margin`. With a warming allowance, a node buys cold blobs at the
market price and warms; without operator action the gate still bounds speculative
loss per source. `policy = "off"` returns no ceiling, so only
`cache.max_rate_per_mb` applies; that reproduces the static-ceiling-only
behavior.

The margin policy and the cache policies of [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies) share
one frequency estimator, but each enables it on its own. The node builds the
estimator when any consumer asks for it: a `tinylfu` admission selector, a
`tinylfu` eviction selector, or a `margin` serve-economics policy. No feature is
inert for want of another. A node that runs `margin` alone still gets a live
heat signal, so a warming blob earns its amortized premium instead of staying
pinned at the market price. The estimator reads its sketch size from the existing
`cache.tinylfu.sketch_bytes`.

The `node` layer owns the name-to-implementation mapping and validates the
values. An unknown policy name is a config error at load, with no silent
fallback. A `discount` outside `(0, 1]` or an `n_max` of zero is a config
error.

The policy is node-local. A node's margin target is its own commercial choice.
The fee split it reads is governed on-chain; the margin policy over that split is
not. This matches the node-local stance of [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies).

### Adversarial reasoning

The gate bounds the forced-loss attack; it does not relocate it. An attacker
controls a sink node that is the sole holder of blobs and quotes a rate, and
controls the clients that request them from a victim node.

There are two attacks, one for each buy regime.

**Overpriced route.** The attacker's sink quotes far above the market rate. The
buy ceiling is at most `max(sell, amortized)`, so a quote above that is refused.
At a cold `n_hat = 1` the ceiling is the market price. To lift it, the attacker
must lift `n_hat`, and heat rises only on served, paid requests: the
[ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies) estimator observes one sighting per serve at the
serve chokepoint, never on a fill or a probe. Inflating `n_hat` therefore forces
the attacker to pay the victim real margin first, and heat and eviction read the
same signal, so a hot blob stays cached and triggers no second buy. `n_max` and
`discount` bound the residual gap between the estimate and realized serves.

**At-market dud flood.** The attacker plants many distinct one-hit blobs on its
sink at the market price. Each clears the warming band and costs the victim the
fee skim. The per-source warming allowance bounds this: after the sink has spent
its allowance `B` on duds that never re-serve, the victim buys from it only at
the profit-guarantee price and stops speculating on it. The allowance is keyed
on the sink's bonded node identity, so the attacker cannot reset it with fresh
client identities, and a multi-source flood costs one bond per source. Grief is
bounded to `B` per source per refill window, and the operational deposit — which
the hard solvency floor ([ADR 003](003-payments.md#adr-003-payment-model)) already protects — is never
the target. A node cannot lose its bond, and this keeps it from losing its
deposit.

## Consequences

### Positive

- A node warms cold content at the market price, yet never overpays for it and
  never lets speculation drain its deposit. The ceiling adapts to the sell price,
  the governed fee split, and blob popularity.
- The policy plugs in behind a trait, mirrors the [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies)
  seam, and is unit-testable as a pure function over its context. The allowance
  is a small node-local per-source accumulator.
- The gate reuses the existing probe cache, rank order, and lower-of ceiling
  path. It adds no wire surface and no probe fan-out.
- Refusal reuses the existing `NotFound` outcome, so the node leaks no pricing
  floor and clients re-route through existing failover.

### Negative

- Warming is speculative: a node loses the fee skim on a genuine one-hit blob.
  The per-source allowance bounds this loss and cuts off a source that keeps
  selling duds, but it cannot make a one-hit profitable.
- The amortized premium depends on the frequency estimate. Enabling `margin`
  builds the shared estimator on its own, so the gate has live heat whenever it
  runs; a blob commands only the market price until it warms.
- Neither the estimate nor the allowance is durable. After a restart a hot blob
  re-enters at the market price and each source's allowance resets to full.

## Acceptance Criteria

1. `ServeEconomicsPolicy` is defined in the `node` buy path, selectable by
   `cache.serve_economics.policy`, and defaults to a shape that preserves the
   static-ceiling-only behavior when set to `off`.
2. The margin policy computes `amortized = (operator_bps / 10_000) * n_hat *
   sell` with `n_hat = clamp(round(discount * heat), 1, n_max)`, and
   `max_buy = max(sell, amortized)` while the source's warming allowance is
   positive, `amortized` once it is spent.
3. A cold blob (`n_hat = 1`) clears at the market price while the source has
   allowance, so warming works on a flat market; once the source's allowance is
   spent, its cold blobs clear only at `amortized`.
4. Each source (a seller node identity) has a warming allowance capped at `B`; a
   speculative buy debits it, and each re-serve of a source-tagged blob credits it
   (serve-vindicated), with a slow time refill. The allowance is keyed on the
   source node, not the client.
5. `max_buy` composes with `cache.max_rate_per_mb` by the lower-of rule and is
   enforced at buy commit, so a higher rate in a signed response aborts the leg.
6. The operator share comes from the `FeeRouter` shares, seeded at startup and
   refreshed through the shared chain-event poller; the node holds no hardcoded
   split.
7. The heat signal comes from the shared `FrequencyEstimator`
   ([ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies)), which observes only served requests. The
   node builds the estimator when any of `tinylfu` admission, `tinylfu`
   eviction, or `margin` serve-economics is active; each feature enables it
   independently and none is inert without another.
8. When no candidate clears the ceiling, or discovery finds no holder, the node
   returns `NotFound` and emits no redirect or route hint.
9. Own-origin legs, which buy at rate zero, bypass the gate.
10. An unknown policy name, a `discount` outside `(0, 1]`, an `n_max` of zero, or
    a non-positive `warming_budget` is a config error at load, with no silent
    fallback.
11. The workspace builds clean under the anti-panic clippy lints
    (`unwrap_used`, `expect_used`, `panic`, `indexing_slicing` denied).
