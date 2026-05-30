# ADR 037: Latency-Driven Proxy Warming for Regional Locality

**Date:** 2026-05-30
**Status:** Draft

## Context

Content discovery is keyspace-routed — `cdn/dht/v1` places provider records and routes FIND_VALUE queries by XOR distance from the BLAKE3 hash ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). Demand, however, is geographic. The two coordinate systems are independent, so a node has no protocol path to learn that geographically-nearby clients want content that currently lives only in a distant region.

The canonical failure: a client near Tokyo requests blob `H` whose only holder is a London origin. Nodes near Tokyo never learn that local clients want `H`, so no nearby copy is ever made, and every Tokyo client pays inter-continental latency. Neither demand signal in [ADR 022 § Popularity Signals](022-content-discovery.md#popularity-signals-and-market-dynamics) surfaces this:

- **FIND_VALUE query frequency** accrues at the K nodes closest to `H` *in keyspace*, which are geographically uniform-random. The node that observes "`H` is in demand" is, with high probability, not near the clients generating the demand.
- **Local cache-miss frequency** fires only on a node that itself received a request for `H`. Clients route to the lowest selection score ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm)), which is the London origin, so nearby cache nodes never receive the request, never record a miss, and never cache.

This is a cold-start locality trap: a nearby node cannot earn the traffic for `H` until it holds `H`, and it does not receive `H` until it has earned the traffic. The trap is self-clearing the instant *one* nearby copy exists — that node publishes a DHT STORE ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish)), a regional FIND_VALUE returns it, its low RTT wins the selection score, and subsequent demand reinforces it. The unsolved problem is bootstrapping the first regional copy.

A demand-broadcast layer (gossiping per-region hash popularity so nearby nodes prefetch) was weighed and rejected: it adds a new gossip message type, a new poisoning surface on top of the manufactured-demand vectors already analyzed in [ADR 022 § Threat model](022-content-discovery.md#threat-model), and a privacy surface over client request patterns ([ADR 017](017-privacy.md#adr-017-privacy-analysis)). This ADR instead bootstraps the first copy at selection time, as a side effect of ordinary paid delivery, with no new wire surface.

## Decision

A client that finds only distant holders for a cache miss routes its paid `cdn/client/v1` request *through a nearby bonded node that does not yet hold the blob*. That node fills the request by chunk-paced cache-miss pull-through, serving the client while caching the blob, and thereby becomes the first regional copy. The mechanism decomposes into a client-side selection policy and a node-side serving behavior, neither of which changes the wire format.

### Client selection policy: latency-driven proxy preference

On a cache miss for hash `H`, the client runs the normal FIND_VALUE → probe lookup and obtains measured RTTs to the live holders. The client then applies a proxy-warming pre-step before committing to a holder:

- **Trigger.** Proxy warming engages only when the best holder's RTT exceeds `proxy_warming.rtt_threshold_ms` (the holders are all distant) **and** the client's cumulative RTT map contains a bonded node whose measured RTT is lower than the best holder's by at least `proxy_warming.margin_ms`. If no measured candidate clears the margin, the client routes directly to the best holder — proxy warming is a no-op, never a gamble.
- **Candidate pool.** The bonded nodes in the client's peer table ([ADR 001 § Node Discovery](001-network.md#adr-001-network-topology-and-peer-mesh)), excluding the distant holders, filtered to reputation ≥ the client's minimum-reputation floor ([ADR 001 § Minimum-reputation rejection floor](001-network.md#node-selection-algorithm)).
- **Ranking key is measured RTT only.** Self-attested `region` ([ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation)) is not consulted. Round-trip time is ground truth and cannot be forged: a node that misreports its region to appear local simply exhibits a high measured RTT and is never selected as a proxy. The region-spoofing surface is therefore irrelevant to this mechanism by construction.
- **RTT source.** The client's cumulative probe history. Every normal cache-miss probe ([ADR 001 § Probe response collection](001-network.md#adr-001-network-topology-and-peer-mesh)) samples RTT to whatever nodes it probes; across many lookups the client accumulates RTT coverage of a broad swath of the bonded set, including nearby nodes probed for unrelated content. No new measurement traffic is introduced. A warming request itself yields a fresh RTT sample for the chosen node, refining future selection.
- **One proxy, not a fan-out.** The client routes the request to the single best candidate. Only one regional copy is needed; fanning out across several near nodes multiplies the cold-pull cost without improving the outcome.
- **Fallback.** If the chosen proxy declines or fails to make progress within `proxy_warming.max_wait_ms`, the client falls back to the next-best candidate and then to the direct holder. The fallback is transparent to the caller — no error is surfaced for a proxy that declines.

Proxy warming defaults on (`proxy_warming.enabled = true`) and is fully disableable. Because selection is entirely client-side and rides existing probe data, the only client-observable cost is a bounded one-request latency premium the first time a locale warms a given blob.

### Node serving: chunk-paced pull-through

A node that receives a `cdn/client/v1` `StreamRequest` for a blob (or byte range) it does not hold fills it by pull-through, paced by incoming payment:

- The node pulls the first chunk (`pull_chunk_bytes`, default ~1 MB) from an upstream provider discovered via the normal `cdn/dht/v1` path, verifies it against the root hash, serves it to the client, and collects the first voucher.
- Thereafter, on receipt of voucher `N`, the node pulls chunk `N+1`. The node is never more than one chunk ahead of cleared payment, so its speculative exposure on a request the client abandons is bounded to one chunk's upstream cost — not the whole blob.
- Pulled chunks are written to the local store as they arrive. iroh-blobs partial blobs and bao verified-streaming make a partially-held blob first-class: the node verifies and serves any chunk range against the root hash and records which ranges it holds. A node accumulates `H` across requests until it holds the blob in full.

Because per-request speculative exposure is one chunk rather than the entire blob, implicit pull-through is cheap enough that **the `StreamRequest` itself is the trigger** — there is no warming flag, no `allow_pull_through` field, and no accept/decline handshake. A request for content the node lacks is the demand signal. This also makes the probe-to-evict race benign: a node that advertised `H`, was selected, then evicted `H` before the request arrives re-pulls a single chunk and continues serving, rather than erroring.

The node's own upstream pull is an ordinary cache-miss pull against actual holders, so a proxy never chains its pull through another non-holding proxy.

### Seed-leech caps

Speculative pull-through is governed by two composing caps. The node refuses to begin or continue a speculative pull (one where it does not already hold the requested range) when either cap is breached; it never refuses to serve a range it already holds.

- **Global unrecouped-leech budget.** The node maintains a rolling node-wide counter of `(bytes pulled to satisfy cache misses) − (bytes served)`. When the counter exceeds `max_unrecouped_leech_bytes`, speculative pull-through pauses and resumes as the node serves bytes and recoups. This bounds the operator's aggregate speculative loss and absorbs distributed abuse — many sources each requesting one unpopular hash — in aggregate, independent of how the requests are distributed across peers.
- **Per-peer share ratio.** The node will not pull more than `share_ratio ×` the bytes it has already served *to that requesting peer*, with a one-chunk initial allowance so a peer with no service history can still be served the first chunk. This bounds concentrated abuse — a single peer attempting to drive the node into speculative pulls for content no real client wants — and mirrors a BitTorrent share ratio.

Both caps are operator-policy parameters. The optional `require_authorized_origin` gate and prefetch budget already specified for speculative acquisition in [ADR 022 § Recommended configuration](022-content-discovery.md#popularity-signals-and-market-dynamics) compose with these caps where an operator wants the additional restriction; the seed-leech caps are the load-bearing defense specific to this mechanism.

### Loop closure

Pull-through populates the node's cache, which fires the existing DHT STORE on blob commit ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish)). The node becomes a discoverable holder for `H`; the next regional FIND_VALUE returns it, the normal probe measures its low RTT, and the unified selection score selects it outright. Proxy warming stops engaging for `H` in that locale once a copy exists.

For partial or range-access patterns where the node never holds `H` in full — and therefore never publishes a whole-blob STORE — locality still improves: subsequent warming clients route to the same nearest node by RTT and are served from its cached ranges directly. Loop closure does not depend on the STORE firing.

### DHT advertising stays whole-blob

A node publishes a DHT STORE for `H` only when it holds the blob in full. Discovery remains hash-level; no byte-range availability is added to `cdn/dht/v1`. Range-addressed discovery — advertising "I hold bytes `[a, b)` of `H`" so probers can compose a blob from multiple partial holders — is a larger change to the discovery surface and is deferred. The RTT-routing fallback above means range-access locality improves without it.

### Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `proxy_warming.enabled` | `true` | Master switch for the client-side proxy preference. |
| `proxy_warming.rtt_threshold_ms` | operator/client-set | A best-holder RTT above this marks the holders as distant enough to warrant warming. |
| `proxy_warming.margin_ms` | operator/client-set | A candidate proxy must beat the best holder's RTT by at least this margin to be chosen. |
| `proxy_warming.max_wait_ms` | operator/client-set | Progress deadline before falling back from a proxy to the next candidate or the direct holder. |
| `pull_chunk_bytes` | ~1 MB | Pull/voucher granularity; bounds per-request speculative exposure to one chunk. |
| `max_unrecouped_leech_bytes` | operator-set, finite | Global circuit breaker on aggregate speculative pull spend. |
| `share_ratio` | operator-set | Per-peer ceiling on pulled-vs-served bytes; one-chunk initial allowance. |

Concrete defaults for the latency and budget parameters are modeled before locking; the load-bearing commitments are that `pull_chunk_bytes` is small relative to typical blob size, that `max_unrecouped_leech_bytes` is finite, and that `share_ratio` is bounded.

## Consequences

### Positive

- Closes the regional-locality cold-start trap with no new wire surface, no new gossip message type, and no demand-broadcast layer — the warming request is ordinary paid delivery.
- Region self-attestation ([ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation)) is sidestepped entirely: proxy selection ranks by measured RTT, so a misdeclared region cannot attract warming traffic.
- Chunk-paced pull-through bounds speculative loss for *every* cache-miss serve, not only warming requests — it is independently valuable to the node-to-node delivery path.
- The self-reinforcing DHT + selection-score loop is unchanged; this mechanism only seeds it, then disengages.
- Costs fall where the benefit lands: the first client in a locale absorbs a bounded one-request latency premium, and every subsequent local client is served from a warm nearby copy.

### Negative

- The first client to warm a locale for a given blob pays a latency premium (one extra hop plus the cold first-chunk pull) relative to going direct. The premium is bounded by `proxy_warming.max_wait_ms` and the fallback path.
- A warmed node may hold `H` only partially under range-access patterns and never publish a whole-blob STORE, so such copies are discoverable only via RTT routing, not via the normal FIND_VALUE path. Range-addressed discovery is deferred.
- Speculative pull-through that is throttled by the global budget or per-peer ratio leaves some warming opportunities unserved during recovery windows; this is the intended trade against operator loss.

### Risks

- **Manufactured-demand routing.** A client can route warming requests toward content it chooses, forcing nodes into speculative pulls. Bounded by the global unrecouped-leech budget (aggregate, distribution-independent) and the per-peer share ratio (concentrated, single-source), and by the fact that the node is paid for bytes it serves. Not free; self-limiting.
- **Parameter drift.** A `max_unrecouped_leech_bytes` set too high, or a `share_ratio` set too loose, widens the speculative-loss and abuse surface; set too tight, warming rarely engages and locality does not improve. The defaults are modeled before locking and are operator-tunable.
- **RTT-map coldness.** A client with sparse RTT history may not yet know its genuinely-nearest node and may warm a sub-optimal but still-closer proxy. This is self-correcting: each request adds RTT samples, and any copy closer than the distant holder still improves locality and feeds the discovery loop.

## Cross-ADR Impact

- [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm): selection gains a client-side proxy-warming pre-step that may route a request to a measured-nearby non-holder; the unified selection score and probe path are otherwise unchanged.
- [ADR 005 § Wire Protocol](005-protocol.md#adr-005-wire-protocol): the `cdn/client/v1` node handler fills a request for an unheld blob/range by chunk-paced pull-through paced on vouchers; no message-format change. The handler does not yet exist (its bring-up is tracked separately); this ADR is the design it implements.
- [ADR 012 § Client Architecture](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): clients implement the latency-driven proxy-preference policy and maintain the cumulative RTT map used to rank candidates.
- [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale): this mechanism is the locality complement to the keyspace-routed demand signals; it reuses the STORE-on-commit loop for closure and composes with the prefetch-side `require_authorized_origin` gate and budget where configured. The keyspace demand signals and prefetch behavior are unchanged.
- [ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation): proxy selection deliberately uses measured RTT rather than the self-attested region field, so this mechanism neither relies on nor strengthens region attestation.

## Acceptance Criteria

1. A client whose probed holders all exceed `proxy_warming.rtt_threshold_ms`, and whose RTT map contains a bonded node beating the best holder by `proxy_warming.margin_ms`, routes its `StreamRequest` to that nearer non-holder; with no qualifying candidate it routes directly to the best holder.
2. Proxy candidate ranking is invariant to peers' self-attested `region` values — selection depends only on measured RTT and the reputation floor.
3. A node filling a request for an unheld blob pulls the first chunk to serve, then pulls chunk `N+1` only after voucher `N`; on client abandonment its unrecouped speculative spend is at most `pull_chunk_bytes` of upstream cost.
4. Speculative pull-through pauses when the global unrecouped-leech counter exceeds `max_unrecouped_leech_bytes` and resumes after the node recoups; it pauses for a peer that has exceeded its `share_ratio` (beyond the one-chunk initial allowance) while continuing to serve ranges already held.
5. After a warming serve completes the blob, the node publishes a DHT STORE for `H`, and a subsequent regional FIND_VALUE returns the node as a holder.
6. A proxy that declines or misses `proxy_warming.max_wait_ms` causes the client to fall back to the next candidate and then the direct holder, with no error surfaced to the caller; the client records the observed RTT regardless of outcome.
7. A node's upstream pull for a warming request targets actual holders via the normal `cdn/dht/v1` path and is not itself routed through another non-holding proxy.
