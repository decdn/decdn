# ADR 039: Multi-Source Parallel Fetch Scheduling on `cdn/client/v1`

**Date:** 2026-06-15
**Status:** Accepted

## Context

[ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1) makes any byte range of a blob verifiable against the BLAKE3 content-hash root on its own. A lying source corrupts only the range it served. This property is the safety precondition for fetching one blob from **several sources at once**. That ADR stops at verification and defers the orchestration. This ADR specifies that orchestration.

A client can fetch a blob over one `cdn/client/v1` stream to one node, sequentially from `byte_offset` to the end ([ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol)). Single-source throughput is bounded by one peer's upload bandwidth and one path's latency, even when many nodes hold the blob. Large-blob delivery is the first product wedge (AI-model distribution), so a scheduler that saturates the client's downlink from several holders at once is load-bearing, not speculative.

The scheduler owns the unsolved problem: which bytes each source fetches, how work reassigns as sources finish or fail, and what bounds the tail. The objective is not "use N sources" — it is to saturate the client's download capacity from the cheapest adequate source set. A single fast, nearby holder that already fills the downlink is a correct one-source fetch.

Two constraints shape the design:

- **Discovery is range-keyed.** A node publishes a `cdn/dht/v1` STORE once it holds ≥1 verified block of a blob, carrying a coarse 64 MiB coverage bitmap ([ADR 022 § Range-keyed partial-holder discovery](022-content-discovery.md#range-keyed-partial-holder-discovery)). So a source may be a **full holder** that serves any range or a **partial holder** that serves only the blocks it covers. Warmed fragments ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)) are discoverable supply, not dark. Composing across partial holders is the point: it spreads serve load and revenue over the holder population rather than funnelling onto the full-holder set.
- **Every byte is paid, and every request is bounded.** `StreamRequest.byte_len` already bounds a request to the half-open range `[byte_offset, byte_offset + byte_len)`, with `byte_len == 0` meaning "to end-of-blob" ([ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol)). The node bills exactly the aligned span it delivers. So a segment is a hard, self-bounding unit on the wire: the client never depends on stopping payment to end a request, and the node never streams past the boundary it was asked for.

Each source serves the assigned blocks it holds from its own store; a block no admitted holder covers is warmed once through a single node's origin chain. So parallel fetch reduces to partitioning `[0, total_bytes)` across holders **by coverage**, verifying each part against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)), and reassembling — plus the failure handling that any untrusted-multi-peer transfer requires. This is the BitTorrent model with a Merkle-verified content address in place of a piece-hash list, and aria2/IDM-style dynamic segmentation in place of a fixed-size work-unit queue, since every byte here carries a cash cost that a free-bandwidth peer-to-peer system does not.

## Decision

A client fetching a blob above the multi-source size floor runs a **multi-source scheduler**: it admits a set of full holders and **eagerly fans out** to all of them at once, opening every admitted source's stream up front rather than growing the set gradually. It segments the byte range dynamically across the fanned-out sources, verifies each segment against the content-hash root, and reassigns the unfinished remainder of a stalled or failed source's range to a different source. Each source is driven by an ordinary bounded `cdn/client/v1` stream. The mechanism adds **no new wire surface**: it is a client-side orchestration policy over the existing protocol, and the bounded `byte_len` request is the range-bounding mechanism it relies on.

### Engagement gate

The scheduler engages only when it pays for itself:

```
multi_source.enabled AND total_bytes > multi_source_min_bytes AND admissible_holders >= 2
```

Otherwise the client uses the existing single-source path unchanged. The gate keeps coordination overhead off small fetches, where one fast source is already optimal.

### Source set and selection

The two consumers of the scheduler ([§ Node pull leg and the demand window](#node-pull-leg-and-the-demand-window)) build and rank the candidate set on different paths.

- **Client.** The candidate set is the bonded set the client already knows. The client issues no `cdn/dht/v1` lookup. It builds the set on one of three tiers. When its persisted peer store ([ADR 037 § Client RTT map](037-regional-proxy-warming.md#client-rtt-map-and-latency-discovery)) holds enough fresh, unsuppressed records, it takes them as they are and skips the probe round. When the store holds enough identity-fresh records, it re-probes those. Otherwise it reads the `CapacityBond` registry ([ADR 001 § Node Discovery](001-network.md#node-discovery-registry)). On the two probed tiers the client shuffles the candidates, then shortlists them by its own region and its configured region allowlist. Registry order and store order are not selection inputs. The client widens to the unfiltered set when the allowlist leaves too few. The shortlist decides which nodes the client probes; it is not a rank key. The client probes each shortlisted candidate for live availability, RTT, and **fresh** coverage ([ADR 005 § `cdn/probe/v1`](005-protocol.md#cdnprobev1--latency-probe)). The failover order is measured RTT only ([ADR 037 § Client selection policy](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)), from the probe or from the stored latency EWMA. Price is not a rank key: the client pays the signed `StreamResponse.rate_per_mb` it receives, and refuses a quote above its optional hard ceiling (`--max-rate-per-mb`, off by default). Reputation does not enter: the client keeps no reputation score ([§ Source diversity and per-peer memory](#source-diversity-and-per-peer-memory)).
- **Node pull leg.** The candidate set is the holders that `cdn/dht/v1` FIND_VALUE returns for the hash, each with a coarse coverage bitmap ([ADR 022 § FIND_VALUE Flow](022-content-discovery.md#find_value-flow-cache-miss--dht-lookup)) — full holders and partial holders alike. The node probes each for live availability, RTT, `rate_per_mb`, and its **fresh** coverage, and ranks by the unified selection score ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm)) over rate, RTT, and its local reputation ([ADR 008](008-reputation.md#adr-008-reputation-system)).

On both paths the scheduler admits up to `max_sources` from the ranked list, and admits **at most one node per operator** ([§ Source diversity and per-peer memory](#source-diversity-and-per-peer-memory)). This is a payment-correctness rule, not a preference: a voucher lane is keyed on `(signer, provider)`, and `provider` is the operator address, so two nodes of one operator are two concurrent voucher streams on ONE watermark. The admitted set therefore shrinks below `max_sources` rather than repeat an operator. A blob with fewer than two admissible holders, or whose unfetched range is smaller than one segment, uses single-source delivery.

### Assignment from coverage

The scheduler assigns each block by fresh probe coverage: a block goes to an admitted holder that covers it — a **cache-hit serve**, fast and paid to a holder that already has the bytes. This is where the spread comes from — disjoint blocks of one blob source from different holders, so revenue distributes across the population. A block **no admitted holder covers** is a genuine gap: the client has no origin access, so it is assigned to one node that warms it through its origin chain ([ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through)). The concurrency invariant that at most one source owns any range at a time keeps each gap warmed exactly once, and the warmer's new coverage becomes discoverable supply for the next fetch. Coverage confines warming to the exception (uncovered blocks); the partial-holder population absorbs the rest. A stale-coverage miss — a holder that evicted a block between probe and request — self-heals through that holder's own warm-on-miss path and, on a hard fault, through the reassign-only tail below.

### Dynamic segmentation and tail-stealing

The client computes the gap set — the bytes it does not already hold and verify. On a fresh fetch this is the whole requested range; on a resume it is only the true gaps. It splits the gap set into large, bao-group-aligned contiguous **segments**, one per admitted source, and drives each through a bounded `StreamRequest`.

When a source finishes its segment, it does not idle: the scheduler finds the source with the largest remaining un-fetched range, splits that range in half on a bao chunk-group boundary, and hands the second half to the freed source as a new bounded request. It skips the split, and lets the range drain on its current stream, when the remaining range is below `min_split_size`. The freed source then waits. It does not leave the set. A source that left could not take over the range a peer re-queues a moment later, which strands recoverable work at a healthy, already-paid lane. It leaves only when no peer holds work and the queue is empty. This is the aria2 dynamic-segmentation pattern: assignment restarts only on a completion or a steal event, never on a fixed schedule, and load balances toward whichever source is currently fastest without the client ever measuring peer bandwidth up front.

**Concurrency invariants:**

- **Exactly one outstanding segment per source (`per_source_inflight = 1`, fixed, not configurable).** This is a correctness constraint, not a simplification: two concurrent segments to one node share its `(signer, provider)` payment lane and its single cumulative voucher watermark. A fast segment advances that watermark ahead of a slow co-segment, so the node cannot attribute the slow segment's payment and stalls it. The scheduler keeps one segment per source to avoid this; bundle pull bounds same-lane concurrency with `--max-lane-streams` (default 1). Parallelism comes from the number of sources, not from stacking requests on one source.
- Global in-flight equals the count of active sources, bounded by `max_sources`; no separate global concurrency cap exists or is needed.
- At most one source owns any given byte range at a time.

### Reassembly

Verified bytes are admitted into the local partial blob at their offsets. The client's range-aware store tracks held and verified ranges, so out-of-order arrival across sources is first-class. The fetch is complete when the gap set is empty and whole-blob verification passes against the content hash. On completion the client may publish its own STORE per the existing cache-commit loop ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish)). The reassembled blob is byte-identical to a single-source fetch of the same hash.

### Failure handling: reassign-only tail

A source's outstanding range is re-queued and reassigned to a **different** source when that source:

- **Fails verification.** The bao decoder rejects a chunk group against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)). The client signs no voucher for the corrupt range, so the source is paid nothing for it. Each consumer remembers the failure its own way ([§ Source diversity and per-peer memory](#source-diversity-and-per-peer-memory)): a node folds the outcome into its local reputation score ([ADR 008](008-reputation.md#adr-008-reputation-system)); a client reports the lane and its reason in the fetch result and persists nothing for it. Only the *unverified remainder* of its range is reassigned. Corruption is not an on-chain offense: the wire absorbs it through post-verification voucher signing ([ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery)), and the only two slashable offenses are rate manipulation and blacklist violation ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)).
- **Stalls.** No verified progress within `unit_deadline_ms`.
- **Drops or errors.** The stream closes or returns `ok: false`.

Reassignment is range-scoped: only the unfinished part of a range is reassigned, verified bytes already stored are never refetched, and the download never restarts. A source is dropped from the set on its first retryable fault, and MAY be backfilled from the remaining ranked candidates.

The scheduler keeps each dropped source's reason. A fetch that runs out of sources reports what every source did, and carries the last real error as its cause. A count of unfetched bytes alone tells the user nothing they can act on.

**A failed fan-out falls back to single-source failover.** The client re-runs the sequential provider-failover loop over the same candidates ([ADR 037 § Client selection policy](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)). The `.partial` store resumes, so the fallback re-pays for nothing. Two failures do NOT fall back, because no other source can fix either: a shared-pool exhaustion, and what the shared failover classifier rules terminal (a payment-layer voucher rejection, an origin blacklist, or an over-cap blob).

Speculative duplicate requests are out of scope **by decision, not by sequencing**. Racing an outstanding range against idle sources — BitTorrent's final-piece duplication tactic — shortens the tail only by paying for the copies that lose the race; BitTorrent accepts that cost because a leecher has no cost signal, but here every byte is paid, so the tail is bounded by source selection and deadline-based reassignment instead. A capped, opt-in paid hedge — duplicating only the final sub-`min_split_size` tail and cancelling the loser — stays a closed door: it returns only if measured tail latency on real large blobs proves reassignment insufficient.

### Payment

Each source is paid from the client's single pool via node-addressed vouchers on its own `(signer, provider)` lane ([ADR 003 § Payment Model](003-payments.md#adr-003-payment-model)), for verified bytes only. There is **no channel per source**: one deposit backs every source, so the payer holds no per-source deposit and there is no fragmentation to size. The pool must hold enough deposit to cover the value in flight across the whole source set at once; each source stops serving when the pool's remaining balance nears its reserved floor `M` ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)) — the payer-side counterpart of the node's pre-flight deposit guard. Reactive top-up heals a mid-fetch exhaustion, for every lane at once: the client credits the new deposit to every lane, and counts the top-up against ONE fetch-wide budget. Three facts belong to the pool and not to a lane — the deposit, the spend that gates it, and the top-up budget. A per-lane copy of any of them lets N lanes each spend what one pool holds. Each segment is a bounded aligned request, so the node reserves, delivers, and bills exactly that span: no over-delivery, no client overpay, no lost credit window. Each lane is redeemed independently by its node; there is no cross-source settlement, and redeem cost is the node's, not the client's.

### Source diversity and per-peer memory

Each consumer keeps its own per-peer memory of source outcomes.

- **Client.** The client keeps no reputation score and links no reputation code. Its only per-peer memory is the persisted peer store ([ADR 037 § Client RTT map](037-regional-proxy-warming.md#client-rtt-map-and-latency-discovery)): an EWMA of measured latency, the most recent quoted `rate_per_mb` (stored for diagnostics; selection does not read it), and a failure stamp. The stamp keeps the peer out of the store-projected candidate sets for a fixed window; a registry read ignores it. The probe round and the first admitted lane's stream open feed that store. The fetch result reports the scheduler's other per-lane outcomes — stalls, drops, verification failures; the client does not persist them. The client keeps no reputation score because it needs none. Verification and no-pay-on-failure bound what a bad source costs a client to one unpaid range. The failure stamp plus live RTT carry every selection signal a persistent score would add.
- **Node pull leg.** Per-source outcomes feed the node's local reputation EWMA ([ADR 008](008-reputation.md#adr-008-reputation-system)): verified delivery raises a source's score; drops and verification failures lower it.

The scheduler admits at most one node per operator, so no single operator serves the whole blob. Region does not enter admission: the candidate list is already in rank order, and re-ordering it by region would rank by something other than the selection score. This bounds the blast radius of a misbehaving operator to its own reassignable range and avoids a self-inflicted eclipse onto one party.

### Node pull leg and the demand window

The same scheduler drives two consumers over one shared code path: the client fetching a blob, and a **node** assembling a blob on its own cache-miss pull leg. The node case adds one bound. A client fetch runs the gap set to completion — it wants the whole blob now. A node serving a downstream client fills only to meet that client's demand, so its gap set is the sliding window `[served_paid_frontier, frontier + credit_window]` ([ADR 003 § Credit window](003-payments.md#credit-window)), advancing as the client consumes and pays, and no source steals ahead past the window. The node becomes a full holder exactly when the downstream client consumes the whole blob; a client that abandons at half leaves the node holding the demanded prefix. This upholds the network invariant that data is not duplicated without realized demand ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)). Parallelism is therefore demand-proportional: a tight window spans one segment and uses one source; a hot blob's sustained wide window fans blocks across the partial-holder population.

### Parameters

| Parameter | Kind | Default | Purpose |
|-----------|------|---------|---------|
| `multi_source.enabled` | config (bool) | `true` | Operator kill switch for the whole scheduler. |
| `multi_source_min_bytes` | config | 64 MiB | Blob-size floor to engage the scheduler; below it, single-source delivery. Below this floor one fast source already saturates a typical link and coordination is pure overhead. |
| `max_sources` | config | 4 | Cap on concurrently-used holders, and the initial segment count. Enough to saturate typical downlinks without paying for marginal lanes. |
| `min_split_size` | const, bao-group-aligned | 16 MiB | Floor below which an idle source does not split and steal a remaining range, so a tiny tail never triggers a restart. |
| `unit_deadline_ms` | config | 10,000 | No-verified-progress deadline before a source's remaining range is reassigned. Balances stall detection against premature reassignment of a merely-slow source. |

`per_source_inflight` is **fixed at 1** and is not a tunable: it is the lane-collision correctness constraint from [§ Dynamic segmentation and tail-stealing](#dynamic-segmentation-and-tail-stealing). The load-bearing commitments are that `max_sources` is finite, and that `min_split_size` and every split are bao-group-aligned, so each range stays independently verifiable.

## Consequences

### Positive

- Aggregate download throughput scales with the number of admitted sources rather than one peer's upload bandwidth, while deadline-based reassignment prevents a stalled source from delaying completion indefinitely.
- No new wire surface: each source is an ordinary bounded `cdn/client/v1` paid stream. The mechanism is a pure client-side policy.
- A corrupt or vanished source costs only a range reassignment, never a restart — the verified-range property ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)) localizes every failure.
- Verification failures cost the source its payment for that range, and a node lowers the source's local reputation score, so multi-source fetch hardens the network against bad sources rather than merely tolerating them.
- No new node-side abuse surface: a source sees a bounded paid stream identical to today's, so the existing ramped credit window and deposit guard apply unchanged.
- Tail-stealing needs no separate "keep the fastest source hot" controller: the fastest source finishes its segment soonest and so steals the most remaining work, as an emergent property of the assignment rule.

### Negative

- Per-source redemption from the shared pool raises the number of on-chain `redeem` calls for a single large fetch (one lane per source), though the deposit stays unified — there is no per-source channel or deposit spread.
- Tail latency is bounded against stalled and failing sources, not against merely slow ones: `unit_deadline_ms` triggers on absent verified progress, so a source delivering steadily below the set's rate keeps its range and sets the finish time of whatever range it owns at the end.
- Coordination state (the segment map, per-source live-rate tracking, reassignment) is new always-on client complexity that the single-source path does not carry.
- Coverage-driven assignment adds a per-block source pick and a gap set to the scheduler, and the node pull leg carries the demand-window driver on top of the single-source path it replaces.

### Risks

- **Parameter drift.** `max_sources` set too high wastes connections and deposit on marginal throughput. `min_split_size` too small inflates proof overhead and request count near the tail; too large coarsens load-balancing near completion. Defaults are pinned above and are meant to need no operator tuning.
- **Source collusion / eclipse.** A set dominated by one operator concentrates failure and pricing power. One-per-operator admission mitigates this, but the client depends on accurate operator identity in the registry to spread the set.
- **Under-sized pool.** One pool backs all sources, so there is no deposit fragmentation; the residual is that the single deposit must cover the value in flight across all sources at once, each source bounded by the node-side floor `M`. Mitigated by sizing the deposit to the admitted set and reactive top-up rather than over-committing up front ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)).

## Cross-ADR Impact

- [ADR 038 § Scope boundary](038-bao-verified-range-streaming.md#scope-boundary): this ADR is the multi-source scheduler that it names as its deferred follow-up; it consumes the per-range verification property and adds no verification logic of its own.
- [ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol): a client MAY drive several concurrent `cdn/client/v1` streams to distinct nodes for one blob, each a bounded segment obtained with `StreamRequest.byte_len`. The `StreamRequest` / `StreamResponse` / voucher surface is unchanged; the bounded `byte_len` request already carries the range-bounding this ADR needs.
- [ADR 003 § Payment Model](003-payments.md#adr-003-payment-model): per-`(signer, provider)` lane vouchers from one shared pool, each lane redeemed independently; the payer sizes the single deposit to cover the value in flight across the admitted set and pays only for verified delivered bytes.
- [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm): selection becomes set-valued for large blobs — the consumer admits a set of holders rather than choosing one. The node pull leg ranks the set by the unified score; the client ranks it by measured RTT only, per the client carve-out in [ADR 037 § Client selection policy](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference).
- [ADR 008 § Reputation System](008-reputation.md#adr-008-reputation-system): on the node pull leg, per-source verified-delivery, drop, and verification-failure outcomes feed the node's local EWMA. The client keeps no reputation score; its per-peer memory is the peer store of [ADR 037 § Client RTT map](037-regional-proxy-warming.md#client-rtt-map-and-latency-discovery).
- [ADR 037 § Latency-Driven Proxy Warming](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality): complementary modes — proxy warming seeds a regional copy through a near non-holder for locality and is the warm-on-miss path this scheduler routes an uncovered block to; multi-source fetch parallelizes across existing holders for throughput. Both leave the wire unchanged.
- [ADR 022 § Content Discovery](022-content-discovery.md#adr-022--content-discovery-at-scale): on the node pull leg the scheduler consumes range-keyed FIND_VALUE results — providers with coverage — and admits partial holders by coverage; the client's candidates come from its peer store or the registry instead. On both paths the fresh authority is the `cdn/probe/v1` coverage bitmap.

## Acceptance Criteria

1. For a blob above `multi_source_min_bytes` with at least two admissible holders, the client fetches it as dynamically segmented, bao-aligned ranges assigned by coverage across up to `max_sources` holders; below the gate it uses single-source delivery unchanged.
2. Each segment is verified against the content-hash root on receipt; a segment failing verification receives no voucher for the corrupt range and has its unverified remainder reassigned to a different source. A node lowers the failing source's local reputation score; a client names the lane and its reason in the fetch result. No slashing evidence is produced.
3. A source that stalls past `unit_deadline_ms`, drops, or returns `ok: false` has only the unfinished part of its range reassigned; verified bytes already stored are not refetched and the download does not restart. A fan-out that runs out of sources names each one's reason, and the client falls back to single-source failover unless the failure is terminal.
4. Each source is driven by an ordinary bounded `cdn/client/v1` stream (`StreamRequest.byte_len` bounding the range) and paid over its own lane for verified delivered bytes only.
5. Exactly one segment is outstanding per source at a time (`per_source_inflight = 1`, fixed), the admitted source set is bounded by `max_sources` and holds at most one node per operator, and no byte range is outstanding at more than one source at a time.
6. When a source finishes its segment, the scheduler splits the largest remaining range and hands the freed source the new tail, unless that remaining range is below `min_split_size` — in which case the freed source waits for work rather than leaving the set. No duplicate or hedged request is issued.
7. The reassembled blob verifies against the content hash and is byte-identical to a single-source fetch of the same hash.
8. The scheduler admits partial holders by their probe coverage and assigns each 64 MiB block to a covering holder; a block no admitted holder covers is warmed exactly once through one node's origin chain, and the warmer's new coverage is discoverable to the next fetch.
9. The same scheduler drives the node's cache-miss pull leg with a sliding demand window `[served_paid_frontier, frontier + credit_window]`: no source steals past the window, the node becomes a full holder only when the downstream client consumes the whole blob, and an abandoned fetch leaves the node holding the demanded prefix.
