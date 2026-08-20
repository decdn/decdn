# ADR 039: Multi-Source Parallel Fetch Scheduling on `cdn/client/v1`

**Date:** 2026-06-15
**Status:** Accepted

## Context

[ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1) makes any byte range of a blob verifiable against the BLAKE3 content-hash root on its own. A lying source corrupts only the range it served. This property is the safety precondition for fetching one blob from **several sources at once**. That ADR stops at verification and defers the orchestration. This ADR specifies that orchestration.

A client can fetch a blob over one `cdn/client/v1` stream to one node, sequentially from `byte_offset` to the end ([ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol)). Single-source throughput is bounded by one peer's upload bandwidth and one path's latency, even when many nodes hold the blob. Large-blob delivery is the first product wedge (AI-model distribution), so a scheduler that saturates the client's downlink from several holders at once is load-bearing, not speculative.

The scheduler owns the unsolved problem: which bytes each source fetches, how work reassigns as sources finish or fail, and what bounds the tail. The objective is not "use N sources" — it is to saturate the client's download capacity from the cheapest adequate source set. A single fast, nearby holder that already fills the downlink is a correct one-source fetch.

Two constraints shape the design:

- **Discovery is hash-level.** A node publishes a `cdn/dht/v1` STORE for a blob only when it holds the blob **in full** ([ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob)); range-addressed availability is deferred. So every discoverable source is a **full holder** that can serve any range. Partial copies (for example warmed fragments under [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)) are not range-discoverable and are out of the source set for this ADR.
- **Every byte is paid, and every request is bounded.** `StreamRequest.byte_len` already bounds a request to the half-open range `[byte_offset, byte_offset + byte_len)`, with `byte_len == 0` meaning "to end-of-blob" ([ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol)). The node bills exactly the aligned span it delivers. So a segment is a hard, self-bounding unit on the wire: the client never depends on stopping payment to end a request, and the node never streams past the boundary it was asked for.

Every source is a full holder that serves from its own store rather than pulling through. So parallel fetch reduces to partitioning `[0, total_bytes)` across full holders, verifying each part against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)), and reassembling — plus the failure handling that any untrusted-multi-peer transfer requires. This is the BitTorrent model with a Merkle-verified content address in place of a piece-hash list, and aria2/IDM-style dynamic segmentation in place of a fixed-size work-unit queue, since every byte here carries a cash cost that a free-bandwidth peer-to-peer system does not.

## Decision

A client fetching a blob above the multi-source size floor runs a **multi-source scheduler**: it admits a set of full holders and **eagerly fans out** to all of them at once, opening every admitted source's stream up front rather than growing the set gradually. It segments the byte range dynamically across the fanned-out sources, verifies each segment against the content-hash root, and reassigns the unfinished remainder of a stalled or failed source's range to a different source. Each source is driven by an ordinary bounded `cdn/client/v1` stream. The mechanism adds **no new wire surface**: it is a client-side orchestration policy over the existing protocol, and the bounded `byte_len` request is the range-bounding mechanism it relies on.

### Engagement gate

The scheduler engages only when it pays for itself:

```
multi_source.enabled AND total_bytes > multi_source_min_bytes AND admissible_holders >= 2
```

Otherwise the client uses the existing single-source path unchanged. The gate keeps coordination overhead off small fetches, where one fast source is already optimal.

### Source set and selection

The candidate set is the full holders that `cdn/dht/v1` FIND_VALUE returns for the hash ([ADR 022 § FIND_VALUE Flow](022-content-discovery.md#find_value-flow-cache-miss--dht-lookup)). The client probes each for live availability, RTT, and `rate_per_mb` ([ADR 005 § `cdn/probe/v1`](005-protocol.md#cdnprobev1--latency-probe)). The client admits up to `max_sources`, ranked by the unified selection score ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm)) over rate, RTT, and reputation. Where candidate metadata allows, it spreads the set across distinct operators and regions ([§ Source diversity and reputation](#source-diversity-and-reputation)). A blob with fewer than two admissible holders, or whose unfetched range is smaller than one segment, uses single-source delivery.

### Dynamic segmentation and tail-stealing

The client computes the gap set — the bytes it does not already hold and verify. On a fresh fetch this is the whole requested range; on a resume it is only the true gaps. It splits the gap set into large, bao-group-aligned contiguous **segments**, one per admitted source, and drives each through a bounded `StreamRequest`.

When a source finishes its segment, it does not idle: the scheduler finds the source with the largest remaining un-fetched range, splits that range in half on a bao chunk-group boundary, and hands the second half to the freed source as a new bounded request. It skips the split, and lets the range drain on its current stream, when the remaining range is below `min_split_size`. This is the aria2 dynamic-segmentation pattern: assignment restarts only on a completion or a steal event, never on a fixed schedule, and load balances toward whichever source is currently fastest without the client ever measuring peer bandwidth up front.

**Concurrency invariants:**

- **Exactly one outstanding segment per source (`per_source_inflight = 1`, fixed, not configurable).** This is a correctness constraint, not a simplification: two concurrent segments to the same node would share one `(signer, provider)` payment lane and reintroduce the concurrent-same-lane voucher hazard that bundle pull serializes with a `provider_lock` — nondeterministic node-side voucher ordering causes `BadSignature`. Parallelism comes from the number of sources, never from stacking requests on one source.
- Global in-flight equals the count of active sources, bounded by `max_sources`; no separate global concurrency cap exists or is needed.
- At most one source owns any given byte range at a time.

### Reassembly

Verified bytes are admitted into the local partial blob at their offsets. The client's range-aware store tracks held and verified ranges, so out-of-order arrival across sources is first-class. The fetch is complete when the gap set is empty and whole-blob verification passes against the content hash. On completion the client may publish its own STORE per the existing cache-commit loop ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish)). The reassembled blob is byte-identical to a single-source fetch of the same hash.

### Failure handling: reassign-only tail

A source's outstanding range is re-queued and reassigned to a **different** source when that source:

- **Fails verification.** The bao decoder rejects a chunk group against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)). The client signs no voucher for the corrupt range, so the source is paid nothing for it. The failure lowers the source's local reputation score ([ADR 008](008-reputation.md#adr-008-reputation-system)), and only the *unverified remainder* of its range is reassigned. Corruption is not an on-chain offense: the wire absorbs it through post-verification voucher signing ([ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery)), and the only two slashable offenses are rate manipulation and blacklist violation ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)).
- **Stalls.** No verified progress within `unit_deadline_ms`.
- **Drops or errors.** The stream closes or returns `ok: false`.

Reassignment is range-scoped: only the unfinished part of a range is reassigned, verified bytes already stored are never refetched, and the download never restarts. A source that accumulates failures is dropped from the set and MAY be backfilled from remaining FIND_VALUE candidates.

Speculative duplicate requests are out of scope **by decision, not by sequencing**. Racing an outstanding range against idle sources — BitTorrent's final-piece duplication tactic — shortens the tail only by paying for the copies that lose the race; BitTorrent accepts that cost because a leecher has no cost signal, but here every byte is paid, so the tail is bounded by source selection and deadline-based reassignment instead. A capped, opt-in paid hedge — duplicating only the final sub-`min_split_size` tail and cancelling the loser — stays a closed door: it returns only if measured tail latency on real large blobs proves reassignment insufficient.

### Payment

Each source is paid from the client's single pool via node-addressed vouchers on its own `(signer, provider)` lane ([ADR 003 § Payment Model](003-payments.md#adr-003-payment-model)), for verified bytes only. There is **no channel per source**: one deposit backs every source, so the payer holds no per-source deposit and there is no fragmentation to size. The pool must hold enough deposit to cover the value in flight across the whole source set at once; each source stops serving when the pool's remaining balance nears its reserved floor `M` ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)) — the payer-side counterpart of the node's pre-flight deposit guard. Reactive top-up heals a mid-fetch exhaustion. Each segment is a bounded aligned request, so the node reserves, delivers, and bills exactly that span: no over-delivery, no client overpay, no lost credit window. Each lane is redeemed independently by its node; there is no cross-source settlement, and redeem cost is the node's, not the client's.

### Source diversity and reputation

Per-source outcomes feed the local reputation EWMA ([ADR 008](008-reputation.md#adr-008-reputation-system)): verified delivery raises a source's score; stalls, drops, and verification failures lower it. Where holder metadata permits, the scheduler prefers a set spread across distinct operators and regions, so that no single operator serves the whole blob. This bounds the blast radius of a misbehaving operator to its own reassignable range and avoids a self-inflicted eclipse onto one party.

### Scope boundary

This ADR schedules across **full holders only**. Composing a blob from **partial holders** — nodes that each hold only some ranges — requires range-addressed discovery (advertising "I hold bytes `[a, b)` of `H`" on `cdn/dht/v1`), which stays deferred per [ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob) and [ADR 038 § Scope boundary](038-bao-verified-range-streaming.md#scope-boundary). When that lands, partial holders become additional sources with no change to the scheduling, verification, or payment logic here — only the candidate-discovery step gains range awareness.

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
- Verification failures cost the source its payment for that range and lower its local reputation score, so multi-source fetch hardens the network against bad sources rather than merely tolerating them.
- No new node-side abuse surface: a source sees a bounded paid stream identical to today's, so the existing ramped credit window and deposit guard apply unchanged.
- Tail-stealing needs no separate "keep the fastest source hot" controller: the fastest source finishes its segment soonest and so steals the most remaining work, as an emergent property of the assignment rule.

### Negative

- Per-source redemption from the shared pool raises the number of on-chain `redeem` calls for a single large fetch (one lane per source), though the deposit stays unified — there is no per-source channel or deposit spread.
- Tail latency is bounded against stalled and failing sources, not against merely slow ones: `unit_deadline_ms` triggers on absent verified progress, so a source delivering steadily below the set's rate keeps its range and sets the finish time of whatever range it owns at the end.
- Coordination state (the segment map, per-source live-rate tracking, reassignment) is new always-on client complexity that the single-source path does not carry.
- Restricting sources to full holders leaves warmed partial copies unused until range-addressed discovery lands.

### Risks

- **Parameter drift.** `max_sources` set too high wastes connections and deposit on marginal throughput. `min_split_size` too small inflates proof overhead and request count near the tail; too large coarsens load-balancing near completion. Defaults are pinned above and are meant to need no operator tuning.
- **Source collusion / eclipse.** A set dominated by one operator concentrates failure and pricing power. The diversity preference mitigates this, but the client depends on accurate holder metadata to spread the set.
- **Under-sized pool.** One pool backs all sources, so there is no deposit fragmentation; the residual is that the single deposit must cover the value in flight across all sources at once, each source bounded by the node-side floor `M`. Mitigated by sizing the deposit to the admitted set and reactive top-up rather than over-committing up front ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)).

## Cross-ADR Impact

- [ADR 038 § Scope boundary](038-bao-verified-range-streaming.md#scope-boundary): this ADR is the multi-source scheduler that it names as its deferred follow-up; it consumes the per-range verification property and adds no verification logic of its own.
- [ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol): a client MAY drive several concurrent `cdn/client/v1` streams to distinct nodes for one blob, each a bounded segment obtained with `StreamRequest.byte_len`. The `StreamRequest` / `StreamResponse` / voucher surface is unchanged; the bounded `byte_len` request already carries the range-bounding this ADR needs.
- [ADR 003 § Payment Model](003-payments.md#adr-003-payment-model): per-`(signer, provider)` lane vouchers from one shared pool, each lane redeemed independently; the payer sizes the single deposit to cover the value in flight across the admitted set and pays only for verified delivered bytes.
- [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm): selection becomes set-valued for large blobs — the client admits and ranks a set of full holders rather than choosing one, using the same RTT, price, reputation, and diversity inputs.
- [ADR 008 § Reputation System](008-reputation.md#adr-008-reputation-system): per-source verified-delivery, stall, drop, and verification-failure outcomes feed the local EWMA and the diversity preference.
- [ADR 037 § Latency-Driven Proxy Warming](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality): complementary modes — proxy warming seeds the first regional copy through a near non-holder for locality; multi-source fetch parallelizes across existing full holders for throughput. A client engages warming when only distant holders exist and a near empty node can seed, and multi-source when enough acceptable full holders exist; both leave the wire unchanged.
- [ADR 022 § Content Discovery](022-content-discovery.md#adr-022--content-discovery-at-scale): the scheduler consumes hash-level FIND_VALUE results; range-addressed discovery for partial-holder composition remains deferred.

## Acceptance Criteria

1. For a blob above `multi_source_min_bytes` with at least two admissible holders, the client fetches it as dynamically segmented, bao-aligned ranges assigned across up to `max_sources` full holders; below the gate it uses single-source delivery unchanged.
2. Each segment is verified against the content-hash root on receipt; a segment failing verification receives no voucher for the corrupt range, has its unverified remainder reassigned to a different source, and lowers the failing source's local reputation score. No slashing evidence is produced.
3. A source that stalls past `unit_deadline_ms`, drops, or returns `ok: false` has only the unfinished part of its range reassigned; verified bytes already stored are not refetched and the download does not restart.
4. Each source is driven by an ordinary bounded `cdn/client/v1` stream (`StreamRequest.byte_len` bounding the range) and paid over its own lane for verified delivered bytes only.
5. Exactly one segment is outstanding per source at a time (`per_source_inflight = 1`, fixed), the admitted source set is bounded by `max_sources`, and no byte range is outstanding at more than one source at a time.
6. When a source finishes its segment, the scheduler splits the largest remaining range and hands the freed source the new tail, unless that remaining range is below `min_split_size`; no duplicate or hedged request is issued.
7. The reassembled blob verifies against the content hash and is byte-identical to a single-source fetch of the same hash.
8. Partial-holder composition and range-addressed discovery are out of scope; the scheduler operates only over full holders returned by FIND_VALUE.
