# ADR 039: Multi-Source Parallel Fetch Scheduling on `cdn/client/v1`

**Date:** 2026-06-15
**Status:** Draft

## Context

[ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1) makes any byte range of a blob verifiable against the BLAKE3 content-hash root on its own. A lying source corrupts only the range it served. This property is the safety precondition for fetching one blob from **several sources at once**. That ADR stops at verification and defers the orchestration. This ADR specifies that orchestration.

Today a client fetches a blob over one `cdn/client/v1` stream to one node, sequentially from `byte_offset` to the end ([ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol)). Throughput is therefore bounded by one peer's upload bandwidth and one path's latency, even when many nodes hold the blob. The scheduler owns the unsolved problem: who fetches which bytes from whom, at what concurrency, and what happens when a source is slow, drops, or serves bad bytes.

Two constraints shape the design:

- **Discovery is hash-level.** A node publishes a `cdn/dht/v1` STORE for a blob only when it holds the blob **in full** ([ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob)); range-addressed availability is deferred. So every discoverable source is a **full holder** that can serve any range. Partial copies (for example warmed fragments under [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)) are not range-discoverable and are out of the source set for this ADR.
- **Delivery is self-enforcing.** Payment and delivery are coupled per stream: the payer stops sending vouchers → the node stops sending chunks, and the reverse ([ADR 005 § Voucher granularity and credit window](005-protocol.md#voucher-granularity-and-credit-window)). To obtain a bounded sub-range, request from its start offset and cease payment at its end. There is no end-offset field on the wire.

Every source is a full holder that serves from its own store rather than pulling through. So parallel fetch reduces to partitioning `[0, total_bytes)` across full holders, verifying each part against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)), and reassembling — plus the failure handling that any untrusted-multi-peer transfer requires. This is the BitTorrent model with a Merkle-verified content address in place of a piece-hash list.

## Decision

A client fetching a blob it expects to be large enough to benefit MAY run a **multi-source scheduler**. The scheduler selects a set of full holders, partitions the blob into bao-aligned work units, assigns units to sources dynamically, verifies each completed unit against the content-hash root, and re-dispatches failed units. Each source is driven by an ordinary `cdn/client/v1` paid stream. The mechanism adds **no new wire surface**: it is a client-side orchestration policy over the existing protocol.

### Source set and selection

The candidate set is the full holders that `cdn/dht/v1` FIND_VALUE returns for the hash ([ADR 022 § FIND_VALUE Flow](022-content-discovery.md#find_value-flow-cache-miss--dht-lookup)). The client probes each for live availability, RTT, and `rate_per_mb` ([ADR 005 § `cdn/probe/v1`](005-protocol.md#cdnprobev1--latency-probe)). The client admits up to `max_sources`, ranked by the unified selection score ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm)) over rate, RTT, and reputation. Where candidate metadata allows, it spreads the set across distinct operators and regions ([§ Source diversity](#source-diversity-and-reputation)). A blob with fewer than two admissible holders falls back to single-source delivery.

### Work-unit partitioning and dynamic assignment

The blob is divided into fixed-size **work units** aligned to bao chunk-group boundaries (`work_unit_bytes`, a small multiple of the 16 KiB group size — for example a few MB), so each unit is independently verifiable. Assignment is **dynamic, not a static split**: units sit in a work queue, and each source is handed the next unit when it has capacity. A faster source completes more units; the client never needs to know peer bandwidth in advance, and a source that slows down stops drawing new units. This work-stealing discipline adapts to heterogeneous and time-varying source speed without re-planning.

Each source runs up to `per_source_inflight` units concurrently, and the scheduler caps total concurrency at `max_inflight_units`. A source serves a unit by a `cdn/client/v1` `StreamRequest` at the unit's start offset; the client accepts `work_unit_bytes` of verified data, ceases vouchers, and closes. The unit's end is enforced by stopping payment, not by a wire bound. Because full holders serve from disk, over-read past the unit boundary is at most the in-flight voucher window, not a speculative whole-blob pull.

### Reassembly

Verified units are written into the local partial blob at their offsets. `iroh-blobs` partial blobs track which ranges are held and verified, so out-of-order arrival is first-class. The blob is complete when every unit is verified and stored. On completion the node may publish its own STORE per the existing cache-commit loop ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish)).

### Failure handling and re-dispatch

A unit is re-queued — and reassigned to a **different** source — when its source:

- **Fails verification.** The bao decoder rejects the unit against the root ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)). The client signs no voucher for that unit, so the source is paid nothing for the corrupt range. The failure lowers the source's local reputation score ([ADR 008](008-reputation.md#adr-008-reputation-system)), and the unit goes to a different source. Corruption is not an on-chain offense: the wire absorbs it through post-verification voucher signing ([ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery)), and the only two slashable offenses are rate manipulation and blacklist violation ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)).
- **Stalls.** No verified progress within `unit_deadline_ms`.
- **Drops or errors.** The stream closes or returns `ok: false`.

Re-dispatch is unit-scoped: only the failed unit is reassigned; verified units already stored are untouched. So a bad or vanished source never restarts the download. A source that accumulates failures is dropped from the set and MAY be backfilled from the remaining FIND_VALUE candidates.

At most one source owns a unit at a time. Because every delivered byte is paid, the scheduler bounds completion latency through source selection and deadline-based re-dispatch rather than speculative duplicate requests. Re-dispatch preempts a source that has stopped making verified progress; a source that keeps delivering, only slowly, holds its unit to completion.

### Payment

Each source is paid from the client's single pool via node-addressed vouchers on its own `(signer, provider)` lane ([ADR 003 § Payment Model](003-payments.md#adr-003-payment-model)), and only for the verified bytes it delivered. There is **no channel per source**: one deposit backs every source, so the payer holds no per-source deposit and there is no fragmentation to size. The pool must hold enough deposit to cover the value in flight across the whole source set at once; each source (a node) stops serving when the pool's remaining balance nears its reserved floor `M` ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)) — the payer-side counterpart of the node's pre-flight deposit guard ([ADR 037 § Implementation status](037-regional-proxy-warming.md#implementation-status)). Sources price independently via `rate_per_mb`. The selection preference for cheaper sources and the work-stealing bias toward faster ones together steer spend. Each lane is redeemed independently — there is no cross-source settlement.

### Engagement gate

Multi-source fetch engages only when it pays for itself: the blob's advertised `total_bytes` exceeds `multi_source_min_bytes` **and** at least two holders are admissible. Otherwise the client uses the single-source path unchanged. The gate keeps coordination overhead off small fetches, where one fast source is already optimal.

### Source diversity and reputation

Per-source outcomes feed the local reputation EWMA ([ADR 008](008-reputation.md#adr-008-reputation-system)): verified delivery raises a source's score; stalls, drops, and verification failures lower it. Where holder metadata permits, the scheduler prefers a set spread across distinct operators and regions, so that no single operator serves all units. This bounds the blast radius of a misbehaving operator to re-dispatchable units and avoids a self-inflicted eclipse onto one party.

### Scope boundary

This ADR schedules across **full holders only**. Composing a blob from **partial holders** — nodes that each hold only some ranges — requires range-addressed discovery (advertising "I hold bytes `[a, b)` of `H`" on `cdn/dht/v1`), which remains deferred per [ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob) and [ADR 038 § Scope boundary](038-bao-verified-range-streaming.md#scope-boundary). When that lands, partial holders become additional sources with no change to the scheduling, verification, or payment logic here — only the candidate-discovery step gains range awareness. An optional bounded-range request field (an end offset on `StreamRequest`) is likewise deferred: it is unnecessary for full holders serving from disk and becomes useful only for pull-through sources, which arrive with partial-holder support.

Speculative duplicate requests are out of scope **by decision, not by sequencing**. Racing an outstanding unit against idle sources — BitTorrent's endgame mode — shortens the tail only by paying for the copies that lose the race. BitTorrent needs it because a leecher has no cost signal; here every byte is paid, so the tail belongs to source selection and re-dispatch. If measured tail latency on large blobs proves that insufficient, the option that returns is a **capped, opt-in hedge** — a small finite duplicate count per outstanding unit once the queue is drained — designed against real data rather than pre-committed here.

### Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `multi_source.enabled` | `true` | Master switch for the client-side scheduler. |
| `multi_source_min_bytes` | client-set | Minimum advertised blob size below which single-source delivery is used. |
| `max_sources` | client-set, finite | Cap on concurrently-used holders for one blob. |
| `work_unit_bytes` | bao-group-aligned, ~few MB | Size of an independently-verifiable assignment unit. |
| `per_source_inflight` | client-set | Units a single source may serve concurrently. |
| `max_inflight_units` | client-set | Global concurrency cap across all sources. |
| `unit_deadline_ms` | client-set | Verified-progress deadline before a unit is re-dispatched. |

Concrete defaults are modeled before locking. The load-bearing commitments are that `max_sources` and `max_inflight_units` are finite (bounding source count and concurrency), and that `work_unit_bytes` is bao-group-aligned (so every unit is independently verifiable).

## Consequences

### Positive

- Aggregate download throughput scales with the number of admitted sources rather than one peer's upload bandwidth, while deadline-based re-dispatch prevents a stalled source from delaying completion indefinitely.
- No new wire surface: each source is an ordinary `cdn/client/v1` paid stream, bounded by the existing voucher backpressure. The mechanism is a pure client-side policy.
- A corrupt or vanished source costs only a unit re-dispatch, never a restart — the verified-range property ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)) localizes every failure.
- Verification failures cost the source its payment for that range and lower its local reputation score, so multi-source fetch hardens the network against bad sources rather than merely tolerating them.
- No new node-side abuse surface: a source sees a bounded paid stream identical to today's, so the existing seed-leech caps and deposit guard ([ADR 037](037-regional-proxy-warming.md#seed-leech-caps)) apply unchanged.

### Negative

- Per-source redemption from the shared pool raises the number of on-chain `redeem` calls for a single large fetch (one lane per source), though the deposit stays unified — there is no per-source channel or deposit spread.
- Tail latency is bounded against stalled and failing sources, not against merely slow ones: `unit_deadline_ms` triggers on absent verified progress, so a source delivering steadily below the set's rate keeps its unit and sets the finish time of the last unit it owns.
- Coordination state (work queue, per-source in-flight tracking, re-dispatch) is new always-on client complexity that the single-source path does not carry.
- Restricting sources to full holders leaves warmed partial copies unused until range-addressed discovery lands.

### Risks

- **Parameter drift.** `max_sources` or `max_inflight_units` set too high wastes connections and deposit on marginal throughput. `work_unit_bytes` too small inflates proof overhead and request count; too large coarsens load-balancing and re-dispatch cost. Defaults are modeled and client-tunable.
- **Source collusion / eclipse.** A set dominated by one operator concentrates failure and pricing power. The diversity preference mitigates this, but the client depends on accurate holder metadata to spread the set.
- **Under-sized pool.** One pool backs all sources, so there is no deposit fragmentation; the residual is that the single deposit must cover the value in flight across all sources at once (each source bounded by the node-side floor `M`). Mitigated by sizing the deposit to the admitted set and topping up rather than over-committing up front ([ADR 003 § Pool solvency and the refundable floor `M`](003-payments.md#pool-solvency-and-the-refundable-floor-m)).

## Cross-ADR Impact

- [ADR 038 § Scope boundary](038-bao-verified-range-streaming.md#scope-boundary): this ADR is the multi-source scheduler that it names as its deferred follow-up; it consumes the per-range verification property and adds no verification logic of its own.
- [ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol): a client MAY drive several concurrent `cdn/client/v1` streams to distinct nodes for one blob, each a bounded sub-range obtained by requesting from an offset and ceasing vouchers at the unit boundary. The `StreamRequest` / `StreamResponse` / voucher surface is unchanged; no end-offset field is added.
- [ADR 003 § Payment Model](003-payments.md#adr-003-payment-model): per-`(signer, provider)` lane vouchers from one shared pool, each lane redeemed independently; the payer sizes the single deposit to the sum of per-source credit windows and pays only for verified delivered bytes.
- [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm): selection becomes set-valued for large blobs — the client admits and ranks a set of full holders rather than choosing one, using the same RTT, price, reputation, and diversity inputs.
- [ADR 008 § Reputation System](008-reputation.md#adr-008-reputation-system): per-source verified-delivery, stall, drop, and verification-failure outcomes feed the local EWMA and the diversity preference.
- [ADR 037 § Latency-Driven Proxy Warming](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality): complementary modes — proxy warming seeds the first regional copy through a near non-holder for locality; multi-source fetch parallelizes across existing full holders for throughput. A client engages warming when only distant holders exist and a near empty node can seed, and multi-source when enough acceptable full holders exist; both leave the wire unchanged.
- [ADR 022 § Content Discovery](022-content-discovery.md#adr-022--content-discovery-at-scale): the scheduler consumes hash-level FIND_VALUE results; range-addressed discovery for partial-holder composition remains deferred.

## Acceptance Criteria

1. For a blob above `multi_source_min_bytes` with at least two admissible holders, the client fetches it as bao-aligned work units assigned dynamically across up to `max_sources` full holders; below the gate it uses single-source delivery unchanged.
2. Each work unit is verified against the content-hash root on receipt; a unit failing verification receives no voucher, is re-queued to a different source, and lowers the failing source's local reputation score. No slashing evidence is produced.
3. A source that stalls past `unit_deadline_ms`, drops, or returns `ok: false` has only its outstanding unit re-dispatched; verified units already stored are not refetched and the download does not restart.
4. Each source is driven by an ordinary `cdn/client/v1` stream and paid over its own channel for verified delivered bytes only; no end-offset field is added to `StreamRequest`, and bounded ranges are enforced by ceasing vouchers at the unit boundary.
5. Concurrency is bounded by `per_source_inflight` and `max_inflight_units`, the admitted source set is bounded by `max_sources`, and no unit is outstanding at more than one source at a time.
6. The reassembled blob verifies against the content hash and is byte-identical to a single-source fetch of the same hash.
7. Partial-holder composition and range-addressed discovery are out of scope; the scheduler operates only over full holders returned by FIND_VALUE.
