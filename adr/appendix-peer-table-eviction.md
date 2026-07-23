# Appendix: Peer-Table Eviction Policy

> **This is an appendix, not a core protocol ADR.** Peer-table eviction is a local implementation choice — two nodes running different TTLs or admission policies still interoperate so long as they satisfy the gossip-validation rules in [ADR 001 § Gossip validation](001-network.md#gossip-validation). This appendix codifies the recommended approach (default 600 s TTL on `last_seen_us`, `NodeDeregistered` / `NodeAutoEjected`-driven active eviction, optional `gossip.max_peer_entries` ceiling, observability metrics). Alternative implementations are acceptable.

## Context

[ADR 001 § Node Discovery (Gossip)](001-network.md#node-discovery-gossip) defines the peer table (`NodeId → NodeAnnounce`) and the five gossip-validation rules that gate insertion (signature, on-chain stake, ±60 s clock skew, monotonic `timestamp_us`, region format). It does **not** specify when entries leave the table. This appendix resolves the missing decisions:

1. **Size cap and eviction order** — should a long-running node bound the table, and if so how is the victim chosen (LRU? oldest-timestamp? lowest-reputation?)
2. **Age-based expiry** — independent of size pressure, when does a quiet entry get removed
3. **Offline handling** — when does a missing-from-gossip peer get evicted vs. just marked stale
4. **Registry-cache interaction** — does deregistration trigger immediate removal, or wait for natural expiry

The current implementation in `crates/gossip/src/peer_table.rs` already evicts by TTL on `last_seen_us` (default 600 s, sweep cadence 30 s — `crates/gossip/src/service.rs:430–446`, default `DEFAULT_PEER_TTL_SEC = 600` at `crates/common/src/config/mod.rs:49`). Reputation interaction, registry-cache interaction, and size-cap policy are unimplemented and unspecified. This appendix codifies the existing TTL behavior and resolves the four questions above.

## Decision

The peer table is **TTL-bounded by `last_seen_us`**, with **active eviction** on registry deregistration / origin blacklisting and **no hard size cap by default**. Reputation does not factor into eviction. The five gossip-validation rules in [ADR 001 § Gossip validation](001-network.md#gossip-validation) are unchanged; this appendix specifies what happens *after* a successful insert.

### Lifecycle and TTL

Each `PeerEntry` carries `first_seen_us` (insertion time) and `last_seen_us` (last refresh). A periodic sweeper removes entries whose `last_seen_us < now_us - peer_ttl_us`.

| Parameter | Value | Source |
|---|---|---|
| Default TTL | 600 s (= 10× announce interval) | `DEFAULT_PEER_TTL_SEC` in `crates/common/src/config/mod.rs` |
| Configuration key | `gossip.peer_ttl_sec` | resolved in `resolve_gossip` (`crates/common/src/config/mod.rs`), validated `> 0` |
| Sweep cadence | 30 s (currently hardcoded in `ttl_sweeper_task`, not exposed in config) | `crates/gossip/src/service.rs` |
| Eviction key | `last_seen_us` (refresh wall-clock, not announce timestamp) | `PeerTable::evict_expired` in `crates/gossip/src/peer_table.rs` |

The 10× ratio (not the absolute 600 s) is the stable invariant: if the announce interval is later raised or lowered, the recommended TTL moves with it.

**Minimum sane TTL.** Operators MAY tune `peer_ttl_sec` but SHOULD keep it ≥ 2× the announce interval. Below that bound a single dropped announce can evict a healthy peer; PlumTree gossip is best-effort and transient drops occur. The PoC default (10×) tolerates ~9–10 minutes of silence before eviction — comfortably above realistic loss rates at PoC scale (tens of nodes).

**Why `last_seen_us` and not `announce.timestamp_us`.** The receiver's wall clock decides staleness. A peer whose clock drifts forward briefly gets no extra TTL credit; one drifting backward briefly is not evicted early. This matches the ±60 s skew window enforced at insert: drift outside it is silently rejected at insert, drift inside it produces no eviction surprises.

**Sweep is not real-time.** Up to one sweep period (≤ 30 s) elapses between TTL expiry and physical removal. Callers iterating the table MUST tolerate brief over-counting. Selection (probe + DHT) is unaffected — it never consults the peer table.

### No hard size cap (optional safety ceiling)

The peer table is **not size-capped by default**. Growth is bounded externally:

- Gossip-validation rule (2) rejects every `NodeAnnounce` whose `node_id` is not active in the on-chain `CapacityBond`. The insertable `node_id` set is exactly the active bonded operator set.
- Memory cost is small even at production scale: a `NodeAnnounce` worst case is 800 B per [ADR 001 § Gossip Bandwidth Analysis](001-network.md#gossip-bandwidth-analysis), so `≤ 1 KB/entry × 10,000 active nodes ≲ 10 MB` including HashMap overhead. At PoC scale the table is ~tens of KB.
- An LRU/age/reputation-priority layer would solve a problem the CapacityBond registry gate already constrains.

For defense-in-depth against an unforeseen growth path (registry-validation regression, future schema change), operators MAY set an optional ceiling:

- **Config key:** `gossip.max_peer_entries` — `Option<usize>`, default `None` (unlimited).
- **When set and exceeded:** new inserts are rejected; the failure surfaces in the existing gossip-rejection counter `decdn_gossip_messages_rejected_total{reason=table_full}` (per [appendix-observability.md § Gossip Metrics](appendix-observability.md#gossip-metrics)). **No existing entry is evicted to make room** — eviction-by-priority would conflate discovery with selection trust (see [§ Reputation does not factor into eviction](#reputation-does-not-factor-into-eviction)) and is rejected in *Alternatives Considered*.
- **Operator signal:** sustained `decdn_peer_table_size > registered_node_count × 1.5` indicates registry validation is not constraining inserts as expected and warrants investigation, not silent eviction.

Implementing `gossip.max_peer_entries` is OPTIONAL for the PoC (`peer_table_size` already covers the observable signal); the config key is reserved here so a follow-up implementation needs no ADR amendment.

### Registry-cache interaction (active eviction)

The local registry cache, when implemented per [ADR 001 § Registry cache](001-network.md#registry-cache) and [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry), subscribes to `NodeRegistered`, `NodeDeregistered`, and `NodeAutoEjected` events. On `NodeDeregistered` and `NodeAutoEjected` for a `node_id`, the subscriber MUST also **remove the matching peer-table entry** in the same handler, alongside its registry-cache update. Origin blacklisting ([ADR 011 § Hash Evasion and Origin Blacklisting](011-content-takedown.md#hash-evasion-and-origin-blacklisting)) routes through `CapacityBond.ejectNode` and emits `NodeAutoEjected`, so the same code path covers it. The subscriber does not yet exist; this clause adds one behavior on top of the subscriber introduced by [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow).

**Rationale.**

- The subscriber runs on every relevant event anyway; one extra map removal per event is negligible.
- Without it, a deregistered or blacklisted node stays visible in admin RPC `peers_list()` and gossip-derived analytics for up to TTL (default 10 minutes), creating misleading operator output.
- Selection is unaffected either way (probe + DHT consult the registry directly), so this is purely a discovery/observability cleanup.
- It is symmetric with rule (2): that rule blocks new inserts of deregistered nodes; this clause removes existing entries.

`NodeRegistered` does NOT trigger any peer-table action — the entry will arrive (if ever) via gossip, validated normally.

### Reputation does not factor into eviction

Reputation governs *selection* (the `selection_score` formula in [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm), fed by the local reputation score in [ADR 008](008-reputation.md#adr-008-reputation-system)), not retention. A peer whose reputation falls to the 0.1 floor stays in the peer table until TTL or deregistration removes it. The selection-score formula already makes such a node ~100× less likely to be selected (see [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) reputation table), the appropriate response.

**Why not reputation-priority eviction.**

- It conflates discovery and selection. The peer table is a discovery surface; selection is the trust-weighted decision built on top of it.
- Reputation is noisy in the tail; a transient bad-luck dip should not erase a peer from discovery.

This appendix therefore excludes reputation from the eviction decision. Reputation-system changes ([ADR 008](008-reputation.md#adr-008-reputation-system)) need not consider peer-table side effects.

### Offline handling

**Peer is offline (this node is online).** Receiver stops getting valid `NodeAnnounce` messages for the peer. After ≥ TTL of silence, `evict_expired` removes the entry; a later valid `NodeAnnounce` re-inserts a fresh entry with new `first_seen_us` / `last_seen_us`. There is no "stale" intermediate state — eviction is binary and the receiver does not surface or act on partial freshness.

**Implicit ~60 s replay window for not-yet-tabled peers.** Replay protection is asymmetric across the table boundary. For a peer with **no current entry** — never seen, or already TTL-evicted — the only guard is gossip-validation rule (3): the announce `timestamp_us` must be within ±60 s of the receiver's clock ([ADR 001 § Gossip validation](001-network.md#gossip-validation)). A captured `NodeAnnounce` can therefore be replayed against such a node for up to ~60 s after it was signed; the replay merely (re-)inserts the peer's own announce, so the only effect is a slightly stale `last_seen_us`. For a peer **already in the table**, rule (4) adds strict `timestamp_us` monotonicity, which rejects any replay of an earlier announce outright. The window is a deliberate consequence of using clock-skew tolerance (not per-peer state) to admit first-contact announces; it is bounded and benign because the announce is self-authenticating and carries no instruction beyond "I exist."

**This node is offline.** While the process is offline, no announces arrive and no sweeps run. On resume the TTL sweep and bootstrap paths together restore correct state with no special-case handling: the next sweep evicts every entry whose `last_seen_us` is now older than TTL (for an outage longer than TTL the table effectively empties), then the node re-bootstraps from the on-chain registry per [ADR 019 § Phase 3](019-node-onboarding.md#phase-3--node-startup-state-synchronization) (active-node fetch + event subscription) and re-converges via gossip per [ADR 019 § Step 4.3](019-node-onboarding.md#step-43--observe-incoming-nodeannounce-messages) (≥ 1 announce-interval to re-converge).

**Process restart.** The peer table is in-memory only; restarts start empty. This is intentional — persistence would save < one announce interval (~60 s) of cold gossip and add durability machinery the protocol does not need.

### Observability

Naming follows [appendix-observability.md § Gossip Metrics](appendix-observability.md#gossip-metrics). The existing `decdn_peer_table_size` gauge and `decdn_gossip_messages_rejected_total` counter cover most of the surface; this appendix adds two eviction counters and one label value:

| Metric | Type | Description |
|---|---|---|
| `decdn_peer_table_size` | gauge | Distinct peers in the local peer table — **existing**, see appendix |
| `decdn_gossip_messages_rejected_total` | counter, labeled by `reason` | **Existing**, see appendix; [§ No hard size cap (optional safety ceiling)](#no-hard-size-cap-optional-safety-ceiling) specifies when the `reason=table_full` label value fires (the optional `gossip.max_peer_entries` rejection path) |
| `decdn_peer_table_evicted_ttl_total` | counter, unlabeled | New: entries removed by the TTL sweeper |
| `decdn_peer_table_evicted_registry_total` | counter, labeled by `reason ∈ {deregistered, ejected}` | New: entries removed in response to a registry event ([§ Registry-cache interaction (active eviction)](#registry-cache-interaction-active-eviction)) |

A sustained non-zero `decdn_gossip_messages_rejected_total{reason="table_full"}` rate signals that `gossip.max_peer_entries` is misconfigured or that registry validation is letting through an unexpected number of `node_id`s.

## Consequences

### Positive

- Memory bound is set by external policy (the staking registry) without per-table bookkeeping. At realistic scales (≤ 10 k nodes) the table is < 5 MB.
- Active eviction on deregistration / blacklisting keeps admin output and gossip-derived analytics accurate within one event-handler turn instead of ~10 minutes.
- Discovery and selection stay separable. Reputation, blacklist, and registry inputs each have a single clear role; no new coupling.
- The implementation already matches [§ Lifecycle and TTL](#lifecycle-and-ttl) — no code change ships this appendix's TTL behavior. [§ Registry-cache interaction (active eviction)](#registry-cache-interaction-active-eviction) adds a single map-removal step inside the registry-cache subscriber introduced by [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry) (subscriber not yet implemented). [§ Observability](#observability) adds two counters and one label value. [§ No hard size cap (optional safety ceiling)](#no-hard-size-cap-optional-safety-ceiling)'s `max_peer_entries` is opt-in.

### Negative

- TTL 600 s on a 60 s announce interval means an offline peer stays discoverable but unreachable for up to ~10 minutes. Probe + DHT route around it (the peer table is not consulted during selection); the `EvictedSinceProbe` handling in [ADR 001 § Content Discovery](001-network.md#content-discovery-dht--probe) covers the corresponding blob-eviction case. Operators concerned about stale visibility can lower `peer_ttl_sec` toward the [§ Lifecycle and TTL](#lifecycle-and-ttl) minimum.
- Without `max_peer_entries` set, a registry-validation regression admitting unstaked `node_id`s could allow unbounded growth. `decdn_peer_table_size` is the early-warning signal; operators tracking it can set the ceiling reactively.
- Up to one sweep period (≤ 30 s) elapses between TTL expiry and physical removal. Callers iterating the table MUST tolerate brief over-counting. No iterator currently relies on real-time accuracy.
- Active registry-driven eviction adds one `HashMap::remove` per `NodeDeregistered` / `NodeAutoEjected` event. These events are infrequent (operator-initiated or auto-ejection at 50 % stake floor); cost is negligible.
