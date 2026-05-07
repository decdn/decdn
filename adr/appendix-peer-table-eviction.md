# Appendix: Peer-Table Eviction Policy

> **This is an appendix, not a core protocol ADR.** Peer-table eviction is a local implementation choice — two nodes running different TTLs or admission policies still interoperate so long as they satisfy the gossip-validation rules in [ADR 001 § Gossip validation](001-network.md#gossip-validation). This appendix codifies the recommended TTL-based approach (default 600 s on `last_seen_us`), the `NodeDeregistered` / `NodeAutoEjected`-driven active-eviction step, the optional `gossip.max_peer_entries` safety ceiling, and the observability metrics. Alternative implementations are acceptable.

**Touches:** [ADR 001](001-network.md), [ADR 008](008-reputation.md), [ADR 011](011-content-takedown.md), [ADR 019](019-node-onboarding.md)

## Context

[ADR 001 § Node Discovery (Gossip)](001-network.md#node-discovery-gossip) defines the peer table (`NodeId → NodeAnnounce`) and the five gossip-validation rules that gate insertion (signature, on-chain stake, ±60 s clock skew, monotonic `timestamp_us`, region format). It does **not** specify when entries leave the table. Issue [#401](https://github.com/decdn/decdn/issues/401) (split out from the broader [#190](https://github.com/decdn/decdn/issues/190) gap-10 audit) tracks the missing decisions:

1. **Size cap and eviction order** — should a long-running node bound the table at some maximum, and if so, how is the victim chosen (LRU? oldest-timestamp? lowest-reputation?)
2. **Age-based expiry** — independent of size pressure, when does a quiet entry get removed
3. **Offline handling** — when does a missing-from-gossip peer get evicted vs. just marked stale
4. **Registry-cache interaction** — does deregistration trigger immediate peer-table removal, or wait for natural expiry

The current implementation in `crates/gossip/src/peer_table.rs` already evicts by TTL on `last_seen_us` (default 600 s, sweep cadence 30 s — `crates/gossip/src/service.rs:430–446`, default `DEFAULT_PEER_TTL_SEC = 600` at `crates/common/src/config/mod.rs:49`). Reputation interaction, registry-cache interaction, and a size-cap policy are all unimplemented and unspecified. This ADR codifies the existing TTL behavior and resolves the four open questions above.

## Decision

The peer table is **TTL-bounded by `last_seen_us`**, with **active eviction** on registry deregistration / origin blacklisting and **no hard size cap by default**. Reputation does not factor into eviction. The five gossip-validation rules in [ADR 001 § Gossip validation](001-network.md#gossip-validation) are unchanged; this appendix specifies what happens to entries *after* a successful insert.

### 1. Lifecycle and TTL

Each `PeerEntry` carries `first_seen_us` (insertion time) and `last_seen_us` (last successful refresh). A periodic sweeper removes entries whose `last_seen_us < now_us - peer_ttl_us`.

| Parameter | Value | Source |
|---|---|---|
| Default TTL | 600 s (= 10× announce interval) | `DEFAULT_PEER_TTL_SEC` in `crates/common/src/config/mod.rs` |
| Configuration key | `gossip.peer_ttl_sec` | resolved in `resolve_gossip` (`crates/common/src/config/mod.rs`), validated `> 0` |
| Sweep cadence | 30 s (currently hardcoded in `ttl_sweeper_task`, not exposed in config) | `crates/gossip/src/service.rs` |
| Eviction key | `last_seen_us` (refresh wall-clock, not announce timestamp) | `PeerTable::evict_expired` in `crates/gossip/src/peer_table.rs` |

The 10× ratio (rather than the absolute 600 s) is the stable invariant: if the announce interval is later raised or lowered, the recommended TTL moves with it.

**Minimum sane TTL.** Operators MAY tune `peer_ttl_sec` but SHOULD keep it ≥ 2× the announce interval. Below that bound, a single dropped announce can evict a healthy peer; PlumTree gossip is best-effort and transient drops occur. The PoC default (10× announce interval) lets a peer go silent for up to 10 announces (~9–10 minutes) before eviction — comfortably above realistic loss rates at PoC scale (tens of nodes).

**Why `last_seen_us` and not `announce.timestamp_us`.** The receiver's wall clock decides when an entry is stale. A peer whose clock drifts forward briefly does not get extra TTL credit; a peer whose clock drifts backward briefly does not get evicted early. This also matches the ±60 s skew window already enforced at insert time: drift outside that window is silently rejected at insert, and drift inside it produces no surprises at eviction.

**Sweep is not real-time.** Up to one sweep period (≤ 30 s) elapses between TTL expiry and physical removal. Callers iterating the table MUST tolerate brief over-counting. Selection (probe + DHT) is unaffected — it does not consult the peer table.

### 2. No hard size cap (optional safety ceiling)

The peer table is **not size-capped by default**. Growth is bounded externally:

- Gossip-validation rule (2) rejects every `NodeAnnounce` whose `node_id` is not active in the on-chain `StakingRegistry`. The set of insertable `node_id`s is exactly the active staked node set.
- Memory cost is small even at production scale: a `NodeAnnounce` worst case is 800 B per [ADR 001 § Gossip Bandwidth Analysis](001-network.md#gossip-bandwidth-analysis), so `≤ 1 KB/entry × 10,000 active nodes ≲ 10 MB` including HashMap overhead. At PoC scale (tens of nodes) the table is ~tens of KB.
- Adding an LRU/age/reputation-priority eviction layer would solve a problem the staking gate already constrains.

For defense-in-depth against an unforeseen growth path (registry-validation regression, future schema change), operators MAY set an optional safety ceiling:

- **Config key:** `gossip.max_peer_entries` — `Option<usize>`, default `None` (unlimited).
- **Behavior when set and exceeded:** new inserts are rejected; the failure surfaces in the existing gossip-rejection counter `decdn_gossip_messages_rejected_total{reason=table_full}` (per [appendix-observability.md § 2.6 Gossip Metrics](appendix-observability.md#26-gossip-metrics)). **No existing entry is evicted to make room** — eviction-by-priority would conflate discovery with selection trust (see §4) and is rejected in *Alternatives Considered*.
- **Operator signal:** sustained `decdn_peer_table_size > registered_node_count × 1.5` indicates registry validation is not constraining inserts as expected and warrants investigation, not silent eviction.

Implementing `gossip.max_peer_entries` is OPTIONAL for the PoC (`peer_table_size` already covers the observable signal); the config key is reserved here so a follow-up implementation does not require an ADR amendment.

### 3. Registry-cache interaction (active eviction)

The local registry cache, when implemented per [ADR 001 § Registry cache](001-network.md#registry-cache) and [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry), subscribes to `NodeRegistered`, `NodeDeregistered`, and `NodeAutoEjected` events. On `NodeDeregistered` and `NodeAutoEjected` for a `node_id`, the subscriber MUST also **remove the matching peer-table entry** in the same handler, in addition to its registry-cache update. Origin blacklisting ([ADR 011 § Hash Evasion and Origin Blacklisting](011-content-takedown.md#hash-evasion-and-origin-blacklisting)) routes through `StakingRegistry.ejectNode` and emits `NodeAutoEjected`, so the same code path covers it. The subscriber does not yet exist in the codebase; this clause specifies one additional behavior on top of the subscriber introduced by ADR 019.

**Rationale.**

- The subscriber runs on every relevant event anyway; one additional map removal per event is negligible.
- Without active eviction, a deregistered or blacklisted node remains visible in admin RPC `peers_list()` and gossip-derived analytics for up to TTL (default 10 minutes), creating misleading operator output.
- Selection is unaffected either way (probe + DHT consult the registry directly), so this is purely a discovery/observability cleanup.
- The path is symmetric with the validation rule: rule (2) blocks new inserts of deregistered nodes; this clause completes the symmetry by removing existing entries.

`NodeRegistered` does NOT trigger any peer-table action — the entry will arrive (if ever) via gossip, validated normally.

### 4. Reputation does not factor into eviction

Reputation governs *selection* (the `selection_score` formula in [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm) and the local/network blend in [ADR 008](008-reputation.md)), not retention. A peer whose reputation falls to the 0.1 floor remains in the peer table until TTL or deregistration removes it. The selection-score formula already makes such a node ~100× less likely to be selected (see ADR 001 reputation table), which is the appropriate response.

**Why not reputation-priority eviction.**

- It conflates discovery and selection. The peer table is a discovery surface; selection is the trust-weighted decision built on top of it.
- It creates a vector for collusive eviction reports: a coalition of nodes sending negative `ReputationReport` messages could push a competitor below an "eviction threshold" and remove them from peer tables network-wide, bypassing the hard floor in ADR 008.
- Reputation is noisy in the tail; a transient bad-luck dip should not erase a peer from discovery.

This ADR therefore explicitly excludes reputation from the eviction decision. Reputation-system changes (ADR 008) do not need to consider peer-table side effects.

### 5. Offline handling

**Peer is offline (this node is online).** Receiver stops receiving valid `NodeAnnounce` messages for the peer. After ≥ TTL of silence, `evict_expired` removes the entry. A subsequent valid `NodeAnnounce` re-inserts a fresh entry with new `first_seen_us` / `last_seen_us`. There is no "stale" intermediate state — eviction is binary and the receiver does not surface or act on partial freshness.

**This node is offline.** While the process is offline, no announces arrive and no sweeps run. On resume:

1. Wall-clock time has advanced; the next sweep will evict every entry whose `last_seen_us` is now older than TTL. For an outage longer than TTL, the table effectively empties.
2. The node re-bootstraps from the on-chain registry per [ADR 019 § Phase 3](019-node-onboarding.md#phase-3--node-startup-state-synchronization) (active-node fetch + event subscription) and re-converges via gossip per [ADR 019 § Step 4.3](019-node-onboarding.md#step-43--observe-incoming-nodeannounce-messages) (≥ 1 announce-interval to re-converge).
3. No special-case handling is required — the TTL sweep and the bootstrap paths together restore correct state.

**Process restart.** The peer table is in-memory only; restarts start with an empty table. This is intentional. Persistence would save < one announce interval (~60 s) of cold gossip and add durability machinery the protocol does not need.

### 6. Observability

Naming follows [appendix-observability.md § 2.6 Gossip Metrics](appendix-observability.md#26-gossip-metrics). The existing `decdn_peer_table_size` gauge and `decdn_gossip_messages_rejected_total` counter cover most of the surface; this appendix adds two eviction counters and one new label value:

| Metric | Type | Description |
|---|---|---|
| `decdn_peer_table_size` | gauge | Distinct peers in the local peer table — **existing**, see appendix |
| `decdn_gossip_messages_rejected_total` | counter, labeled by `reason` | **Existing**, see appendix; §2 specifies when the `reason=table_full` label value fires (the optional `gossip.max_peer_entries` rejection path) |
| `decdn_peer_table_evicted_ttl_total` | counter, unlabeled | New: entries removed by the TTL sweeper |
| `decdn_peer_table_evicted_registry_total` | counter, labeled by `reason ∈ {deregistered, ejected}` | New: entries removed in response to a registry event (§3) |

A sustained non-zero `decdn_gossip_messages_rejected_total{reason="table_full"}` rate is the operational signal that `gossip.max_peer_entries` is misconfigured or that registry validation is letting through an unexpected number of `node_id`s.

## Consequences

**Positive.**

- Memory bound is determined by external policy (the staking registry) without per-table bookkeeping. At realistic protocol scales (≤ 10 k nodes) the table is < 5 MB.
- Active eviction on deregistration / blacklisting keeps admin output and gossip-derived analytics accurate within one event-handler turn instead of within ~10 minutes.
- Discovery and selection stay separable. Reputation, blacklist, and registry inputs each have a single clear role; this appendix adds no new coupling.
- The implementation already matches §1 — no code change is required to ship this appendix's TTL behavior. §3 adds a single map-removal step inside the registry-cache subscriber introduced by [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry) (subscriber not yet implemented). §6 adds two counters and one label value. §2's `max_peer_entries` is opt-in.

**Negative.**

- TTL of 600 s on a 60 s announce interval means an offline peer remains discoverable but unreachable for up to ~10 minutes. Probe + DHT route around it (the peer table is not consulted during selection); the `EvictedSinceProbe` handling in [ADR 001 § Content Discovery](001-network.md#content-discovery-dht--probe) covers the corresponding blob-eviction case. Operators concerned about stale visibility can lower `peer_ttl_sec` toward the §1 minimum.
- Without `max_peer_entries` set, a registry-validation regression that admits unstaked `node_id`s could allow unbounded growth. The metric `decdn_peer_table_size` is the early-warning signal; operators tracking it can set the safety ceiling reactively.
- Up to one sweep period (≤ 30 s) elapses between TTL expiry and physical removal. Callers iterating the table MUST tolerate brief over-counting. No iterator currently relies on real-time accuracy.
- Active registry-driven eviction adds one `HashMap::remove` per `NodeDeregistered` / `NodeAutoEjected` event. These events are infrequent (operator-initiated or auto-ejection at 50 % stake floor); cost is negligible.

## Alternatives Considered

- **Reputation-priority eviction.** Rejected. Conflates discovery with selection (§4); creates a collusive-reporting vector against ADR 008's hard reputation floor; punishes transient noise. Reputation already governs selection via the score formula, which is the right place for it.
- **Lazy deregistration (TTL-only, no active evict).** Rejected. The registry-cache subscriber already runs on every event; the marginal cost is one `HashMap::remove`. Lazy handling would leave a deregistered node visible to operators and analytics for up to TTL with no benefit.
- **LRU under a hard size cap.** Rejected. Adds eviction-priority bookkeeping for a problem the staking registry already bounds. If observed `peer_table_size` exceeds `registered_node_count × 1.5` in production, revisit — but the right next step is a registry-validation audit, not an LRU layer.
- **Persisting the peer table across restarts.** Rejected. Restart cost is < one announce interval (~60 s) of cold gossip; durability machinery is not justified.
- **Eviction by `announce.timestamp_us` rather than `last_seen_us`.** Rejected. Couples eviction to peer wall-clock instead of receiver wall-clock; creates surprises when peer clocks drift within the ±60 s skew window. The existing implementation correctly uses `last_seen_us`.
- **Shorter TTL aligned to a single announce interval (60 s).** Rejected. Below 2× announce interval, a single dropped announce evicts a healthy peer; PlumTree gossip is best-effort, so single drops occur.
