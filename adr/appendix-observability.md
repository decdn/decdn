# Appendix: Observability and Metrics

> **This is an appendix, not a core protocol ADR.** Metric implementation is consumer-side — operators choose their own monitoring stack, dashboards, and alerting. This appendix specifies a recommended naming convention, the canonical metric registry, and slash-risk alert thresholds, so that monitoring tooling and operator runbooks can converge on a common vocabulary.

## Context

Metrics are referenced throughout the protocol ADRs and listed informally in `architecture.md § Observability`, but no single document defines: the canonical naming convention; the complete registry of names, types, and labels; which metrics are **mandatory** vs. **recommended**; alert thresholds for slash-risk metrics; and the HTTP export format and endpoint contract. Without this, operators cannot build dashboards, detect slashable conditions before they occur, or compare metrics across nodes — instrumentation becomes ad-hoc.

### Note on existing ADR names

Several ADRs reference informal metric names (e.g., `gossip_messages_rejected_clock_skew`, `probe_hold_violations`, `blacklist_sync_lag_seconds` — from ADRs 001, 005, 011). This appendix is the authoritative canonical registry; the names below are the canonical forms of those informal references, with identical semantic intent.

## Decision

### Naming Convention

All metrics use the `decdn_` prefix, snake_case, and Prometheus-standard unit suffixes:

| Pattern | Example | Rule |
|---------|---------|------|
| `decdn_{subsystem}_{noun}_{unit}` | `decdn_cache_bytes` | Gauge: descriptive noun + unit |
| `decdn_{subsystem}_{noun}_total` | `decdn_streams_completed_total` | Counter: always `_total` suffix |
| `decdn_{subsystem}_{noun}_seconds` | `decdn_probe_collection_latency_seconds` | Histogram/Summary: `_seconds` for duration |

Label names: snake_case, no abbreviations. Label values: lowercase where possible.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port (default `9090`) at `/metrics`. The same port exposes `/health` (see [Health Endpoint](#health-endpoint)). The port MUST be operator-configurable and MUST NOT be publicly accessible without authentication in production (firewall or auth proxy).

### Metric Registry

Metrics are grouped into **mandatory** (M) and **recommended** (R) tiers.

**Mandatory (M):** The node MUST expose these or refuse to start. They cover slash-risk conditions and delivery accountability.

**Recommended (R):** The node SHOULD expose these. Absence is not a startup blocker, but operators lose subsystem visibility.

#### Slash-Safety Metrics (all Mandatory)

Early warning for the three slashable offenses in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn), grouped here with the closely-related probe-hold capacity metrics. A sustained non-zero value for the slash-evidence and blacklist-lag **counters** requires immediate operator attention. The **gauges** (`decdn_probe_hold_slots_used`/`_max`, `decdn_blacklist_version_behind`) are normally non-zero — alert on the thresholds/rates in the table below, not on presence. `decdn_probe_hold_violations_total` is an availability/budget-pressure signal (raise `max_probe_holds`), not a slash risk — see its row.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_probe_hold_violations_total` | Counter | M | Blob present but un-holdable because **all** hold slots were live (`max_probe_holds` reached) — the node signs `has_blob: false` and forgoes revenue under genuine budget pressure ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). The literal "evicted after signing `has_blob: true`" case is unreachable by construction (held hashes are invisible to the LRU driver). The config-disabled case (`max_probe_holds == 0`) is counted separately as `decdn_probe_holds_disabled_total` (#739), and the stake-lane reservation case as `decdn_probe_stake_lane_reserved_total` (#757), so this counter is a clean "raise `max_probe_holds`" signal. |
| `decdn_probe_holds_disabled_total` | Counter | M | Blob present but un-holdable because the eviction-hold path is **disabled by config** (`max_probe_holds == 0`) — an intentional operator choice, not budget pressure (#739, [ADR 005](005-protocol.md#probe-triggered-eviction-hold)). Split from `decdn_probe_hold_violations_total` so the "raise `max_probe_holds`" alert never fires on a deliberate disable. |
| `decdn_probe_stake_lane_reserved_total` | Counter | M | End-client probe answered `has_blob: false` because hold usage reached the stake-lane-reserved end-client ceiling (`max_probe_holds − cache.stake_lane_reserved_holds`), keeping headroom for registered node-to-node cache-miss probes (#757, [ADR 003 § Admission and Priority](003-payments.md#admission-and-priority)). Fires *before* the hold attempt, so the cache is not consulted — a content-independent admission decision, unlike the two rows above. A deliberate priority decision — distinct from budget pressure (`decdn_probe_hold_violations_total`) and config disable (`decdn_probe_holds_disabled_total`). Zero unless `cache.stake_lane_reserved_holds > 0`. |
| `decdn_probe_hold_slots_used` | Gauge | M | Eviction-hold slots in use out of `max_probe_holds`. Saturation forces `has_blob: false` at probe time. |
| `decdn_probe_hold_slots_max` | Gauge | M | Configured `max_probe_holds`. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |
| `decdn_blacklist_sync_lag_seconds` | Gauge | M | Seconds since the last successful `getBlacklistVersion()` poll. Exceeding the compliance window makes serving any recently-blacklisted hash slashable ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)). |
| `decdn_blacklist_version_behind` | Gauge | M | `on_chain_version − local_version`. Positive means new blacklist entries not yet fetched. |
| `decdn_rate_bounds_clamp_events_total` | Counter | M | Times `rate_per_mb` was clamped to governance bounds before signing a `ProbeResponse` — configured rate is outside the current governance window ([ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |
| `decdn_slash_evidence_exposure_total` | Counter | M | Self-detected `has_blob: true` probe followed by a stream response within the 30-second slashing window — valid phantom slash evidence ([ADR 005](005-protocol.md#adr-005-wire-protocol)). Non-zero is a critical bug signal. |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_violations_total` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. |
| `decdn_blacklist_sync_lag_seconds` | > 600s (1 poll interval) | > 1800s | Check RPC provider; manual sync if needed. |
| `decdn_blacklist_version_behind` | > 0 | > 1 | Investigate RPC / poll failure. |
| `decdn_rate_bounds_clamp_events_total` (rate) | > 0 | — | Update `rate_per_mb` config to within governance bounds. |
| `decdn_slash_evidence_exposure_total` (rate) | — | > 0 | File a bug; stop node immediately if rate is sustained. |

#### Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_streams_active` | Gauge | M | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | `direction={inbound,outbound}` | Successfully completed streams. |
| `decdn_streams_failed_total` | Counter | M | `direction, reason` | Failed streams. `reason` values: `hash_mismatch`, `channel_insufficient`, `rate_mismatch`, `blob_too_large`, `evicted`, `timeout`, `protocol_error`, `other`. |
| `decdn_bytes_served_total` | Counter | M | — | Bytes delivered to clients and downstream nodes (inbound streams from the requester's perspective). |
| `decdn_bytes_received_total` | Counter | M | — | Bytes received as a client in node-to-node cache-miss pulls. |

#### Cache Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_cache_bytes` | Gauge | M | Total cache size in bytes (all blobs). Paired with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_size_limit_bytes` | Gauge | R | Configured cache capacity in bytes (`cache.cache_size_mb × 1 048 576`). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md#appendix-blob-cache-eviction-policy). |
| `decdn_cache_hits_total` | Counter | M | Probe or stream requests satisfied from local cache. |
| `decdn_cache_misses_total` | Counter | M | Probe or stream requests requiring origin pull or peer pull. |
| `decdn_cache_bytes_returned_total` | Counter | R | Bytes returned from `CacheEngine::get` to the caller on success. Counts both cache-hit and pull-through-success paths. |
| `decdn_cache_pull_through_bytes_total` | Counter | R | Bytes received from origin during a cache miss, counted regardless of BLAKE3 verification or store landing — origin egress is paid either way. Independent of `decdn_cache_bytes_returned_total`: equal values mean pure pass-through; `bytes_returned_total >> pull_through_bytes_total` indicates effective caching. |
| `decdn_cache_evictions_total` | Counter | R | Blobs evicted by LRU pressure (eviction-driver loop). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md#appendix-blob-cache-eviction-policy). |
| `decdn_cache_evicted_operator_total` | Counter | R | Hashes removed via `decdn node evict` (durable, persisted to `<cache_dir>/evicted.log`). Distinct from `decdn_cache_evictions_total`. See [appendix-blob-cache-eviction.md § Operator-evict is orthogonal to LRU](appendix-blob-cache-eviction.md#operator-evict-is-orthogonal-to-lru). |
| `decdn_cache_pinned_count` | Gauge | R | Size of the operator-pinned set (LRU-exempt). See [appendix-blob-cache-eviction.md § Operator pinning overrides LRU](appendix-blob-cache-eviction.md#operator-pinning-overrides-lru). |
| `decdn_probe_post_eviction_failures_total` | Counter | R | `EvictedSinceProbe` responses from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |

#### Prefetch Metrics

Operator-policy prefetch from popularity signals per [ADR 022 § Prefetch Decision](022-content-discovery.md#prefetch-decision). All metrics are exposed regardless of whether `prefetch.enabled` is true — a node with prefetch disabled reports zero counters and `decdn_prefetch_enabled` as `0`, giving operators a uniform schema to scrape against and the DAO an off-chain signal for outlier behavior.

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_prefetch_enabled` | Gauge | M | — | `1` if `prefetch.enabled` is true, `0` otherwise. Provides a stable schema across enabled/disabled nodes. |
| `decdn_prefetch_acquisitions_total` | Counter | M | `gate_result={authorized,unauthorized,bypassed}` | Prefetch acquisitions attempted (triggered by the FIND_VALUE demand signal), broken down by `prefetch.require_authorized_origin` gate outcome. `authorized` and `bypassed` (gate disabled) proceed to pull; `unauthorized` is a rejected attempt. |
| `decdn_prefetch_spend_usdc_total` | Counter | M | — | Cumulative USDC paid out for prefetch acquisitions. Pairs with `decdn_prefetch_acquisitions_total{gate_result≠unauthorized}` to derive average spend per acquisition. |
| `decdn_prefetch_budget_exhaustion_events_total` | Counter | M | — | Times `prefetch.budget_usdc_per_hour` was hit, preventing further acquisitions until the rolling window advanced. Non-zero values indicate the operator should raise the budget or investigate elevated demand-signal volume. |
| `decdn_prefetch_origin_gate_rejections_total` | Counter | R | — | Acquisitions skipped because no candidate in the DHT FIND_VALUE result set was authorized as origin under the relevant `OriginAssignment` lookup. Identical-by-construction to `decdn_prefetch_acquisitions_total{gate_result=unauthorized}`; surfaced as a standalone counter for alerting convenience. |
| `decdn_prefetch_demand_quality_ratio` | Gauge | R | — | Current rolling-window `served_bytes / acquired_bytes` for prefetched content. Below `prefetch.demand_quality_min_ratio` triggers the auto-throttle described in [ADR 022 § Prefetch Decision](022-content-discovery.md#prefetch-decision). |
| `decdn_prefetch_throttle_active` | Gauge | R | — | `1` while the demand-quality auto-throttle is suppressing prefetch; `0` otherwise. |

#### Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_collection_latency_seconds` | Histogram | M | `outcome={0rtt_warm,1rtt_cold}` | Duration of a complete probe collection window, send to collection end. The `outcome` label measures 0-RTT impact per [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment). Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_responses_total` | Counter | R | `result={has_blob,no_blob,timeout}` | Probe responses received, by result. |
| `decdn_probe_holds_disabled_total` | Counter | M | — | Probes answered `has_blob: false` for a *present* blob because the eviction-hold path is disabled by config (`max_probe_holds == 0`). An intentional operator decision, not budget pressure — split out from `decdn_probe_hold_violations_total` so a deliberate disable does not trip its "increase `max_probe_holds`" alert ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). |

#### Payment Channel Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_channels_open` | Gauge | M | Currently open payment channels (as node — inbound from clients). |
| `decdn_channels_settled_total` | Counter | M | Channels settled on-chain. |
| `decdn_channel_deposit_usdc` | Gauge | M | Total USDC deposited across all currently open inbound channels. Represents maximum on-chain recoverable value. |
| `decdn_vouchers_signed_total` | Counter | M | Vouchers signed by this node as the payee. |
| `decdn_vouchers_received_total` | Counter | R | Vouchers received by this node as the payer (node-to-node pulls). |
| `decdn_channel_disputes_total` | Counter | R | Channels that entered the dispute window. |

#### Gossip Metrics

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_gossip_messages_rejected_total` | Counter | M | `reason={clock_skew,invalid_signature,not_registered,stale_timestamp,invalid_region,duplicate_hashes,table_full}` | Gossip messages rejected during validation ([ADR 001](001-network.md#gossip-validation)) or peer-table admission ([appendix-peer-table-eviction.md](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy)). `reason=clock_skew` is the canonical replacement for [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)'s `gossip_messages_rejected_clock_skew`. `table_full` fires only when the optional `gossip.max_peer_entries` ceiling is set and exceeded. |
| `decdn_peer_table_size` | Gauge | M | — | Number of distinct peers in the local peer table. |
| `decdn_peer_table_evicted_ttl_total` | Counter | R | — | Peer-table entries removed by the TTL sweeper ([appendix-peer-table-eviction.md § Lifecycle and TTL](appendix-peer-table-eviction.md#lifecycle-and-ttl)). |
| `decdn_peer_table_evicted_registry_total` | Counter | R | `reason={deregistered,ejected}` | Peer-table entries removed in response to a `NodeDeregistered` or `NodeAutoEjected` registry event ([appendix-peer-table-eviction.md § Registry-cache interaction (active eviction)](appendix-peer-table-eviction.md#registry-cache-interaction-active-eviction)). |
| `decdn_gossip_announces_sent_total` | Counter | R | — | `NodeAnnounce` messages published. |
| `decdn_gossip_announces_received_total` | Counter | R | — | `NodeAnnounce` messages accepted (passed validation). |
| `decdn_gossip_subscriber_reconnections_total` | Counter | R | — | Successful subscriber reconnections after a gossip stream drop. |

#### Reputation Metrics

The metrics below apply once the reputation gossip layer in [ADR 008](008-reputation.md#adr-008-reputation-system) is implemented; local-only-scoring nodes expose only `decdn_reputation_score`.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_reputation_reports_sent_total` | Counter | R | `ReputationReport` messages published to `cdn/reputation/v1`. |
| `decdn_reputation_reports_received_total` | Counter | R | `ReputationReport` messages accepted from peers. |
| `decdn_reputation_score` | Gauge | R | This node's current `final_score` (0.0–1.0) as computed locally — local observations (70%) + gossip (30%) per [ADR 008](008-reputation.md#adr-008-reputation-system). |

#### QUIC / 0-RTT Metrics

Per [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment). All labeled by `alpn`.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_quic_0rtt_attempts_total` | Counter | R | 0-RTT connection attempts. |
| `decdn_quic_0rtt_accepted_total` | Counter | R | 0-RTT connections accepted by server. |
| `decdn_quic_0rtt_rejected_total` | Counter | R | 0-RTT rejected, fell back to 1-RTT. |

These are the canonical forms of the identical names in [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment) — identical semantics under the canonical naming regime.

#### Node / Process Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | Seconds since the node process started. Used by the `/health` endpoint and operator dashboards to correlate events with restarts. |

#### Tokenomics Metrics

Per [ADR 026](026-tokenomics.md#adr-026-tokenomics). These metrics expose the `FeeRouter`, `CapacityBond`, and `SlashAppeal` contract surfaces to operator dashboards, keeper monitoring, and governance dashboards ([ADR 009](009-governance.md#adr-009-governance-model)).

A subset is sourced from on-chain contract state (`FeeRouter`, `CapacityBond`, `SlashAppeal`, `BuybackBurner`) via the same RPC client used for blacklist polling and channel-state queries ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting), [ADR 003](003-payments.md#adr-003-payment-model)). They use the **same Prometheus text-format `/metrics` endpoint, scrape interval, and retention defaults** defined in Section 1 — no separate export pipeline. Contract-sourced gauges sample at the existing RPC-poll cadence; counters tracking on-chain events advance only when the node observes the corresponding event log.

##### FeeRouter Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_fee_router_inflow_usdc_total` | Counter | R | `bucket={operator_base,burn,treasury}` | `FeeRouter` settlement events (RPC) | Operator + governance dashboards | Cumulative per-bucket USDC inflow at `FeeRouter.routeSettlement`. All three legs transfer same-tx ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)). |
| `decdn_fee_router_inflow_usdc_rate` | Gauge | R | `bucket={operator_base,burn,treasury}` | Derived (rolling 7-epoch avg over `..._inflow_usdc_total`) | Governance + capacity-planning dashboards | Rolling-average per-bucket USDC inflow per epoch. |

##### Served-Bytes Voting Metrics

Per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight): the per-operator `bytesPerEpoch` and trailing-window sums on `FeeRouter` are the governance vote-weight source.

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_operator_bytes_delivered` | Gauge | R | `epoch` | `FeeRouter.bytesPerEpoch(operator, epoch)` (RPC) | Operator dashboard, governance dashboard | This operator's served-bytes share for the labeled epoch — input to the trailing-window vote-weight numerator per [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula). |
| `decdn_operator_bytes_in_window` | Gauge | R | `window={windowEpochs}` | `FeeRouter.bytesInWindow(operator, currentEpoch, windowEpochs)` (RPC) | Operator dashboard, governance dashboard | This operator's trailing-window served-bytes sum — the pre-cap, pre-`age_ramp` numerator of the vote-weight formula per [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula). |
| `decdn_capacity_bond_slashed_at_epoch` | Gauge | R | — | `CapacityBond.slashedAtEpoch(operator)` (RPC) | Governance dashboard, operator alerting | The epoch of this operator's most recent slash; zero if never slashed. Vote weight is zero while this watermark falls inside the trailing window per [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out). |

##### Genesis Bond Credit Metrics

Per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits): a bounded, one-shot, 24-month-vest TOKEN grant to verified pre-launch testnet operators, held in `CapacityBond.PendingCredit`. There is no ongoing emission to expose; these metrics let an operator track their vesting position.

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_capacity_bond_pending_credit_total_token` | Gauge | R | — | `CapacityBond.pendingCredit(operator).total` (RPC) | Operator dashboard | This operator's total Genesis Bond Credit grant (vested + unvested), zero if not eligible. |
| `decdn_capacity_bond_pending_credit_vested_token` | Gauge | R | — | `CapacityBond.pendingCredit(operator).vested` (RPC) | Operator dashboard | This operator's vested-but-still-bonded portion of the Genesis Bond Credit grant. |

##### CapacityBond Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_capacity_bond_amount_token` | Gauge | R | — | `CapacityBond.bondOf(operator)` (RPC) | Operator dashboard, governance | This operator's current bonded TOKEN (voluntary bond plus the vested portion of any Genesis Bond Credit grant per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits)); used to compute the operator's tier and bond-curve position. Unvested `PendingCredit` is tracked separately via `decdn_capacity_bond_pending_credit_*`. |
| `decdn_capacity_bond_declared_mbps` | Gauge | R | — | `CapacityBond.declaredMbps(operator)` (RPC) | Operator dashboard | This operator's declared bandwidth capacity in Mbps; used for capacity-tier checks and bond-curve calculations. Does not feed voting weight directly under [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). |
| `decdn_fee_router_total_bytes_in_window` | Gauge | R | `window={windowEpochs}` | `FeeRouter.totalBytesInWindow(currentEpoch, windowEpochs)` (RPC) | Governance dashboard | Network-wide sum of served bytes over the trailing `windowEpochs` window — the quorum / proposal-threshold denominator per [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula). |
| `decdn_capacity_bond_active_operator_count` | Gauge | R | — | `CapacityBond.getActiveNodeCount()` (RPC) | Governance dashboard, bootstrap-multisig transition tracking | Active operator count; gates the bootstrap-multisig → DAO transition (≥30 operators AND ≥100 Gbps per [ADR 009](009-governance.md#bootstrap-multisig-phase)). The NodeId↔Ethereum-address binding is 1:1 ([ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)), so active-node count and active-operator count coincide. |

##### Slash-Escrow / SlashAppeal Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_slash_escrow_total_token` | Gauge | R | — | `CapacityBond.escrowedTotal()` (RPC) | Governance dashboard, keeper monitoring | Total slashed TOKEN currently held in escrow (status `Escrowed` or `AppealOpen`), awaiting finality or appeal resolution per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn). |
| `decdn_slash_appeals` | Gauge | R | `state={open,fastTracked,resolved}` | `SlashAppeal` records (RPC) | Public dashboard, governance | Slash-appeal count per state. `open` = filed, awaiting multisig review; `fastTracked` = multisig granted interim relief, awaiting Governor; `resolved` = granted/upheld/lapsed. |
| `decdn_slash_finalized_total` | Counter | R | `outcome={upheld,granted}` | `CapacityBond` `SlashUpheld` / `SlashReversed` events | Public dashboard, governance reporting | Cumulative finalized slashes by outcome (upheld → 50/50 distributed; granted → refunded to operator). |

#### DHT / Content-Discovery Metrics

Per [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale) (`cdn/dht/v1`). DHT STORE and FIND_VALUE carry no protocol-level fee; these metrics expose discovery health only.

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_dht_store_published_total` | Counter | R | — | DHT STORE records this node published to the K-closest peers ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). |
| `decdn_dht_findvalue_queries_total` | Counter | R | — | DHT FIND_VALUE lookups this node issued to discover providers. |
| `decdn_dht_routing_table_size` | Gauge | R | — | Distinct entries in the local Kademlia routing table. |

##### Active-Staker Set Watcher Metrics

The `ChainStakerSet` watcher follows `CapacityBond` membership events to keep a cached active-staker set in sync with chain state ([ADR 022 § STORE Flow](022-content-discovery.md#adr-022--content-discovery-at-scale), [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry)). On a mid-run watcher RPC outage the cache can **drift** from chain state (there is no `getActiveNodes` resync after extended outage). That drift is revenue-impacting: the cached set decides which probes the stake-lane reservation sheds ([ADR 003 § Admission and Priority](003-payments.md#admission-and-priority), #757) and gates DHT `Store` admission. These metrics make the drift window alertable rather than log-grep-only — `decdn_rpc_healthy` tracks the reachability watchdog, **not** this watcher (#783).

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_staker_set_watcher_restarts_total` | Counter | M | — | Distinct drift windows the watcher has entered (#783, edge-triggered #788): bumped **once** on the transition from a healthy cycle into the error/backoff state, so each increment brackets exactly one drift window (it does **not** count individual backoff iterations of one continuous outage). A clean stream end (filter expiry / provider rotation) is **not** an error and does not advance this. Pairs with the per-error `warn!` in `watcher_loop`. |
| `decdn_staker_set_watcher_resolve_failures_total` | Counter | M | — | Operator-indexed events (`Reinstated` / `UnbondingRequested`) dropped because the follow-up `nodeIdOf(operator)` RPC failed (#788). The membership change is lost, leaving the cached set out of sync for that operator until a later event corrects it — silent drift that trips no restart/down-seconds metric, hence its own counter. Pairs with the per-failure `warn!` in `apply_operator_change`. |
| `decdn_staker_set_watcher_down_seconds` | Gauge | M | — | True downtime (#783, semantics corrected #788): seconds the watcher has been in the error/backoff state with no established filters. Reads `0` for the **entire life of any established cycle**, however long or quiet (a healthy filter stream persists indefinitely — this is *not* cycle age), and climbs only while between a failed cycle and the next re-establishment, so an alert fires on a *sustained* outage rather than a transient restart. Reads `0` until the first cycle is established after bootstrap; a poisoned internal lock reports `i64::MAX` (conservative — never masks an in-progress outage). |
| `decdn_staker_set_active_count` | Gauge | R | — | Current cached active-staker set size (#783), sampled on bootstrap and on every membership change. Pair with `decdn_staker_set_watcher_down_seconds`: the count holding flat while down-seconds climbs means the cache is frozen, not that the network genuinely lost operators. |

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_staker_set_watcher_down_seconds` | > 120s | > 600s sustained | Check the blockchain RPC provider; the cached active-staker set may be drifting from chain state, mis-shedding stake-lane probes and DHT `Store`s. |
| `decdn_staker_set_watcher_restarts_total` (rate) | > 0 | > 0 sustained | Investigate flapping RPC / event subscription; correlate with `decdn_staker_set_watcher_down_seconds` depth. |
| `decdn_staker_set_watcher_resolve_failures_total` (rate) | > 0 | > 0 sustained | A `nodeIdOf` RPC is failing and silently dropping membership changes — the cached active-staker set is drifting from chain state. Check the RPC provider; correlate with `decdn_staker_set_active_count`. |

### Health Endpoint

`GET /health` (same HTTP port as `/metrics`) returns a JSON object:

```json
{
  "status": "ready" | "degraded" | "not_ready",
  "node_id": "<hex iroh NodeId>",
  "registry_active": true,
  "blacklist_version": 42,
  "blacklist_synced": true,
  "rate_bounds_loaded": true,
  "peer_table_size": 12,
  "channels_open": 3,
  "channel_deposit_usdc": "15.23",
  "node_uptime_seconds": 3601
}
```

**JSON key → Prometheus metric mapping:**

| JSON key | Prometheus metric | Notes |
|----------|------------------|-------|
| `peer_table_size` | `decdn_peer_table_size` | Direct gauge value |
| `channels_open` | `decdn_channels_open` | Direct gauge value |
| `channel_deposit_usdc` | `decdn_channel_deposit_usdc` | Formatted as decimal string for readability; metric stores raw value |
| `node_uptime_seconds` | `decdn_node_uptime_seconds` | Direct gauge value |
| `blacklist_version` | `decdn_blacklist_version_behind` (derived) | Absolute version number from RPC, not the lag gauge |

**Status semantics:**

| `status` | Meaning |
|----------|---------|
| `ready` | All Phase 5 acceptance criteria satisfied ([ADR 019](019-node-onboarding.md#phase-5--accepting-paid-delivery)); serving traffic. |
| `degraded` | Running but one or more non-critical conditions impaired (e.g., gossip mesh thin, 0-RTT cache cold). Traffic still accepted. |
| `not_ready` | A mandatory startup check failed or is incomplete (blacklist un-synced, rate bounds not loaded, not registered). Not accepting traffic. |

HTTP status codes: `200` for `ready` and `degraded`; `503` for `not_ready`. Monitoring systems SHOULD alert on `503` responses.

### Structured Logging

Metrics cover aggregates; structured logs cover per-event detail. Complementary — logs are not a substitute for metrics.

- **Library:** `tracing` crate (standard in the iroh ecosystem).
- **Format:** JSON (`tracing-subscriber` `json` formatter) for production machine consumption. Human-readable (`pretty`) available via config flag for local development.
- **Log levels:**
  - `ERROR` — unrecoverable, needs operator intervention (startup failures, slash-evidence exposure, RPC unreachable after all retries).
  - `WARN` — recoverable degraded conditions (blacklist poll lag > 1 interval, startup clock skew > 10 s, probe hold slot saturation > 90%).
  - `INFO` — significant lifecycle events (node ready, channel opened/settled, peer joined/left, `NodeAnnounce` published).
  - `DEBUG` — per-stream and per-probe events. Not for high-volume production.

**Mandatory log fields** on every event:

- `node_id` — iroh NodeId (hex)
- `ts` — RFC 3339 timestamp
- `level` — log level
- `target` — Rust module path

### Canonical Metric Name Cross-Reference

Earlier ADRs used informal metric names; this table maps them to canonical replacements. **No wire protocol or on-chain change** — instrumentation names only.

| Informal name (prior ADR) | Canonical name (this appendix) | Source ADR |
|---------------------------|---------------------------|------------|
| `gossip_messages_rejected_clock_skew` | `decdn_gossip_messages_rejected_total{reason="clock_skew"}` | [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) |
| `probe_hold_violations` | `decdn_probe_hold_violations_total` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `probe_holds_disabled` | `decdn_probe_holds_disabled_total` | [ADR 005](005-protocol.md#adr-005-wire-protocol), #739 |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `rate_bounds_clamp_events` | `decdn_rate_bounds_clamp_events_total` | architecture.md |
| `blacklist_sync_lag_seconds` | `decdn_blacklist_sync_lag_seconds` | [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) |
| `blacklist_version_behind` | `decdn_blacklist_version_behind` | [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) |
| `slash_evidence_exposure` | `decdn_slash_evidence_exposure_total` | architecture.md |
| `quic_0rtt_attempts_total` | `decdn_quic_0rtt_attempts_total` | [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment) |
| `quic_0rtt_accepted_total` | `decdn_quic_0rtt_accepted_total` | [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment) |
| `quic_0rtt_rejected_total` | `decdn_quic_0rtt_rejected_total` | [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment) |
| `probe_collection_latency_seconds` | `decdn_probe_collection_latency_seconds` | [ADR 015](015-zero-rtt.md#adr-015-quic-0-rtt-connection-establishment) |
| `streams_active` | `decdn_streams_active` | architecture.md |
| `streams_completed` | `decdn_streams_completed_total` | architecture.md |
| `streams_failed` | `decdn_streams_failed_total` | architecture.md |
| `vouchers_signed` | `decdn_vouchers_signed_total` | architecture.md |
| `vouchers_received` | `decdn_vouchers_received_total` | architecture.md |
| `reputation_reports_sent` | `decdn_reputation_reports_sent_total` | architecture.md |
| `reputation_reports_received` | `decdn_reputation_reports_received_total` | architecture.md |
| `channels_open` | `decdn_channels_open` | architecture.md |
| `channels_settled` | `decdn_channels_settled_total` | architecture.md |
| `cache_hits` | `decdn_cache_hits_total` | architecture.md |
| `cache_misses` | `decdn_cache_misses_total` | architecture.md |
| `cache_bytes` | `decdn_cache_bytes` | architecture.md |

### Reference dashboards and alerts

A reference Grafana dashboard and starter Prometheus alerting rules ship in the top-level [`monitoring/`](../monitoring/) directory. They consume only canonical [§ Metric Registry](#metric-registry) metric names — no new metrics — and are an onboarding starting point, not a normative deliverable.

| File | Purpose |
|------|---------|
| [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml) | Three rule groups: `decdn-slash-safety` (thresholds copied verbatim from [§ Slash-Safety Metrics (all Mandatory)](#slash-safety-metrics-all-mandatory)), `decdn-liveness`, `decdn-delivery`. |
| [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) | Single overview dashboard (`uid: decdn-poc-overview`); rows for health, slash safety, delivery, cache, probes, payments. Datasource parameterised via `${DS_PROMETHEUS}`; node selection via the `instance` template variable. |

#### Importing the dashboard

In Grafana, *Dashboards → New → Import*; upload or paste the JSON. Select your Prometheus datasource at the prompt; the `instance` variable auto-populates from `decdn_node_uptime_seconds`.

#### Using the alerts

Add the file via Prometheus `rule_files:` and reload. Validate with `promtool check rules monitoring/prometheus-alerts.yml`. Tune `for:` durations and thresholds for your fleet size before paging.

#### Scope

Covers M-tier slash-safety metrics and the most common R-tier panels for a first dashboard. Deliberately not exhaustive: [§ Tokenomics Metrics](#tokenomics-metrics) tokenomics, reputation, and 0-RTT panels are left to deployment-specific dashboards.

## Consequences

### Positive

- Single reference for dashboard configuration — no hunting across 8 ADRs for metric names.
- Mandatory M-tier slash-risk metrics enforced at startup, so operators cannot accidentally run without slash-risk visibility.
- Canonical `decdn_` prefix and `_total` suffix allow automated registry validation (e.g., a CI check that exported names match the registry).
- Alert thresholds provide actionable defaults for new operators.
- The `/health` endpoint integrates with standard load balancers and container readiness probes without parsing Prometheus text.

### Negative

- Existing ADRs reference informal names differing from the canonical ones here. The cross-reference table (Section 6) documents all renames; no ADR is retroactively edited (avoids draft-document churn), but implementations must use this appendix's canonical names.
- Mandatory metrics add startup complexity — all M-tier collectors must initialize before accepting connections. Small overhead for guaranteed observability.

## Deferred & Open

- **OpenMetrics migration.** Prometheus text format 0.0.4 is the current default; the OpenMetrics exposition format (used by `prometheus_client` crate's `MetricsEncoder`) adds exemplars and native histograms — evaluate once tooling support is broader.
- **Tokenomics + reputation dashboard panels.** The reference dashboard in [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) is deliberately scoped to M-tier core operations. [§ Reputation Metrics](#reputation-metrics) reputation and [§ Tokenomics Metrics](#tokenomics-metrics) tokenomics metrics warrant their own dedicated dashboards (served-bytes voting weight + quorum-denominator tracking per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), Genesis Bond Credit vesting tracking per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits), bootstrap-multisig transition tracking) — these are deployment-specific and belong outside the reference set.
