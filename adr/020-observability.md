# ADR 020: Observability and Metrics Standard

**Date:** 2026-04-08
**Status:** Draft

## Context

Metrics are referenced throughout the existing ADRs (001, 004, 005, 011, 015) and listed
informally in `architecture.md § Observability`, but no single document defines:

- A canonical metric naming convention
- The complete registry of metric names, types, and labels
- Which metrics are **mandatory** (operators must expose them) vs. **recommended**
- Alert thresholds for slash-risk metrics
- The HTTP export format and endpoint contract

Without this, operators running PoC nodes cannot build monitoring dashboards, cannot
detect slashable conditions before they occur, and cannot compare metrics across nodes.
Instrumentation becomes ad-hoc, making cross-node analysis impossible.

**Note on existing ADR names.** Several ADRs reference informal metric names
(e.g., `gossip_messages_rejected_clock_skew`, `probe_hold_violations`,
`blacklist_sync_lag_seconds` — from ADRs 001, 005, 011). This ADR is the authoritative
canonical registry; the names below supersede those informal references. The changes are
purely naming — the semantic intent is unchanged.

## Decision

### 1. Naming Convention

All metrics use the `decdn_` prefix, snake_case, and Prometheus-standard unit suffixes:

| Pattern | Example | Rule |
|---------|---------|------|
| `decdn_{subsystem}_{noun}_{unit}` | `decdn_cache_bytes` | Gauge: descriptive noun + unit |
| `decdn_{subsystem}_{noun}_total` | `decdn_streams_completed_total` | Counter: always `_total` suffix |
| `decdn_{subsystem}_{noun}_seconds` | `decdn_probe_fanout_latency_seconds` | Histogram/Summary: `_seconds` for duration |

Label names: snake_case, no abbreviations. Label values: lowercase where possible.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port
(default `9090`) at path `/metrics`. The same port exposes `/health` (see [Health
Endpoint](#health-endpoint)). The port MUST be configurable via operator config; it MUST
NOT be publicly accessible without authentication in production (firewall or auth proxy).

### 2. Metric Registry

Metrics are grouped into **mandatory** (M) and **recommended** (R) tiers.

**Mandatory (M):** The node MUST expose these metrics or refuse to start. They cover
slash-risk conditions and delivery accountability.

**Recommended (R):** The node SHOULD expose these metrics. Absence is not a startup
blocker, but operators lose visibility into specific subsystems.

---

#### 2.1 Slash-Safety Metrics (all Mandatory)

These metrics provide early warning for the five slashable offenses defined in
[ADR 004](004-tokenomics.md#staking-and-slashing-schedule). A sustained non-zero value
for any of these requires immediate operator attention.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_probe_hold_violations_total` | Counter | M | Blob evicted within `probe_hold_duration` after signing `has_blob: true`. Each increment is a signed phantom-announcement slash risk ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). |
| `decdn_probe_hold_slots_used` | Gauge | M | Current eviction-hold slots in use out of `max_probe_holds`. Saturation (→ `max_probe_holds`) forces the node to answer `has_blob: false` at probe time. |
| `decdn_probe_hold_slots_max` | Gauge | M | Configured `max_probe_holds` value. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |
| `decdn_blacklist_sync_lag_seconds` | Gauge | M | Seconds elapsed since the last successful `getBlacklistVersion()` poll. Exceeding the compliance window makes serving any recently-blacklisted hash slashable ([ADR 011](011-content-takedown.md)). |
| `decdn_blacklist_version_behind` | Gauge | M | `on_chain_version − local_version`. A positive value means the node has not yet fetched new blacklist entries. |
| `decdn_rate_bounds_clamp_events_total` | Counter | M | Times `rate_per_mb` was clamped to governance bounds before signing a `ProbeResponse` or `RateChange`. Indicates the operator's configured rate is outside the current governance window ([ADR 003](003-payments.md), [ADR 005](005-protocol.md)). |
| `decdn_slash_evidence_exposure_total` | Counter | M | Self-detected instances where the node produced a signed `has_blob: true` probe followed by a stream response within the 30-second slashing window that would constitute valid phantom slash evidence ([ADR 005](005-protocol.md)). Non-zero is a critical bug signal. |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_violations_total` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. |
| `decdn_blacklist_sync_lag_seconds` | > 600s (1 poll interval) | > 1800s | Check RPC provider; manual sync if needed. |
| `decdn_blacklist_version_behind` | > 0 | > 1 | Investigate RPC / poll failure. |
| `decdn_rate_bounds_clamp_events_total` (rate) | > 0 | — | Update `rate_per_mb` config to within governance bounds. |
| `decdn_slash_evidence_exposure_total` (rate) | — | > 0 | File a bug; stop node immediately if rate is sustained. |

---

#### 2.2 Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_streams_active` | Gauge | M | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | `direction={inbound,outbound}` | Successfully completed streams. |
| `decdn_streams_failed_total` | Counter | M | `direction, reason` | Failed streams. `reason` values: `hash_mismatch`, `channel_insufficient`, `rate_mismatch`, `blob_too_large`, `evicted`, `timeout`, `protocol_error`, `other`. |
| `decdn_bytes_served_total` | Counter | M | — | Bytes delivered to clients and downstream nodes (inbound streams from the perspective of the requester). |
| `decdn_bytes_received_total` | Counter | M | — | Bytes received as a client in node-to-node cache-miss pulls. |

---

#### 2.3 Cache Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_cache_bytes` | Gauge | M | Current total cache size in bytes (all blobs). |
| `decdn_cache_hits_total` | Counter | M | Probe or stream requests satisfied from local cache. |
| `decdn_cache_misses_total` | Counter | M | Probe or stream requests requiring origin pull or peer pull. |
| `decdn_cache_evictions_total` | Counter | R | Blobs evicted by LRU/LFU pressure. |
| `decdn_probe_post_eviction_failures_total` | Counter | R | `EvictedSinceProbe` responses received from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md), [ADR 005](005-protocol.md)). |

---

#### 2.4 Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_fanout_latency_seconds` | Histogram | M | `outcome={0rtt_warm,1rtt_cold}` | Duration of a complete probe fan-out from send to collection end. The `outcome` label enables measuring 0-RTT impact per [ADR 015](015-zero-rtt.md). Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_responses_total` | Counter | R | `result={has_blob,no_blob,timeout}` | Probe responses received, by result. |

---

#### 2.5 Payment Channel Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_channels_open` | Gauge | M | Currently open payment channels (as node — inbound from clients). |
| `decdn_channels_settled_total` | Counter | M | Channels settled on-chain. |
| `decdn_channel_deposit_usdc` | Gauge | M | Total USDC deposited across all currently open inbound channels. Represents maximum on-chain recoverable value. |
| `decdn_vouchers_signed_total` | Counter | M | Vouchers signed by this node as the payee. |
| `decdn_vouchers_received_total` | Counter | R | Vouchers received by this node as the payer (node-to-node pulls). |
| `decdn_channel_disputes_total` | Counter | R | Channels that entered the dispute window. |

---

#### 2.6 Gossip Metrics

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_gossip_messages_rejected_total` | Counter | M | `reason={clock_skew,invalid_signature,not_registered,stale_timestamp,invalid_region,duplicate_hashes}` | Gossip messages rejected during validation ([ADR 001](001-network.md#gossip-validation)). `clock_skew` was previously `gossip_messages_rejected_clock_skew` in ADR 001 — this metric with `reason=clock_skew` is the canonical replacement. |
| `decdn_peer_table_size` | Gauge | M | — | Number of distinct peers in the local peer table. |
| `decdn_gossip_announces_sent_total` | Counter | R | — | `NodeAnnounce` messages published. |
| `decdn_gossip_announces_received_total` | Counter | R | — | `NodeAnnounce` messages accepted (passed validation). |
| `decdn_gossip_subscriber_reconnections_total` | Counter | R | — | Successful subscriber reconnections after a gossip stream drop. |

---

#### 2.7 Reputation Metrics (production only)

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_reputation_reports_sent_total` | Counter | R | `ReputationReport` messages published to `cdn/reputation/v1`. |
| `decdn_reputation_reports_received_total` | Counter | R | `ReputationReport` messages accepted from peers. |
| `decdn_reputation_score` | Gauge | R | This node's current `final_score` (0.0–1.0) as computed locally — local observations (70%) + gossip (30%) per [ADR 008](008-reputation.md). |

---

#### 2.8 QUIC / 0-RTT Metrics

Per [ADR 015](015-zero-rtt.md). All labeled by `alpn`.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_quic_0rtt_attempts_total` | Counter | R | 0-RTT connection attempts. |
| `decdn_quic_0rtt_accepted_total` | Counter | R | 0-RTT connections accepted by server. |
| `decdn_quic_0rtt_rejected_total` | Counter | R | 0-RTT rejected, fell back to 1-RTT. |

These replace the identical names from ADR 015 — no semantic change, only now under the
canonical naming regime.

---

#### 2.9 Node / Process Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | Seconds since the node process started (Unix epoch of start subtracted from current time). Used by the `/health` endpoint and operator dashboards to correlate events with restarts. |

---

### 3. Health Endpoint

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
| `ready` | All Phase 5 acceptance criteria satisfied ([ADR 019](019-node-onboarding.md#phase-5--accepting-paid-delivery)); node is serving traffic. |
| `degraded` | Node is running but one or more non-critical conditions are impaired (e.g., gossip mesh thin, 0-RTT cache cold). Traffic is still accepted. |
| `not_ready` | A mandatory startup check has failed or not yet completed (blacklist un-synced, rate bounds not loaded, not registered). Node is not accepting traffic. |

HTTP status codes: `200` for `ready` and `degraded`; `503` for `not_ready`. Monitoring
systems SHOULD alert on `503` responses.

---

### 4. Structured Logging

Metrics cover aggregates. Structured logs cover per-event detail. The two systems are
complementary — logs are not a substitute for metrics.

- **Library:** `tracing` crate (standard in the iroh ecosystem).
- **Format:** JSON (`tracing-subscriber` with `json` formatter) for machine consumption
  in production. Human-readable (`pretty`) format available via config flag for local
  development.
- **Log levels:**
  - `ERROR` — unrecoverable conditions requiring operator intervention (startup failures,
    slash-evidence exposure, RPC endpoint unreachable after all retries).
  - `WARN` — recoverable degraded conditions (blacklist poll lag > 1 interval, clock
    skew > 10 s detected at startup, probe hold slot saturation > 90%).
  - `INFO` — significant lifecycle events (node ready, channel opened/settled, peer
    joined/left, `NodeAnnounce` published).
  - `DEBUG` — per-stream and per-probe events. Not for production use at high traffic
    volumes.

**Mandatory log fields** on every event:

- `node_id` — iroh NodeId (hex)
- `ts` — RFC 3339 timestamp
- `level` — log level
- `target` — Rust module path

---

### 5. PoC vs. Production Differences

| Area | PoC | Production |
|------|-----|------------|
| Metrics port auth | None (firewall-protected) | Auth proxy (basic auth or mTLS) |
| Log format | Either (configurable) | JSON mandatory |
| Reputation metrics | Not required (reputation system simplified) | Recommended |
| Watchtower metrics | Not required | Recommended (`decdn_channel_disputes_total`) |
| Alerting | Operator's choice; thresholds from Section 2.1 are guidelines | PagerDuty / Alertmanager integration recommended |
| Dashboard | Not provided; Grafana dashboard template is future work | — |

---

### 6. Canonical Metric Name Cross-Reference

Earlier ADRs used informal metric names. This table maps them to their canonical
replacements. **No wire protocol or on-chain change is required** — these are
instrumentation names only.

| Informal name (prior ADR) | Canonical name (this ADR) | Source ADR |
|---------------------------|---------------------------|------------|
| `gossip_messages_rejected_clock_skew` | `decdn_gossip_messages_rejected_total{reason="clock_skew"}` | ADR 001 |
| `probe_hold_violations` | `decdn_probe_hold_violations_total` | ADR 005, architecture.md |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | ADR 005, architecture.md |
| `rate_bounds_clamp_events` | `decdn_rate_bounds_clamp_events_total` | architecture.md |
| `blacklist_sync_lag_seconds` | `decdn_blacklist_sync_lag_seconds` | ADR 011 |
| `blacklist_version_behind` | `decdn_blacklist_version_behind` | ADR 011 |
| `slash_evidence_exposure` | `decdn_slash_evidence_exposure_total` | architecture.md |
| `quic_0rtt_attempts_total` | `decdn_quic_0rtt_attempts_total` | ADR 015 |
| `quic_0rtt_accepted_total` | `decdn_quic_0rtt_accepted_total` | ADR 015 |
| `quic_0rtt_rejected_total` | `decdn_quic_0rtt_rejected_total` | ADR 015 |
| `probe_fanout_latency_seconds` | `decdn_probe_fanout_latency_seconds` | ADR 015 |
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

## Consequences

**Positive:**

- Operators have a single reference for dashboard configuration — no more hunting
  across 8 ADRs for metric names.
- Mandatory M-tier slash-risk metrics are enforced at startup, ensuring operators
  cannot accidentally run without slash-risk visibility.
- Canonical `decdn_` prefix and `_total` suffix allow automated registry validation
  (e.g., a CI check that all exported metric names match the registry).
- Alert thresholds provide actionable defaults for new operators.
- The `/health` endpoint integrates with standard load balancers and container
  orchestration readiness probes without parsing Prometheus text.

**Negative:**

- Existing ADRs reference informal metric names that differ from the canonical names
  defined here. The cross-reference table (Section 6) documents all renames; no ADR is
  retroactively edited to avoid churn on draft documents, but implementations must use
  the canonical names from this ADR.
- Mandatory metrics add startup complexity — the node must successfully initialize all
  M-tier metric collectors before accepting connections. This is a small overhead in
  exchange for guaranteed observability.

## Future Work

- **Grafana dashboard template.** A reference `dashboard.json` for the PoC testnet,
  pre-wired to all M-tier metrics with the recommended alert thresholds, would reduce
  new operator setup time from hours to minutes.
- **Prometheus alerting rules file.** A `decdn_alerts.yml` with the thresholds from
  Section 2.1 as Alertmanager rules.
- **OpenMetrics migration.** Prometheus text format 0.0.4 is sufficient for PoC; the
  OpenMetrics exposition format (used by `prometheus_client` crate's `MetricsEncoder`)
  adds exemplars and native histograms — evaluate at production scale.
