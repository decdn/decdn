# Operator Runbook

First-response steps for the failure modes operators hit most. Each section
names the symptom an operator sees first, then the metric or alert that
surfaces it, then the action to take. For the underlying protocol semantics,
follow the ADR cross-references.

Companion assets:

- [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json)
- [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml)
- [`adr/architecture.md`](../adr/architecture.md)
- [`adr/appendix-observability.md`](../adr/appendix-observability.md) — full
  metric catalogue and alert rationale.

## Disk full

**Symptoms:** origin pull-through fails with I/O errors; cache writes blocked;
`decdn_streams_failed_total{reason="other"}` rises; new content cannot be
admitted.

**Detect:**

- `df -h $cache_dir` against the configured `cache.cache_dir`.
- `decdn_cache_bytes` gauge approaching `cache.cache_size_mb × 1_000_000`.
- No dedicated alert today; covered by the `decdn_cache_bytes` Grafana panel
  and by `DecdnHighStreamErrorRate` once origin writes start to fail.

**Remediate:**

1. Raise `cache.cache_size_mb` in the config file, **or** prune the cache
   directory manually, **or** move `cache.cache_dir` to a larger volume.
2. Restart the node to pick up the new value. SIGHUP reload for live re-tune
   lands with [#236](https://github.com/decdn/decdn/issues/236); until then,
   restart is the only path.
3. If pruning manually, prefer evicting whole blob files — never truncate.
   Truncated bytes will fail BLAKE3 verification and surface as
   `decdn_streams_failed_total{reason="hash_mismatch"}`, which is a slashing
   signal (see [Slashing risk](#slashing-risk)).

## RPC unreachable

**Symptoms:** startup fails fast with the existing `check_rpc_reachability`
error in `crates/node/src/runtime/mod.rs`; running nodes stop receiving
blacklist updates and stop being able to settle channels. Once
[#283](https://github.com/decdn/decdn/issues/283) lands, the
`decdn_rpc_healthy` gauge drops to 0.

**Detect:**

- Existing alerts in `monitoring/prometheus-alerts.yml`:
  - `DecdnBlacklistSyncLagWarning` — blacklist poll lagging > 10 minutes.
  - `DecdnBlacklistSyncLagCritical` — blacklist sync stale > 30 minutes
    (every served hash is now potentially slashable).
- Direct probe:

  ```bash
  curl -sS \
    -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}' \
    "$RPC_URL"
  ```

- Provider status page.

**Remediate:**

1. Rotate `blockchain.rpc_url` to a secondary provider in the config and
   restart.
2. For Arbitrum testnet, fall back to a public RPC
   (`https://sepolia-rollup.arbitrum.io/rpc`) — rate-limited; for production,
   use a paid provider.
3. After recovery, confirm `decdn_blacklist_sync_lag_seconds` returns to
   baseline before considering the incident closed.

## Slashing risk

**Triggers** (per [ADR 008](../adr/008-reputation.md),
[ADR 011](../adr/011-content-takedown.md),
[ADR 014](../adr/014-on-chain-verification.md)):

- **Phantom announce** — claiming a hash you don't actually hold (signed
  `has_blob: true` then evicted within `probe_hold_duration`, or refused to
  serve when the stream request arrived).
- **Missed challenge response** — failure to respond to a slash challenge
  within the 24-hour window (ADR 014).
- **Blacklist violation** — serving content after the on-chain blacklist
  added it.

**Detect:**

- Alerts in `monitoring/prometheus-alerts.yml` (verbatim names):
  - `DecdnProbeHoldViolations` (critical) — phantom-announcement evidence
    is being produced.
  - `DecdnSlashEvidenceExposure` (critical) — node served bytes for a hash
    inside the slash window after `has_blob: true`.
  - `DecdnBlacklistSyncLagCritical` (critical) — blacklist > 30 minutes
    stale; serving any recently blacklisted hash is now slashable.
  - `DecdnBlacklistVersionFarBehind` (critical) — multiple blacklist
    versions missed.
  - `DecdnRateBoundsClamp` (warning) — `rate_per_mb` outside governance
    bounds; not directly slashable but indicates configuration drift.
- Grafana: the slash-safety row in `monitoring/grafana-dashboard.json`.

**Remediate:**

1. **If the trigger was a missed challenge response:** investigate the root
   cause first (RPC outage, watchtower offline, signing host crash, clock
   skew). Fix that before submitting evidence — counter-evidence filed while
   the underlying problem persists will not stop the next strike.
2. **Submit counter-evidence on-chain within the 24-hour challenge window.**
   The procedure is defined in
   [ADR 014](../adr/014-on-chain-verification.md); deadlines are absolute
   wall-clock — once the window closes the slash is final.
3. **For phantom announces (`DecdnProbeHoldViolations`):** reduce admission
   pressure or raise `max_probe_holds` per the alert annotation; investigate
   OOM. See
   [ADR 005 § Probe-Triggered Eviction Hold](../adr/005-protocol.md#probe-triggered-eviction-hold).
4. **For self-detected exposure (`DecdnSlashEvidenceExposure`):** stop the
   node immediately and file a bug — this signals a code-path defect, not an
   operator misconfiguration.

## Gossip / peer table degraded

**Symptoms:** node not discovering peers; clients stop selecting it;
`NodeAnnounce` from this node not seen by neighbours.

**Detect:**

- `DecdnPeerTableThin` (warning) — `decdn_peer_table_size < 3` for 10
  minutes.
- `DecdnNoActiveStreams` (warning) — `decdn_streams_active == 0` across all
  directions for 15 minutes (gossip degradation is one of several causes).
- `decdn_iroh_*` transport-level metrics for connection failures (registered
  under the `decdn_iroh_` prefix per `crates/node/src/metrics.rs`).

**Causes:** NAT traversal failure, firewall blocking the QUIC port, stale
bootstrap peer list, host clock skew breaking TLS.

**Remediate:**

1. Verify the QUIC bind port is reachable from the public internet. If
   bound to a private interface, switch to `0.0.0.0` or configure NAT/port
   forwarding.
2. Confirm the bootstrap peer list in the node config is current — old peer
   IDs that have rotated their iroh keys (see
   [`adr/appendix-operator-key-rotation.md`](../adr/appendix-operator-key-rotation.md))
   will never connect.
3. Inspect `decdn_iroh_magicsock_*` metrics for connect failures; high
   failure rates with low success rates indicate NAT/firewall problems
   rather than gossip-layer issues.
4. Verify host time is synchronised (`chronyc tracking` /
   `timedatectl status`). Skew greater than the QUIC handshake tolerance
   breaks every connection silently.

## Where to ask for help

- File or browse issues:
  [github.com/decdn/decdn/issues](https://github.com/decdn/decdn/issues).
- Architecture overview and ADR index:
  [`adr/architecture.md`](../adr/architecture.md).
- Observability metric catalogue:
  [`adr/appendix-observability.md`](../adr/appendix-observability.md).
- Monitoring assets:
  [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json),
  [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml).
