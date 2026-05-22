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
new content cannot be admitted.

**Detect:**

- `df -h $cache_dir` against the configured `cache.cache_dir` — this is the
  authoritative signal today.
- No dedicated cache-capacity metric is exported yet (the cache crate has
  no metrics; `cache.cache_size_mb` is parsed but the engine does not
  enforce it). Track host-level disk usage until cache instrumentation
  lands.
- Existing alert `DecdnHighStreamErrorRate` will fire downstream once
  origin writes start to fail, but it is not capacity-specific.

**Remediate:**

1. Prune the cache directory manually, **or** move `cache.cache_dir` to a
   larger volume, **or** provision more disk on the host. Raising
   `cache.cache_size_mb` is a no-op today: the engine accepts only
   `max_blob_size_mb` as input and performs no eviction.
2. If you moved `cache.cache_dir`, restart the node to pick up the new
   value. SIGHUP reload for live re-tune lands with
   [#236](https://github.com/decdn/decdn/issues/236); until then, restart
   is the only path for a `cache_dir` change.
3. If pruning manually, prefer evicting whole blob files — never truncate.
   Truncated bytes will fail BLAKE3 verification on read, which is a
   slashing signal (see [Slashing risk](#slashing-risk)).

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

  A `405 Method Not Allowed` response indicates an invalid endpoint URL
  (e.g. wrong path) rather than a provider outage — JSON-RPC 2.0 requires
  `POST`, so a 405 is the server saying the URL is wrong, not that it is
  down.

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
   cause first (RPC outage, dispute monitor down, signing host crash, clock
   skew). Fix that before submitting evidence — counter-evidence filed while
   the underlying problem persists will not stop the next strike.
2. **Submit counter-evidence on-chain within the 24-hour challenge window.**
   The procedure is defined in
   [ADR 014](../adr/014-on-chain-verification.md); deadlines are absolute
   wall-clock — once the window closes the slash is final.
3. **For phantom announces (`DecdnProbeHoldViolations`):** investigate OOM
   and resource pressure on the node — the violations indicate that signed
   `has_blob: true` answers are not being honoured by the eviction-hold
   mechanism. There is no operator-tunable knob for hold capacity in
   `crates/node/src/config/types.rs` today; the alert annotations reference
   `max_probe_holds` but it is not yet a config field. Until that lands,
   the practical levers are reducing offered load, increasing host
   memory, and following
   [ADR 005 § Probe-Triggered Eviction Hold](../adr/005-protocol.md#probe-triggered-eviction-hold)
   for context. See also
   [ADR 008](../adr/008-reputation.md) for reputation impact.
4. **For self-detected exposure (`DecdnSlashEvidenceExposure`):** stop the
   node immediately and file a bug — this signals a code-path defect, not an
   operator misconfiguration.

## Gossip / peer table degraded

**Symptoms:** node not discovering peers; clients stop selecting it;
`NodeAnnounce` from this node not seen by neighbours.

**Detect:**

- `DecdnPeerTableThin` (warning) — `decdn_gossip_peer_table_size < 3` for
  10 minutes (the alert and metric are registered as
  `decdn_gossip_peer_table_size` in `crates/node/src/metrics.rs`).
- `DecdnNoActiveStreams` (warning) — `decdn_streams_active == 0` across all
  directions for 15 minutes (gossip degradation is one of several causes;
  note the underlying `decdn_streams_active` metric is not yet exported by
  the node — track in `monitoring/prometheus-alerts.yml` until stream
  instrumentation lands).
- `decdn_iroh_*` transport-level metrics for connection failures (registered
  under the `decdn_iroh_` prefix per `crates/node/src/metrics.rs`).

**Causes:** NAT traversal failure, firewall blocking the QUIC port, peer
identity rotation, host clock skew breaking TLS.

**Remediate:**

1. Verify the configured `network.bind_port` is reachable from the public
   internet. The node already binds `Ipv4Addr::UNSPECIFIED` (`0.0.0.0`) in
   `crates/node/src/runtime/mod.rs::build_endpoint`, so there is no
   operator-tunable bind interface; fixes here are at the firewall, NAT,
   or port-forwarding layer.
2. If a peer's iroh key was rotated, neighbours referring to its prior
   `NodeId` will not reconnect until they re-discover the new identity via
   gossip. There is no static bootstrap-peer list in the node config
   (`crates/node/src/config/types.rs` exposes only `network.bind_port` and
   `network.relay_url`); follow
   [`adr/appendix-operator-key-rotation.md`](../adr/appendix-operator-key-rotation.md)
   for the staged-rotation procedure that keeps connectivity continuous.
3. Inspect `decdn_iroh_magicsock_*` metrics for connect failures; high
   failure rates with low success rates indicate NAT/firewall problems
   rather than gossip-layer issues.
4. Verify host time is synchronised (`chronyc tracking` /
   `timedatectl status`). Skew greater than the QUIC handshake tolerance
   breaks every connection silently.

## Testnet faucet

**⚠️ Testnet only.** `contracts/testnet/TestnetFaucet.sol` is **not** part of
the audited production surface
([#452](https://github.com/decdn/decdn/issues/452)) and **must not be
deployed to mainnet**. The CI gate in
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) (`solidity
build+test` job) fails any PR that references `TestnetFaucet` outside
`contracts/testnet/` or `contracts/script/TestnetFaucet.s.sol`.

**What it is.** A per-address TOKEN dispenser. Each address may call
`claim()` to receive `claimAmount` TOKEN once per `cooldown` seconds.
Defaults at deploy: `1_000 TOKEN` per `24h`.

**Pre-funding.** The constructor pulls `initialFunding` TOKEN from a
`treasury` address via `safeTransferFrom`. The treasury must `approve` the
predicted faucet address before deployment; the
[deploy script](../contracts/script/TestnetFaucet.s.sol) does this in a
single broadcast under the treasury sender. Source the funding from the
initial-holder multisig — there is no other TOKEN source pre-TGE.

**USDC is not in scope.** Circle operates the canonical Sepolia USDC
faucet: <https://developers.circle.com/stablecoins/docs/usdc-on-testnet>.
The deCDN faucet only dispenses TOKEN.

**Tuning at runtime.** `GOVERNANCE_ROLE` can adjust `claimAmount` and
`cooldown` without redeploying. `PAUSER_ROLE` can halt claims via `pause()`
(`withdraw` is intentionally pause-independent so governance can drain a
paused faucet without unpausing it).

**Abuse / drain escape hatch.** `withdraw(address to, uint256 amount)` is
gated by `GOVERNANCE_ROLE` and sweeps any portion of the faucet's balance
to a chosen destination. Use this to reclaim funds if the faucet is being
abused (paired with `pause()`) or to retire it at the end of a testnet
campaign.

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
