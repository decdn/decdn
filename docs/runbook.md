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
    (every served hash is now potentially slashable). **Note:** these two
    blacklist alerts cannot fire yet — the node emits no blacklist-sync
    metrics; see [ContentBlacklist compliance](#contentblacklist-compliance).
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

- `decdn_staker_set_watcher_down_seconds` climbing (with
  `decdn_staker_set_watcher_restarts_total` advancing) is the chain-side
  symptom of the shared `capacity-bond` watcher (#783, #1226): a mid-run RPC
  outage stops the watcher following `CapacityBond` events. Since #1226 one loop
  feeds **both** projections, so this pair is the health of both:
  - The cached active-staker set drifts from chain state and mis-sheds
    stake-lane probes / DHT `Store`s.
  - Fresh `NodeRegistered` bindings are missed, so those providers become
    unpayable and are silently skipped — the pull path's reachable-provider set
    may be capped by stale bindings. (Only on nodes with
    `cache.node_to_node_pull_through_enabled` on; the projection is not built
    otherwise.)

  This watcher is **not** covered by `decdn_rpc_healthy`. See
  [appendix-observability § Active-Staker Set Watcher Metrics](../adr/appendix-observability.md#active-staker-set-watcher-metrics).

**Remediate:**

1. Rotate `blockchain.rpc_url` to a secondary provider in the config and
   restart.
2. For Arbitrum testnet, fall back to a public RPC
   (`https://sepolia-rollup.arbitrum.io/rpc`) — rate-limited; for production,
   use a paid provider.
3. After recovery, confirm RPC reachability is restored before considering the
   incident closed. (The `decdn_blacklist_sync_lag_seconds` gauge that would
   track this is not emitted by the node yet — see
   [ContentBlacklist compliance](#contentblacklist-compliance).)

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
    versions missed. (Both blacklist alerts above are pre-wired but not yet
    emitted by the node — see
    [ContentBlacklist compliance](#contentblacklist-compliance).)
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

## ContentBlacklist compliance

**Symptoms:** governance or a regional body has published a blocked BLAKE3
hash on-chain via `ContentBlacklist`, and this node may still be caching,
announcing, or serving it. Serving a globally blocked hash is a slashable
offense (see [Slashing risk](#slashing-risk)). Protocol semantics:
[ADR 011](../adr/011-content-takedown.md),
[ADR 031](../adr/031-content-blacklist-appeals-contract.md).

**How a node is meant to learn about a blocked hash.**
[ADR 011 § Node Behavior](../adr/011-content-takedown.md#node-behavior)
*designs* a sync loop: poll `getBlacklistVersion()` on `blacklist_poll_interval`
(10 minutes), fetch the new entries on a version bump, then **in order** stop
publishing DHT records, stop serving (`StreamRequest` → `HashBlacklisted`), and
evict. **None of this is implemented at PoC, on either side.** The deployed
`ContentBlacklist` exposes no `getBlacklistVersion()` accessor (so even the
delta-sync query the ADR assumes would need a contract change), and the node
has no blacklist watcher in `crates/`. Alerts for
`decdn_blacklist_sync_lag_seconds` / `decdn_blacklist_version_behind` exist in
`monitoring/prometheus-alerts.yml` (and the sync-lag panel in the Grafana
dashboard), but the underlying metrics are **not emitted by the node**. Until
the watcher and metrics land, hash-level takedown is a
**manual operator action** — see Remediate below.

Operator-*level* blacklisting is different and **is** enforced today: when
governance calls `ContentBlacklist.addOperator`, the contract calls
`CapacityBond.ejectNode`, which emits both `EjectedByBlacklist` and a
nodeId-indexed `NodeAutoEjected`. The node's staker-set watcher follows
`NodeAutoEjected` and drops the node from its active set live; the
`EjectedByBlacklist` event is deliberately not subscribed because
`NodeAutoEjected` already carries the nodeId (see
`crates/node/src/dht/chain_staker_set.rs`). No operator action is required.

**Detect:**

- Alerts in `monitoring/prometheus-alerts.yml` (verbatim names) — **note
  these will not fire until the node emits the underlying metrics**:
  - `DecdnBlacklistSyncLagWarning` — blacklist poll lagging > 10 minutes.
  - `DecdnBlacklistSyncLagCritical` — sync stale > 30 minutes (every served
    hash is now potentially slashable).
  - `DecdnBlacklistVersionFarBehind` — multiple blacklist versions missed.
- Manual on-chain check (works today): query `ContentBlacklist` directly with
  the hash from the takedown notice — `isHashBlacklisted(hash)` for global
  entries, `isHashBlacklistedInRegion(hash, region)` for a regional entry. The
  `region` argument is `bytes32`, not a string: global scope is the sentinel
  `bytes32("GLOBAL")` (with `cast`, pass the literal `GLOBAL` right-padded to
  32 bytes). `getHashEntry(region, hash)` returns the raw `(addedAt, suspended)`
  that the slashing predicate reads.

**Remediate:**

1. **Purge the blob.** Evict it from this node's cache:

   ```bash
   decdn node evict <hash> --dry-run   # pre-flight: size, pin status,
                                       # already-evicted flag — no mutation
   decdn node evict <hash>             # durable logical eviction
   ```

   The eviction is *logical*: it is recorded in `<cache_dir>/evicted.log`
   (durable across `decdn-node run` restarts) and blocks subsequent serves;
   the underlying bytes are reclaimed by the next GC sweep (`#518`,
   `cache.gc_interval_sec`). `--dry-run` is the pre-flight check before a
   DMCA/takedown action — confirm you have the right blob and catch a pinned
   hash or an idempotent re-run.

2. **You have a grace window, but evict immediately anyway.**
   [ADR 011 § Compliance Window](../adr/011-content-takedown.md#compliance-window)
   gives every entry an `effectiveAt = addedAt + window` — 24 hours for a
   governance or regional add, 2 hours for an emergency multisig add — and
   `SlashJudge` anchors slash eligibility to `effectiveAt`, not `addedAt` (see
   `_checkBlacklistedBefore` in `contracts/src/SlashJudge.sol`). A delivery whose
   signed response timestamp is at or before `effectiveAt` is not slashable.
   Practical consequences:
   - The window is a safety margin for poll latency and downtime, not a licence.
     Evict on discovery; do not schedule against the deadline.
   - Both windows are governance-tunable within `[1 hour, 7 days]`, so read
     `complianceWindow()` / `emergencyComplianceWindow()` rather than assuming
     24h/2h. The value is stamped on the entry at add time, so an entry keeps
     the window that was in force when it landed — check the entry's own
     `effectiveAt` via `getHashEntry(region, hash)`.
   - Slash exposure comes from **global and in-scope regional** entries alike
     (ADR 030 scope: global ∪ current region ∪ ripening previous region).
   - An emergency entry (`emergency == true`) auto-expires — 14 days for
     `GENERAL`, 90 for `CSAM`/`TERRORIST` — unless governance ratifies it with
     `addHashGlobal`. It stops being slashable at that deadline whether or not
     anyone has called `expireEmergencyEntry` to materialize the removal.
   - An entry under active appeal (`suspended == true`) is not slashable —
     `isHashBlacklisted` returns `false` and `SlashJudge` rejects the challenge —
     but `addedAt` and `effectiveAt` are both preserved when the suspension
     clears, so the grace does **not** restart: re-evict before serving again.

3. **If you believe the entry is wrong, appeal it — don't just keep serving.**
   `openBlacklistAppeal(hash, region, evidenceBundleHash, standingPath, namespaceId)`
   opens an appeal against an `appealBond` deposit (governance-set; testnet deploy
   default **100 TOKEN**, bounds `[50, 5000]`) within a 14-day filing window from
   `addedAt`. The emergency multisig (`EMERGENCY_MULTISIG_ROLE`) fast-tracks —
   suspending the entry for interim relief — or rejects; DecdnGovernor
   (`GOVERNANCE_ROLE`) then ratifies the removal or reverses. Bond outcomes in
   the deployed contract: **refunded only on ratification**; **burned on
   rejection, reversal, and lapse** (`cleanupExpiredBlacklistAppeal`). The
   declared `standingPath` is **verified at filing** (audit I-3): **`Publisher`**
   requires that `namespaceId` is a namespace you own which has claimed the hash
   (ignored on the other paths); **`Operator`** requires your current attested
   region to match the entry's region; **`TokenHolder`** needs no extra
   credential — the escrowed appeal bond is the standing, so there is no balance
   threshold and no synthetic-standing clawback. PoC caveat vs. the ADR 011/031
   design: any non-zero `region` is appealable (global included — the
   regional-only restriction is not enforced). Global entries also remain
   removable via the slow-path DecdnGovernor `removeHashGlobal` override. Design
   intent:
   [ADR 011 § Blacklist Entry Appeals](../adr/011-content-takedown.md#blacklist-entry-appeals),
   [ADR 031](../adr/031-content-blacklist-appeals-contract.md).

4. **A slash you already took is a separate matter.** Appealing the blacklist
   *entry* (step 3) removes the entry; it does **not** refund a slash you
   already incurred for serving the hash. Restitution for the slash itself —
   e.g. you were offline during the window — is the
   [ADR 028 SlashAppeal](../adr/028-slashing-appeals.md) path, with its own
   bond and evidence rules. Operational-failure evidence is inadmissible on
   the content-policy path and vice versa.

## Gossip / peer table degraded

**Symptoms:** node not discovering peers; clients stop selecting it;
`NodeAnnounce` from this node not seen by neighbours.

**Detect:**

- `DecdnPeerTableThin` (warning) — `decdn_gossip_peer_table_size < 3` for
  10 minutes (the alert and metric are registered as
  `decdn_gossip_peer_table_size` in `crates/node/src/metrics.rs`).
- `DecdnNoActiveStreams` (warning) — `decdn_streams_active == 0` across all
  directions for 15 minutes (gossip degradation is one of several causes).
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
