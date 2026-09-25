# Operator Runbook

First-response steps for the failure modes operators hit most. Each section
names the symptom an operator sees first, then the metric or alert that
surfaces it, then the action to take. For the underlying protocol semantics,
follow the ADR cross-references.

Companion assets:

- [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) —
  fleet overview. Start here.
- [`monitoring/dashboard-delivery.json`](../monitoring/dashboard-delivery.json) —
  serve leg, pull leg, cache and origin. Every refusal and failure reason.
- [`monitoring/dashboard-chain.json`](../monitoring/dashboard-chain.json) —
  watcher liveness, registries, and both sides of the payment flow.
- [`monitoring/dashboard-node.json`](../monitoring/dashboard-node.json) —
  one node at a time: host, transport, DHT, logs and traces.
- [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml) —
  every rule carries a `component` label and, where a section below matches, a
  `runbook_url` annotation pointing straight at it.
- [`adr/architecture.md`](../adr/architecture.md)
- [`adr/appendix-observability.md`](../adr/appendix-observability.md) — full
  metric catalogue and alert rationale.

## Disk full

**Symptoms:** origin pull-through fails with I/O errors; cache writes blocked;
new content cannot be admitted.

**Detect:**

- `df -h $cache_dir` against the configured `cache.cache_dir` — the
  host-level ground truth.
- In-node, `decdn_cache_bytes` (current on-disk footprint) against
  `decdn_cache_size_limit_bytes` (the configured `cache.cache_size_mb`
  ceiling) shows headroom, and `decdn_cache_gc_bytes_reclaimed_total` shows
  eviction keeping up. A footprint pinned at the ceiling with reclaim flat
  means eviction is starved — confirm `cache.gc_interval_sec` is non-zero.
- Existing alert `DecdnHighStreamErrorRate` will fire downstream once
  origin writes start to fail, but it is not capacity-specific.

**Remediate:**

1. Prune the cache directory manually, **or** move `cache.cache_dir` to a
   larger volume, **or** provision more disk on the host. `cache.cache_size_mb`
   is enforced by the eviction driver (#1173) — to make the node hold less,
   **lower** it; raising it lets the cache grow and worsens disk pressure.
   Enforcement is indirect: the driver releases LRU blobs and the iroh-blobs GC
   sweep (`cache.gc_interval_sec`, default 300s) reclaims the disk, so a write
   burst can overshoot the ceiling until a sweep catches up — and with
   `cache.gc_interval_sec = 0` nothing is reclaimed and the ceiling is
   unenforceable (the node warns at startup).
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

- Existing alerts in `monitoring/prometheus-alerts.yml`: a dead endpoint stalls
  every chain-event watcher at once, so expect up to five stalled alerts to
  fire together — `DecdnBlacklistWatcherStalled`, `DecdnSlashWatcherStalled`,
  `DecdnStakerSetWatcherStalled`, `DecdnFeeSharesWatcherStalled` (only where
  the fee-shares watcher is registered), and `DecdnSettlementWatcherStalled`.
  Several firing together points at the RPC endpoint
  rather than at any one watcher. Each fires on the *age* of that watcher's last
  successful tick (guarded against the pre-first-tick sentinel) **or** on
  `decdn_*_watcher_down_seconds`, which counts from the first failed poll tick —
  so a watcher that never established at all fires too.
  `DecdnBlacklistWatcherStalled` is the one carrying slash risk; see
  [ContentBlacklist compliance](#contentblacklist-compliance).
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

- `RPC provider rejected the eth_getLogs block range; shrinking the poll
  window` `warn!` lines mean the provider caps `eth_getLogs` (dRPC
  `ranges over 10000 blocks are not supported`, Alchemy
  `up to a 10 block range`, Infura `more than 10000 results`). The poller sets
  its window to half the rejected window and retries at once, and doubles it
  back after 32 accepted windows, up to the ceiling. `decdn_chain_get_logs_span`
  shows the current span and `decdn_chain_get_logs_range_rejections_total` the
  rejections; the "eth_getLogs window span" panel in
  `monitoring/dashboard-chain.json` plots both. One old rejection is a
  transient: the span climbs back on its own. Rejections that keep coming mean
  a cap. Then set `blockchain.get_logs_max_block_span` to the lowest span the
  gauge (or the shrink `warn!`) reaches while they come, **not** to the number
  in the provider's message — dRPC's free tier says 10 000 but rejects about
  150. Until you do, each regrow probes the cap again at the cost of one
  rejected request. A cap below one poll interval of blocks (Arbitrum makes
  about 4 blocks per second, so about 30 blocks per 7 s tick) still works, but
  each tick then costs several `eth_getLogs` requests against the provider's
  quota.
- `multiplexed poller tick error` `warn!` lines on `get_logs` while
  `decdn_rpc_healthy` stays 1 mean the watchers cannot read events although
  the endpoint answers. `rejects even a one-block eth_getLogs window` means the
  provider cannot serve the watchers at all: change provider. Repeated
  `get_logs` timeouts, or a range or size error the poller does not recognise
  (it logs no shrink line), mean the windows are too wide for this provider:
  lower `blockchain.get_logs_max_block_span` and restart.
- `DecdnChainWatcherFlapping` means a node's chain watchers fail and recover
  again and again (four or more failure windows in 30 minutes, held for 45
  minutes; a single burst does not fire it). Each recovery resets the stalled
  alerts, so they stay quiet while the watchers lag head. Treat it as an
  unreliable RPC provider: read the `multiplexed poller tick error` lines for
  the cause, and check `decdn_chain_get_logs_range_rejections_total` for an
  `eth_getLogs` cap.

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
   incident closed. (Blacklist-watcher liveness is tracked by
   `decdn_blacklist_watcher_last_tick_timestamp_seconds` — see
   [ContentBlacklist compliance](#contentblacklist-compliance).)

## Keystore will not unlock

**Symptoms:** a command or `decdn-node` startup exits with
`no keystore password source available (...)`, or with
`failed to decrypt eth keystore at <path>`.

**Sources, in precedence order.** Every command that touches the Ethereum
keystore consults the same three, and takes the first one that is **present**:

1. The `DECDN_KEYSTORE_PASSWORD` environment variable.
2. A password file: `--keystore-password-file` (env
   `DECDN_KEYSTORE_PASSWORD_FILE`) on `decdn-node run`, `decdn fetch`,
   `decdn bundle pull`, `decdn pool`, the operator commands, and
   `decdn key-gen`. One trailing newline is stripped; other whitespace is
   part of the password.
3. An interactive prompt, when `stdin` is a TTY.

**Presence decides, not content.** A variable that is set supplies its value
even when that value is empty, and a file that exists supplies its contents even
when they are empty — an empty password is a real password. Only an *unset*
variable, a path that does *not exist*, and a non-TTY `stdin` fall through to
the next source. A path that exists but cannot be read (a directory, a
permission denial, non-UTF-8 contents) is an error rather than a skipped source.

**`decdn key-gen` refuses an empty password from `DECDN_KEYSTORE_PASSWORD`.**
Creating a keystore is unverifiable — a wrong password is only caught at the
next unlock — and a set-but-empty env var is almost always a shell expanding an
unset variable. To create an empty-password keystore on purpose, point
`--keystore-password-file` at an empty file. Loading (everything except
`key-gen`) still accepts an empty env password: a wrong one simply fails to
decrypt.

**Diagnose:**

- `no keystore password source available (...)` lists every source that fell
  through and why, including the password-file path that was tried. A path in
  that list is a typo, an unexpanded `~`, or a file the unit cannot see.
- `export DECDN_KEYSTORE_PASSWORD=` is *not* the same as
  `unset DECDN_KEYSTORE_PASSWORD`. The first means "use an empty password" and
  produces a decrypt failure against a keystore that has one; the second falls
  through to the password file.
- `failed to decrypt eth keystore at <path>` means a source *was* found and the
  password was wrong. A `warning: password file <path> ...` line says when a
  password file was not found and another source won, or when a set
  `DECDN_KEYSTORE_PASSWORD` shadowed the file — check it before changing the
  password file. The daemon logs the same warning through `tracing`.

**Resolve:**

- Headless hosts (systemd, containers) must reach source 1 or 2 — there is no
  TTY, so the prompt always falls through. Point
  `--keystore-password-file` at a `0o600` file the unit can read.
- Recreate the keystore with `decdn key-gen --force` only as a last resort: it
  archives the prior ciphertext to `keystore.json.bak.<ts>` but the node's
  on-chain identity changes with the key. Follow
  [the operator key-rotation appendix](../adr/appendix-operator-key-rotation.md)
  instead.

## Slashing risk

**Triggers** — the two, and only two, offenses `SlashJudge` adjudicates,
one per `submit*Challenge` entry point (per
[ADR 011](../adr/011-content-takedown.md),
[ADR 014](../adr/014-on-chain-verification.md)):

- **Rate manipulation** — charging a *higher* stream rate than this node
  itself probe-quoted, inside the 30-second slashing window. The direction
  matters: `SlashJudge` reverts `NotRateManipulation` unless the signed
  `StreamResponse` rate strictly exceeds the signed `ProbeResponse` rate, so
  quoting high and serving cheap is never an offense. Only a reprice
  *upward* has to wait out the window before you serve at the new rate.
- **Blacklist violation** — serving content after the blacklist entry's
  `effectiveAt` (`addedAt` plus the compliance window), not after `addedAt`
  — see [ContentBlacklist compliance](#contentblacklist-compliance).

There is no "missed response" offense: every slash is driven by the
operator's own signed messages, so a node that stays silent cannot be
slashed by an external adversary.

**Detect:**

- Alerts in `monitoring/prometheus-alerts.yml` (verbatim names):
  - `DecdnProbeHoldViolations` (critical) — hold budget exhausted, so
    present blobs are being advertised without an eviction hold and may be
    evicted before the pull arrives. Budget pressure and lost deliveries,
    not slash evidence; see step 4.
  - `DecdnBlacklistWatcherStalled` (critical) — the blacklist watcher has
    not completed a poll tick for several intervals, so the node may be
    serving content blacklisted since the last successful tick. It fires on
    the age of `decdn_blacklist_watcher_last_tick_timestamp_seconds`, or on
    `decdn_blacklist_watcher_down_seconds` when every poll tick fails. See
    [ContentBlacklist compliance](#contentblacklist-compliance).
  - `DecdnBlacklistEnforcementFailing` (critical) — a re-scope could not
    re-verify or evict every known deny-set entry, so a blacklisted hash may
    still be servable.
  - `DecdnRateBoundsClamp` (warning) — your configured `rate_per_mb` sits
    *below* the governance `deliveryFloor`, so every quote is being raised to
    the floor before signing. The clamp is raise-only; there is no ceiling. Not
    directly slashable, but it means you are not charging what you configured.
    Raise `payment.rate_per_mb` to at least the on-chain floor.
- Grafana: the slash-safety row in `monitoring/grafana-dashboard.json`.

**Remediate:**

1. **Fix the root cause before anything else.** Each offense has its own:
   blacklist watcher lag or a stale RPC endpoint (blacklist violation), a rate
   reconfiguration applied inside the 30-second window (rate manipulation).
   A slash you appeal while the underlying problem persists will not stop
   the next strike.
2. **There is no counter-evidence window.** If on-chain verification passes,
   the slash executes synchronously inside the challenger's
   `submit*Challenge` reveal — see
   [ADR 014 § Bond Handling](../adr/014-on-chain-verification.md#bond-handling).
   Nothing you send afterwards can undo it in-protocol; there is no
   counter-evidence deadline to race, only the appeal window in step 3.
3. **The recourse is a slash appeal.** File it with the CLI, which reads the
   governable bond, sets the TOKEN allowance for you, and surfaces the
   contract reverts verbatim:

   ```bash
   # Get the slashId — admin RPC on the loopback admin port (default 9191).
   curl -sS -H 'content-type: application/json' \
     -d '{"jsonrpc":"2.0","method":"admin_v1_slashes","params":[],"id":1}' \
     http://127.0.0.1:9191/

   decdn appeal slash <SLASH_ID> <EVIDENCE_BUNDLE_HASH> --dry-run
   ```

   Drop `--dry-run` to send. Under the hood this is
   `SlashAppeal.openSlashAppeal(slashId, evidenceBundleHash)`, callable only
   by the slashed operator, within `APPEAL_FILING_WINDOW` — **30 days from
   the slash**, enforced by `CapacityBond.markAppealOpen`, which reverts once
   it lapses (a protocol pause extends it by the paused duration). It costs
   `APPEAL_BOND` (1,000 TOKEN by default, governable within `[100, 10,000]`),
   burned in full if the appeal fails. The evidence bundle is assembled
   off-chain and committed by hash. You may appeal every slash in a cluster,
   but only **one appeal per 365 days can be granted**
   (`APPEAL_FREQUENCY_WINDOW`), so lead with the most clear-cut case. Flow,
   windows, and what counts as evidence:
   [ADR 028](../adr/028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation).
4. **For `DecdnProbeHoldViolations`:** this is lost deliveries, not slash
   evidence. The alert watches
   `decdn_probe_hold_unavailable_total{reason="exhausted"}` and fires when a
   blob is present but *un-holdable*
   because every hold slot is live. Holds are best-effort, so the node still
   signs `has_blob: true` and forgoes only the hold — the blob stays visible to
   the LRU driver and may be evicted before the requester's pull arrives, which
   costs a wasted round trip. No offense pairs a probe with a later miss, so
   this is never slash evidence. Raise the
   budget with `[cache] max_probe_holds` (`--max-probe-holds` /
   `DECDN_MAX_PROBE_HOLDS`, default 256); a busy node serving many peers
   should scale it up proportionally, while a node with a *small* cache
   should keep it under ~25% of cache capacity. **This field is
   restart-required** — a config reload logs "requires restart" and keeps the
   old value. Add host memory or shed load if the pressure is genuine. If the
   series is flat but `reason="disabled"` or `reason="stake_lane_reserved"` is
   climbing, the forgone holds are deliberate — a `max_probe_holds` of 0, or
   end-client probes shed to keep stake-lane headroom — and neither calls for
   this remedy. Note only `disabled` also suppresses the advertisement;
   `stake_lane_reserved` still answers `has_blob: true`.
   Background:
   [ADR 005 § Hold budget](../adr/005-protocol.md#hold-budget) and
   [Appendix: Observability](../adr/appendix-observability.md#slash-safety-metrics-all-mandatory).

## ContentBlacklist compliance

**Symptoms:** governance or a regional body has published a blocked BLAKE3
hash on-chain via `ContentBlacklist`, and this node may still be caching,
announcing, or serving it. Serving a globally blocked hash is a slashable
offense (see [Slashing risk](#slashing-risk)). Protocol semantics:
[ADR 011](../adr/011-content-takedown.md).

**How a node learns about a blocked hash.** `crates/node/src/blacklist_watcher.rs`
implements what
[ADR 011 § Node Behavior](../adr/011-content-takedown.md#node-behavior)
specifies. Absence of `blockchain.content_blacklist_address` is a hard startup
failure, so every paid-delivery node runs it. It enumerates the whole in-scope
deny-set from `ContentBlacklist` at one pinned block on boot —
`getScopeRegions(operator)` for the one-to-three regions in scope, then
`blacklistedHashes` / `blacklistedHashCount` per region — and follows
`HashBlacklisted` / `HashRemoved` on the shared log poller as a low-latency tail.
There is no version-checkpoint delta cursor: enumeration reads the full current
state on every boot, so a node returning after any downtime rebuilds the complete
deny-set from one snapshot. A periodic re-scope is the backstop: an operator
region or ripening change (`CapacityBond.updateRegion`) can bring a hash into
scope while emitting nothing on `ContentBlacklist` at all.

Enforcement is two writes per hash, in order — a governance-sourced *deny*, then
the *eviction*. `CacheEngine::evict` durably records the takedown in `evicted.log`
and cascades to every serving surface through `CacheEngine::refuses` — including
the DHT republisher dropping the hash on its next tick, the probe handler no
longer signing `has_blob: true`, and the client handler refusing to re-pull-fill
it. `evict` is sticky and works on absent hashes, so a hash blacklisted while the
node was offline is pre-blocked. The separate deny
records *why*, which `evicted.log` cannot express — otherwise a governance
takedown would answer `EvictedSinceProbe` while a local `[content] denied_hashes`
entry answers `HashBlacklisted`, and the difference tells a client which list a
hash is on (the fingerprint ADR 011 § `StreamRequest` Response forecloses). A
fail-closed readiness gate keeps every ALPN listener shut until the first
enumeration + enforcement pass completes, so a node refuses to serve rather than
serve un-enforced.

Manual eviction (Remediate below) is therefore for a takedown notice you received
out of band, or for confirming the watcher already acted — not the primary
mechanism.

Operator-*level* blacklisting runs on a different path, and takes effect in two
places. When governance calls `ContentBlacklist.addOperator`, the contract calls
`CapacityBond.ejectNode`, which emits both `EjectedByBlacklist` and a
nodeId-indexed `NodeAutoEjected`. The node's staker-set watcher follows
`NodeAutoEjected` and drops the node from its active set live; the
`EjectedByBlacklist` event is deliberately not subscribed because
`NodeAutoEjected` already carries the nodeId (see
`crates/node/src/dht/chain_staker_set.rs`). Separately, the blacklist watcher
folds the blacklisted-address union into the node's origin deny-set, so a
`StreamRequest` on a channel funded by a blacklisted operator is refused at the
delivery gate with `OriginDenied` and any in-flight stream is cut at the next MB
boundary (`crates/node/src/content_deny.rs`). Both are automatic — no operator
action is required.

**Detect:**

- Alerts in `monitoring/prometheus-alerts.yml` (verbatim names):
  - `DecdnBlacklistWatcherStalled` (critical) — no successful poll tick for
    several intervals; fires on the age of
    `decdn_blacklist_watcher_last_tick_timestamp_seconds`, or on
    `decdn_blacklist_watcher_down_seconds` when every poll tick fails.
  - `DecdnBlacklistEnforcementFailing` (critical) — a re-scope could not
    re-verify or evict every known entry, so a blacklisted hash may still be
    servable even while `decdn_blacklist_watcher_down_seconds` reads 0. The two
    answer different questions: deny-set enforced vs chain readable.
- Grafana: the "Oldest watcher tick" panel in
  `monitoring/grafana-dashboard.json`, and the per-watcher "Watcher tick age"
  table in `monitoring/dashboard-chain.json`.
- There is no sync-lag or version-delta coverage: the watcher rebuilds the
  deny-set by full enumeration and keeps no version cursor
  (`adr/011-content-takedown.md` § Node Behavior), so no such gauge exists. The
  tick-age gauge above is the liveness coverage.
- Manual on-chain check: query `ContentBlacklist` directly with
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

3. **If you believe the entry is wrong, escalate it — don't just keep serving.**
   There is no per-entry appeal contract; the entry comes off through the
   ordinary removal path, and **which path depends on the entry's scope.**

   - **Global entry** (`region == GLOBAL`): a DecdnGovernor `removeHashGlobal`
     proposal through the standard timelock (~10 days).
   - **Regional entry**: only the registered body for that region can remove it.
     `removeHashRegional` is `REGIONAL_BODY_ROLE`-gated *and* requires the
     caller to be that region's currently-registered, unsuspended body — so
     `removeHashGlobal` cannot reach it and neither can governance directly.
     Raise it with the body: for them it is one transaction, no vote.
   - **Regional entry, body will not act**: governance must replace the body —
     `deregisterRegionalBody(region)` then `registerRegionalBody(region, …)`
     with a body that will act, which then calls `removeHashRegional`. Two
     governance actions, so budget more than one timelock cycle.

   **Do not reach for `suspendRegionalBody` here.** It is the right tool for a
   body that is issuing bad entries, and the wrong one for a body that will not
   remove them: `_requireActiveBodyFor` gates `addHashRegional` **and**
   `removeHashRegional`, so suspending closes the only route by which that
   body's existing entries could come off. Suspension also retracts nothing —
   every entry already issued stays live, enforceable and slashable, because
   `_bodySuspended` is not consulted by `_isLive` or by `SlashJudge`. If a
   suspension is already in place and you need an entry removed, governance must
   first `unsuspendRegionalBody` or replace the body outright.

   In every case, keep the hash evicted until the removal actually lands: the
   entry is enforceable, and therefore slashable, right up to that point.
   Semantics:
   [ADR 011 § Removing a Wrongful Entry](../adr/011-content-takedown.md#removing-a-wrongful-entry).

4. **A slash you already took is a separate matter.** Getting the entry removed
   (step 3) stops future exposure; it does **not** refund a slash you already
   incurred for serving the hash. Restitution for the slash itself — e.g. you
   were offline during the window — is the
   [ADR 028 SlashAppeal](../adr/028-slashing-appeals.md) path, with its own bond
   and evidence rules, and it is the only appeal surface the protocol carries.

## Node-to-node pulls never succeed (buyer wallet or pool)

**Symptoms:** `decdn_node_pull_attempts_total` climbs while
`decdn_node_pull_success_total` stays flat; origin pull-through volume is high
against content the fleet already holds; the node serves inbound requests
normally, so nothing else alerts. `DecdnNodePullNeverSucceeds` fires after 6h.

The node's buyer leg is the one part of it that *spends*. It opens one
`PaymentPool` deposit of `blockchain.buyer_working_deposit_micro_usdc` (10 USDC
by default) and reuses it for every upstream pull. Two things break it.

**Cause (1): the operator wallet holds no USDC.** Every `openPool` reverts on
the ERC-20 transfer, so the node can pay no provider and every cache miss falls
through to origin.

1. Read `decdn_buyer_wallet_usdc`, and
   `decdn_pool_open_failures_insufficient_deposit_total` beside it. The gauge is
   refreshed once per reclaim sweep, so allow up to an hour after funding.
2. Send USDC to the operator address. Circle's Sepolia faucet is the testnet
   source (see [Testnet faucet](#testnet-faucet)); `decdn whoami` prints the
   address, given the keystore password.
3. No restart is needed — the next miss opens the pool.

A wallet with an old string-revert USDC reports the same shortfall through
`insufficient_deposit` rather than `contract_revert`; both mean fund the wallet.

**Cause (2): the node owns a pool it has forgotten.** The buyer-pool store lives
under `identity.data_dir`. If that directory is reset — a moved volume, a
re-provisioned host — the node loses the only local record that it owns a pool.
It reconciles against `PaymentPool.getPools` at startup and adopts the pool it
already owns, so this heals on its own; what follows is for confirming it, and
for recovering deposits an older build stranded before it did.

**Read the node's pools with `decdn node pools`, not `decdn pool list`.** Two
unrelated buyer stores can sit under `identity.data_dir`: the daemon's
`buyer.redb`, and a client-owned `buyer-pools.redb` that `decdn fetch` and
`decdn pool` use. On a clean node host only the first exists. A running daemon holds an exclusive lock on its file, so
nothing can read it from disk; `decdn node pools` asks the daemon over the admin
RPC. `decdn pool list --config /etc/decdn/node.toml` routes to the same place and
names the file it read, so `pools=0` is always attributable to one store or the
other. With the daemon **stopped** the lock is gone, and that same `pool list`
reads `buyer.redb` off disk and marks the listing `(read from disk; no daemon
running)` — the post-mortem route after a crash. A crashed daemon may leave the
file needing redb's repair pass, which only a writer runs; start `decdn-node`
once and read it again.

**`decdn pool list --all` answers the question the stores cannot.** It
enumerates `PaymentPool.getPools` by the keystore address and shows every pool
in every lifecycle state, with its on-chain `deposit`, `totalRedeemed`, reclaim
window, and whether the local record tracks it. That is the view that survives a
reset `identity.data_dir`, because it reads no local file to produce the list.
It is read-only, so it is not refused on a node's data dir the way `close --all`
is. When the two views disagree, believe the chain: the disagreement is the
diagnostic.

1. Run `decdn node pools`, and `decdn pool list --all` beside it. The first
   reports every pool the daemon tracks, its deposit, and the per-lane amounts
   already signed away; the second reports every pool the wallet owns on chain.
   A pool in the second and not the first is a stranded deposit. Compare both
   against
   `decdn_buyer_pool_adoption_failures_total`: any increment means the node
   could not tell whether it already owned a pool and was about to open a second
   one.
2. The node adopts the newest solvent `Open` pool at startup, and on **every**
   boot it enumerates `getPools` to name deposits it is not using. Look for
   `this node owns further open payment pools it is not using`, which lists the
   stranded ids, and — only when an adoption happened — `adopted this node's
   existing on-chain payment pool`. The stranded warning fires whether or not
   anything was adopted.

   Absence of that warning only means "nothing stranded" **if the sweep
   completed**. A failed enumeration logs `could not enumerate this node's
   on-chain pools`, an unreadable individual pool logs `could not read an owned
   pool's state`, and an unreadable buyer store skips reconciliation entirely
   (`buyer pool store read failed before on-chain reconciliation`). Any of the
   three means the answer is unknown, not clean — fix the cause and restart to
   re-run the sweep.
3. Recover those with `decdn pool close --pool <poolId>`, then
   `decdn pool reclaim --pool <poolId>` once the pool's `disputeWindow` has
   elapsed (48-72h, governance-set). Both commands run their on-chain leg
   normally from a node host but leave the daemon's row alone — they cannot
   write a store the daemon holds — and say so.

   If you closed the pool the daemon was using, restart `decdn-node`. Its
   bootstrap checks the tracked pool against the chain's open set, drops a row
   whose pool is no longer open, and adopts or opens a replacement
   (`the tracked buyer pool is no longer open on chain`). Until that restart the
   node keeps pinning its pulls to the closed pool. Its vouchers still redeem
   while the dispute window is open, and stop the moment it elapses — so the
   wedge is delayed, not absent, and the residual is refunded to the owner at
   `reclaim` either way. Confirm with `decdn node pools`.
4. `close --all` and `reclaim --all` are refused on a node's data dir. They
   enumerate from chain by keystore address, so on a node host they would close
   the pool the daemon is paying from right now. Name the stranded pools
   individually — `pool list --all` is how you find their ids, and it is allowed
   there because it sends no transaction and writes no pool record.
5. Lanes on an adopted pool resume from their on-chain watermark, so a provider
   still holding an unredeemed voucher is briefly ahead of the node and rejects
   its first vouchers. That clears on the provider's next redemption; no action.

## Refusing paying clients (insufficient deposit)

**Symptoms:** clients report a blob as missing that this node holds; delivery
revenue flat while requests arrive; `decdn fetch` against this node returns
`delivery refused: NotFound`.

**Detect:**

- `DecdnRefusingPayingClients` (warning) —
  `rate(decdn_serve_stream_rejected_insufficient_deposit_total[10m]) > 0.1` for
  15 minutes. A raw rate, not a ratio: the exporter has no serve-attempt counter
  to divide by.
- The node's own throttled `warn!` ("refusing paying clients: remaining channel
  deposit below the reserved cost"), at most one per 5 minutes, carrying the
  channel id, the remaining headroom, the amount reserved, and how many refusals it
  suppressed since the last line. Raise the log level to `debug` for one line per
  refusal — that one also carries the hash.
- `decdn_serve_stream_rejected_signer_floor_at_cap_total`, the sibling counter for
  a refusal where the pool is solvent but ONE capability signer has filled its share
  of the un-vouchered floor. It rises while `…insufficient_deposit_total` stays flat.
  This counter has **no alert rule**: it is dashboard- and log-only, so cause 3
  below is found by looking, not by being paged. The node's own `warn!` for it runs
  on a separate 5-minute window from the deposit line above, so neither arm can
  starve the other or mix its `suppressed` count.
- Grafana panel **Payment-class serve refusals (all collapse to NotFound)**. Its
  series are a subset of the ten reject reasons that share that one wire code, so
  a client cannot distinguish them and the server-side counters are the only place
  the cause exists at all.

**Causes:** three, with different remedies, and the `insufficient_deposit` counter
alone cannot tell the first two apart — this is why the log line exists:

1. **Clients genuinely running dry.** Their remaining deposit cannot cover one
   credit window. Nothing is wrong with this node. Expect a low background rate.
2. **This node's chain watcher is lagging an on-chain top-up.** The seller's view
   of a channel's deposit is only raised by observing `ChannelToppedUp`, so a
   client that just topped up is refused until the watcher catches up. Funded
   clients are being turned away and will route elsewhere.

3. **One signer is at its live floor cap.** The pool is solvent, but one capability
   signer is running more concurrent un-vouchered streams than its share of the
   floor covers
   ([ADR 003 § Pool solvency](../adr/003-payments.md#pool-solvency-and-the-refundable-floor-m)).
   `decdn_serve_stream_rejected_signer_floor_at_cap_total` rises while
   `…insufficient_deposit_total` stays flat, and the `warn!` names the signer. The
   cap holds no permanent charge — it is pure concurrency, released as each of the
   signer's streams pays, so it clears on its own and needs no top-up or reclaim.
   If a signer is honest and legitimately highly concurrent, the operator raises
   `blockchain.pool_floor_signer_live_windows`. That knob is **restart-required** —
   `blockchain.*` is not a reload section, so `decdn node reload` neither applies
   nor validates a new value. Rotating the session key is the remedy that takes
   effect immediately.

A fourth, rarer cause: the operator's own
`blockchain.buyer_working_deposit_micro_usdc` is too small for the *upstream*
rate, in which case this node is the one being refused. (The node opens
node-to-node channels at the working deposit and the proactive low-water refill
tops them back up on reuse, so a channel that runs short mid-pull reactively
tops up — see
[ADR 003 § Deposit Economics](../adr/003-payments.md#deposit-economics).) Look for
`decdn_node_pull_refused_unattributable_total` climbing toward
`decdn_node_pull_refused_total` instead.

**Remediate:**

1. Distinguish the two causes. Take a channel id from the `warn!` line and compare
   the node's view against the chain: `decdn node channels` reports the deposit the
   node believes, and `PaymentChannel.getChannel(channelId)` reports the truth. A
   disagreement is cause (2).
2. For cause (2), check watcher liveness —
   `decdn_settlement_watcher_last_tick_timestamp_seconds` should advance every
   `blockchain.event_poll_interval_ms`. A stalled watcher usually means the RPC
   endpoint is unreachable or rate-limiting; see [RPC unreachable](#rpc-unreachable).
3. For cause (1), no action. If the rate is high because many clients open dust
   channels deliberately, note that the refusal now happens *before* any fill
   (#1519), so it costs this node nothing beyond the signature.
4. Do not raise a deposit floor to "fix" this. `PaymentPool.minDeposit` ships
   dormant at 0 and is governance-set
   ([ADR 003 § Deposit Economics](../adr/003-payments.md#deposit-economics));
   service is bounded by what a deposit funds, which is exactly what this refusal
   is enforcing.

## Cache coalescing mutex poisoned

**Symptoms:** none that a user would notice. This is a latent-bug report, not a
degradation — the node keeps coalescing correctly.

**Detect:** `DecdnCacheInflightMutexPoisoned` fires on
`decdn_cache_inflight_mutex_poisoned_total > 0`, alongside a single
`inflight coalescing mutex poisoned` line at `ERROR`. The log is latched to one
line per process; the counter carries the true count of poisonings (the engine
clears the poison each time, so it counts incidents, not requests that followed
one).

**What it means:** some task panicked while holding the cache's in-flight
fill-coalescing map. The workspace anti-panic policy (`unwrap`/`expect`/`panic`
denied by clippy) means no production path is supposed to be able to do this, so
**any** nonzero value is a bug worth filing with the surrounding logs — not a
threshold to tune. The engine recovers the guard and coalescing survives, so
there is no operator action beyond reporting it.

**Why it is metered at all:** before #1517 the node discarded the poison and
fell through to an uncoalesced direct pull. Because nothing cleared the poison, and a
`std::sync::Mutex` stays poisoned until something does, one panic permanently
turned every concurrent request for a missing blob into its own origin fetch — unbounded
egress on a metered `http`/`s3` origin, and duplicate USDC vouchers upstream on
a node-to-node pull. The only symptom was
`decdn_cache_pull_through_bytes_total` climbing faster than request volume.

**Remediate:** nothing operational — **do not restart**. The engine calls
`Mutex::clear_poison` after recovering the guard, so the mutex is already
healthy and coalescing never stopped. File the panic with the surrounding logs.
The counter does not reset without a restart, which is deliberate: the evidence
should outlive the incident. Alert on `> 0` rather than `rate()` for the same
reason.

## Active-staker set repair not running

**Symptoms:** none directly visible. The node keeps serving, probing and
settling normally while the cached active-staker set drifts from chain state,
mis-shedding stake-lane probes and gating DHT `Store` admission on stale
membership.

**Detect:** `DecdnCapacityBondRegistryResyncFailing` on
`rate(decdn_capacity_bond_registry_resync_failures_total[15m]) > 0`, or
`DecdnCapacityBondRegistryResyncStale` on
`decdn_capacity_bond_registry_last_resync_timestamp_seconds` going stale, with
`capacity-bond registry resync failed; keeping current projections` at `WARN`.

**What it means:** the re-enumeration of `getRegisteredNodes` is the only
systematic repair for a drifted set. It deliberately reports success upward — an
error would mark the watcher route errored and stall event pickup — so the
counter is the only thing that moves when its read fails. The staleness rule
covers the other case: while the route is errored the repair is skipped
entirely, which emits nothing at all, not even the counter.

A route recovering from an errored tick forces a re-enumeration on the tick it
comes back, so a stale gauge on a healthy route means the reads themselves are
failing. Two limits on that trigger. It covers poll-level outages only: a
`nodeIdOf` resolution failure is returned as `Ok` and never errors the route, so
a membership change dropped that way waits for the cadence even while everything
else looks healthy — watch `decdn_staker_set_watcher_resolve_failures_total` for
it. And it is floored, so an endpoint that flaps every few seconds does not earn
a full enumeration per flap.

**Remediate:** check the RPC provider's `eth_call` path — the enumeration is
paginated `eth_call`s, not `eth_getLogs`, so it can fail while the event tail
still ticks. Correlate with `decdn_staker_set_watcher_down_seconds` and
`decdn_staker_set_watcher_resolve_failures_total`: down-seconds climbing means
the poll is failing too, and the repair will run on its own when the route
recovers. A restart re-enumerates at bootstrap and is the fallback if the
provider is healthy and the gauge stays stale.

## Origin rescan probes faulting

**Symptoms:** the node advertises less content than it holds. Peers and clients
find some of its blobs through the DHT but not others, with no serve errors —
what is missing was never announced.

**Detect:** `decdn_cache_origin_probe_failures_total` or
`decdn_cache_origin_enumerate_failures_total` nonzero, with
`rescan_origins: origin size probes faulted` or `rescan_origins: enumerate
failed` at `WARN`.

**What it means:** a rescan resolves each candidate against the origin — one
`HEAD`/`HeadObject`/stat per deduped candidate — to decide what this node
advertises. A throttle window or transport outage during that pass leaves
candidates unresolved.

A retry-eligible fault on a candidate an earlier pass indexed keeps that entry,
on the bet that the origin comes back. The bet has a cost: while it holds, the
node advertises content the origin may have stopped holding, and every request
for it is a refusal. A *permanent* fault — a revoked ACL, a symlink escape — is
not carried, because it would read the same on every rescan and hold the entry
until an operator intervened. A candidate first seen inside the fault window has
nothing to carry and is simply absent until a later rescan resolves it.

One case this counter cannot see: an HTTP origin reports a server-side 5xx as a
plain "not held", indistinguishable from a 404. Its *transport* faults, and
S3/R2 and filesystem origins throughout, do count.

The enumerate counter is the more severe of the two. A listing that fails
produces no candidates at all, so every hash discoverable only through that
origin leaves the announce set with no per-hash fault and nothing to carry
forward — only operator pins naming those hashes survive the pass.

**Remediate:** check the origin backend's own error and throttle rates. The next
successful rescan repairs the index on its own — at boot, on the
`cache.fs_rescan_interval_sec` timer, or on `decdn node reload`. Rescans are
serialized, so a slow one delays the next rather than stacking with it. A
sustained nonzero rate means the rescan cadence is racing a backend that cannot
serve it; raise `cache.fs_rescan_interval_sec` or the origin's request budget
rather than restarting.

## Delivery hash mismatch — metered on the pull leg only

**Symptoms:** clients report corrupt or rejected blobs; a peer is serving bytes
that do not hash to the requested root.

**Detect:** mismatch **is** counted, but only where this node is the *buyer*:

- `decdn_node_pull_corruption_total` — an upstream served hash-mismatched bytes
  for a paid pull. This is the one to watch: USDC was spent on wrong content.
- `decdn_node_pull_through_upstream_verify_failed_total` — the tee/window path's
  bao decoder rejected a chunk group (`VerifyFailed` or `HashMismatch`).
- `decdn_node_pull_through_errors_total` — the fill path's catch-all, shared
  with `BlobTooLarge` / `VerifyFailed` / `EvictionLimitExceeded`, so a rise here
  is a hint rather than a diagnosis.

None of the three has an alert or a panel in `monitoring/`. That is the
actionable gap.

**Attribute:** `decdn_node_pull_{success,unreachable,corruption}_total` carry no
peer label. To find the peer behind a rise, run with
`RUST_LOG=info,decdn::reputation=debug`. Every recorded reputation outcome then
logs one `decdn::reputation` event with the peer's `NodeId`, the outcome, and the
peer's new local score. The two `pull_through` counters above get no attribution
from this event. `RUST_LOG` applies at startup only: a config reload (SIGHUP or
`decdn node reload`) replaces the filter with `observability.log_level` and drops
the per-target setting, so restart the daemon with `RUST_LOG` set to restore it.

**The real blind spot is the serve leg.** `HashMismatch` is deliberately *not*
classified as a `HardFault` (`crates/node/src/handlers/client/mod.rs`) — it is
deterministic, and reporting it as "this node is broken" would steer clients off
a healthy node forever over one bad blob. The consequence is that
`miss_reason()` collapses it to `ServeRejectReason::CacheMiss`, so a serve-side
integrity fault is indistinguishable from a blob the node simply does not have,
inside the noisiest benign counter on the serve path
(`decdn_serve_stream_rejected_cache_miss_total`). `DecdnServeInternalErrorRate`
cannot fire on it either. Closing that needs a dedicated serve-side counter, not
a rule.

`monitoring/` carried a `DecdnHashMismatchAppearing` alert until #1513. It
queried `decdn_streams_failed_total{reason="hash_mismatch"}` — a series the
exporter has never emitted, and there is no outcome-labelled stream family to
rebuild it from — so it never fired. It was deleted rather than repointed at one
of the counters above, because the pull-leg counters mean something different
from what that alert claimed to watch.

**Escalate if seen:** a mismatch means either a corrupted local store or an
origin (or upstream peer) serving bytes that do not match their address. Both
are data-integrity faults — see [ADR 002](../adr/002-content-addressing.md).

## Gossip / peer table degraded

**Symptoms:** node not discovering peers; clients stop selecting it;
`NodeAnnounce` from this node not seen by neighbours.

**Detect:**

- `DecdnPeerTableThin` (warning) — `decdn_gossip_peer_table_size < 3` for
  10 minutes. Until #1513 the alert queried `decdn_peer_table_size`, a name
  nothing has ever exported, so it could not fire; if you run a forked copy of
  `monitoring/prometheus-alerts.yml`, check that its `expr` carries the
  `gossip_` prefix.
- `DecdnNoActiveStreams` (warning) — `decdn_streams_active == 0` across all
  directions for 15 minutes (gossip degradation is one of several causes).
- `decdn_iroh_*` transport-level metrics for connection failures (registered
  under the `decdn_iroh_` prefix per `crates/node/src/metrics.rs`).

**Causes:** NAT traversal failure, firewall blocking the QUIC port, peer
identity rotation, host clock skew breaking TLS.

**Remediate:**

1. Verify the configured `network.bind_port` is reachable from the public
   internet. The node binds `0.0.0.0:<bind_port>` and `[::]:<bind_port>` in
   `crates/node/src/runtime/mod.rs::build_endpoint`, so there is no
   operator-tunable bind interface; fixes here are at the firewall, NAT,
   or port-forwarding layer. Open UDP `bind_port` for both IPv4 and IPv6.
   If the log shows `IPv6 bind failed`, the node is IPv4-only: enable IPv6
   on the host or register `/ip4/` multiaddrs only.
2. If a peer's iroh key was rotated, neighbours referring to its prior
   `NodeId` will not reconnect until they re-discover the new identity via
   gossip. The node config carries no static bootstrap-peer list
   (`crates/common/src/config/types.rs` exposes `network.bind_port`,
   `network.relay_urls`, and `network.discovery`; the optional
   `network.discovery.peers` address book is keyed by `NodeId`, so a rotated
   key orphans its entry rather than bridging the rotation); follow
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
