# Operator Runbook

First-response steps for the failure modes operators hit most. Each section
names the symptom an operator sees first, then the metric or alert that
surfaces it, then the action to take. For the underlying protocol semantics,
follow the ADR cross-references.

Companion assets (the four dashboards and the alert rules live in
[`decdn/devops`](https://github.com/decdn/devops/tree/main/charts/decdn-node/files/monitoring)):

- [`grafana-dashboard.json`](https://github.com/decdn/devops/blob/main/charts/decdn-node/files/monitoring/grafana-dashboard.json) —
  fleet overview. Start here.
- [`dashboard-delivery.json`](https://github.com/decdn/devops/blob/main/charts/decdn-node/files/monitoring/dashboard-delivery.json) —
  serve leg, pull leg, cache and origin. Every refusal and failure reason.
- [`dashboard-chain.json`](https://github.com/decdn/devops/blob/main/charts/decdn-node/files/monitoring/dashboard-chain.json) —
  chain refresh liveness, registries, and both sides of the payment flow.
- [`dashboard-node.json`](https://github.com/decdn/devops/blob/main/charts/decdn-node/files/monitoring/dashboard-node.json) —
  one node at a time: host, transport, DHT, logs and traces.
- [`prometheus-alerts.yml`](https://github.com/decdn/devops/blob/main/charts/decdn-node/files/monitoring/prometheus-alerts.yml) —
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
  eviction keeping up. That counter reads low: it misses blobs created and
  swept within one GC pass. A footprint pinned at the ceiling with reclaim
  flat means eviction is starved — confirm `cache.gc_interval_sec` is
  non-zero.
- Existing alert `DecdnServeInternalErrorRate` will fire downstream once
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
blacklist updates and stop being able to redeem lanes. Once
[#283](https://github.com/decdn/decdn/issues/283) lands, the
`decdn_rpc_healthy` gauge drops to 0.

A provider that passes the preflight but fails intermittently delays boot
rather than ending it: each boot-time chain read logs `boot chain read failed;
retrying after backoff` at `WARN` and retries. The daemon exits when the next
retry could not start before a shared 10-minute boot deadline (`gave up after
N attempts`), or at once on a deterministic fault (`deterministic failure, not
retried`: a wrong contract address, an ABI mismatch, a revert, a disk error
that decides a blacklist enforcement attempt). A best-effort
`PaymentPool.operatorBps()` read that gives up logs the same text but falls
back to the floor share instead of exiting. The node
serves nothing until the blacklist enumeration and enforcement complete. The
metrics listener serves during boot: `decdn_node_boot_ready` reads `0` until
bring-up finishes, and `decdn_chain_boot_read_retries_total` rises while the
boot chain reads retry. The RPC preflight's own retries show only in its
`WARN` lines.

**Detect:**

- The node reads chain state with `eth_call` refresh loops only; it reads no
  event logs. A dead governance-chain endpoint stops every refresh on that
  chain at once: `decdn_blacklist_watcher_last_tick_timestamp_seconds` and
  `decdn_capacity_bond_registry_last_resync_timestamp_seconds` stop advancing,
  and `decdn_blacklist_ledger_poll_failures_total`,
  `decdn_capacity_bond_registry_resync_failures_total` and
  `decdn_slash_resync_failures_total` rise together. Several rising together
  points at the RPC endpoint rather than at any one loop. A stale blacklist
  timestamp is the one carrying slash risk; see
  [ContentBlacklist compliance](#contentblacklist-compliance). Each failed
  refresh keeps the projection the node already has and retries on its next
  interval.
- `DecdnRpcErrorRatioHigh` fires when more than 10% of the node's JSON-RPC
  requests fail for 15 minutes, which catches a provider that is reachable but
  unreliable. `decdn_rpc_requests_total{method,outcome}` counts every request
  the node's providers send, and the "Chain RPC provider" row in
  `dashboard-chain.json` plots the rate, the error ratio and the p95
  latency by method, plus the failures by outcome. `rate_limited` means the
  provider plan is too small for the node's poll cadence; `transport_error`
  and `timeout` mean the provider is unreachable or overloaded; `rpc_error`
  on `eth_call` can mean the provider does not support the `safe` block tag,
  which the blacklist sync needs.
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

- A stale `decdn_capacity_bond_registry_last_resync_timestamp_seconds` (with
  `decdn_capacity_bond_registry_resync_failures_total` advancing) means the
  5-minute `getRegisteredNodes` refresh fails. One snapshot feeds both the
  active-staker set and the `NodeId → operator` bindings, so while it fails:
  - The cached active-staker set drifts from chain state and mis-admits
    DHT `Store`s.
  - New node registrations are missed, so those providers stay unpayable and
    are skipped — the pull path's reachable-provider set may be capped by
    stale bindings. (Only on nodes with
    `cache.node_to_node_pull_through_enabled` on; the projection is not built
    otherwise.)

  This loop is **not** covered by `decdn_rpc_healthy`. See
  [appendix-observability § Chain Refresh Metrics](../adr/appendix-observability.md#chain-refresh-metrics).

**Remediate:**

1. Rotate `blockchain.rpc_url` to a secondary provider in the config and
   restart.
2. For Arbitrum testnet, fall back to a public RPC
   (`https://sepolia-rollup.arbitrum.io/rpc`) — rate-limited; for production,
   use a paid provider.
3. After recovery, confirm RPC reachability is restored before considering the
   incident closed. (Blacklist sync liveness is tracked by
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
   `DECDN_KEYSTORE_PASSWORD_FILE`) on `decdn-node run`, `decdn get`,
   `decdn pool`, the operator commands, and
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

**Trigger** — the one offense `SlashJudge` adjudicates, through
`submitBlacklistChallenge` (per
[ADR 011](../adr/011-content-takedown.md),
[ADR 014](../adr/014-on-chain-verification.md)):

- **Blacklist violation** — serving content after the blacklist entry's
  `effectiveAt` (`addedAt` plus the compliance window), not after `addedAt`
  — see [ContentBlacklist compliance](#contentblacklist-compliance). The
  evidence is a signed `StreamResponse` with `ok: true`; the answer to a
  discovery open is one too.

There is no rate slash: a client sees the signed quote before it pays and
signs that price into each voucher. A rate change needs no wait.

There is no "missed response" offense: every slash is driven by the
operator's own signed messages, so a node that stays silent cannot be
slashed by an external adversary.

**Detect:**

- Alerts in `prometheus-alerts.yml` (verbatim names):
  - `DecdnBlacklistWatcherStalled` (critical) — the blacklist sync has
    not completed a ledger poll for several intervals, so the node may be
    serving content blacklisted since the last successful poll. It fires on
    the age of `decdn_blacklist_watcher_last_tick_timestamp_seconds`. See
    [ContentBlacklist compliance](#contentblacklist-compliance).
  - `DecdnBlacklistEnforcementFailing` (critical) — a re-scope could not
    re-verify or evict every known deny-set entry, so a blacklisted hash may
    still be servable.
- Grafana: the slash-safety row in `grafana-dashboard.json`.

**Remediate:**

1. **Fix the root cause before anything else:** blacklist sync lag or a
   stale RPC endpoint.
   A slash you appeal while the underlying problem persists will not stop
   the next strike.
2. **There is no counter-evidence window.** If on-chain verification passes,
   the slash executes synchronously inside the challenger's
   `submitBlacklistChallenge` reveal — see
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
failure, so every paid-delivery node runs it. On boot it reads the whole
in-scope deny-set from `ContentBlacklist` at the `safe` block —
`getScopeRegions(operator)` for the one-to-three regions in scope, then the
paged hash and address lists per region — together with the
`mutationCount` at that block. After boot it polls the mutation ledger every
`blockchain.content_blacklist_poll_interval_sec` (at most 5 minutes):
`mutationCount` at the `safe` block, then `getMutations` for the new records,
applied in ledger order. The cursor is in memory only, so a node returning
after any downtime rebuilds the complete deny-set from one snapshot. Every
tick also re-applies local expiry and region ripening, which change scope
without a new ledger record.

Enforcement is two writes per hash, in order — a governance-sourced *deny*, then
the *eviction*. `CacheEngine::evict` durably records the takedown in `evicted.log`
and cascades to every serving surface through `CacheEngine::refuses` — including
the DHT republisher dropping the hash on its next tick, and the client handler
refusing every stream for it, discovery opens included, and refusing to
re-pull-fill it. `evict` is sticky and works on absent hashes, so a hash blacklisted while the
node was offline is pre-blocked. The separate deny
records *why*, which `evicted.log` cannot express. Every one of these refusals
reaches the client as the one class `Declined`, so a client cannot tell which
list a hash is on (ADR 011 § `StreamRequest` Response); the deny keeps the
cause on the node's own per-reason counters
(`decdn_serve_stream_rejected_chain_hash_denied_total`,
`..._hash_denied_total`, `..._withdrawn_total`). A
fail-closed readiness gate keeps every ALPN listener shut until the first
enumeration + enforcement pass completes, so a node refuses to serve rather than
serve un-enforced.

Manual eviction (Remediate below) is therefore for a takedown notice you received
out of band, or for confirming the sync already acted — not the primary
mechanism.

Operator-*level* blacklisting runs on a different path, and takes effect in two
places. When governance calls `ContentBlacklist.addOperator`, the contract calls
`CapacityBond.ejectNode`, so the ejected node leaves `getRegisteredNodes`. The
node's 5-minute registry refresh drops it from the active set (see
`crates/node/src/dht/capacity_bond_registry.rs`). Separately, the blacklist sync
folds the blacklisted-address union into the node's origin deny-set, so a
`StreamRequest` on a pool funded by a blacklisted operator is refused at the
delivery gate with `OriginDenied` and any in-flight stream is cut at the next MB
boundary (`crates/node/src/content_deny.rs`). Both are automatic — no operator
action is required.

**Detect:**

- Alerts in `prometheus-alerts.yml` (verbatim names):
  - `DecdnBlacklistWatcherStalled` (critical) — no successful ledger poll for
    several intervals; fires on the age of
    `decdn_blacklist_watcher_last_tick_timestamp_seconds`.
  - `DecdnBlacklistEnforcementFailing` (critical) — a re-scope could not
    re-verify or evict every known entry, so a blacklisted hash may still be
    servable even while the poll timestamp is fresh. The two answer different
    questions: deny-set enforced vs chain readable.
- `decdn_blacklist_ledger_poll_failures_total` rising means ledger polls fail;
  the next poll retries from the same cursor.
- The tick-age gauge above is the liveness coverage
  (`adr/011-content-takedown.md` § Node Behavior).
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

The node pulls from peers only with `cache.node_to_node_pull_through_enabled`
on. Absent, it is on for a node with no origin backend and off for an origin
node; `decdn node status` prints the resolved value on its `pull_through=`
line. With it off, the node never opens a buyer pool
or spends USDC on pulls, and the symptoms above do not apply. The buyer service
still starts. With `blockchain.buyer_max_approve = true` (the default), it gives
the `PaymentPool` an unlimited USDC allowance at startup, sending the `approve`
only when the current allowance is lower; that standing authorization stays in
place with pull-through off. Set it to `false` to manage the allowance yourself.
The service also publishes `decdn_buyer_wallet_usdc` and runs the reclaim sweep
for any pool it already owns. A paid miss fills from the
node's own origin backend; a node without one returns not-found.

The node's buyer leg is the one part of it that *spends*. It opens one
`PaymentPool` deposit of `blockchain.buyer_working_deposit_micro_usdc` (10 USDC
by default) and reuses it for every upstream pull. Two things break it.

**Cause (1): the operator wallet holds no USDC.** Every `openPool` reverts on
the ERC-20 transfer, so the node can pay no provider and every cache miss falls
through to origin.

1. Read `decdn_buyer_wallet_usdc`, and
   `decdn_pool_open_failures_insufficient_deposit_total` beside it, or the
   `pull_through=` line of `decdn node status`, which flags an empty wallet
   `(unfunded …)`. With pull-through on, the node also logs a warning at each
   read of an empty wallet. The balance is read once per reclaim sweep, so
   allow up to an hour after funding.
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
`buyer.redb`, and a client-owned `buyer-pools.redb` that `decdn get` and
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
3. Recover those with `decdn pool close --pool-id <poolId>`, then
   `decdn pool reclaim --pool-id <poolId>` once the pool's `disputeWindow` has
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

**`decdn_buyer_pool_open_unresolved` at 1 means the node holds its pool open for
an `openPool` it could not confirm.** The submit returned no tx hash (the send
request failed in transport after the RPC node may have accepted it, or the RPC
node already held the transaction, counted its nonce as spent, or held another
transaction of this key at that nonce), or the receipt read failed, so the
transaction may still mine. The node opens no second
pool until the chain resolves it, and every cache miss that needs a pool is
refused as a local fault meanwhile. The node re-reads the chain every 5 s: a
receipt, or the account's nonce passing the open's, ends the wait. A nonce that
no transaction holds gets a fresh `openPool` at that same nonce, so at most one
of them escrows. `still holding the open slot for an unconfirmed openPool`
repeats every minute with the elapsed time and the last thing the chain said. A
gauge that stays at 1 means the RPC cannot answer, a pending transaction from
this key holds the open's nonce or an earlier one, or every re-send at that
nonce fails because the wallet cannot fund the deposit or the gas, or the
contract refuses the open. Read `last` in the repeating warning, then fix the
RPC, let that transaction mine, or fund the wallet. Do not restart to clear it:
a restart forgets the wait, and a transaction that mines after the restart is a
deposit the node does not track until the next restart adopts or reports it.

**`decdn_buyer_topup_unresolved` at 1 means the node holds its top-up slot for a
`topUp` it could not confirm.** The submit returned no tx hash (a transport
failure, or the RPC node already held the transaction, counted its nonce as
spent, or held another transaction of this key at that nonce), or the receipt
read failed, so the transaction may still mine. The node sends no other `topUp`
until the chain resolves it. A low-water refill does nothing. A funding recovery
step that arrives meanwhile fails at once as a local fault and counts on
`decdn_node_pull_recovery_step_refused_total`, so a cache miss whose upstreams
all refuse it `Unfunded` fails fast instead of waiting out its deadline. A step
that joined the `topUp` before the settle began, during its receipt wait, still
waits for the outcome. The node re-reads the chain every 5 s. A receipt, or the
account's nonce passing the `topUp`'s, ends the wait and credits what escrowed.
A re-send that shows the pool closed also ends it, and the recovery step then
opens a new pool. A nonce that no transaction holds gets a fresh `topUp` at that
same nonce, so at most one of them escrows. `still holding the top-up slot for an
unconfirmed topUp` repeats every minute with the elapsed time and the last thing
the chain said. A gauge that stays at 1 means the RPC cannot answer, a pending
transaction from this key holds the `topUp`'s nonce or an earlier one, or every
re-send at that nonce fails because the wallet cannot fund the `topUp` amount or
the gas. Read `last` in the repeating warning, then fix the RPC, let that
transaction mine, or fund the wallet with USDC and gas. Do not restart to clear
it: a restart forgets the settle, and a `topUp` that mines after it raises the
pool's on-chain deposit without crediting the local row. `decdn pool list --all`
then shows a larger `deposit` for the tracked pool than `decdn node pools`
reports. A later settle that resolves through a spent nonce credits that surplus
with its own `topUp`.

## Refusing paying clients (insufficient deposit)

**Symptoms:** clients report a blob as missing that this node holds; delivery
revenue flat while requests arrive; `decdn get` against this node returns
`delivery refused: NotFound`.

**Detect:**

- `DecdnRefusingPayingClients` (warning) —
  `rate(decdn_serve_stream_rejected_insufficient_deposit_total[10m]) > 0.1` for
  15 minutes. A raw rate, not a ratio: the exporter has no serve-attempt counter
  to divide by.
- The node's own throttled `warn!` ("refusing paying clients: pool's refundable
  remaining deposit below the reserved cost"), at most one per 5 minutes, carrying
  the pool id (`pool_id`), the remaining headroom (`headroom`), the amount reserved
  (`ceiling`), and how many refusals it suppressed since the last line
  (`suppressed`). Raise the log level to `debug` for one line per
  refusal — that one also carries the hash.
- Grafana panel **Payment-class serve refusals (all collapse to NotFound)**. Its
  series are a subset of the ten reject reasons that share that one wire code, so
  a client cannot distinguish them and the server-side counters are the only place
  the cause exists at all.

**Causes:** two, with different remedies, and the `insufficient_deposit` counter
alone cannot tell them apart — this is why the log line exists:

1. **Clients genuinely running dry.** Their remaining deposit, less the
   refundable floor `M`, cannot cover one chunk. Nothing is wrong with this
   node. Expect a low background rate.
2. **This node's pool read is older than an on-chain top-up.** The seller's view
   of a pool's deposit comes from a `getPool` read that lives for 30 seconds,
   so a client that just topped up can be refused until the next read. A
   view that stays stale means the reads fail. Funded clients are being turned
   away and will route elsewhere.

A third, rarer cause: the operator's own
`blockchain.buyer_working_deposit_micro_usdc` is too small for the *upstream*
rate, in which case this node is the one being refused. (The node opens its
buyer pool at the working deposit and the low-water refill tops it back up on
reuse. When no upstream serves a pull at the current deposit, the
fill runs one funding recovery step, which `decdn_node_pull_recovery_step_total`
counts; see
[ADR 003 § Funding recovery](../adr/003-payments.md#funding-recovery).) Look for
`decdn_node_pull_refused_unattributable_total` climbing toward
`decdn_node_pull_refused_total` instead.

**Remediate:**

1. Distinguish the two causes. Take `pool_id` and `headroom` from the `warn!`
   line: `headroom` is the pool's remaining deposit as the node sees it, less the
   refundable floor `M` (`blockchain.pool_min_remaining_deposit_micro_usdc`). Read
   `PaymentPool.getPool(poolId)` on-chain (for example with `cast call`) and
   compute `deposit − totalRedeemed − M`. If that exceeds
   `headroom` — typically after a recent `PoolToppedUp` — the node's view is
   stale: cause (2).
2. For cause (2), check the payment-chain RPC: `decdn_rpc_requests_total` for
   `method="eth_call"` with an outcome other than `ok`. Failing reads usually
   mean the RPC endpoint is unreachable or rate-limiting; see
   [RPC unreachable](#rpc-unreachable).
3. For cause (1), no action. If the rate is high because many clients open dust
   pools deliberately, note that the refusal happens *before* any fill
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

**Symptoms:** none directly visible. The node keeps serving, discovering and
settling normally while the cached active-staker set drifts from chain state,
gating DHT `Store` admission on stale membership.

**Detect:** `DecdnCapacityBondRegistryResyncFailing` on
`rate(decdn_capacity_bond_registry_resync_failures_total[15m]) > 0`, or
`DecdnCapacityBondRegistryResyncStale` on
`decdn_capacity_bond_registry_last_resync_timestamp_seconds` going stale, with
`capacity-bond registry resync failed; keeping current projections` at `WARN`.

**What it means:** the node reads the active-staker set as one
`getRegisteredNodes` snapshot every 5 minutes, and again (at most once a
minute) when a peer presents a `NodeId` the projection does not know. A failed
read keeps the projection the node already has and bumps the counter. A stale
gauge with a flat counter means the refresh loop itself stopped.

**Remediate:** check the RPC provider's `eth_call` path. The refresh runs on
its own when the provider recovers. A restart reads the snapshot at bootstrap
and is the fallback if the provider is healthy and the gauge stays stale.

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

The reference `DecdnNodePullCorruption` alert fires on any increase of
`decdn_node_pull_corruption_total` in an hour. The reference delivery
dashboard plots all three.

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

The reference alert rules carried a `DecdnHashMismatchAppearing` alert until #1513. It
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
3. Inspect `decdn_iroh_socket_*` metrics for connect failures; high
   failure rates with low success rates indicate NAT/firewall problems
   rather than gossip-layer issues.
4. Verify host time is synchronised (`chronyc tracking` /
   `timedatectl status`). Skew greater than the QUIC handshake tolerance
   breaks every connection silently.

## Repointing at a redeployed PaymentPool

Every row of the node's payment stores is keyed by its `PaymentPool`
deployment: the payment chain id and the pool address (ADR 003 § Off-chain
voucher state persistence). A pool id repeats across deployments, so the key
keeps them apart. Changing a `[[blockchain.payment_chains]]` entry's `chain_id`
or `payment_pools` therefore drops nothing: the node serves the pools now
configured, and the rows of a pool no longer configured stay in `lanes.redb`,
`settle.redb` and `buyer.redb` untouched. Put the pool back
in the list to redeem its unredeemed vouchers before its owner reclaims it.

## A settlement ledger row that does not decode

`decdn node earnings` and `decdn node status` answer
`settlement ledger load failed: corrupt record … settlement ledger row at block
N log M does not decode` when a row in `settle.redb`'s `settlement_ledger_v2`
table is not in this binary's layout. The redeemer keeps landing redemptions
and keeps its paid watermarks; only the earnings figures are affected. The
ledger shares `settle.redb` with both pending-settle sets and the sent
`redeemMany` hashes the node reads receipts for on boot, so do not move the
file aside while a pool this node holds unredeemed vouchers on is closing.
Wait until `decdn_unredeemed_usdc` reads 0. Then stop the node, move
`settle.redb` aside and restart: the earnings history starts over, and
`covered_from` reads that boot. `decdn node earnings --address` still has the
chain's record.

**Before repointing:**

1. While the node still points at the old deployment, set
   `blockchain.redeem_threshold_micro_usdc = 1` and restart it. The redeemer
   submits a chunk only when the chunk's aggregate unredeemed total clears
   this floor, and every redeem path uses it, graceful shutdown included. At
   the default (0.05 USDC), a batch of small lanes can sit under the floor and
   `decdn_unredeemed_usdc` may never reach 0. With the floor at 1 µUSDC, the
   self-tick (`blockchain.redeem_interval_secs`) redeems every lane whose
   pool can still pay — accepting that dust redeems at a gas loss.
2. Wait at least two `redeem_interval_secs` after the restart (the gauge
   shows its initial 0 until the first sweep runs), then wait for
   `decdn_unredeemed_usdc` to read 0 — near-zero is enough on a node that is
   still serving; the shutdown sweep is the final drain. Also check that
   `decdn_redemption_skipped_insolvent_total` stays flat across a sweep:
   lanes on a drained-but-`Open` pool are held, never enter the gauge, and
   are only redeemable after the payer tops the pool up. The repoint
   forfeits them — they surface only in step 3's `forfeited_micro_usdc` —
   so decide about them before proceeding. Then stop the node gracefully
   and confirm the log has no `shutdown redeem deadline elapsed` warning;
   if it has one, start the node on the old config and stop it again.
3. Update the config (new deployment, and restore
   `redeem_threshold_micro_usdc`) and restart. The boot logs one WARN per dropped lane
   with its `pool_id`, `signer`, `provider`, and unredeemed value — the last
   record of each claim — plus a summary WARN with `dropped_lanes`,
   `undecodable_lanes`, and `forfeited_micro_usdc`. Capture these lines if
   you need an audit trail, and tally from the final boot's summary line:
   the per-lane lines repeat if a boot crashes mid-drop.

Startup also verifies the configured deployment before touching the store:
`eth_chainId` must match `blockchain.chain_id`, and the configured address
must hold code that answers `PaymentPool.getRateBounds()`, a view only
`PaymentPool` declares, so a typo — including a sibling contract pasted from
the same deploy manifest — aborts boot instead of triggering the drop. Buyer pool rows are not dropped by this path. Each
buyer row carries its own chain id and `PaymentPool` address, and the buyer
ignores a row from another deployment.

## Sweeping the protocol share

Each `redeemMany` pays the operator share on the payment chain at once. The
pool keeps the protocol share as `pending(operator)` until a sweep sends it
over CCTP to the governance chain's `CctpSweepReceiver`, which deposits it into
`FeeRouter`. The sweep carries the operator's rebate and its vote weight. The
daemon does not sweep: run it by hand, about once per epoch.

One sweep is a batch of 1 to 32 operators (`MAX_SWEEP_BATCH()`): one CCTP
transfer on the payment chain and one receive on the governance chain, whose
cost the batch shares. The batch total, not each share, must reach the pool's
`minSweep`, so operators with small shares reach it together.

1. Check what each pool holds: `decdn node earnings` prints one `pool …` line
   per `PaymentPool` with the pending protocol share and `minSweep`.
2. Send it: `decdn node sweep` reads the operator's `pending` and `minSweep()`
   on every configured pool and calls `sweep([operator])` where the share has
   reached `minSweep`. To share the receive with other operators, list them
   with `--with <address>` (repeat it for each). The command reads each listed
   operator's `pending`, leaves out an operator with nothing pending (the
   pool skips one too), and sends the batch when its total reaches
   `minSweep`:

   ```bash
   decdn node sweep --with 0xAbc… --with 0xDef…
   ```

   For each pool it prints the batch total, and after the sweep the
   transaction, the `SweepSent` total and bridge fee, and one line per swept
   operator with its share and served bytes (`--json` for the same as JSON).
   `--pool eip155:<chainId>:<address>` sweeps one pool; `--dry-run` sends
   nothing. The operator keystore pays the gas on each payment chain.
3. Complete it on the governance chain: `decdn node receive-sweeps` scans each
   pool for the operator's `Swept` events (from the known deployment block, or
   `--from-block`). A batch another operator sent is found too, since
   `Swept` indexes each operator. For each sweep transaction the command reads
   the message and attestation from Circle's attestation service once
   (`blockchain.cctp_iris_url` or `--iris`; the sandbox
   `https://iris-api-sandbox.circle.com` on testnets) and calls
   `CctpSweepReceiver.receiveSweep` once for the whole batch. Each line names
   the batch size and the operator's share. A standard transfer waits for
   source-chain finality, so the attestation can stay pending for many
   minutes: the command waits about half an hour, then reports `pending`, and
   a re-run completes it. A sweep already received, by this operator or
   another one of the batch, reports `already_received`. To complete one
   known sweep, pass `--pool <caip10> --tx <sweep tx hash>`.

The command needs `blockchain.cctp_sweep_receiver_address` (or
`--cctp-sweep-receiver-address`). `decdn node doctor` checks that each pool's
`sweepRecipient()` equals it: a mismatch means the pool belongs to another
deployment, and its sweeps land where this governance chain never sees them.

### CCTP outage

The protocol share travels from each payment chain to the governance chain
over CCTP standard transfers. An outage of CCTP delays the protocol share
only. It does not stop serving, payment or governance.

**What keeps working:**

- Clients pay and nodes serve. Each redemption pays the operator its
  `operatorBps` share on the payment chain, with no bridge.
- Nothing is lost. A sweep that already burned waits for its attestation,
  and standard CCTP messages never expire. A share not yet swept stays in the
  pool's `pending`.
- Governance keeps its quorum. Vote weight is the served bytes of the last
  `windowEpochs` epochs (13 weekly epochs by default), and quorum is a
  fraction of the same window total. An outage lowers both together.

**Detect:**

1. `decdn node receive-sweeps` reports attestations as `pending` for hours.
2. Circle's status page confirms the outage.
3. If Circle has paused the burn side, `decdn node sweep` reverts and the
   share stays in `pending`. Nothing needs undoing.

**Keep governance working:**

Each week of outage removes about 1/`windowEpochs` of the new weight. Before
a vote that must pass, or once the outage reaches 4 to 6 weeks, operators
that need weight deposit USDC directly into `FeeRouter` on the governance
chain. `$USDC` is the governance chain's USDC, `$FEE_ROUTER` the `FeeRouter`,
`$OPERATOR` the operator address the weight is for, `$AMOUNT` the deposit in
micro-USDC and `$BYTES` the served bytes it credits:

```bash
cast send $USDC "approve(address,uint256)" $FEE_ROUTER $AMOUNT --rpc-url $GOVERNANCE_RPC --account <operator>
cast send $FEE_ROUTER "deposit(address,uint256,uint256)" $OPERATOR $BYTES $AMOUNT --rpc-url $GOVERNANCE_RPC --account <operator>
```

- Size `$BYTES` to about one normal epoch of the operator's traffic.
- Only an operator `CapacityBond` reports as active earns the credit and the
  rebate.
- The credit is at most `net × 1 MiB / deliveryFloor`, and the capacity clamp
  applies. `net` is the part of the deposit that is not rebated; the rebate
  returns to the operator in the same transaction.
- At least two active operators must deposit. The share cap sizes each
  operator's weight from the other operators' bytes, so one depositor alone
  gets no weight.
- The credit counts after its arrival epoch fully elapses.

**Recover:**

1. When attestations complete again, list the backlog with
   `decdn node receive-sweeps --dry-run`.
2. Receive the backlog about one epoch's worth per epoch, with
   `decdn node receive-sweeps --pool <caip10> --tx <sweep tx hash>` for the
   sweeps of that epoch. Bytes count in the epoch they arrive, and the
   capacity clamp limits each operator's credit per epoch. A backlog of
   several weeks received in one epoch loses most of its weight credit; the
   USDC still arrives. Messages never expire, so a later receive costs
   nothing.
3. Resume normal batched sweeps (`decdn node sweep`, with `--with` for the
   operators that share the receive).

**If CCTP leaves a payment chain for good:**

1. Deploy a `PaymentPoolBase` variant on another bridge that authenticates
   the source pool, and its governance-chain adapter (ADR 016 § Bridge
   adapters). `FeeRouter` does not change.
2. Nodes replace the old pool with the new one in
   `[[blockchain.payment_chains]].payment_pools`. Clients open pools on the
   new contract.
3. A share still unswept in the old pool stays there. Frequent batched sweeps
   keep that amount small.

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
faucet: <https://faucet.circle.com>.
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
- Monitoring assets: the reference dashboards and alert rules in
  [`decdn/devops`](https://github.com/decdn/devops/tree/main/charts/decdn-node/files/monitoring).
