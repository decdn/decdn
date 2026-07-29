# Changelog

All notable changes to deCDN will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
SemVer applies once the first tag (`v0.1.0`) is cut.

## Conventions

- Entries group by **subsystem** inside each release (runtime, cache, gossip,
  config, CLI, …) so operators can scan for what affects them.
- **Wire-breaking** changes (ALPN bump, message-layout change per ADR 013)
  are called out explicitly — operators cannot hot-upgrade across them.
- **Config-breaking** changes name the field (rename, new required field,
  default shift).
- Security advisories cite the `RUSTSEC-YYYY-NNNN` id.
- Commit prefixes follow [Conventional Commits](https://www.conventionalcommits.org/).

## [Unreleased]

Pre-release development — no versioned tag yet. Entries below track state
since project inception and will roll into the first tagged release.

### Changed (BREAKING)

- **Phantom-announcement offense retired; probe holds are now best-effort.**
  `SlashJudge` adjudicates two offenses instead of three. The
  announce-then-fail-to-deliver ("phantom announcement") offense is gone: it
  punished an availability miss, and enforcing it required the node to withhold
  truthful `has_blob` answers under load, which a probe flood could weaponize
  into a network-wide availability blackout.
  - **ABI:** `submitPhantomChallenge(address,bytes32,bytes,bytes,bytes,bytes,bytes32)`
    is removed, along with the `NotPhantom()` error. `ISlashJudge.OffenseType`
    drops its leading `Phantom` variant and **renumbers**: `RateManipulation`
    `1 → 0`, `Blacklist` `2 → 1`. The ordinal is durable — it is persisted in
    `SlashEscrowLib.SlashRecord.offenseType`, emitted in both (non-indexed)
    `Slashed` events, and folded into the `evidenceHash` preimage that keys
    `usedEvidenceHash` and `commitments`. `SlashAppeal` is offense-agnostic and
    needs no migration. `InterfaceFreeze.t.sol` now pins the ordinals, since
    selectors are invariant under a reorder.
    - **Migration:** any consumer decoding `offenseType` off-chain — including
      the `decdn node slashes` admin RPC field `offense_type` — must be
      updated. A pre-existing on-chain record or log with `offenseType == 1`
      meant `RateManipulation` and now decodes as `Blacklist`; `2` no longer
      names an offense. Re-deploy rather than upgrade in place.
  - **Rate manipulation now requires `stream.ok == true`.** A signed refusal
    cannot overcharge on a delivery it declined, so a refusal is inert as
    evidence and a node may sign `ok: false` freely — under either remaining
    offense (blacklist violation requires a served claim).
  - **Probe behaviour (observable on the wire).** Presence, not a guaranteed
    hold, governs `has_blob`. Under hold-budget exhaustion or a stake-lane
    reservation the node now answers `has_blob: true` and forgoes only the
    eviction hold, where it previously answered `has_blob: false`. Such a blob
    stays LRU-evictable, so a pull that loses the race costs one wasted round
    trip. `max_probe_holds = 0` still answers `has_blob: false`, but only for
    store-backed content — origin-servable content takes no hold and is
    advertised regardless. Peer selection will now pick nodes that previously
    excluded themselves.
  - **Monitoring-breaking:** `decdn_slash_evidence_exposure_total` and its
    `DecdnSlashEvidenceExposure` alert are deleted (the series was documented
    but never emitted, so no dashboard was ever populated by it). The
    `decdn_probe_hold_unavailable_total{reason}` counter keeps its name while
    two of its three reasons change meaning — `exhausted` and
    `stake_lane_reserved` now record an advertised-without-hold probe rather
    than a suppressed answer. Alert text and the runbook are updated
    accordingly; review any custom rules built on them.
- **`PaymentChannel.minDeposit` removed (#1515).** **ABI-breaking, not
  config-breaking.** The network minimum-deposit parameter is gone: the
  `minDeposit()` view and the `setMinDeposit(uint256)` governance setter no
  longer exist (an integrator calling either now reverts on a missing selector),
  the `MinDepositUpdated` event is gone (a subscriber filtering its topic sees
  zero events, silently), and the `DepositBelowMinimum` error selector is
  retired. `openChannel` now accepts **any non-zero deposit** and reverts
  `ZeroAmount` on zero — both before the transfer and on the received balance
  delta, so a fee-on-transfer token cannot shave a deposit to nothing. Because
  the parameter no longer exists, any pending or scripted `setMinDeposit`
  governance proposal is un-executable. `PaymentChannel` has a constructor and no
  proxy, so this requires a **fresh deployment**; existing testnet instances must
  be redeployed. The floor bounded nothing that is not already bounded — service
  by the seller-side per-voucher ceiling (see the #1516 entry under Fixed),
  channel spam by gas — and the client-side 10 USDC recommendation of
  [ADR 003 § Deposit Economics](adr/003-payments.md#deposit-economics) is
  unchanged. `blockchain.buyer_deposit_micro_usdc` keeps its name, its 10 USDC
  default, and its `> 0` validation; buyer paths simply no longer read the
  contract to clamp up to a floor. Unrelated to the deposit: the internal
  `MIN_DEPOSIT_FLOOR` constant, which only ever bounded the delivery-*rate*
  floor, is renamed `MIN_RATE_FLOOR` — same value, same two call sites, no
  behaviour change.
- **Log-replay start-block config knobs removed.** **Config-breaking:** the
  three `[blockchain]` scan-floor fields — `origin_directory_from_block`,
  `slash_judge_from_block`, and `content_blacklist_from_block` — are removed.
  Every chain watcher now enumerates its state on chain and seeds the live tail
  rather than replaying `eth_getLogs` history from a configured floor, so none
  of the three is read anymore. Because `[blockchain]` uses
  `deny_unknown_fields`, a config file that still sets any of them now fails
  `decdn config validate` and node startup — delete the keys. There is no
  replacement setting.
- **Payment-channel funder and voucher signer are now separate roles.**
  `openChannel` takes a third argument pinning the channel's `voucherSigner` —
  the address every voucher signature is verified against, on all four
  settlement paths (`closeChannel`, `disputeChannel`, `withdraw`,
  `cooperativeClose`). It is fixed at open and has no setter. Passing the zero
  address resolves it to `msg.sender`, so a funder that signs its own vouchers
  keeps today's behaviour. `ch.client` keeps the funder role — it deposits and
  tops up, receives the refund, owns the `channelId` nonce, and is the address
  the ADR 011 compliance gates check — but its signature no longer authorizes
  anything on its own.
  - **ABI:** `openChannel(address,uint256)` →
    `openChannel(address,uint256,address)`, so the **selector changes** and an
    integrator built against the old ABI reverts on every open. `ChannelOpened`
    gains a sixth parameter (`voucherSigner`), so **topic0 changes** — a log
    subscriber still filtering the old event hash sees zero events and silently
    registers no channels, which fails quietly rather than loudly. `getChannel`
    returns a `Channel` tuple with `voucherSigner` inserted at index 3, so an
    old-ABI consumer mis-decodes `provider`, `expiresAt`, `token`,
    `disputeDeadline` and every field after them. All three must be upgraded in
    lockstep with the deployment.
  - **On-disk (runtime):** the node's seller channel-state record in
    `<data_dir>/channels.redb` (`channel_state_v1`) goes to `schema_version` 3,
    which appends the channel's pinned `voucher_signer` as a trailing segment.
    The version — not the trailer's byte width — is what tells the decoder the
    segment is there, so unknown trailing bytes on an older record can never be
    misread as a signer address and silently move the voucher
    signature-recovery target. Upgrade is transparent: a v1/v2 record hydrates
    `voucher_signer` from the stored `client`, correct by construction because
    those channels predate the funder/signer split and are self-signing.
    **Rollback is not supported** — an older binary reading a v3 record raises
    `UnsupportedSchema`, and the seller table is fail-closed, so node startup
    aborts. Downgrading means restoring `channels.redb` from a pre-upgrade
    backup.
- **Blacklist-entry appeals removed (#1432).** `ContentBlacklist` no longer
  carries a second appeal state machine on top of enforcement. The six appeal
  entry points (`openBlacklistAppeal`, `fastTrackBlacklistAppeal`,
  `rejectBlacklistAppeal`, `rejectAppealAsPerjury`,
  `ratifyBlacklistAppealRemoval`, `reverseBlacklistAppeal`,
  `cleanupExpiredBlacklistAppeal`), the `StandingPath` enum, the per-filer
  rejection cooldown and perjury denylist, the interim-relief caps, and the
  `setAppealBond` / `setRejectionCooldownWindow` governance knobs are gone.
  Enforcement is untouched: adding hashes/origins/operators (global, regional,
  emergency), the compliance window, emergency auto-expiry, regional-body
  registration and suspension, and slashing for serving blacklisted content all
  behave exactly as before. A wrongful entry comes off via `removeHashRegional`
  (the issuing body) or a DecdnGovernor `removeHashGlobal` proposal; restitution
  for a slash already taken remains `SlashAppeal` (ADR 028), now the protocol's
  only appeal surface.
  - **ABI:** the `ContentBlacklist` constructor drops `publisherRegistry_` and
    `appealBond_`. `getHashEntry` and `IContentBlacklistHashView` lose the
    `suspended` tuple slot — a node built against the old ABI mis-decodes the
    entry and must be upgraded in lockstep with the deployment.
    `HashSuspensionUpdated` and the seven `BlacklistAppeal*` events are removed,
    as is the `IPublisherRegistryStanding` interface. `PublisherRegistry` itself
    is unchanged; `OriginAssignment` reaches it through
    `IPublisherRegistryOwnership`.
  - **Deploy:** `BLACKLIST_APPEAL_BOND` is no longer read, and the
    `ContentBlacklist` constructor no longer takes a token: with the appeal-bond
    escrow gone it custodies no funds at all.
  - **Size:** `ContentBlacklist` deployed bytecode drops 19,723 → 11,143 bytes.
  - ADR 031 is archived to `adr/_history/`; ADR 011 § Blacklist Entry Appeals is
    replaced by § Removing a Wrongful Entry. ADR 030's `REGION_STABILITY_WINDOW`
    is retained — only its appeals-standing leg is cut, since the window also
    forecloses a reactive blacklist-scope flip.
- **Settlement-weighted bootstrap ranking removed (#1434).**
  `FeeRouter.routeSettlement` no longer calls
  `CapacityBond.recordSettlement(operator)` on every settlement and mid-channel
  withdraw. Nothing consumed the resulting `SettlementRecorded` log: the
  client's bootstrap ranker reads `getActiveNodes`, orders region-first, and
  ranks by probe result, which is a strictly fresher signal than a historical
  settlement record. An external indexer that wants settlement recency should
  read `FeeRouter.Settled`, already emitted on the same path with the same
  operator address.
  - **ABI:** `CapacityBond.recordSettlement(address)`,
    `SettlementRecorded(address)` and `SETTLEMENT_REPORTER_ROLE()` are removed;
    `ICapacityBondReporter` is renamed `ICapacityBondEpoch` and narrowed to
    `epochLength()`. No Rust binding referenced any of them, so nodes need no
    change.
  - **Deploy:** one fewer post-deploy `grantRole` and one fewer cross-contract
    trust edge. `FeeRouter`'s `capacityBond_` constructor arg **stays** — it
    backs the `bondEpoch == epochLength_` assertion that stops a mismatched
    deployment mis-anchoring `DecdnGovernor` epoch arithmetic.
  - **Size:** `CapacityBond` gains 231 bytes of EIP-170 margin (1,165 → 1,396 free).
- **Advisory `deliveryCeiling` rate bound removed (#1441).** The enforced
  `deliveryFloor` is unchanged and still gates settlement in
  `_advanceClaimWatermark`. The ceiling enforced nothing — it appeared in no
  `require`/`revert` on the settlement path — and asked a seller to self-clamp
  its own advertised rate downward, which buys no on-chain safety. The absolute
  upper bound remains the wire constant `MAX_RATE_PER_MB`, enforced in
  `ProbeResponse` validation; it simply stops being governance-tunable.
  - **Config-breaking:** `payment.delivery_ceiling` is removed, along with
    `--delivery-ceiling` and `DECDN_DELIVERY_CEILING`. The TOML key and the CLI
    flag both fail loudly (`deny_unknown_fields` / clap); a stale
    `DECDN_DELIVERY_CEILING` in the environment cannot, so the node now logs a
    startup warning naming it rather than ignoring it in silence. `[payment]` uses
    `deny_unknown_fields`, so a TOML that still sets the key now fails startup
    and `decdn config validate` rather than ignoring it. Delete the key; nothing
    replaces it.
  - **Governance bound tightened:** the floor is now capped at
    `MAX_RATE_PER_MB` (10^12, the ADR 005 wire cap) rather than
    `type(uint64).max`. Every value in the ~18-million-fold gap between them was
    silently network-isolating — nodes raise every quote to the floor before
    signing, so a floor above the wire cap makes every `ProbeResponse` and
    `StreamResponse` undecodable to every honest peer and reverts essentially
    every voucher at settlement, while the node's only local signal is a clamp
    warning indistinguishable from a routine retune. The node now also refuses
    to start against such a floor rather than serving into the void.
  - **ABI:** `setRateBounds(uint256,uint256)` → `setRateBounds(uint256)`
    (selector changes), `RateBoundsUpdated` drops `newDeliveryCeiling` (topic0
    changes), `RateBoundsInvalid` drops its second parameter, `getRateBounds()`
    returns a single `uint256`, and the `PaymentChannel` constructor drops
    `deliveryCeiling_`. A node built against the old ABI mis-decodes the event
    and must be upgraded in lockstep with the deployment.
  - **API:** `decdn_node::rate_bounds::Bounds` is gone and `RateBounds` collapses
    to a single atomic floor — `RateBounds::new` and `store` take one argument,
    and `snapshot()` / `ceiling()` are removed. The `ArcSwap`-for-pair-consistency
    machinery went with it: with one value there is no half-applied-retune state
    to defend against.

- **QUIC 0-RTT probe establishment removed (#1429).** `cdn/probe/v1`
  connections always complete a full TLS 1.3 handshake before the request is
  sent; no ALPN transmits application bytes as replayable early data. The
  optimization saved one round trip, and only on a warm reconnection to an
  already-probed peer, at the cost of a replay-safety surface no server-side
  gate could enforce. TLS session resumption is retained — a client
  reconnecting to a known node still skips certificate transmission and
  signature verification, though the ECDHE key exchange still runs — and so is
  stream multiplexing on open connections (ADR 005).
  - **Resumption cache shrinks:** dropping the `max_tls_tickets` call with
    `SESSION_TICKET_CACHE_SIZE` leaves the client-side `rustls` session cache
    at iroh's default of 256 entries, down from 1000. That cache backs the
    retained 1-RTT resumption, not just early data, so a node probing more
    than 256 distinct peers between reconnections now re-handshakes in full
    where it previously resumed. Re-tune with `Endpoint::max_tls_tickets` if
    peer fan-out warrants it.
  - **Config-breaking:** the `network.enable_0rtt` field is removed. Since
    `[network]` uses `deny_unknown_fields`, a config that still sets it now
    fails `decdn config validate` and node startup — delete the line. The
    resulting behaviour equals the previously supported
    `network.enable_0rtt = false`, so no other config change is needed.
  - **Metrics removed:** `decdn_quic_0rtt_attempts_total`,
    `decdn_quic_0rtt_accepted_total`, `decdn_quic_0rtt_rejected_total`,
    `decdn_quic_session_ticket_cache_size`,
    `decdn_quic_session_ticket_peers_dropped_total`. All were label-free, so
    no dashboard query loses a dimension; panels referencing them go blank.
    Separately, `decdn_probe_collection_latency_seconds` — specified in
    `adr/appendix-observability.md` but never implemented — loses the
    `outcome={0rtt_warm,1rtt_cold}` label it was planned to carry. No
    deployed series is affected.
  - **API:** `decdn_protocol::SESSION_TICKET_CACHE_SIZE` is gone, and both
    `probe_once` requesters drop their 0-RTT switch and metrics-sink
    parameters.
- **Trusted-IP rate-limit exemption removed (#1440).** The `cdn/probe/v1`
  and `cdn/dht/v1` three-layer limiters no longer support an allow-list that
  bypasses the per-IP layer; every source IP is now bounded by that layer.
  - **Config-breaking:** the `probe.rate_limit.trusted_ips` and
    `dht.rate_limit.trusted_ips` fields are removed. Because both
    `[probe.rate_limit]` and `[dht.rate_limit]` use `deny_unknown_fields`, a
    config file that still sets either key now fails `decdn config validate`
    and node startup — delete the key. If the exemption was providing needed
    headroom, raise `per_ip_rate_per_sec` / `per_ip_burst` instead; that keeps
    the layer's invariant that no source IP is ever unbounded. Note the
    exemption never bypassed the per-peer or global layers, nor the always-on
    `security.per_source_*` bucket in front of the probe path.
- **`cdn/probe/v1` content-availability + slashing evidence (#318).**
  `cdn/probe/v1` is now a content-availability query, not just a
  latency/rate probe (ADR 005, ADR 014). Wire changes (same ALPN —
  this establishes the v1 signed baseline, not a version bump):
  `ProbeRequest` is now `{ hash, timestamp_us }` (was `{ nonce }`);
  `ProbeResponse` is now a signed `{ body: { hash, has_blob,
  rate_per_mb, timestamp_us }, total_bytes: Option<u64>, slash_sig }`
  (was `{ nonce, measured_at_unix_ms, node_id, rate_per_mb }`).
  `slash_sig` is a mandatory, non-empty EIP-712 secp256k1 signature
  (65-byte EOA `r‖s‖v` form in the PoC, ADR 024 §Off-Chain ERC-1271
  Verification); requesters reject
  missing/zero-length or wrong-length signatures.
  - **CLI** `decdn probe` now requires `--hash <BLAKE3>` (64 hex
    chars, the `cache.pinned_hashes` form). `--json` keys changed:
    removed `node_id`, `measured_at_unix_ms`, `nonce`; added `hash`,
    `has_blob`, `total_bytes` (nullable), `timestamp_us`, `slash_sig`
    (hex). `rate_per_mb`/`rtt_ms` unchanged.
  - **Config** new required `blockchain.slash_judge_address`
    (EIP-712 `verifyingContract` for `slash_sig`; no default — a
    wrong/zero address silently breaks every signature); new optional
    `blockchain.chain_id` (default 421614, Arbitrum Sepolia),
    `cache.max_probe_holds` (default 256; `0` disables `has_blob:
    true`), and `payment.delivery_floor`/`delivery_ceiling`
    (PoC-local rate-bounds clamp; defaults `0`..`MAX_RATE_PER_MB` =
    no-op). Env vars: `DECDN_SLASH_JUDGE_ADDRESS`, `DECDN_CHAIN_ID`,
    `DECDN_MAX_PROBE_HOLDS`, `DECDN_DELIVERY_FLOOR`,
    `DECDN_DELIVERY_CEILING`.

- **Config** Origin backend selection moved into a tagged
  `[cache.origin]` table (#437). The pre-existing flat
  `cache.origin_url`, `cache.origin_path`, and `cache.decompress`
  fields are removed; `CacheConfig` now carries
  `#[serde(deny_unknown_fields)]` so operators with the old shape get
  a clear "unknown field" error at config load instead of a silent
  "no origin configured" surprise. Migration:

  ```toml
  # before
  [cache]
  origin_url = "https://origin.example/"
  decompress = "auto"

  # after
  [cache.origin]
  kind = "http"
  url = "https://origin.example/"
  decompress = "auto"          # optional; defaults to "auto"
  ```

  ```toml
  # before
  [cache]
  origin_path = "/var/lib/decdn/origin"

  # after
  [cache.origin]
  kind = "fs"
  path = "/var/lib/decdn/origin"
  ```

  The same table also accepts `kind = "s3"` for the new S3 backend
  (see the Cache entry below for the supported keys and provider
  examples).
- **CLI** `--origin-url` / `--origin-path` flags (and their
  `DECDN_ORIGIN_URL` / `DECDN_ORIGIN_PATH` env vars) are removed
  (#437). Origin selection is now config-only — the S3 backend has
  too many fields (bucket, region, endpoint, credentials) to fit
  cleanly on a command line, and keeping all three backends file-only
  avoids the trap of a CLI-vs-TOML mismatch silently picking the
  wrong backend.
- **CLI** Split into two binaries (#421). The daemon is now
  `decdn-node` (single subcommand: `decdn-node run [--config <path>]`);
  `decdn run` no longer exists. The user CLI is `decdn` and gains
  `node {peers,health,announce,drain,evict,reload}`, `key-gen`,
  `config {init,validate}`, `probe` — all moved from the old fused
  binary, no behaviour changes. Container image entrypoint becomes
  `decdn-node`. Release archives ship two tarballs per target:
  `decdn-node-${VERSION}-${TARGET}.tar.gz` (operators) and
  `decdn-${VERSION}-${TARGET}.tar.gz` (publishers). See
  [`adr/appendix-binaries.md`](adr/appendix-binaries.md).
- **CLI** `decdn probe --json` `rtt_ms` field no longer carries
  trailing zeros. Pre-#421 always emitted three decimal digits via
  `{:.3}` (`12.500`); post-#421 the field is quantized to ms precision
  but emitted as a JSON number, so significant trailing zeros are
  dropped (`12.5`, `12.501`). Numerically identical to any JSON parser;
  operator scripts that match a `\.\d{3}` regex must update to
  `\.\d+`.
- **Config** Speculative prefetch removed (#1396, #1399). The entire
  `[prefetch]` config section is gone. Because `FileConfig` uses
  `deny_unknown_fields`, a config that still carries a `[prefetch]`
  table now **fails at startup** — and on `decdn node reload` — with an
  `unknown field` parse error instead of being silently ignored.
  Operators upgrading MUST delete any `[prefetch]` block from their
  config. Content propagation now relies solely on reactive cache-miss
  pull-through ([ADR 037](adr/037-regional-proxy-warming.md)) plus
  explicit operator pinning ([ADR 022](adr/022-content-discovery.md)).

### Fixed

#### Node serve path

- **An underfunded channel no longer gets one interval free per request
  (#1516).** The direct-serve path signed a success `StreamResponse` and streamed
  a full credit window — one 1 MB voucher interval at the default cadence, more
  if `credit_window_bytes` is configured — before the per-voucher deposit ceiling
  could fire at the first voucher boundary. A channel that could not cover even
  that first window was therefore served it anyway, on every request, and a
  client's resume loop could farm a fresh one per retry attempt. The serve path
  now reserves `min(credit window, requested span)` against the channel's
  *remaining* headroom (`deposit − last claimed amount`, matching how both the
  off-chain and on-chain ceilings compare cumulative voucher amounts) and refuses
  before signing, metered as `serve_stream_rejected_insufficient_deposit` — the
  same guard the cache-miss pull-through path has carried since #856, against a
  narrower ceiling. Residual free egress drops from a whole credit window to the
  bao proof overhead on one (well under 1%), since the reservation is priced in
  content bytes while delivery bills wire bytes; the mid-stream ceiling remains
  the exact authority. A funded request for a blob or bounded range smaller than
  one interval is unaffected — the reservation is capped by the span, not by the
  cadence. No wire, config, or ABI change.
- **A degraded node no longer reports itself as merely empty (#1129).** On a
  `cdn/client/v1` cache miss, a transient origin/store fault during a reactive
  pull-through fill (an S3 5xx surviving retry exhaustion, an open circuit
  breaker, an fs I/O error) was indistinguishable from a clean miss and refused
  with `CacheMiss` — wire `NotFound`, the code a *healthy but empty* node
  returns. Such faults now refuse with `InternalError` ("do not retry this
  node"), so clients route around a node whose origin is down and the operator's
  reject metric names the real cause — which matters because seven distinct
  reject reasons collapse to the single `NotFound` wire code, making the
  per-reason counter the only server-side place the true cause is visible. A
  genuine absence still returns `NotFound`; a deterministic refusal
  (`BlobTooLarge`, `HashMismatch`) is not a fault and does not steer clients off
  the node; and a fault on one tier is latched across a legitimate fall-through
  to a later tier. No wire change — `StreamError::InternalError` already existed;
  this is a reclassification within the existing surface.
- **A wedged upstream candidate no longer starves the fallback loop (#1141).**
  The window-paced node→node pull (`open_progressive_pull`) applied no
  per-candidate timeout, so a provider that accepted the connection and then went
  quiet consumed the entire outer deadline — which is deliberately sized to fit
  all `MAX_PROVIDER_ATTEMPTS` per-candidate budgets precisely so candidates #2..N
  stay reachable (#859) — and the serve path then refused a blob the honest
  fallback held. The progressive-OPEN stage is now bounded by `pull_timeout`, as
  the buffered fetch stage already was; a timed-out candidate is skipped and, per
  #857, not blamed. (The `open_or_reuse_channel` stage remains unbounded on both
  paths — a wedged on-chain RPC can still consume the outer deadline. Tracked
  separately.)

#### Origin directory

- **Origin-directory genesis replay no longer skips silently on an inverted
  block range (#1152).** Superseded before release by the #1504 enumeration
  rewrite, which removed genesis replay entirely: there is no `replay_from`, no
  persisted origin checkpoint and no `backfill_windows` call left on that path,
  so the inverted-range case it guarded against is unreachable and the
  `decdn_origin_directory_bootstrap_range_anomaly_total` counter it added no
  longer exists. Retained here only so the issue number resolves.

### Changed

#### Node selection & observability

- **`Candidate.stake` is now `u64`, not `Option<u64>`** (internal API,
  `decdn-node`). The `Option` conflated "not looked up" with "looked up, holds
  no bond", and the stake tie-break ranked unknown strictly *below* a known
  zero. That is a hazard the moment on-chain stake lookup is wired: a chain read
  succeeds for some peers and fails for others, so one flaky RPC call would have
  silently sunk a well-staked peer below an unbonded one. The type no longer has
  a way to express a failed read — the lookup layer must retry or drop the
  candidate. Nothing populates stake yet, so the tie-break tier is unchanged in
  behaviour.
- **The metric reason-split convention is settled: sibling counters, not
  labels.** `decdn_probe_hold_unavailable_total{reason}` remains the one labeled
  *reason split* — not the only labelled metric, since `decdn_streams_active`
  is labelled on another axis — and is now
  documented as the deliberate exception (its values share one aggregate and one
  budget axis; they pointedly do not share an alert, which is why the alert
  filters to `reason="exhausted"`); `dispatch_rejected_*`,
  `probe_rate_limit_rejected_*`, `channel_open_failures_*` and the gossip
  rejection counters stay siblings. **No metric is renamed.** The
  observability appendix is corrected accordingly: it documented
  `decdn_gossip_messages_rejected_total{reason="clock_skew"}`, a labeled name
  the node has never exported — the real series is
  `decdn_gossip_messages_rejected_clock_skew_total` — and the three sibling
  families are now listed in the registry, where previously they appeared in no
  operator-facing artifact at all.

#### Cache / node

- **Blob delivery no longer materialises the whole blob in memory.** The serve
  path drives a streaming bao export instead of building the entire aligned wire
  form up front, and the origin pull-through no longer reads a freshly-committed
  blob back out of the store to hand to `populate`, which discarded it. Serving a
  708 MB blob previously cost ~708 MB resident per concurrent serve, and again on
  the cache-miss leg; both are now bounded by one chunk group.
  - Observable change: the truncated-export refusal (the store's item channel
    closing without a terminal `Done`) can only be detected after the last item,
    so it now aborts the delivery **mid-stream** rather than failing before the
    first byte. The billing invariant is unchanged — the client sees a short
    delivery and never pays the closing voucher.

#### Contracts

- **`closeChannel`'s voucher-less path now works at any watermark.** Calling it
  with `amount == 0`, `nonce == 0`, `bytesDelivered == 0` and an empty signature
  skips voucher verification and closes at the recorded `claimed*`; the extra
  `claimedNonce == 0` condition that restricted it to channels no `withdraw` had
  ever touched is gone. The safety argument is unchanged — the path advances no
  watermark — and the old condition only forced a party with no newer voucher to
  wait for `expiresAt`. Observable change for anyone who built around the old
  revert: the call now succeeds where it used to fail.

#### Documentation

- **Safe-as-recommended-wallet and the node-side off-chain ERC-1271 path are
  dropped from the PoC surface (#1431).** ADR 024 keeps the piece that shipped
  — OpenZeppelin `SignatureChecker` at every on-chain verification site — and
  stops recommending a wallet. The encrypted EOA keystore is now the documented
  default for node operators and clients; a Safe, or any other ERC-1271 smart
  account, stays **supported** on the on-chain paths precisely because
  `SignatureChecker` is retained, but deCDN neither recommends one nor commits
  to tooling for one. By ADR 024's own words a 1-of-1 Safe carries "the same
  trust posture as today's `eth_keystore`", and the multi-owner threshold that
  would buy real security cannot be reached at `slash_sig` wire speed — so the
  recommendation delivered nothing the retained contract-level piece does not
  already enable, at the cost of a Safe-deployment step on every operator's
  critical path. The node-side off-chain ERC-1271 verifier (an address-code
  probe plus an `isValidSignature` RPC per client connection, behind a code
  cache) is relabelled from a PoC deliverable to Production-deferred, which is
  what the Rust has said all along. The Production session-key design
  (Safe-7579 + `erc7579/smartsessions`) is untouched and remains the answer to
  hot-path multisig and to smart-account clients.
  - **Contracts:** unchanged. `SignatureChecker` stays wired in
    `PaymentChannel` (voucher + provider waiver), `CapacityBond`
    (`registerNode` / `bindNodeId`), `SlashJudge` (rate / blacklist
    evidence), and `DecdnGovernor` (EIP-712 delegation); the
    `MockERC1271Wallet` fixtures and ERC-1271 branch tests stay with them. No
    ABI, deploy, or bytecode change. This is the insurance against a
    coordinated on-chain retrofit and is exactly why deferring the node-side
    path is cheap.
  - **Node:** no code removed — the off-chain ERC-1271 path was never built.
    `bind_sig::verify_binding`, voucher and `slash_sig` verification, and the
    65-byte length checks are unchanged. Only the deferral comments move to a
    stable citation: the `ADR 024 §18` **line**-number references in
    `crates/protocol` become `ADR 024 §Off-Chain ERC-1271 Verification`, the
    heading `crates/incentive` already cited, so a reworded ADR can no longer
    silently rot them.
  - **Config / CLI:** nothing removed. There is no Safe, smart-account, or
    wallet-type config surface, and `decdn setup` never grew the Safe-creation
    flow ADR 024 § Consequences promised — dropping that obligation retires an
    unmet promise rather than deleting a feature.
  - **Newly documented constraint:** a Safe-addressed *node operator* cannot
    serve traffic today. Requesters verify `slash_sig` off-chain by recovery
    against the registered address, and a Safe owner-key signature recovers to
    the owner, not the Safe. ADR 024 § Node Operators and
    `appendix-operator-key-rotation.md` now state this; the appendix's EOA →
    Safe migration is retained in full but is *optional* rather than
    *recommended*.
  - ADR 024 keeps its number and title — Safe is still supported. § Safe as
    Recommended Wallet becomes § Wallet Support — EOA Default, Safe Supported;
    § Off-Chain ERC-1271 Verification keeps its heading and both inbound
    anchors but loses its `alloy` implementation sketch and now reads as
    Production-deferred; § Session Keys is unchanged. ADR 003 § Smart Account
    Support and § Off-Chain (Ephemeral) Binding, ADR 012 § Ethereum Key and
    § Identity Lifecycle, ADR 019, `appendix-operator-key-rotation.md`, and
    `architecture.md`'s ADR 024 summary follow.

#### Runtime (observability)

- **The three probe-hold refusal counters are collapsed onto one `reason`
  label (#1443).** `decdn_probe_hold_violations_total`,
  `decdn_probe_holds_disabled_total` and `decdn_probe_stake_lane_reserved_total`
  answered one question — "could not hold, by cause" — under three names. They
  are now `decdn_probe_hold_unavailable_total{reason="exhausted"|"disabled"|
  "stake_lane_reserved"}`. Every semantic distinction is preserved as a label
  value, including that `stake_lane_reserved` fires *before* the hold attempt
  and so never consults the cache. All three children are materialized at
  startup, so each series is exported at zero from a fresh registry rather than
  appearing on first increment — the property the three separate counters had,
  and one a `Family` does not give for free. **Migration:** replace
  `decdn_probe_hold_violations_total` with
  `decdn_probe_hold_unavailable_total{reason="exhausted"}`,
  `decdn_probe_holds_disabled_total` with `{reason="disabled"}`, and
  `decdn_probe_stake_lane_reserved_total` with `{reason="stake_lane_reserved"}`.
  The shipped `monitoring/prometheus-alerts.yml` and
  `monitoring/grafana-dashboard.json` are updated in place; the
  `DecdnProbeHoldViolations` alert keeps its name and now filters on
  `reason="exhausted"`, which is what keeps a deliberate disable or a
  stake-lane reservation from tripping a "raise `max_probe_holds`" page. Custom
  dashboards querying the old names go blank. Note this makes
  `probe_hold_unavailable` the one labeled counter in `decdn-node`; the other
  reason-style splits (`dispatch_rejected_*`, `probe_rate_limit_rejected_*`,
  `channel_open_failures_*`) remain sibling counters for now.

- **The `decdn_node_address_watcher_*` metrics are removed (#1231).** Gone:
  `decdn_node_address_watcher_restarts_total` and
  `decdn_node_address_watcher_down_seconds`. Since #1226 collapsed the
  node-address and staker-set watchers into one `capacity-bond` loop, these were
  a perfectly-correlated shadow of `decdn_staker_set_watcher_restarts_total` /
  `decdn_staker_set_watcher_down_seconds` — one loop's health reported twice.
  **Migration:** use the `decdn_staker_set_watcher_*` family, which now covers
  the bindings projection too because the same loop feeds it. Worse than
  redundant, the removed family was gated on the bindings projection existing,
  so a node with `cache.node_to_node_pull_through_enabled = false` reported
  `down_seconds` frozen at `0` forever — an alert that could never fire.
  `decdn_node_address_directory_size` is **not** affected: it measures the
  projection's cardinality rather than the loop's health, and is retained. Note
  that it is exported even when pull-through is off, where it sits at a
  permanent `0`; scope any alert on it to nodes with pull-through on.

#### CLI

- **`decdn fetch` streams to disk and resumes an interrupted download.** Verified
  bao chunk groups are written to `<output>.partial` as they land and the file is
  renamed into place at the end, instead of the whole blob being buffered in RAM
  (twice — wire form then decoded form) and written once. Peak memory is now
  independent of blob size. If a fetch is interrupted, re-running it picks the
  partial up, asks the node for the un-fetched tail only, and **re-pays only for
  that tail**; previously it restarted from byte 0.
  - Observable change: a failed fetch now leaves a `<output>.partial` file behind
    on purpose — that is what the next run resumes from. It is renamed into place
    on success, and discarded on a failed integrity check or when the node cannot
    serve a resume at its offset (which means it belongs to a different blob).
  - Bytes inherited from a previous run's partial are not verified as they are
    read: the CLI persists no bao outboard sidecar beside the `.partial`, so it
    has nothing to check the prefix against. A **resumed** fetch therefore
    re-hashes the assembled file against the content hash before promoting it,
    and on mismatch discards the partial and fails rather than writing a wrong
    output file. A fetch that started at byte 0 skips this — every byte was
    verified on the wire as it landed.
- **`decdn node channels` gained a `SIGNER` column.** It reports the channel's
  pinned `voucherSigner` — the key whose signature is required on every voucher —
  next to `COUNTERPARTY` (the funder, and the address the ADR 011 compliance
  gates check). For every channel opened today the two are equal, because a zero
  `voucherSigner` argument to `openChannel` resolves on-chain to `msg.sender`;
  they diverge only when a funder pins a delegate. **Output-breaking for scripts:**
  the table is ~15 characters wider and column positions after `COUNTERPARTY`
  have shifted, so positional parsers (`awk '{print $N}'`, fixed-offset `cut`)
  need updating.

- **The CLI now rejects the zero address for the four addresses resolved by
  `resolve` / `resolve_appeal` / `resolve_publish`, not just the appeal address
  (#1153).** Those four (`capacity_bond_address`, `slash_appeal_address`,
  `publisher_registry_address`, `origin_assignment_address`) route through a
  shared `parse_nonzero_address` guard, so a misconfigured `0x0000…0000` fails
  fast at resolve time with a clear "must not be the zero address" error instead
  of an opaque on-chain revert later. Previously only `slash_appeal_address` was
  guarded. This is a new hard error on `0x0` for the three other addresses
  (present-but-zero only; an unset optional publish address still resolves to
  `None`).

- **The zero-address guard now covers every parsed contract address across the
  CLI, not just the resolve\* family (#1213).** The `fetch`
  (`payment_channel_address`, `slash_judge_address`, `capacity_bond_address`),
  `channel` (`payment_channel_address`), and `setup` (`usdc_address`) sites, plus
  the swap venue addresses parsed in `decdn-incentive`'s `swap_venue`
  (`swap_router_address`, `swap_quoter_address`, `swap_pool_address` /
  `swap_balancer_pool`, `usdc_address`), now reject `0x0000…0000` with the same
  "must not be the zero address" error instead of an opaque on-chain revert later.
  This closes the inconsistency where the *same
  logical address* was guarded via `resolve()` but not via `fetch`. New hard error
  on `0x0` (present-but-zero only; unset optionals still resolve to `None`).
  Account/EOA addresses (`--provider-address`, `operator`) are unchanged — the
  guard is contract-specific.

#### Gossip

- Gossip publisher / subscriber / TTL-sweeper tasks now shut down
  cooperatively via a `CancellationToken` owned by `GossipService`,
  instead of the runtime reaching in with `JoinHandle::abort()` (#805).
  Each loop returns at a clean await boundary on cancellation — including
  interrupting the subscriber's reconnect backoff — so the drain phase
  finishes promptly without abrupt mid-await cancellation. Lifecycle
  ownership only; no steady-state behavior change.

### Added

#### Cache / config

- **`cache.prewarm` — opt-in remote-origin warm of the pinned set.** With
  `prewarm = true`, a node fetches every hash in `cache.pinned_hashes` from its
  configured `http`/`s3` origin at startup and again on `decdn node reload`,
  so the first request for pinned content does not pay full pull-through
  latency. Defaults to `false` and is **restart-required**: prewarm spends
  origin egress on bytes nobody has asked for yet, so it never turns itself on
  across an upgrade. An **fs-only** origin chain ignores the flag — that content
  is already local and is advertised through the origin-held index, so importing
  it would only duplicate the bytes on the same disk; a *mixed* fs+remote chain
  does warm, and pins the fs entry serves are imported as part of that.
  `decdn config validate` reports a flag set against an fs-only or empty chain
  as `set but INERT` rather than echoing it back. Warming runs detached, so an
  unreachable origin cannot block bring-up; it is cancelled at the top of
  shutdown so a restart mid-warm does not manufacture a false
  `prewarm_failures_total` spike (cancellation is checked between hashes, so a
  single in-flight blob can still fail against a closing store); it fills local-origin-only, so it never fronts
  USDC to a peer; and a hash filled by a concurrent pass is not counted as a
  fetch, so overlapping warms do not double-count paid egress. A reload warms
  only when it actually adds a pin, and at most one warm runs at a time.
  - New metrics: `decdn_cache_prewarm_blobs_total`,
    `decdn_cache_prewarm_bytes_total`, `decdn_cache_prewarm_refused_total`,
    `decdn_cache_prewarm_failures_total`.
- A pinned set larger than `cache.cache_size_mb` now logs a warning at startup
  and on reload. Pinned blobs are LRU-exempt, so the eviction driver could
  otherwise never reach its high-water target and the disk grows past the
  configured ceiling. Independent of `prewarm` — the hazard applies to every
  node however the pinned content arrived.
- `decdn config validate` now reports `fs_rescan_interval_sec` and `prewarm`,
  and `decdn config init`'s template documents both. `fs_rescan_interval_sec`
  shipped without either, so its resolved value was invisible to operators.

#### Contracts

- `PaymentChannel.closeChannelWithoutVoucher(bytes32)` — a named entry point for
  the voucher-less close, callable by either party. It takes the same path as
  `closeChannel` with all-zero arguments and an empty signature: no signature
  check, no watermark advance, the channel enters `Closing` at its recorded
  `claimed*` and emits the same `ChannelCloseInitiated`. Purely additive; the
  all-zero `closeChannel` spelling still works.

#### Node runtime & wire protocol

- Download-receipt audit log (`download_receipts.jsonl`) is now bounded by
  size-based rotation (#802). New optional `[receipts]` config section:
  `receipts.max_file_bytes` (default 128 MiB; rotate the live file at this
  size) and `receipts.retained_files` (default 4; numbered backups
  `download_receipts.jsonl.1`..`.N` to keep, `0` truncates in place). Bounds
  `data_dir` growth to roughly `(retained_files + 1) * max_file_bytes`.
  Config-additive — absent section preserves prior behaviour with the
  defaults; rotation is best-effort and never aborts paid delivery.
- iroh QUIC endpoint bring-up with protocol router (#213).
- `cdn/probe/v1` ALPN with ADR-013 varint framing and `ProbeMessage`
  request/response (#225).
- Compile-time guardrail on `MAX_MESSAGE_SIZE = 16 MiB` per ADR 013 (#287).

#### Cache

- Cache engine wrapping `iroh-blobs` with pull-through on cache miss (#232).
- HTTP origin adapter with connect / response-headers / chunk-idle timeouts.
- Filesystem origin adapter using a git-style sharded layout
  (`{base}/{hex[0..2]}/{hex}`).
- Origin pull-through retry policy with exponential backoff and jitter
  (#285). Configurable via `[cache.origin_retry]` (`max_retries`,
  `initial_backoff_ms`, `max_backoff_ms`, `jitter_ratio`); set once at
  startup, changes require a restart. **Behaviour change:** *enabled by
  default* — cache misses now retry transient HTTP (5xx/408/429/timeouts)
  and filesystem (Interrupted/TimedOut/ResourceBusy/WouldBlock) failures
  up to 3 times with 100ms…10s exponential backoff. Operators relying
  on first-attempt failure semantics must set
  `cache.origin_retry.max_retries = 0`. The active policy is logged at
  startup on the `cache engine ready` line. Two
  `decdn_cache_*_total` Prometheus counters track per-fetch volume
  (`origin_fetches_total`) and terminal exhaustion of the retry budget
  (`origin_retry_exhausted_total`); operators alert on the rate ratio.
  The `Origin` trait surface changed to return
  `Result<OriginFetch, OriginPullError>`; downstream `Origin` impls (if
  any out-of-tree) must be updated.
- S3-compatible origin backend (#437). Supports plain AWS S3, Cloudflare
  R2, Backblaze B2, MinIO, and any other service that speaks the S3 API.
  Object keys follow the same `{prefix?}{hex[0..2]}/{hex}` sharded layout
  as the filesystem origin so operators can `aws s3 sync` blobs between
  the two without renaming. The SDK is wired with `aws-config` for the
  AWS-CLI-equivalent credential chain (env vars, `~/.aws/credentials`
  profile, container/instance role) plus an explicit static-credentials
  variant for non-AWS providers. The HTTP layer uses `aws-smithy-http-client`
  on hyper-1 + rustls 0.23 + aws-lc-rs to match the rest of the workspace
  TLS stack — the SDK's stock hyper-0.14 + rustls 0.21 + ring stack is
  suppressed via `default-features = false`.

  ```toml
  # AWS S3 (default credential chain via env / profile / IAM role)
  [cache.origin]
  kind = "s3"
  bucket = "decdn-blobs"
  region = "us-east-1"

  # Cloudflare R2 (virtual-hosted-style addressing on a custom endpoint;
  # static credentials read from an R2 API token)
  [cache.origin]
  kind = "s3"
  bucket = "decdn-blobs"
  region = "auto"
  endpoint_url = "https://<account-id>.r2.cloudflarestorage.com"
  prefix = "blobs/"

  [cache.origin.credentials]
  source = "static"
  access_key_id = "<R2 access key>"
  secret_access_key = "<R2 secret>"

  # MinIO (path-style addressing required; static creds for the local IAM
  # surface)
  [cache.origin]
  kind = "s3"
  bucket = "decdn-blobs"
  region = "us-east-1"
  endpoint_url = "http://minio.internal:9000"
  path_style = true

  [cache.origin.credentials]
  source = "static"
  access_key_id = "minioadmin"
  secret_access_key = "minioadmin"
  ```

  `Content-Encoding` on S3 responses is handled the same way as the HTTP
  origin (#804): gzip/zstd bodies are transparently decompressed to
  canonical bytes before the engine's BLAKE3 verify. Controlled by the
  optional `[cache.origin] decompress` knob (`"auto"` default decompresses;
  `"strict"` refuses any non-identity encoding), mirroring the HTTP origin.
  Unknown encodings (e.g. `br`) are rejected with an operator-actionable
  permanent error.

  **Config-breaking (default shift):** the S3 backend previously rejected
  *all* `Content-Encoding`; it now defaults to `decompress = "auto"`.
  Operators who relied on that blanket rejection as a guard against
  mis-stored objects should set `decompress = "strict"` to keep refusing
  encoded bodies. The shift is safe by construction — BLAKE3 verify runs
  over canonical bytes, so a mis-decode fails closed as a hash mismatch
  rather than caching corrupt data.

  This release also fixes a latent #804 bug on the **HTTP** origin (a second
  backend, not part of the pure S3 extraction): a compressed body whose
  *encoded* `Content-Length` fit under `cache.buffered_max_bytes` but decoded
  above it was falsely rejected as `BlobTooLarge`. Compressed responses now
  report no `size_hint`, so they always take the streaming path capped at the
  blob-size limit — parity with the S3 fix, pinned by an HTTP regression test.

#### Gossip

- `NodeAnnounce` publish/subscribe over `iroh-gossip` with an in-memory
  peer table (#231).
- Topics: `cdn/global/v1` and `cdn/region/{code}/v1`.
- Validation of signature, timestamp skew, region, and `popular_hashes`
  length/dedup.

#### Config

- Three-layer resolution: CLI flag > TOML file > built-in default.
- `${VAR}` and `~` expansion in TOML fields (#226).
- EIP-55 checksum validation for contract addresses (#227).
- `max_blob_size_mb < cache_size_mb` enforced at startup (#256).
- `rate_per_mb > 0` enforced (#251).
- `eth_keystore` readability verified at startup (#259).
- `identity.region` required when `gossip.subscribe_global = true`.

#### CLI

- `decdn-node run` — run the daemon.
- `decdn config init` / `decdn config validate` (#229).
- `decdn key-gen` — Ed25519 node key + Ethereum keystore.
- `decdn probe` — one-shot latency probe over `cdn/probe/v1`.

#### Observability

- Prometheus metrics over loopback HTTP at `observability.metrics_port`
  (ADR 020).
- Optional OTLP span export behind the `otlp` feature flag.
- `pretty` and `json` log formats (`--log-format`).

#### Identity

- Ed25519 node-identity key at `{data_dir}/node.secret`.
- Permissions validated at load: `0600` on the key file,
  non-world-writable parent (#261).

#### Documentation

- 25 Architectural Decision Records (ADRs 000–025) — see `adr/` for the
  full set and `adr/architecture.md` for the living overview.
- `CONTRIBUTING.md` with build / lint / test commands and pre-commit setup.

### Security

- `quinn-proto` → 0.11.16 (GHSA-4w2j-m93h-cj5j: remote memory exhaustion in
  the QUIC state machine, fixed in 0.11.15) (#1465). Lockfile-only bump; no
  dependency requirement changed.
- **Gossip rule-2 enforced against the live registry, not a static allowlist
  (#1170).** `NodeAnnounce` admission (ADR 001 rule 2) now checks the announcer
  against the live on-chain staker set (`CapacityBond`, kept fresh by the
  `NodeRegistered` / `NodeDeregistered` / `NodeAutoEjected` event tail) instead
  of the file-configured allowlist, so a deregistered or slashed node can no
  longer enter peer tables during its stale-cache window. The rejection metric
  label changed from `not_allowlisted` to `not_staked`.
  - **Config-breaking:** the `gossip.allowlist` field is removed. Because
    `[gossip]` uses `deny_unknown_fields`, a config file that still sets
    `gossip.allowlist` now fails `decdn config validate` and node startup —
    delete the key. The staked-node check is no longer operator-tunable; it is
    always enforced against the registry.
- `rustls-webpki` → 0.103.12 (RUSTSEC-2026-0098, RUSTSEC-2026-0099) (#253).
- `rustls-webpki` → 0.103.13 (RUSTSEC-2026-0104: reachable panic in CRL
  parsing) (#286).
- **DHT rate-limiter keyspace bounded (#645).** The `cdn/dht/v1` per-IP and
  per-peer keyed token-bucket maps grew unboundedly under churning sources;
  same DoS shape that #440 fixed for the connection-level dispatcher. Adds
  `[dht.rate_limit] max_tracked_per_ip` / `max_tracked_per_peer` knobs
  (default 4096, `0` = unbounded with a `tracing::warn!` on resolve), an
  opportunistic prune from `check` at `cap + cap/10`, and a periodic GC
  task at 60s that calls `retain_recent` on both keyed maps. New metrics:
  `decdn_dht_rate_limit_prune_sweeps_{per_ip,per_peer}_total` and
  `decdn_dht_rate_limit_tracked_{per_ip,per_peer}`.

[Unreleased]: https://github.com/decdn/decdn/commits/main
