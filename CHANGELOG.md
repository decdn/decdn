# Changelog

<!-- Maintained by hand, one entry per PR. `cargo release` does NOT rewrite
     this file — git-cliff is used only to generate the GitHub release notes
     (see .github/workflows/release.yml). Keep entries grouped by subsystem
     as described under Conventions below. -->

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

- **An underpaying voucher gets an `Underpaid` rejection carrying the node's
  watermark, and a diverged payer lane heals itself.** Wire-breaking:
  `VoucherRejectReason::Underpaid` is appended as discriminant 14 and is
  watermark-gated. A node used to answer a voucher that underpaid its span by
  dropping the stream with no frame, so the payer saw
  `frame I/O error: early eof`. The payer had already counted that voucher as
  paid, because a voucher is committed once its send succeeds, and it saved
  it. After that, every later voucher looked short to the node and the lane
  was stuck for good, across restarts. The node now sends
  `VoucherRejected { Underpaid, bundle }` with its last-accepted watermark.
  The payer checks the bundle against its own signature and moves its ledger
  down to it with `PoolLedger::rebase`, which waits for any voucher still
  being sent. Each rebase starts a new ledger generation, and every voucher
  proof carries the generation it was signed under
  (`StreamProof::Voucher { generation }`,
  `UpstreamVoucherRejected::proof_generation`). A rejection of a voucher from
  an older generation is stale: the payer retries without moving again. A
  rejection from the current generation rebases again. The payer saves the
  rebased watermark once, through the new `BuyerPoolStore::rebase_progress`
  (overwrite down to that watermark, then advance). Every later save is a
  monotone advance again. The node does the same for the pools it pays from:
  `record_progress` takes the rebase point. A bundle-less `Underpaid` on the
  node's pull leg is ruled `OurLocalFault` rather than a dead lane. ADR 003 §
  Voucher withholding and ADR 005 § `VoucherRejected` semantics describe the
  behaviour.
- **Buyer pool rows are scoped to their `PaymentPool` deployment, and the
  buyer table moves to `_v4` (#2087).** `PaymentPool.openPool` derives
  `poolId = keccak256(owner, ownerPoolNonce)` — no contract address, no chain
  id — and a fresh deployment restarts that nonce at zero, so the same owner's
  Nth pool carries a byte-identical id on every deployment. A persisted row was
  therefore ambiguous across a redeploy, and worse than ambiguous: once the
  owner's nonce walked back over the tracked id, the row named an existing,
  unrelated pool, and the node resumed lane progress the live pool had never
  redeemed against — paying a provider for bytes it never delivered.
  `BuyerPoolState` now carries `payment_pool`, every path that reuses a row
  checks it (node bootstrap, the node's pull hot path, `decdn fetch`), and the
  node drops a foreign row at bootstrap. **Both buyer tables move from `_v3` to
  `_v4`, which orphans every row written by an earlier binary** — including
  rows for pools on the configured contract. Those read as an empty store on
  first boot, and both a node and a client re-adopt the live pool from chain
  (see the client-adoption entry under Fixed). A
  deposit held by an orphaned row is recoverable only against the contract it
  was opened on, and the bootstrap warning names that address. `payment_pool`
  is now surfaced by `decdn pool list --json` and `admin_v1_pools`.

- **CLI: `decdn fetch` and `decdn bundle pull` refuse a `decdn-node` data dir
  they were not pointed at (#2082).** `decdn pool` already refused to escrow
  into a daemon's data dir (#2078), but these two opened the client store on
  the same resolved `data_dir` with no such check — and the keystore defaults
  to that dir too, so on a node host they opened a client pool under the
  **node's own operator address**. The daemon's stranded-pool report then names
  a pool a human may be fetching against, and its adoption path can take that
  pool over, putting two independent voucher watermarks on one lane. A human
  buying on a node host is a legitimately separate client, so the rule is
  weaker than `pool`'s: `--data-dir <node dir>` on the command line is accepted,
  while arriving there implicitly — `identity.data_dir` from
  `~/.decdn/node.toml`, which is the node's dir on a node host — is refused.
  Scripts that ran a bare `decdn fetch` on a node host must add
  `--data-dir <client dir>` (the usual intent) or name the node's dir
  explicitly. The refusal runs before the keystore prompt. Recorded in
  [ADR 012](adr/012-client.md) beside the store-split paragraph, where the
  asymmetry was previously an omission rather than a decision.

- **CLI: `decdn pool` refuses to write a store a `decdn-node` daemon owns
  (#2078).** Every store-backed `pool` subcommand — `list`, `open`, `top-up`,
  `close`, `reclaim`; `assign` touches no store — used
  `<data_dir>/buyer-pools.redb`, the *client's* store. A daemon keeps its buyer state in `<data_dir>/buyer.redb`, so
  on a node host the CLI read and wrote a different file than the node it was
  pointed at — and `Database::create` manufactured that file where none existed.
  A data dir containing any of the daemon's store files (`buyer.redb`,
  `lanes.redb`, `settle.redb`, `checkpoint.redb` — the whole set, because the
  buyer store is exactly the file a reset loses) is now recognized as a
  daemon's:
  `pool open` and `pool top-up` refuse (they escrow USDC into a store the node
  never reads, which is exactly the stranded deposit #2075 exists to prevent);
  `close --all` and `reclaim --all` refuse (they enumerate from chain by keystore
  address, so they would close the pool the daemon is paying from right now);
  `close --pool` and `reclaim --pool` still run their on-chain leg — the stranded
  pool recovery path — but no longer report a no-op local `forget` as a clean
  close, naming the daemon row they could not touch instead. `pool list` routes to
  the daemon (see Added) and prints `store=<path>` above the table, so `pools=0`
  names the file that produced it. `pool list --json` gains `store` and `source`
  fields instead; read `source` before the pool objects, because the two stores
  emit different shapes (`deposit_usdc` as a decimal string and lowercase
  addresses from the client store, `deposit_micro_usdc` as a number and EIP-55
  addresses from the daemon). `decdn node pools` names the admin URL it asked.
  Scripts that ran `pool open` against a node config must stop: the daemon opens
  and tops up its own pool from `blockchain.buyer_working_deposit_micro_usdc`.

- **Cache: `Origin::fetch_range` is now the data-only `Origin::fetch_range_data`
  (#2065).** The outboard comes from `Origin::fetch_outboard`, read once per fill.
  A custom `Origin` that overrode `fetch_range` must implement both methods;
  overriding only one silently disables range pulls. `CacheEngine::origin_encode_range`
  is now the streaming `CacheEngine::origin_range_wire`.
- **Node: every authorized `cdn/client/v1` cache miss — whole blob, bounded range,
  resumed tail — routes through the two-leg serve-miss spine when its origin (or a
  window provider) is serviceable (#2060).** The buffered `try_range_pull_through`
  tier and `CacheEngine::pull_through_range` / `RangePullOutcome` are deleted: the
  spine signs `StreamResponse` before the first origin draw and never holds a
  requested span in memory. The whole-blob fallback tiers (local populate, buffered
  pull-through) remain for origins with no `{H}.obao4`. `FillRegistry::claim`
  attaches a request to a live fill only when the request starts at or behind that
  fill's paid frontier (#2062).

- **Contracts: the Balancer V3 buyback venue is deleted; Uniswap V3 is the sole
  canonical TOKEN/USDC POL and buyback venue (#2043).** `BuybackBurnerBalancerV3`, the
  three `IBalancerV3*` interfaces, `IBalancerV3PoolCreation`, `IPermit2` (Permit2 was
  Balancer-only — Uniswap V3 pulls `tokenIn` through a direct ERC20 allowance), and the
  four Balancer test/fork suites are gone. The abstract `GuardedBuybackBurner` base and
  the single concrete `BuybackBurnerUniswapV3` subclass stay, so a second venue is a
  subclass-and-deploy change rather than a dispatch layer.
  - `BuybackVenueLib` loses the venue enum, the venue-string parser and the seed/venue
    drift machinery; it holds `steadyShares()`, `requireUniswapWiring` and
    `deployUniswapBurner`. `BuybackActivation` carries one unconditional
    `UniswapVenueParams uni` field in place of the `uni`/`bal` pair, so
    `VenueFieldsCrossWired`, `VenueFieldsUnwired` and `PoolSeedVenueMismatch` go with
    the choice they guarded. `_assertVenueSeedMatches` is now `_assertPoolSeedDerived`.
  - Deploy and activation drop the `BUYBACK_VENUE` and `BALANCER_POOL` env vars along
    with the Balancer Vault/Permit2 ones. `contracts/foundry.toml` drops the `arbitrum`
    and `sepolia` `[rpc_endpoints]` aliases, and the `solidity fork test` job drops the
    matching `ARBITRUM_RPC_URL` and `SEPOLIA_RPC_URL` secrets: one fork suite remains,
    `GenesisBuybackActivation.fork` on `ARBITRUM_SEPOLIA_RPC_URL`, superseding the
    four-suite counts in the fork-RPC and #1090 entries below.
  - [ADR 018](adr/018-liquidity-strategy.md#adr-018-liquidity-strategy-uniswap-v3-5050-pol)
    is rewritten around a full-range 50/50 Uniswap V3 TOKEN/USDC pool, dropping the
    80/20-weighted decision and the Balancer-specific Vault/Permit2/Router indirection.
    The venue-neutral MEV stack (TWAP floor, `minTokenOut`, private-RPC routing,
    per-epoch cap) is unchanged.
  - POL keeps its 10% allocation, but the pool is not seeded at genesis: the DAO funds
    the USDC side from its 10% `FeeRouter` revenue share and creates the pool at its own
    discretion, then deepens it from a Timelock reserve and the buyback glidepath (each
    swap adds USDC to the pool, so seed depth is a floor that glides up). The
    [ADR 026](adr/026-tokenomics.md#bootstrap-mechanism--pre-seed-usdc) pre-seed table
    therefore drops the POL-seed earmark into operator infrastructure subsidies —
    ~85% subsidies, ~10% incident contingency, ~5% audits and legal.

- **Runtime: OTLP span export is always compiled into `decdn-node`; the `otlp`
  cargo feature is gone (#2039).** Release archives and the Docker image now honour
  `observability.otlp_endpoint` / `--otlp-endpoint` / `DECDN_OTLP_ENDPOINT`
  instead of ignoring it with a stderr warning. With no endpoint set, no OTel
  layer is installed. Building with `--features otlp` is now an error.
  - The endpoint must be `http://host:port`: config resolution rejects
    `https://` (the exporter has no TLS), a missing port, a path, query or
    fragment (an OTLP/HTTP `…:4318/v1/traces` URL is the wrong protocol),
    userinfo (including an empty `@`), and surrounding whitespace. Rejections never echo any part of the
    endpoint. An exporter that fails to build aborts start-up.
  - New counter `decdn_otlp_export_failures_total` and warning alert
    `DecdnOtlpExportFailing` surface a dead or wrong collector. The counter
    counts failed export batches only: queue-full drops and partial-success
    rejections are not counted. While a collector is down, the SDK also logs
    each failed batch at `error` under the `opentelemetry_sdk` target.
  - A failed run logs its (redacted) error before the exit-time flush, so the
    error is exported with the run's last spans.
  - A SIGHUP that changes a restart-required `[observability]` field
    (`log_format`, `metrics_port`, `metrics_bind`, `admin_port`,
    `otlp_endpoint`) now logs one `warn` per changed field, comparing the
    resolved value (CLI/env > file) with startup. An unchanged field stays
    silent, where it previously logged an `info` notice on every reload.
  - Queued spans flush on exit, on both the clean and the failed-run path. The
    flush runs on its own thread and the exit path waits at most 6 s for it.
  - The OTel layer drops `h2`/`hyper`/`hyper_util`/`tonic`/`tower` spans, so
    `trace` level cannot feed the export connection back into itself.

- **Contracts: `OriginAssignment` drops its `ContentBlacklist` coupling; the
  routing consumer filters instead (#2032).** The seat set is a routing hint, so
  blacklist filtering moves to the party that acts on it. Removed from
  `OriginAssignment`: the `contentBlacklist` pointer and `setContentBlacklist`
  setter, the `ContentBlacklistUpdated` event, the `addOrigin` blacklist guard,
  and the `ContentBlacklistNotSet` / `OperatorBlacklisted` / `OperatorNotBlacklisted`
  errors. The `contentBlacklist_` constructor argument is gone, which erases the
  post-deploy `setContentBlacklist` wiring step (ADR 016 § Post-Deployment
  Initialization renumbers 1–6) and the deployment-window bootstrap. The
  `IContentBlacklistOriginView` interface is deleted (`OriginAssignment` was its
  only user; `SlashJudge`'s `IContentBlacklistHashView` is unrelated and
  unchanged).
  - **Renamed** (an indexer should map these rather than treat them as removed):
    `pruneBlacklistedOrigin(uint256,address)` → `pruneInactiveOrigin(uint256,address)`,
    now keyed on `CapacityBond.isActive(operator) == false` (reverts
    `OperatorStillActive`) rather than on blacklist membership — it reaches every
    exit from the active set, including an operator-level blacklist, which ejects
    from `CapacityBond`. Event `BlacklistedOriginPruned` → `InactiveOriginPruned`
    (same topic shape: `(uint256 indexed, address indexed, address indexed)`).
  - The node's chain-backed origin directory now drops any `getOrigins` operator
    on its live blacklist deny-set (the origin ∪ operator union already synced
    for the delivery gate), applied at resolve time so a governance blacklist
    takes effect without waiting out the `getOrigins` cache TTL. ADR 011
    § Interaction with ContentBlacklist and ADR 016 are rewritten to match.
- **CLI/runtime: a keystore password source is chosen by PRESENCE, not by being
  non-empty, so an empty password is a password.** `DECDN_KEYSTORE_PASSWORD` set
  to the empty string, and a `--keystore-password-file` that is empty after the
  one stripped trailing newline, each supply the empty string instead of falling
  through to the next source. A password file that does not EXIST is now the
  only file case that falls through; a path that exists but cannot be read — a
  directory, a permission denial, non-UTF-8 contents — is a hard error, as is an
  env var set to non-UTF-8. Precedence is unchanged: env var, then file, then an
  interactive prompt on a TTY.
  - Why: the old rule could not tell "the operator chose no password" from "the
    source is absent", so a deliberately empty password — what tools that write
    a keystore with no password produce — was unreachable from any
    non-interactive source, and an empty file silently fell through to a prompt
    a systemd unit can never answer.
  - Two operator workflows change. `export DECDN_KEYSTORE_PASSWORD=` no longer
    means "unset"; `unset DECDN_KEYSTORE_PASSWORD` does. And a mistyped
    `--keystore-password-file` no longer fails at the read: on a terminal it
    prompts, and headless it fails with `no keystore password source available
    (...)`, which now lists every source that fell through — including the path
    that was tried — rather than only the last one.
  - `decdn fetch`, `decdn bundle pull`, and `decdn pool` gain
    `--keystore-password-file` (env `DECDN_KEYSTORE_PASSWORD_FILE`), the flag
    the operator commands and `decdn-node run` already had. Without it those
    commands had only the env var and a prompt, so a headless client had no file
    source to make empty. `decdn key-gen --keystore-password-file` also
    tilde-expands its argument now, matching every other path flag — an
    unexpanded `~/pw.txt` would otherwise fall through silently.
  - `decdn key-gen` warns on stderr when it CREATES a keystore under an empty
    password. The file stays encrypted and stays `0o600`, but a password anyone
    can guess leaves that mode and the `0o700` data dir as the only protection.
    Creation only — warning on every load would fire on every fetch.

- **CLI: `decdn key-gen` renames its password-file flag from `--password-file`
  to `--keystore-password-file`**, matching the spelling every other command
  uses. The env var `DECDN_KEYSTORE_PASSWORD_FILE` is unchanged; scripts or
  runbook steps that pass the old flag must be updated.

- **config: `cache.tinylfu.sketch_bytes` now carries a floor of 16384 bytes,
  enforced whichever cache policy is selected.** A value below the floor is a
  load-time error on both `decdn node` startup and `decdn config validate`; the
  node never clamps it, because a clamp hides the mistake. The check does not
  consult `cache.eviction_policy` / `cache.admission_policy`, and for this knob
  that is not merely defensive: the default `cache.serve_economics.policy =
  "margin"` builds the same frequency sketch, so `sketch_bytes` sizes a live
  estimator on a node running the default `lru` / `always` selectors. Operators
  with an explicit `[cache.tinylfu] sketch_bytes` below 16384 must raise it
  before upgrading. See [ADR 040 § Configuration
  surface](adr/040-cache-policy.md).

- **`ProbeResponse` and `StreamResponse` unsigned fields moved into trailing
  extension structs; `cdn/dht/v1` gains the same seam.** `ProbeResponse` is now
  exactly `{body, slash_sig}` with `total_bytes` in a new `ProbeResponseExt`;
  `StreamResponse` is `{body, slash_sig}` with `error` in `StreamResponseExt`.
  Both are encoded and decoded two-phase, via `encode_probe_response` /
  `parse_probe_response_ext` and the `StreamResponse` twins. `dht.rs` gains
  `FindValueResponseExt` and `StoreRequestExt` (both empty today) plus
  `encode_find_value_response` / `encode_store_request`, wired through the DHT
  handler's write and read paths so all three ALPNs carry the seam ADR 013 §Tier 1
  requires — and so the DHT half is exercised rather than merely reserved.
  - Why: postcard fills no defaults for absent trailing fields, so an `Option<T>`
    appended to an existing struct fails to decode against an older sender. A
    separately-encoded extension is the only way to add an unsigned field without
    an ALPN bump. The ADRs already described these fields as trailing; the code
    had them embedded between `body` and `slash_sig`.
  - Why now rather than later: only the *relocation* is wire-breaking and so
    pre-launch-only. The extension mechanism itself could be added at any time —
    every reader decodes with `take_from_bytes` and already tolerates trailing
    bytes. What this buys is a frozen base that is exactly signed content plus its
    signature, with every unsigned field in one place.
  - `StreamResponse::validate` no longer checks `ok`/`error` agreement, because it
    can no longer see `error`. That rule moved to `StreamResponseExt::validate`,
    which takes `ok`; a receiver must call both.
  - No contract change: `SlashJudge` is coupled to the signed field set, not the
    wire encoding — the challenger re-encodes the four signed fields as a fresh
    ABI blob, so an unsigned extension is invisible on-chain.

- **`cdn/client/v1`: `ChunkData` frame size is now the sender's choice.** The
  protocol bounded a payload at 1,024 bytes; it now bounds it only as non-empty,
  with the framing layer's 16 MiB `MAX_MESSAGE_SIZE` as the effective ceiling.
  `decdn_protocol::CHUNK_SIZE` and `MessageValidationError::ChunkTooLarge` are
  removed. A serving node coalesces to `payment.frame_target_bytes` (new, default
  1 MiB) instead of chopping at 1 KiB, so a served MiB costs about one frame
  rather than 1,024 — the per-frame validate/copy/encode/write cost per byte
  served drops by the same factor. The 1,024-byte value was vestigial: it tracked
  iroh-blobs' internal granularity, which ADR 038 superseded with 16 KiB bao chunk
  groups verified independently of frame boundaries.
  - Wire-breaking in the new-sender-to-old-receiver direction: an old receiver
    rejects an oversized frame. Both sides change here, per the pre-launch policy.
  - A frame never crosses a `CHUNK_BYTES` payment boundary. The old 1 KiB size got
    that property for free (1,024 divides 1 MiB); at any other size it has to be
    arranged, or the payer settles residuals with a signed voucher per frame
    instead of releasing one hash-chain preimage per interval.
  - New config key `payment.frame_target_bytes` (restart-required, like the rest
    of `[payment]`), valid in `1..=1048576`. Node-local and never negotiated:
    nothing on the wire carries it, and the serve loop clamps each frame both to
    the payment-chunk boundary and to the credit window's remaining room. One
    payment chunk is the ceiling because a frame never crosses a boundary; a
    larger value is rejected rather than silently ignored.

- **Container image renamed to `decdn-node`, and now published to Docker Hub as
  well as GHCR.** `ghcr.io/decdn/decdn` becomes `ghcr.io/decdn/decdn-node`, and
  the same image is published as `decdn/decdn-node` on Docker Hub. The image
  ships the daemon only — the `decdn` CLI is deliberately not in it — so it is
  no longer named after the repository. No release had been cut under the old
  name, so nothing existing breaks; any local script pinning
  `ghcr.io/decdn/decdn` must be updated.
  - The mirror is a manifest copy, not a second build: `sign-release.sh` uses
    `docker buildx imagetools create`, which copies the manifest bytes verbatim,
    so both registries serve one identical digest and the existing GPG signature
    over `image-digest.txt` covers both. The script re-reads every tag on both
    registries and refuses to publish the release if any resolves to a different
    digest. `image-digest.txt` still records a single GHCR reference.
  - New env overrides: `DECDN_SKIP_DOCKERHUB=1` (tag on GHCR only) and
    `DECDN_DOCKERHUB_REPO`. No new Actions secret — the mirror happens on the
    maintainer's machine at signing time, keeping "no publish credential in CI".
- **`TERMS.md` and `TERMS_README.md` moved from the repo root to
  `crates/cli/`.** The terms text is embedded by `decdn-cli` with
  `include_str!`, which cannot reach outside its own package; from the root it
  was unreachable in a published `.crate` and `cargo install decdn-cli` would
  have failed to build. The file's bytes are unchanged, so the on-chain
  `currentTermsHash` preimage and the hash lock in `terms.rs` are unaffected.
  The out-of-band verification command becomes
  `cast keccak 0x$(xxd -p -c1000000 crates/cli/TERMS.md)`.

- **ABI-breaking: `OriginAssignment` replaces propose/ratify with publisher
  vetting plus instant origin seating (#1491).** Origin authorization used to
  couple two decisions in one flow — a publisher proposed a whole operator set
  and governance ratified it after a timelock — so every routine add needed a
  governance vote and re-validated the already-serving origins. Governance now
  vets the publisher **wallet** once; the vetted publisher then seats and unseats
  origins for its own namespaces itself, one operator at a time, effective in the
  transaction that carries them.
  - **Removed, with no replacement:** `proposeAssignment`,
    `activateAssignment`, `cancelAssignmentProposal`, `getPendingAssignment`,
    and the events `AssignmentProposed` / `AssignmentProposalCancelled` /
    `AssignmentActivated`. Seating is still evented — `OriginAdded` replaces it,
    as a per-operator delta rather than a whole-set activation.
  - **Added:** `requestVetting()`, `cancelVettingRequest()`,
    `grantVetting(address)`, `setPublisherVetted(address,bool)`,
    `isVettedPublisher(address)`, `getPendingVetting(address)`, and
    `addOrigin(uint256,address)` — the single "origin seated" path. New events:
    `OriginAdded`, `VettingRequested`, `VettingRequestCancelled` (the publisher
    withdrawing its own request), and `PublisherVetted` — which also closes any
    pending request, since `grantVetting` and `setPublisherVetted` both clear one
    while fulfilling or revoking it.
  - **Renamed** (an indexer migrating off the old topics should map these rather
    than treat them as removed): `revokeAssignment` → `removeOrigin`,
    `pruneBlacklistedAssignment` → `pruneBlacklistedOrigin`,
    `assignmentTimelock` / `setAssignmentTimelock` → `vettingTimelock` /
    `setVettingTimelock` (same 24h–14d bounds, same 3-day default — it now delays
    vetting, not any assignment). Events follow: `AssignmentRevoked` →
    `OriginRemoved`, `BlacklistedAssignmentPruned` → `BlacklistedOriginPruned`,
    `AssignmentTimelockUpdated` → `VettingTimelockUpdated`.
  - `IPublisherRegistryOwnership` gains `namespaceCount(address)`, which
    `requestVetting` reads to reject a caller that owns no namespace.
  - The node's origin-directory watcher follows the renamed events; its read
    semantics (authoritative `getOrigins` re-read, fail-closed retry, no replay)
    are unchanged. ADR 011 § Origin Assignment Authority and ADR 016 are rewritten
    to match; the ADR 009 governable-parameter row is now "Publisher vetting
    timelock".
- **CLI-breaking: `decdn publish assign` is instant and per-operator, and gains
  two sibling subcommands (#1491).**
  - `publish assign <ns> <op…>` now sends one `addOrigin` per operator instead of
    a single `proposeAssignment`, in the order given, stopping at the first
    failure. Its receipt changes accordingly: the `ready_at` and `replaced_prior`
    fields are gone, `status` is `seated` / `partial` / `failed` / `unknown` /
    `dry_run` instead of `proposed_pending_dao`, and the JSON gains an
    `origins: [{operator, tx, state}]` array listing **every** requested operator
    with what happened to it (`seated`, `reverted`, `not_attempted`, or
    `in_flight`). A run that stops part-way prints its receipt before the error,
    so the operator can see which seats landed; `in_flight` marks a transaction
    that was broadcast without a readable outcome and must be checked rather than
    blindly re-sent.
  - **New `publish request-vetting`** — the one governance-gated step; prints the
    `ready_at` the vetting timelock elapses.
  - **New `publish revoke <ns> <operator>`** — unseats one authorized origin.
  - `request-vetting` and `revoke` report the same status vocabulary as `assign`
    where it applies: `dry_run`, `failed` (nothing reached the chain), `unknown`
    (broadcast, but the receipt could not be read — check the printed tx before
    retrying), and the per-command success label.
- **Config-breaking and CLI-breaking: `blockchain.buyer_deposit_micro_usdc` is
  split into two knobs (#1497).** The buyer path now opens a channel small and
  graduates it, so the single deposit knob becomes a pair. There is **no alias**:
  `[blockchain]` is `deny_unknown_fields`, so any existing config file carrying
  the old key fails at load with an unknown-field error until it is migrated.
  - `blockchain.buyer_initial_deposit_micro_usdc` — the first-contact
    `openChannel` lock. **Default 0.5 USDC (`500_000`), down from the old key's
    10 USDC**: a channel now escrows 20× less at open, so an unproven provider
    holds correspondingly less of the buyer's capital. Validated `> 0`.
  - `blockchain.buyer_working_deposit_micro_usdc` — the target every `topUp`
    refills toward. Default 10 USDC (`10_000_000`), matching the old key's
    default. `0` disables top-up entirely; any other value must be
    `>= buyer_initial_deposit_micro_usdc`.
  - The `decdn fetch` flag `--deposit-micro-usdc` is likewise **renamed with no
    alias** into `--initial-deposit-micro-usdc` / `--working-deposit-micro-usdc`;
    the old spelling is now rejected as an unexpected argument.
  - Operator impact beyond the rename: because channels open at 0.5 USDC, a
    *first-contact* pull is bounded by what that deposit buys (~500 MB at ADR
    003's ceiling rate) until the channel graduates. `decdn fetch` recovers
    in-flight by topping up mid-transfer and resuming at the paid frontier; the
    daemon's node-to-node pull and `decdn bundle pull` graduate only on reuse,
    so an oversized *first* pull on those paths can now fail where a 10 USDC
    open previously succeeded. Raise `buyer_initial_deposit_micro_usdc` if that
    matters for a given deployment. See
    [ADR 003 § Deposit Economics](adr/003-payments.md#deposit-economics).

- **Deploy-script-breaking: `DeployConfig` and `BuybackActivation` changed shape
  (#1090, #1175).** Both are `BaseProtocolDeploy` structs, so only out-of-tree
  callers that construct them by hand are affected — no contract ABI, config
  file, or CLI surface changes, and a `DeployProtocol` run with an unchanged
  environment produces the same deploy it did before. The written manifest does
  change shape: `deployments/<chainId>.json` gains an `externalDeps.bootstrapMultisig`
  key, which matters to anything parsing it.
  - `DeployConfig` gains `bootstrapMultisig` (read from the new optional
    `BOOTSTRAP_MULTISIG` env var). Unset/zero keeps today's behaviour exactly:
    `DecdnGovernor` is seated as the `TimelockController`'s proposer at deploy,
    so DAO voting is live immediately.
  - `BuybackActivation`'s 17 flat fields regroup into the venue-independent `guard`,
    the venue-derived `seed`, and the venue-scoped `uni` / `bal`. The flat layout let a
    Balancer field be set under `venue == UNISWAP` and silently dropped;
    `_activateBuyback` now rejects both halves of that mistake —
    `VenueFieldsCrossWired` for a field on the unselected venue, `VenueFieldsUnwired`
    for a required field missing on the selected one (a Balancer activation without
    its Vault otherwise ships a burner that can never swap while already receiving
    30% of protocol revenue). `PoolSeed` additionally records the venue it was
    derived for, since the 80/20-vs-1:1 seed weighting means a mismatched pair
    mis-anchors the genesis pool silently (`PoolSeedVenueMismatch`).
  - `BalancerVenueParams` nests `BuybackVenueLib.BalancerWiring` rather than
    restating five of its fields, and the guard band is
    `GuardedBuybackBurner.GuardParams` rather than a fourth copy of the same shape.

- **Monitoring-breaking: `monitoring/` no longer ships rules and panels that
  could never fire (#1513).** Eleven `decdn_*` series referenced by the
  reference alerts and dashboard were never exported by any node. Nothing in CI
  compared the two, so each shipped as coverage while being permanently silent.
  Both files now reference only live series, enforced by a new
  `monitoring_selectors_are_exported` test.
  - **Renamed:** `decdn_peer_table_size` → `decdn_gossip_peer_table_size`
    (exporter field `gossip_peer_table_size`). This revives
    `DecdnPeerTableThin`, which has never been able to fire, and fixes the
    "Peer table size" dashboard panel. **Any forked dashboard or alert file
    needs the same edit.**
  - **Alerts deleted:** `DecdnBlacklistSyncLagWarning`,
    `DecdnBlacklistSyncLagCritical`, `DecdnBlacklistVersionBehind`,
    `DecdnBlacklistVersionFarBehind` (on `decdn_blacklist_sync_lag_seconds` /
    `decdn_blacklist_version_behind`, neither emitted — the blacklist watcher is
    unimplemented; `DecdnBlacklistWatcherStalled` is the real coverage), and
    `DecdnHashMismatchAppearing` (on
    `decdn_streams_failed_total{reason="hash_mismatch"}`). Delivery hash
    mismatch is genuinely unmetered — a real coverage gap, now recorded in
    `docs/runbook.md` rather than papered over with a rule that cannot fire.
  - **Alert replaced:** `DecdnHighStreamErrorRate` →
    `DecdnServeInternalErrorRate`. The old rule divided
    `decdn_streams_failed_total` by `decdn_streams_{failed,completed}_total`;
    none exist. There is no serve-attempt counter to build a ratio from, so the
    replacement is an absolute rate on
    `decdn_serve_stream_rejected_internal_error_total` — the one refusal reason
    that means the node itself is at fault rather than the client.
  - **Panels changed:** "Blacklist sync lag" → "Blacklist watcher tick age";
    "Throughput (served vs received)" now reads
    `decdn_cache_bytes_returned_total` / `decdn_cache_pull_through_bytes_total`
    (whose ratio is the origin-egress amplification factor) instead of
    `decdn_bytes_{served,received}_total`; "Probe responses by result" and the
    p50/p95/p99 "Probe fan-out latency" panel are replaced by one probe
    request-rate + probe-cache hit/miss panel (the exporter registers no
    histograms at all, so no quantile panel is buildable today); "Stream
    failures by reason" is dropped as redundant with the real serve-refusal
    panel, which gains an `internal error` series; the payments panel drops its
    settled-channel and vouchers-signed series for
    `decdn_voucher_nonce_gaps_total`.

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
  unchanged. `blockchain.buyer_deposit_micro_usdc` kept its name, its 10 USDC
  default, and its `> 0` validation at the time of this change; buyer paths
  simply no longer read the contract to clamp up to a floor. (That knob was
  subsequently split into `buyer_initial_deposit_micro_usdc` /
  `buyer_working_deposit_micro_usdc` — see the two-tier deposit entry above.)
  The internal `MIN_DEPOSIT_FLOOR` constant
  bounded two different things — `setMinDeposit`'s own lower bound and the
  delivery-*rate* floor. With the former gone it is renamed `MIN_RATE_FLOOR` to
  match what it still does; same value, two remaining call sites (the constructor
  and `setRateBounds`), no behaviour change.
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

- **Tracing: dependency spans no longer export, and each run of a streaming
  miss has an `upstream_stream` span (#2048).** The OTLP export filter
  admitted any span at `WARN` from a non-deCDN crate. iroh opens its periodic
  network-report spans (`QADv4`, `QADv6`, `reportgen-actor`, `run-probe`,
  `captive-portal`) at `WARN`, so they made up about 95% of the spans in
  Tempo. The filter now exports no span from another crate at any level. A
  dependency's `WARN` and `ERROR` events still land on the deCDN span around
  them, as [ADR appendix-observability](adr/appendix-observability.md#trace-spans)
  specifies. The ranged pull leg now opens an `upstream_stream` span for each
  run it pulls from a peer, with the same `peer`, `local_node_id`, `hash` and
  `pool_id` fields as the buffered path. Its `outcome` is `filled`,
  `reassigned`, `terminal` or `cancelled`. A streaming miss from a peer now
  nests as `serve_stream` → `serve_miss_pull` → `upstream_stream` →
  `open_progressive_pull`; a first leg adopted from the header handshake
  stays under `serve_stream`.
- **Runtime: every JSON log event carries `node_id` (#2050).** ADR
  appendix-observability lists `node_id` as a mandatory log field, but only
  the identity line and the startup banner carried it, so a log line in the
  aggregator could not be joined to the NodeId that peers and the chain see.
  The JSON formatter now writes `node_id` (lowercase hex) as a top-level key
  beside `timestamp`, `level`, `target` and `fields`. The daemon loads its
  node key before the tracing subscriber starts, so the first event of the
  process already has it. The start order is now: OTLP exporter, node key,
  subscriber, RPC preflight. So a key-load error prints to stderr as a plain
  `Error:` line, like a config error, and is not a JSON event or an OTLP
  span. A first start writes `node.secret` even when the RPC is unreachable.
  The `pretty` format is unchanged. ADR appendix-observability now names the
  JSON keys as written (`timestamp`, not `ts`) and records which OTLP
  resource attributes the node sets and which the collector adds.
- **Client: a fault before a range's first leaf no longer marks the rest of
  the blob present.** A leg that faults after its bao parents and before its
  first leaf checkpoints the parents with an empty received prefix. The
  checkpoint treated that zero length as "to the end of the blob" and added
  every byte from the leg's start to the end of the blob to the present set,
  with no data behind it. A later drive then skipped those bytes, the
  `.ranges` record kept them, and the local verify at finalize failed. An
  empty prefix now adds nothing.
- **CLI: a multi-source fetch whose lanes all crawl now stalls as a whole
  (#2123).** The drive-level throughput floor (`--min-throughput-bps` over
  `--stall-timeout-ms`) covered single-source and range-dedup drives only. The
  multi-source scheduler's per-unit watchdog judges one lane's unit at a time,
  so a fetch where every lane moved a little inside `--unit-deadline-ms` never
  tripped anything. The floor now also runs over a multi-source fetch, measured
  on its position across every lane. A trip falls back to single-source
  failover, which resumes the `.partial`. The clock stops while the fetch
  waits on its own top-up, and the present record is flushed before the
  fallback. At `-v` the fetch logs its progress, rate and ETA every 10 s. The
  SDK adds `multi_source_fetch_until` and `Downloader::fetch_to_paths_until`,
  which take the same `stop` future as `drive_range_set`, and their progress
  callbacks take a borrowed trait object.
- **Node: serve-loop payment faults are metered by cause (#2134, #2135).** The
  miss serve loop counted every proof error and every chunk-write error as a
  client abandon, including a lane-store `record` failure, a broken paid-frontier
  invariant, and a write to a stream the node had already closed. Those are
  node faults, which `decdn_serve_stream_node_fault_total` already meters, so
  `decdn_node_pull_through_client_abandoned_total` overstated client abandons.
  The miss serve loop now counts an abandon only when the peer caused the error,
  in both its chunk-write and its proof-recoup phase. A payer that sends
  `MAX_PROOFS_PER_CHUNK` proofs for one chunk without settling it now bumps
  the new counter `decdn_serve_stream_proof_budget_exhausted_total` on both the
  cache-hit and the miss serve loop. Before, the cache-hit exit logged only at
  `debug!` and bumped no counter. The delivery dashboard shows the counter
  beside the other mid-stream stops, and both "Unattributed stream failures"
  panels subtract it.
- **Node: a proof that pays part of a chunk no longer wedges the runtime
  (#2132).** A proof credits at most the lane headroom it finds, so a sealed
  voucher that moves the watermark by a sliver pays a sliver of the chunk. Both
  serve loops added that sliver to `paid` but dropped the whole chunk from the
  queue of owed chunks. The unpaid rest was then tracked nowhere. On a cache
  hit, once it filled the credit window, the serve loop went around forever
  without awaiting, and held one tokio worker per stream. Two such streams
  stopped a 2-vCPU node: serving, watchers, logs, and `/metrics` all went
  silent, while systemd still showed the unit `active`. Any paying client could
  trigger it. A partly paid chunk now stays owed until proofs pay all of it.
  `MAX_PROOFS_PER_CHUNK` counts every proof that leaves the chunk unsettled, so a
  payer that sends sliver after sliver hits the bound. The chunk's delivered
  length, not its unpaid rest, still decides whether a metering voucher may pay
  it. Both serve loops now check after each recoup that `delivered − paid`
  equals the bytes not yet cut into a chunk. When an iteration neither delivers
  nor credits a byte, they end the stream as a node fault, logged at `error!`.
  The miss path used to file that state as a client abandon, so
  `ServeStop::ClientAbandoned` is gone.
- **CLI: a range-dedup bundle entry pays its complement over one warm session,
  concurrently (#2119).** An entry that shares most of its chunks with a sibling
  pays for about one 16 KiB chunk group at each seam its donors cannot cover —
  about 1,300 ranges for a 13.8 GB file. `bundle pull` paid them one at a time,
  each on a new QUIC connection with its own handshake, so the complement took
  close to an hour. The entry now opens one session per provider and reuses it
  for every drive (the complement, a donor re-fetch, each deferred fallback, the
  self-heal re-drive). The session keeps one connection open, redials it if it
  closes, and fills the merged ranges up to `--max-lane-streams` at a time
  through `decdn_client_pull::drive_range_set`. Concurrent ranges on one deposit
  top up one at a time: `SharedPool` gains a `topup_lock` (the run's is
  `LaneLedgers::topup_lock`), and a range that waited on it re-reads the deposit
  and draws again instead of escrowing a second shortfall. Without
  `--max-approve` the second `topUp` used to revert on the allowance the first
  one spent.
- **CLI: the fetch prelude's first open is the drive's first leg, not a
  throwaway (#2063).** `decdn fetch` and `bundle pull` opened every entry once
  to read its size, dropped that pull, and opened the same range again. The
  node treats each open as real, so every cold entry cost a duplicate origin
  draw and a delayed first byte. The prelude now opens exactly the range the
  drive opens first (`decdn_client_pull::first_leg`) and parks it in a
  `PrimedSource`, which hands it to that open. A range-dedup entry, whose size
  the manifest gives, opens a bounded first leg instead of the whole blob. On a
  whole-blob resume the size must be read before the store can say what is
  missing, so that open is still dropped, and so is a multi-source fetch's size
  probe.
- **CLI: a drive whose short legs each clear the floor still stalls as a whole
  (#2120).** The throughput floor (`--min-throughput-bps` over
  `--stall-timeout-ms`, by default 4096 B/s over 30 s) judged one stream at a
  time, and each short range leg is briefly healthy, so an entry could crawl
  for 20 minutes with no stall and no failover. Every single-source and
  range-dedup drive now also runs under the same floor, measured on the drive's
  own progress across all of its legs and the waits between them. The clock
  stops while the drive waits on its own top-ups, and it races only the gap
  work (`drive_range_set`'s new `stop`), so the local check of a complete blob
  is never cut off and the present record is flushed before the drive returns.
  A drive that averages below the floor fails over like a stall. The
  multi-source path keeps its per-unit watchdog. At `-v`, a drive logs its
  progress, rate and ETA every 10 s, and each bundle entry logs its size, time
  and rate when it lands.
- **CLI: failed bundle entries are retried, and a provider list that runs out
  says so (#2118).** Each entry walked its candidates once, the last failure
  logged no warning, and a provider that failed once never served the entry
  again, so one transient fault could lose a large entry for the whole run. The
  new `--entry-retries N` (default 2) gives each entry that failed in a way
  another round can fix up to N more rounds after the first pass, backing off
  2 s, then 4 s, doubling to a 30 s cap. Each round probes again and resumes
  from the entry's `.partial`, so no byte is paid for twice, and a group that
  runs again is not "finished" for its deferring siblings until it finishes
  again, so they wait for its shared chunks rather than paying for them too. A
  spent pool, a size that disagrees with the manifest and a local disk fault
  never start a round. A pool exhaustion now fails over on the
  range-dedup path as it already did on the whole-file path, the last failover
  logs a warning, and the error of an entry that ran out of providers names how
  many it tried. A retried entry's bar does not count its landed prefix into the
  total bar twice. A range-dedup entry with nothing to pay for up front now
  creates a real ranged store before it splices, so a later drive no longer
  truncates the spliced bytes and pays for the whole blob again, and
  `ClientRangedStore::open_or_create` treats a failed stat of the `.ranges`
  record as an error rather than as "no record", which would truncate the
  `.partial`.
- **CLI: an unreadable data dir no longer passes as a client's (#2086).**
  `decdn_common::data_dir::daemon_marker` used `Path::exists`, which reads every
  stat error as "absent", so a node data dir whose store files could not be
  stat'd (a permission denial, a loop, a transient I/O fault) was classified as a
  client's, and the guards that refuse to escrow, sweep or buy into a node's dir
  let the command through. It now returns `io::Result` and treats only
  `NotFound` as absent; the guards, `ChainAdoption::for_buy` and `pool list`
  refuse or report the owner as unknown instead.
- **Node: a cold multi-GB cache miss streams at bandwidth, not one round trip
  per chunk (#2061).** The two-leg serve-miss spine re-read the whole
  `{H}.obao4` outboard (blob / 256 bytes) from the origin on every draw, and
  opened a new upstream request for each small piece of room a voucher
  released — often one chunk. A proxy-warmed bundle pull fell from 40–60 MiB/s
  to 1–8 MiB/s once it reached content only one node could fill.
  - The cache engine keeps each outboard, tagged with the origin that served
    it, in a 256 MiB memory cache. The serviceability probe and every draw read
    it from there. Each draw takes the outboard and the data from the same
    origin, and a verify fault evicts the copy the draw used. Concurrent cold
    misses of one hash wait for one origin read. An outboard over 256 MiB (a
    blob over 64 GiB) is read on each draw.
  - Once the ramped window reaches four pull-window floors, the pull leg waits
    until half of it is open before it draws, except for a serve-demand draw or
    the end of a gap. The exposure bound is unchanged. The new counter
    `decdn_node_pull_through_min_draw_waits_total` meters these waits apart
    from `decdn_node_pull_through_window_paused_total`.
  - Each origin read on this path (an outboard fetch or one data window of at
    most 4 MiB) has a time budget of `cache.node_pull_stall_window_sec` plus
    the read size at `cache.node_pull_min_throughput_bps`. A read past it fails
    as an origin fault and bumps `decdn_cache_origin_range_timeouts_total`.
    `node_pull_min_throughput_bps = 0` leaves these reads unbounded.
  - `decdn_cache_pull_through_bytes_total` counts each outboard the origin
    serves once, including wrong-length copies.

- **CLI: `decdn fetch` and `decdn bundle pull` adopt the pool the wallet
  already owns on chain, instead of opening a second one.** The client decided
  whether it owned a pool from its local store alone, so a lost row read as "no
  pool": it escrowed a fresh `buyer_working_deposit_micro_usdc` beside the live
  one. On a wallet whose USDC was already escrowed in that pool the second
  `openPool` reverted outright (`ERC20: transfer amount exceeds balance`, graded
  `insufficient_deposit`). The `_v4` buyer-table move in #2087 made this the
  common case — every client row written before it reads as absent — but any
  store loss reached it: a reset data dir, a new machine. With no usable row the
  client now asks the chain (`getPools`) and adopts the newest `Open` pool that
  still has deposit to spend, recording it; it opens a pool only when none
  qualifies, and a chain read that fails aborts rather than opening one. A lane
  with no local record — every lane of an adopted pool, and a tracked pool's
  first contact with a provider — resumes from the chain watermark
  (`getWatermark`) rather than zero, where every voucher at or below it redeems
  nothing; that is one extra read per new lane, and a fault on it now aborts the
  fetch. An adopted pool's refill decision counts what the pool has already paid
  out (`totalRedeemed`), so a pool other lanes drained is topped up rather than
  trusted as full. A buy whose store or key belongs to a node never adopts —
  a node's dir named with `--data-dir` (#2082), or a client dir pointed at the
  node's keystore with `--keystore`/`blockchain.eth_keystore`: the wallet is the
  operator's, so its live pool is the daemon's own, and adopting it would put a
  second voucher series on the daemon's lanes. Adoption assumes one buyer per
  wallet, and a key copied out of a node's dir, or shared between two clients,
  is not detectable locally.

- **CLI: `decdn whoami` reports a keystore the shared password does not open
  (#2008).** The command resolved one password and applied it to both
  `<data_dir>/keystore.json` and `<data_dir>/client/keystore.json`. `key-gen`
  writes those at different times and prompts for each, so they can hold
  different passwords — and when they did, the first `Mac Mismatch` aborted the
  whole command. Nothing printed: not the node id, not the key paths, not the
  address of the keystore whose password was correct. The shared password is
  still tried first; a keystore it does not open now degrades its own line to a
  note naming the keystore and the reason, and on a TTY that keystore gets one
  prompt of its own before the note. Every other line still prints and the
  command exits 0, which is what its documented ordering already promised for
  the node id and the paths.

- **CLI/node: `decdn doctor` checks the store files a daemon actually owns
  (#2083).** `doctor` kept a private copy of the daemon's store-file list and
  it had drifted: it reported on a `floor-loss.redb` that nothing creates, and
  a fifth daemon store would have been checked by nothing. The list now comes
  from `decdn_common::data_dir::DAEMON_STORE_FILES` plus `CLIENT_BUYER_DB_FILE`,
  the same names the daemon's own store and the client store use. A new
  `channel_store` test asserts that opening `PersistentPoolStateStore` creates
  every file in that set and no `.redb` outside it, which turns the premise
  `daemon_marker` — and with it the node-vs-client classification from #2078 —
  rests on into a checked fact.

- **Node: bootstrap distinguishes "this pool is not open" from "its status could
  not be read" (#2078).** The stale-row drop below judged a tracked pool by its
  absence from the set of pools read as `Open`, and a `getPool` call that faulted
  produced the same absence. One transient RPC error at bootstrap — there is no
  retry on that call — would therefore delete the node's only record of a funded
  pool, and the next miss would escrow a second deposit: the #2072 failure,
  self-inflicted, and invisible because the stranded-pool report cannot name a
  pool it failed to read either. A row is now dropped only on a successful read
  that returned a non-`Open` status. An id `getPools` does not list also counts
  as unknown: that view derives ids from `ownerPoolNonce` and `closePool` /
  `reclaim` only change a pool's status, so it is append-only and an absent id
  has no on-chain producer.

- **Node: a buyer-pool row the chain no longer lists as open wedged the buy leg
  (#2078).** `reuse_or_report` reads the tracked row without a status check, and
  bootstrap answered `AlreadyTracked` for any row at all, so a pool closed or
  reclaimed out of band — which is what `decdn pool close --pool` from a node
  host leaves behind, since it cannot write the daemon's store — pinned every
  later pull to a pool `redeemMany` rejects every voucher against. Restarting
  did not help. Bootstrap now checks the tracked pool against the chain's open
  set, drops a stale row, and adopts or opens a replacement.

- **Node: a node with an intact buyer store never reported its stranded pools
  (#2078).** `reconcile_owned_pool` returned as soon as the store named a tracked
  pool, before it enumerated `getPools` — so `report_stranded_pools` was reachable
  only on the adoption path. The steady state (store intact, a deposit stranded by
  an earlier build) was therefore permanently silent, and only a direct `getPools`
  read surfaced it. The enumeration now runs on every bootstrap, and the stranded
  set is measured against the pool the *store* names rather than the one an
  adoption would have selected. A fully-redeemed `Open` pool is no longer listed:
  `reclaim` refunds `deposit - totalRedeemed`, so there is nothing to recover.
  `decdn_buyer_pool_adoption_failures_total` is deliberately **not** widened — an
  enumeration failure on the already-tracked path warns instead, because the
  counter means "about to open a second pool" and that path never is.

- **Node: the pre-redeem watermark reconciliation never decoded on a chain
  without Multicall3 (#2076).** The seller's last gas check before a `redeemMany`
  batched its per-lane `getWatermark` reads through Multicall3 at
  `0xcA11bde05977b3631167028862bE2a173976CA11` — a contract the protocol neither
  deploys nor verifies. Where it is absent, including every `anvil` chain, the
  `eth_call` returned empty, alloy decoded the empty buffer as `ABI decoding
  failed: buffer overrun`, and the fail-open arm submitted the plans unchanged on
  every sweep. It stayed silent because the contract's own `claimed <= w.amount`
  guard makes an already-redeemed lane a harmless no-op, so the only symptom was
  wasted gas. The read is now `PaymentPool.getWatermarks(bytes32[], address[],
  address[])`, the batch companion to `getWatermark`: one `eth_call` per batch of
  at most 512 lanes, against the protocol's own deployment. A failed or
  short-returning batch keeps the prefix already read instead of discarding the
  whole result, and a return whose length does not match the batch fails that
  batch rather than pairing values it cannot trust. Each batch now meters itself
  as `decdn_redemption_reconcile_ok_total` /
  `decdn_redemption_reconcile_failures_total`, with a `DecdnWatermarkReconcileFailing`
  alert — a zero `decdn_redemption_reconciled_skip_total` alone cannot separate a
  healthy sweep from a read that never landed, which is what let this hide. This
  is an **additive contract-surface change** — one new view selector on
  `PaymentPool`; no storage layout, event or write path changes.

- **Node: a reset buyer-pool store stranded the node's deposit and wedged every
  node-to-node pull (#2072).** The buyer store was the only record that the node
  owned a `PaymentPool` deposit, so a moved or re-provisioned `identity.data_dir`
  lost it. The node then opened a second deposit beside the first, forgot that one
  too, and once the wallet was drained every cache-miss pull failed
  `ERC20: transfer amount exceeds balance` with its own escrow sitting idle
  on-chain — contradicting ADR 003 §node→node ("a pool is opened once and reused",
  "owner funds are never stranded"). `BuyerPoolService::bootstrap` now reconciles
  against `PaymentPool.getPools` and adopts the newest `Open` pool the owner
  already holds; adoption failure is soft, leaving the first miss to open one the
  old way. A lane with no local progress reseeds from the contract's
  `watermark(pool, signer, provider)`, because `PoolLedger` signs `prior + accrued`
  and resuming a paid lane from zero signs cumulatives the contract pays nothing
  for. That read **refuses the pull** on failure rather than degrading: the pull
  persists its own progress on every exit path, so a zero-resumed lane gains a
  local row and the reseed never runs for that provider again — one RPC blip
  would strand the lane permanently. A fully-redeemed pool is skipped (it is
  still `Open` on chain, and adopting one would wedge buying against a deposit
  that funds no voucher), and any further open pools are named in the log so
  their deposits are recoverable. `enumerate_owned_pools` moved from `decdn-cli`
  to `decdn_incentive::payment_pool` so both binaries share one pager.

- **Node: one failed `openPool` moved `node_pull_pool_open_failures_total` twice,
  and blamed the wrong host (#2072).** `run_open`'s `openPool` leg metered the
  counter without marking the error `OpenReported`, so the `node_origin` classifier
  counted it again in its residual arm — the live fleet read 46 pool-open failures
  against 23 pull attempts. That fall-through also scored a chain revert as
  `node_pull_local_fault_total` and logged "suspect this node's store or lock
  state" for an on-chain failure. The leg now meters once where it is raised,
  marks `OpenReported`, and adds `LocalPullFault` for the failures that no other
  provider can answer. `openPool` names no provider, so neither an unfunded
  wallet nor a chain lane that cannot carry the transaction varies per candidate:
  both now refuse rather than walking the candidate list and then reporting the
  blob absent. A `ContractRevert` still walks on. The join-error leg meters too,
  which is what makes the "meters iff marks `OpenReported`" invariant true rather
  than merely documented. The ladder's arm choice is a pure
  `classify_pool_open_arm`, so the one-increment invariant is unit tested.

- **Incentive: an under-funded wallet was classified `contract_revert` (#2072).**
  `PoolOpenFailureReason` matched only `OpenZeppelin` v5 custom errors, so a USDC
  that reverts `Error("ERC20: transfer amount exceeds balance")` — the deployed
  Sepolia token — reported as an opaque on-chain fault naming no remedy. String
  reverts now classify as `insufficient_deposit` on their message.

- **Metrics: `decdn_pool_deposit_usdc` was hardcoded to zero (#2072).** Both
  callers of the seller-side lane snapshot passed `U256::ZERO`, because a lane
  carries no pool deposit — the gauge has read zero since #1667 while
  `adr/appendix-observability.md` documented it `live`, and the name gate only
  checks that a series is exported, not that anything sets it. The redeemer tick
  now publishes `Σ (deposit − totalRedeemed)` over the pools it plans lanes
  against, beside `decdn_unredeemed_usdc`; the lane count gets its own
  `set_lanes_open`. The publish is skipped when nothing is pending redemption —
  the healthy steady state — so the gauge does not sawtooth to zero after every
  successful sweep and read as insolvent counterparties.

- **Cache: origin range pulls held the whole requested span in memory — twice
  (#2065).** A few multi-GB range requests against a partially held blob OOM-killed
  the node. `CacheEngine::pull_through_range` now reads the `{H}.obao4` outboard once
  and fetches, verifies, and imports the span in `RANGE_PULL_WINDOW_BYTES` (4 MiB)
  windows, so a pull holds `O(window + outboard)` bytes whatever `byte_len` is. The
  own-origin serve-miss (Flow A) streams its wire the same way:
  `CacheEngine::origin_range_wire` runs one coherent encode over on-demand windows
  behind a bounded channel and ends a faulted wire with one terminal `Err` — a window
  that fails verification against `H` is still a hard `VerifyFailed`, and a panicked
  or cancelled encode is an `OriginError`, never a clean end. Each path runs at most
  `MAX_CONCURRENT_RANGE_PULLS` (4) origin range pulls at once, from separate pools so
  a long range pull never stalls a committed own-origin serve; a pull past the bound
  waits rather than degrading to a whole-blob pull. See the `Origin::fetch_range_data`
  entry under Changed (BREAKING).
- **`DecdnWatcherTaskPanicked` could never evaluate.** Its expr was
  `rate({__name__=~"decdn_.+_task_panicked_total"}[10m]) > 0`, and `rate()` drops
  `__name__`, so the five per-watcher series on a node collapsed to five identical
  label sets and Prometheus refused the vector outright
  (`vector cannot contain metrics with the same labelset`). Wrapping it in
  `sum by (instance)` does not help — the duplicate exists before the aggregation
  runs. The rule now lifts the watcher out of the metric name with `label_replace`
  while the raw selector still has it, and subtracts a 10-minute offset in place of
  `rate()` so nothing strips the name mid-expression; a counter reset reads negative,
  so a restart cannot fire it. The annotation reports `{{ $labels.watcher }}`, which
  a binary operation preserves, rather than `{{ $labels.__name__ }}`, which it does not.
  Nothing evaluated these rules before, so the breakage was invisible.

- **`DecdnCacheInflightMutexPoisoned` could never resolve.** The counter is
  monotonic and process-local and the annotation tells the operator not to restart,
  so a bare `> 0` latched for the life of the process. It now reads
  `increase(...[1h]) > 0`: a single occurrence still fires, and the window only
  decides when it clears.

- **`DecdnNodeRestarted` fired for five minutes after every deploy.**
  `decdn_node_uptime_seconds < 300` holds true for the whole window and a
  crash-looping node re-entered it continuously instead of escalating. Now
  `resets(decdn_node_uptime_seconds[15m]) > 0`, so one restart is one event.

- **A stale comment claimed six chain-event watchers.** The exporter has five —
  `slash`, `staker_set`, `blacklist`, `settlement`, `fee_shares`.

- **Cache: a held blob whose stored bytes change after admission is
  quarantined on the first serve that trips it (#1984).** Every serve export
  already validates the exported chunk groups against the content root, so a
  rotten or tampered group aborts the serve and no buyer pays for it. The node
  did not act on the mismatch: the hash stayed advertised, and every buyer
  tripped it again until an operator ran `decdn node evict`, which is a
  permanent takedown. Now a hash mismatch or short read over held content
  quarantines the hash in memory. The node stops serving, announcing, and
  re-acquiring it, answers stream requests with `EvictedSinceProbe`, and drops
  its protecting tags even when it is pinned. When GC reclaims the entry, the
  quarantine lifts and a pull-through re-admits a verified copy. New counter:
  `decdn_cache_held_corruption_quarantined_total`. Reclaim needs
  `cache.gc_interval_sec > 0`. `ServeAudit::Unavailable { evicted }` is renamed
  `withdrawn` (`is_evicted()` → `is_withdrawn()`) and covers both evict and
  quarantine.
- **CLI: `pool top-up`, `pool close`, `pool assign` and `fetch`'s auto-refill now
  exit non-zero when a landed on-chain effect cannot be recorded locally.**
  Four sites printed the failure to stderr and returned `Ok(())`. All shared one
  shape — the on-chain effect landed, the local write failed — and the money had
  already moved, so exiting 0 was the one outcome that guaranteed nobody
  reconciled it. A wrapper (`if decdn pool top-up …; then mark_funded; fi`)
  recorded the pool as funded while the local `deposit` stayed short by
  `credited`, which both re-triggered the low-water auto-top-up on every later
  fetch and made a retry of the command escrow again. After a `pool close` whose
  row-clear failed, `open_or_reuse_pool` reused a winding-down pool: later
  fetches signed vouchers that stop being redeemable at the dispute deadline,
  against a deposit the owner is about to reclaim.
  - This is a consistency fix, not a new policy. The reactive mid-fetch top-up
    already bails terminally on the same hazard, and the daemon already meters it
    as a top-up *failure* rather than a success. Only the proactive and manual
    legs had not adopted it.
  - `decdn pool assign` is in scope on different grounds: its middle arm had a
    `getPool` read that **succeeded** and proved the on-chain owner is not this
    keystore, yet still printed the `dcap1:` token, so
    `decdn pool assign … > delegate.token` exited 0 with a capability already
    proven dead at redemption. That arm now fails. The sibling arms — an
    unreachable RPC, a provider that would not build — keep degrading to a
    warning, because offline issuance is valid and a read that could not be
    performed proves nothing.
  - `pool close` fails on the `Err` only. `forget_if_pool` is compare-and-delete,
    and its `Ok(false)` means it found nothing to delete — no row, or a row for a
    newer pool — so nothing maps the owner to the closed pool and the close is
    clean. Closing a pool the local store never tracked (a second machine, a
    fresh `--data-dir`) lands there routinely. The `Err` error carries both the
    close tx and the `pool reclaim` deadline, and the reclaim is what clears the
    stale row.
  - Unchanged and deliberately so: `pool reclaim`'s row-clear and `fetch`'s
    `persist_watermark`. The reclaim stays a warning because the refund itself
    landed and re-running the reclaim clears the row; its warning now names that
    remedy. `persist_watermark` runs after delivery, so failing the fetch would
    neither un-pay the bytes nor do anything but misreport a fetch that
    succeeded; its warning now names the close-and-reopen remedy.
  - `client_pull::buyer_pool::top_up` now returns the `topUp` tx hash alongside
    the credited amount as a `ToppedUpPool`, so every one of these errors names
    the transaction an operator reconciles against — the handle the `open` path
    already had. The node's `fund_pool` returns the new deposit rather than a
    `DepositOutcome` and grades both credit-failure channels through the same
    helper, so its propagated error names the tx too and a store fault meters as
    `buyer_topup_failure` rather than slipping past unmetered.
  - The shared error now tells the operator not to re-run the command: an escrow
    that already landed escrows a second time on retry.
  - `pool assign` also distinguishes a pool that does not exist on the contract
    (`getPool` zero-fills an unknown key rather than reverting) from an ownership
    dispute, so a wrong `--pool` / `--payment-pool-address` / `--chain-id` is
    diagnosed as such instead of "sign with the owner keystore".
  - **Script impact:** these commands previously exited 0 in the degraded case.
    Wrappers that treated exit 0 as "recorded" must now handle a non-zero exit
    that means the on-chain effect landed and needs reconciling — not that it
    failed to happen.
- **config: resolve-time notices now reach the operator instead of raw stderr.**
  Every `*_into` resolver wrote its non-fatal warnings with `eprintln!`, on the
  documented grounds that no `tracing` subscriber exists at `resolve_config`
  time. That held for startup, but `resolve_security_into` also runs on the
  SIGHUP reload path, against a daemon whose subscriber has been live for hours.
  An operator on `log_format = "json"` who set `security.max_tracked_sources = 0`
  and reloaded got the "bookkeeping map is unbounded" warning on a stderr nobody
  was reading, and nothing in their log pipeline — the reload's own "section
  applied" event carried the new value but neither the severity nor the word to
  alert on. The resolvers now record `ConfigNotice`s on the shared
  `ConfigDiagnostics` bag (formerly `ConfigErrorBag`, renamed because it carries
  both channels), and the caller renders them once it knows which sink it has:
  `decdn-node` replays them through `tracing` right after `init_tracing`,
  the SIGHUP path emits them from `reload` once every gate that can abort the
  reload has passed, `decdn config validate` prints them under its field
  summary, and `decdn node doctor` turns them into findings. Nothing became a
  hard error — `0 = unbounded` stays a documented escape hatch and an inherited
  orchestrator env var must not refuse boot. Notices carry a severity: an
  unbounded bookkeeping map or a collapsed relay-failover list is `Warn`, which
  is what an operator alerts on and what `decdn node doctor --strict` gates its
  exit status on, while a deliberately disabled rate limit is `Info` and never
  reaches an exit status.
  - Covers all eleven former direct-emission sites — the retired-env-var
    notice, the well-known-port notice, the #843 `--relay-url`-overrides-the-list
    notice, the duplicate-`cache.origins` notice, and the unbounded-map notices
    across `security`, `dht.rate_limit` and `probe.rate_limit`. Each is now
    asserted by a test; previously the only thing a test could check was that
    the value resolved.
  - `resolve_config` returns `(ResolvedConfig, Vec<ConfigNotice>)`. Notices are
    dropped on the error path: a config that does not resolve is not the one the
    operator is running, so they do not surface until the config resolves.
  - Delivery on the daemon follows the log filter, where raw stderr did not: a
    node on `log_level = "error"`, or on a `RUST_LOG` naming other targets,
    receives no notices. Reach them without a log stream via `decdn node doctor`
    or `decdn config validate`, neither of which depends on a subscriber.

- **cache: an origin size probe that faults no longer drops the hash from the
  announce set.** `rescan_origins` resolved each candidate through
  `origin_size`, whose per-origin error handling is swallow-and-advance, so a
  transport fault arrived as `Ok(None)` — indistinguishable from an origin that
  genuinely does not hold the object. A throttled `HeadObject` window during a
  rescan therefore evicted those hashes from the origin-held index, and an
  origin-only node that hit one at boot advertised a fraction of its content for
  a whole rescan interval with every downstream signal green. The rescan now
  walks the origin chain through the same fault-aware primitive the serve gate
  uses, bypassing its memo so a large enumeration cannot evict the live path's
  warm entries. A faulted candidate keeps whatever the previous index held for
  it; one first seen inside the fault window has nothing to carry forward and is
  absent until a later rescan. Faults surface on
  `decdn_cache_origin_probe_failures_total` and reach every DHT seed path through
  `HolderSnapshot`, the origin half's counterpart to `store_error`. An HTTP
  origin still reports a server-side 5xx as a plain negative (the `Origin::size`
  contract), so that one case stays invisible; its transport faults, S3/R2 and
  the filesystem all count.
  - The index and its unresolved counts are one `ArcSwap` payload, published and
    read together, so a seed cannot pair a fault-truncated set with a later
    rescan's zero count and call it healthy. `origin_held_snapshot` is the only
    way to read it — `origin_held_hashes` is gone, because a caller that could
    take the set without the counts is how that pairing gets lost.
  - One rescan at a time, with any number of triggers arriving during a pass
    collapsing into a single rerun. Carrying an entry forward makes a rescan a
    read-modify-write spanning the whole walk, and both triggers fire detached,
    so overlapping passes let the slower one publish a payload derived from a
    pre-empted index — dropping whatever the fresher pass found. Excluding alone
    is not enough: queueing every trigger behind a lock piles up one waiter per
    tick for as long as a walk outruns the cadence, then runs that backlog of
    obsolete passes back to back against an origin already struggling.
  - Only a retry-eligible fault carries an entry forward. A permanent one — a
    revoked ACL, a symlink escape — reads the same on every rescan, so carrying
    it would advertise content the serve path refuses until an operator
    intervened.
  - Candidates dedupe on their own `seen` set. Keying off the index being built
    re-probed every hash that resolved absent, or faulted with nothing to carry
    forward, once per listing it appeared in — and counted a fault per probe.
  - Rescan probes are bounded by their own ceiling rather than
    `cache.origin_probe_timeout_ms`, which is sized to stop a slow origin
    stalling the probe *serve* path. Borrowing it would turn a merely slow origin
    into an all-fault rescan, and on a cold boot — where nothing can be carried
    forward — into a permanently empty announce set.
  - `decdn_cache_origin_enumerate_failures_total` covers the rescan's other leg.
    An origin whose listing fails contributes no candidates at all, so its hashes
    leave the announce set with no per-hash fault and nothing to carry forward:
    the same silent shrink, one step earlier.
  - The origin-only ownership probe each stored hash is put to counts its faults
    on `decdn_dht_republish_seed_origin_probe_failures_total`, the origin-side
    twin of the store-walk counter beside it. A `Fault` there is skipped rather
    than announced, which on a remote origin dropped a node's own store-only
    content on a transport blip while the snapshot read healthy. `HolderSnapshot`
    keeps those faults separate from the ones it inherits from the rescan: the
    seed's counter must not move on a node that asks the origin nothing, and the
    rescan's are already metered on the cache's own counter.
  - All three seed paths report a short set. The periodic rescan seed runs far
    more often than the boot seed or the lag sweep, so leaving it silent made the
    most common truncation the least visible one.

- **dht: a poisoned scheduler lock no longer wedges the republisher for the
  process lifetime.** Every `Mutex` site in `RepublishScheduler` swallowed
  poison — `len()` read `0`, `is_empty()` read `true`, `drain_due` returned
  empty, `unschedule` and the schedule path no-opped. `Mutex` poison is sticky,
  so one panic under a guard turned the scheduler into a black hole: nothing
  scheduled, nothing drained, and the node silently stopped advertising every
  hash it held while continuing to serve, probe and settle. All five sites, plus
  the two routing-table sites in the same file, now run through
  `chain_projection::with_lock`, a `Mutex` analogue of the existing
  `with_read`/`with_write` that recovers via `PoisonError::into_inner` and warns.
  Recovery is safe here: the invariant is "`scheduled` mirrors the heap", and
  both sides already tolerate the other's entry being absent, so a torn state
  costs one spurious or one skipped republish rather than corruption. All three
  helpers clear the poison as they recover, so the warning is one line per panic
  rather than one per acquisition — these locks sit on a 1 Hz timer and on the
  inbound `Store` path, where an ungated log is the flood `dispatch`'s own poison
  gate exists to prevent.

- **dht: a lag sweep is cancelled at shutdown instead of racing the cache
  flush.** The sweep is detached and holds a `CacheEngine` clone, and
  `run_republish` returned on its stop signal without touching it. A walk still
  in flight when the runtime flushed and closed the store saw the store go away,
  took the store-walk failure branch, and fired
  `decdn_dht_republish_seed_store_walk_failures_total` plus a degradation warning
  on an ordinary clean restart — an alertable counter on a non-event.
  `run_republish` now takes a `CancellationToken` the runtime owns and cancels
  before it flushes the store, and the sweep worker selects against that same
  token. The ordering is the point: a signal the republisher had to forward would
  reach the sweep only once that task was next polled, which is not ordered
  against the flush at all. The worker's `select!` is `biased` on that token, so
  a sweep polled any time after the flush takes the cancel arm instead of
  observing the store it was walking disappear. Nothing joins the worker, so it
  may outlive the flush by a poll — it just cannot report anything once
  cancelled. A queued pass is discarded along with it, which is correct here and
  is now stated rather than claimed away — see #1814 for the panic path, where it
  is not.

- **node: the capacity-bond registry resync now reports whether it is working.**
  `RegistrySink::on_tick_complete` is the only systematic repair for a drifted
  active-staker set, and it returns `Ok` on a failed read by design — an `Err`
  would mark the route errored and stall event pickup. That left a persistently
  failing repair moving no metric at all. It now bumps
  `decdn_capacity_bond_registry_resync_failures_total` on failure and stamps
  `decdn_capacity_bond_registry_last_resync_timestamp_seconds` on success. The
  gauge is what catches the repair being *skipped* rather than failing: the
  reconcile does not run at all while the route is errored, which emits nothing.
  It reads `0` until the first success, so its alert carries the `> 0` guard.

- **node: an active-staker set drifted by an RPC outage is repaired when the
  watcher recovers, not on the next cadence tick.** The re-enumeration was
  cadence-driven only, and the cadence slips in exactly the scenario that causes
  the drift: the reconcile is skipped while the route is errored, and a failed
  read stamps the clock and defers a further interval. `LogSink` gains
  `on_recovered`, a sync no-op-by-default seam the poller fires on the tick a
  route comes back from an errored one — before that tick's reconcile, so a sink
  that clears its cadence clock there re-reads immediately rather than a full
  interval later. All five sinks on the shared poller use it: the registry,
  slash, blacklist, fee-shares and rate-bounds sinks each gate their reconcile on
  a clock they stamp *before* the read, so all five deferred a further interval
  after exactly the outage that made them stale.
  - The forced re-read is floored at a fifteenth of each sink's own cadence. A
    recovery edge fires whenever a route comes back from an errored tick, so an
    endpoint that flaps rather than staying down would otherwise buy a full
    authoritative re-read per flap, aimed at an endpoint already failing.
  - The bootstrap enumeration stamps
    `decdn_capacity_bond_registry_last_resync_timestamp_seconds`. It is the same
    read against the same contract, and without it the staleness alert's `> 0`
    guard suppressed the alert forever on exactly the node it exists to find —
    one where no later resync ever succeeds. Its companion failure alert measures
    `increase(...[1h])`: attempts are one resync interval apart, so a window of
    that same length sees a zero-rate gap on any delay and never sustains.
  - `ErasedSink`'s method defaults are gone. It has exactly one implementation —
    the blanket forward — so a default there is unreachable, and its only effect
    was to turn a forgotten forward into a silent no-op that swallowed every real
    sink's implementation.
  - The recovery edge is armed by `fail_whole_tick` too, so a shared head-read
    failure — the shape a whole-RPC outage takes — arms it for every route.

- **dht: coalesced lag sweeps are countable.**
  `decdn_dht_republish_lag_sweeps_total` counts lag *events* rather than store
  walks, which is what makes a wedged sweep slot diagnosable, but left no way to
  tell a climbing counter caused by commits outrunning the scheduler from one
  caused by a long walk absorbing a burst. The sibling
  `decdn_dht_republish_lag_sweeps_coalesced_total` splits it into the lags that
  claimed an idle slot and the lags that did not. Neither it nor the difference
  counts walks — the slot holds one queued position, so any number of lags
  arriving behind a running worker collapse into a single further pass. The ratio
  is what reads: coalesced climbing at the lag rate with the difference flat is a
  slot that is never released.

- **dht: a lagged cache-commit channel now re-seeds the republish scheduler.**
  The republisher schedules a blob's first DHT `Store` off the cache's
  `subscribe_inserts` broadcast. That channel is bounded and retains nothing it
  drops, so a `Lagged` receiver cannot backfill — and the handler only logged,
  leaving every hash committed inside the lag window with no DHT record until an
  operator restarted the node. It now re-derives the held set (origin-held index
  plus, when relaying foreign namespaces, the committed store) and seeds whatever
  is missing, drawing `uniform(0, 40 min)` per record so the repair costs one
  cold-start window rather than a burst (ADR 022 §Bootstrap, AC 20). The sweep
  runs detached, and coalesces by re-running rather than skipping: each pass
  re-derives the held set once at its start, so a lag observed mid-walk earns a
  further pass instead of riding a snapshot taken before its own commits. The
  single-flight slot is released through the shared `PruneGuard`, so a panic in
  the store walk cannot strand it and silently disable every later sweep.
  - `RepublishScheduler` now tracks each hash's authoritative due time rather
    than bare membership, and `drain_due` discards a popped entry whose due time
    disagrees. A `BinaryHeap` cannot reschedule an interior entry, so every
    supersede left a tombstone that a later schedule resurrected into a spurious
    republish — reachable three ways: a re-seed racing the eager per-insert
    publish, a re-seed racing the tick path's drain-then-reschedule, and
    `unschedule` followed by a re-seed. `seed_cold_start` leaves an
    already-scheduled hash alone and returns the count *newly* scheduled;
    `schedule_cold_start` is removed, since nothing called it.
  - The due-time gate accepts origin-held content. `cache_still_holds` consulted
    only the iroh-blobs store, so a filesystem or pinned-origin hash that was
    never imported got advertised at probe time yet dropped at its first due
    time, publishing no `Store` at all — silently discarding what the cold-start
    seed has fed it since #1130.
  - An origin-only node now recovers its own store-only content. The origin-held
    index covers enumerable origins plus present pins, and a remote S3/R2/HTTP
    origin deliberately does not enumerate, so an unpinned object the node owns
    was in neither half of the snapshot. Stored hashes are now put to
    `origin_probe_presence` — the same question the origin-only serve gate asks —
    so the snapshot advertises exactly what the node would serve.
  - New counters `decdn_dht_republish_lag_sweeps_total`,
    `decdn_dht_republish_sweep_reseeded_total`, and
    `decdn_dht_republish_seed_store_walk_failures_total` make the window
    alertable. The last covers the boot-time cold start too, which shares one
    derivation with the sweep and previously degraded with no metric at all.
  - A closed cache-commit channel now stops the republisher instead of being
    logged and re-polled. `recv` on a closed channel resolves instantly and
    forever, so the old arm would have spun a core; the task's own `CacheEngine`
    clone keeps the sender alive, which is what made it unreachable.

- **node: the cache-miss serve leg now re-ramps its credit window.** `serve_leg`
  resolved the window once before its delivery loop and never recomputed it, so a
  stream served through a miss stayed pinned at the one-chunk ramp floor however
  much the client paid — strict stop-and-wait, one chunk per round trip, while the
  cache-hit path ramped toward `payment.credit_max` for the same client. It now
  recomputes per iteration exactly as the hit path does. Throughput only; the
  window bound itself was never exceeded.

#### Payments

- **`PoolFloorLossStore` enforces its monotonic contract, and a no-op write no
  longer fsyncs.** The trait doc stated that a pool's dead charge was monotonic
  "by caller discipline", but a floor-reservation drop reads its cumulative total
  under the pool-map lock and then persists it from an independent blocking task,
  so no caller can order its write against another's — and the redb store already
  clamped while the in-memory
  store overwrote, leaving tests written against the memory store unrepresentative
  of what ships. Both impls now raise the stored total and never lower it, with
  `forget_loss` the only downward transition. The redb store reads the row and
  aborts the transaction instead of committing when the total does not advance,
  so a late, smaller write costs no fsync — this table shares one file with the
  lane table, where an unconditional commit contended with the periodic voucher
  flush. Skipping the commit is sound only because every writer of that file uses
  `Durability::Immediate`, so the stored total is already durable.

- **A cooperative close now reconciles when the client's watermark lags the
  node's (#1495).** `decdn channel coop-close`
  refuses any provider tuple above what the client persisted — correctly, since
  without that guard a provider could ask the client to sign away up to the full
  deposit. But the guard had no reconciliation branch, so a client that signed
  vouchers it did not durably persist before an unclean exit could not
  cooperatively close at all. Nothing was lost — the `closeChannel` → dispute
  window → `settleChannel` fallback remains — but the one-transaction settle was
  unreachable, and the fallback submits a voucher *below* what the client
  actually signed, underpaying the provider unless it watches the window and
  disputes. The node now echoes the client's
  **own** last-accepted voucher signature alongside the waiver, and the client
  settles at the node's state only when that signature recovers to its own
  voucher-signing key over exactly the tuple being settled. Anything else keeps
  the refusal. This is the same self-heal primitive the fetch path already uses
  (`WatermarkBundle.last_signature`, #1481), and it strengthens rather than
  relaxes the check: only the client could have produced the signature.
  - **Wire-breaking (pre-launch).** The echo is a new `last_signature` field on
    `CooperativeCloseAuth` itself, not a trailing extension — the network is
    pre-launch, so there is no mixed-version population to preserve and the field
    is added directly rather than negotiated. It is empty when the node holds no
    stored signature for the channel, in which case the client keeps the
    pre-existing refusal.
  - `cooperative_close` gains a `reconciled: &mut Option<AuthorizedWatermark>`
    out-param carrying the healed watermark, written **before** the transaction
    is sent so every return path reports it — including the error ones, where a
    failed `get_receipt` leaves the settlement unknown and the chain may already
    have settled higher. Both callers persist it before inspecting the outcome,
    so the buyer store is advanced before the channel row is dropped and a
    `closeChannel` fallback submits the voucher actually signed.
    `CooperativeCloseOutcome` itself is unchanged (three fieldless variants).

#### Contracts

- **`GuardedBuybackBurner` no longer accepts a slippage tolerance that disables
  the MEV defense, or a buyback band that can never execute (#1532).**
  `slippageBps` was rejected only at `>= 100%`, so a value like 9999 scaled the
  TWAP floor to 0.01% of fair value — the entire ADR 018 MEV stack off, with no
  revert, no event, and a green deploy. It is now bounded at
  `SLIPPAGE_CEILING = 1000` (10%, against a 200 bps default) at both the
  constructor and `setSlippageTolerance`, mirroring the `[1%, 30%]` bound the
  neighbouring `epochLiquidityCapFraction` already carried at both sites.
  Separately, `maxBuybackAmount == 0` constructed cleanly and then reverted
  `AboveMaxBuyback` on every call forever — a burner accruing the FeeRouter's
  buyback bucket with no way to spend it — and governance could walk the band to
  `0/0` in two calls, since a zero ceiling is not an *inverted* band once the
  floor is also zero. Both entry points now revert `BuybackBandDead`. The
  deploy-time fail-fast `BuybackVenueLib.GuardBandDead` is retained so a mis-set
  env var still surfaces before the deploy transaction is broadcast. Both bounds
  are stated in the ADR 018 parameter table; the shipped defaults satisfy them,
  so no deployment path changes.

#### Cache

- **A poisoned coalescing mutex no longer silently costs origin egress and USDC
  (#1517).** `CacheEngine`'s in-flight fill-coalescing map is what stops N
  concurrent requests for one missing blob from opening N origin pulls. Three of
  its **six** lock sites discarded the `PoisonError` — `get`'s loop and
  `populate_inner`'s fell through to a *direct* pull, and `InflightGuard::drop`
  skipped its removal — while the three tee-path sites (`open_tee_sink`,
  `TeeReservation::drop`, `TeeSink::drop`) already recovered the guard with
  `PoisonError::into_inner`. Since nothing cleared the poison, and a
  `std::sync::Mutex` stays poisoned until something does, one panic permanently
  disabled coalescing: an unbounded
  egress multiplier on a metered `http`/`s3` origin, and on the `Peer` origin
  reached via `populate` a double-spend of USDC vouchers upstream — the exact
  hazard `TeeOpen::InFlight` is documented to prevent. Nothing logged it and
  nothing counted it; the only observable was
  `decdn_cache_pull_through_bytes_total` outrunning request volume.
  - All six sites now go through one `Inner::lock_inflight` choke point that
    recovers the guard, matching the crate's dominant idiom and the rationale
    already written for `evicted` / `is_evicted`. **Coalescing survives poison**,
    so the hazard is removed rather than merely reported. Having established that
    the critical sections contain no user code (so the map cannot be torn), it
    also calls `Mutex::clear_poison`, returning the mutex to a healthy state — no
    restart is needed, and the counter below counts *poisonings* rather than
    locks-taken-since-a-poisoning, so a `rate()` panel shows incidents instead of
    request volume.
  - The dropped removal in `InflightGuard::drop` was a second, unrecorded bug:
    `notify_waiters()` wakes only *current* waiters, so a leaked entry left every
    later request for that hash parked on a `Notify` that would never fire again
    — the permanent hang the guard exists to prevent.
  - **New metric:** `decdn_cache_inflight_mutex_poisoned_total`, paired with a
    single latched `tracing::error!`, an `appendix-observability.md` registry
    row, and a `DecdnCacheInflightMutexPoisoned` alert on `> 0`. The anti-panic
    policy makes poison close to unreachable, so any nonzero value is a bug
    report, not a threshold to tune; `docs/runbook.md § Cache coalescing mutex
    poisoned` says so, and says explicitly **not** to restart. No config, wire,
    or ABI change.

#### Observability

- **The ADR metric registry can no longer document series the node never emits
  (#1513).** `adr/appendix-observability.md` calls itself the canonical registry
  but had no way to say "specified, not built" — its only axis was the M/R
  *requirement* tier — so roughly a third of its rows read as shipped while
  nothing exported them.
  - **New `Status` column** (`live` / `planned`) on every registry table, and a
    new `adr_registry_names_are_exported` test asserting every `live` row
    resolves against the encoder's output. `planned` is the allowlist that gate
    skips, which is what lets the check be absolute instead of carrying a
    hand-maintained skip list that would rot the same way the names did.
  - **Corrected names:** `decdn_peer_table_size` →
    `decdn_gossip_peer_table_size` (also in `adr/appendix-peer-table-eviction.md`
    and a `crates/common` config doc) and `decdn_gossip_announces_sent_total` →
    `decdn_gossip_announces_published_total`. ADR 005 and ADR 022 specified a
    single labelled `decdn_{probe,dht}_rate_limit_rejections_total`; the exporter
    has always emitted sibling trios, and the ADRs now say so.
  - **Added rows** for both rate-limit families (rejected / prune-sweeps /
    tracked, 14 series), which existed only as brace-expanded shorthand in the
    reason-splits table and so carried no Type, Tier, or description anywhere.
  - `decdn_peer_table_evicted_registry_total` was specified with a `reason`
    label, contradicting the same appendix's claim that
    `decdn_probe_hold_unavailable_total{reason}` is the one labelled reason
    split. It is now two sibling counters, per #1475.
  - The doc comments in `crates/node/src/dht/rate_limit.rs` and
    `crates/node/src/handlers/probe_rate_limit.rs` justified sibling counters
    with "the `iroh_metrics::MetricsGroup` backend has no per-field labels".
    That is false — `probe_hold_unavailable` and `streams_active` are both
    `Family<L, M>` — and it contradicted five other comments in the tree.
    Rewritten to state the #1475 convention as the deliberate choice it is.
  - `decdn_cache_tag_drop_failures_total` joins the name-pinning array; it has
    been bumped since #860/#837 but was never pinned.

#### Node serve path

- **A fault in this node is no longer signed to clients as a `NotFound` about
  the content (#1560).** When a node-to-node pull failed for a reason that was
  *ours* — a buyer key that cannot sign the ADR 005 client binding, a voucher
  signature the upstream cannot verify (`BadSignature` / `WrongSigner`), an
  unusable `cache.node_pull_timeout_sec` / `cache.node_pull_stall_timeout_sec`
  budget, or a buyer-channel store this node cannot read — the serve path
  collapsed it onto the same clean `NotFound` as "no provider had it". The
  failure was metered honestly all along
  (`decdn_node_pull_local_fault_total`, "any sustained rate is an emergency"),
  but the wire answer told the client the blob does not exist, when in fact it
  may be one hop away and perfectly reachable. That is the laundering
  `StreamError::InternalError` ("unexpected failure; do not retry this node")
  exists to prevent. Each candidate attempt now reports either the payload or a
  `PullMiss` saying whether the failure was ours: the buffered `Origin::fetch`
  surfaces a local fault as `OriginPullError::Permanent`, which the cache engine
  already maps through `CacheError::OriginError` → `FillOutcome::HardFault` →
  `ServeRejectReason::InternalError`, and the window-paced path folds it into
  the same `fault_seen` latch the reactive local-origin tier has used since
  #1129. Buyer channel-open failures are attributed at the site that raises them
  rather than guessed at by the caller: a poisoned open lock, an unreadable
  channel store, a store write that leaves a deposit untracked, a panicked open
  task, and a wallet that cannot fund a deposit all refuse, while a pending open,
  a reconcile-held slot, an unreclaimable expired channel, a per-provider on-chain
  revert, and a transient RPC fault stay clean misses.
  - Scope is deliberately narrow: only a local fault changes the wire code. A
    wedged or settled *channel* to one provider still answers `NotFound` — it is
    not evidence this node is broken for every client and every blob, and it
    already has its own remedy. So does an `OriginBlacklisted` refusal, which is
    node-wide but is a governance policy state rather than an unexpected failure,
    and the node can still serve everything already in its cache. A local fault
    also latches rather than aborting the walk, so a candidate that faults on our
    own encode or range does not throw away a blob the next candidate was about
    to serve.
  - **Operator-visible:** refusals that used to land in
    `decdn_serve_stream_rejected_cache_miss_total` — the noisiest benign counter
    on the serve path — now land in
    `decdn_serve_stream_rejected_internal_error_total`, so
    `DecdnServeInternalErrorRate` can fire on a node whose buyer side is broken.
    A buffered-path local fault also now bumps
    `decdn_node_pull_through_errors_total`, where it previously took the silent
    clean-miss branch. No metric was added or renamed.
  - **Known gaps, unchanged by this fix:** when the pull-through deadline expires,
    `tokio::time::timeout` drops the walk and any latched fault dies with it, so
    that exit can only report a fault an earlier tier saw. A node whose buyer
    bootstrap never completed is likewise indistinguishable on the wire from one
    with pull-through switched off.

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
  narrower ceiling. The span is chunk-group-aligned, because
  `export_bao_range_stream` serves the aligned superset and the serve path does
  not trim back, so pricing the requested span would under-reserve a small
  bounded range by up to two 16 KiB groups. What remains unreserved is only bao's
  proof interleave (the reservation counts content bytes, delivery bills wire
  bytes): at most ~1.3 KiB per request, against the 1 MiB-or-wider window that
  used to ship free. The mid-stream ceiling remains the exact authority. A funded
  request for a blob or range smaller than one interval is unaffected — the
  reservation is capped by the span, not by the cadence. This gate's placement
  relative to the cache-miss fill tiers is corrected by the next entry. No wire,
  config, or ABI change.
- **An underfunded channel can no longer make the node spend before it is refused
  (#1519).** The gate above runs on the serve path, which a cache miss reaches
  only *after* the fill tiers — and `try_range_pull_through`,
  `try_local_populate` and `try_pull_through` were gated on channel ownership
  (`pull_authorized`) but not on solvency, the last of them reaching the paid
  `Peer` origin. So a dust-deposit channel could name absent hashes, make the
  operator pay origin egress and upstream USDC for each, and be refused
  afterwards: the client gained nothing, but the bill was real. A **pre-spend
  floor** now runs above every tier, refusing when remaining headroom cannot
  cover one credit window. It is deliberately a floor rather than the serve
  path's exact span-capped ceiling, because `total_bytes` is unknowable before
  the fill — so it catches a channel that cannot pay for anything at all, and
  does not pre-judge a merely small request. Two deliberate consequences: a cold
  sub-window fetch that previously got a free fill is now refused (such a channel
  could not have completed the transfer either way), and this refusal *pre-empts*
  the #1129 fault latch — a client-attributable refusal wins over "this node is
  degraded", because it would refuse regardless of origin health. **Residual:** a
  channel funded to exactly one window can still trigger a whole-blob buffered
  node-to-node pull costing more than it can repay; that tier is bounded by the
  cache engine's enforcement of `cache.max_blob_size_mb` rather than by anything the
  handler prices (not a *separate* knob — both derive from the same config field, but
  only the engine bounds that tier), and giving it a ceiling in its own quantity is
  tracked separately.
  Internally, `respond_error` now takes the request's price as a required
  argument and `serve_stream` holds the crate's only production `clamped_rate()`
  call. That stops this function from recomputing the price *implicitly*, which is
  the shape the bug took — it does not make the invariant type-checked, since a
  caller can still pass an inline `self.clamped_rate()` or the wrong value. What
  holds it is the single call site plus two new tests
  (`a_refused_request_clamps_the_rate_exactly_once` and its zero-case sibling),
  where before it was review attention alone. No wire, config, or ABI change.
- **Buyer channel rows now record the deposit the contract credited, not the amount
  requested (#1521).** `openChannel` and `topUp` credit a measured balance delta —
  deliberately, so a fee-on-transfer settlement token cannot over-state a channel
  against the shared USDC pool — and emit that delta in `ChannelOpened.deposit` /
  `ChannelToppedUp.additionalDeposit`. The buyer path recorded its own requested
  amount instead, so under such a token its local row would over-state the deposit.
  The harm is buyer-side rather than a settlement revert: nothing in the buyer bounds
  vouchers by this field, and the seller refuses off-chain via `stage_voucher` long
  before any on-chain call. What breaks is the buyer's own headroom arithmetic — the
  auto-refill reads `deposit - prior_amount` from the inflated row, so it fires late
  or short, and because credits accumulate the drift compounds with every top-up
  until every seller refuses us. `open_channel` now reads `ChannelOpened.deposit` (it
  already took `channelId` and `expiresAt` from that event) and `top_up` decodes
  `ChannelToppedUp` rather than assuming the requested amount landed. If that event
  is absent — meaning our view of the contract is wrong, which is the worst state in
  which to guess — it reconciles against `getChannel` and `warn!`s; only if that
  read *also* fails does it credit the requested amount, and then at `error!`. Not reachable
  today — mainnet USDC is not fee-on-transfer — so this is defensive depth, not a
  live fix. Note what protects us is that the settlement token's ADDRESS is immutable
  at deployment, not its behaviour: real USDC is an upgradeable proxy, which is the
  hazard `PaymentChannel`'s own comment names. **Contract
  behaviour is unchanged:** a partial shave is still credited as received rather
  than reverted, which is now an asserted decision instead of an unexercised path;
  reverting would hard-code "the settlement token must never be fee-bearing" into
  the contract, a policy call larger than the test gap that prompted this.
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

- **CLI: a range-dedup entry stripes its ranges across its holders (#2123).**
  A `bundle pull` entry that pays for many scattered complement ranges used
  one provider's link for all of them, even when several holders were probed.
  The entry now opens one warm session on each admitted full holder (one per
  operator, at most `--max-sources`, with multi-source on) and fills its ranges
  from one shared queue across all of them, up to `--max-lane-streams` per
  lane. A lane that faults leaves the stripe, with a warning that names its
  provider, and the other lanes fill what it left. A drive stall is every
  lane's fault, so the whole stripe leaves; a local disk fault or a terminal
  fault ends the entry without failover. When no striped lane is left, the
  entry fails over to its other candidates one at a time, as before. All lanes write into the entry's one
  ranged store, run under one drive-level floor, and spend one top-up budget.
  A lane that served every gap it took in a drive that filled every range
  settles at its committed cumulative; any other lane settles at its armed
  cumulative. Only the last session opened for a drive opens its first leg, so
  the primed pull is not left to go stale. Every leg now checks the provider's
  signed size against the store's before it reads a byte
  (`SignedSizeMismatch`), so a lane that never opened a first leg still
  refuses a manifest whose size is wrong. That refusal blames no peer and
  starts no retry round. The SDK adds `drive_range_lanes`, `RangeLane`,
  `RangeSetOutcome` and `SignedSizeMismatch`. `drive_range_set` is the
  one-lane case.
- **Logging: one field name per concept, and peer-triggered warnings are
  throttled.** Every tracing event names its error field `error` (was a mix of
  `err`, `%err` and `error`); a `tracing-field-names` pre-commit hook keeps it
  that way. Update any log query or alert that matched `err=`. DHT logs name the
  remote id `peer` (was `responder` / `seed`) and print it as lowercase hex, the
  same as the transport's `peer`. The client-binding, unbound-request,
  unknown-lane and probe read-fault `warn!` lines — all of which a remote peer
  can trigger at will — now fire at most once a minute per cause and carry
  `peer` and a `suppressed` count. A failed `redeemMany` receipt wait or timeout
  logs the transaction hash, since the transaction may still mine.
- **A config reload no longer replaces a `RUST_LOG` filter.** `RUST_LOG` wins
  over the config file `log_level` at startup; a SIGHUP or admin reload used to
  swap it for the bare file level, dropping its per-target directives. The
  reload now leaves a `RUST_LOG` filter in place and logs that it did.

- **The workspace reads `0.0.0` until the first release is cut, and the
  versioning invariants are enforced rather than described.** `decdn
  --version` and the default user agent report `0.0.0`; the first `cargo
  release minor` makes it `0.1.0`. Three guards under `.github/scripts/` run
  in the `packaging` CI job and as pre-commit hooks: the Rust version must be
  one identical `X.Y.Z` across `rust-toolchain.toml`, `Cargo.toml`'s
  `rust-version` (now `1.95.0`) and every `dtolnay/rust-toolchain` step;
  every member inherits `version`/`edition`/`license`/`rust-version` and the
  workspace lint table, and reaches a sibling only through its
  `[workspace.dependencies]` alias; and the crate dependency flow in CLAUDE.md
  is held as a table over `cargo metadata`, with `decdn-cli`'s closure kept
  free of `iroh-blobs` and the AWS SDK. GitHub release notes open with a
  `### Breaking` section.
  - Why: the manifest claimed `0.1.1`, a release that was never tagged or
    published, and nothing compared the fourteen sites that spell the Rust
    version, so a Dependabot action bump could move CI onto a compiler no
    developer runs.

- **`missing_docs` is on, and the 230 public items that lacked a doc comment
  have one.** 170 were struct fields and 24 enum variants — the shapes rustdoc
  renders as a bare name with no explanation, which is where the gap actually
  hurt: `ResolvedConfig`'s twelve sections, `ClientHandlerDeps`' required
  wiring, the `expected`/`recovered` pairs on every signature-recovery error,
  and the `PoolError` operands a node reads when it refuses a voucher. The
  `alloy::sol!` bindings needed nothing: each generated block already opts out
  where it is declared. Every new comment is also link-checked, since the `doc`
  gate denies rustdoc warnings.

- **`elided_lifetimes_in_paths` and `unreachable_pub` are on.** A type path that
  borrows now says so — `TableDefinition<'_, …>` across `channel_store`'s eleven
  redb table definitions, `EvictionContext<'_>` in the cache's admission and
  eviction policy traits — so a reader sees the borrow at the call site instead of
  having to look the type up. `unreachable_pub` narrows 46 items that were `pub`
  inside a private module to the visibility they actually had: mostly the shared
  integration-test helper modules (`node/tests/support`, `cache/tests/util`,
  `cli/tests/common`), plus `client-pull`'s `progress` and `ledger` internals and
  `load_shed`'s egress-budget helper. No item's real reachability changes.

- **The workspace moves to cargo's MSRV-aware resolver and denies stray output,
  scaffolding macros, and lossy numeric casts.** `resolver = "3"` changes nothing
  about feature unification — that was resolver 2, which the workspace already
  had — but it defaults `resolver.incompatible-rust-versions` to `fallback`, so
  cargo prefers dependency versions whose declared `rust-version` is at or below
  the pinned 1.95. Nothing resolves differently today and `Cargo.lock` is
  unchanged; the guard is against a future `cargo update` pulling a dependency
  that raised its MSRV past the toolchain. Alongside it, `print_stdout`,
  `print_stderr`, `dbg_macro`, `todo`, and `unimplemented` become `deny`, and
  `cast_possible_truncation` / `cast_sign_loss` / `cast_precision_loss` move from
  `warn` to `deny`. The cast promotion adds no new violations — CI clippy and the
  pre-commit hook already pass `-D warnings`, so every cast was fatal there; it
  only makes a bare local `cargo clippy` agree. `dbg_macro`, `todo`, and
  `unimplemented` had zero occurrences and are pure regression guards. The `decdn`
  CLI allows both print lints at its crate roots because it is a terminal UI; the
  handful of production `eprintln!` sites that run before the tracing subscriber
  exists carry a per-site `#[expect]` naming that reason, which leaves every
  library crate and every node handler with no way to print.

- **Dev builds compile the keystore KDF at `opt-level = 3`.** `alloy`'s
  `signer-keystore` runs scrypt on every keystore encrypt and decrypt, and an
  unoptimized scrypt costs about 0.5s per operation. Raising `scrypt`, `salsa20`,
  `pbkdf2`, `sha2`, and `hmac` to `opt-level = 3` under `[profile.dev.package]`
  takes the `decdn-cli` suite from 3.79s to 3.20s (-16%), with the `key_gen_e2e`
  tests themselves 16-22% faster; the `anvil-e2e` journeys pay the same cost once
  per keygen. Nothing else about the dev profile changes, so debuginfo on
  workspace crates is untouched.

- **The node flushes lane writes to redb in table-key order, and the workspace
  floor moves to `redb = "4.2"`.** `flush` drains a `HashSet` of dirty lanes, so
  the batch reached redb in an arbitrary order. Sorting it by the
  `pool_id ‖ signer ‖ provider` table key first hits redb's append fast path,
  which fills a leaf page before opening the next instead of leaving each one
  part-full at a random split point: measured over 512 lanes, `lane_state_v1`
  occupies about 30% fewer leaf pages (47 against 65-68). The gain lands on keys
  appended past the end of the table — new lanes, and the cold-load case — since
  an overwrite of a hydrated lane replaces in place whatever order it arrives in.
  The `4.2` floor is load-bearing: that append fast path arrives in 4.2, and below
  it the same sort is a pessimization (1.31x MORE leaf pages on 4.1), so a
  lockfile regeneration resolving under the floor would silently invert the
  change. Tombstones sort alongside the writes, so a flush is deterministic.

- **config: `cache.max_blob_size_mb` now defaults to `cache.cache_size_mb`
  instead of a fixed 1 GiB.** The old memory-era default refused a blob a node's
  disk could easily hold. Unset, the disk budget is now the admission ceiling; a
  node admits any blob it can store. The knob stays node-configurable for
  operators who want a tighter per-blob bound (for example to cap the RAM the
  buffered miss tier spends on one pull). The load-time invariant relaxes from
  `max_blob_size_mb < cache_size_mb` to `<=` so the default (equality) is legal;
  a value above `cache_size_mb` is still rejected as a permanent-reject
  misconfiguration, and `0` still means unlimited.

- **`contracts/script/lib/BuybackVenueLib.sol` is now the single home for buyback
  venue dispatch and burner construction (#1090).** The steady-state FeeRouter
  split, the canonical Permit2 address, the `BUYBACK_VENUE` string dispatch, and
  the per-venue `Config` literal were duplicated across `BaseProtocolDeploy`,
  `ActivateBuyback`, and `DeployProtocol`. Four items each had two copies — the
  steady-state split, the canonical Permit2 address, the venue-string dispatch, and
  the per-venue `Config` literal — so the deploy-time genesis activation and the
  post-deploy runbook could wire different burners from the same inputs. Behaviour
  is unchanged; the duplication is gone.
- **Burner wiring and guard-band validation moved into `BuybackVenueLib`.** The
  completeness checks added earlier lived on `BaseProtocolDeploy`, so only the
  genesis path had them — `ActivateBuyback`, the post-deploy runbook with no
  `_assertBuybackActivated` backstop, had none. That is the two-entry-point drift
  #1090 exists to remove, reintroduced by the fix for it. Both paths now validate at
  the library both already call (`WiringIncomplete`), which also closes a second
  dead-burner door: at the time `GuardedBuybackBurner` rejected an inverted band
  but not a zero one, so `MIN_BUYBACK_AMOUNT=0 MAX_BUYBACK_AMOUNT=0` constructed
  cleanly and then reverted `AboveMaxBuyback` on every non-zero buyback
  (`GuardBandDead`). Superseded by #1532 — the burner now rejects a zero ceiling
  itself, and the library guard is retained as a pre-broadcast fail-fast.
- **`PoolSeed` is self-checking rather than self-describing.** It now records
  `targetPrice`, and `_assertVenueSeedMatches` re-derives `tokenSeed` instead of
  trusting the venue tag. A tag alone is an unverifiable claim by whoever built the
  struct, and `Venue.UNISWAP == 0` made it vacuous on a default-constructed seed —
  which is the venue the Arbitrum Sepolia launch uses. A mismatch now reverts
  `PoolSeedNotDerived`; an unset one reverts `TargetPriceZero`.
- **`_activateBalancer` rejects a caller-supplied `bal.wiring.pool`** rather than
  overwriting it (`PoolPrefilled`). The field is filled from the pool the script
  creates, so a supplied value was silently discarded and a second pool created and
  seeded with real protocol-owned liquidity — the natural mistake, since
  `ActivateBuyback` reads `BALANCER_POOL` from env because it wires a live pool.
- **The `solidity fork test` job now reports which fork RPCs are configured.** A
  skipped step yielded a green job rendered identically to one where every fork
  suite passed, which is how it went unnoticed that no fork RPC secret has ever
  been set — all four suites have been permanently unexecuted behind a passing
  check. The job now always writes each secret's state to the run summary and
  emits a warning for the missing ones. Deliberately not a hard failure: fork PRs
  cannot carry secrets by policy.
- **`_assertTimelockRoleSeating` is mode-independent and also checks the deployer.**
  Its negative half was gated on bootstrap mode, so a default deploy asserted
  nothing about anyone *other* than the Governor holding a scheduling role, and the
  deployer was never checked for one at all — it admins the Timelock through phases
  3-5, so a future `_postWiringHook` could grant itself one and survive the handoff.
  `ProposerNotSeated(address, bytes32, bool)` splits into `TimelockRoleMissing` and
  `TimelockRoleUnexpected`: the two directions mean opposite things (ungovernable
  vs. governance live too early) and a boolean three commas deep does not say which
  at the moment a deploy aborts mid-broadcast.
- **CI runs the Uniswap genesis-activation fork suite (#1090).** The
  `solidity fork test` job now passes `ARBITRUM_SEPOLIA_RPC_URL` through, so
  `GenesisBuybackActivation.fork.t.sol` can execute instead of self-skipping. It
  covers the Arbitrum Sepolia deploy path, which had no executable CI coverage.
  **Still gated on the secret existing** — no fork RPC secret is configured on the
  repo today, so all four fork suites remain unexecuted; see the fork-RPC reporting
  entry above, which is what makes that visible.

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

- **Observability: the first latency histograms, so a latency SLO can be
  written in PromQL (#2047).** Before this change the exporter had no
  histogram, and latency came only from Tempo span durations. Those depend on
  trace sampling, and the `serve_stream` duration grows with blob size. Four
  histograms are now live:
  `decdn_probe_collection_latency_seconds` (M-tier, from the start of the
  concurrent probes to the end of collection), `decdn_serve_first_byte_hit_seconds`
  and `decdn_serve_first_byte_miss_seconds` (from the decoded request to the
  first `ChunkData` frame, split by the availability gate's class), and
  `decdn_node_pull_first_byte_seconds` (from the start of each paid pull leg's
  open, dial included, to its first bao bytes). A drive that adopts a
  header-handshake pull keeps the start of the handshake open, so that leg's
  time to first byte does not read as zero. On a buffered miss fill the whole
  blob lands before the first frame, so that tier's miss latency grows with
  blob size. The delivery dashboard gains serve and pull time-to-first-
  byte panels, and the node dashboard gains a probe collection latency panel.
  No alert rule reads them yet.

- **Runtime: `decdn-node` heartbeats the systemd watchdog from its tokio
  runtime.** When the unit sets `WatchdogSec=`, the node sends `WATCHDOG=1` to
  `NOTIFY_SOCKET` every half period from a runtime task. A node whose runtime
  workers are all held by work that never yields stops the heartbeat, and
  systemd restarts it. Without the watchdog, such a node stays `active` while it
  serves nothing, logs nothing and answers no `/metrics` scrape. The node has
  no setting for this: `WatchdogSec=` turns it on, and without `WATCHDOG_USEC`
  in the environment the heartbeat does not start. The unit must allow
  `NotifyAccess=main` and `AF_UNIX`. A regression test runs the #2132
  sliver-voucher case on a one-worker runtime and fails if a heartbeat task on
  that runtime stops ticking.

- **CLI: `decdn pool list --all` shows every pool the keystore owns on chain
  (#2077).** `pool list` read only the local buyer store, so it could not show
  a pool whose store row was gone — precisely the situation an operator runs it
  in. A reset `identity.data_dir` loses the only local record of a funded
  deposit, and the default listing then prints nothing, because the file it
  reads is the file that went missing. `--all` enumerates
  `PaymentPool.getPools` by the keystore address and prints every pool in every
  lifecycle state with its on-chain `deposit`, `totalRedeemed` and reclaim
  window, marking which ones the local record tracks. Every local read on this
  path is read-only: `--all` sends no transaction, writes no pool record, and
  manufactures no store — creating one and reporting its emptiness is the #2078
  defect, and it would make a lost store indistinguishable from a store that
  tracks nothing. `TRACKED` distinguishes `no` from `?`: a pool the local record
  cannot answer for — an unreadable store, or a row that will not decode — is
  unknown rather than untracked, because the remedies differ (a lost store
  versus a record to repair) and one bad row must not blank the verdict for the
  pools either side of it. A store that will not open is named on stderr with
  the reason, and the table carries `local_store=read|unreadable` so the
  distinction is not `--json`-only. Unlike `close --all` / `reclaim --all` it is
  **not** refused on a node's data dir — those two write, this one reads, and a
  node host is where it is most needed. `--json` emits a third document shape
  with `source: "chain"`; read `source` before `pools`, as with the other two.
  `PoolListArgs` now flattens `PoolChainArgs`, matching `close` and `reclaim`:
  `--data-dir` is unchanged and the chain flags are new, read only by `--all`.
  The `docs/runbook.md` stranded-pool section that was written around this gap
  now uses it.

- **CLI: `decdn pool list` reads a stopped node's `buyer.redb` off disk
  (#2084).** #2081 routed the listing on a node data dir through
  `admin_v1_pools` and deliberately did not fall back to the client store,
  which left a stopped node's buyer store readable by nothing — no route for a
  post-mortem after a crash, or for a host down for maintenance. `redb` holds
  its process-exclusive lock only for the lifetime of an open `Database`, so on
  a refused admin connection the CLI now tries a read-only open of the daemon's
  own `buyer.redb`. It opens, so no daemon holds it: the listing renders with
  `store=… (read from disk; no daemon running)` and `source=node_store_offline`
  under `--json` — its own value, never the client store's. It is write-locked,
  so a daemon **is** running: the error says the admin URL or `admin_port` is
  wrong rather than telling the operator to start a node that is already up.
  Anything else keeps the previous error with the disk read's reason appended.
  New `decdn_incentive::buyer_pool_redb::ReadOnlyBuyerPoolStore` takes a file
  path rather than a data dir and never creates, which is what keeps #2078
  closed; a `redb` database left unrepaired by a crash reports the new
  `StoreError::NeedsRepair`, naming the one fix (start the daemon once). Only a
  refused connection takes this route — a timeout or a JSON-RPC error means the
  daemon answered, so neither goes near the file.

- **Admin/CLI: `admin_v1_pools` and `decdn node pools` read the node's buyer-side
  `PaymentPool` state (#2078).** The buyer-side counterpart of `admin_v1_lanes` /
  `decdn node lanes`: every pool the buy leg tracks, the deposit it believes each
  holds, and the per-lane amount already signed away to each provider. This is the
  *only* read path to that state on a running node — `redb` holds a
  process-exclusive lock on `buyer.redb` for the daemon's lifetime, so no other
  process can open it, not even read-only. A node with no `[blockchain]` wiring
  answers `BUYER_POOL_UNAVAILABLE_CODE` (`-32011`) rather than an empty list,
  because "this node never pays for pulls" and "this node owns no pools" send an
  operator hunting a stranded deposit in opposite directions; a store read failure
  answers `BUYER_POOL_STORE_ERROR_CODE` (`-32010`). `decdn pool list` gains
  `--admin-url` and `--timeout-ms`, used only when the data dir turns out to be a
  daemon's.

- **Metrics: the buyer leg reports itself (#2072).** New gauge
  `decdn_buyer_wallet_usdc`, read once per reclaim sweep and at bootstrap, plus
  two counters for the states that silently cost USDC:
  `decdn_buyer_lane_seed_failures_total` (pulls refused because a lane's
  already-paid watermark could not be established) and
  `decdn_buyer_pool_adoption_failures_total` (bootstraps that could not tell
  whether this node already owns a pool, and so are about to open a second). The
  buyer leg is the one part of the node that spends rather than earns, and
  nothing reported on it: an operator who never funded the wallet saw a node that
  served perfectly and silently bought nothing. Two alerts go with it —
  `DecdnBuyerWalletUnfunded` (below one working deposit, scoped to nodes that
  actually pull) and `DecdnNodePullNeverSucceeds` (attempts with no success over
  6h), plus a `docs/runbook.md` section covering wallet funding and recovering a
  pool the node has forgotten.

- **Metrics: the serve path reports its own cache hit rate.** New siblings
  `decdn_serve_cache_{hit,partial_hit,miss}_total`, bumped at the
  blob-availability gate in the client dispatch path — ahead of the load-shed
  admission, so the ratio stays a property of the store rather than of current
  pressure. Exactly one of the three fires per request reaching the gate; a
  withdrawn hash is refused before it and counts in none.
  - These exist because `decdn_cache_hits_total` and
    `decdn_cache_bytes_returned_total` are scoped to `CacheEngine::get`, the
    whole-blob buffered read the paid serve path never calls — it streams
    through `export_bao_range_stream` instead. A node serving only paying
    clients therefore holds both at zero under full production load — while
    `decdn_cache_misses_total`, which the fill tiers bump too, keeps climbing,
    so the pair reads as a populated permanent 0% rather than an empty series, so the fleet dashboard's hit-ratio panel read a permanent
    0% and its throughput panel plotted a flat-zero series as "served". The
    registry rows for the `get`-scoped counters now say so, and the panels read
    `decdn_bytes_served_total` for delivered bytes.
  - `decdn_serve_cache_partial_hit_total` is counted apart from the plain hit so
    the payoff of partial-holder advertisement (ADR 038) stays legible; both are
    hits for hit-rate purposes.

- **Monitoring: unattributed stream failures and pull success rate are on the
  dashboards.** The fleet overview and the delivery dashboard gain a residual
  panel — `rate(decdn_streams_failed_total{direction="inbound"})` minus the 22
  seller-leg counters that each end exactly one inbound stream — which makes a failure mode that no counter names
  visible as a step change. The overview also promotes node-to-node pull success
  rate, so a pull leg failing every attempt reads as a ratio pinned at zero
  rather than as low traffic. `decdn_serve_stream_rejected_bad_binding_total` and
  `decdn_node_pull_pool_open_failures_total` join the refusal and pull-failure
  breakdowns they were missing from, and eleven further exported-but-unplotted
  series (probe read faults, the fee-shares watcher downtime/restart pair, the
  reconciled-redemption skip, the per-peer rate-limit prune sweeps and the iroh
  path-composition family) land on their existing panels.

- **Metrics: stream outcomes, byte volume, on-chain transactions and DHT
  health are exported, and the gaps they expose now alert.** New series:
  `decdn_streams_{completed,failed}_total{direction}`, `decdn_bytes_served_total`,
  `decdn_bytes_received_total`, `decdn_pool_redemptions_total`,
  `decdn_vouchers_received_total`, `decdn_pool_grace_closes_total`,
  `decdn_onchain_tx_{landed,reverted,send_failed,receipt_failed,timeout}_total`,
  `decdn_dht_{findvalue_queries,lookup_round_timeouts,store_published,bucket_refresh_failures,bootstrap_find_node_failures}_total`,
  `decdn_dht_routing_table_size`, `decdn_fee_shares_watcher_{poll_failures_total,restarts_total,down_seconds}`,
  `decdn_config_reload_failures_total`, `decdn_receipt_write_failures_total` and
  `decdn_serve_stream_midstream_pool_exhausted_total`. The ten registry rows
  that were `planned` for these ship as `live`; `decdn_streams_failed_total`
  carries `direction` only, with the reason split in the existing sibling
  counters. New alerts: `DecdnSlashDetected`, `DecdnRpcUnhealthy`,
  `DecdnFeeSharesWatcherStalled`, `DecdnFeeSharesPollFailing`,
  `DecdnRedemptionFailing`, `DecdnOnchainTxTimeouts`, `DecdnServeNodeFault`,
  `DecdnFrameAccountingFault`, `DecdnWatcherPersistFailures`,
  `DecdnLaneFlushFailures`, `DecdnNodePullCorruption`,
  `DecdnConfigReloadFailed` and `DecdnReceiptLogWriteFailing`. The delivery,
  chain and node dashboards gain panels for each family.
- **Tracing: deCDN emits its own spans.** Each inbound client stream is a
  `serve_stream` span with its `outcome` (`completed`, `refused`, `stopped`,
  `reset`, `failed`, `panicked`, `cancelled`), `reason`, `bytes` and `error`. A
  buffered cache miss nests `pull_through`, `origin_pull`, `node_pull`,
  `upstream_stream` and `open_progressive_pull` under it; a streaming miss nests
  `serve_miss_pull` and `open_progressive_pull`. `dht_lookup`, `redeem_cycle`
  and `onchain_tx` cover discovery and settlement. A failed pull or
  transaction carries an error status. Both ends of a transfer record the same `hash`, `pool_id`,
  `byte_offset`, `peer` and `local_node_id`, so one TraceQL query finds both
  sides. No trace context crosses the wire. The span export no longer follows
  `log_level`; it exports `INFO` spans from the deCDN crates and `WARN`
  events from the rest, and the OTLP
  resource carries `service.version`. The node dashboard gains a deCDN
  operation latency panel ([appendix-observability](adr/appendix-observability.md#trace-spans)).
- **CLI: `-v` / `-vv` / `-vvv` turn on client-side logging** at `info` /
  `debug` / `trace`. `--log-level` wins over the count, and `RUST_LOG` wins over
  both.

- **Monitoring: the chain dashboard shows a world map of active nodes by
  declared region.** New gauge `decdn_staker_set_active_by_region{node_region}`
  counts active stakers per region (ADR 030) from the CapacityBond registry.
  Each node holds the whole network, so a single scrape counts every active node.
  New gauge `decdn_staker_set_active_unknown_region` counts active stakers with
  an empty or invalid `regionHint`. `monitoring/dashboard-chain.json` gains an
  "Active nodes by country" Geomap panel, an "Active nodes per region" table
  (which also lists codes the map cannot place) and an "Active nodes without a
  region" stat panel.
  - The label is `node_region`, not `region`, so it does not collide with the
    scrape-side `region` target label. Query it with `max by (node_region)`:
    summing across nodes multiplies the count by fleet size.
  - The CapacityBond registry now follows `RegionUpdated`, so a region change
    reaches the region map, the dashboard and the ADR 030 latency penalty on
    the next watcher tick instead of the next 15-minute re-enumeration.
  - The registry's region map stores the canonical region code: a raw
    `" de "` on chain becomes `DE`, and an invalid hint has no entry. The
    ADR 030 latency penalty compares these codes against the node's own
    normalized `identity.region`, so a padded or lower-case hint no longer
    evades it.

- **Observability: the single reference dashboard becomes a four-dashboard suite
  covering metrics, logs and traces.** `monitoring/` gains
  `dashboard-delivery.json` (`decdn-delivery`), `dashboard-chain.json`
  (`decdn-chain`) and `dashboard-node.json` (`decdn-node`) beside the rebuilt
  `grafana-dashboard.json` (`decdn-poc-overview`, retitled "fleet overview"; uid
  and filename unchanged so existing links hold). Coverage goes from 25 named
  series to 207 — every serve-refusal reason, the whole `decdn_node_pull_*`
  funnel, all five chain-event watchers, the `decdn_iroh_*` transport
  sub-registry, and `node_exporter` host panels that join on the same `instance`
  label. Loki panels parse the daemon's tracing JSON (`| json lvl="level"`),
  since journald stamps every stdout line priority `info` and the Loki `level`
  label reports that, not the event's real severity. Tempo panels supply the only
  latency distribution available anywhere: the exporter registers no histograms,
  so `histogram_quantile` has nothing to read.
  - `monitoring_selectors_are_exported` now sweeps every `.yml` and `.json` under
    `monitoring/` instead of two hard-coded filenames, so a new dashboard is gated
    the day it lands. The exported set it compares against includes the
    `decdn_iroh_*` sub-registry, which `Metrics::new()` alone does not carry —
    `register_iroh_endpoint` is split so the test can register an
    `EndpointMetrics::default()` without standing up a socket. Without that split
    every iroh panel would read as an unexported name.
  - Query bugs fixed while rebuilding: the cache hit ratio no longer clamps a
    per-second *rate* denominator to a floor of 1.0 (which understated every node
    doing under one lookup per second — all of them); USDC gauges are scaled out
    of raw 6-decimal base units and carry `currencyUSD`; watcher tick-age
    thresholds move from 600/1800s to 120/180s so the panel cannot read green
    while `DecdnBlacklistWatcherStalled` is firing; panels that aggregate
    `by (instance)` carry `{{instance}}` in the legend instead of emitting one
    identically-labelled series per node.

- **Alert rules gain `component` labels and `runbook_url` annotations.** All 18
  rules carry a `component` for routing; the 14 with a matching `docs/runbook.md`
  section link straight to it, so Alertmanager and Grafana can render a button
  rather than burying the path in prose.

- **`decdn fetch` fans a large blob out across several holders at once (ADR 039,
  #1164).** A blob above `--multi-source-min-bytes` (64 MiB) with at least two
  operator-distinct holders is fetched in parallel over the existing paid
  `cdn/client/v1` protocol: bao-aligned segments, one per source, with a freed
  source stealing the second half of the largest range still in flight. No new
  wire surface — a bounded `byte_len` request already exists and the node
  already bills the exact aligned span. New flags: `--multi-source` /
  `--no-multi-source`, `--max-sources` (4), `--multi-source-min-bytes` (64 MiB),
  `--unit-deadline-ms` (10 s). The node's cache-miss pull leg is untouched, and
  `bundle pull` stays single-source.
  - **One payment lane per operator, enforced.** A voucher lane is keyed on
    `(signer, provider)` and `provider` IS the operator address, so admission
    takes at most one node per operator and the admitted set shrinks rather than
    repeat one. The scheduler re-checks the same precondition on the lane set it
    is handed.
  - **One deposit, one set of pool facts.** Every lane's deposit gate subtracts
    the spend committed across ALL lanes, the reactive-top-up budget is counted
    once per fetch rather than once per lane, and a landed top-up credits every
    lane's context.
  - **A failed fan-out falls back to single-source failover** unless the failure
    is terminal (a pool exhaustion, or what the shared classifier rules
    terminal). The fetch reports what every source did and keeps the last real
    error as the cause.

- **The workspace is published to crates.io.** Eleven crates ship —
  `decdn-protocol`, `-config-types`, `-bao-range`, `-common`, `-cache`,
  `-gossip`, `-reputation`, `-incentive`, `-client-pull`, `-node` and
  `decdn-cli` — making `cargo install decdn-cli` and `cargo install decdn-node`
  work. `decdn-e2e` stays `publish = false`.
  - Publishing is a new maintainer-run step, `.github/scripts/publish-crates.sh`,
    which runs *after* `sign-release.sh`. It verifies the tag against `KEYS` and
    against `origin`, requires the GitHub Release to be published rather than a
    draft, and publishes from a detached worktree at the tag rather than from the
    working copy. There is still no registry credential in Actions secrets.
  - `[workspace.package]` gains `version`, `repository`, `homepage`, `keywords`
    and `categories`; every crate inherits them and carries its own `README.md`.
    The internal `[workspace.dependencies]` entries gain `version` fields, which
    is what makes them publishable — cargo rejects a path dependency with no
    version, since a published crate has no path to follow.
- **Two packaging gates, because a crates.io version can never be replaced.**
  `release.yml` now runs `cargo publish --workspace --dry-run --locked` before
  the draft release is created, which packages and verify-builds every crate. A
  cheap `packaging` CI job (and matching pre-commit hooks) runs
  `check-package-embeds.sh` — no publishable crate may `include_str!` a file
  outside its own package — and `check-deployment-mirror.sh`.
  - The checker covers `include!`, `include_str!`, `include_bytes!` in all three
    delimiter forms, and `#[path = "…"]`; it enumerates workspace members from
    the root manifest rather than a `crates/*` glob, treats a target that git
    does not track as unshipped, and fails rather than reporting success when it
    finds no publishable crates. Its logic lives in
    `.github/scripts/check_package_embeds.py` with unit tests under
    `.github/scripts/tests/`.
  - `#[cfg(test)]` sites are accepted rather than failed — the verify build does
    not enable `cfg(test)`, so they do not block publishing — but each must be
    listed in the checker's `KNOWN_TEST_ONLY` allowlist, and a listed entry that
    no longer fires also fails. Two are listed today, both embedding
    `examples/configs/arbitrum-sepolia.toml`
    (`crates/cli/src/commands/config.rs`, `crates/common/src/config/mod.rs`);
    `cargo test` cannot run from those published crates as a result.
  - The `packaging` job also runs `cargo package --workspace --no-verify` and
    asserts the image repository agrees across `release.yml`, `sign-release.sh`
    and `security.yml`.
- **`crates/cli/deployments/421614.json`,** a byte-identical mirror of
  `contracts/deployments/421614.json` (which stays canonical — the Foundry
  deploy script writes it). Same `include_str!` constraint as `TERMS.md`. A
  redeploy must be copied across; `check-deployment-mirror.sh` fails on drift in
  both CI and pre-commit, so stale baked-in contract addresses cannot ship
  silently.
- **The Dockerfile's base image is pinned by digest, and Dependabot now tracks
  the `docker` ecosystem** to bump it. The pin is what makes two builds of the
  same release tag ship identical base layers; it is also what makes the
  Dependabot entry work at all, since Dependabot can update a digest but cannot
  derive a version from the `bookworm-slim` tag.
- **`publish-crates.sh` refuses to start when more than five crates are new to
  crates.io.** `PublishNew` allows a burst of five and then roughly one per ten
  minutes, and the first release creates ten crates — one run would be
  rate-limited partway through, leaving some permanently published and the
  version spent. Clearing this is a one-time manual step; see RELEASING.md.
  Override with `DECDN_ALLOW_RATE_LIMIT=1` once the limit has been raised.
- **ADR 009 bootstrap-multisig governance phase (#1175).** `DeployProtocol` run
  with `BOOTSTRAP_MULTISIG=<addr>` seats that multisig as the Timelock's sole
  `PROPOSER_ROLE`/`CANCELLER_ROLE` holder and grants `DecdnGovernor` neither, so
  no operator vote can execute while the operator set is still thin enough for a
  cheap fleet to capture. The Timelock keeps `GOVERNANCE_ROLE` on every target in
  both modes, so the 48-hour delay applies to the multisig's own changes.
  `contracts/script/TransitionToGovernor.s.sol` prints the one-way Timelock batch
  that ends the phase; `_assertNoBackDoors` now fails a deploy whose proposer is
  not the one its mode seats. Off by default — the initial testnet deploy is
  unaffected.

#### Cache / config

- A pinned set larger than `cache.cache_size_mb` now logs a warning at startup
  and on reload. Pinned blobs are LRU-exempt, so the eviction driver could
  otherwise never reach its high-water target and the disk grows past the
  configured ceiling. The hazard applies to every node however the pinned
  content arrived.
- `decdn config validate` now reports `fs_rescan_interval_sec`, and
  `decdn config init`'s template documents it. It shipped without either, so its
  resolved value was invisible to operators.

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
- OTLP span export to `observability.otlp_endpoint` (`--otlp-endpoint`).
- `pretty` and `json` log formats (`--log-format`).

#### Identity

- Ed25519 node-identity key at `{data_dir}/node.secret`.
- Permissions validated at load: `0600` on the key file,
  non-world-writable parent (#261).

#### Documentation

- 25 Architectural Decision Records (ADRs 000–025) — see `adr/` for the
  full set and `adr/architecture.md` for the living overview.
- `CONTRIBUTING.md` with build / lint / test commands and pre-commit setup.

### Removed

- **ADR 024's ERC-7579 session-key production plan (#1848).** The deferred
  plan — a Safe-7579 adapter plus the `erc7579/smartsessions` session-key
  module for hot-path `slash_sig` / voucher signing, an ERC-4337 paymaster,
  and a bundler — is retired from ADR 024, along with its off-chain ERC-1271
  verification branch. It had no live consumer and no code footprint (only
  prose and doc-comments referenced it), and the buyer side it aimed at is
  already covered by ADR 003 capability delegation: a Safe funds a pool and
  delegates a capped, expiring EOA `signer`, verified off-chain by recovery,
  with no account-abstraction machinery. The shipped, load-bearing piece —
  universal `SignatureChecker` on-chain (`PaymentPool`, `CapacityBond`,
  `SlashJudge`, `DecdnGovernor`) and EOA-recovery-only off-chain verification —
  stays as present-tense canon. ADR 024 gains an **§ Unimplemented — Operator
  Custody While Serving** section that records the one real residual (a node
  whose serving identity is a smart account, unreachable via capability
  delegation because `registerNode` binds `msg.sender`) and why the obvious
  ERC-1271 route is unsound for it: ERC-1271 validity is mutable and
  operator-controlled, so a signature re-verified later as slash evidence can
  be made invalid after payment (selective validator, post-payment revocation),
  DoS'd via a gas-bomb validator, or wrongly cached from an off-chain RPC. The
  recorded direction is a native EOA operator-signer delegation mirroring the
  ADR 003 buyer capability, keeping `slash_sig` EOA-recovered — a future ADR,
  not a commitment here. Dependents move with it: ADR 012, ADR 019, ADR 003's
  ephemeral-binding note, `appendix-operator-key-rotation.md` (the third
  "slash-sig session key" and its rotation procedures are gone; the runbook is
  two keys), `architecture.md`'s ADR 024 summary, and the `RotateKeyTarget`
  doc note plus the renamed off-chain-verification citations in
  `crates/protocol` and `crates/incentive`. Docs and comments only — no
  runtime, wire, config, or ABI impact. ADR 024 stays **Draft**.

- **GitHub Agentic Workflows (gh-aw) and the Agentic Triage workflow.** The
  `issue-triage` workflow (`.md` source and generated `.lock.yml`), `aw.json`,
  `.github/aw/actions-lock.json`, the `agentic-workflows` Copilot agent file,
  `.github/mcp.json`, and the gh-aw-only `.gitattributes` are deleted. The
  `agentic-lock` CI job, its `agentic` paths filter, its `ci-success` entries,
  the `copilot-setup-steps` workflow, and the `github/gh-aw-actions` Dependabot
  ignore block go with them. Repository automation only — no runtime, wire,
  config, or ABI impact.

- **The VS Code devcontainer (`.devcontainer/`).** The container image and its
  firewall init script are gone, and most of CONTRIBUTING.md went with them. No
  CI job built the image, so nothing shipped depended on it; contributors now
  install a native toolchain per the rewritten
  [Development Environment](CONTRIBUTING.md#development-environment) section.
  Contributor tooling only — no runtime, wire, config, or ABI impact.

### Security

- **Probe `slash_sig` is now cryptographically verified before a response can
  influence selection.** The requester previously checked only the signature's
  length; `ProbeSlashData::verify_signer` had no production caller, so a node
  could return 65 bytes of anything and be selected on the `has_blob` and
  `rate_per_mb` it claimed. `decdn_client_pull::probe::verify_probe_response`
  now runs the value invariants, the echoed-field correlation, and recovery to
  the candidate's registered operator address (ADR 014 §1), and `fetch`/`bundle
  pull` call it before ordering candidates, as does the daemon's own
  provider-selection probe on a cache miss. A failure drops the response and skips
  the candidate as requester-local policy — never scored against the peer, since a
  signature that does not recover attributes nothing to anyone.
  - `decdn fetch` now distinguishes the two ways every candidate can drop out. A
    wrong `blockchain.slash_judge_address` or `chain_id` fails verification against
    every honest node, and reporting that as "none of the probed nodes hold the
    blob" sends an operator hunting for missing content instead of a local
    misconfiguration.
  - The quoted rate is what `slash_sig` makes non-repudiable: quoting `R1` on
    probe and charging `R2 > R1` within 30 s is slashable on those two signed
    messages alone. An unverified response is not evidence of anything.

- `ruint` → 1.20.0 (RUSTSEC-2026-0220: `Uint::overflowing_shl`/`overflowing_shr`
  returned false-negative overflow flags, so `checked_*` returned `Some` instead
  of `None`, `strict_*` failed to panic, and `saturating_*` wrapped; the bad
  `checked_shl` result makes `to_base_be` and string formatting loop forever on
  no-alloc builds for non-limb-aligned widths). Reaches us transitively through
  `alloy-primitives`, i.e. the U256 arithmetic on the payment path. No deCDN code
  calls the affected APIs — every `checked_shl` in `crates/` is on a primitive
  integer (`u64` in `cache/src/retry.rs`, `u32` in `protocol/src/framing.rs`), not a
  `ruint::Uint`.
  Lockfile-only bump; no dependency requirement changed. The six new `ark-*`
  lockfile entries are optional features we do not enable — `cargo tree -e normal`
  links none of them.
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
