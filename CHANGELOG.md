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

- **sponsord-onramp: `ONRAMP_DECDN_RELEASE` must be v0.3.0 or later
  (#2683).** The installers run `decdn get` without `-o` for a bundle or
  entry URI so that it keeps its overwrite and hidden-path guards. v0.2.x's
  `decdn get` has no hidden-path guard (GHSA-rff9-95mx-xpw9), so the onramp
  refuses to start with a pin below v0.3.0, a pre-release of it included.

- **CLI: a whole-bundle `decdn get decdn:<hash>/` without `-o` pulls into
  `./<hash>/` (lowercase hex), not `.`, and a pull into a default
  destination refuses hidden entry paths (GHSA-rff9-95mx-xpw9).** Without
  `-o`, a bundle lands in its own directory, and any landed path with a
  `.`-prefixed component — an entry URI's landing name included — is
  refused before any entry lands; the error names the first such path and
  how many more there are, and `--overwrite` does not lift it. An explicit
  `-o` names the directory, so it takes hidden paths unchanged. `--dry-run`
  with a cached manifest applies the same refusal. `decdn quote` reads and
  caches a bundle's manifest under the same `./<hash>/` default, and
  without `-o` warns when a default `decdn get` would refuse the plan. When
  `./<hash>` holds a file — the raw manifest a `decdn get decdn:<hash>`
  without the trailing `/` writes — a whole-bundle `get` or `quote` without
  `-o` stops before any network call instead of paying for a manifest it
  cannot land. A resumed default-destination pull now finds its state
  under `./<hash>/`. An older interrupted pull into `.` resumes with
  `-o .`, but that turns off both the overwrite and the hidden-path
  guards: use it only for a bundle you trust. Otherwise delete the old
  `.decdn-partial/` and pull again into the new default.

- **CLI: `decdn quote` opens no pool unless `--open-pool` (#2675).** A
  bundle quote with no cached manifest fetches it with a small paid pull from
  the buyer's open pool. With no open pool to reuse or adopt, the quote sends
  no approve or `openPool`, exits non-zero, and names both ways on:
  `--open-pool`, which lets the fetch open one at the working deposit
  (10 USDC by default) and pay its gas, and `decdn pool open`. A script that
  relied on `quote` opening the pool adds `--open-pool`. With or without the
  flag, an open pool gets a `topUp` when it runs low or a node refuses it for
  funds, which escrows USDC and pays gas. `decdn get` opens a pool as before.
  - `decdn-client`: `DepositPolicy` carries `open:
    decdn_client::session::PoolOpen` (`Allowed` or `Refused`), a required
    field. Under `Refused`, a lane build with no open pool to reuse or adopt,
    and a funding recovery step that would replace the run's pool, end the
    command with `decdn_client::PoolOpenRefused` before any allowance or
    `openPool`; `PoolOpenRefused::find` detects it in a pull's error.

- **Config: `cache.node_to_node_pull_through_enabled` defaults by role
  (#2603).** Absent, it resolves on for a node with no origin backend and off
  for a node that configures one, the same derivation as
  `cache.relay_foreign_namespaces`. It was off everywhere, so a node started
  from `decdn config init` or the README refused every miss it could not fill
  from its own backend. A node with no origin and no explicit value now buys
  its misses from peers and spends USDC from its buyer wallet. It escrows
  `blockchain.buyer_working_deposit_micro_usdc` (10 USDC) in a pool and
  refills the pool toward it whenever it falls below low water, so its spend
  is bounded by the wallet's USDC, not by the working deposit. Set the field
  to `false` to keep the old behaviour.
  - Admin wire: `StatusResponse` carries `pull_through`
    (`PullThroughStatus { enabled, buyer_wallet_micro_usdc }`), and `decdn node
    status` prints it on a `pull_through=` line that flags an empty buyer
    wallet. The field is required, so a `decdn` CLI at this release reads
    `admin_v1_status` only from a daemon at this release.
  - A node with pull-through on logs a warning at each read of an empty buyer
    wallet: at bootstrap, then once per hourly reclaim sweep.

- **Admin wire: `EarningsResponse` carries `covered_from_unix` in place of
  `ledger_since_unix` (#2624).** The field is the time the figures cover from:
  on the daemon, when the settlement ledger started recording (a stamp in
  `settle.redb`), or an earlier row's time; with `decdn node earnings
  --address`, the first scanned block's time. It is a required `u64`, so a
  `decdn` CLI at this release reads `admin_v1_earnings` only from a daemon at
  this release. `decdn node earnings` prints `covered_from=<date>` in its
  header and labels a period that starts earlier `(covered from <date>)`; the
  `decdn dashboard` earnings panel always shows the date.

- **Admin wire: `BuyerPoolSnapshot` carries `spent_micro_usdc`.** The field
  is the pool's spend as the store knows it (every tracked lane plus the
  spend other signers redeemed). It is a lower bound, so deposit less spend
  is an upper bound on what every signer on the pool can still spend. The
  field is required, so a `decdn` CLI at
  this release reads `admin_v1_pools` only from a daemon at this release.
  `decdn node pools` and `decdn pool list` gain a `SPENT` column between
  `DEPOSIT` and `LANES`, which moves `LANES`.
  - Why: `decdn dashboard` shows each pool's headroom, which the deposit
    alone overstates on a shared pool.
- **Admin wire: `LaneSnapshot` carries `unredeemed_micro_usdc` (#2598).** The
  field is required, so a `decdn` CLI at this release reads `admin_v1_lanes`
  only from a daemon at this release (`decdn node lanes` fails with "missing
  field `unredeemed_micro_usdc`" against an older one), and the `node lanes`
  text table gains an `UNREDEEMED` column between `OUTSTANDING` and
  `DEPOSIT`, which moves the positional columns after it.
- **Bundles: a manifest with an entry or chunk hash that does not parse is
  malformed, and serves no entry (#2561).** `BundleManifest::parse` checks
  the version first, then returns the new `MalformedManifest` error naming
  the entry and the hash. `decdn get` of the bundle or of any one entry, and
  the gateway's index and every entry, fail with it and say the publisher
  must publish a fixed manifest; they give no raw-blob hint, since the raw
  blob is the same broken JSON. Before, `get` of the bad entry failed, a
  bundle pull failed only that entry's group and landed the rest, and the
  gateway answered 404 for that entry while its index linked it.
  `decdn-client`: `ManifestEntry::hash` and `ManifestChunk::hash` are
  `[u8; 32]`, `ManifestEntry::new` and `ManifestChunk::new` take one, and
  `bundle::format_hash` writes the `b3:<hex>` form. `ManifestEntry` and
  `ManifestChunk` no longer implement `Deserialize`: `BundleManifest::parse`
  is the way from bytes to entries. A bundle pull groups entries by the
  parsed hash, so two spellings of one blob's hash (`b3:`, `0x` or no
  prefix; upper- or lowercase hex) fetch it once.
  `decdn get decdn:<hash>/ --dry-run --json` prints every entry hash in the
  `b3:<hex>` form.
- **`decdn-client` pools connections per node on one ALPN, on a map-chosen
  runtime (#2475).** `Connections::with_options(endpoint, alpn, dial_runtime)`
  builds a map whose every dial and connection driver runs on `dial_runtime`
  (`None` dials on the caller's runtime), `Connections::alpn` names its ALPN,
  and `Connections::get_or_dial` returns a `Lease` that says whether the call
  dialled the connection or took one already pinned. A pull on a map's
  connection that was pinned before the open and is closed under it redials
  once; a fresh dial that fails is not retried. `PeerSource::open_span` opens
  one range of a hash on the source's connection. `PeerSource::with_dial_runtime`
  and the `dial_runtime` argument of `open_progressive_pull` do not exist: a
  caller that pulls on a runtime it drops afterwards opens its pulls through a
  map with a dial runtime, and a map-less open dials on the caller's runtime.
- **`decdn-protocol`: `ProbeResponseExt::consistent_with` takes the signed
  `&ProbeResponseBody`, not `has_blob` alone (#2471).** The rule keys on
  `body.hash`: for `EMPTY_ROOT` (new constant, the BLAKE3 hash of zero bytes)
  the coverage must be empty and `has_blob` alone says whether the node serves
  the blob; every other hash keeps `has_blob ⟺ non-empty coverage`. On the
  wire, a node signs `has_blob: true` for the empty root, which an older
  requester rejects as malformed. The fix is under Fixed.
- **`decdn-client`: `BundleOutcome::replaced` is a `Vec<PoolReplaced>`
  (#2519).** It lists every pool replacement the pull made, in order, a
  second-pass replacement too. Iterate it where code matched the `Option`.
  `BundlePuller::replacements` lists the puller's every replacement, a
  manifest fetch's and a cancelled pull's included, and
  `RecoveryGate::replacements` does the same for one gate.
- **`decdn-client`: `buyer_pool::open_pool` and `buyer_pool::top_up` return
  an `UnrecordedEscrow` beside the outcome (#2515).** `open_pool` returns
  `(OpenedPool, UnrecordedEscrow)` and `top_up` returns
  `(ToppedUpPool, UnrecordedEscrow)`. The `UnconfirmedEscrows` handle keeps a
  mined transaction pending until the caller resolves the guard:
  `recorded()` once its record or credit commits, or `untracked(err)` once it
  fails, which holds the escrow as untracked before it releases the pending
  entry. A guard dropped unresolved leaves the transaction pending. A caller
  that passes no handle can drop it. An `openPool` whose receipt holds no
  `PoolOpened` event is now held as untracked by `open_pool` itself.
- **`decdn-client`: `buyer_pool::open_pool` and `buyer_pool::top_up` take a
  `receipt_timeout: Option<Duration>` (#2433).** `Some` bounds the receipt
  wait. A lapse returns `OpenUnconfirmed` / `TopUpUnconfirmed` with the tx
  hash, because the transaction may still mine. `None` waits until the receipt
  arrives or its read fails. The CLI passes
  `decdn_incentive::tx::RECEIPT_TIMEOUT` (3 min) to both. The node passes it
  to both as well, and settles an unconfirmed open or top-up against the chain.
  `open_pool` also refuses to send while the `UnconfirmedEscrows` it is given
  already holds an open, and fails with that `OpenUnconfirmed`.
- **Admin/CLI: a lane's `counterparty` is its pool owner, not its voucher
  signer (#2484).** `admin_v1_lanes` filled `LaneSnapshot.counterparty` with
  the lane signer, so every `decdn get` lane (signed by a key derived for its
  data dir) and every `--capability` lane showed that key instead of the
  wallet that funds the pool. The node now names the owner (`getPool.owner`)
  from its event-fed pool view, then from an owner cache, then with one bounded
  `getPool` read per pool in neither, at most eight at once. A pool's owner
  never changes, so the cache keeps every owner it reads. The first
  `decdn node lanes` after a restart reads the owner of each carried-over pool.
  The read is separate from the serve path's pool resolver, so a failed read
  never makes the node refuse serves on that pool. **Wire-breaking:**
  `counterparty` is `Option<String>`, `null` when the read fails or finds no
  such pool. The node logs one warning per call that has failed reads, and
  reads those pools again on the next call. It never falls back to the signer.
  `decdn node lanes` prints `unknown` in that case, and `--json` prints
  `null`. `voucher_signer` is unchanged. To find a client by its funding
  wallet, match on `counterparty`; to find a lane by its signing key, match
  on `voucher_signer`.
- **SDK: an owned pool pays through `decdn_client::buyer_pool::OwnedPayer`
  (#2468).** `OwnedPayer::for_store(owner, store)` pairs the owner key with
  the lane key the store's salt derives; `OwnedPayer::sole(owner)` signs with
  the owner key alone. `self_owned_lane_ctx` and `issue_self_capability` take
  an `&OwnedPayer`, and `lanes::LanePayer::Owned` holds one.
  `buyer_pool::open_pool` and `opened_from_receipt` no longer take a signer,
  and `OpenedPool` drops its `ctx` and `capability`: build a lane on the new
  pool with `self_owned_lane_ctx`. `bundle::Funding::SelfOwned` names its key
  `owner`. `BuyerPoolStore` gains the required `lane_salt` method.
- **CLI: `decdn get <decdn: URI>` replaces `decdn fetch` and `decdn bundle
  pull` (#2449).** One `decdn:` URI names what to pull
  (`adr/appendix-bundles.md` § `decdn:` URIs), and `get` routes it:
  - `fetch --hash H -o file` is `get decdn:H -o file` (`-o -` streams to
    stdout as before).
  - `bundle pull --hash H -o dir` is `get decdn:H/ -o dir`.
  - `get decdn:H/a/b.txt -o dir` pulls the one entry whose manifest path is
    `a/b.txt`, matched exactly, to `dir/a/b.txt`.
  - `--namespace N` is `?ns=N` in the URI.
  - `bundle pull -i <manifest file>` has no replacement: publish the manifest
    (`origin import` writes it) and pull it by hash.
  - The bundle flags (`--include`, `--exclude`, `--select`, `--jobs`,
    `--max-lane-streams`, `--overwrite`, `--dry-run`, `--json`) are an error
    on an entry URI with `-o -`, and the entry filters are an error on an
    entry URI. On a blob URI they are an error too, except `--dry-run`, and
    `--json` with `--dry-run`.
  - `decdn:H/` on a blob that is not a manifest fails with a hint to drop the
    trailing `/`. `decdn_client::bundle::BundleManifest::parse` returns a
    typed `NotAManifest` for that case.
- **`decdn_client::buyer_pool::open_pool` and `top_up` take an `escrows`
  handle (#2418, #2491).** The argument before `receipt_timeout`,
  `Option<&UnconfirmedEscrows>`, holds the `openPool` or `topUp` from before
  its submit until the caller resolves the returned `UnrecordedEscrow`
  (#2515). Pass `None` when nothing can drop the
  future or its error. `UnconfirmedEscrows::pending` returns
  `EscrowUnconfirmed` entries (`Open` or `TopUp`).
  `BundlePullerBuilder::unconfirmed_escrows` hands a bundle pull the same
  handle. The fix is under Fixed.
- **Buyer-store API: delegate lanes (#2435).** `BuyerPoolStore` gains three
  required methods, `delegate_lanes`, `advance_delegate_progress` and
  `rebase_delegate_progress`, so an implementor outside the workspace must add
  them. In `decdn-client`, `ProgressWrite::apply` takes a `WatermarkRow` (the
  owner's pool row, or the delegate lanes of a deployment) in place of the
  owner address, and no longer takes a separate pool id: the lane key carries
  it.
- **Buyer metrics (#2405).** A lane-seed refusal now moves
  `decdn_node_pull_pool_open_failures_total` once, beside
  `decdn_buyer_lane_seed_failures_total`, so the open-failure total counts
  every cause. A dashboard or alert that reads the total now sees seed refusals.
  A buyer row that vanishes while a lane seed is persisted now refuses the pull
  and counts as a seed refusal; the pull no longer pays against a pool the store
  does not record. A store read fault under the seed lock also counts in
  `decdn_buyer_lane_seed_failures_total`.
  `decdn_voucher_nonce_gaps_total` is removed: vouchers carry no nonce, and
  nothing emitted it.
- **The `decdn` CLI ships as a container image, and the `decdn-node` image
  carries it too (#2398).** A release publishes two images, built as two stages of
  one `Dockerfile` from the same signed Linux archives:
  - `ghcr.io/decdn/decdn` / `decdn/decdn` (new): the CLI only, with `decdn`
    as entrypoint. Mount `~/.decdn` at `/home/decdn/.decdn`. The name was
    unused by any release; a local script still pinning it for the daemon
    now gets the CLI.
  - `ghcr.io/decdn/decdn-node` / `decdn/decdn-node`: the daemon as before,
    plus `/usr/local/bin/decdn`, so `docker exec <container> decdn node
    status` reaches the loopback admin port.
  - Release assets are per image. `image-digest.txt` becomes
    `decdn-node-image-digest.txt` and `decdn-image-digest.txt`; the SBOM
    `decdn-<version>-sbom.spdx.json` becomes `decdn-node-<version>-sbom.spdx.json`,
    and `decdn-<version>-sbom.spdx.json` now describes the CLI image. Each
    file has its own `.asc`.
  - `sign-release.sh`: `DECDN_DOCKERHUB_REPO` is the Docker Hub counterpart
    of `DECDN_REPO` (default `DECDN_REPO`), and both Hub names derive from it
    — set it to `decdn/decdn`, not `decdn/decdn-node`; the script refuses a
    value ending in `-node`. The Docker Hub repository `decdn/decdn` must
    exist before the next release.
  - `sign-release.sh` checks every promoted repository without credentials
    and refuses to publish while one is private. GHCR creates a package
    private on its first push, so each package must be made public once.
- **Pre-launch compatibility shims removed (#2367).**
  - Admin wire: `admin_v1_drain` requires its `DrainRequest` parameter with
    `wait_admin` set, and `admin_v1_evict` requires `dry_run`. A call without
    them gets `-32602 Invalid params`, and a refused drain does not fire.
    The positional wire form is `"params":[{"wait_admin":false}]`.
    `decdn node drain --wait` still refuses to poll when another drain without
    `wait_admin` fired first.
  - Storage: both buyer tables move to `buyer_pool_state_v1` /
    `buyer_pool_owner_index_v1` (record schema 1). **Existing buyer rows are
    not read:** on first run the store reads as empty. A node re-adopts its
    live pool from chain, and so does a `decdn fetch` / `bundle pull` client
    whose store and keystore are its own. A client whose store or keystore sits
    in a node's data dir does not adopt: its next buy opens a second pool, and
    the old one keeps its deposit. Recover it with `decdn pool list --all`, then
    `decdn pool close` and `reclaim`. The node no longer drops the
    `capability_v1` / `capability_v2` tables from `lanes.redb` at open.
  - Config: the startup warning for a set `DECDN_DELIVERY_CEILING` is removed.
    `decdn node doctor` no longer suggests unsetting an environment variable.
- **Monitoring-breaking: `monitoring/` moves to
  [`decdn/devops`](https://github.com/decdn/devops/tree/main/charts/decdn-node/files/monitoring).**
  The four Grafana dashboards and `prometheus-alerts.yml` live in
  `charts/decdn-node/files/monitoring/` there, and this repository no longer
  ships them. The four `decdn-node` tests that checked the dashboards and
  rules against the exporter are deleted, and nothing checks them in CI now.
  `adr_registry_names_are_exported` still gates the ADR metric registry. Import dashboards and load alert rules from
  the devops repository instead.
- **`decdn-cache` network origins bind to the runtime they are built on
  (#1675).** `HttpOrigin::new`, `HttpOrigin::new_with_user_agent`,
  `HttpOrigin::parse`, `S3Origin::new` and `S3Origin::from_parts` capture the
  current tokio runtime, and every request send runs there, so build them on
  a runtime that outlives every fetch. Each fails outside a tokio runtime, so
  a caller that built an `HttpOrigin` from synchronous code builds it inside
  the runtime instead. `S3Origin::from_parts` returns `anyhow::Result<Self>`;
  a caller adds `?`.
- **Buyer pool rows keep an adopted pool's redeemed spend, and both buyer
  tables move to `_v6` (#2292).** A row adopted from chain has no lanes, so
  the pool's `totalRedeemed` is its only record of what other lanes paid
  out. The row now stores it (record schema 4), so every later `decdn fetch`
  and `decdn bundle pull` that reuses the row counts it, not only the run
  that adopted the pool. A lane seeded from its on-chain watermark takes its
  own share out of that amount, so the pool spend is the tracked lanes plus
  what no tracked lane accounts for, and stays right as a seeded lane
  advances. A node adopts its pool the same way, and its low-water refill
  reads the same pool spend. **Existing buyer rows are not read:**
  `buyer_pool_state_v5` / `buyer_pool_owner_index_v5` become `_v6`, so on
  first run the store reads as empty, and a node or client re-adopts its live
  pool from chain.

- **`bundle pull --json` renames `deduped` to `reused` and adds `excluded`
  (#2190).** `deduped` counted whole-file reuse only, so beside a
  `range-dedup: spliced 12.7 GB` line a `0 deduped` count read as a
  contradiction. The field, the counts line and the `whole-file dedup` line
  now say `reused`, which pairs with `reused_bytes`. `excluded` counts the
  entries that `--include`/`--exclude` dropped or `--select` deselected, so
  every manifest entry appears in one count:
  `(4 fetched, 0 linked, 12 skipped, 0 reused, 1 excluded, 0 failed)`.
  Scripts that read `deduped` must read `reused`.

- **Buyer pool rows carry the chain id beside the `PaymentPool` address, and
  both buyer tables move to `_v5` (#2198).** The row tag was the contract
  address alone, but the same deployer nonce gives the same `PaymentPool`
  address on every chain, and `poolId = keccak256(owner, ownerPoolNonce)`
  repeats there too. A node or client repointed at another chain with its data
  dir intact therefore reused a row from the other chain as its own.
  `BuyerPoolState` now carries a `Deployment` (chain id and `PaymentPool`
  address, the type the seller store is stamped with, now in
  `decdn-incentive`), and every path that checks the tag compares both: node
  bootstrap, the node's pull hot path, and `decdn fetch`. The bootstrap and
  reuse WARNs add `foreign_chain_id` and `configured_chain_id`. **Existing
  buyer rows are not read:** `buyer_pool_state_v4` / `buyer_pool_owner_index_v4`
  become `_v5` (record schema 3), so on first boot the store reads as empty and
  both a node and a client re-adopt the live pool from chain. A deposit held
  by an orphaned row is recoverable only on the deployment it was opened on.
  `decdn pool list --json` and `admin_v1_pools` gain `chain_id`.
  `decdn_client::buyer_pool::open_pool` takes the `Deployment` in place of the
  voucher EIP-712 domain, and `BuyerPoolService::bootstrap` takes it in place
  of the `PaymentPool` address and the voucher domain.

- **`decdn fetch` and `decdn bundle pull` recover through one acquire loop,
  and a blob's size is a hint (#2239; #2223, #2215, #2214, #2225, #2213,
  #2218, #2230).** Both commands drive every holder of a blob from one loop.
  A node that faults cools (2 s, doubling to 60 s) and returns when its
  cooldown ends or it delivers a verified byte, so a holder that restarts
  mid-fetch still finishes the fetch. The manifest fetch of a bundle shares the
  same holder table and progress clock. A fetch stops on a fault only the user
  can fix (a voucher rejection, an origin blacklist, no affordable holder, a
  local disk fault), or when no verified byte arrives for the give-up window.
  A terminal has no window; a script gives up after 10 minutes. The give-up
  exits with code 75. A blob over `--max-blob-mb` ends only its own item. The
  new `--give-up-after-secs` sets the window. Config-breaking, removed with no
  alias: `--stall-timeout-ms`, `--unit-deadline-ms`, `--min-throughput-bps`,
  `--multi-source-min-bytes`, `--multi-source` / `--no-multi-source` and
  `--entry-retries`. A lane's fixed 10 s watchdog replaces the stall and
  throughput floors. The size of a blob is now a hint. The first claim comes
  from the nearest probe answer (or one holder's header, or the manifest
  `size` of a bundle entry), every claim is clamped to `--max-blob-mb`, and
  each leg verifies under the size its sender signs. A verified final chunk
  proves the size. Until then the bound grows by `max(bound + 64 MiB, first
  claim + 2 * verified bytes past it)` and shrinks when a leg proves less. A
  disagreeing size no longer ends a fetch, and the client's size-consensus
  rule is removed. A bundle entry whose blob differs from its manifest `size`
  succeeds and prints `<path>: manifest says X bytes, the blob is Y bytes`.
  Wire-visible: a node serves a range up to the blob's end and refuses
  (`RangeNotSatisfiable`) only a start at or past the end, on every serve tier.
  The client's ranged store is keyed by offset, not by size, so a partial
  written under one claim resumes under another. A finalize whose whole-file
  hash does not match fetches the blob once more before the item fails. One
  command keeps one connection per node for all of its entries and lanes. When
  a lane faults and no idle lane covers its remainder, a busy lane that covers
  it takes one extra stream for it (#2231). Config-breaking:
  `--max-lane-streams` takes 1 to 64; 0 and values above 64 are refused.
  `decdn-client` SDK breaks: `PeerSource::new` and
  `IngestStore::ingest_stream` take new parameters, `IngestStore` gains
  `proven` and `set_bound`, `SourceSet::record_fault` takes the faulted
  `LaneRange`, `ClientRangedStore::open` takes no size and
  `ClientRangedStore::create` takes a bound hint. Removed:
  `SignedSizeMismatch`, `NoSourceAgreesOnSize`, `DownloadTarget::size_signer`,
  `Fault::WrongSize`, `SourceSet::mark_wrong_size`,
  `SourceSet::set_size_signer`, `ClientRangedStore::discard` and
  `ClientRangedStore::tree`. Added: `Connections`, `LaneWiden`, `LaneRange`,
  `StreamCandidate::widen`, `Probed::total_bytes`,
  `ClientRangedStore::{bound, proven, set_bound}`, `AcquireEnv::max_blob_bytes`
  and the `Downloader::max_blob_bytes` and `Streamer::max_blob_bytes`
  builders. ADR 039, ADR 005, ADR 037 and the bundles appendix describe the
  behavior.
- **`decdn-client` lanes carry what they hold while they stream (#2208).**
  `RangeLane` gains `release: Option<LaneRelease>`, which the range driver
  calls as each gap worker stops. `StreamCandidate` gains `lease: LaneLease`
  and `SourceLane` gains `lease: Option<&LaneLease>`; the scheduler drops the
  lease when that lane's worker stops. A caller that builds these structs
  sets the new field (`None`, or `LaneLease::default()`).
- **`decdn-client` dials on a caller-chosen runtime (#2185, #2475).**
  `WarmConnection::connect` takes a `dial_runtime: Option<&Handle>` argument,
  and a `Connections` map built with `with_options` dials every connection on
  its own dial runtime; `None` dials on the caller's runtime. A caller that
  pulls on a runtime it drops afterwards opens its pulls through a map with a
  long-lived dial runtime, so every connection's QUIC driver outlives the pull.
  `DialObserver` and `PeerSource::with_dial_observer` do not exist.
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
  with the framing layer's `MAX_CHUNK_FRAME` (one `CHUNK_BYTES` payload) as the
  effective ceiling.
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
  runs the daemon, so it is not named after the repository. No release had
  been cut under the old name, so nothing existing breaks. The repository
  name `decdn` now belongs to the CLI image (see the container-image entry
  above).
  - The mirror is a manifest copy, not a second build: `sign-release.sh` uses
    `docker buildx imagetools create`, which copies the manifest bytes verbatim,
    so both registries serve one identical digest and the existing GPG signature
    over the image's digest file covers both. The script re-reads every tag on
    both registries and refuses to publish the release if any resolves to a
    different digest. The digest file records the GHCR reference.
  - New env overrides: `DECDN_SKIP_DOCKERHUB=1` (tag on GHCR only) and
    `DECDN_DOCKERHUB_REPO` (meaning as in the container-image entry above). No new Actions secret — the mirror happens on the
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

- **Client docs: clarify that a stopped multi-source pull pays only for verified bytes (#2803).** The multi-source ADR and client comments now distinguish verified bytes from frames that arrive beyond a stolen range split.

- **CLI: `decdn dashboard` keeps a row's progress bar or gauge in one place
  as its text changes (#2776).** A running pull's text changes width on each
  poll, and near the wrap point its bar switched between the right side and a
  line below on alternate 2 s polls. From the `sm` breakpoint up the bar now
  stays at the right and the text wraps inside its own column; below it the
  bar stays under the text, as before. The pool and daily-allowance rows get
  the same layout. A running pull's output path shows its last two segments,
  as the finished-pulls table does, with the full path on hover, and breaks
  inside its column.

- **sponsord-onramp: the gate page tells a refused check apart from a service
  fault (#2720).** The built-in page showed "Something went wrong. Please
  refresh and try again." for every failed `POST /v1/fund` other than a `409`.
  It now asks for a refresh on a `403` (the check refused the token), asks the
  person to wait a minute on a `429`, and says the service has a problem on
  any other status or a network failure, where a refresh does not help.
  `docs/sponsord/operator.md` lists the statuses a custom gate page handles.

- **Tests: three load-sensitive tests model the node and the planner exactly
  (#2663, #2723).** `cli_bundle_pull_stripes_a_complement_across_two_holders`
  seeds `a` on one holder, so the planner never splits it across both lanes
  and duplicates its proof path. `over_cap_stream_is_reset_without_signing_a_response`
  writes its second request in one `write_all` and accepts the cap-full
  `STOP_SENDING` as the expected signal.
  `throwaway_open_torn_down_before_real_open_still_serves` counts the
  serviceability probe apart from the origin draws.
- **Transport: iroh and noq stay at 1.3.0, and CI holds them there (#2585).**
  noq-proto 1.4 closes a connection with "too many gaps in stream buffer"
  once the reader falls ~2.4 MiB behind, so a paid pull dies mid-leg and the
  node's credit-window bytes go unpaid. A lockfile refresh had moved iroh,
  iroh-base, iroh-dns and iroh-relay to 1.4.1 and noq, noq-proto and noq-udp
  to 1.4.0; all seven are back at 1.3.0, and the other updates stay. The
  `packaging` CI job and the `iroh-hold` pre-commit hook now fail when
  Cargo.lock resolves any of the seven to another version
  (`.github/scripts/check_iroh_hold.py` holds the table). The hold lifts once
  a noq release ships its quinn#2814 port (n0-computer/noq#866).
- **Client: a stream that a lost connection, a stall or a cancel ends bills
  its delivered tail when the lane resumes (#2718, #2722).** The node streams
  up to one credit window ahead of the voucher that pays for it. Only a
  funding refusal kept the delivered-but-unpaid span on the lane: any other
  early end re-queued the missing bytes alone, and the next stream opened at
  the delivered frontier, so the span was never billed. A QUIC connection
  that closed mid-stream under load left about 2 MiB unpaid this way in
  `cli_fetch_topup` and `cli_bundle_pull_range_dedup`. Now every early end
  keeps the span owed, priced from the lane's settlement, so a voucher armed
  when the connection dropped counts as paid. After a delivery fault or a
  cancel, the missing bytes are queued again from the paid frontier, and one
  stream bills the span and fetches the rest, so the proof path at the seam
  is paid once. After a funding refusal, the span still bills in its own
  stream first. A stream that re-bills a span and pays nothing past its start
  queues the rest from the delivered frontier, so a source that keeps failing
  at one point cannot stop progress. ADR 003 § Funding recovery states the
  rule.
- **Client: a fetch whose first lanes are still building runs no probe round
  (#2677).** The acquire loop asks for a discovery when a missing range has
  no holder to serve it. It judged that against running lanes only, so on
  its first pass, with every lane still connecting or building, every range
  read as uncovered. A peer-store fast-path fetch then read the registry and
  probed every holder mid-transfer, although the store had routed it without
  a probe. A range a lane still connecting or building covers now counts as
  covered, so discovery runs only for a range no running or building lane
  covers, or when no source can start. ADR 012 and ADR 039 § Source set and
  selection state the rule.
- **Client: the lane that answered the size open plans its first leg alone
  (#2740).** A fetch with no size hint keeps the pull it opened for the size
  on the lane that answered, for that lane's first leg to adopt. The lane
  reached the acquire loop as a build, and the loop plans the work across
  every build that has finished when it looks. When another holder's lane
  built just as fast, the loop split a download between the two lanes. The
  answering lane's first leg was then part of the blob, so it closed the kept
  pull after the holder had written up to a credit window into it, and the
  legs pulled those bytes again: a small blob was served twice. The loop now
  starts a lane its provider built before the fetch
  (`SourceProvider::built`) before it polls any build, so that lane is
  planned alone and its first leg is the pull it keeps. ADR 039 states the
  rule.
- **Node: a recovery `topUp` whose nonce the redeemer took is sent again
  (#2721).** The node signs its sellers' `redeemMany` and its buyer `topUp`
  with one key. A redemption that mined at the nonce the `topUp` had read
  made the `topUp` fail `nonce too low`. The settle proved from the chain
  that nothing escrowed, but the funding recovery step took that as final,
  so the node-to-node fetch failed. The funding task now sends a fresh
  `topUp` at a later nonce, at most twice, while it keeps the top-up slot.
  Every copy of the lost `topUp` was signed at the spent nonce, so none can
  mine later, and the fresh one cannot escrow twice. Each retry ticks the new
  `decdn_buyer_topup_nonce_lost_total` counter.
- **CLI, client, node and sponsord: a transaction goes out with gas headroom
  above its estimate (#2730).** `eth_estimateGas` prices a call at the pending
  block's timestamp, and every wallet provider sent at that raw estimate.
  `CapacityBond.declareMbps` and `deregisterNode` overwrite the operator's
  last tier checkpoint in the same second, and push a fresh slot in a later
  one, which costs about 22k gas more. So a `decdn node bond --mbps`, `unbond`
  or `deregister` estimated in the second of the operator's last checkpoint
  ran out of gas when it mined in the next second. The transaction reverted,
  and the `unbond --all` hint wrongly blamed registration. A node's redeem
  had the same exposure at an epoch roll: `FeeRouter` keys its byte counters
  by epoch, so a redeem mined in a later epoch than its estimate writes two
  fresh slots.
  `decdn_client::provider::build_provider`, the node's buyer and seller
  wallets and sponsord's treasury provider now fill gas with
  `decdn_incentive::tx::GasHeadroomFiller`, which adds `GAS_HEADROOM` (50,000)
  to an estimated limit. A caller-set limit goes out unchanged. Unused gas is
  refunded.
- **`decdn-e2e`: a fixture never releases a port before its child binds it
  (#2729).** Parallel journeys no longer fail at bring-up with `Address
  already in use`. `anvil` binds port 0 and the fixture reads the port from
  its `Listening on` line. The daemon's admin and metrics ports and the
  gateway and dashboard ports are held by a `ports::PortReservation`, a
  `SO_REUSEADDR` socket that never listens, for the child's lifetime, a
  `NodeFixture::restart` included. Linux hands a held port to no `bind(…:0)`
  and to no `connect()`, and the child still binds it. The daemon's QUIC port
  is a `ports::UdpPortClaim` below the kernel's ephemeral range, locked across
  fixtures by a TCP socket on the same number. `free_ports` and the retry of
  an anvil that died at startup are gone. The `decdn-node` anvil tests and the
  `sponsord` binary anvil test hold their ports the same way.
- **e2e: `NodeFixture` counts a health answer only from its own daemon
  (#2738).** `wait_healthy` compares the `node_id` in `admin_v1_health` with
  the key in the fixture's data dir. The admin port is picked free and released
  before the daemon binds it, so a daemon from another test process can hold
  it first. The wait then returned on that daemon's answer, and the journey
  asserted against the wrong node: `g_node_04::global_blacklist_compliance`
  saw a dry-run `evict` report `was_present: false` for a blob its node held.
  The wait now keeps polling until its own daemon answers. If that daemon exits
  first, the error names the node that held the port.
- **`decdn-node` tests: `over_cap_accept_prunes_refilled_keyspace` holds on
  a loaded runner (#2733).** The test fills 8 per-source buckets and expects
  none to refill before the loop ends. A 1 ms refill gave the loop a 1 ms
  margin, so a preempted loop lost a bucket to the over-cap sweep. The refill
  is now 100 ms and the wait 400 ms.
- **`decdn-e2e`: the G-NODE-07 iroh rotation stops the daemon before it
  rotates (#2727).** The journey rotated with the daemon running, then
  restarted it with SIGKILL. A pre-rotation voucher the hint path had not sent
  yet then waited for the restarted daemon's next full redeem interval, past
  the 90 s settlement poll. The journey now sends SIGTERM first, as the
  runbook's "Stop" step does, so the final redeem sweep settles it.
- **`decdn-e2e`: the bundle multi-source journey splits its large entry on the
  first pull (#2741).** The entry was 640 KiB. Its probe round starts the
  fetch at the first holder that answers, and that lane takes the whole blob.
  The other holder joins a few milliseconds later, but ADR 039 splits no range
  below `min_split_size` (16 MiB) for a late lane. So a pull split the entry
  only when both holders answered before the fetch started (6 of 82 pulls in a
  local sample), and the 90 s retry ladder sometimes ran out. The entry is now
  64 MiB, so the late holder steals half of it.
- **Client: a streamed fetch with no size hint keeps the pull it opened for
  the size (#2644).** With no size hint (`decdn get -o -` on the peer-store
  fast path, or a gateway pull whose size lookup finds none), the client read
  the size from a whole-blob open and primed that pull for the first leg. A
  streamed drive's first leg is cut to the read-ahead window, so it never took
  the pull. It closed the pull and opened offset 0 again, and the node had
  already written into the closed pull for up to a round trip. The size open
  of a streamed fetch now pulls the streamed drive's first leg, which the
  drive adopts. A download still opens the whole blob. ADR 039 states the
  rule.
- **Client: a funding recovery step replaces a stored pool the chain has no
  record of (#2665).** `getPool` on an unknown pool id returns a zeroed
  struct: a zero owner, and the status `Open` (enum value 0). The step read
  that as another owner's pool and tried a `topUp`, which reverts
  `NotPoolOwner`, so the fetch ended "funding needed" behind an opaque revert.
  On a full pool it settled on a pool that does not exist instead. The step
  now opens a replacement, as for a closed pool, unless a run of the same
  command opened the pool, topped it up, or read it on chain as this wallet's.
  Such a pool reads as unknown only on a lagging RPC, and a replacement would
  escrow a second deposit beside it. The step decides under the run's `topUp`
  lock and refuses while a `topUp` of the command is unconfirmed. The run's escrow handle (`UnconfirmedEscrows`)
  records the pools it saw live. ADR 003 § Funding recovery states the rule.
- **Client: a fetch the node stops for a short deposit or a spent signer cap
  bills its whole credit-window tail when it resumes (#2629).** When the
  node's mid-stream solvency check stopped a stream with `PoolExhausted` or
  `SignerCapExhausted`, the client rewound its last proof, although the node
  had accepted it. The resumed stream re-sent that proof, drew
  `AmountRegression`, and reseeded to the node's watermark. The reseed then
  marked the whole delivered frontier paid, so the bytes delivered past the
  node's watermark (up to one credit window) were never billed. The client now
  rewinds a proof only when the reason refuses it
  (`VoucherRejectReason::refuses_proof`, new in `decdn-protocol`). A reseed's
  jump no longer counts as payment for the leg it interrupts, since it can be
  the node's watermark from earlier blobs, and a stale heal keeps the leg's
  anchor. The next leg re-delivers and bills what the leg's own vouchers did
  not pay for.
- **Client: a funding recovery top-up covers the pool's overdraft (#2629).**
  The node's mid-stream solvency check reads the on-chain balance, which moves
  only on redemption, and runs on a cadence, so a fast stream can sign
  vouchers past the deposit before the check trips. The recovery step
  topped up only toward the working deposit, and the node then redeemed the
  overdraft out of that top-up and kept refusing the pool, so the fetch ended
  "funding needed" with a funded wallet. The step now adds the overdraft (the
  pool's spend past its on-chain deposit) to the top-up, taking the larger of
  the recorded and the on-chain deposit so a lagging read invents none. When
  the pool read fails, it takes the overdraft from the run's own lanes and the
  stored deposit.
- **Gateway: the first request for a raw blob has its length, `HEAD` and
  `Range` too (#2645, #2616).** A raw blob that no pull on the gateway had
  verified streamed with no `Content-Length`, ignored `Range`, and its
  `HEAD` had no length. Before it answers such a blob, the gateway now
  proves its size: it pays for the last chunk group under the first node's
  size claim, and only the true size verifies a final chunk (ADR 039). The
  proven size gives the response its length, admits it against the
  allowance (402 when it does not fit), caps its pull and lets one `Range`
  through (206, or 416 past the end). A proof costs at least one chunk group
  (16 KiB and its proof) per blob of unknown size, counted against a public
  gateway's allowance, a request dropped mid-proof included. A cold `HEAD`
  pays for its proof too: it now waits through discovery and a paid open,
  and answers as its `GET` would when the proof cannot run (402 once the
  allowance is spent, 502 when no node holds the blob, 503 when the pool
  cannot pay). A blob whose proof proves nothing (a claim whose last chunk
  group is not the blob's last, a claim past `--max-blob-mb`, or every
  holder fails it) still streams with no length; a node's claim never gives
  one. `decdn-client` gains `Streamer::prove_size`.
- **Gateway: a raw blob the gateway pulled whole answers with its length,
  `HEAD` and `Range` (part of #2616).** `GET /<hash>` on the bare host and
  `/_decdn/<hash>` on a site streamed with no `Content-Length` and ignored
  `Range`, and a `HEAD` had no length, even for a blob the gateway had
  already verified. A raw blob that a whole pull on the gateway verified now
  gets `Content-Length` and `Accept-Ranges`, takes one `Range` (206, or 416
  past the end), and is refused with 402 before its pull when it does not
  fit the allowance, as an entry is; its pull is capped at that size. A
  node's size claim is not used, since a claim below the real size would
  end a short pull as a whole response; the size proof above (#2645) gives
  the first request its length. `HEAD /_decdn/<hash>` on a site answered
  404; it
  now answers as the bare host does, and `HEAD` of a bundle or an entry
  under `/_decdn/` redirects to its site.
- **Client: a download adopts the stream it opened for the blob's size
  (#2633).** A fetch with no size hint (the peer-store fast path, or a
  pinned node whose probe gave no size) opened the whole blob to read its
  signed size, then dropped the stream unread, after the node had already
  written up to a credit window into it. The stream is now kept on its lane
  for up to 2 seconds, and a download whose first leg is the whole blob
  adopts it. Any other first leg (a resumed download's gap, a streamed
  fetch's window) closes the kept stream before it opens its own, so the
  node never holds the floor it reserved for both at once (#2644 tracks
  streamed fetches). The peer-store e2e checks that
  a fast-path download makes the holders write the blob's wire bytes once
  and probe nothing.
- **Gateway: a blob that is no bundle manifest is pulled once (#2478).** A
  `/<hash>/` or `/<hash>/<path>` request for a blob over the manifest cap,
  one whose bytes do not parse, or a malformed manifest answered 404 but
  pulled, and paid for up to the manifest cap, again on every request. The
  gateway now keeps up to 1024 such hashes for its run, and a repeat request
  answers the same 404 with no pull; its raw-blob hint names the asker's
  namespace. Each kept verdict holds at most 1 KiB of text, since a parse
  error quotes the publisher's paths and hash strings. A pull that failed
  upstream, on funding or on the allowance is not kept.
- **Gateway: a repeat bare-hash pull on a pinned gateway sends no probe
  (#2632).** A `decdn gateway` pinned with `--node-id` probed the node for
  the size of every bare-hash pull, and above about 5 pulls per second the
  node's per-peer probe bucket shed them, so those pulls fell back to the
  header-only open. The fetch host now keeps the verified size of up to
  4096 blobs it pulled whole, and a repeat pull takes it as the first size
  claim. The gateway e2e checks that repeat pulls of verified blobs probe
  nothing.
- **Gateway: `/_status` counts a capability's pulls in flight (#2640).**
  Each `pools.delegate_lanes` row gains `spent_micro_usdc` and
  `bytes_delivered`: per lane, the larger of the stored watermark and the
  live voucher ledger of a pull in flight, read before the store so a pull
  that ends during the read is not lost. `decdn dashboard`'s "Spent of
  cap" reads them and marks a capability with pulls in flight as live, so
  a long pull no longer reads as nothing spent until it ends.
- **Client: a pull pinned to one node takes its size from a probe, not
  from a stream it drops unread (#2619).** A fetch pinned with `--node-id`
  (`decdn get` of a blob or a bundle, or `decdn gateway`) read its first
  size claim from the header of a whole-blob paid open, then dropped that
  stream; `decdn get` of a bundle did so only for the manifest and entries
  the manifest gives no size. The node wrote up to a credit window into
  it first, 1 to 2 MiB per pull in the gateway e2e, and counted it in
  `decdn_bytes_served_total`; there the node wrote 1.36 times the wire bytes
  of the pulls. A known size is now the first claim: a manifest's entry
  size, which the gateway also uses for the entries it serves. With no known
  size, the fetch probes the pinned node once on `cdn/probe/v1` (2 s
  timeout, no backoff on a shed probe, no wait for a direct path) and takes
  the size it reports as the hint. The header open stays only for a probe that fails, does not
  verify, or gives no size, such as a node that does not hold the blob yet.
  A rediscovery of a pinned node probes nothing. The gateway e2e checks that
  the node writes the wire bytes of its pulls and no more. The
  observability registry names `decdn_bytes_served_total` as bao wire
  bytes, the unit payment uses (content plus about 0.4% proof), including
  frames a requester never pays for; the gateway appendix says how it
  differs from `decdn_gateway_bytes_served_total`.
- **Node: `decdn node lanes` / `admin_v1_lanes` report what each lane is owed
  (#2337).** `LaneSnapshot.outstanding_micro_usdc` was the cumulative of the
  last signed voucher only. A hash-chain lane pays with preimages, which move
  only the verified frontier, so the field trailed the lane's claim by up to
  one chain, and read `0` on a lane's first epoch even while the node redeemed
  it. The field is now the lane's owed amount: the signed anchor plus the
  verified chain frontier, the same claim the redeemer submits.
  `settlement_eligible` is measured against that figure too, and its doc no
  longer says it predicts the next redemption: the redeemer gates on the
  unredeemed value per `redeemMany`, not on this per-lane bar.
  `seconds_since_last_voucher` also counts preimages. The delegated-capability
  e2e journey now checks that the node's owed figure stays within the
  registered cap and that the chain pays exactly that figure.
- **Node: an oversize `redeemMany` that gets a bare HTTP 413, or that reads
  as possibly broadcast, is split in half (#2575).** The redeemer halved a
  chunk only when the refused send's text named an oversize cause. A
  gateway's 413 with an empty body (`HTTP error 413 with empty body`) never
  matched, and a proxy that turns an upstream 413 into a 5xx made the send
  possibly broadcast, which the split never saw. In both cases the chunk was
  re-sent at full size on every sweep, and its lanes stayed unredeemed until
  the operator lowered `blockchain.redeem_max_vouchers_per_tx`. The split now
  reads the HTTP status as well as the text, and runs on a possibly-broadcast
  send too: redemption is cumulative, so if the full chunk already mined, its
  halves settle nothing more and cost gas only. The split warning carries the
  error and `send_outcome` (`send_failed` or `maybe_broadcast`). The halved
  send still counts into `decdn_onchain_tx_send_failed_total` or
  `decdn_onchain_tx_maybe_broadcast_total`, but not into
  `decdn_redemption_failures_total`; a split on every sweep means the cap sits
  above the RPC's body-size limit. An oversize send that cannot be halved
  further (one lane, or the split-depth bound) is a redemption failure, even
  when it reads as possibly broadcast, and its warning names the cap. A
  connection reset while the body uploads carries no size signal and still
  does not halve.
- **Node: every warning of a `redeemMany` or `reclaim` that did not land
  names the sending account as `signer` (#2576).** A send with no tx hash
  resolves only by comparing that account's pending and latest nonce
  (`cast nonce <signer> --block pending` against `--block latest`), and the
  warning did not name it.
- **Node: a `redeemMany` or sweep `reclaim` send that fails with no tx hash
  but may have broadcast is reported as possibly broadcast, not refused
  (#2568).** The node's wallet providers already carried `SendLegLayer`, but
  the seller's redeemer and the buyer's reclaim sweep read every failed send
  as "nothing issued". A send request that times out, resets or gets an HTTP
  5xx, and a send answered `already known`, `nonce too low` or `replacement
  transaction underpriced`, now log that the transaction may be in a mempool.
  Such a `redeemMany` counts into the new
  `decdn_onchain_tx_maybe_broadcast_total`, not into
  `decdn_onchain_tx_send_failed_total` or `decdn_redemption_failures_total`,
  and still parks a hinted lane until a sweep finishes clean. The claims stay
  for the next sweep either way: if the `redeemMany` mined, that sweep skips
  the lanes it paid; if it is still pending, the re-send is an on-chain no-op
  that costs gas only. A `reclaim` still counts into
  `decdn_buyer_reclaim_failures_total`. Alert on the new counter: an operator
  who alerts on `decdn_redemption_failures_total` no longer sees a send leg
  that keeps failing there.
- **Payments: the sponsord keeper re-sends a `topUp` whose submit returned
  no tx hash once its nonce is vacant (#2572).** A hold on such a top-up
  ended only when the treasury's confirmed nonce passed the nonce it was
  sent with, which never happens on its own when the transaction was not
  broadcast, so every further top-up waited for an operator's 0-value
  self-transfers; a `topUp` whose hash the node did not know was held the
  same way. The keeper now reads the pending nonce too for a top-up the
  node shows no transaction of and, when the confirmed and pending nonces
  both equal that nonce, re-sends the `topUp` at it and settles the hold by
  that receipt; two transactions at one nonce cannot both mine, so the
  re-send cannot escrow twice. A re-send that reverts `PoolNotOpen` ends
  the hold, since the pool accepts no funds; any other failure is retried
  next sweep. An operator steps in only when a transaction stays stuck at
  the nonce, or an earlier nonce is unfilled: a 0-value self-transfer at
  that nonce, priced above the stuck one, clears it.
- **Payments: a send the RPC node answers with `nonce too low` or
  `replacement transaction underpriced` is reported as possibly broadcast, and
  an `openPool` or `topUp` that gets either is not sent again (#2562, #2565).**
  A load-balancing RPC gateway that loses one backend's response and retries
  the same signed bytes on a second backend gets `nonce too low` once the
  first copy is sequenced, or `replacement transaction underpriced` while a
  same-key transaction holds the nonce on that backend and the first copy can
  still win it. `topUp` read either answer as a foreign nonce collision and
  re-sent on a fresh nonce, and `openPool` read it as a rejection and released
  its entry, so either could escrow twice. Both now return `TopUpUnconfirmed`
  / `OpenUnconfirmed` at the nonce, which the node settles against the chain
  and the CLI reports instead of retrying. A node whose seller redemption took
  the nonce resolves the settle as "escrowed nothing" once that redemption
  mines, and funds again on the next pull that finds the deposit below the
  low-water mark; the settle reads the chain every 5 s meanwhile, where the
  re-send cost a quarter second. Every other chain write reports both
  answers as possibly broadcast too, as for `already known` (#2432): `pool
  close` and `pool reclaim` warn that the tx may still mine, and so do
  `publish`, key rotation, bond, register, appeal, and the USDC `approve`.
- **CLI: `decdn gateway` answers 404, not 502, for a bundle with an entry
  or chunk hash that does not parse (#2479, #2561).** The manifest is
  content-addressed, so a bad hash never changes and nothing upstream
  failed. The manifest is malformed, so its index and every entry answer 404
  naming the bad entry, logged at info, and no pull for an entry starts. The
  gateway caches only a manifest that parses, so each request pulls, and
  pays for, the manifest again (#2478).
- **Payments: a chain read that fails before a transaction is signed is
  reported as rejected, not as possibly broadcast (#2431).** A wallet
  provider reads the pending nonce, the gas estimate, the fees and the chain
  id before it signs and sends; a timeout or a 5xx on one of those reads used
  to be reported as a send whose outcome is unknown. The sending providers
  (`decdn_client::provider::build_provider`, the node's wallet providers,
  sponsord's treasury provider) now carry `decdn_incentive::tx::SendLegLayer`,
  which marks a failure of the send request itself, so only that failure (or
  an `already known` answer) reads as possibly broadcast. A send request the
  gateway refused with a 4xx that carries `Retry-After` (a rate limit), and
  one that never connected (a refused connection, a DNS or TLS failure), read
  as rejected too. `pool close --all` /
  `reclaim --all` count a pool whose reads failed as `failed`, `openPool` /
  `topUp` hold no unconfirmed escrow for it, the USDC `approve` does not say
  it may be pending, and `publish`, bond, register, appeal and key rotation
  report it as `rejected`. An SDK host that builds its own provider for the
  `decdn-client` senders must add the layer, or every transport failure reads
  as "nothing sent".
- **Payments: a send the RPC node answers with `already known` or `known
  transaction` is reported as possibly broadcast, not rejected (#2432).** The
  node returns it when it already holds the same signed transaction, for
  example after a gateway retried the send on a second backend. `pool close`
  and `pool reclaim` now warn that the tx may still mine, `--all` counts the
  pool as unconfirmed, and `openPool` / `topUp` keep the unconfirmed escrow
  record, so `topUp` holds it at its nonce. Every
  other chain write reports it as possibly broadcast too: `publish` namespace,
  seat and revoke statuses, key rotation (which keeps the parked key), bond,
  register, appeal, and the USDC `approve`. A send with no tx hash now says
  the transaction may be in a mempool, and the `approve` note says a
  re-approve to the same value is harmless.
- **CLI: a bundle pull lands no entry outside its output directory, and
  applies `[client] region_allowlist` to every entry (#2439).** A manifest
  path is checked for `..` and absolute components, but the write followed a
  symlink in the entry's parent directories: with `out/link -> /elsewhere`
  and an entry `link/file`, the pull wrote `/elsewhere/file`. The pull now
  holds the output directory open and resolves each entry beneath that
  handle (cap-std). Such an entry fails with a message that names the path;
  when the escaping link already exists, it fails before its blob is paid
  for. The rest of the pull goes on, and a fetched blob stays in
  `.decdn-partial/` for a rerun. An entry already present behind such a link
  is still skipped. A relative symlink inside the output directory still
  works; one with an absolute target is treated as an escape. Each entry's
  candidate sample now keeps to `region_allowlist` with the same widening as
  a single fetch, and a list the peer store supplied during a registry
  outage is not narrowed.
- **DHT: a bonded peer that is offline costs a lookup one dial budget, once
  per cooldown, not the 8 s round on every lookup that picks it (#2532).**
  The registry seeds every bonded peer into every routing table, and a bond
  does not say whether the node behind it answers. One such peer resolved an
  address from the registry mirror, so every `FIND_VALUE` dial to it sent a
  handshake that nothing answered, and the single 8 s exchange timeout ran
  level with the 8 s round timeout: a third of the fleet's lookups waited the
  whole round, and nothing recorded which peer it was. The DHT client now
  bounds the connect and handshake separately at `DHT_DIAL_TIMEOUT` (2 s)
  and fails them as a typed `DialTimeout`; the lookup marks that peer in the
  routing table (`RoutingTable::mark_unreachable`), and this node's lookups
  skip it for `DIAL_FAILURE_COOLDOWN` (5 min): the seed is the nearest peers
  that are not skipped, a `closer_nodes` referral to a skipped peer is not
  followed, and a dead peer reached only through a referral is marked too.
  The peer stays in the table and in the
  answers this node gives; an inbound request from it ends the skip. The
  `dht_lookup` span records the `cooling` peers skipped, and
  `decdn_dht_lookup_dial_timeouts_total` counts the dials that timed out
  ([ADR 022 § Routing Table](adr/022-content-discovery.md#routing-table)).
- **Node-origin: a DHT answer that names only the requester falls back to
  the origin directory (#2534).** The serve-miss pull leg drops the node that
  sent the request from its candidates, because a node never pulls from the
  node it answers. The drop ran after discovery had chosen between the DHT
  answer and the directory fallback, so when the requester was the hash's
  only DHT provider (a partial holder asking for the blocks it lacks) the leg
  ended with no provider and refused `NotFound` with
  `source=dht discovered=0`, without reading the directory that lists the
  origin. Discovery now drops the requester from both the DHT answer and the
  directory list before it judges the fallback, so that answer reads the
  directory and the origin fills the serve
  ([ADR 022 § FIND_VALUE Flow](adr/022-content-discovery.md#find_value-flow-cache-miss--dht-lookup)).
- **Probe: a node advertises the empty blob, and a requester accepts the
  answer (#2471).** The empty (0-byte) blob spans zero discovery blocks, so
  its coverage bitmap is empty on every holder. The probe handler derived
  `has_blob` from the bitmap and signed `has_blob: false` for a blob every
  store holds and serves, and the requester's consistency rule
  (`has_blob ⟺ non-empty coverage`) rejected a `has_blob: true` answer for it
  as malformed. A client saw no holder, dropped the size hint, and opened a
  paid lane for zero bytes; a node pulling through for the empty root found no
  provider. Now the handler signs `has_blob: true` for the empty root when its
  presence check passes, and `ProbeResponseExt::consistent_with` takes the
  signed body: for the empty root the bitmap must be empty and `has_blob`
  alone decides. The rule keys on the signed hash, never on the unsigned
  `total_bytes` hint. `decdn_protocol::EMPTY_ROOT` names the hash. ADR 022
  § Consistency rule records the exception. The API change is under Changed
  (BREAKING).
- **Node-origin: the coverage-union probe round spans the empty root once one
  holder is admitted (#2526).** The ranged pull leg gathers holders until their
  coverage union covers every discovery block of the blob. The empty blob has
  zero blocks, so the block target stayed at zero and the union never spanned
  it: the probe round waited for every probe to answer or time out, the
  origin-directory lookup always ran, and a cached holder list never counted
  as a probe-cache hit. Now the union spans the empty root once one holder is
  admitted. The rule keys on the requested hash, never on the unsigned
  `total_bytes` hint, so a holder that reports a zero size for a real blob
  closes nothing.
- **CLI: a `decdn get` that ends before a `topUp` confirms names the tx
  (#2491).** A refill or a funding recovery step can still wait on its `topUp`
  receipt when the command ends: on a give-up after the no-progress limit, a
  Ctrl-C, a fault in a sibling bundle entry, or the 30 s drain of late lane
  builds after a fetch that succeeded. The command dropped the wait and named
  no tx. A give-up exited 75 ("rerun to resume"), a drain exited 0, and a
  re-run before the `topUp` confirmed escrowed a second top-up. Now a
  `topUp` is named on every such end, as an `openPool` is: a give-up with one
  pending exits 1, not 75, a command that otherwise succeeded exits nonzero,
  and a second Ctrl-C prints it. A `topUp` left unconfirmed also refuses every
  later `topUp` in the same command, across passes and fetches.
  `decdn pool top-up` now handles Ctrl-C as `decdn pool open` does: it names
  the `topUp` instead of dying with no output. A `topUp` whose submit
  returned no hash reads "returned no tx hash", not "failed in transport",
  since a submit dropped in flight holds no hash either.
- **Client: a late lane build that may have escrowed with no record fails the
  fetch (#2500).** During the 30 s drain of late lane builds, a build could
  send a `topUp` or `openPool` that mined, or may have mined, with no local
  record to credit it. The drain logged that error at debug only. A completed
  `decdn get` exited 0, a give-up exited 75, and nothing named a tx the
  receipt read had already cleared from the pending-escrow handle. Now such a
  build fails `acquire` with a `LaneBuildFault` that names the tx when the
  submit returned one, whatever the fetch's own outcome was. A fetch that
  completed fails. A fetch that failed keeps its error only as text, so its
  give-up cannot exit 75 and a `PoolReplaced` cannot start a second pass,
  either of which would invite a rerun that escrows again. Any other late
  build failure leaves the result unchanged. `decdn get -o -` and the manifest
  fetch of an entry URI also dropped the drain once the reader hit the end of
  the stream. They now wait for it through the new `StreamDrive::finish`,
  which runs a stream's drive to its end and returns the error the fetch
  ended with. A consumer that reads a stream to its end calls it before it
  reports success.
- **Client: no second escrow after an untracked one (#2506).** When a run's
  `topUp` mined and its local credit failed, the build failed as escrowed and
  untracked, but a sibling lane build of the same run could still escrow
  again. Builds of one fetch wait behind each other on the pool lock. The
  next one in line ran before the acquire loop saw the first one's error, or
  in the drain after it. That build read the uncredited row, saw the pool low,
  and sent a second `topUp`: the receipt read had already cleared the first
  one, so nothing refused it. A failed record after a mined `openPool` could
  let the next build open a second pool the same way. Now a run that holds an
  untracked `topUp` or `openPool` refuses every later `topUp` and `openPool`
  of that command, across passes, before its allowance read or send, from a lane build or a
  funding recovery step. The refusal names the held escrow and ends the
  command. It is not reported as a second escrow. An `openPool` that mined
  with no `PoolOpened` event for the owner also fails as escrowed and
  untracked (`EscrowUntracked`), not as a plain error: the acquire loop
  retried that lane build, and the retry opened a second pool.
- **Client, CLI: every end of a command names its untracked escrows (#2511).**
  The hold on a run's untracked `topUp` or `openPool` lived in the run's
  per-pass state, and no end of the command read it. A Ctrl-C during the 30 s
  drain of late lane builds dropped the fetch, and `decdn get` printed
  "interrupted; run the same command again to resume": the receipt read had
  already cleared the tx from the pending-escrow handle, and a re-run read the
  uncredited row and escrowed again. A bundle pull whose entry with the escrow
  was dropped could give up with exit 75 the same way. The hold also kept
  only the first such escrow, so a second one in a dropped entry was named
  nowhere. Now the command's `UnconfirmedEscrows` handle holds every
  untracked escrow (`UnconfirmedEscrows::untracked`, a new `UntrackedEscrow`
  with the tx; `EscrowUntracked::tx` reads it from the error). The hold
  outlives a dropped fetch, a dropped bundle entry and a pass, and every
  later pass on the handle refuses to escrow. `decdn get`, for a blob or a
  bundle, names each one on every end that does not already name its tx: a
  Ctrl-C says `interrupted: <escrow>` and still exits 130, a give-up exits 1,
  not 75, and a success exits 1. A second Ctrl-C names them too. `decdn pool
  open` and `decdn pool top-up` hold theirs too, and their own error names the
  tx, so it is named once. A `BundlePuller` pull that is not interrupted ends
  `BundleEnd::Stopped` while
  its handle holds one, with the escrow as its error and the pull's own stop
  as text, so a give-up, a pool replacement or "funding needed" cannot invite
  a rerun. An interrupted pull stays `Interrupted`, and its caller names what
  the handle holds through the new `BundlePuller::unconfirmed_escrows`.
- **Client: a bundle pull lets sibling entries finish their lane builds
  (#2509).** When one entry ended the pull (a give-up, a pool replacement, a
  fault that stops every entry), every other entry was dropped where it
  stood. An entry in the middle of a `topUp` or `openPool` lane build lost it
  before the receipt read and the local record, which left a tx for the
  operator to reconcile by hand. Now the pull halts the other entries. Each
  stops at its next wait or loop pass, starts no new lane build or funding
  recovery step, and waits up to 30 s for the builds in flight, as one fetch
  does when it ends. A recovery step already in progress runs to its outcome
  first. The pull waits up to 40 s for them all, then drops any entry still
  running, as before: the command names an `openPool` or `topUp` it left
  unconfirmed. A build that may have escrowed USDC no record credits wins
  over the stop: the pull ends on the escrow, with the stop kept as text, so a
  give-up does not exit 75. A halted entry, like a dropped one, counts as not
  run in the summary (#2517), and a pass that keeps an entry's earlier
  landing does so for a halted one too. The first open of a blob (its size
  from a header) waits for its lane build the same way on a give-up. A
  funding recovery step there runs to its outcome, so a give-up can land
  after the limit while the step finishes.
- **Client: a fatal lane build names every escrow in its batch (#2508).** The
  acquire loop takes every lane build that is ready as one batch. When one
  build in the batch was fatal, the loop returned at once and dropped the rest
  of the batch. Those builds were already out of the set the drain of late
  builds awaits, so a second build in the batch that may have escrowed USDC no
  record credits was named nowhere. A full disk that failed a lane's record
  write beside a mined `topUp` whose credit failed reported only the disk
  fault, and a re-run after the operator freed space escrowed again. Now each
  such escrow in the batch is folded into the error the fetch ends with, as
  the drain folds a late build's: the escrow is the error a caller downcasts,
  and the fault that ended the fetch survives as text.
- **Client: a lane build sends no `openPool` or `topUp` after its fetch ended
  (#2513).** When a fetch ends, it awaits its lane builds still in flight for
  up to 30 s. A build in that drain that had not yet read its pool could still
  run the proactive refill and send a `topUp`, or open a pool, for a lane that
  never runs. The escrow was recorded and withdrawable, but no fetch needed
  it. Now each fetch ends for its builds as its drain starts. A build that
  reaches its `openPool` or `topUp` after that sends none and reads no wallet
  balance. It fails, and the drain logs it at `debug`. The build checks before
  its allowance leg and again before the send, so a fetch that ends during an
  `approve` sends no `topUp` after it. A build already inside its `openPool` or
  `topUp` send (its nonce read, its submit) runs to its receipt, as before. The first open of a blob ends
  its builds the same way on a stop or an error. A re-fetch on the same
  lanes after a hash mismatch is a new fetch, so its builds can still escrow.
- **Client, CLI: an escrow stays named until its record commits (#2515).**
  `open_pool` and `top_up` released a mined transaction from the
  pending-escrow handle as soon as they read its receipt. The caller wrote the
  pool record or the deposit credit after that, with an fsync. During that
  write the transaction was in neither the pending nor the untracked list. A
  second Ctrl-C there exited at once, named no escrow, and left the record
  unwritten, so a re-run read an uncredited row and escrowed again. Now the
  handle keeps the transaction pending until the record commits, and a failed
  record holds it as untracked before it releases the pending entry. `decdn
  get`, for a blob or a bundle, `decdn pool open` and `decdn pool top-up` name
  it on every exit during the write. The pending line reads "was broadcast and
  no local record credits it", which holds before and after the receipt
  read.
- **Client, CLI: a halted bundle pull reports its replacement, the entries it
  did not run, and its pending escrows (#2517).** Three gaps remained at the
  end of a pull that one entry ended. First, the pull took its pool
  replacement only from the fault that ended it. A sibling entry's recovery
  step that replaced the pool during the drain, or a replacement that a late
  escrow folded over, went unreported, and `decdn get` of a bundle printed no
  "reclaim it with: decdn pool reclaim" note for the closed pool. The pull now
  reads the replacement from the recovery gate, and lists it in
  `BundleOutcome::replaced`. Only a replacement that ended the first pass runs
  the rest again. A later pull on the same `BundlePuller` starts as a pass
  against the new pool, with fresh lane and funding state: it runs no extra
  pass, and its recovery step tops up the new pool, not the closed one. Second, entries the pull did not run were in no count, so
  the summary did not add up to the entry count. The new
  `BundleReport::not_run` counts them, the summary line adds `N not run` when
  there are some, and `--json` carries `not_run`. Third, a `BundlePuller` pull
  that dropped an entry while its `topUp` or `openPool` receipt read was
  pending ended with only its own stop. Only `decdn get` named the tx, from
  the handle. A pull that is not interrupted now ends
  `BundleEnd::Stopped` on each pending escrow, as it does on an untracked one.
  `decdn get` names each one once.
- **Client, CLI: `decdn get` names every pool a replacement closed, on
  every exit (#2519).** Several paths printed no "reclaim it with: decdn pool
  reclaim" note for a closed pool that may still hold a balance:
  - a blob `get` took its replacement only from the error that ended the
    pass, so a late lane build's escrow folded over it hid it;
  - after a second replacement, in the pass against the new pool, the
    command named only the first closed pool, for a blob or a bundle alike,
    though it had funded the pool the second one closed at the full working
    deposit;
  - a Ctrl-C during a blob `get` dropped the note with the fetch;
  - a bundle `get` whose manifest fetch replaced the pool named it nowhere,
    and its error said the remaining work runs again, though nothing did.
  Now the recovery gate lists every replacement its steps make, and the
  command prints one reclaim note per closed pool as it ends, however it
  ends. A replacement runs the rest again only when it ended the first pass.
  A replaced manifest fetch says the new pool is current and the pull must
  be run again. An entry URI's fetch starts clear of the replacement its
  manifest fetch left, as a bundle pull does.
- **Client: a lane build drained after its fetch ended sends no `topUp`,
  and waits on no lock (#2520).** A drained build waited for the
  open-or-reuse lock (bundle-wide, or the payer's for a blob `get`) and the
  run's `topUp` lock before it checked the fetch end. A sibling that held
  either through its receipt wait kept the build waiting for the whole 30 s
  drain, which then logged "lane builds still running". Now a build queued on
  either lock ends at once with the fetch, and one that reaches its `topUp`
  send after the fetch ended is refused before the send. Neither sent
  anything, so neither escrows. A refill refused so logs that it was not
  sent, beside its "topping up" line.
- **Node: a top-up for one pool no longer joins a `topUp` for another
  (#2496).** The top-up slot did not record which pool its `topUp` funds. When
  the store replaced the pool while a `topUp` for the old one waited for its
  receipt, a recovery top-up for the new pool joined it. It claimed the old
  pool's refill as its own headroom, and when that `topUp` escrowed, the
  recovery failed as escrowed and untracked. A fill could also wait on a
  sibling's recovery top-up for the old pool. It then failed, or, when that
  `topUp` reverted `PoolNotOpen`, replaced the new pool, which was still open.
  The slot now records its pool. A recovery step that finds the slot funding
  another pool fails at once as a local fault, names both pools, and counts on
  `decdn_node_pull_recovery_step_refused_total`. A low-water refill does
  nothing meanwhile.
- **Node: a funding recovery step fails at once while a `topUp` settles
  (#2494).** A recovery top-up that found the top-up slot held for an
  unconfirmed `topUp` joined it and waited for the whole settle, which lasts
  as long as the chain takes. Each such cache miss waited until its deadline
  dropped it. The recovery step now fails at once as a local fault, like a
  miss that finds the open slot held, and counts on
  `decdn_node_pull_recovery_step_refused_total`. A recovery step that joined
  before the settle began still waits for its outcome, and a low-water refill
  still does nothing and no longer logs that it funds. A recovery top-up whose
  earlier join already landed returns that headroom when a later funding call
  finds the slot held.
- **CLI: a transaction dropped from the mempool no longer hangs the command
  (#2433).** alloy's receipt wait has no bound, so a broadcast transaction
  that never mined left these commands waiting forever: `decdn pool {open,
  top-up, close, reclaim}`, `decdn get`'s pool open and top-up,
  `decdn node {bond, unbond, register, deregister, update-region,
  update-multiaddrs, rotate-key}`, `decdn publish`, `decdn appeal slash` and
  `decdn setup`. Under `pool close --all` and `reclaim --all`, a pool whose
  transaction was dropped also blocked every pool after it. Each wait now ends
  after 3 minutes (`decdn_incentive::tx::RECEIPT_TIMEOUT`, the bound the USDC
  `approve` already used). A lapse is reported like a receipt that could not
  be read: the error names the tx hash and says it may still mine, and a
  `--all` sweep counts the pool as unconfirmed and moves on. A transaction
  that is stuck rather than dropped holds its nonce, so each later pool's
  transaction queues behind it and lapses in turn. A `decdn get` whose
  `openPool` or `topUp` lapsed never sends a second one in the same run, so
  no other lane, bundle entry or funding recovery step escrows a second
  deposit beside it. The run sends one `topUp` at a time.
- **Node: an `openPool` dropped from the mempool is re-sent (#2433).** The
  node waited for the receipt of its own `openPool` with no bound, so a
  dropped open held the open slot forever, and every miss that joined it
  waited out its budget. The wait now ends after `decdn_incentive::tx::RECEIPT_TIMEOUT`, and the open
  goes to the settle loop, which re-sends it at the same nonce when the chain
  holds no transaction there.
- **Node: an unconfirmed `topUp` is settled against the chain, not
  forgotten (#2489).** A `topUp` whose receipt read failed, or whose submit
  failed in transport, freed the node's top-up slot. The next low-water
  refill then sent a second `topUp` beside one that could still mine. A
  recovery top-up that joined the failed one also sent its own. A dropped
  `topUp` held the slot forever instead, so the node funded no pool again
  until a restart. The receipt wait now ends after
  `decdn_incentive::tx::RECEIPT_TIMEOUT`. An unconfirmed `topUp` keeps the
  slot until the chain resolves it: its receipt credits the row, a nonce that
  no transaction holds gets a re-send at that nonce, and a spent nonce with no
  receipt credits what the pool's on-chain deposit holds above the row as it
  stood when the settle began. A re-send that reverts `PoolNotOpen` ends the
  settle once one block shows the nonce unspent and the pool closed, so the
  recovery step opens a new pool. A re-send that reverts on the allowance
  sends an `approve` at the nonce, which restores the allowance and fills the
  nonce. A settle that may have
  escrowed but cannot credit the row logs the "ESCROWED AND UNTRACKED" error,
  and so does a funding task that dies mid-settle, which no joiner then funds
  over. The
  hourly reclaim sweep's `reclaim` receipt wait is bounded the same way, so
  one dropped `reclaim` no longer stalls the sweep. `decdn-client` adds
  `buyer_pool::send_top_up_at` and `buyer_pool::topped_up_from_receipt` for
  the settle step.
- **CLI: two `decdn get` runs with separate `--data-dir`s on one wallet no
  longer pay on the same lanes (#2468).** A fresh data dir adopts the
  wallet's live pool, and both runs signed vouchers with the wallet key, so
  they shared each `(pool, signer, provider)` lane from separate watermarks.
  Providers rejected the run that fell behind as `UnderFold`, and the runs
  spent their resume budget re-fetching bytes elsewhere. Each client data dir
  now signs with a key derived from the wallet key and a random salt that its
  `buyer-pools.redb` keeps in the new `buyer_lane_salt_v1` table. The derived
  key is never stored, and the wallet key signs the capability that authorizes
  it. Data dirs keep sharing the pool, but each pays on its own lanes, so the
  `AlreadyOpen` advice to give each concurrent run its own `--data-dir` is
  safe on the wallet's own pool. Two data dirs on one `--capability` delegate
  key still sign with that key and share its lanes. A data dir tracks only its
  own lanes, so its funding reads the pool's deposit and `totalRedeemed` from
  chain at the run's first lane build and before each funding recovery step:
  a pool another data dir drained gets a top-up instead of a settle wait that
  ends the run "funding needed". The daemon still signs its
  own buyer lanes with the node key. An existing pool row keeps its
  wallet-signed lanes as spend; new vouchers sign on fresh lanes.
- **CLI: `decdn gateway` pays for at most 64 MiB to read a bundle manifest
  (#2474).** A `/<hash>/` or `/<hash>/<path>` request reads the manifest
  `<hash>` into memory. That pull ran under the gateway's `--max-blob-mb`
  (1 TiB by default), so for a large blob that is not a manifest, the
  gateway paid for 64 MiB plus the lanes' read-ahead before it failed. Anyone
  who could reach the gateway could spend the operator's funds this way. The
  manifest pull now runs under the manifest cap (64 MiB, or a lower nonzero
  `--max-blob-mb`), as `decdn get decdn:<hash>/<path> -o -` does, and its
  lanes request no byte past it. A blob over that cap, or one that does not
  parse as a manifest, answers 404 instead of 502, because nothing upstream
  failed. The 404 names the raw blob's gateway path when the gateway serves
  that blob.
- **CLI: a command that ends before an `openPool` confirms names the tx
  (#2418).** A Ctrl-C during the `openPool` submit or receipt wait in `decdn
  fetch` or `decdn bundle pull` printed only "run the same command again", and
  `decdn pool open`, or the `--hash` manifest fetch of `bundle pull`, died with
  no output. A give-up after the no-progress limit printed "rerun to resume"
  and exited 75. The tx can still mine and escrow its deposit, and a re-run
  before it confirms opened a second pool. Now every such end prints each
  pending tx hash, or the nonce when the submit returned no hash, says that a
  second open escrows again, and points to `decdn pool list --all`.
  - An interrupt still exits 130, and a second Ctrl-C names the opens before
    it exits. A give-up with an open pending exits 1, not 75, because 75 tells
    a wrapper that a re-run is safe.
  - A fault that ends the command, or a receipt read that fails while the
    command drains, names the tx the same way. A command that otherwise
    succeeded exits nonzero.
  - The `OpenUnconfirmed` message points to `pool list --all`, because only
    the on-chain list shows a pool the local store never recorded.

- **CLI: Windows builds embed the canonical operator terms.** A Windows
  checkout converted `crates/cli/TERMS.md` to CRLF, so `decdn.exe` hashed
  different bytes and `decdn node register` failed with an operator terms
  mismatch against the network's `currentTermsHash`. `.gitattributes` now
  keeps `TERMS.md` byte-exact, and the build fails if the embedded terms
  contain a carriage return.

- **A delegated client records its lane watermarks (#2435).** A client that
  pays under a `dcap1` capability on a pool another owner funds owns no pool
  row, so every lane watermark write it made was dropped with a warning. Its
  lanes now go to a new `buyer_delegate_lanes_v1` table in
  `buyer-pools.redb`, keyed by chain id, `PaymentPool` address, pool id,
  signer key and provider. A later `decdn fetch`, `bundle pull` or
  `decdn_client::bundle` run on the same data dir resumes each lane from its
  record instead of from zero, and the ADR 039 funding signal counts what
  earlier runs signed under the key. The pool tables are unchanged, so
  existing rows still read and no lane reseeds. A delegate-lane record the
  store cannot read fails the fetch as a local fault instead of retrying. The
  API changes are under Changed (BREAKING).
- **`decdn pool close` and `pool reclaim` clear the local row of a pool that
  is no longer Open (#2425).** When a `closePool` or `reclaim` mined but its
  run could not clear the client-store row (the clear faulted, or the tx's
  outcome was unknown), every re-run stopped at a status refusal or skip
  before it touched the store. The row survived, and a later fetch reused a
  closing or closed pool whose vouchers `redeemMany` rejects. Now each `close`
  or `reclaim` that reads the pool as `Closing` or `Closed` clears its row:
  `close --pool` and `reclaim --pool` on their refusal, and `close --all` and
  `reclaim --all` on their skip. The refusal or skip line says when it cleared
  a row. Exit codes for a `--pool` refusal do not change: it still exits
  nonzero.
  - The clear is compare-and-delete, so a row for a newer pool stays.
  - Every row-clear in `pool close` and `pool reclaim`, including the one
    after a landed tx, touches only a row on the command's chain and
    `PaymentPool`. A pool id repeats across deployments, so a command run
    against an old `--payment-pool-address` leaves the live row for the same
    id on the current deployment alone.
  - `reclaim --pool` clears the row with the pool's on-chain owner, so its
    refusal still asks for no keystore password. A local store that will not
    open (another `decdn` process holds it) only warns: the refusal still
    names its cause.
  - A skipped pool whose row-clear faults counts in a new `row not cleared`
    summary count, and the sweep exits nonzero, so a failed repair does not
    read as success.
  - On a daemon data dir the CLI leaves the daemon's row as it is and says so.
- **`decdn pool close --all` counts a landed close whose row-clear faulted as
  closed, not as failed (#2425).** The per-pool line no longer reads "failed
  to close" for a close that is on-chain. The pool counts in `row not
  cleared`, and the sweep still exits nonzero, naming the re-run that clears
  the row. `close --pool` names that re-run as well, and both put the store
  fault next to the word "failed".
- **`decdn pool close` and `pool reclaim` name the revert that gas estimation
  rejects, and no longer call it on-chain (#2426, #2417).** A `closePool` or
  `reclaim` that would revert fails at gas estimation, before anything is
  sent. The error now says so, names the `PaymentPool` error
  (`PoolNotOpen()`, `NotPoolOwner()`, `PoolNotClosing()`,
  `GraceWindowActive()`) with its likely cause, and keeps the raw revert data.
  A `reclaim` estimation revert with any other selector points at the USDC
  refund transfer to the owner.
- **`decdn pool close`, `pool reclaim`, and the USDC `approve` warn when a
  failed send may have broadcast (#2416).** A `closePool`, `reclaim`, or
  `approve` send that fails in transport (a timeout, a connection reset, an HTTP
  5xx) may have reached the node and broadcast the tx with no hash returned.
  The error now says so and tells the operator to check this signer's pending
  and mined transactions before re-running. The `approve` runs from `pool
  open`, `pool top-up`, `fetch`, and the daemon's buyer funding. A send the RPC
  node rejects outright carries no warning.
- **`decdn pool close --all` and `pool reclaim --all` count a transaction
  that may still mine as unconfirmed, not failed (#2427).** A send that fails
  in transport, or a broadcast tx whose receipt cannot be read, now adds to an
  `unconfirmed` count in the summary line. The final error names that count
  apart from the failed one and tells the operator to check this signer's
  pending and mined transactions before re-running. The exit code stays
  nonzero.
- **The daemon no longer escrows a second pool deposit after an `openPool`
  whose outcome it did not see (#2415).** When the `openPool` receipt read
  failed, or its submit failed in transport, the open task freed the open
  slot and the next cache miss sent a second `openPool` while the first could
  still mine. The open task now keeps the slot until the chain resolves the
  transaction, and only a receipt or a spent nonce releases it: a receipt
  yields the pool; a nonce spent at block `B` reads `getPools` at `B` and
  adopts the owner's newest solvent pool, or concludes the open escrowed
  nothing; a nonce the account has reached and no transaction holds gets a
  fresh `openPool` at that same nonce, so at most one of them mines. A
  reconciliation that cannot answer backs the next one off, up to 5 min. While the slot is held, a miss that
  needs a pool fails at once as a local fault, `decdn_buyer_pool_open_unresolved`
  reads 1, and a warning repeats every minute with the elapsed time.
  `openPool` is now sent with an explicit pending nonce, and its unconfirmed
  error carries the hash and nonce (`OpenUnconfirmed`). `decdn fetch` and
  `bundle pull` treat that error as fatal to the command, as for an
  unconfirmed `topUp`.
- **`decdn fetch` / `bundle pull` steal a slow leg's tail by pull-through
  (#2348).** A lane could steal only inside a range's covered suffix, so a
  range whose last block only one lane covered ran on that lane to the end,
  however slow it was: one 0.5 MiB/s leg ran alone for 35 min while every
  other lane sat idle. When no covered steal is due, a lane not barred from
  pull-through now steals the rate-weighted tail of a range on another lane
  that runs at a quarter of its rate or less, measured over at least 30 s.
  Its node serves the part outside its coverage by pull-through. A parked
  lane looks for such a steal every 5 s, and the steal logs at `info`
  (`kind="slow_victim"`).
- **`decdn fetch` / `bundle pull` with a spent capability stops with the
  owner-side remedy instead of retrying until it gives up (#2338).** A node
  refuses a registered signer whose `cap − spent` cannot cover its serve floor
  at admission, and answers with the same `NotFound` code as a cache miss. The
  client cooled the holder and retried until its stop policy gave up. A
  delegated lane now reads its signer's `getAuthorization` row when a node
  refuses it with `NotFound`, bounded by the open deadline. An expired
  registration, or one with nothing left of its cap, ends the command with
  `SignerCapDrained`. A `cap − spent` below one chunk at the refusing node's
  signed rate bars that node for the blob, since a cheaper node can still
  serve; once every node is barred the item ends with `NoSourceServesSigner`.
  Both ask the pool owner for a capability for a new signer key, as do a
  mid-stream `SignerCapExhausted` and, for a registered signer, a
  `SpendingCapExhausted`. A failed or slow read leaves the refusal as it was
  and logs at info. `SignerAuthorization` moves from `decdn-node` to
  `decdn_incentive::payment_pool`, so the client and the node test a
  registration with one predicate.

- **`bundle pull`: a freed provider stream goes to the lane that waited for it
  first, so one entry's queued range no longer waits minutes behind a sibling
  entry's short legs (#2341).** Entries share each provider's
  `--max-lane-streams` permits. A lane asks for an extra stream for a queued
  range once per growth pass, at most once a second after a refusal. A sibling
  lane whose short legs restart, or whose leg ends ask for growth, asked the
  instant a permit freed and so took every one. On testnet a bcl-only 1 GB
  range waited 450 s while a sibling entry opened 54 legs to bcl. A lane that
  `grow` refuses now claims the provider's next free permit. Each claim holds
  back one free permit from every later `grow` ask, and the largest kept
  permit count stays free after them. A lane that runs no stream goes ahead of
  the claims of lanes that run one, so every lane gets its first stream before
  any lane gets a second. A grant or the lane's end clears the claim, and a
  claim not renewed for 3 s lapses. A lane's first stream still takes any free
  permit.

- **Every voucher rejection logs its cause at `info` (#2342).** A rejected
  voucher or chunk preimage ends a paid stream and counts on
  `decdn_serve_stream_voucher_rejected_total`, which has no reason split. The
  node logs `rejecting a payment proof` once per rejection, with `proof`
  (`voucher` or `reveal`), `reason`, `pool_id`, `signer` and `with_watermark`.
  A voucher that fails the rate check writes no reject frame but counts on the
  same counter, so it logs the same line with `reason=RateCheck` and the error.
  Before, the reason logged at `debug` or not at all, so a node at the default
  level showed the count with no cause.

- **A serve miss keeps the lane answer it resolved, so a lane registered
  mid-request no longer makes it spend origin egress or answer a false
  `InternalError` (#2315).** `serve_stream` resolves the request's lane once.
  Each fill tier then re-read the live lane registry to decide whether the
  request may make the node spend. When a sibling stream registered the lane
  between the two reads, the window tier fell through to the size gate, logged
  a false "just-filled blob" warning and refused `InternalError`, so the
  requester routed around a healthy node. The local-origin populate tier
  (#1116) instead filled the blob from the operator's origin and then refused
  `UnknownLane`. Every fill tier now takes its answer from the request's one
  lane resolution. A request whose lane was unknown when it resolved refuses on
  the `no_lane` path (`NotFound`, counted as `cache_miss`) and runs no fill
  tier.
- **A lane seeded from the chain watermark below the provider's unredeemed
  voucher now heals instead of suppressing the provider (#2254).** After a
  buyer-table schema bump, a node adopts its pool with no lanes, and each
  lane seeds from `getWatermark` on its first use. The seed sits below any
  voucher the provider has not redeemed yet. The ledger prices its spans from
  that lower seed, so its amount passes the provider's anchor while its bytes
  still trail it, and the provider rejects `BytesRegression` with the anchor
  attached. The payer now rebases to that anchor, its own voucher, and
  resumes (ADR 005). Before, the pull ended and node-origin suppressed a
  healthy provider. A healed `BytesRegression` that keeps rejecting past the
  resume budget now cools only that source. Two streams that find one lane
  missing seed it once. Each lane has its own seed lock, and the watermark
  read times out after 10 s.
- **A sub-millisecond probe RTT no longer erases the price signal in
  candidate ranking (#2268).** The selection score multiplies the rate by the
  probe RTT in whole milliseconds, and a probe faster than 1 ms truncated to
  0. Every such peer scored 0, so two of them tied whatever they quoted, and a
  dear one outranked every cheaper peer at 1 ms or more. The score now uses an
  RTT floor of 1 ms (ADR 001), so peers on a LAN or in one datacenter rank on
  rate and reputation.
- **Origin requests keep their connections across serve-miss pulls, and HTTP
  origins reuse keep-alive connections again (#1675).** A serve-miss pull leg
  runs on its own current-thread runtime, which drops when the leg returns.
  A pooled origin connection opened there lived on that runtime, so a
  concurrent serve that reused it lost its in-flight request with "dispatch
  task is gone" (#1673). `HttpOrigin` avoided this by turning its keep-alive
  pool off, and paid a fresh TCP and TLS handshake on every request.
  `S3Origin` kept its pool and was still exposed. Both origins now capture the
  runtime they are built on, which is the node's main runtime, and run every
  request send there. Each pooled connection outlives the pull leg that opened
  it, and `HttpOrigin` reuses keep-alive connections. A request task that
  panics, or that the shutting-down main runtime cancels, fails as a
  permanent origin fault. The constructor changes are under Changed
  (BREAKING).
- **The deployment preflight rejects every sibling contract of the
  `PaymentPool` (#2279).** Startup identified `blockchain.payment_pool_address`
  by calling `usdc()`, which `FeeRouter` and `BuybackBurner` also answer. A
  node pointed at either one passed the preflight, then dropped its seller
  lane state and forfeited the unredeemed vouchers. The probe is now
  `PaymentPool.getRateBounds()`, which only `PaymentPool` declares
  (`feeRouter()` would not do: `DecdnGovernor` answers it too), so the node
  exits before the lane store opens. The startup error now names
  `PaymentPool.getRateBounds()`.

- **A reactive top-up of a few micro-USDC no longer goes on chain, and a
  refill reaches lanes already running (#2296).** Right after a refill, a
  node whose chain watcher had not seen it yet refused the next open as
  `InsufficientDeposit`, and the client topped the pool up by the few
  micro-USDC it was short of the working deposit: an `approve` and a `topUp`
  each, and one of the run's three reactive top-ups. Such a refusal while the
  deposit sits within the low water (working deposit / 5) of the working
  deposit now waits out the node's watcher on its own settle budget, and a
  confirmed exhaustion the deposit can still afford tops up only when it adds
  at least that low water. A node that keeps refusing past the budget faults
  as a source (it cools and is asked again, with one WARN), not as priced out
  by a deposit no top-up raises. A lane build that refilled the pool now raises
  every running lane to the new deposit, so a lane built before the refill no
  longer reads a gap the refill paid for as unaffordable.

- **A partial holder that refuses blocks its coverage claims stops being
  asked for them (#2281).** A `NotFound` for a range inside a probed partial
  holder's advertised coverage counted as a delivery fault only, so the
  holder was asked for the same range again every few seconds for the rest
  of the entry. Such a refusal on the lane's own stream now counts against
  every discovery block the refused part of the range touches. After three,
  the block leaves the holder's coverage for a minute, rediscovery included,
  and its ranges go to the other holders. A `NotFound` can also be a
  transient refusal, so the block then returns, refusals within one cooldown
  count once, refusals a minute or more apart never add up, and a verified
  byte in the block clears its count. An extra stream's covered `NotFound`
  does not count: it is most often the node's per-signer live cap.

- **A range-dedup entry that waits for a sibling's chunks no longer holds a
  `--jobs` slot (#2283).** `decdn bundle pull` kept the slot of an entry
  whose complement had landed while it waited for the sibling that fetches
  its donor chunks. On an SDXL pull with `--jobs 5`, such waiters held the
  run to 2–3 downloading entries for minutes at a time. A waiter now gives
  its slot to a queued entry while it waits, and takes one again as the wait
  ends, ahead of entries still to start. A pull's file bar appears once the
  entry holds a slot.

- **A partial holder steals the covered tail of a busy leg (#2303).** A
  freed lane stole from a busy lane only when it covered the victim's whole
  range in flight. In a `decdn bundle pull` of mistral-7b, one node held
  blocks 0–43 of a shard as one 2.95 GB leg. A lane that covered blocks
  18–45 went idle in seconds, and the leg ran alone for 8 min on one
  stream. A freed lane now steals inside the longest suffix of a range in
  flight whose blocks it covers: a split before that suffix moves to its
  start, and the victim keeps every byte before it. A lane whose blocks stop
  short of a range's last block still cannot steal from that range. A
  holder counts as having work, and starts, only when it would steal now:
  a late partial holder starts and steals, and a holder whose only option
  is a tail below the steal floors no longer takes a lane slot to park.

- **A queue that one lane drains takes an extra stream (#2252).** In
  `decdn bundle pull`, a lane's own worker takes queued ranges one after
  another and does not end between them, so no stream end asked for growth.
  The first growth pass ran as the lane started, before its worker held a
  range, and found no busy lane to ask. Nothing asked again, so a
  range-deduped entry's complement went to one holder as one serial request
  per run. While a range waits and a running lane can be granted streams,
  the acquire now asks again every second, also when no busy lane could be
  asked. A busy lane takes an extra stream for every waiting range that its
  provider has a permit for, not only one, and one growth pass fills every
  free permit. A lane that waits with nothing to take gives its permit back
  while it waits, so an idle lane of one entry no longer holds a stream that
  another entry's queue needs; it takes a free permit again before it takes
  a range. At `--max-lane-streams` 3 or more, an extra stream never takes a
  provider's last free permit, so a sibling entry's first stream to that
  provider still opens; a lane that starts again can take it. The info line
  `a queued range waits for a stream` logs on the first wait and then at
  most every 30 s while ranges wait, in place of a line on every change;
  each change between those lines logs at debug.

- **The client funds a buyer pool from its pool-wide spend, and a short
  wallet no longer drops lanes (#2288, #2289).** The low-water refill
  compares the deposit with the spend of every lane of the pool, not only
  the lane being built, so a pool spent across many providers refills. The
  reactive top-up and the check of a node's `SpendingCapExhausted` refusal
  use the same pool-wide view. When the wallet holds too little USDC for a
  refill, the fetch continues on the pool's remaining deposit, logs one WARN,
  makes no further refill attempts that run, and ends with one
  `warning: wallet … holds … µUSDC, less than the … µUSDC top-up …` line on
  stderr, visible without `-v`. A pool with nothing left and a short wallet
  ends the command with the top-up remedy. A `topUp` whose receipt cannot be
  read, or that mined but could not be recorded locally, now ends the command
  and names the tx instead of retrying, because a retry escrows again. A
  failed reactive top-up retries without cooling the serving node.

- **The node no longer faults a paying client whose bytes are still in
  flight (#2230).** A write returns once the transport buffers it, so the
  node's wait for a proof can start while several chunks are still on the
  way to the client. The wait now faults only after 10 s with no proof and no
  new STREAM frame sent on the connection. All the proofs for one owed chunk
  share a 30 s ceiling from the chunk's first wait, so proofs that credit
  nothing cannot stretch it. A client that stops reading faults at 10 s when
  its connection carries nothing else; traffic on sibling streams of the same
  connection counts as progress and can hold the wait up to the 30 s ceiling.
  A ceiling fault does not count on
  `decdn_node_pull_through_client_abandoned_total`. The fault text gives the
  measured times and the STREAM frames the wait saw, then the connection's
  selected path (relay or direct, IPv4 or IPv6) with its RTT and congestion
  window and the path's lifetime totals of lost packets, congestion events and
  black holes detected: `no proof and no transport progress for 10.0s; …` or
  `the chunk's proofs ran past their wait ceiling after 30.0s; …`. It replaces
  `proof read timed out after 10s` in the `serve_stream` failure span.

- **A faulted lane's remainder starts within a second of a stream freeing
  (#2230).** In `decdn bundle pull`, a lane gives its stream permit back when
  its own worker ends, so a sibling entry can use it. It starts again only on
  a permit that is free then, and tries again each second while none is. A
  queued range that no idle lane takes goes to a busy lane on one extra
  stream; when part of the range has no running lane that covers it, any busy
  lane not barred from pull-through can take it by pull-through. When a
  provider has no free permit for that stream, the acquire asks again every
  second. A node that refuses a lane's extra streams is asked again after
  1 s, doubling to 30 s, and its `NotFound` refusals of uncovered ranges on
  extra streams bar it from pull-through as on its own stream. The info line
  `a queued range waits for a stream` names the waiting ranges and logs when
  they change, and every 30 s while they stay.

- **Chain-read logs no longer carry the RPC URL (#2264).** The node's
  binding check, buyer lane seed, owned-pool walk, top-up, pool open and
  reclaim sweep log their RPC errors through the URL-stripping redactor, so a
  provider API key in `blockchain.rpc_url` does not reach the node logs. The
  client's lane-build and discovery debug lines and the CLI's "no holder
  resolved yet" warning do the same.

- **A transient `getAuthorization` fault no longer refuses a paying signer as
  cap-exhausted (#2220).** When the admit-time read faults, the node answers
  with the last registration it read for that signer, whatever its age, with
  `spent` raised to the settlement projection's fold. A registration's cap and
  expiry are write-once, so the old read stays true; an old "unregistered"
  read is not trusted, because a registration can land at any time. A signer
  with no registered read is still refused, now as `pool_unconfirmed`
  (`decdn_serve_stream_rejected_pool_unconfirmed_total`), so
  `decdn_serve_stream_rejected_signer_cap_exhausted_total` counts only signers
  whose on-chain cap cannot pay a floor.

- **Partial holders no longer pull a blob from each other in a loop of three
  or more (#2224).** While a node's own upstream open for a blob is in
  progress, it refuses a whole-blob request for that blob from an active
  staker with `NotFound`, so the requester tries its next candidate. New
  counter: `decdn_serve_stream_rejected_pull_loop_guard_total`, on the "Serve
  refusals by reason" panels; the debug line names the requester. Ranged
  requests and client requests are not refused for this reason. A node also drops its own entry from the origin directory's
  candidates, as the DHT lookup already does.

- **Clustered `eth_getLogs` failures no longer flap every chain watcher
  (#2253).** A window that fails every in-tick retry no longer fails the
  tick. The poller ends the tick there, keeps the progress of the windows
  before it, and the next tick resumes at the deferred window. The tick fails
  (backoff, `*_watcher_restarts_total`, `*_watcher_down_seconds`) only when no
  tick reaches head for 2 minutes, so ticks that progress but keep falling
  behind head still surface. A deferred tick that completes no window does
  not stamp the watchers' tick gauges. New counter:
  `decdn_chain_get_logs_deferred_total`, plotted on the chain dashboard's
  "eth_getLogs window span" panel.

- **A partial holder is not asked again for chunks it cannot pull through
  (#2262).** A probed holder that refuses a chunk outside its advertised
  coverage with `NotFound` now counts toward the same three-refusal limit as
  a non-holder. At the limit the client stops sending it uncovered chunks of
  that blob for one minute, and keeps sending it the chunks it covers. A node
  also answers `NotFound` when it sheds miss load, so when the minute ends the
  holder takes one uncovered chunk again as a probe, and three more refusals
  bar it again. A rediscovery that reports wider coverage, or a byte the
  holder verifies on an uncovered chunk after its last refusal, lifts the bar
  at once. When only uncovered work is left and every source is absent or
  barred again after its probe, the item ends as a unanimous `NotFound` does.

- **`max_blob_size` caps the bytes of one blob held, not the offsets served
  (#2256, #2463).** A node serves a pull-through range at any offset of a
  blob larger than its `max_blob_size` while the bytes it holds for that blob
  fit; a read past 16 GiB of a 30 GB blob on a 16 GiB node is served, not
  failed as a node fault. Each pull leg spans at most the room the bytes held
  leave under the cap, and the pull ends before it opens a leg that could only
  add bytes past it, so nothing past the cap is paid for. Before it commits, a
  node refuses `Declined` only when it already holds the cap's worth of the
  blob and lacks a byte of the range; it never reads the request's offset or
  the upstream's unverified size. A fill that ends at the cap mid-stream sends
  the bytes that landed, then a mid-stream `Declined`, and counts on the new
  `decdn_serve_stream_midstream_blob_too_large_total`, never on
  `decdn_serve_stream_node_fault_total`; the refusal before commit counts on
  `decdn_serve_stream_rejected_blob_too_large_total`. The client excludes a
  proxy or whole holder that refuses as too large for the rest of the blob; a
  partial holder loses only pull-through. `decdn get --max-blob-mb` caps the
  bytes the fetch holds the same way: `open_progressive_pull` refuses no
  offset, and a fetch of ranges that could hold more than the cap runs one
  stream at a time so concurrent legs cannot pass it. A resumed download whose
  proven size needs more than the cap ends at once. `decdn_client::BlobTooLarge`
  names its field `reached`: the bytes held, or the wire bytes one stream
  received, past `ceiling`.

- **An under-fold heal after a payer crash no longer loops, and a healed
  lane fault does not end the pull (#2257).** When a trailing-proof
  rejection's bundle is at or behind the ledger, the client retires the
  in-process chain root the node never adopted and folds the ledger to its
  settlement value, as a fresh process does. A watermark rejection that the
  client heals but cannot resume within the retry budget now cools that
  source instead of ending the command. A rejection that no bundle heals,
  and a spending-cap rejection, still end the command. The heal branch and
  each healed retry log at `info`.

- **A delegated lane is swept when its expiry margin starts (#2233).** The
  node refuses preimage reveals inside a capability's expiry margin, as it
  already refused vouchers, so a lane's claim is final when the margin
  starts. The redeemer sweeps one second later. Before, a lane's final value
  waited for the next periodic sweep, and a sweep that a receipt wait
  delayed into the expiry's landing slack skipped the lane and forfeited it.
  The cutoff sweep batches the lane with the node's other owed lanes, so
  lanes that are each below `redeem_threshold_micro_usdc` but clear it
  together are redeemed before the expiry. A node that restarts after a
  lane's margin starts sweeps that lane at once.

- **Expiry and close deadlines hold a safety margin (#2242).** A node
  refuses a voucher with `CapabilityExpired` once the capability expires
  within one redeem interval plus 120 s, so every voucher it accepts meets at
  least one redeem sweep before expiry. The redeemer skips a `Closing` pool within 120 s of its
  dispute deadline, and a lane with value owed whose capability has expired
  (new counter `decdn_redemption_skipped_expired_total`). `decdn pool assign`
  checks expiry against the chain head timestamp when an RPC is configured,
  falls back to the local clock with a warning, and warns when the expiry
  falls inside the default node margin.

- **Node dials use registry multiaddrs, not only the relay.** A node
  publishes only its relay URL through pkarr, so a dial by bare node id
  opened over the relay even when the target registered its QUIC address.
  The daemon's `CapacityBond` watcher now keeps each node's registry
  `multiaddrs` in an iroh address lookup on the endpoint, so DHT RPCs and
  upstream probes and pulls connect directly to a reachable node. The CLI
  keeps the registry addresses on warming proxies and cold-order
  pull-through targets, and `decdn node lookup` dials with them.
  `decdn-client`'s `WarmingCandidate` gains `multiaddrs`. A node's pkarr
  record now carries its globally routable IP addresses next to the relay
  URL (private, loopback, link-local and CGNAT addresses stay out), so a
  node with no or stale registry multiaddrs is still dialable directly by
  node id, on the n0 pkarr server and on an operator-configured
  `network.discovery.pkarr_url` alike.

- **Node reachability is reported, and registration defaults `--multiaddr`.**
  At bring-up the node logs whether its default route has a public address
  (public) or not (behind NAT, where relayed inbound is expected), sets
  `decdn_node_public_address`, and warns when a public address is missing
  from its registry multiaddrs. New counters
  `decdn_inbound_connections_direct_total` and
  `decdn_inbound_connections_relayed_total` split inbound connections by
  arrival path, with a "Reachability" row on the node dashboard and a
  `DecdnPublicNodeInboundRelayed` alert scoped to public nodes. `decdn
  doctor` reports the same reachability. `decdn setup` and `decdn node
  register` without `--multiaddr` now register the host's public address on
  `network.bind_port`, and register none behind NAT.

- **`bundle pull` no longer counts a resumed `.partial` prefix as downloaded
  (#2236).** A prefix that an earlier, interrupted pull fetched counted again
  in the entry line's downloaded bytes and rate, and in the summary's
  `downloaded`. On the testnet a re-pull that resumed a 1.46 GB prefix
  reported 9.7 GB downloaded for about 8.3 GB moved. The entry reads its
  resume record before it drives anything and reports the prefix apart
  (`7.7 GiB downloaded at R, 1.4 GiB resumed, 3.7 GiB spliced from disk`); a
  byte both resumed and spliced counts as spliced. The summary adds
  `resumed N from earlier partials`, and `--json` gains `resumed_bytes`.
  `decdn-client` exports `ClientRangedStore::present_byte_ranges`.

- **A slow client leg phase is logged (#2230).** A leg phase (`unpolled`,
  `pay` or `read`) that runs past 5 s logs at debug, with the peer, hash,
  offset and unproved bytes, so a proof stall is named before the node's
  10 s proof timeout stops the stream.

- **Partial holders no longer pull a blob from each other in a loop (#2227).**
  A serve-miss read the blob size by opening the whole blob at its
  top-ranked candidate, even a partial holder that lacked the first block.
  That holder treated the open as a miss and started its own pull, often
  from another partial holder or from the node that asked it. On the
  testnet, fra1, hel1, nbg1 and lon1 asked each other for a Mistral shard
  none of them fully held, at up to 166 failed streams per node per 5
  minutes. The size open now asks for one chunk group of a block the
  candidate advertises, and the requesting node is never a candidate.

- **A serve-miss pull draws for its parked serve leg while a sibling stream
  of the same blob waits further down (#2219).** Every serve leg's demand went to
  every live fill of the hash as one high-water mark, so a sibling stream's
  demand hid a leg parked at its own pull's frontier. That pull paced to
  `Wait` and neither leg moved until the client gave up. On the testnet, lon1
  proxying a 13.5 GiB entry went silent for 31 s and the client failed over.
  Each serve leg now stands its own demand through a `DemandSlot`, routed only
  to the fill that produces the awaited byte and withdrawn once the leg moves
  again, and a pull reads the nearest standing demand. `decdn-cache` exports
  `Demand` and `DemandSlot`; `FillSession::demand_up_to` is removed and
  `FillSession::serve_demand` returns a `Demand`.
- **`bundle pull` holds a lane-stream permit only while that stream runs
  (#2208).** A range-dedup entry held a permit on every candidate, reserve
  and proxies included, for the whole entry, and a fan-out held one per
  provider after its lane died. A proxy lane that needed width then ran one
  stream at a time. Each drive pass now reserves a permit only for the
  sessions it drives, and each lane gives a permit back as each of its
  streams stops. A lane with no range left keeps one permit for one idle
  stream.
- **The multi-source unit watchdog counts verified bytes, not durable
  checkpoints (#2209).** It judged progress by the store's missing-byte
  count, which moves only per 4 MiB checkpoint, so a proxy warming a cold
  miss in ~1 MB draws was killed inside the 10 s default deadline.
  `--unit-deadline-ms` is now the longest gap between verified bytes.
- **`bundle pull` samples probe candidates per entry (#2204).** One
  `SELECT_K` sample served the whole run, so with more registered nodes than
  `SELECT_K` some nodes reached no entry, even when they were an entry's only
  nearby holders.
- **`bundle pull` no longer counts re-fetched bytes as spliced (#2193).** A
  failed donor's group-wide re-fetch can cover part of a neighbour that
  spliced, and the summary's downloaded bytes read low by that overlap.
- **A peer store that refuses writes is logged (#2203).** The probe-round
  harvest dropped every write error, freezing the latency ranking without a
  trace. Each refused write logs at `debug`, and a round whose every write
  fails logs once at `warn`.
- **A serve-miss starts its upstream pull without re-hashing held ranges
  outside the stream's own range (#2205).** Before it spawned the pull, every
  serve-miss stream seeded the shared outboard from all of the blob's held
  ranges, and `outboard_pairs` reads and verifies every leaf it exports. On the
  testnet, lon1 proxied a 13.5 GiB entry: the dead time before each stream's
  pull grew from 2.4 s to 8.2 s as the node cached more of it, and the stream
  sent no data frame in that time. The upstream leg itself moved ~50 MB in 6 s.
  The primed handshake pull also idled past its 2 s adoption limit, so the node
  opened the same range a second time and forfeited its carried ramp credit at
  the holder. The seed now covers only the held part of the stream's own
  chunk-group-aligned range, the only span whose proof nodes its encoder loads.
  A corrupt held range outside that span no longer fails the stream; the first
  stream that covers it quarantines the hash. A failed seed is now logged at
  `WARN`.

- **`DecdnFeeSharesPollFailing` fires on repeated re-read failures, not on
  one transient RPC error.** The rule fired for 6 h on any single failed
  `getShares()` re-read, although the next hourly re-read heals it. On the
  testnet it fired 18–40 h per node over 7 days from 6–11 failures each. It
  now fires on `increase(decdn_fee_shares_watcher_poll_failures_total[3h]) >= 2`:
  two failed re-reads inside 3 h, which at the default 1 h cadence is two of
  the last three. The same 7 days would have fired it for 2–9 h per node.

- **The client peer store no longer ranks a nearby node behind the origin
  after a cold miss (#2196).** The store's `latency_ms` folded each stream
  open's time to first byte alongside the probe RTT. On a cache miss that time
  includes the serving node's own upstream pull, so after a bundle pull the EU
  nodes read 1–1.8 s against a 57–78 ms RTT, and the sole origin ranked first
  on the probe-less fast path for the next 10 minutes. Only probes now feed
  `latency_ms`; a stream open files its quoted rate and clears the failure
  stamp (`PeerStore::record_open`). `UpstreamPullHeader::ttfb_ms` is removed
  from the `decdn-client` API, since nothing else read it. Every peer-store
  mutation now takes one process-wide lock, so a stream open that races the
  off-path probe harvest no longer writes back a record read before the probe
  sample landed.

- **A serve-miss whose DHT answer names only partial holders reaches the
  namespace's origin instead of truncating the client's stream (#2195).**
  The on-chain origin directory was read only when the DHT answer was empty.
  Since #2188 a DHT answer can name only partial holders, so the ranged
  assembly ran out of candidates for the blocks none of them held, and the
  client got a short stream after `ok: true`.
  - On the ranged pull leg, when the probed holders' coverage does not span the
    blob, the node probes the origin-directory candidates that the first round
    did not probe, an origin past the `probe_fanout` cut among them. A
    supplemented miss probes at most twice `probe_fanout` peers and records two
    `decdn_probe_collection_latency_seconds` samples.
  - A probe-cache entry whose providers do not span the blob counts as a
    `decdn_probe_cache_misses_total` on the ranged pull leg and runs a fresh
    lookup.
  - A partial holder's probe response carries the blob size
    (`ProbeResponseExt.total_bytes`) with its coverage, so a set of partials
    that covers every block counts as spanning. No wire change: the field
    already exists.
  - Before it signs `ok: true`, the serving node checks, against the signed
    blob size, that its candidates cover the part of the pull range it does
    not hold. If they do not, it refuses with a signed miss the client can fail
    over on, and logs an `info!` naming the range.
  - A ranged assembly that still ends early logs a `warn!` with its cause:
    every covering candidate faulted, no progress, an uncovered range, or the
    reassign budget.
- **A serve-miss pull no longer re-pays one upstream range for hours after its
  client leaves (#2194).** Two faults combined.
  - A serve leg that waits on its pull for the next frame now also watches the
    client's stream. Before, a wait touched neither stream, so a client that
    left went unseen. The serve leg held its observer lease and kept the pull
    alive until the node restarted. A client that stops the stream or loses the
    connection now ends the serve at once and meters
    `decdn_node_pull_through_client_abandoned_total`. The serve releases its
    lease, which cancels the pull once no other serve leg observes it. The
    node's own close (shutdown) ends the wait without counting an abandon, and a
    send stream this node already closed is a node fault, as on the write path.
  - `drive`, `drive_range_set` and `drive_range_lanes` never re-open a range
    after a clean leg that moved neither the paid nor the delivered frontier.
    The gap ends with the new `decdn_client::LegNoProgress` error, which fails
    over to another source (at most one more leg each). A rebase of the shared
    ledger during the leg voids the check. On a node, every pull path (ranged
    runs, whole-blob pulls, own-origin fills) bumps
    `decdn_node_pull_leg_no_progress_total` and logs a `warn!` with the range,
    both frontiers and the channel's paid wire since the leg opened. An
    upstream is not scored for it: the `(peer, hash)` pair is suppressed
    briefly, and a speculative run still debits its warming allowance for the
    paid leg.
- **Monitoring: four dashboard panels show live data (#2197).** Each panel
  showed zero, NaN or a dead series by construction.
  - The delivery dashboard's "Bytes served, received and pulled through" panel
    does not plot `decdn_cache_bytes_returned_total` ("served to callers"). That
    counter is scoped to `CacheEngine::get`, and the serve path never calls
    `get`, so the line held at zero. "served on the wire"
    (`decdn_bytes_served_total`, bao wire bytes) is the delivered-bytes series.
  - The overview's "Pull success rate" reads over a trailing `[1h]` window.
    `decdn_node_pull_{attempts,success}_total` step only when a pull starts or
    ends, so a `$__rate_interval` window read NaN during one long pull. "Pull
    funnel" and the delivery dashboard's "Attempts vs success" say this in
    their descriptions.
  - The node dashboard's "File descriptors" panel does not plot
    `node_filefd_maximum`. A host with no limit reports 2^63−1, and that value
    flattened the allocated line to zero.
  - The Tempo latency query, the span-rate panel description and the
    `adr/appendix-observability.md` span table do not name `origin_range_pull`.
    No code emits that span.
- **A node that fills its cache through ranged pulls announces it to the DHT
  (#2186).** The DHT republisher learned about cached content only from
  whole-blob pull-through commits and from a boot-time walk of `Complete`
  blobs. A node that filled only through ranged serve-miss pulls therefore
  published no provider records. A blob that completed through ranged fills
  was announced only after the next restart. Now a ranged admit
  (`admit_bao`, `admit_bao_stream`) that completes a 64 MiB discovery block
  announces the hash on `subscribe_inserts`, as ADR 022 §STORE Flow requires.
  It does this even when the admit fails partway, because the store keeps the
  blocks it verified. The cold-start seed and the lag sweep also walk partial
  blobs, and the due-time gate drops any partial that covers no block. The
  eager `Store` on a cache event now goes out only for a hash that has not
  announced yet, and never with empty coverage. Later blocks widen the
  advertised coverage on the next republish cycle.

- **A seller repointed at a redeployed `PaymentPool` drops the old
  deployment's lane state (#2181).** `poolId` names neither the chain nor the
  contract, so a returning buyer's first pool on the new deployment has the
  same id as its pool on the old one. The seller kept the old lane for it: it
  rejected the buyer's first voucher as `AmountRegression`, which suppressed
  the provider, and it redeemed the old deployment's signatures against the
  new contract. `lanes.redb` now records the deployment (chain id and
  `PaymentPool` address) in a `deployment_v1` table. When the node starts
  against another deployment, it drops the lane table, both pending-settle
  sets and the watcher checkpoint before it reads them, and logs the drop at
  WARN with the lane count. The dropped lanes' unredeemed vouchers are forfeit.
  A store with no record yet takes the configured deployment and keeps its
  rows — so a node that was repointed at a redeploy **before** this upgrade
  still carries the colliding rows under the new record; delete its
  `lanes.redb`, `settle.redb` and `checkpoint.redb` by hand to clear them,
  and leave `buyer.redb` in place — its rows carry per-row deployment tags
  and are handled individually.
  `PersistentPoolStateStore::open` takes the configured `Deployment`.

- **`decdn-node` no longer hangs until systemd's stop timeout after
  node-to-node pulls (#2185).** A serve-miss pull leg runs on its own
  current-thread runtime, which drops when the leg returns. An upstream dial
  made there put the connection's QUIC driver on that runtime, so the driver
  died with it and the connection never reached QUIC's draining state. On
  SIGTERM, `router.shutdown()` then waited in `Endpoint::close` until systemd
  killed the process, which skipped the final redeem sweep, the lane-store
  flush, the receipt tail and `cache.shutdown`. Every upstream dial the node
  makes now runs on its main runtime, so each driver outlives the pull leg. The
  per-leg drain wait is gone, and so is its counter
  `decdn_node_pull_abandon_drain_timeout_total` and its dashboard series. The
  router close is also bounded by the 15 s shutdown deadline and logs an error
  when it gives up, so a stuck endpoint close can no longer cost settlement
  state. A dial task that panics or is cancelled by its runtime shutting down
  is this node's fault, and no longer scores the provider unreachable.
- **Steady-state DHT republishes batch again (#2179).** The republish
  scheduler ticked every second and sent only the records already due. The
  per-record `uniform(30, 50 min)` jitter spread due times so thinly that each
  `BatchStoreRequest` carried about 1.2 hashes, and each paid a fresh QUIC
  connection, most of them on the relay path. A tick with a due record now
  also takes the records due within the next 5 minutes, in due order, while
  each receiver's set stays at or below 32 hashes (below the 40-token per-peer
  burst). A cycle less than 2 s after the previous one takes due records only,
  so a receiver's bucket refills between look-ahead batches. Sending early only
  shortens a record's refresh gap, which stays between 25 and 50 minutes,
  inside the receiver-anchored 1 h TTL (ADR 022 §STORE Flow "Drain cycle").

- **A backpressure refusal from the only holder of a range no longer fails
  paid serve streams (#2178).** A node pays every upstream leg from one buyer
  signer, so its own concurrent pulls can fill a seller's per-signer live cap,
  and the seller sends that refusal as `NotFound`. The node's ranged assembly
  dropped the refusing source on the first refusal. When that source was the
  only holder of a still-missing range, the assembly ended short and every
  serve stream attached to it failed after payment. The 30 s `(peer, hash)`
  suppression then took the holder out of every other pull on the hash.
  - An open-stage `NotFound` or `Overloaded`, or a transport rate-limit, on a
    ranged run is now backpressure. When no other holder covers the range, the
    node waits (250 ms, doubling to 4 s) and asks the same holder again. After
    six waits with no new verified bytes, the node ends the pull, logs a
    `warn!`, and suppresses the pair (ADR 039 §Failure handling).
  - Ranged runs and the first-leg and header handshakes meter a backpressure
    refusal but do not suppress the pair for it.
  - New counters: `decdn_node_pull_backpressure_backoffs_total` (one per
    completed wait) and `decdn_node_pull_backpressure_exhausted_total` (a
    holder outlasted the wait budget; zero is the expected value).

- **Back-to-back range pulls keep their credit ramp, and a bundle lane widens
  as sibling entries finish (#2168).** A range-deduped bundle entry served
  through a cold two-leg miss pulled its complement (147 runs, median
  11.6 MiB) at about 1 MiB/s. There were two causes, and the fix for the
  first exposed a third issue in the client:
  - Every new `cdn/client/v1` stream restarted the node's credit window at the
    one-chunk floor, on the serve loop and on the pull leg's `RampPacer`. A
    lane now keeps an in-memory ramp-credit pool of paid content bytes. A
    stream takes the whole pool when it starts. When it ends fully paid, it
    returns its carried credit plus the content of its request, so the next
    request on the lane opens at the window the last one earned. A stream the
    node refuses before its first byte returns its credit unchanged. A stream
    that ends any other way forfeits the credit, so a payer that abandons
    stream after stream gets no more unpaid bytes than
    `paid / credit_ramp_divisor` plus one floor for each stream. The pool
    holds at most `credit_max × credit_ramp_divisor`. A restart or a forgotten
    lane starts at the floor again (ADR 003 §Credit window). The deposit guard
    and floor reservations still price the floor (ADR 037).
  - `bundle pull` sized each provider lane once, from the permits free when
    the drive started, so a lane that started at width 1 while sibling entries
    held the provider's permits ran its runs one at a time. The lane now takes
    freed permits, without waiting, each time it starts a gap while more gaps
    wait (`decdn_client::LaneGrowth`), up to `--max-lane-streams`.
  - After a proof write fails, the client now reads past the `ChunkData`
    frames the node sent ahead of its `StreamError`, so an opening window wider
    than one frame no longer turns an `UnderFold` rejection into a transport
    error. This also applies at `credit_ramp_divisor = 0`. The skipped frames
    stay under the request's promised wire length and the received-byte
    ceiling, and a `StreamEnd` behind skipped frames fails the leg.
  - `decdn_client::LaneGrowth` hooks receive the number of waiting gaps and
    never add more workers than that.

- **The `UnderFold` heal documents the path that reaches it (#2169).** A
  reveal that fails to send locally never reaches the node: the client counts
  a reveal only after the transport accepted its whole frame. The trailing
  watermark comes from a payer process that exits after it sends reveals and
  before it persists them, for example a second Ctrl-C during a `bundle pull`
  that already persisted earlier entries on the same chain. ADR 003 and the
  lane docs now name that path, and a loopback test drives it end to end.

- **An under-folding voucher no longer stalls the stream for 10 s (#2167).**
  A lane can hold a live hash chain from an earlier payer process. When a new
  process sent a voucher at or above the signed anchor, under a new root, but
  short of that chain's claim, the node credited it 0 bytes. It then waited
  for a proof that never came, until the 10 s voucher read timeout. The client failed over to another
  provider, so every small bundle entry routed to that node took about 11 s.
  The node now rejects the voucher with the new reason `UnderFold` and the
  resume bundle. The client folds the proved frontier, reseeds, and resumes on
  the same provider (ADR 003 §Chain length and rollover).
  `decdn_serve_stream_client_declined_total` stops rising for this case.
  - The fold check now binds `bytes_delivered` as well as `amount`. A rollover
    that pays the whole claim but signs fewer bytes than the chain proved is
    also refused `UnderFold`. That one does not heal: the bundle does not
    advance the client's amount, so the client stops and logs a `warn!`.
  - Concurrent streams on one lane get the same rejection. The first one
    reseeds; the others find the ledger already healed and retry.
  - A sealed closing voucher that reaches the node at the anchor after a
    sibling opened the live chain stays already-satisfied, as before.
  - The node logs the fold shortfall (axis, owed, got) at `debug!` when it
    rejects a voucher on lane validation.
  - **Wire-breaking:** `VoucherRejectReason::UnderFold` is a new variant,
    appended at the end. A peer built before it cannot decode the reason.

- **Every failed inbound stream now has a reason counter (#2073).** About 91 %
  of `decdn_streams_failed_total{direction="inbound"}` had none, so the
  "Unattributed stream failures" panel showed a baseline no operator could
  read. Five new counters close the gap:
  `decdn_serve_stream_rejected_stream_cap_full_total`,
  `decdn_serve_stream_request_unreadable_total`,
  `decdn_serve_stream_voucher_rejected_total`,
  `decdn_serve_stream_client_declined_total` (the requester left before a
  voucher credited a byte, which includes the header handshake a downstream
  miss pull does not adopt) and `decdn_serve_stream_client_abandoned_total`
  (the requester left after paying). The serve handler now counts every end in
  one place, so each inbound failure counts on exactly one reason and the panel
  reads 0; any positive rate is a bug.
  - A refusal whose signing fails counts only as a node fault; it used to count
    as the refusal reason too.
  - A refusal, a voucher reject, or a mid-stream pool or signer-cap stop whose
    frame the peer never reads (it already left) keeps its own reason instead of
    counting as the peer leaving.
  - A write to a stream the node already closed is a node fault on every write
    path; the framed writes used to file it as the peer's fault.
  - A panicked stream counts its node fault before its failure.
  - The `serve_stream` span records `outcome = "stopped"`,
    `reason = "proof_budget_exhausted"` for a spent proof budget and
    `outcome = "reset"`, `reason = "request_unreadable"` for an unreadable
    request, where both used to record `outcome = "failed"`. Both keep the cause
    in the span's `error` field, as does a refusal or stop whose write failed.
  - Both Unattributed panels subtract the node crate's `INBOUND_FAILURE_REASONS`
    list, and tests hold the panels, the list, and every exported
    `decdn_serve_stream_*` counter to one another.

- **A transient `eth_getLogs` failure no longer takes every chain watcher down
  (#2161).** The shared chain-event poller retries a failed window on the same
  block range up to two times, 2 s apart, before it fails the tick. A retry
  that succeeds does not fire `on_backoff`, count a watcher restart or start
  `*_watcher_down_seconds`. Before, the first failed window failed the whole
  tick, the backoff grew the backlog, and the longer backlog needed more
  windows, each one another chance to fail: on the dRPC free plan (100-block
  cap, about 15–20 % of calls failing with code 19 or 30) every watcher showed
  minutes of downtime although no event was lost. Range rejections keep the
  shrink path. A rate limit (HTTP 429, `Retry-After`, or rate-limit wording)
  and a permanent error (a revert, invalid request/method/params, an HTTP 4xx
  other than 408/429) are not retried and fail the tick at once. Each retry
  logs its cause at `info` and bumps the new counter
  `decdn_chain_get_logs_retries_total` (one window adds up to two), plotted on
  the "eth_getLogs window span" panel. A hung provider now takes up to 34 s
  per window (three 10 s timeouts plus the sleeps) before the tick fails.
- **A transient RPC error during a boot-time chain read no longer exits the
  daemon (#2159).** The CapacityBond registry snapshot, the slash enumeration,
  the PaymentPool `usdc()` self-check and the ContentBlacklist boot enumeration
  plus its enforcement pass now retry in process on the chain watchers' backoff
  (1 s doubling to 60 s), with a `WARN` (`boot chain read failed; retrying
  after backoff`) and a `decdn_chain_boot_read_retries_total` bump per retry.
  One 10-minute boot deadline covers all of them, so a dead endpoint still
  exits. A deterministic fault still fails boot at once: no contract at the
  address, an ABI mismatch, a revert, an invalid request/method/params
  response, an HTTP 4xx other than 408/429, or a local eviction error that
  decides a blacklist enforcement attempt. Startup stays fail-closed: the ALPN router
  still waits on the blacklist enumeration and enforcement. Supporting changes:
  - The registry, slash and `usdc()` reads carry the 10 s per-call RPC
    timeout, so a hung provider reaches the retry instead of wedging boot.
  - A failed head read keeps its typed provider error through the
    `SharedHead` cache, so the first read of each bootstrap is classified too.
  - The slash enumeration reads at its snapshot block, so a lagging
    load-balanced backend answers "header not found" (retried) instead of
    reverting on a slash index it has not seen (fatal).
  - The best-effort fee-share reads (`PaymentPool.feeRouter()`,
    `FeeRouter.getShares()`) retry on a 1-minute sub-budget of the boot
    deadline and carry the per-call timeout. They still fall back to the floor
    share rather than fail boot, but a transient `feeRouter()` error no longer
    leaves the node on the floor share with the fee-shares watcher
    unregistered until restart. When it does fall back, the new
    `decdn_fee_shares_watcher_unregistered` gauge reads `1`. Their failure
    logs are sanitized, so an RPC URL key cannot leak into them.
  - The fee-shares watcher's periodic `getShares()` re-read carries the
    per-call timeout and logs a sanitized error.
  - The blacklist boot enforcement pass stops at its first failed scope read
    instead of re-checking every remaining hash against a failing provider.
  - The client's registry discovery shares the classifier
    (`decdn_client::provider::is_permanent_contract_error` /
    `is_permanent_rpc_error`): a provider outage answered as a JSON-RPC error
    response (for example code `1` "no available upstreams" or `19` "Temporary
    internal error") is now retried on the ADR 012 schedule instead of failing
    the read at once.
- **An own origin that declines `Range` degrades before the node signs
  (#2069).** The own-origin serve-miss probe now reads the blob's first chunk
  group as a ranged GET. An origin that publishes `{H}.obao4` but ignores
  `Range` falls back to the buffered whole-blob pull instead of signing
  `ok: true` and failing the stream on its first draw. A per-origin latch skips
  the probe's range read once the origin serves a clean range window, and an
  origin that serves a wrong-length first window no longer shadows a healthy
  later origin. A panic in the range encode now surfaces as the new
  `CacheError::Internal` with its message, logged as a code bug, not as a
  local-origin fault; a store fault such as a full disk is not. New series:
  `decdn_cache_range_pull_permit_waits_total` (draws that waited on the full
  range-pull pool, warning past 10 s) and
  `decdn_node_pull_through_wait_seconds` (length of each pull pause, warning
  past 45 s). `decdn_cache_fill_not_coalesced_total` now counts once per claim,
  and only when the skipped fill overlaps the request.

- **The cache footprint counts a partial blob's present bytes (#2157).**
  `size_snapshot` sized a partial blob from iroh-blobs' `status()`, which
  leaves the size unknown until the blob's last chunk arrives and then reports
  the whole blob. A middle-range or front-range fill counted as 0 bytes, and a
  fill that holds the tail counted as the full blob. `decdn_cache_bytes` read 0
  on a node that held gigabytes of node-tier miss fills, and the eviction
  driver undercounted those fills: they never pushed the cache over its
  high-water mark, and an eviction of one counted 0 bytes freed. A partial
  blob now counts the bytes its bitfield marks present. This fixes the gauge,
  the eviction footprint, and GC reclaim attribution. The size walk re-reads a
  partial's bitfield at most once per 30 s, because each read of an idle
  partial fsyncs its store files, so a growing fill's count trails by up to
  30 s. A partial whose bitfield read fails counts at its last known count,
  else at `status()`'s size, and the walk goes on; each such failure ticks the
  new `decdn_cache_partial_size_observe_failures_total` and logs the hash. A
  partial fill is now an eviction candidate as soon as it is admitted. Before,
  it became one only after a serve completed, so the driver never selected a
  fill whose serve aborted or went unpaid.

- **A redemption whose receipt wait fails but whose transaction mined counts
  as landed, not as a failure (#2154).** A load-balanced RPC can fail the
  receipt wait with `Unknown block` when the backend that answers has not seen
  the block yet. The node counted each such `redeemMany` in
  `decdn_redemption_failures_total`, even though it had mined. The landed path
  never ran, so `decdn_pool_redemptions_total` missed the vouchers and
  `registered_until` was not saved, and a later sweep registered the signer
  again. When a receipt wait fails or times out, the node now fetches the
  receipt by hash up to three times, waiting 1 s, 2 s and 4 s before each
  fetch. A receipt found this way counts as landed or reverted, the same as
  one the wait returned, and also ticks the new
  `decdn_onchain_tx_receipt_recovered_total`.
  `decdn_onchain_tx_receipt_failed_total` and `decdn_onchain_tx_timeout_total`
  now count only transactions that are still unconfirmed after those fetches,
  and the node's warning names the last fetch's result (`last_lookup`). An
  unconfirmed `redeemMany` is no longer a redemption failure. The
  `DecdnOnchainTxTimeouts` alert is renamed `DecdnOnchainTxUnconfirmed` and
  fires on both unconfirmed counters, so a node that cannot see its own
  receipts still pages.

- **Chain watchers survive an RPC that caps `eth_getLogs` ranges, and the
  stalled-watcher alerts catch a watcher that never ticked.** The chain-event
  poller scanned `[cursor, head]` in fixed 10 000-block windows and never read
  the error. A provider with a lower range cap (dRPC's free tier rejects about
  150 blocks, Alchemy's free tier 10) rejected every window, the cursor held,
  and each backoff retry asked for a wider range, so every watcher stayed
  blind. A restart did not help once the settlement checkpoint was more than
  the cap behind head. The poller now sets its window to half the rejected
  window when the provider's JSON-RPC error names a range or result limit,
  retries at once, and doubles the span back after 32 accepted windows;
  timeouts, rate limits, head-lag errors and other errors keep the backoff
  path. The five `*WatcherStalled` alerts only read the tick gauge behind a
  `> 0` guard, and a never-ticked watcher reads `0`, so none fired. Each now
  also fires on `decdn_<watcher>_watcher_down_seconds > 180`. The "Oldest
  watcher tick", "Watcher tick age" and "Settlement watcher freshness" panels
  show a failing never-ticked watcher by its down-seconds instead of dropping
  it.

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
  `pool_store` test asserts that opening `PersistentPoolStateStore` creates
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
    close tx and the `pool reclaim` deadline, and a re-run of the close or the
    reclaim clears the stale row.
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

- **A `pull_leg_miss` refusal names the walk that found no target (#2366).**
  The `serve-miss: refusing the request` line for a pull-leg miss now records
  where the candidate list came from (`source`: `cache`, `dht`, `directory`
  or `none`), how many providers it held (`discovered`), how many the probe
  rounds skipped as suppressed or wedged (`skipped`), probed (`probed`), lost
  to a failed probe (`unreachable`), heard `has_blob: false` from (`absent`)
  or dropped on this node's own policy (`dropped`), how many holders they
  admitted (`holders`) and whether those span the blob (`spans`), how many
  handshakes ran (`handshakes`) and why the last one failed (`last_fault`).
  Every serve-miss refusal line also carries the request's `namespace_id`.
  Before, the line said only `cause=clean_miss`, so a fleet logging at info
  could not tell an unreachable sole candidate from an empty directory.
- **A fault in this node is no longer signed to clients as a `NotFound` about
  the content (#1560).** When a node-to-node pull failed for a reason that was
  *ours* — a buyer key that cannot sign the ADR 005 client binding, a voucher
  signature the upstream cannot verify (`BadSignature` / `WrongSigner`), an
  unusable `cache.node_pull_timeout_sec` / `cache.node_pull_stall_timeout_sec`
  budget, or a buyer pool store this node cannot read — the serve path
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
  #1129. Buyer pool-open failures are attributed at the site that raises them
  rather than guessed at by the caller: a poisoned open lock, an unreadable
  pool store, a store write that leaves a deposit untracked, a panicked open
  task, and a wallet that cannot fund a deposit all refuse, while a pending open,
  a reconcile-held slot, a per-provider on-chain
  revert, and a transient RPC fault stay clean misses.
  - Scope is deliberately narrow: only a local fault changes the wire code. A
    wedged or settled *lane* to one provider still answers `NotFound` — it is
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

- **client: the lane-fault line reports the range the lane held after a
  steal and the bytes it re-queued (#2376).** A steal trims the victim's
  range to end at the split, and the stolen tail is the stealer's. The
  "a lane faulted" line logs that held range as `offset`/`len`, the missing
  bytes of it that go back to the queue as `requeued`, and the range as the
  lane picked it as `picked_offset`/`picked_len`. `landed` counts each byte
  the lane made present once, so a byte a healed leg or an owed span
  delivered again does not count twice. For a range the store held no byte
  of at the pick, `landed` and `requeued` add up to `len`. A
  `NotFound` from a partial holder counts toward the blocks of the held
  range only. The "stole the tail of a lane's missing remainder" line
  carries `hash` at debug and at info, so a steal matches the fault it
  precedes without offset arithmetic.
- **CLI: a pull record prune deletes only records, and an entry run's record
  keeps its counts (#2754, #2753).** A `decdn get` run prunes only files named
  `<start-ms>-<pid>.json` in `~/.decdn/activity/pulls/`. Any other `*.json`
  there stays, does not count toward the 200 kept records, and the dashboard
  still lists it. The record of a bundle entry written to a
  file (`decdn get decdn:<hash>/<path> -o FILE`) holds its entry counts, as a
  bundle run's does, so its dashboard row says whether the entry was fetched,
  skipped, reused, failed or not run. An entry streamed to stdout records
  `counts: null`.
- **Cache: iroh-blobs 0.104 and bao-tree 0.17.** iroh-blobs ends an
  `export_bao` stream after the leaf that completes the requested ranges and
  no longer sends `Done`, so the serve path reads the end of the stream as a
  complete export. An export the store stops sending early arrives as an
  `UnexpectedEof` error from iroh-blobs instead. That error fails the serve
  but does not quarantine the blob, because it is a store fault and not a
  short data file. iroh and its QUIC crates stay at 1.3.0: the noq gap-cap
  fix (n0-computer/noq#866) is merged but not yet released.
- **Dev builds carry line tables only, and dependencies no debuginfo.**
  `[profile.dev]` sets `debug = "line-tables-only"` and
  `[profile.dev.package."*"]` sets `debug = false`, so a panic backtrace and a
  test failure still name `file:line` but a debugger shows no locals. Measured
  on a 16-core `cargo build --workspace --all-targets`: a clean build 329s ->
  204s, a rebuild after a one-line `decdn-protocol` edit 75s -> 44s, and
  `target/` 44 GB -> 11 GB. `CARGO_PROFILE_DEV_DEBUG=true` restores full
  debuginfo on workspace crates. Release builds do not change.
  - Why: every one of the ~100 integration-test binaries links the
    debuginfo, so it dominated dev build time, disk, and the CI cache.
- **`decdn-e2e` finds `contracts/` at run time.** `chain::contracts_dir` reads
  `CARGO_MANIFEST_DIR` from the test process, not with `env!`, so the checkout
  path is not part of the library's compiled output. A compiler cache (kache,
  sccache) reuses `decdn-e2e`, and every test binary that links it, in a new
  worktree.
- **Gateway: `--metrics-listen` works on every gateway, and serves
  `/_status` beside `/metrics`.** A loopback gateway counts
  its requests too, and the flag no longer needs `--public-url`. The
  listener answers only its own loopback names in `Host` and refuses any
  other with 403, `/metrics` included, so a scraper addresses it by
  `127.0.0.1:<port>`, `localhost:<port>`, `[::1]:<port>` or the listen
  address. A non-loopback address is refused when the flag is parsed.
- **Gateway: a public gateway answers every origin (#2628).** `decdn gateway
  --public-url` adds `Access-Control-Allow-Origin: *` and exposes
  `Content-Length`, `Content-Range`, `Accept-Ranges`, `Content-Type`,
  `Content-Disposition`, `ETag`, `Retry-After` and `Link` on every answer,
  and `OPTIONS` is a 204 preflight that admits `GET`/`HEAD` with `Range` on
  content, and `PUT` with `content-type` on the bare host's `/_pointer`. This
  is a behaviour change of the public gateway: `OPTIONS` answered 404 on the
  bare host and 405 on a site host before. A proxy in front must pass
  `OPTIONS` and these headers through, and add no CORS headers of its own. A
  loopback gateway is unchanged.
  - Why: a web page, and the TypeScript SDK
    ([decdn/sdk-typescript](https://github.com/decdn/sdk-typescript)) in a
    browser, read content and the 402/503 fallback URI straight from a public
    gateway.
- **sponsord joins the workspace.** The sponsorship daemon and its onramp,
  which were the `decdn/sponsord` repository, are workspace members:
  `sponsord` (the daemon binary), `sponsord-onramp` (the public onramp
  binary), `sponsord-core`, `sponsord-api` and the unpublished
  `sponsord-e2e`, with their history. They depend on `decdn-client`,
  `decdn-common` and `decdn-incentive` by path, so a change to a shared type
  is one pull request, and they ship with every release: the `sponsord` and
  `sponsord-onramp` Linux archives join `SHA256SUMS`, and the
  `ghcr.io/decdn/sponsord` and `ghcr.io/decdn/sponsord-onramp` images are
  two more stages of the root Dockerfile, signed, tagged and mirrored to
  Docker Hub like the others. The crates.io names do not change; their
  `repository` metadata is this repository. Their guides live under
  `docs/sponsord/`, the Compose and systemd material under
  `deploy/sponsord/`, and the OpenAPI snapshots inside `sponsord-api`.
  `publish-crates.sh` no longer opens a sponsord bump PR, and sponsord's
  decdn-pin tooling is gone with the pin. The sponsord crates take the
  workspace's `alloy` and `reqwest` entries, so both servers link ring and
  no aws-lc-rs, and `check_crate_edges.py` guards their closures like the
  CLI's; `sponsord-core` installs the rustls provider before it builds a
  provider, and the onramp before its first `reqwest::Client`.
  - Why: every decdn release needed a sponsord bump PR against crates.io and
    a `decdn-e2e` git pin, and sponsord ran its own CI, release pipeline and
    a mirror of this repository's tooling.
- **Node: one warm connection per peer for DHT calls and pull legs (#2475).**
  A node keeps one `cdn/dht/v1` connection per peer, shared by bootstrap,
  bucket refresh, republish and lookups, and one `cdn/client/v1` connection
  per upstream, shared by every pull leg and header handshake. Both maps dial
  on the main runtime, so no pull leg's throwaway runtime owns a connection
  driver. A DHT call whose pinned connection is closed under it redials once
  and never retries a connection it dialled itself; a call that times out, or
  that a lookup round deadline aborts, unpins its connection. The DHT server
  waits 5 s for a connection's first stream and 30 s between streams, the
  `cdn/client/v1` idle window, then closes an idle connection with reason
  `dht-idle`; the 256-request and 5-minute caps are unchanged. A warm
  connection holds one dispatch permit while it idles; `decdn_dispatch_in_flight`
  shows the headroom under `max_concurrent_handlers`.
- **`decdn gateway` runs pulls at once on one long-lived worker (#2458).**
  The endpoint, the buyer-pool store and the connections to nodes are built
  once per gateway run instead of once per request, and up to 16 pulls stream
  at once instead of one. A response whose reader stops (a preloaded
  `<video>`, a background tab) holds only its own pull until the no-progress
  give-up, so other requests no longer wait behind it. Pulls under one
  capability that overlap share each lane's voucher ledger, so the node
  rejects none of their vouchers as `AmountRegression`.
- **Client builds link one TLS backend and no JSON-RPC server (#2454).**
  `decdn-client` and the `decdn` CLI link ring alone: alloy's default
  `reqwest-rustls-tls` and the workspace's reqwest `rustls` feature each
  compiled in aws-lc beside the ring that iroh's QUIC stack already uses.
  reqwest is now built with `rustls-no-provider`, which panics in
  `Client::build` until a process-wide rustls provider is installed, so
  `decdn_common::tls::install_crypto_provider` installs ring. Both binaries
  call it at start, and the SDK's provider builders and registry discovery
  call it before they build a client; a provider the host application
  installed first stays in place. The daemon's chain RPC moves from aws-lc to
  ring as well; its S3 client keeps aws-lc. `decdn-common` puts the admin
  RPC's server half (`AdminRpcServer`, `jsonrpsee-server`) behind a new
  `admin-server` feature that only `decdn-node` enables. The crate-edges check
  now resolves the `decdn-cli` and `decdn-client` closures per crate with
  `cargo tree` and fails on `aws-lc-*` or `jsonrpsee-server` in either.

- **Published crate docs read for crates.io and docs.rs users (#2442).** The
  crate roots of `decdn-protocol`, `-incentive`, `-node`, `-reputation`,
  `-bao-range`, `-config-types` and `-cache` each open with a one-sentence
  purpose and a map of the crate's API, and link ADRs by absolute URL. Six of
  them map it as a module or entry-point table; `decdn-reputation`, whose API
  is one type and its config, names them in prose.
  `decdn-protocol` and `decdn-incentive` each carry a compiled example (frame
  round-trip, voucher sign and verify). docs.rs builds `decdn-incentive` with
  the `redb` feature, so `buyer_pool_redb` and `buyer_pool_table` render with
  feature badges. The `decdn-node`, `-bao-range`, `-config-types`,
  `-incentive` and `decdn-e2e` package descriptions say what the crate is.

- **`decdn-client`: a delegated fetch or bundle pull takes a credential swap
  mid-run (#2429).** Every lane reads the `CredentialSlot`'s current key and
  capability when it is built, so a credential the application swaps in takes
  over at the next voucher boundary. Before, lanes signed under the key the
  fetch started with. A fetch shares one slot across all its lanes, and a
  bundle pull shares it across all its entries. The slot records a swapped-in
  key's earlier spend from the buyer store. A bundle pull prices `RunningLow`
  on the bundle's undelivered download (the entries that still fetch after the
  disk check, less resumed partials) at the highest rate any entry was quoted,
  so the signal fires before the bundle stalls and every entry reports the
  same need. `decdn fetch` and `decdn bundle pull` swap in no credential and
  behave as before. The `bundle` example swaps one in on `RunningLow`.

- **`decdn-client`: the paid fetch's network leg lives in the SDK (#2429).**
  Holder discovery and probing, lane building, pool open-or-reuse and refill,
  the funding recovery step, watermark persistence and the per-provider
  stream cap move from the `decdn` CLI into hidden `decdn-client` modules
  (`session`, `targets`, `lanes`, `funding`, `lane_sources`, `stream_cap`,
  `ordered_writes`). `decdn fetch` and `decdn bundle pull` drive them, and
  behave as before, with one exception: the moved code logs under
  `decdn_client::…` targets, so a module-scoped `RUST_LOG` filter that named a
  `decdn_cli::commands::fetch` or `cli_sources` target names the new module.
  The lanes take any `BuyerPoolStore`, so the client needs no `redb` feature.
  The public API does not change.
- **Release: `sign-release.sh` and `publish-crates.sh` live in `scripts/`.**
  Run them as `scripts/sign-release.sh <tag>` and
  `scripts/publish-crates.sh <tag>`. `.github/scripts/` keeps only what
  workflows and hooks run.
- **Both container images run on Debian 13 (`trixie-slim`, glibc 2.41)
  (#2409).** The binaries are the same cross-built release archives, so
  nothing changes for an operator who runs the daemon or CLI as shipped. An
  image built `FROM` either of ours now inherits trixie's packages and apt 3.
- **Release: `sign-release.sh` signs with your `@decdn.org` key from `KEYS`
  by default (#2391).** With `DECDN_SIGNING_KEY` unset, the script picks the
  one secret key that can sign, has a live `@decdn.org` uid and is published
  in `KEYS`. With no such key, it picks the one secret key that can sign and
  is published in `KEYS`, whatever its uids. Two or more candidates at either
  step stop the script with every fingerprint listed; it no longer falls back
  to the first secret key gpg lists. A key that `KEYS` marks revoked or
  expired counts as unpublished, even when the local copy still looks live.
  gpg failures while listing keys now stop the script with gpg's own error
  instead of reading as "no key". `DECDN_SIGNING_KEY` keeps priority, and
  `==> Signing as` names the rule that chose the key.
  - Why: a maintainer keyring usually holds a personal key too. When that key
    sorted first, the script picked it and died at the KEYS check, so every
    run needed `DECDN_SIGNING_KEY` set by hand. Keyring order also decided
    between two published keys during a key rotation.
- **Toolchain: Rust 1.99.1 is the pinned toolchain and the MSRV (#2378, #2545).**
  `rust-toolchain.toml`, `rust-version` and every `dtolnay/rust-toolchain`
  step move from 1.95.0 together. Rust 1.97 makes v0 symbol mangling the
  default, so a profiler or debugger that reads `decdn-node` symbols needs a
  v0-aware demangler. CI and the pre-commit hooks deny warnings through
  `CARGO_BUILD_WARNINGS=deny` (cargo's `build.warnings`) instead of
  `-- -D warnings` and `RUSTDOCFLAGS`. The setting is not part of the build
  fingerprint, so a plain local `cargo clippy` and the hook share one build.
  CI no longer sets `CARGO_INCREMENTAL`: cargo turns incremental compilation
  off itself when `CI` is set. Tests whose value is `Debug` assert on enum
  shapes with `assert_matches!`, which prints the value it got when the match
  fails. The two new pedantic lints `unused_async_trait_impl` and
  `assert_is_empty` are allowed, because their rewrites read worse than the
  code they replace.
  - Why: 1.96.1, 1.97.1 and 1.98.1 each fix a miscompilation, 1.96 fixes
    Cargo CVE-2026-5222 and CVE-2026-5223, and 1.99 moves to LLVM 23 with a
    rustdoc its release notes put at 20% faster on average (up to 40%). No dependency was held
    back by the 1.95 floor.
- **Node: every cache-miss refusal logs one info line that names its path
  (#2282).** A node that refuses a paid request from a serve-miss exit
  (reason `cache_miss`, or `internal_error` when a fill tier faulted) now logs
  `serve-miss: refusing the request` with `hash`, `byte_offset`, `byte_len`,
  `reason` and `path`. `path` is `coverage_gate`, `pull_leg_miss`,
  `pull_leg_timeout`, `buffered_miss`, `no_pull_through` or `no_lane`. A
  `pull_leg_miss` line adds `cause` (`clean_miss`, `below_margin` or
  `local_fault`). A `coverage_gate` line adds `pull_offset`, `pull_len`,
  `candidates` and `first_uncovered_chunk`; the separate coverage-gate detail
  line is gone. `no_lane` (the request carries no binding, or this node holds
  no lane for its signer, so no fill tier ran) is a path any peer can reach, so
  its line is throttled to one per minute with a `suppressed` count. Before,
  only the coverage gate logged at info, so most refusals showed only as a
  tick on `decdn_serve_stream_rejected_cache_miss_total`. Other `NotFound`
  refusals (`pull_loop_guard`, `load_shed_miss`, the channel-class reasons)
  keep their own logging. The wire answer and the reject counters do not
  change.
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
  borrows now says so — `TableDefinition<'_, …>` across `pool_store`'s eleven
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

- **The node's upstream probe budget is 1 s (#2366).** `PROBE_TIMEOUT` was
  500 ms. A probe dials a fresh connection, so it costs two RTTs (the
  connection handshake and the request); at inter-continental RTTs that sits
  at or past 500 ms, and a node whose only candidate was a far origin dropped it as
  unreachable and signed `NotFound` for content that origin serves. On the
  testnet, 106 of hel1's 156 probe rounds in one day overran the old ceiling.
  The outer pull-through deadline at defaults is 168 s (was 167.5 s). ADR 001
  § Probe response collection and ADR 005 carry the new figure.
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
  `probe_rate_limit_rejected_*`, `pool_open_failures_*` and the gossip
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
  `pool_open_failures_*`) remain sibling counters for now.

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
- **`decdn node lanes` has a `SIGNER` column.** It reports the lane's voucher
  signer — the key whose signature is required on every voucher — next to
  `COUNTERPARTY` (the pool owner, and the address the ADR 011 compliance gates
  check). The two are equal only when the owner signs its own vouchers, as a
  node buyer does. A `decdn get` on its own wallet's pool signs with a key
  derived for its data dir, and a `decdn get --capability` signs with the
  delegate key the capability names. **Output-breaking for scripts:**
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

- **Release: `cargo binstall` verifies each archive's minisign signature (#2765).**
  `sign-release.sh` now signs every archive with the maintainer minisign key,
  in the same run as `SHA256SUMS.minisig`, and uploads `<archive>.minisig`
  beside it. The four binary crates' `[package.metadata.binstall.signing]`
  names that key, so binstall refuses an archive whose signature is missing or
  does not verify. `sign-release.sh` reads the key from the tag's manifests
  before it signs anything, verifies every minisign signature with `-H` (the
  prehashed format `decdn self-update` and binstall accept), and refuses a
  release whose archives do not verify under the key. `publish-crates.sh`
  refuses a release missing an archive `SHA256SUMS` names or its `.minisig`,
  and runs `check_binstall_metadata.py` at the tag before the first upload.
  `check-binstall-metadata.sh` also checks that the crates name the same key,
  that it is one of `crates/cli/minisign-*.pub`, that it sits on the one line
  form `sign-release.sh` reads, and that no override carries its own
  `signing`. The key follows whoever signs: RELEASING.md § The minisign keys.
- **Release: `cargo binstall` installs the release archives (#2764).** `decdn-cli`,
  `decdn-node`, `sponsord` and `sponsord-onramp` carry
  `[package.metadata.binstall]`, so `cargo binstall <crate>` downloads the
  GitHub Release archive for the target instead of compiling. The fallback to
  third-party cargo-quickinstall builds is off; a target with no archive
  compiles from source. Windows overrides are keyed by exact target triple,
  which every binstall release reads. `.github/scripts/check-binstall-metadata.sh`
  (in the `packaging` CI job and pre-commit) fails when a crate's `pkg-url`,
  `pkg-fmt` or `bin-dir` stops naming what release.yml's `build-binaries` job
  ships, including its archive step lines, or uses a placeholder binstall does
  not offer that key. `decdn self-update` now names
  `cargo binstall --locked decdn-cli` next to `cargo install --locked decdn-cli`
  when it refuses a binary in a cargo bin directory.
- **CLI: `decdn setup` checks the buyer wallet's USDC on a pull-through node
  (#2676).** A node with pull-through on buys its cache misses from peers.
  Opening the pool those pulls pay from escrows
  `blockchain.buyer_working_deposit_micro_usdc` (10 USDC by default). Setup
  now prints a `buyer_usdc` pre-flight row with the working deposit and the
  wallet's USDC (`PaymentPool.usdc()`). Below the deposit, the row warns with
  the shortfall, the address to fund and, on the testnet, the USDC faucet. It
  reads the pool as `decdn-node` does (`DECDN_PAYMENT_POOL_ADDRESS`, else
  `blockchain.payment_pool_address`) and flags a missing or zero pool address
  or a zero deposit, which the node refuses to start with. The row never fails
  the run. `--json` reports it as `preflight.buyer_usdc`: `null` when
  pull-through is off, else an object whose `state` is `covered`, `short`,
  `no_wallet` (the keyless dry run), `config_error` or `read_error`.
- **Node: metrics for the settlement re-anchor path (#2710).** The settlement
  watcher re-reads each pool that it seeded from one `getPool` after a restart,
  at a pinned block. Before, it reported the outcomes only in logs, so a pool
  that stayed pending for good was invisible. A reorg that dropped the open,
  a reorg that lowered `totalRedeemed` or the deposit, or a read that keeps
  failing are such cases. New series:
  `decdn_settlement_reanchor_pending_pools` (gauge, set each tick), and the
  counters `decdn_settlement_reanchor_{block_read_failures,
  behind_watcher, pool_read_failures, absent, skipped_below_folded,
  skipped_lower_redeemed, skipped_lower_deposit, anchored}_total`. A skipped
  read now also logs a `warn!` that names the pool, the held value and the read
  value. See `adr/appendix-observability.md`.
- **Gateway: `GET /_status`.** `/_status` is one JSON document of what the
  gateway holds and did: its counters, the pulls in flight, the
  capabilities, a public gateway's allowance buckets and pointers, and the
  buyer-pool store's pools and lanes. A loopback gateway answers it on its
  own host behind the gateway token. `--metrics-listen` serves it too, with
  no token on a public gateway and behind the token on a loopback one. A
  public gateway's public host does not answer it. See
  [`adr/appendix-gateway.md` § State](https://github.com/decdn/decdn/blob/main/adr/appendix-gateway.md#state).
- **CLI: `decdn dashboard`, one page of this host's pulls, gateway and
  node.** It serves a page on `127.0.0.1:8485` (`--listen`) that shows the
  `decdn get` runs on the host, live and finished; the gateway at
  `--gateway` (default `http://127.0.0.1:8484`), from its `/_status`, when
  one answers, with the gateway token sent to a loopback address only; and the local node's health, `node top` metrics, lanes,
  earnings, buyer pools and slashes, from its admin RPC and `/metrics`, when
  one runs. The dashboard process reads every source and the page reads
  only the dashboard's own `/_status`, behind the dashboard's token
  (`~/.decdn/dashboard/token`); the printed
  `http://127.0.0.1:8485/#token=<token>` link hands the page the token. The
  page only reads. It is a Vite app in `crates/cli/dashboard/`, embedded by
  `decdn-cli`'s build script; a build with no Node toolchain embeds a page
  that says how to build it, and no release ships that page. A finished
  pull shows the bytes it downloaded apart from those it resumed or reused,
  and a bundle pull its entry counts (`11 skipped, 9 excluded`), failed and
  not-run counts in red (#2638, #2639). A file in the records directory that
  is not a record shows with its path and why (#2666). See
  [`adr/appendix-binaries.md` § Dashboard](https://github.com/decdn/decdn/blob/main/adr/appendix-binaries.md#dashboard).
- **CLI: every `decdn get` run keeps a record for the dashboard.** A run
  writes `~/.decdn/activity/pulls/<start-ms>-<pid>.json`, whatever its
  `--data-dir`, and rewrites it every second: the URI, the destination, the
  bytes verified, split into downloaded, resumed from an earlier run's
  `.partial`, and reused from files on disk (an interrupted run's
  unfinished blobs included), what each node was paid, and at the end the
  result, with a bundle run's entry counts (also when it has nothing to
  fetch). A run
  that cannot write its record pulls on, and warns once. A run prunes the
  directory to the newest 200 records when it starts. `--dry-run` writes
  none.
- **CLI: `decdn get` ends with a pull receipt, and `decdn receipt
  {verify,settle}` checks and settles it (#2599).** Every `get` that paid
  prints a receipt block: the hash, the bytes verified and paid, each node
  and the bytes it was paid for, the USDC and pool, and the start and end
  times. `--receipt <FILE>` writes the receipt as JSON (`decdn-receipt/1`):
  per `(pool, signer, provider)` lane, the node's first signed
  `StreamResponse` for each hash it served and a `proof` of the lane's claim
  (the voucher that signs its anchor and the deepest revealed chain
  preimage). The path is checked before the pull pays anything, and may not
  name the blob's destination or a bundle entry's. A run that succeeds
  without paying writes no receipt, says so, and removes the file an earlier
  run left at that path; a failed run and a `--dry-run` leave it alone. A pull that fails after paying still gets a receipt,
  marked incomplete, that lists the started blobs that did not land. A
  lane's prior watermark moves with every jump to a node's watermark, so a
  jump never counts as the pull's payment. `decdn receipt verify <FILE>`
  re-checks every signature offline against the receipt's chain id,
  `PaymentPool` and `SlashJudge` (and, on a chain the build knows, that those
  are its contracts; on any other chain it says they are unchecked), and
  exits non-zero on any fault. `decdn receipt settle <FILE>` verifies the
  receipt first, then scans `PaymentPool.PoolRedeemed` from an hour before
  the receipt's start and rebuilds each lane's settlement from the events it
  reads: the redemption that covers its claim, or the highest one short of
  it. The block goes to stderr when stdout
  carries data (`-o -`) or JSON (`--json`). The SDK side is
  `decdn_client::receipt` (`ReceiptEvidence`, `PullReceipt`) and
  `BundlePullerBuilder::receipt_evidence`. What a receipt proves, and that it
  adds no slashing offense or evidence path, is in
  `adr/appendix-pull-receipts.md`. The date parser behind it also backs
  `decdn node earnings --since`, which refuses a signed year (`+202-10-25`).
- **CLI: `decdn quote <URI>` prices a pull from the nodes' signed probe
  rates, before any paid byte (#2600).** One unpaid `cdn/probe/v1` round
  reaches every node a pull can draw from (the peer store's or the registry's
  list, narrowed by the region allowlist, not sampled down) and verifies each
  answer's signed rate against its operator. The quote prints the size, how
  many nodes hold the content (and how many did not answer or rate-limited
  the probe), their rate range in USDC per GB, the `--max-rate-per-mb`
  ceiling, and the cost: every byte at the cheapest rate, at the most
  expensive, and the likely cost split evenly over the first `--max-sources`
  nodes in the order a pull that probed every node would try them, one node
  per operator. Costs are on bao wire bytes, as the nodes meter them, for a
  fresh pull: files already on disk still count. A blob's size is the nearest
  holder's report. A bundle's size is its manifest's planned download, with a
  duplicate blob and a shared chunk counted once; the quote probes the 4
  largest blobs and prices the rest, and any probed blob with no usable rate,
  at their mean rate, because the reference node signs one rate for every
  hash. The manifest comes from the output directory's cache, or from a small
  paid pull from the buyer's open pool that caches it for the `get` that
  follows; with no open pool, it opens one only under `--open-pool` (#2675).
  `--json` gives every amount in micro-USDC. `decdn get --dry-run` prints the same block, on a blob
  URI too, where `--json` also applies with `--dry-run`; it sends no
  transaction and opens no keystore, but reads the registry when the peer
  store cannot supply it, and probes. Its `--json` plan carries `quote` and
  `quote_error`. Without a cached manifest it names the `decdn quote` command
  to run. `get` prints a one-line `Estimate:` on stderr before the pull (not
  under `--json`), from the rates its own probe round and the peer store
  hold. In `decdn-client`: the `quote` module (`census`,
  `HashCensus::serve_order`, `price`, `wire_bytes`) and
  `bundle::planned_downloads`.

- **CLI: `decdn node earnings` prints settled and owed USDC per period
  (#2598).** The table has one row per period (the last 24 hours, the last
  7 days, and since the node's settlement ledger started) with the USDC the
  operator received, the bytes those settlements paid for and the settlement
  count, then the last settlement (time, tx, block) and the pending line:
  what the lanes still owe and when the next redeem sweep is due. `--json`
  prints the `admin_v1_earnings` response; `--since <YYYY-MM-DD|unix>` adds a
  period. `--address <0x…>` builds the same table from the chain alone (no
  daemon, no pending line): a windowed `FeeRouter.Settled` scan for that
  operator from the deployment's L2 block, with the net per settlement
  reconstructed from the fee split in force; slow on an RPC that caps
  `eth_getLogs` windows, so progress prints to stderr and `--from-block`
  shortens it. Behind it:
  - the daemon keeps a forward-only **settlement ledger** (`settlement_ledger_v1`
    in `settle.redb`): one row per `redeemMany` that paid, decoded from the
    receipt (gross, bytes and epoch from `FeeRouter.Settled`, the exact net
    from the USDC transfer to the operator, the block's timestamp). The
    `PoolRedeemed` watcher backfills a row for a landing the redeemer never
    saw, off the poller's task; shutdown waits for the backfills in flight, so
    only a crash mid-catch-up can lose one (`--address` has the chain's
    record). Each row names its operator, so after `decdn
    node rotate-key eth` the earlier address's rows stay in the file but out
    of the figures. The file also stamps when the ledger started recording
    (`settlement_ledger_start_v1`), and both drop with the other seller tables
    on a `PaymentPool` repoint. Nothing is backfilled from before the
    watcher's first scan, so the bottom row reads `since <date>`, never "all
    time", and a period that starts earlier reads `(covered from <date>)`
    (#2624). The date is when the ledger started (or an earlier row the
    watcher backfilled), not its first settlement: once the ledger has run
    for 7 days, the 7-day row has no note, whether or not anything settled in
    it. In `--address` mode the date is the first scanned block's time;
  - the admin RPC gains `admin_v1_earnings` (and `EARNINGS_UNAVAILABLE_CODE`):
    `covered_from_unix` (the time the figures cover from), named
    periods `last_24h`, `last_7d`, `since_ledger_start` and the optional
    `since`, the last settlement, and `pending` split into what the next sweep
    can redeem and what is stranded (expired capability, another provider's
    lane). `LaneSnapshot` gains `unredeemed_micro_usdc` (`owed −
    paid_cumulative`, the per-lane term of the pending total), which `decdn
    node lanes` shows as an `UNREDEEMED` column;
  - the redeemer publishes its sweep schedule for the "next sweep in" and
    "last sweep" figures;
  - two counters: `decdn_settlement_ledger_write_failures_total` and
    `decdn_settlement_ledger_skipped_rows_total`, so a ledger that stops
    growing is visible to a dashboard and not only to `node earnings`;
  - `decdn node status` appends an `earnings=ok …` / `earnings=unavailable
    reason=…` line (text mode only) and `decdn setup` closes with
    `earned_so_far=` when a daemon answers, or `earned_so_far=unavailable
    reason=…` when one is listening but its answer is an error, late, or does
    not decode; only a refused connection prints nothing. Both lines render a
    daemon's JSON-RPC error as its code and message.
- **CLI: a namespace is a stable host on a public gateway (#2614).**
  `decdn publish point <ns> decdn:<hash>/ --gateway <origin>` signs a
  namespace pointer (an EIP-712 record over the `PublisherRegistry`
  deployment: `namespaceId`, `root`, `seq`) with the namespace owner's
  keystore and `PUT`s it to the gateway as `/_pointer`. The gateway recovers
  the signer, checks it against `PublisherRegistry.ownerOf`, keeps the
  pointer in `<data-dir>/pointers/<ns>.json` (bound to the chain and
  registry it was verified on: a data dir moved to another deployment stops
  the gateway instead of serving its pointers), and from then on serves the
  bundle as a website at `<ns>.<host>`. A pointer at a `seq` not above
  what the gateway stored from the same signer is 409 before any chain
  read; one from another signer who owns the namespace now replaces the
  held one whatever the holder's `seq`; a stranger's is 403; the owner
  lookups are budgeted per minute (429 past it), with a budget of its own
  per namespace for a signer stored before. On a namespace host,
  `/_decdn/` content from another bundle spends the shared allowance. `decdn gateway` takes `--publisher-registry-address`
  (default: the config file, else the built-in testnet) and
  `--allowance <ns>=<bytes>`, repeatable, which gives one namespace a daily
  allowance of its own for the requests its pointer serves, in place of the
  shared one; a `?ns=` or a `<ns>.<hash32>.` label still spends the shared
  one. `decdn_gateway_pointer_updates_total` counts stored pointers. The
  design is in `adr/appendix-gateway.md` § Namespace pointer. Part of #2593.

- **CLI: the public gateway serves its metrics on a loopback listener
  (#2611).** `decdn gateway --public-url … --metrics-listen <addr>` serves
  Prometheus text at `GET /metrics`: content bytes served, answers by outcome
  (ok, not found, refused, allowance exhausted, pool cannot pay, upstream
  failed) as one unlabeled sibling counter each, and pulls in flight as a
  gauge. The listener takes a loopback address only. The series are listed in
  `adr/appendix-gateway.md`, the gateway's new appendix. Part of #2593.

- **CLI: the gateway answers `Range` on a sized bundle entry (#2612).** An
  entry whose manifest declares its size takes one `Range` (`bytes=a-b`,
  `bytes=a-`, `bytes=-n`): the response is 206 over exactly that window, with
  `Content-Range` and the window's length, and the pull pays only for the
  chunk-group-aligned span around it (ADR 038). A range past the end is 416.
  A range the gateway cannot honour (several, another unit, a rewritten page,
  a blob of unknown size) is ignored and the whole body is served. A sized
  response advertises `Accept-Ranges: bytes`, on `HEAD` too, so `curl -r`, a
  resumed download and a BEP 19 webseed work. Part of #2593.

- **CLI: `decdn gateway --public-url` serves anyone from its own pool
  (#2608).** A public gateway serves a domain behind a TLS-terminating
  proxy: `<origin>/<hash>[/<path>]` on the bare host, every bundle as a site
  at `[<ns>.]<hash32>.<host>`, and a namespace host that answers 404 until it
  has a pointer. It takes no token and no capability: every request pays from
  the keystore's own pool, and `--allowance-bytes-per-day` bounds the bytes it
  delivers per UTC day. A request past the allowance, or an entry that does
  not fit what is left, answers 402 before any pull; a stream of unknown size
  is cut when the allowance runs out; a pool that cannot pay answers 503. Each
  names the canonical `decdn:` URI in a `Link` header and a JSON body. `HEAD`
  is answered from the bundle manifest at no pull of the entry. The buyer
  store lives in `<data-dir>/store`, so the pool survives a restart. Loopback
  mode is unchanged. Part of #2593.

- **CLI: `decdn gateway` serves every bundle as a website on its own origin
  (#2457).** `http://[<ns>.]<hash32>.localhost:<port>/` (`<hash32>` is the
  manifest hash in lowercase unpadded base32) serves the bundle's
  `index.html`, its entries by path, and a directory's `index.html`, with
  HTML, CSS and scripts as themselves under a policy that confines the page
  to its origin. It works in a plain browser: `Sec-Fetch-*` lets the site's
  own requests, typed URLs and top-level navigation spend, refuses every
  other cross-origin request, and pays with the capability covering the
  site's host, `*` included. A site's `decdn:` URIs in HTML attributes,
  `<style>` and CSS are rewritten as they stream to `/_decdn/<hash>…` on the
  site's origin, which serves the page's requests and redirects a navigation
  to the other bundle's site. A tokenless `GET /<hash>/[<path>]` on the
  gateway origin redirects to the site.

- **Client: `Streamer::open_range` streams one verified byte window (#2607).**
  A caller that knows a blob's size opens a consumption-paced stream over
  `[offset, offset + len)` instead of the whole blob. The fetch opens, and
  pays for, only the chunk-group-aligned span around the window (ADR 038);
  the reader hands out exactly the window, a zero length is a tail to the
  proven size, a window is never teed to the cache, and a whole-blob cache
  hit serves the window from memory. `open` is the whole-blob window. This
  is the client half of the HTTP `Range` the public gateway answers (#2593).

- **CLI: `decdn self-update` and a daily update notice (#2537).**
  `decdn self-update` resolves the latest GitHub release, verifies its
  `SHA256SUMS.minisig` against a maintainer's minisign public key embedded
  from `crates/cli/minisign-<maintainer>.pub`, checks the archive against the
  signed manifest,
  and replaces the running binary in place (`--check` only reports). A binary
  `cargo install` placed is refused with `cargo install --locked decdn-cli`;
  one in a directory the user cannot write (the container image, a managed
  fleet path) is refused by path. A downgrade or a pre-release is never
  applied. At most once a day (stamp `~/.decdn/self-update-check`) the CLI
  looks the latest release up alongside the command and, once the command
  succeeds, prints one stderr line naming the newer version and
  `decdn self-update`; the command waits at most one second for the lookup,
  and a `get` outlasts it. Never for `--json` output and never when stdout or stderr is not a
  terminal. New config section `[self_update]` with
  `check` (default `true`), overridden by `DECDN_SELF_UPDATE_CHECK`.
  `DECDN_RELEASES_BASE` points a test at another release page.
- **Release tooling: `SHA256SUMS.minisig` and pre-release marking (#2537).**
  `scripts/sign-release.sh` signs `SHA256SUMS` with the minisign key
  `DECDN_MINISIGN_KEY` names (default `~/.minisign/minisign.key`), verifies
  it against the `crates/cli/minisign-*.pub` maintainer keys in the tag's tree
  (the keys the released binary embeds), and uploads it beside the `.asc`
  files;
  It also verifies the signature against the keys in the previous tag's
  tree, the keys installed binaries embed, so a key new in a release cannot
  sign that release. `scripts/publish-crates.sh` requires it. A version with a
  pre-release suffix is created and published with `--prerelease`, so it never
  becomes GitHub's "latest" release.
- **Node: `decdn_buyer_topup_unresolved` gauge (#2493).** It reads 1 while
  the node holds its top-up slot for a `topUp` whose outcome the chain has not
  resolved, and 0 again once the settle ends, including when its task is
  aborted. `adr/appendix-observability.md` and `docs/runbook.md` describe what
  keeps it at 1 and what the operator does.
- **CLI: `decdn get decdn:<hash>/<path> -o -` streams one bundle entry to
  stdout (#2467).** The command reads the bundle manifest into memory (at
  most 64 MiB, and the manifest fetch requests and pays for no more), then
  streams the entry's verified blob to stdout. Both fetches share one payer,
  so the keystore password is read once. The bundle flags are an error with
  `-o -`, and `-o -` on a whole bundle (`decdn:<hash>/`) stays an error.
- **CLI: `-o` is optional on `decdn get` (#2466).** Without `-o`, a blob
  lands at `./<hash>`, a bundle in `./<hash>/`, and an entry URI
  (`decdn:<hash>/<path>`) at `./<name>`, the last segment of its path, with
  no parent directories. A bundle or entry pull without `-o` refuses a path
  that holds a file no earlier pull left there unchanged, unless
  `--overwrite` (#2472). The earlier pull must be of the same entry (the
  same manifest hash and manifest path), or have written the same bytes. A
  new version of a bundle has a new hash, so it lands in its own directory;
  `-o <old dir>` updates the old one in place: it replaces the files that
  changed and skips the rest. `--overwrite` fetches every file again. The
  error says why: no pull wrote the file, it changed after a pull wrote it,
  or a pull wrote it from another entry, named as a `decdn:` URI. So `decdn get
  decdn:<h>/tokenizer/config.json` does not replace the `./config.json`
  that `decdn:<h>/text_encoder/config.json` wrote. Each
  `.decdn-manifest.json` record names its manifest hash and manifest path.
  A `.decdn-manifest.json` an older build wrote vouches for no file, so the
  first pull without `-o` into such a directory refuses its files, and the
  error names the version mismatch; name the directory with `-o <dir>` once
  to pull there. The pull leaves its `.decdn-bundles/`,
  `.decdn-manifest.json` and `.decdn-partial/` in the output directory.
  Without `-o`, the command prints `Downloading to <absolute path>` on stderr
  before the pull starts (not under `--json` or `--dry-run`). With `-o`, each
  target lands where `-o` names. A bundle entry at `.decdn-manifest.json` or
  under `.decdn-bundles/` fails, like one under `.decdn-partial/`: those
  paths hold the pull's own state.

- **Release: Docker Hub overviews for `decdn/decdn-node` and `decdn/decdn`
  (#2440).** Each image's Hub short description and overview live in
  `docker/README.<stage>.md`. `sign-release.sh` pushes them after the Docker
  Hub mirror, from the files as committed at the tag, and reads them back
  without credentials. A prerelease leaves them alone. A failed push only
  warns; running `scripts/dockerhub-description.py` by hand repeats it. The
  push logs in with `DECDN_DOCKERHUB_USERNAME` and `DECDN_DOCKERHUB_TOKEN`
  when both are set, else with the `docker login docker.io` credential, and
  needs `python3`. `DECDN_SKIP_DOCKERHUB` skips it with the mirror.
  `check-image-name.sh` fails an image with no overview file.
- **`decdn-client`: a public bundle pull, `decdn_client::bundle` (#2429).**
  `BundlePuller` pulls every blob of a bundle manifest into a directory over
  one paid run, with what `decdn bundle pull` does: fetch-once per blob,
  hard-link duplicates, whole-file and range dedup against the output
  directory, resume from `.partial` prefixes and the skip-cache, a jobs cap
  and a per-provider stream cap, and the funding recovery second pass.
  `select_nodes` resolves the pinned node or the run's registry list first, so
  a failed discovery costs no key unlock. `Funding` is the buyer's own pool or
  a delegated `CredentialSlot`. Progress reaches a `BundleObserver`, a
  `CancellationToken` stops the pull, and the result is a typed
  `BundleOutcome`: how it ended (`BundleEnd`: `Complete`, `Interrupted`,
  `FundingNeeded`, `Stopped`), the pool replacements, and a `BundleReport`. The new public `decdn_client::session` module holds the
  inputs: `ChainConfig`, `PullSite`, `DepositPolicy`, `DiscoveryOptions`.
  `decdn bundle pull` is now a caller of it; its output, `--json` report and
  exit codes do not change. The pull future is not `Send`.

- **Release: `publish-crates.sh` opens devops' decdn bump PR (#2399, #2421).**
  After the upload, the script clones decdn/devops into a temporary directory
  and runs its `open-decdn-bump.sh <tag>`, which opens the PR that pins devops
  to the new release with the maintainer's `gh` login: the Ansible role, Helm
  chart, Compose and onramp pins. A failure prints the command to run later
  and never fails the publish; a `DECDN_REPO` other than `decdn/decdn` opens
  nothing, and `DECDN_SKIP_DEVOPS_BUMP=1` (or `true`/`yes`) skips it.
  - Why: devops pins decdn at a release tag, and the move was a PR opened by
    hand after the release.
- **`serve_stream` spans time their serve loop's waits by cause (#2348).** A
  serve loop records `store_wait_ns`, `send_wait_ns` and `proof_wait_ns` as
  it ends, with the QUIC path's state: `path_kind`, `path_rtt_us`,
  `path_cwnd`, `path_congestion_events`, and the connection's
  `conn_lost_packets`, `conn_lost_bytes` and `conn_sent_bytes` over the
  loop. A slow stream's trace now says whether the store, the path, the
  client's receive window, or the client's proofs hold it back
  ([appendix-observability § Trace Spans](adr/appendix-observability.md#trace-spans)).
- **`bundle pull` warns on a filter glob that matches no entry, and on
  leftover partials (#2190).** Each `--include`/`--exclude` pattern that
  matches no manifest entry prints one `warning:` line on stderr, in a real
  pull and under `--dry-run`. A pattern matches the whole path from the bundle
  root, so when its `**/` form would match, the warning names it:
  `--exclude 'metal/*' matched no entries (… did you mean '**/metal/*'?)`. At
  run end, staging files under `.decdn-partial/` for blobs the run did not
  select (a partial an earlier run left for an entry this run excluded) print
  one warning with their count and size. The pull keeps them for a later run
  that selects the entry again.
- **`bundle pull --hash --dry-run` checks the filters against a cached
  manifest (#2327).** When an earlier pull into the same output directory
  cached the bundle manifest, the dry run lists the filtered plan and warns
  on each `--include`/`--exclude` pattern that matches no entry, as
  `-i --dry-run` does. The dry run reads the cache even under `--overwrite`.
  With no cached manifest, it prints the bundle hash and output directory it
  would pull into; under `--json` it prints `count`, `entries` and `quote`
  as `null`. A malformed `--hash` fails the dry run, as it fails a real pull.
- **`bundle pull --hash` names a cached manifest it cannot use (#2361).** A
  cache file under `.decdn-bundles/` that cannot be read, or whose bytes fail
  the hash check, fails `--dry-run` with the file path and the reason. A real
  pull prints the same reason as a warning and fetches the manifest fresh. A
  cache write that fails prints a warning that names the path.

- **Paid-leg diagnostics at `debug` (#2211).** Each paid leg's open logs its
  peer, hash, `byte_offset` and `byte_len`. A throughput-floor trip logs the
  range, bytes against expected, the window's bytes, and the gap since the
  previous floor sample. An ingest wait of 1 s or more on a checkpoint slot
  logs its length.
- **`blockchain.get_logs_max_block_span` sets the ceiling on the block span of
  one chain-watcher `eth_getLogs` request** (default 10 000, must be > 0,
  restart-required). Set it to the RPC provider's range limit so the poller
  neither learns it from rejections after each restart nor probes it again
  each time the span doubles back.

- **Chain-event poller span metrics and a flapping-watcher alert.**
  `decdn_chain_get_logs_span` reports the poller's current `eth_getLogs`
  window span and `decdn_chain_get_logs_range_rejections_total` counts the
  windows the provider rejected; the chain dashboard plots both.
  `DecdnChainWatcherFlapping` fires, once per node, when the chain watchers
  enter four or more failure windows in 30 minutes and keep doing so for 45
  minutes: each recovery resets the stalled alerts, so watchers that fail on
  and off otherwise lag head unseen. It uses `increase()`, so a daemon restart
  does not clear it, and a single burst does not fire it.

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
    `decdn_bytes_served_total` (bao wire bytes) for delivered bytes.
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
  - Publishing is a new maintainer-run step, `scripts/publish-crates.sh`,
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
  derive a version from the `trixie-slim` tag.
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

- **CI: every workflow pins its actions, tools and checkouts, and release
  artifacts download by source.** `ci.yml`, `codeql.yml`, `sdk-contract.yml`
  and the `foundry-setup` composite action now pin every action by commit SHA,
  as `release.yml` does. `dtolnay/rust-toolchain` is pinned to a commit and
  names the Rust version in its `toolchain:` input. Every
  `taiki-e/install-action` tool is installed at a fixed version with
  `fallback: none`, and no checkout persists its token. The guard is now
  `.github/scripts/check-workflow-hardening.sh`. It reads every workflow, and
  each workflow needs a `WRITE_JOBS` row naming the jobs that may write.
  Dependabot no longer moves the toolchain through action refs: a Rust release
  arrives as the `deploy/sponsord` `rust:` bump, which `check-toolchain-pin.sh`
  holds red until every site moves. In `release.yml`, `upload-assets` and
  `attach` download each artifact into its own directory. An archive must name
  the target its artifact is named for, and `docker` downloads the two Linux
  artifacts by exact name. An extra artifact can no longer overwrite a real
  file in a merged download.
- **Release: each job holds the narrowest token, and every action is pinned
  by SHA (#2771).** `release.yml` grants `contents: read` at the workflow
  level. Every job that compiles or installs a tool can only read.
  `contents: write` belongs only to `draft`, `upload-assets` and `attach`,
  which use no action but `actions/*`. `packages: write` belongs only to
  `docker`, which runs the `docker/*` actions under it and cannot write release
  contents. Release notes come from the read-only `tag` job,
  and the image SBOMs from a new `sbom` job that can only read packages. No
  checkout in `release.yml` or `security.yml` persists its token. Every action
  in both, and in the `dashboard-build` composite action, is pinned by commit
  SHA, and the QEMU and BuildKit images the `docker` job runs under its
  `packages: write` token are pinned by digest. `cross`, `git-cliff` and
  `cargo-nextest` install at fixed versions that install-action checksums, with
  no unchecked fallback. `upload-assets` and `attach` refuse to replace the
  assets of a release that is no longer a draft.
  `.github/scripts/check-workflow-hardening.sh` holds the action pins, the
  checkouts, which job may write, and the digest of every image an action runs,
  in the `packaging` CI job and in pre-commit; it does not read `run:` steps.
  `check-image-name.sh` also holds the `sbom` and `attach` image lists to the
  docker matrix. `sign-release.sh` refuses an archive
  signature whose signed trusted comment, as `minisign -Q` reports it, does
  not name the archive. SECURITY.md says
  that binstall does not check that name, so the GPG-signed `SHA256SUMS` and
  `decdn self-update` are the rollback-proof paths. Dependabot now also bumps
  the composite actions' refs.
- **client: a bundle entry path must be a portable file name, and the CLI
  escapes control characters in the paths it prints (GHSA-jr93-46mf-88wx,
  #2746, #2705).** Windows joins `README`, `README.` and `README` with a
  trailing space into one file, and also resolves a `:` stream name, a device name (`NUL.txt`) and an
  8.3 short name (`LONGFI~1.TXT`) to something other than the path suggests,
  so two entries could still land at one file there after #2691. A publisher
  could also put terminal escape sequences in a path and have `decdn get`
  print them raw. `BundleManifest::parse` now fails with `MalformedManifest`
  for a path segment that holds a control character or a character Windows
  reserves (`\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`), ends in `.` or a space,
  has a Windows device name as its stem, or has the 8.3 short-name stem form (`decdn_client::bundle::path_segment_problem`). The
  rule applies on every platform. A pull of a manifest built with
  `BundleManifest::new` fails each such entry, and `decdn origin import`
  refuses such a tree before it imports, moves or writes any file. The
  dry-run listing, the `failed:` lines and the progress labels escape control
  characters. A published bundle with such a path no longer pulls; republish
  it with the file renamed.

- **incentive: a voucher that repeats the live root can no longer credit a
  lane past its spending cap (GHSA-jr93-46mf-88wx, #2700).** Such a voucher
  keeps the verified frontier and adopts its own `chunk_price`, so its claim is
  `amount + verified_index × chunk_price`. The node checked only `amount`
  against the cap, so a bump just under the cap credited up to one chain of
  chunks past it, which the contract never pays. The voucher path now refuses
  the full claim above the cap with `CapExceeded`, as a reveal already is.

- **client: a bundle manifest with two entry paths that name one file is
  rejected (GHSA-3c38-gqx6-v7fm, #2691).** A manifest could hold `{p → A}`
  and `{p → B}`, or paths that differ only in case or Unicode normalization.
  With A already at `p`, the pull skipped B too and reported success while
  `p` kept bytes B never matched. `BundleManifest::parse` now fails with
  `MalformedManifest` when two paths name one file on a case- or
  normalization-insensitive file system (`decdn_client::bundle::path_collision`:
  NFD, lower then upper then lower case, NFC), so `decdn get` and the gateway
  refuse the bundle. A pull of a manifest built with `BundleManifest::new`
  fails every colliding entry and writes none of them. A saved file whose path
  names the same file as one the pull writes is no longer a whole-file donor.
  `decdn origin import` refuses a tree that holds such a pair (`README` and
  `readme`) before it imports, moves or writes any file.

- **sponsord-onramp: clients behind the documented proxy no longer share one
  rate-limit budget (GHSA-674v-whmr-xvmw, #2693).** The deploy binds the
  onramp to localhost behind a reverse proxy, but the env example left
  `ONRAMP_CLIENT_IP_HEADER` unset. Every request was keyed to the proxy's
  address, so about four waiting installers filled the poll budget, and one
  client could spend the fund budget for everyone. The env example now sets
  `ONRAMP_CLIENT_IP_HEADER=X-Forwarded-For`. The limits count an IPv6 client
  by its /64, not its full address, and an IPv4-mapped address as its IPv4
  address. The limiter tracks at most 100,000 clients per minute; any more
  share one budget, so a flood of addresses can neither grow its memory nor
  get around the limits. Installer contract: `decdn.sh` and `decdn.ps1` now
  wait out a `429` from `GET /v1/capability` and retry, the wait doubling from
  2 s up to 30 s for up to 600 s, instead of failing the run.

- **sponsord-onramp: a Turnstile token is bound to the onramp's hostname and
  action (GHSA-jr93-46mf-88wx).** The gate decoded only `success` from the
  siteverify response, so a token minted for the same sitekey on another
  hostname passed, and nothing tied a token to this gate's widget. The widget
  now sets `data-action="onramp-fund"`, and `verify()` refuses a response
  whose `hostname` differs from `ONRAMP_PUBLIC_URL`'s host or whose `action`
  is not the onramp's. A custom `ONRAMP_GATE_TEMPLATE` must set
  `data-action="{{ACTION}}"` on its Turnstile widget; the onramp refuses to
  start with a template that lacks the placeholder. A siteverify failure
  caused by the gate itself (a missing or wrong `ONRAMP_TURNSTILE_SECRET`,
  `bad-request`, `internal-error`) is now the gate's own error, answered
  `500` and logged at warn, not a silent `403` refusal (#2719).

- **Node: a peer's own rate-limit bucket refuses it before the shared
  global one is charged.** The three-layer limiter behind `cdn/probe/v1` and
  `cdn/dht/v1` charged the global bucket first, so a
  request that the peer's own bucket then refused had already spent a global
  token. One `NodeId` flooding at the global rate drained the node-wide
  budget for every other requester, even though its own cap let only a small
  part of the flood through. The layers now fire per-peer → per-IP → global,
  so a refused request spends no global token, and the new `cdn/client/v1`
  limiter uses the same order. A request that both its own
  bucket and the global cap would refuse now counts on the `per_peer` (or
  `per_ip`) rejection counter instead of `global`. ADR 005 and ADR 022 state
  the order.

- **Node: unbound requests read no pool on-chain, and admission reads are
  capped (#2706).** At admission, an unseen `pool_id` cost one `getPool` and
  an unseen signer one `getAuthorization`, with no global bound, so a stream
  of requests for fresh random pool ids could spend the operator's RPC quota
  until the blacklist watcher stalled and the node refused every serve. A
  request with no verified client binding can name no lane, so the node now
  refuses it `NotFound` before any chain read, and before an origin-only
  node's backend `HEAD` (it counts on
  `decdn_serve_stream_rejected_owner_mismatch_total`). Admission `getPool` and
  `getAuthorization` reads hold one of 16 node-wide slots. A read waits up to
  5 s for a slot and its own timeout starts once it holds one; a request that
  finds no slot in time is refused without caching a fault against its pool,
  so the pool's next request reads again, and counts on
  `decdn_serve_admit_read_slot_timeouts_total`. Bound requests are bounded by these
  slots and by the `[serve.rate_limit]` limiter.

- **Node: inbound `cdn/client/v1` requests pass a three-layer rate limiter
  before they are read (#2694).** The only gates on `cdn/client/v1` were the
  per-connection limiter and the stream cap. Most refusals after the request
  is read sign a response, and an origin-only node sent its backend a `HEAD` for each unseen
  hash before any lane or payment check, so a flood of unpaid requests spent
  node CPU and origin requests without limit. A new `[serve.rate_limit]`
  section (per-peer 100/s burst 400, per-IP 200/s burst 800, global 2000/s
  burst 5000; a rate of 0 turns a layer off) is checked once per stream,
  after the stream permit and before the request is read. A refused stream is
  reset with `RATE_LIMITED` and no signed response, and counts on
  `decdn_serve_rate_limit_rejected_{per_peer,per_ip,global}_total`. Those
  three counters are inbound failure reasons, so the devops "Unattributed
  stream failures" panels need them too. `decdn-client` reads the reset as
  back-off. ADR 005 § Request rate limiting describes the layers.

- **Node: the DHT routing table admits bonded peers only (#2708).** ADR 022
  restricts the routing pool to bonded operators, but the table admitted every
  peer that contacted the node and every id a `closer_nodes` answer named, and
  a full bucket evicted its oldest entry unconditionally. Free NodeIds could
  therefore push out the bonded entries, so publish fan-out and lookup seeds
  went to non-participants, and discovery fell back to the on-chain origin
  directory. Every routing-table insert now checks the active staker set. The
  `FindNode` / `FindValue` answers, lookup seeds and publish fan-out skip an
  entry that has unbonded since it entered. A full bucket evicts an unbonded
  or dial-cooling entry before the least-recently-seen bonded one.

- **Node: the admin JSON-RPC server refuses requests a web page can send
  (#2690).** It answered any request on its loopback port. A page that the
  operator opened could reach that port through DNS rebinding and call
  `evict` (persisted across restarts) or `drain` (shuts the node down). The
  server now answers only requests whose `Host` is `127.0.0.1` or
  `localhost` (any port), refuses any request that carries an `Origin`
  header with `403`, and refuses batch requests. `decdn node …`, the
  dashboard and an SSH tunnel to the admin port are unaffected.

- **Node: after a restart, the redeemer reads a pool it does not know before
  it submits that pool's lanes (#2692).** The redeemer's pool view learns a
  pool only from the events it folds and the admit path's reads, and the
  watcher resumes at its checkpoint, so a pool opened before a restart stayed
  unknown. Its lanes were submitted even once the pool was `Closing` past its
  deadline, reverted `PoolClosed` (taking the whole `redeemMany` with them)
  and reverted again on every sweep. Once the candidate lanes together clear
  the redemption floor, the redeemer now reads each unknown pool with one
  `getPool` and seeds the view from it. A lane whose pool is past its close
  deadline stays out of the submit and counts on
  `decdn_redemption_skipped_insolvent_total`. A lane whose pool is absent,
  `Closed` or unreadable stays out and counts on the new
  `decdn_redemption_skipped_pool_unresolved_total`; an absent or `Closed`
  pool also logs a `warn!` naming it. The first failed read stops the reads
  for that cycle, so an RPC outage costs one round of timeouts and lanes on
  known pools still go through. A failed read counts as a chain fault, so
  parked lanes stay parked until a clean sweep.

- **Node: the mid-stream signer cap check counts a resolved pool's
  pre-restart spend (#2711).** After a restart the projection seeds a pool it
  has not folded from one `getPool`, and it counts a signer's spend only from
  the redemptions it folds after the seed. The mid-stream check read that
  count alone, so a signer that spent most of its cap before the restart kept
  being served after other nodes drained the rest. The check now reads the
  admit-path `getAuthorization` result brought up to the fold, the same
  number admission reads, and falls back to the fold alone only when it holds
  no registered read for the signer.

- **Node: racing first streams on a new lane keep its stored voucher frontier
  (#2701).** `register_lane` recorded the fresh zero lane before
  it claimed the lane's slot in the live map, so two first streams could both
  record it, and the second record could overwrite a voucher the first
  stream's lane had recorded in between. A restart in that window rehydrated
  the lane at zero, and the redeemer found no signature to submit. Only the
  stream that claims the slot now records the fresh lane, under the slot's
  guard.

- **Node: a pool resolved from chain counts its on-chain redemptions toward
  its remaining deposit (GHSA-9x5g-h559-vp5m).** After a restart the pool
  projection is empty, so the admit path reads a pool it has not folded with
  one `getPool`. It seeded that pool with zero redemptions, so a pool drained
  before the restart reported its whole deposit as `remaining` for the rest of
  the node's uptime, and the admission solvency gate and the mid-stream
  re-check served it unpaid. The seed now carries `getPool.totalRedeemed`, and
  the settlement watcher re-reads each seeded pool at one pinned block at the
  end of its tick (up to 64 pools per tick, eight reads at a time, in a
  rotating order; the first faulted read stops the batch). Redemptions
  folded after that block count on top of the read; one on a lane the
  projection has no baseline for marks the pool for another re-read rather
  than a guess, so `remaining` never counts a redemption twice and errs
  toward serving until a re-read lands. The one exception is a reorg that
  drops a counted redemption, which stays counted until on-chain
  `totalRedeemed` passes it again. A re-read pinned below a block the
  watcher already applied, or whose `totalRedeemed` or deposit is below what
  the node already holds (a backend behind the watcher or the admit read), is
  skipped and retried. A pool seeded from a read keeps the read's deposit
  against replayed `PoolOpened` / `PoolToppedUp` logs the read already holds.

- **Node: request timestamps are bounded to the node's clock before anything
  is signed (GHSA-fjcq-8x46-cq5c).** The signed probe and stream response
  bodies echo the requester-generated `timestamp_us` (ADR 014 § Slash
  Signatures — secp256k1 EIP-712), and the node accepted any value. It now
  refuses a `cdn/probe/v1` or `cdn/client/v1` request whose timestamp differs
  from its own clock by more than a fixed 30-second bound
  (`handlers::timestamp_skew`), before any response is built or signed: a
  skewed probe closes the connection, and a skewed stream request resets the
  stream, in both cases with the new `TIMESTAMP_SKEW` application error code
  (`0x04`, ADR 013 § Application Error Codes) and no signed response. Refusals
  count on `decdn_probe_timestamp_skew_refusals_total` /
  `decdn_serve_stream_rejected_timestamp_skew_total` and log a throttled
  `warn!`, so a misconfigured clock — the requester's, or the node's own — is
  operator-visible. `decdn-client` types the code as
  `UpstreamTimestampRejected`: when at least three probed nodes, and most
  of them, refuse a client's timestamp, `decdn get` / `decdn quote` fail
  with an error that names this machine's clock rather than "N did not
  answer"; fewer refusals (which any node can send) keep the ordinary error,
  with a clock hint. A source that refuses a stream at admission for it
  leaves the fetch as a decline, with no peer-store failure stamp; a 0x04
  after bytes have landed is an ordinary source fault. A fetch that every
  source refuses this way names the clock too. A node pulling from an
  upstream that refuses its timestamp records no reputation outcome,
  suppresses the `(peer, hash)` pair briefly as for a rate-limit shed, and
  counts the refusal on `decdn_node_upstream_timestamp_rejected_total`. A requester that has no
  mapping for the code treats it as a generic transport error. Honest
  requesters already stamp a fresh wall-clock read (ADR 005), so no message
  layout changes — but a requester whose clock is more than 30 seconds off
  now fails every probe and stream until its clock is fixed. **Operator
  note:** an upward reprice (`payment.rate_per_mb`, restart-required) now
  waits 90 seconds of real time after the last probe answered at the old
  rate, not 30 seconds: each signed timestamp may sit up to 30 seconds from
  the node's clock, on either side of the 30-second slashing window.
- **Node and client: a capability `expiry` of `0` reads as expired
  (GHSA-vq47-w6q7-4rrj).** The node read a capability `expiry` of `0` as
  "never expires", while the on-chain `PaymentPool` reads it as already
  expired. `intake_capability` now drops a `0`-expiry capability before any
  lane registers, so the request refuses as an unknown lane and the client
  must present a valid capability. Defense in depth: the redeemer's
  `capability_expired` predicate and the shared
  `inside_capability_expiry_margin` (the node's admission, voucher and
  preimage gates, and the client's credential view) read an `expiry` of `0`
  as expired too. A previously persisted `0`-expiry lane is neither served
  nor swept, and a client holding a `0`-expiry credential reports it as
  expired. The wire format and contracts are unchanged.
- **Node: the mid-stream pool-solvency re-check stops delivery whenever the
  pool's remaining deposit falls to the refundable floor `M`, independent of
  outstanding floor-credit accounting (GHSA-3742-7954-qjwq).** Both delivery
  legs now gate the in-flight re-check on `pool_funds_committed`, which
  requires `remaining > M` in addition to the committed-floor covers-check
  (ADR 003 §Pool solvency). Admission and the direct-serve re-validation
  carry a nonzero reserve in the covers-check and are unchanged.
- **Node: a peer could pin up to 16 MiB per stream with an oversized frame
  (#2501).** Every frame read accepted a length prefix up to a global 16 MiB
  `MAX_MESSAGE_SIZE` and allocated the buffer before the payload arrived, though
  no request is larger than a few hundred bytes. A peer with no stake or funds
  could announce a near-16 MiB request, send most of it, and stall, holding the
  buffer for the 5 s read timeout. On `cdn/client/v1` the node read the request
  before it checked the per-connection stream cap, so one connection could hold
  100 such buffers (about 1.6 GiB); the only bound was the attacker's upload
  bandwidth. Every read now names a ceiling sized to its ALPN's largest
  legitimate message (ADR 013 § Wire Framing): `MAX_PROBE_FRAME` (1 KiB),
  `MAX_CLIENT_CONTROL_FRAME` (4 KiB) for requests, payment proofs and
  `StreamResponse`, `MAX_CHUNK_FRAME` (one `CHUNK_BYTES` `ChunkData`) for the
  delivery stream, and `MAX_DHT_FRAME` (64 KiB) for DHT requests and responses.
  The node resets an over-cap client stream with `RATE_LIMITED` before it reads
  the request. The node's QUIC transport also had no connection-level receive
  window, so a peer could park about 1.25 MB of out-of-order data per stream
  below the reader; a new 8 MiB connection window (`QUIC_CONNECTION_RECEIVE_WINDOW`)
  bounds it, which also caps one connection's in-flight download at about
  80 MB/s at a 100 ms round trip. At the default 256 handlers the memory stalled
  peers can hold is now bounded at about 2.2 GiB (ADR 013 § Wire Framing);
  operators with less memory lower `security.max_concurrent_handlers`.
  Legitimate traffic is unchanged, so this is not wire-breaking.
  **API break (`decdn-protocol`):** `read_frame` and `framing::parse_frame` take
  a required `max`, `MAX_MESSAGE_SIZE` is removed, and `FrameError::TooLarge` is
  now `{ len: u64, max: u32 }`. Reported by Radeonares32.
- `aws-smithy-json` → 0.62.7 (CVE-2026-18140: uncontrolled recursion on deeply
  nested JSON, a denial of service). It reaches the node through the S3
  origin's AWS SDK: `aws-sdk-s3` and the `aws-config` credential chain (SSO,
  STS, IMDS) parse JSON responses with it. Lockfile-only bump; no dependency
  requirement changed. The new `aws-smithy-schema` entry is a dependency of
  0.62.7.
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
