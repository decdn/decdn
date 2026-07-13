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

- **`cdn/probe/v1` content-availability + slashing evidence (#318).**
  `cdn/probe/v1` is now a content-availability query, not just a
  latency/rate probe (ADR 005, ADR 014). Wire changes (same ALPN —
  this establishes the v1 signed baseline, not a version bump):
  `ProbeRequest` is now `{ hash, timestamp_us }` (was `{ nonce }`);
  `ProbeResponse` is now a signed `{ body: { hash, has_blob,
  rate_per_mb, timestamp_us }, total_bytes: Option<u64>, slash_sig }`
  (was `{ nonce, measured_at_unix_ms, node_id, rate_per_mb }`).
  `slash_sig` is a mandatory, non-empty EIP-712 secp256k1 signature
  (65-byte EOA `r‖s‖v` form in the PoC, ADR 024 §18); requesters reject
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

### Fixed

#### Node serve path

- **A backend fault no longer signs an authoritative `NotFound` (#1129).** On a
  `cdn/client/v1` cache miss, a hard origin/store fault during a reactive
  pull-through fill (an S3 5xx surviving retry exhaustion, an open circuit
  breaker, an fs I/O error) was indistinguishable from a clean miss and
  terminated as a signed `CacheMiss` — a positive, attributable claim that the
  blob does not exist, sent to a paying client for content the node serves fine
  once the origin recovers. Such faults now terminate as a retryable
  `InternalError`. A genuine absence still signs `NotFound`, and a fault on one
  tier is remembered across a legitimate fall-through to a later tier. No wire
  change: `StreamError::InternalError` already existed; this is a
  reclassification within the existing surface.
- **A wedged upstream candidate no longer starves the fallback loop.** The
  window-paced node→node pull (`open_progressive_pull`) applied no per-candidate
  timeout, so a provider that accepted the connection and then went quiet
  consumed the entire outer deadline — which is deliberately sized to fit all
  `MAX_PROVIDER_ATTEMPTS` per-candidate budgets precisely so candidates #2..N
  stay reachable (#859) — and the serve path then refused a blob the honest
  fallback held. Each candidate is now bounded by `pull_timeout`, as on the
  buffered path; a timed-out candidate is skipped and, per #857, not blamed.

### Changed

#### Gossip

- Gossip publisher / subscriber / TTL-sweeper tasks now shut down
  cooperatively via a `CancellationToken` owned by `GossipService`,
  instead of the runtime reaching in with `JoinHandle::abort()` (#805).
  Each loop returns at a clean await boundary on cancellation — including
  interrupting the subscriber's reconnect backoff — so the drain phase
  finishes promptly without abrupt mid-await cancellation. Lifecycle
  ownership only; no steady-state behavior change.

### Added

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
- Minimum-reputation rejection floor for node selection: the new
  `rank_candidates_with_floor` / `top_n_with_floor` selection APIs drop
  sub-floor candidates before scoring so price/RTT cannot override a
  poor reputation (#441, ADR 001). API-configurable only for now —
  **no config/env/CLI knob yet**; per-client wiring is deferred until
  the client fetch path lands. Default `0.0` keeps prior behavior.

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
