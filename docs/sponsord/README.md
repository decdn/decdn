# sponsord

deCDN's sponsored on-ramp, in three parts:

- **`sponsord`** (`crates/daemon`): the signing and top-up service. It holds
  the owner key of one `PaymentPool` on Arbitrum Sepolia, signs capped,
  expiring capabilities (`dcap1:` tokens) for trusted callers over a small
  HTTP API, and keeps the pool funded from the treasury. It knows nothing
  about who deserves a capability; that is the caller's decision.
- **`sponsord-onramp`** (`crates/onramp`): a frontend of the daemon. A
  captcha-gated `/fund` page, the `/decdn.sh` and `/decdn.ps1` installers,
  and the `/capability` poll the CLI uses.
- **`decdn-sponsored`** (`crates/wrapper`): the end-user CLI. Gives each
  download a throwaway key, gets a capability for it through the onramp,
  and hands the pull to `decdn`.

Capabilities are node-agnostic: issuance involves no content hash or node
discovery, only an allowance against the shared pool. A signer's cap and
expiry are fixed on-chain at its first redemption, which is why
`decdn-sponsored` uses one key per download, and why `/fund` answers
`409 signer_expired` for a key whose registration has expired instead of
renewing it. Spending is bounded by the gate in front of the daemon (the
captcha on `/fund`), the per-capability cap, and the pool's own balance.

`sponsord-core` (`crates/core`) is the daemon's logic as a library, for Rust
programs that embed it instead of calling the daemon.

A capability authorizes a key to spend up to its cap against the sponsor's
pool until its expiry: zero on-chain transactions and zero locked deposit
per user. The pool itself is opened once, out-of-band, with `decdn pool open`.
See `../decdn` for the protocol and contracts.

## The sponsord daemon

`sponsord` holds the pool owner's key and signs capabilities for any caller
that presents its bearer token. Every caller is trusted by the pool owner, so
the daemon carries no per-caller credentials or budgets; the pool balance,
topped up from the treasury, bounds the loss. A background task tops the pool
up from the treasury whenever its remaining balance falls below
`SPONSORD_POOL_LOW_WATER_MICRO_USDC`, checking every
`SPONSORD_POOL_WATCH_INTERVAL_SECS`. The daemon keeps no state, and it
refuses to start without `SPONSORD_API_TOKEN` or if the signing key does not
own the pool on-chain. The pool is opened out-of-band, once, via
`decdn pool open` from the treasury wallet; `SPONSORD_POOL_ID` names that
existing pool.

### Running

```bash
cp daemon.env.example daemon.env   # then edit
set -a; . ./daemon.env; set +a; cargo run -p sponsord
```

Start `sponsord` before `sponsord-onramp`: the onramp reads the daemon's
settings at startup.

### `SPONSORD_*` environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SPONSORD_API_TOKEN` | **yes** | none | Bearer token callers present; at least 32 bytes (`openssl rand -hex 32`) |
| `SPONSORD_RPC_URL` | **yes** | none | Arbitrum Sepolia RPC endpoint |
| `SPONSORD_PAYMENT_POOL_ADDR` | **yes** | none | `PaymentPool` contract address |
| `SPONSORD_POOL_ID` | **yes** | none | Id of the sponsor's pool, opened out-of-band via `decdn pool open` |
| `SPONSORD_TREASURY_KEYSTORE` | **yes** | none | Path to the treasury hot wallet's encrypted keystore JSON |
| `SPONSORD_TREASURY_PASSWORD` | **yes** | none | Password for the keystore |
| `SPONSORD_BIND` | no | `127.0.0.1:8090` | Address the HTTP API listens on |
| `SPONSORD_CHAIN_ID` | no | `421614` | Chain id (Arbitrum Sepolia) |
| `SPONSORD_MAX_SPENDING_CAP_MICRO_USDC` | no | `5000000` ($5) | Largest cap a caller may request per capability |
| `SPONSORD_MAX_TTL_SECS` | no | `172800` (48 hours) | Longest TTL a caller may request |
| `SPONSORD_POOL_LOW_WATER_MICRO_USDC` | no | `20000000` ($20) | Remaining balance below which the pool is topped up |
| `SPONSORD_POOL_REFILL_MICRO_USDC` | no | `100000000` ($100) | Amount the pool is topped up by |
| `SPONSORD_POOL_WATCH_INTERVAL_SECS` | no | `3600` | How often the pool balance is checked |

### HTTP API

| Route | Request | Success |
|---|---|---|
| `POST /v1/capabilities` | `{"signer": "0x..", "spending_cap"?: u64, "ttl_secs"?: u64}` | `200 {"token": "dcap1:..", "spending_cap": u64, "expiry": u64, "registered": bool}` |
| `GET /v1/info` | | `200 {"chain_id": u64, "payment_pool": "0x..", "max_spending_cap": u64, "max_ttl_secs": u64}` |
| `GET /healthz` | | `200 {"ok": true}` |

An omitted `spending_cap` or `ttl_secs` defaults to the daemon maximum.
Errors are `{"error": "<code>"}` plus the fields noted:

| Status | `error` | When |
|---|---|---|
| 400 | `bad_request` | malformed body or signer |
| 400 | `exceeds_max` | cap or TTL above the maximum; the body adds `max_spending_cap` and `max_ttl_secs` |
| 400 | `zero` | cap or TTL of 0 |
| 401 | `unauthorized` | missing or wrong token |
| 409 | `signer_expired` | the signer is registered on-chain and its registration has expired; the body adds `expiry` |
| 503 | `chain_unavailable` | the signer's on-chain authorization could not be read |
| 500 | `internal` | signing failed |

Every `/v1` route needs `Authorization: Bearer <SPONSORD_API_TOKEN>`. The
daemon binds to localhost by default; put a TLS reverse proxy in front of it
when a caller on another host needs it.

**One signer, one set of terms.** The first capability redeemed for a
signer fixes its cap and expiry on-chain for good. For a registered signer,
`POST /v1/capabilities` returns that existing capability (`registered:
true`, with its real terms) whatever terms were requested, and `409
signer_expired` once it has expired. To give someone different terms, use a
new signer key. Requests for a signer that is not yet registered each get a
fresh token; whichever is redeemed first wins.

A minimal gate of your own is one call after your check passes:

    curl -fsS -X POST http://127.0.0.1:8090/v1/capabilities \
      -H "Authorization: Bearer $SPONSORD_API_TOKEN" \
      -H 'content-type: application/json' \
      -d '{"signer":"0x...","spending_cap":1000000,"ttl_secs":86400}'

## The sponsord-onramp frontend

`sponsord-onramp` is the reference frontend of the daemon: a Cloudflare
Turnstile captcha gate in front of `POST /v1/capabilities`, plus the installers
and the grant store behind the `decdn-sponsored` CLI. At startup it reads
`chain_id` and `payment_pool` from the daemon's `/v1/info`, and it refuses to
start if the daemon is unreachable or if its configured cap or TTL exceeds the
daemon's maximum. A change to the daemon's pool settings needs an onramp
restart.

Routes: `/healthz`, `/decdn.sh` and `/decdn.ps1` (templated installers for
macOS/Linux and Windows), `/fund` (captcha page and capability issuance), and
`/capability` (poll for an issued capability).

### Running

```bash
cp onramp.env.example onramp.env   # then edit
set -a; . ./onramp.env; set +a; cargo run -p sponsord-onramp
```

Start `sponsord` first. The onramp exits at startup when the daemon is
unreachable, so run it under a supervisor that restarts it on failure (for
example systemd `Restart=on-failure`).

### `ONRAMP_*` environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `ONRAMP_DAEMON_TOKEN` | **yes** | none | The daemon's `SPONSORD_API_TOKEN` |
| `ONRAMP_RPC_URL` | **yes** | none | Public RPC endpoint baked into the installers for end users |
| `ONRAMP_CAPACITY_BOND_ADDR` | **yes** | none | `CapacityBond` contract address (hash to node/provider discovery) |
| `ONRAMP_TURNSTILE_SECRET` | **yes** | none | Cloudflare Turnstile server-side secret |
| `ONRAMP_TURNSTILE_SITEKEY` | **yes** | none | Cloudflare Turnstile sitekey, shown in the `/fund` widget page |
| `ONRAMP_DECDN_RELEASE` | **yes** | none | `decdn/decdn` release tag (`vMAJOR.MINOR.PATCH`, optionally `-pre`) the installers install `decdn` from |
| `ONRAMP_DECDN_SUMS_SHA256` | **yes** | none | SHA-256 of that release's `SHA256SUMS` file |
| `ONRAMP_WRAPPER_RELEASE` | **yes** | none | `decdn/sponsord` release tag (same shape) the installers install `decdn-sponsored` from |
| `ONRAMP_WRAPPER_SUMS_SHA256` | **yes** | none | SHA-256 of that release's `SHA256SUMS` file (printed in the release notes) |
| `ONRAMP_BIND` | no | `127.0.0.1:8080` | Address the HTTP server listens on |
| `ONRAMP_PUBLIC_URL` | no | `https://up.decdn.org` | This onramp's public base URL; baked into the installers as `{{GATEWAY_BASE}}` |
| `ONRAMP_DAEMON_URL` | no | `http://127.0.0.1:8090` | Base URL of the sponsord daemon |
| `ONRAMP_SPENDING_CAP_MICRO_USDC` | no | daemon maximum | Cap requested for each capability |
| `ONRAMP_TTL_SECS` | no | daemon maximum | TTL requested for each capability |
| `ONRAMP_DATA_DIR` | no | `./data` | Directory for the redb grant store |

### The `decdn-sponsored` flow

The website shows one command per model, with the model's BLAKE3 hash and
the namespace it is published under, both from `models.json`. On macOS and
Linux:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- pull b3:<hash> --namespace <id>
```

On Windows (x64 and ARM64), in PowerShell:

```powershell
irm https://up.decdn.org/decdn.ps1 | iex; decdn-sponsored pull b3:<hash> --namespace <id>
```

1. The installer served at `GET /decdn.sh` (`crates/onramp/assets/decdn.sh`),
   or its PowerShell twin at `GET /decdn.ps1`
   (`crates/onramp/assets/decdn.ps1`), installs the `decdn` and
   `decdn-sponsored` binaries straight from their pinned GitHub Releases. It
   downloads each release's `SHA256SUMS`, checks it against the pinned digest,
   then checks the platform's archive against `SHA256SUMS`; nothing is
   installed unless both match. It then writes `~/.decdn/sponsor.toml` with
   the onramp's contract addresses and RPC URL filled in. Any arguments are
   passed on to `decdn-sponsored`. Running it again is harmless, and
   `decdn-sponsored pull ...` works on its own once installed.
2. `decdn-sponsored pull <hash> [-o <dir>] [--namespace <id>]` (output
   defaults to the current directory) opens the state directory for that hash,
   `~/.decdn/sponsored/downloads/<hash>/`, and generates a throwaway
   voucher-signing key there with a random password stored beside it. The
   user never sees a key, keystore, or password.
3. It polls `GET /capability?client=<addr>` and, while that answers `204`,
   prints (and opens in the browser) `GET /fund?client=<addr>` for the
   captcha. The issued `dcap1:` token is saved in the state directory.
4. It runs `decdn bundle pull --hash <hash> -o <dir> --capability-file ...
   --keystore ... --data-dir <state dir>` with inherited stdio, so `decdn`'s
   own progress and errors reach the user unchanged. The name-to-hash
   mapping happens on the website; the CLI accepts only a hash, and `decdn`
   verifies every byte against it. `--namespace` is passed through as-is: it
   lets a node that has not cached the bundle pull from that namespace's
   origins, and never changes which bytes are accepted.
5. On success the state directory is deleted. On failure it is kept: running
   the same command again resumes with the same key and capability (no new
   captcha), and `bundle pull` resumes from its `.partial` files. A saved
   capability within five minutes of expiry is replaced by a fresh key and
   a new captcha.

The `~/.decdn/sponsor.toml` schema is a hard contract between the installer
(`crates/onramp/assets/decdn.sh`, `crates/onramp/assets/decdn.ps1`) and the
wrapper (`crates/wrapper/src/config.rs`): field names must match exactly.
Fields: `gateway_base`, `decdn_bin`, `data_dir`, `rpc_url`, `payment_pool`,
`capacity_bond` (optional), `slash_judge` (optional), `chain_id`. Unknown
fields are ignored.

## Building against `decdn`

The workspace path-depends on its sibling `decdn` checkout
(`../decdn/crates/*`), so a local build uses whatever that checkout holds.
CI checks out `decdn/decdn` beside this repo: `main` by default, or any ref a
manual run names (`decdn_ref`), so breakage from `decdn` changes shows up
early. Releases build against the commit pinned in `decdn.ref` instead, so a
tag always builds the same code. `cargo test -p sponsord-core --features
anvil-e2e` runs the treasury against a local anvil chain.

## Releasing

1. Point `decdn.ref` at the `decdn` commit (full SHA) or tag to build
   against, and make sure `Cargo.lock` is consistent with it
   (`cargo metadata --locked` with that commit checked out beside this repo).
2. Push a `vMAJOR.MINOR.PATCH[-pre]` tag. `.github/workflows/release.yml`
   builds `decdn-sponsored` for Linux, macOS and Windows (x86_64 and
   aarch64 each) and `sponsord` and `sponsord-onramp` for Linux, and
   publishes them with a `SHA256SUMS` manifest as a GitHub Release.
3. The release notes print the `ONRAMP_WRAPPER_RELEASE` and
   `ONRAMP_WRAPPER_SUMS_SHA256` values that pin it. Set them on the onramp
   to make the installers serve it. `decdn` releases are pinned the same way
   (`ONRAMP_DECDN_RELEASE`, and `ONRAMP_DECDN_SUMS_SHA256` = the SHA-256 of
   that release's `SHA256SUMS`).
