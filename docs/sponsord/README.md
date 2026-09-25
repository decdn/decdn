# sponsord

`sponsord` is deCDN's sponsored on-ramp: a gateway (`sponsord`) that grants a
new client a zero-tx allowance against its own shared `PaymentPool` on the
Arbitrum Sepolia testnet, so a new user can fetch content from the network
without first acquiring testnet USDC, opening a channel, or setting up a
wallet by hand. The sponsor owns a single `PaymentPool`, opened out-of-band
via `decdn pool open` (its id given by `SPONSOR_POOL_ID`) — the gateway never
opens anything per user. A captcha-gated `/fund` flow issues an owner-signed
EIP-712 capability (serialized as a `dcap1:` token) authorizing the caller's
key to redeem against that pool up to a per-capability cap: zero on-chain
transaction and zero locked deposit per user. A companion CLI
(`decdn-sponsored`, in `crates/wrapper`) gives each download a throwaway key,
obtains a capability for it, and hands the pull to the `decdn` binary. The
gateway never sees that key. See `../decdn` for the protocol and contracts
this all sits on top of.

Capabilities are node-agnostic: issuance doesn't involve a content hash or
node discovery, only an allowance against the shared pool. Registration of a
signer against the pool is set-once on-chain — once a key first redeems, its
cap and expiry are frozen for that key, which is why `decdn-sponsored` uses
one key per download. Anti-abuse is bounded by the captcha on `/fund`, the
per-capability cap (`SPONSOR_CAPABILITY_CAP_MICRO_USDC`), and the shared
pool's own balance — there's no per-signer monthly accumulator.

## Crates

- `crates/server` (binary `sponsord`) — the HTTP gateway: `/healthz`,
  `/decdn.sh` and `/decdn.ps1` (templated installers for macOS/Linux and
  Windows), `/fund` (captcha page + capability
  issuance), `/capability` (poll for an issued capability).
- `crates/wrapper` (binary `decdn-sponsored`) — the end-user CLI: reads
  `~/.decdn/sponsor.toml` (written by the installer), obtains a capability
  for a per-download throwaway key, then runs `decdn bundle pull` against the
  shared pool.

## Running the server

```bash
export SPONSOR_RPC_URL=https://sepolia-rollup.arbitrum.io/rpc
export SPONSOR_PAYMENT_POOL_ADDR=0x...
export SPONSOR_POOL_ID=0x...
export SPONSOR_CAPACITY_BOND_ADDR=0x...
export SPONSOR_TREASURY_KEYSTORE=/path/to/treasury-keystore.json
export SPONSOR_TREASURY_PASSWORD=...
export SPONSOR_TURNSTILE_SECRET=...
export SPONSOR_TURNSTILE_SITEKEY=...
export SPONSOR_DECDN_RELEASE=v0.1.0
export SPONSOR_DECDN_SUMS_SHA256=...
export SPONSOR_WRAPPER_RELEASE=v0.1.0
export SPONSOR_WRAPPER_SUMS_SHA256=...
cargo run -p sponsord
```

The pool itself is opened out-of-band, once, via `decdn pool open` from the
treasury wallet; `SPONSOR_POOL_ID` just tells `sponsord` which existing pool
to issue capabilities against. `sponsord` never opens a pool itself.

`sponsord` binds `SPONSOR_BIND` (default `127.0.0.1:8080`), serves the HTTP
routes below, and spawns a background task (`pool_watch::run`) that tops the
pool up from the treasury whenever its remaining balance falls below
`SPONSOR_POOL_LOW_WATER_MICRO_USDC`, checking every
`SPONSOR_POOL_WATCH_INTERVAL_SECS`.

### `SPONSOR_*` environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SPONSOR_BIND` | no | `127.0.0.1:8080` | Address the HTTP server listens on |
| `SPONSOR_PUBLIC_URL` | no | `https://up.decdn.org` | This gateway's own public base URL; baked into the `/decdn.sh` and `/decdn.ps1` installers as `{{GATEWAY_BASE}}` |
| `SPONSOR_RPC_URL` | **yes** | — | Arbitrum Sepolia RPC endpoint |
| `SPONSOR_CHAIN_ID` | no | `421614` | Chain id (Arbitrum Sepolia) |
| `SPONSOR_PAYMENT_POOL_ADDR` | **yes** | — | `PaymentPool` contract address |
| `SPONSOR_POOL_ID` | **yes** | — | Id of the sponsor's own shared pool, opened out-of-band via `decdn pool open` |
| `SPONSOR_CAPACITY_BOND_ADDR` | **yes** | — | `CapacityBond` contract address (used for hash → node/provider discovery) |
| `SPONSOR_TREASURY_KEYSTORE` | **yes** | — | Path to the treasury hot-wallet's encrypted keystore JSON |
| `SPONSOR_TREASURY_PASSWORD` | **yes** | — | Password to decrypt `SPONSOR_TREASURY_KEYSTORE` (never logged, never written to disk elsewhere) |
| `SPONSOR_CAPABILITY_CAP_MICRO_USDC` | no | `5_000_000` ($5) | Spend cap baked into each issued capability |
| `SPONSOR_CAPABILITY_TTL_SECS` | no | `172_800` (48 hours) | How long an issued capability remains valid |
| `SPONSOR_POOL_LOW_WATER_MICRO_USDC` | no | `20_000_000` ($20) | Balance threshold below which `pool_watch` tops the pool up from the treasury |
| `SPONSOR_POOL_REFILL_MICRO_USDC` | no | `100_000_000` ($100) | Amount `pool_watch` tops the pool up by |
| `SPONSOR_POOL_WATCH_INTERVAL_SECS` | no | `3600` | How often the pool-balance background task runs |
| `SPONSOR_TURNSTILE_SECRET` | **yes** | — | Cloudflare Turnstile server-side secret, used to verify captcha tokens |
| `SPONSOR_TURNSTILE_SITEKEY` | **yes** | — | Cloudflare Turnstile sitekey, interpolated into the `/fund` widget page |
| `SPONSOR_DATA_DIR` | no | `./data` | Directory for the redb store (issuance bookkeeping) |
| `SPONSOR_DECDN_RELEASE` | **yes** | — | `decdn/decdn` release tag (`vMAJOR.MINOR.PATCH`, optionally `-pre`, e.g. `v1.0.0-rc.1`) the installers install `decdn` from |
| `SPONSOR_DECDN_SUMS_SHA256` | **yes** | — | SHA-256 of that release's `SHA256SUMS` file |
| `SPONSOR_WRAPPER_RELEASE` | **yes** | — | `decdn/sponsord` release tag (same shape) the installers install `decdn-sponsored` from |
| `SPONSOR_WRAPPER_SUMS_SHA256` | **yes** | — | SHA-256 of that release's `SHA256SUMS` file (printed in the release notes) |

## The `decdn-sponsored` flow

The website shows one command per model, with the model's BLAKE3 hash from
`models.json`. On macOS and Linux:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- pull b3:<hash>
```

On Windows (x64 and ARM64), in PowerShell:

```powershell
irm https://up.decdn.org/decdn.ps1 | iex; decdn-sponsored pull b3:<hash>
```

1. The installer served at `GET /decdn.sh` (`assets/decdn.sh`), or its
   PowerShell twin at `GET /decdn.ps1` (`assets/decdn.ps1`), installs the
   `decdn` and `decdn-sponsored` binaries straight from their pinned GitHub
   Releases. It downloads each release's `SHA256SUMS`, checks it against the
   pinned digest, then checks the platform's archive against `SHA256SUMS`;
   nothing is installed unless both match. It then writes
   `~/.decdn/sponsor.toml` with the gateway's contract addresses and RPC URL
   filled in. Any
   arguments are passed on to `decdn-sponsored`. Running it again is
   harmless, and `decdn-sponsored pull ...` works on its own once installed.
2. `decdn-sponsored pull <hash> [-o <dir>]` (output defaults to the current
   directory) opens the state directory for that hash, `~/.decdn/sponsored/downloads/<hash>/`, and generates a throwaway
   voucher-signing key there with a random password stored beside it. The
   user never sees a key, keystore, or password.
3. It polls `GET /capability?client=<addr>` and, while that answers `204`,
   prints (and opens in the browser) `GET /fund?client=<addr>` for the
   captcha. The issued `dcap1:` token is saved in the state directory.
4. It runs `decdn bundle pull --hash <hash> -o <dir> --capability-file ...
   --keystore ... --data-dir <state dir>` with inherited stdio, so `decdn`'s
   own progress and errors reach the user unchanged. The name-to-hash
   mapping happens on the website; the CLI accepts only a hash, and `decdn`
   verifies every byte against it.
5. On success the state directory is deleted. On failure it is kept: running
   the same command again resumes with the same key and capability (no new
   captcha), and `bundle pull` resumes from its `.partial` files. A saved
   capability within five minutes of expiry is replaced by a fresh key and
   a new captcha.

The `~/.decdn/sponsor.toml` schema is a hard contract between the installer
(`assets/decdn.sh`, `assets/decdn.ps1`) and the wrapper
(`crates/wrapper/src/config.rs`): field
names must match exactly. Current fields: `gateway_base`, `decdn_bin`,
`data_dir`, `rpc_url`, `payment_pool`, `capacity_bond` (optional),
`slash_judge` (optional), `chain_id`. Unknown fields are ignored.

## Building against `decdn`

The workspace path-depends on its sibling `decdn` checkout
(`../decdn/crates/*`), so a local build uses whatever that checkout holds.
CI checks out `decdn/decdn` beside this repo: `main` by default, or any ref a
manual run names (`decdn_ref`), so breakage from `decdn` changes shows up
early. Releases build against the commit pinned in `decdn.ref` instead, so a
tag always builds the same code.

## Releasing

1. Point `decdn.ref` at the `decdn` commit (full SHA) or tag to build
   against, and make sure `Cargo.lock` is consistent with it
   (`cargo metadata --locked` with that commit checked out beside this repo).
2. Push a `vMAJOR.MINOR.PATCH[-pre]` tag. `.github/workflows/release.yml`
   builds `decdn-sponsored` for Linux, macOS and Windows (x86_64 and
   aarch64 each) and `sponsord` for Linux, and publishes them with a
   `SHA256SUMS` manifest as a GitHub Release.
3. The release notes print the `SPONSOR_WRAPPER_RELEASE` and
   `SPONSOR_WRAPPER_SUMS_SHA256` values that pin it. Set them on the gateway
   to make the installers serve it. `decdn` releases are pinned the same way
   (`SPONSOR_DECDN_RELEASE`, and `SPONSOR_DECDN_SUMS_SHA256` = the SHA-256 of
   that release's `SHA256SUMS`).
