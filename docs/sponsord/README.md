# sponsor

`sponsor` is deCDN's sponsored on-ramp: a gateway (`sponsord`) that grants a
new client a zero-tx allowance against its own shared `PaymentPool` on the
Arbitrum Sepolia testnet, so a new user can fetch content from the network
without first acquiring testnet USDC, opening a channel, or setting up a
wallet by hand. The sponsor owns a single `PaymentPool`, opened out-of-band
via `decdn pool open` (its id given by `SPONSOR_POOL_ID`) — the gateway never
opens anything per user. A captcha-gated `/fund` flow issues an owner-signed
EIP-712 capability (serialized as a `dcap1:` token) authorizing the caller's
key to redeem against that pool up to a per-capability cap: zero on-chain
transaction and zero locked deposit per user. A companion CLI wrapper
(`onramp`, in `crates/wrapper`) drives the actual `decdn` fetch using a
locally generated keystore, never handing that key to the gateway.
It is a wrapper around `decdn`'s own paid-pull path — see `../decdn` for the
protocol and contracts this all sits on top of.

Capabilities are node-agnostic: issuance doesn't involve a content hash or
node discovery, only an allowance against the shared pool. Registration of a
signer against the pool is set-once on-chain — once a key first redeems, its
cap and expiry are frozen for that key. There's no per-key top-up; getting
more allowance means generating a fresh keystore key and requesting a fresh
capability. Anti-abuse is bounded by the captcha on `/fund`, the
per-capability cap (`SPONSOR_CAPABILITY_CAP_MICRO_USDC`), and the shared
pool's own balance — there's no per-signer monthly accumulator.

## Crates

- `crates/server` (binary `sponsord`) — the HTTP gateway: `/healthz`,
  `/decdn.sh` (templated installer), `/fund` (captcha page + capability
  issuance), `/capability` (poll for an issued capability).
- `crates/wrapper` (binary `onramp`) — the end-user CLI: reads
  `~/.decdn/sponsor.toml` (written by the installer), talks to `sponsord` to
  obtain a capability, then drives a real `decdn` paid fetch against the
  shared pool with a local keystore.

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
| `SPONSOR_PUBLIC_URL` | no | `https://up.decdn.org` | This gateway's own public base URL; baked into the `/decdn.sh` installer as `{{GATEWAY_BASE}}` |
| `SPONSOR_RPC_URL` | **yes** | — | Arbitrum Sepolia RPC endpoint |
| `SPONSOR_CHAIN_ID` | no | `421614` | Chain id (Arbitrum Sepolia) |
| `SPONSOR_PAYMENT_POOL_ADDR` | **yes** | — | `PaymentPool` contract address |
| `SPONSOR_POOL_ID` | **yes** | — | Id of the sponsor's own shared pool, opened out-of-band via `decdn pool open` |
| `SPONSOR_CAPACITY_BOND_ADDR` | **yes** | — | `CapacityBond` contract address (used for hash → node/provider discovery) |
| `SPONSOR_TREASURY_KEYSTORE` | **yes** | — | Path to the treasury hot-wallet's encrypted keystore JSON |
| `SPONSOR_TREASURY_PASSWORD` | **yes** | — | Password to decrypt `SPONSOR_TREASURY_KEYSTORE` (never logged, never written to disk elsewhere) |
| `SPONSOR_CAPABILITY_CAP_MICRO_USDC` | no | `10_000_000` ($10) | Spend cap baked into each issued capability |
| `SPONSOR_CAPABILITY_TTL_SECS` | no | `2_592_000` (30 days) | How long an issued capability remains valid |
| `SPONSOR_POOL_LOW_WATER_MICRO_USDC` | no | `20_000_000` ($20) | Balance threshold below which `pool_watch` tops the pool up from the treasury |
| `SPONSOR_POOL_REFILL_MICRO_USDC` | no | `100_000_000` ($100) | Amount `pool_watch` tops the pool up by |
| `SPONSOR_POOL_WATCH_INTERVAL_SECS` | no | `3600` | How often the pool-balance background task runs |
| `SPONSOR_TURNSTILE_SECRET` | **yes** | — | Cloudflare Turnstile server-side secret, used to verify captcha tokens |
| `SPONSOR_TURNSTILE_SITEKEY` | **yes** | — | Cloudflare Turnstile sitekey, interpolated into the `/fund` widget page |
| `SPONSOR_DATA_DIR` | no | `./data` | Directory for the redb store (issuance bookkeeping) |

## The wrapper (`onramp`) flow

1. A user runs the installer served at `GET {{gateway}}/decdn.sh` (see
   `assets/decdn.sh`). It installs the `decdn` and `onramp` binaries,
   writes `~/.decdn/sponsor.toml` with the gateway's contract addresses and
   RPC URL already filled in, and generates a local client keystore
   (`~/.decdn/client/keystore.json`) if one doesn't already exist.
2. The user sets `DECDN_KEYSTORE_PASSWORD` and runs `onramp <hash> -o
   out.bin`.
3. `onramp` loads `~/.decdn/sponsor.toml` (schema: `crates/wrapper/src/config.rs`'s
   `Profile`), reads its local signer's address, and asks the gateway for a
   capability: it opens `GET /fund?client=<addr>` in the browser for the
   captcha, then polls `GET /capability?client=<addr>` until it gets back a
   `dcap1:` token (or `204` while still pending). It then drives `decdn
   fetch --capability <dcap1:token> --payment-pool-address <addr> <hash> -o
   out`, signing vouchers with the local keystore — the gateway never sees
   or holds the client's private key, only its own treasury key.

There's no top-up loop: a capability's cap and expiry are fixed at issuance,
and on-chain signer registration is set-once, so once a key's allowance is
exhausted, the only way to get more is to request a fresh capability under a
new keystore key.

The `~/.decdn/sponsor.toml` schema is a hard contract between the installer
(`assets/decdn.sh`) and the wrapper (`crates/wrapper/src/config.rs`): field
names must match exactly. Current fields: `gateway_base`, `keystore_path`,
`decdn_bin`, `data_dir`, `rpc_url`, `payment_pool`, `capacity_bond`
(optional), `slash_judge` (optional), `chain_id`.

## Publish seam

This repo currently depends on its sibling `decdn` checkout via path
dependencies in the root `Cargo.toml`:

```toml
decdn-client-pull = { path = "../decdn/crates/client-pull" }
decdn-incentive   = { path = "../decdn/crates/incentive", features = ["redb"] }
decdn-common      = { path = "../decdn/crates/common" }
```

Before this repo goes public, swap those to git dependencies pinned to a
tagged `decdn` release:

```toml
decdn-client-pull = { git = "https://github.com/decdn/decdn.git", tag = "vX.Y.Z" }
decdn-incentive   = { git = "https://github.com/decdn/decdn.git", tag = "vX.Y.Z", features = ["redb"] }
decdn-common      = { git = "https://github.com/decdn/decdn.git", tag = "vX.Y.Z" }
```

Only flip this repo's visibility to public **after** `decdn` itself is
public — a public repo with a path dependency into a private sibling
doesn't build for anyone outside this workspace, and a public repo pointing
at a private git dependency leaks the existence (and tag names) of a repo
nobody can otherwise see.
