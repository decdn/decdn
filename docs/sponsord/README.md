# sponsor

`sponsor` is deCDN's sponsored on-ramp: a gateway (`sponsord`) that pays the
initial USDC deposit for a client's first payment channel on the Arbitrum
Sepolia testnet, so a new user can fetch content from the network without
first acquiring testnet USDC or setting up a wallet by hand. A captcha-gated
`/fund` flow opens (and, via `/topup`, refills) a channel from the gateway's
own treasury wallet, subject to a per-client monthly spend cap; a companion
CLI wrapper (`onramp`, in `crates/wrapper`) drives the actual `decdn` fetch
using a locally generated keystore, never handing that key to the gateway.
It is a wrapper around `decdn`'s own paid-pull path — see `../decdn` for the
protocol and contracts this all sits on top of.

## Crates

- `crates/server` (binary `sponsord`) — the HTTP gateway: `/healthz`,
  `/decdn.sh` (templated installer), `/fund`, `/channel`, `/topup`.
- `crates/wrapper` (binary `onramp`) — the end-user CLI: reads
  `~/.decdn/sponsor.toml` (written by the installer), talks to `sponsord` to
  open/top-up a channel, then drives a real `decdn` paid fetch with a local
  keystore.

## Running the server

```bash
export SPONSOR_RPC_URL=https://sepolia-rollup.arbitrum.io/rpc
export SPONSOR_PAYMENT_CHANNEL_ADDR=0x...
export SPONSOR_CAPACITY_BOND_ADDR=0x...
export SPONSOR_TREASURY_KEYSTORE=/path/to/treasury-keystore.json
export SPONSOR_TREASURY_PASSWORD=...
export SPONSOR_TURNSTILE_SECRET=...
export SPONSOR_TURNSTILE_SITEKEY=...
cargo run -p sponsord
```

`sponsord` binds `SPONSOR_BIND` (default `127.0.0.1:8080`), serves the HTTP
routes below, and spawns a background sweep (`reclaim::run`) that reclaims
expired channels' escrow back to the treasury every
`SPONSOR_RECLAIM_INTERVAL_SECS`.

### `SPONSOR_*` environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SPONSOR_BIND` | no | `127.0.0.1:8080` | Address the HTTP server listens on |
| `SPONSOR_PUBLIC_URL` | no | `https://up.decdn.org` | This gateway's own public base URL; baked into the `/decdn.sh` installer as `{{GATEWAY_BASE}}` |
| `SPONSOR_RPC_URL` | **yes** | — | Arbitrum Sepolia RPC endpoint |
| `SPONSOR_CHAIN_ID` | no | `421614` | Chain id (Arbitrum Sepolia) |
| `SPONSOR_PAYMENT_CHANNEL_ADDR` | **yes** | — | `PaymentChannel` contract address |
| `SPONSOR_CAPACITY_BOND_ADDR` | **yes** | — | `CapacityBond` contract address (used for hash → node/provider discovery) |
| `SPONSOR_TREASURY_KEYSTORE` | **yes** | — | Path to the treasury hot-wallet's encrypted keystore JSON |
| `SPONSOR_TREASURY_PASSWORD` | **yes** | — | Password to decrypt `SPONSOR_TREASURY_KEYSTORE` (never logged, never written to disk elsewhere) |
| `SPONSOR_INITIAL_DEPOSIT_MICRO_USDC` | no | `2_000_000` ($2) | Deposit `/fund` opens a fresh channel with |
| `SPONSOR_WORKING_BALANCE_MICRO_USDC` | no | `2_000_000` ($2) | Target balance `/topup` refills a channel to |
| `SPONSOR_MONTHLY_CAP_MICRO_USDC` | no | `10_000_000` ($10) | Per-client monthly spend cap enforced by `cap.rs` |
| `SPONSOR_TURNSTILE_SECRET` | **yes** | — | Cloudflare Turnstile server-side secret, used to verify captcha tokens |
| `SPONSOR_TURNSTILE_SITEKEY` | **yes** | — | Cloudflare Turnstile sitekey, interpolated into the `/fund` widget page |
| `SPONSOR_DATA_DIR` | no | `./data` | Directory for the redb store (channel records, cap bookkeeping) |
| `SPONSOR_TOPUP_MAX_SKEW_SECS` | no | `120` | Max allowed clock skew for `/topup`'s signed-timestamp auth |
| `SPONSOR_CHANNEL_TTL_SECS` | no | `604800` (7 days) | Local bookkeeping TTL the reclaim sweep uses to pick candidate channels |
| `SPONSOR_RECLAIM_INTERVAL_SECS` | no | `3600` | How often the reclaim sweep runs |

## The wrapper (`onramp`) flow

1. A user runs the installer served at `GET {{gateway}}/decdn.sh` (see
   `assets/decdn.sh`). It installs the `decdn` and `onramp` binaries,
   writes `~/.decdn/sponsor.toml` with the gateway's contract addresses and
   RPC URL already filled in, and generates a local client keystore
   (`~/.decdn/client/keystore.json`) if one doesn't already exist.
2. The user sets `DECDN_KEYSTORE_PASSWORD` and runs `onramp <hash> -o
   out.bin`.
3. `onramp` loads `~/.decdn/sponsor.toml` (schema: `crates/wrapper/src/config.rs`'s
   `Profile`), calls the gateway's `/fund` (captcha-gated) or `/topup` to get
   a funded channel, then drives `decdn`'s real paid-pull path using the
   local keystore to sign vouchers — the gateway never sees or holds the
   client's private key, only its own treasury key.

The `~/.decdn/sponsor.toml` schema is a hard contract between the installer
(`assets/decdn.sh`) and the wrapper (`crates/wrapper/src/config.rs`): field
names must match exactly. Current fields: `gateway_base`, `keystore_path`,
`decdn_bin`, `data_dir`, `rpc_url`, `payment_channel`, `capacity_bond`
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
