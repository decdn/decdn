# Operator guide

You run one `sponsord` and one `sponsord-onramp` per pool. Start the daemon
first: the onramp reads its `/v1/info` at startup and exits if it can't.

## 1. The treasury wallet and the pool

The treasury wallet owns your `PaymentPool` pool, signs every capability,
and pays for top-ups. Create it as an encrypted keystore (for example with
`decdn key-gen`), fund it with gas and USDC, then open the pool once:

```bash
decdn pool open ...   # from the treasury wallet; note the pool id it prints
```

Keep the keystore and its password on the daemon's host only.

## 2. Run the daemon

Every setting is a flag and an environment variable; `sponsord --help` lists
them. Secrets can be given inline or as files (`*_FILE`); prefer files.

| Variable | Default | Purpose |
|---|---|---|
| `SPONSORD_API_TOKEN` / `_FILE` | required | Bearer token callers present; ≥ 32 bytes (`openssl rand -hex 32`) |
| `SPONSORD_RPC_URL` | required | JSON-RPC endpoint of the pool's chain |
| `SPONSORD_PAYMENT_POOL_ADDR` | required | `PaymentPool` contract |
| `SPONSORD_POOL_ID` | required | Your pool, from `decdn pool open` |
| `SPONSORD_TREASURY_KEYSTORE` | required | The treasury wallet's keystore JSON |
| `SPONSORD_TREASURY_PASSWORD` / `_FILE` | required | Its password |
| `SPONSORD_BIND` | `127.0.0.1:8090` | Listen address |
| `SPONSORD_CHAIN_ID` | `421614` | Chain id (Arbitrum Sepolia) |
| `SPONSORD_MAX_SPENDING_CAP_MICRO_USDC` | `5000000` ($5) | Largest cap per capability |
| `SPONSORD_MAX_TTL_SECS` | `172800` (48 h) | Longest TTL per capability |
| `SPONSORD_POOL_LOW_WATER_MICRO_USDC` | `20000000` ($20) | Top up below this |
| `SPONSORD_POOL_REFILL_MICRO_USDC` | `100000000` ($100) | Each top-up adds this |
| `SPONSORD_POOL_WATCH_INTERVAL_SECS` | `3600` | How often the balance is checked |

At startup the daemon checks that the keystore's key owns the pool on-chain
and refuses to start otherwise. It keeps no state.

Keep it private: it binds to localhost by default and only the onramp (or
your own gate) should reach it. Put a TLS reverse proxy in front if a caller
on another host needs it.

## 3. Run the onramp

| Variable | Default | Purpose |
|---|---|---|
| `ONRAMP_PUBLIC_URL` | required | The onramp's public base URL; baked into the installers |
| `ONRAMP_DAEMON_TOKEN` / `_FILE` | required | The daemon's API token |
| `ONRAMP_RPC_URL` | required | Public RPC endpoint users' `decdn` reads the chain through |
| `ONRAMP_CAPACITY_BOND_ADDR` | required | `CapacityBond` contract, for node discovery |
| `ONRAMP_TURNSTILE_SECRET` / `_FILE` | required (Turnstile gate) | Cloudflare Turnstile secret |
| `ONRAMP_TURNSTILE_SITEKEY` | required (Turnstile gate) | Cloudflare Turnstile sitekey |
| `ONRAMP_DECDN_RELEASE`, `ONRAMP_DECDN_SUMS_SHA256` | required | `decdn` release the installers install, and the SHA-256 of its `SHA256SUMS` |
| `ONRAMP_CLI_RELEASE`, `ONRAMP_CLI_SUMS_SHA256` | required | `decdn-sponsored` release (`decdn-sponsored-vX.Y.Z`), likewise; both values are printed in its release notes |
| `ONRAMP_BIND` | `127.0.0.1:8080` | Listen address |
| `ONRAMP_DAEMON_URL` | `http://127.0.0.1:8090` | The daemon |
| `ONRAMP_SPENDING_CAP_MICRO_USDC` | daemon maximum | Cap requested per capability |
| `ONRAMP_TTL_SECS` | daemon maximum | TTL requested per capability |
| `ONRAMP_SLASH_JUDGE_ADDR` | none | `SlashJudge` contract, handed to `decdn` |
| `ONRAMP_MIN_CLI_VERSION` | none | Oldest `decdn-sponsored` accepted; older ones are told to re-run the installer |
| `ONRAMP_GATE` | `turnstile` | The gate; `custom` only for programs that embed the onramp with their own (docs/integrator.md) |
| `ONRAMP_BRAND_NAME` | `deCDN` | Name on the gate page |
| `ONRAMP_GATE_TEMPLATE` | built-in | Your own gate page (below) |
| `ONRAMP_CLIENT_IP_HEADER` | TCP peer | Header your reverse proxy puts the client address in |
| `ONRAMP_FUND_RATE_PER_MIN` | `10` | `POST /v1/fund` per client address per minute; 0 = off |
| `ONRAMP_POLL_RATE_PER_MIN` | `120` | `GET /v1/capability` per client address per minute; 0 = off |
| `ONRAMP_RELEASES_BASE` | `https://github.com/decdn` | Where installers download releases from |

The onramp refuses to start if the daemon is unreachable, or if its
requested cap or TTL is above the daemon's maximum. A change to the daemon's
pool settings needs an onramp restart. Run it under a supervisor that
restarts it on failure.

It holds issued capabilities in memory for the browser-to-CLI hand-off (30
minutes at most). A restart loses only hand-offs in flight; those users pass
the gate again.

### Behind a reverse proxy

Terminate TLS in front of the onramp and set `ONRAMP_PUBLIC_URL` to what
users reach. If the proxy is the only way in, set `ONRAMP_CLIENT_IP_HEADER`
so rate limits and the gate see the real client address: `CF-Connecting-IP`
behind Cloudflare, or `X-Forwarded-For` (the right-most address counts,
the one your proxy appended). Don't set it if clients can reach the onramp
directly, since they could then choose their own address.

### Your own gate page

`ONRAMP_GATE_TEMPLATE` replaces the built-in Turnstile page with your HTML.
These placeholders are substituted:

| Placeholder | Value |
|---|---|
| `{{CLIENT}}` | The download key's address (hex) |
| `{{SITEKEY}}` | `ONRAMP_TURNSTILE_SITEKEY` |
| `{{BRAND_NAME}}` | `ONRAMP_BRAND_NAME`, HTML-escaped |

Once the widget passes, the page must `POST /v1/fund` with
`{"client": "{{CLIENT}}", "proof": "<turnstile token>"}`. A `200` means the
person can return to their terminal; `409` means the key has expired and the
download must be started again. The built-in page is
[`crates/sponsord-onramp/assets/turnstile.html`](../crates/sponsord-onramp/assets/turnstile.html).
For a different kind of check, see [integrator.md](integrator.md).

## 4. Deploy

- **Docker:** [`deploy/compose.yaml`](../deploy/compose.yaml) runs the
  released images, `ghcr.io/decdn/sponsord` and
  `ghcr.io/decdn/sponsord-onramp` (also on Docker Hub as `decdn/…`; amd64 and
  arm64; uid 1000), and passes secrets as files. Only the onramp is published,
  on localhost, for your proxy. The daemon refuses a keystore anyone but its
  owner can read, so the secret files must be mode `0600` and owned by uid
  1000. Pull by the signed digest to pin exact bytes
  ([SECURITY.md](../SECURITY.md#verify-a-container-image)), or build from
  source with `docker compose build`
  ([`deploy/Dockerfile`](../deploy/Dockerfile)).
- **systemd:** [`deploy/systemd/`](../deploy/systemd) has a unit for each,
  with secrets as `LoadCredential=` credentials (systemd 252 or later). Some
  systemd versions load credentials at mode `0440`, which the daemon refuses
  for a keystore. So the daemon's unit copies the keystore to a `0600` file
  under `/run/sponsord` before start.
- **Release binaries:** each `sponsord-vX.Y.Z` and `sponsord-onramp-vX.Y.Z`
  GitHub Release has its binary for Linux x86_64 and aarch64, with a signed
  `SHA256SUMS`.

## 5. Monitor

`GET /healthz` on both answers `{"ok": true}`. The daemon's `GET /metrics`
(Prometheus text, no token) has:

| Metric | Type | Meaning |
|---|---|---|
| `sponsord_capabilities_issued_total{registered}` | counter | Capabilities handed out, fresh or re-signed for a registered key |
| `sponsord_request_errors_total{code}` | counter | Requests answered with an error, by code |
| `sponsord_pool_remaining_micro_usdc` | gauge | Pool balance at the keeper's last read |
| `sponsord_pool_last_check_unix` | gauge | Last successful balance read |
| `sponsord_pool_last_topup_unix` | gauge | Last successful top-up |
| `sponsord_pool_topups_total` | counter | Successful top-ups |
| `sponsord_pool_keeper_failures_total` | counter | Failed balance reads and top-ups |

Alert on `sponsord_pool_keeper_failures_total` rising (the treasury wallet
may be out of USDC or gas), on `sponsord_pool_last_check_unix` going stale,
and on the treasury wallet's own balance.

## 6. Upgrades

Point `ONRAMP_CLI_RELEASE`/`ONRAMP_CLI_SUMS_SHA256` (and the `decdn` pair) at
a new release and restart the onramp: new installs get it. Users who
already installed keep their CLI until they re-run the installer; set
`ONRAMP_MIN_CLI_VERSION` to make them.
