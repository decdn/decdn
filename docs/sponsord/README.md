# sponsord

[![crates.io](https://img.shields.io/crates/v/sponsord?logo=rust&label=crates.io)](https://crates.io/crates/sponsord)
[![docs.rs](https://img.shields.io/docsrs/sponsord-api?logo=docs.rs&label=docs.rs)](https://docs.rs/sponsord-api)
[![Docker: sponsord](https://img.shields.io/docker/v/decdn/sponsord?sort=semver&logo=docker&label=sponsord)](https://hub.docker.com/r/decdn/sponsord)
[![Docker: sponsord-onramp](https://img.shields.io/docker/v/decdn/sponsord-onramp?sort=semver&logo=docker&label=sponsord-onramp)](https://hub.docker.com/r/decdn/sponsord-onramp)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

Sponsored downloads for [deCDN](../../README.md): your users
download content from deCDN, and you pay for it. They run one command,
pass a check in the browser (a captcha by default), and download. There's no
wallet, key, or token for them to manage.

You run two services next to a `PaymentPool` you fund:

```text
 user's machine                        ──────────────────── your servers ─────────────────────        chain
┌─────────────────┐ GET /v1/capability ┌──────────────────┐ POST /v1/capabilities ┌──────────┐
│ decdn.sh / .ps1 │ ─────────────────▶ │ sponsord-onramp  │ ────────────────────▶ │ sponsord │ ──▶ PaymentPool
│ (throwaway key  │ ◀── installers ─── │ gate · hand-off  │    (bearer token)     │ signs,   │     (your pool)
│  per download)  │                    │                  │                       │ tops up  │
└────────┬────────┘                    └─────────▲────────┘                       └──────────┘
         │ decdn get                             │ POST /v1/fund (gate passed)
         ▼                                       │
    deCDN nodes                           user's browser
```

- **`sponsord`** holds your *treasury wallet*, the key that owns your pool.
  It signs capped, expiring *capabilities* (`dcap1:` tokens) for callers
  holding its bearer token, and keeps the pool topped up from that wallet. It
  has no opinion on who deserves one; that's the gate's job.
- **`sponsord-onramp`** is the public part: the installers, the gate page,
  and the API the installers poll. Turnstile is the built-in gate; you can
  write your own in Rust (see the [integrator guide](integrator.md)).
- **The installers** (`decdn.sh`, `decdn.ps1`), served by the onramp, are
  all a user runs. They install `decdn`, give each download a throwaway key,
  get a capability for it through the onramp, and hand the pull to `decdn`,
  which verifies every byte against the content hash.

A capability lets one key spend up to its cap against your pool until it
expires. Issuing one costs zero transactions and locks no deposit per user.
Your loss is bounded by the gate, the per-capability cap, and the money
behind the pool: its balance plus whatever the treasury wallet can still top
it up with, since the daemon checks the pool periodically (hourly by
default) and tops it up whenever it is below the low-water mark.

## Quickstart

1. Create the treasury wallet (for example with `decdn key-gen`), fund it
   with gas and USDC, and open your pool from it once with `decdn pool open`.
   Note the pool id it prints; you'll set it as `SPONSORD_POOL_ID`. The
   [operator guide](operator.md) has the details.
2. Configure and start both services. With Docker:

   ```bash
   cd deploy/sponsord
   cp sponsord.env.example sponsord.env                # edit
   cp sponsord-onramp.env.example sponsord-onramp.env  # edit
   # put the secrets in secrets/ (see operator.md)
   docker compose up -d
   ```

   That runs the released images (`ghcr.io/decdn/sponsord`,
   `ghcr.io/decdn/sponsord-onramp`). Or run the release binaries under
   systemd ([`deploy/sponsord/systemd/`](../../deploy/sponsord/systemd)).
3. Put TLS in front of the onramp (it listens on `127.0.0.1:8080` by
   default), then give users one line per download:

   ```bash
   curl -fsSL https://downloads.example.org/decdn.sh | sh -s -- get 'decdn:<hash>/?ns=<id>'
   ```

   ```powershell
   & ([scriptblock]::Create((irm https://downloads.example.org/decdn.ps1))) get 'decdn:<hash>/?ns=<id>'
   ```

   The `decdn:` URI names the bundle by its manifest hash; a path after the
   `/` downloads one file of it. A URI without the `/` names a single blob,
   written to the file `-o <file>` names. `?ns=<id>` is optional. It takes the numeric
   deCDN namespace the bundle is published under, not your pool id, and lets
   a node that hasn't cached the bundle fetch it from that namespace's
   origins. Without it, only nodes that already cache the bundle can serve
   it. Any flag after the URI goes straight to `decdn get`
   ([installer](installer.md)).

## Documentation

- [Architecture](architecture.md): the components, the flow, and the
  trust boundaries.
- [Operator guide](operator.md): running the daemon and the onramp,
  every setting, metrics, releases.
- [Integrator guide](integrator.md): your own gate, the daemon API, and
  embedding the library. OpenAPI documents are in
  [`crates/sponsord-api/openapi/`](../../crates/sponsord-api/openapi).
- [Security model](security.md): what each key can do, and what bounds
  a loss.
- [The installer](installer.md): what happens on a user's machine.

## Crates

The sponsord crates are members of the deCDN workspace. They share its
version and ship with every deCDN release, from the same signed `vX.Y.Z`
tag:

| Crate | What it is | Release archives | Image |
|---|---|---|---|
| [`sponsord`](../../crates/sponsord) | The signing and top-up daemon | Linux (x86_64, aarch64) | `ghcr.io/decdn/sponsord` |
| [`sponsord-onramp`](../../crates/sponsord-onramp) | The public onramp: gate, installers, hand-off | Linux (x86_64, aarch64) | `ghcr.io/decdn/sponsord-onramp` |
| [`sponsord-core`](../../crates/sponsord-core) | The daemon's logic as a library, to embed instead of running the daemon | — | — |
| [`sponsord-api`](../../crates/sponsord-api) | Wire types, error codes and typed clients for both servers | — | — |
| [`sponsord-e2e`](../../crates/sponsord-e2e) | Tests that run the daemon, the onramp and an installer together (not released) | — | — |

All but `sponsord-e2e` also go to crates.io. Images are mirrored to Docker
Hub. Releases are signed by a maintainer: see [RELEASING.md](../../RELEASING.md),
and [SECURITY.md](../../SECURITY.md) for verifying one.

## Building

```bash
cargo build -p sponsord -p sponsord-onramp
cargo nextest run -p sponsord -p sponsord-core -p sponsord-api -p sponsord-onramp -p sponsord-e2e
```

The deCDN crates are path dependencies in the same workspace, so a change to
a shared type is one pull request. [CONTRIBUTING.md](../../CONTRIBUTING.md)
covers the toolchain and the checks; the anvil-backed tests (`chain_pool`,
`binary_anvil`) build under `--features sponsord-core/anvil-e2e,sponsord/anvil-e2e`
and need `anvil` and `forge` on `PATH`.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or
[MIT license](../../LICENSE-MIT) at your option.
