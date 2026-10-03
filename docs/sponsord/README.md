# sponsord

Sponsored downloads for [deCDN](https://github.com/decdn/decdn): your users
download content from deCDN, and you pay for it. They install one small CLI,
pass a check in the browser (a captcha by default), and download. There's no
wallet, key, or token for them to manage.

You run two services next to a `PaymentPool` you fund:

```
  user's machine                       your servers                       chain
 ┌────────────────┐  /v1/capability  ┌──────────────────┐  POST /v1/    ┌──────────┐
 │ decdn-sponsored│ ───────────────▶ │ sponsord-onramp  │  capabilities │ sponsord │──▶ PaymentPool
 │  (throwaway    │ ◀─── installers  │  gate · hand-off │ ────────────▶ │ signs,   │    (your pool)
 │   key/download)│                  │  /v1/profile     │  bearer token │ tops up  │
 └──────┬─────────┘                  └────────▲─────────┘               └──────────┘
        │ decdn bundle pull                   │ POST /v1/fund (gate passed)
        ▼                                     │
   deCDN nodes                           user's browser
```

- **`sponsord`** holds the key that owns your pool. It signs capped, expiring
  *capabilities* (`dcap1:` tokens) for callers holding its bearer token, and
  keeps the pool topped up from that wallet. It has no opinion on who
  deserves one; that's the gate's job.
- **`sponsord-onramp`** is the public part: the installers, the gate page,
  and the API the CLI polls. Turnstile is the built-in gate; you can plug in
  your own.
- **`decdn-sponsored`** is the end-user CLI. It gives each download a
  throwaway key, gets a capability for it through the onramp, and hands the
  pull to `decdn`, which verifies every byte against the content hash.

A capability lets one key spend up to its cap against your pool until it
expires. Issuing one costs zero transactions and locks no deposit per user.
Your loss is bounded by the gate, the per-capability cap, and the money
behind the pool: its balance plus whatever the treasury wallet can still top
it up with, since the daemon refills a drained pool automatically.

## Quickstart

1. Open a pool once, from the treasury wallet: `decdn pool open`. Note its
   id.
2. Configure and start both services. With Docker:

   ```bash
   cp deploy/sponsord.env.example deploy/sponsord.env                # edit
   cp deploy/sponsord-onramp.env.example deploy/sponsord-onramp.env  # edit
   # put the secrets in deploy/secrets/ (see docs/operator.md)
   docker compose -f deploy/compose.yaml up -d
   ```

   Or run the release binaries under systemd (`deploy/systemd/`).
3. Put TLS in front of the onramp, then give users one line per download:

   ```bash
   curl -fsSL https://downloads.example.org/decdn.sh | sh -s -- pull b3:<hash> --namespace <id>
   ```
   ```powershell
   irm https://downloads.example.org/decdn.ps1 | iex; decdn-sponsored pull b3:<hash> --namespace <id>
   ```

## Documentation

- [Architecture](docs/architecture.md): the components, the flow, and the
  trust boundaries.
- [Operator guide](docs/operator.md): running the daemon and the onramp,
  every setting, metrics, releases.
- [Integrator guide](docs/integrator.md): your own gate, the daemon API, and
  embedding the library. OpenAPI documents are in
  [`docs/openapi/`](docs/openapi).
- [Security model](docs/security.md): what each key can do, and what bounds
  a loss.
- [The CLI](docs/cli.md): what `decdn-sponsored` does on a user's machine.

## Crates

| Crate | What it is |
|---|---|
| [`sponsord`](crates/sponsord) | The signing and top-up daemon |
| [`sponsord-onramp`](crates/sponsord-onramp) | The public onramp: gate, installers, hand-off |
| [`decdn-sponsored`](crates/decdn-sponsored) | The end-user CLI |
| [`sponsord-core`](crates/sponsord-core) | The daemon's logic as a library, to embed instead of running the daemon |
| [`sponsord-api`](crates/sponsord-api) | Wire types, error codes and typed clients for both servers |
| [`sponsord-e2e`](crates/e2e) | Tests that run all three together |

## Building

```bash
cargo build --workspace
cargo test --workspace
```

The deCDN crates are git dependencies on
[decdn/decdn](https://github.com/decdn/decdn), at the commit `Cargo.lock`
pins. [CONTRIBUTING.md](CONTRIBUTING.md) covers the checks, the anvil tests,
and building against a local deCDN checkout. [RELEASING.md](RELEASING.md)
covers releases.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
