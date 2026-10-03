# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `sponsord-api`: the wire types, error codes and route paths of the daemon
  and the onramp, typed clients (`DaemonClient`, `OnrampClient`), and OpenAPI
  documents for both servers in `docs/openapi/`.
- Both servers take every setting as a flag or an environment variable
  (`--help` lists them), and every secret also as a file:
  `SPONSORD_API_TOKEN_FILE`, `SPONSORD_TREASURY_PASSWORD_FILE`,
  `ONRAMP_DAEMON_TOKEN_FILE`, `ONRAMP_TURNSTILE_SECRET_FILE`.
- `ONRAMP_RELEASES_BASE` (default `https://github.com/decdn`): where the
  installers download binaries from.
- Onramp: a pluggable `Gate` (Turnstile built in; `ONRAMP_GATE`, whose
  `custom` value lets a program embedding the onramp supply its own gate,
  with the Turnstile settings then not required), gate-page
  branding (`ONRAMP_BRAND_NAME`, `ONRAMP_GATE_TEMPLATE`), per-IP rate limits
  on `/v1/fund` and `/v1/capability` (`ONRAMP_FUND_RATE_PER_MIN`,
  `ONRAMP_POLL_RATE_PER_MIN`), the client's address passed to the gate
  (`ONRAMP_CLIENT_IP_HEADER` behind a trusted proxy), and graceful shutdown.
- `GET /v1/profile`: the chain and contracts the CLI runs `decdn` with, and
  an optional `ONRAMP_MIN_CLI_VERSION`; new optional `ONRAMP_SLASH_JUDGE_ADDR`.
- `decdn-sponsored --version`.
- Daemon `GET /metrics` (Prometheus): capabilities issued, request errors
  by code, and the keeper's view of the pool (remaining, last check and
  top-up, top-ups, failures). Graceful shutdown on SIGTERM lets a top-up in
  flight finish.
- `sponsord-e2e`: tests that run the daemon, onramp and CLI together.
- `deploy/`: a Dockerfile for both servers, a compose file, systemd units
  with secrets as credentials, and env examples.
- `docs/`: architecture, operator, integrator, security and CLI guides;
  `RELEASING.md`.
- `sponsord-core`: `Sponsor::keeper_status()`, what the pool keeper has seen
  and done.

### Fixed

- Behind `ONRAMP_CLIENT_IP_HEADER=X-Forwarded-For`, the onramp keyed rate
  limits on the client-controlled left-most address; it now uses the
  right-most (proxy-appended) one and falls back to the TCP peer.

### Changed

- `sponsor.toml` holds only `onramp_url`, `decdn_bin` and an optional
  `data_dir`; the CLI reads the chain and contracts from the onramp's
  `/v1/profile` on every run (falling back to the last one a resumed
  download saved), so they can change without users reinstalling.
- The onramp keeps capabilities in memory for the browser-to-CLI hand-off
  instead of a redb file; `ONRAMP_DATA_DIR` is gone.
- The CLI writes each download key's address beside it instead of reading
  it from the keystore, so it no longer needs alloy's `geth-compat` keystores.

- Crate directories are named after their packages, and the vocabulary is
  one word per component: onramp (not gateway), CLI (not wrapper).
  `sponsor.toml`'s `gateway_base` is `onramp_url`; `ONRAMP_WRAPPER_*` is
  `ONRAMP_CLI_*`.
- Onramp JSON routes move under `/v1`: `POST /v1/fund` and
  `GET /v1/capability`. The fund body's `turnstile_token` is `proof`, and
  `captcha_failed` is `gate_failed`.
- `sponsord-core` takes any alloy signer that signs hashes (local key, KMS)
  in `Sponsor::connect(signer, ChainConfig, Limits)` instead of a keystore
  path and password; terms are `TermsRequest`/`Terms`, money is `MicroUsdc`
  throughout, and the keeper takes a `KeeperConfig` and a
  `CancellationToken`.

- deCDN crates are git dependencies on `github.com/decdn/decdn`, so a fresh
  clone builds on its own; `Cargo.lock` pins the deCDN commit and `decdn.ref`
  is gone.
- Licensed under MIT OR Apache-2.0.
- `ONRAMP_PUBLIC_URL` is required; it no longer defaults to decdn's own
  onramp. URLs baked into the installers and the Turnstile sitekey are
  validated at startup.
