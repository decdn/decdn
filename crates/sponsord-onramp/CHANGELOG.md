# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate, under
     [Unreleased]. `cargo release` only opens the version heading above those
     entries; git-cliff generates the GitHub release notes. Conventions:
     RELEASING.md § Changelogs. -->

All notable changes to `sponsord-onramp` are documented in this file. It shares
one version with every sponsord crate, released together from `vX.Y.Z`
tags; a release with no entry here changed nothing in this crate.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

## [0.1.0] - 2026-10-14

### Changed (BREAKING)

- **`decdn.sh` and `decdn.ps1` take `get <decdn: URI>` and run `decdn get`
  (decdn#2449).** The one-liner is
  `sh -s -- get 'decdn:<hash>/[<path>][?ns=<id>]' [-o <dir>] [<decdn get flags>]`.
  The URI and every argument after it go to `decdn get` as given, with
  `-o .` added when no output directory is given. A path after the `/`
  downloads that one file of the bundle. The download's state stays keyed
  on the hash. Contract-breaking: `pull <hash>`, the `b3:`/`0x`/bare-hash
  forms and `--namespace` are refused (the namespace is `?ns=<id>` in the
  URI), and so is a URI without the trailing `/` (a raw blob). The
  installers need a `decdn` release that ships `decdn get`.

## [0.0.4] - 2026-10-13

### Changed (BREAKING)

- **`decdn.sh` and `decdn.ps1` take the hash first and pass every argument
  after it to `decdn bundle pull`.** Any `decdn bundle pull` flag now works
  in the one-liner (an unknown flag was refused before), with `-o .` added
  when no output directory is given. `decdn` checks the arguments, so a bad
  `--namespace` is reported after the gate instead of before the install.
  Contract-breaking: a command that puts `-o` or `--namespace` before the
  hash (`pull -o <dir> <hash>`) is refused; the documented order,
  `pull <hash> [-o <dir>] [--namespace <id>]`, works as before.

## [0.0.3] - 2026-10-12

### Changed (BREAKING)

- **The installers download through the onramp themselves; there is no
  `decdn-sponsored` CLI.** `decdn.sh pull <hash> [-o <dir>] [--namespace <id>]`
  (and `decdn.ps1` the same) installs only `decdn`, makes the download's
  throwaway key with `decdn key-gen`, gets its capability through the gate,
  and runs `decdn bundle pull --capability <token>`. The state under
  `~/.decdn/sponsored/downloads/<hash>/` gains an `expiry` file; a download
  interrupted under the CLI starts again with a new key. Contract-breaking:
  the installers no longer write `~/.decdn/sponsor.toml`, and on Windows the
  one-line form is
  `& ([scriptblock]::Create((irm <onramp>/decdn.ps1))) pull b3:<hash>`.
  Commands of the form `curl … | sh -s -- pull b3:<hash>` keep working.
- **The download profile is baked into the installers, and
  `GET /v1/profile` is removed.** The chain id, RPC URL, `PaymentPool`,
  `CapacityBond` and `SlashJudge` are rendered into the scripts at startup,
  beside the onramp URL and the `decdn` pin. `decdn-sponsored` binaries
  already installed read `/v1/profile` on every run, so they stop working
  against this onramp; re-running the installer command replaces them.
- **`ONRAMP_CLI_RELEASE`, `ONRAMP_CLI_SUMS_SHA256` and
  `ONRAMP_MIN_CLI_VERSION` are removed.** The onramp ignores them; delete
  them from your configuration.

### Added

- **`GET /v1/capability` and `POST /v1/fund` return the capability's
  `expiry`** beside its `token`, so the installer can replace a capability
  near expiry without decoding it.
- **`decdn.sh` says how to put `~/.local/bin` on `PATH`.** When the
  directory is missing from `PATH`, a run without `pull` prints the line
  that adds it for the user's shell (zsh, bash, fish, or `~/.profile`
  otherwise).

### Changed

- **Debian 13 image base.** The container image is built on
  `debian:trixie-slim` (was `bookworm-slim`).
- **The installers skip binaries already installed from the pinned
  release.** `decdn.sh` and `decdn.ps1` record each install's `SHA256SUMS`
  digest and target in `~/.decdn/installer/<binary>`, and download nothing
  for a binary whose marker matches. A new pin, a deleted marker or a missing
  binary installs it again.

## [0.0.2] - 2026-10-09

## [0.0.1] - 2026-10-09

### Changed (BREAKING)

- **API under `/v1`.** `POST /v1/fund` (body `{"client", "proof"}`; was
  `turnstile_token`) and `GET /v1/capability`. `captcha_failed` is now
  `gate_failed`.
- **`ONRAMP_WRAPPER_RELEASE` is `ONRAMP_CLI_RELEASE`**, and
  `ONRAMP_WRAPPER_SUMS_SHA256` is `ONRAMP_CLI_SUMS_SHA256`. The tag is a
  sponsord release, `vX.Y.Z`, as `ONRAMP_DECDN_RELEASE` is decdn's. The installers take each version from the
  onramp, so archive names are unchanged.
- **`ONRAMP_PUBLIC_URL` is required.** It no longer defaults to decdn's own
  onramp. URLs baked into the installers and the Turnstile sitekey are
  validated at startup, and `ONRAMP_PUBLIC_URL` and `ONRAMP_RELEASES_BASE`
  must be bases, without a query or fragment.
- **No data directory.** Issued capabilities live in memory for the
  browser-to-CLI hand-off; `ONRAMP_DATA_DIR` and the redb store are gone.
- **The installers write only `onramp_url` and `decdn_bin`.** The CLI reads
  the chain and contracts from `/v1/profile`.

### Added

- **Pluggable gate.** The `Gate` trait, with Turnstile built in
  (`ONRAMP_GATE`); the client address is passed to Turnstile as `remoteip`.
  `ONRAMP_GATE=custom` is for programs that embed the onramp with their own
  gate: the Turnstile settings are then not required (the stock binary
  refuses it).
- **Branding.** `ONRAMP_BRAND_NAME` and `ONRAMP_GATE_TEMPLATE`.
- **`GET /v1/profile`.** Chain id, RPC URL, contracts, and the optional
  `ONRAMP_SLASH_JUDGE_ADDR` and `ONRAMP_MIN_CLI_VERSION`.
- **Rate limits.** Per client address on `/v1/fund` and `/v1/capability`
  (`ONRAMP_FUND_RATE_PER_MIN`, `ONRAMP_POLL_RATE_PER_MIN`), behind a trusted
  proxy keyed on `ONRAMP_CLIENT_IP_HEADER` (its right-most address).
- **Flags, secrets from files, `--version`.** Every `ONRAMP_*` setting is
  also a flag; `ONRAMP_DAEMON_TOKEN_FILE`, `ONRAMP_TURNSTILE_SECRET_FILE`;
  `ONRAMP_RELEASES_BASE`.
- **Graceful shutdown.**
- **Container image.** `ghcr.io/decdn/sponsord-onramp` (mirrored to
  `decdn/sponsord-onramp` on Docker Hub), amd64 and arm64; binds
  `0.0.0.0:8080`, runs as uid 1000, keeps nothing on disk.
- **Release metadata.** Released with every sponsord crate under one
  version (`vX.Y.Z`), with crates.io metadata, dual-licensed MIT OR
  Apache-2.0.

### Changed

- **MSRV is Rust 1.99.0** (was 1.95.0), matching decdn.
- **HTTPS through rustls.** reqwest 0.13 replaces OpenSSL (native-tls)
  with rustls and checks certificates with the platform verifier, so the
  binary no longer links `libssl` on Linux.
- **Lighter dependency tree.** The onramp depends on `alloy-primitives`
  instead of all of `alloy`, whose `Address` was the only thing it used. That
  cuts its build from 394 crates to 201.
