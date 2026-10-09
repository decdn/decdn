# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate, under
     [Unreleased]. `cargo release` only opens the version heading above those
     entries; git-cliff generates the GitHub release notes. Conventions:
     RELEASING.md § Changelogs. -->

All notable changes to `decdn-sponsored` are documented in this file. It shares
one version with every sponsord crate, released together from `vX.Y.Z`
tags; a release with no entry here changed nothing in this crate.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Added

- **Container image.** Each release publishes `ghcr.io/decdn/decdn-sponsored`
  (mirrored to `decdn/decdn-sponsored` on Docker Hub; amd64 and arm64), with
  the CLI and the `decdn` binary from decdn's release at the pinned tag,
  verified against decdn's signed `SHA256SUMS` and anchored to the decdn
  commit `Cargo.lock` locks.
- **Configuration from the environment.** When `DECDN_SPONSOR_ONRAMP_URL` is
  set, the CLI does not read `~/.decdn/sponsor.toml`. It takes the onramp
  from that variable, the `decdn` binary from `DECDN_SPONSOR_DECDN_BIN`
  (default `decdn` on `PATH`) and the state root from
  `DECDN_SPONSOR_DATA_DIR` (default `~/.decdn/sponsored`). The variable names
  are now part of the CLI's contract. An empty value counts as unset, and a
  value that is not UTF-8 is an error. Nothing changes when the URL is
  unset.

## [0.0.2] - 2026-10-09

### Changed

- **decdn v0.0.1.** The decdn crates are locked at decdn's `v0.0.1` release
  tag (`454e5137ecb1bd5a57be2888dfcbfb7323719329`) and require `0.0.1`, which
  is on crates.io, so this crate can be published there.

## [0.0.1] - 2026-10-09

### Changed (BREAKING)

- **`~/.decdn/sponsor.toml` names only the onramp.** It holds `onramp_url`
  (was `gateway_base`), `decdn_bin` and an optional `data_dir`; the chain and
  contracts come from the onramp's `/v1/profile` on every run, falling back to
  the profile a resumed download saved. Re-run the installer to rewrite it.
- **Needs an onramp with `/v1`.** It polls `GET /v1/capability`.

### Added

- **`--version`**, and a refusal with an upgrade hint below the onramp's
  `min_cli_version`.
- **Released with every sponsord crate.** Its archives ship in each `vX.Y.Z`
  release; pin one in the onramp with that tag (`ONRAMP_CLI_RELEASE`).
  Archive names are unchanged:
  `decdn-sponsored-<version>-<target>.{tar.gz,zip}`. Also published to
  crates.io, dual-licensed MIT OR Apache-2.0.

### Changed

- **MSRV is Rust 1.99.0** (was 1.95.0), matching decdn.
- **One shared `decdn` data dir.** `decdn bundle pull` gets
  `--data-dir <data_dir>/decdn/`, kept across downloads, so its peer cache
  carries over. The voucher key stays per download under
  `downloads/<hash>/`. A download started while another holds the shared dir
  (`<data_dir>/decdn.lock`) runs in its own directory.
- **No redb, no geth-compat keystores.** Each download key's address is
  written beside it instead of read out of the keystore.
- **Requests carry a timeout and a `User-Agent`** (`decdn-sponsored/<version>`).
- **decdn from GitHub.** The decdn crates are git dependencies on
  `github.com/decdn/decdn`, at the commit `Cargo.lock` pins
  (`54deb333933633121e88fd480e9377b7cd788f30`); `decdn.ref` and the sibling
  checkout are gone.
- **HTTPS through rustls.** reqwest 0.13 replaces OpenSSL (native-tls)
  with rustls and checks certificates with the platform verifier, so the
  binary no longer links `libssl` on Linux.
