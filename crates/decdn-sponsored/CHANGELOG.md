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
  (`a09720adcb055ef205cf0e0d83339c2e7673c498`); `decdn.ref` and the sibling
  checkout are gone.
- **HTTPS through rustls.** reqwest 0.13 replaces OpenSSL (native-tls)
  with rustls and checks certificates with the platform verifier, so the
  binary no longer links `libssl` on Linux.
