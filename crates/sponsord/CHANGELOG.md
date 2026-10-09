# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate, under
     [Unreleased]. `cargo release` only opens the version heading above those
     entries; git-cliff generates the GitHub release notes. Conventions:
     RELEASING.md § Changelogs. -->

All notable changes to `sponsord` are documented in this file. It shares
one version with every sponsord crate, released together from `vX.Y.Z`
tags; a release with no entry here changed nothing in this crate.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Changed

- **decdn v0.0.1.** The decdn crates are locked at decdn's `v0.0.1` release
  tag (`454e5137ecb1bd5a57be2888dfcbfb7323719329`) and require `0.0.1`, which
  is on crates.io, so this crate can be published there.

## [0.0.1] - 2026-10-09

### Changed (BREAKING)

- **Flags as well as environment variables.** Every `SPONSORD_*` setting is
  also a flag (`sponsord --help`), with its default in one place. Zero limits
  and a zero watch interval are refused.

### Added

- **Secrets from files.** `SPONSORD_API_TOKEN_FILE` and
  `SPONSORD_TREASURY_PASSWORD_FILE`, as alternatives to the inline values.
- **`GET /metrics`.** Prometheus text, no token: capabilities issued, request
  errors by code, and the keeper's view of the pool, including
  `sponsord_pool_topup_unconfirmed_since_unix` while a top-up is held.
- **Graceful shutdown.** SIGTERM and Ctrl-C stop the server and the keeper; a
  top-up in flight finishes.
- **`--version`.**
- **Container image.** `ghcr.io/decdn/sponsord` (mirrored to
  `decdn/sponsord` on Docker Hub), amd64 and arm64, built from the release
  binaries; binds `0.0.0.0:8090`, runs as uid 1000.
- **Release metadata.** Released with every sponsord crate under one version
  (`vX.Y.Z`), with crates.io metadata, dual-licensed MIT OR Apache-2.0.

### Changed

- **MSRV is Rust 1.99.0** (was 1.95.0), matching decdn.
- **decdn from GitHub.** The decdn crates are git dependencies on
  `github.com/decdn/decdn`, at the commit `Cargo.lock` pins
  (`54deb333933633121e88fd480e9377b7cd788f30`); `decdn.ref` and the sibling
  checkout are gone.
- **HTTPS through rustls.** reqwest 0.13 replaces OpenSSL (native-tls)
  with rustls and checks certificates with the platform verifier, so the
  binary no longer links `libssl` on Linux.

### Fixed

- **No second top-up while one may still mine.** A pool top-up whose receipt
  could not be read, or whose submit failed in transport, now holds further
  top-ups until it mines or can no longer mine, instead of being retried at
  the next tick, which could refill the pool twice. A dropped one is cleared with self-transfers from the
  treasury, not a restart. See `docs/operator.md` §5
  ([#40](https://github.com/decdn/sponsord/issues/40)).
- **Top-ups after a dropped or outside transaction.** The treasury's nonce
  is read from the node for each transaction, so a dropped transaction or
  one sent from the treasury elsewhere no longer stalls top-ups until a
  restart.
- **Keystore credential in the systemd unit.**
  `deploy/systemd/sponsord.service` copies the keystore credential to a `0600`
  file under `/run/sponsord` before start. Some systemd versions (254+ on
  Linux 6.4+, seen on 255) load credentials at `0440`, which the daemon
  refused as an insecure keystore
  ([#36](https://github.com/decdn/sponsord/issues/36)). If you installed a copy
  of the unit, take its new `RuntimeDirectory=`, `RuntimeDirectoryMode=`,
  `ExecStartPre=` and `SPONSORD_TREASURY_KEYSTORE` lines.

### Security

- **No RPC URL in logs, the exit error or `--help`.** `SPONSORD_RPC_URL`
  often carries a provider API key. A failed chain call no longer prints it
  in the exit error or the daemon's warnings (issue and keeper), which show
  the failure class (connection refused, timeout, DNS, TLS) instead.
  `RUST_LOG=debug` no longer prints it either: `alloy_transport_http`, whose
  request span records it, is held at `info`. `--help` hides its value
  ([#38](https://github.com/decdn/sponsord/issues/38)).
