# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate. `cargo
     release` does NOT rewrite this file; git-cliff only generates the GitHub
     release notes. Conventions: RELEASING.md § Changelogs. -->

All notable changes to `sponsord` are documented in this file. It is
versioned and released on its own, from `sponsord-vX.Y.Z` tags.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Changed (BREAKING)

- **Flags as well as environment variables.** Every `SPONSORD_*` setting is
  also a flag (`sponsord --help`), with its default in one place. Zero limits
  and a zero watch interval are refused.

### Added

- **Secrets from files.** `SPONSORD_API_TOKEN_FILE` and
  `SPONSORD_TREASURY_PASSWORD_FILE`, as alternatives to the inline values.
- **`GET /metrics`.** Prometheus text, no token: capabilities issued, request
  errors by code, and the keeper's view of the pool.
- **Graceful shutdown.** SIGTERM and Ctrl-C stop the server and the keeper; a
  top-up in flight finishes.
- **`--version`.**
- **Container image.** `ghcr.io/decdn/sponsord` (mirrored to
  `decdn/sponsord` on Docker Hub), amd64 and arm64, built from the release
  binaries; binds `0.0.0.0:8090`, runs as uid 1000.
- **Release metadata.** Versioned and released on its own (`sponsord-vX.Y.Z`),
  with crates.io metadata, dual-licensed MIT OR Apache-2.0.

### Changed

- **decdn from GitHub.** The decdn crates are git dependencies on
  `github.com/decdn/decdn`, at the commit `Cargo.lock` pins
  (`7f938e6b8437eba614e54375dcbc504de108379d`); `decdn.ref` and the sibling
  checkout are gone.

### Fixed

- **Keystore credential in the systemd unit.**
  `deploy/systemd/sponsord.service` copies the keystore credential to a `0600`
  file under `/run/sponsord` before start. Some systemd versions (254+ on
  Linux 6.4+, seen on 255) load credentials at `0440`, which the daemon
  refused as an insecure keystore
  ([#36](https://github.com/decdn/sponsord/issues/36)). If you installed a copy
  of the unit, take its new `RuntimeDirectory=`, `RuntimeDirectoryMode=`,
  `ExecStartPre=` and `SPONSORD_TREASURY_KEYSTORE` lines.
