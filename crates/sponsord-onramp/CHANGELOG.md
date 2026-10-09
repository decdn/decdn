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

- **HTTPS through rustls.** reqwest 0.13 replaces OpenSSL (native-tls)
  with rustls and checks certificates with the platform verifier, so the
  binary no longer links `libssl` on Linux.
- **Lighter dependency tree.** The onramp depends on `alloy-primitives`
  instead of all of `alloy`, whose `Address` was the only thing it used. That
  cuts its build from 394 crates to 201.
