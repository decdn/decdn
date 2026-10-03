# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `sponsord-api`: the wire types, error codes and route paths of the daemon
  and the onramp, typed clients (`DaemonClient`, `OnrampClient`), and OpenAPI
  documents for both servers in `docs/openapi/`.
- `sponsord-core`: `Sponsor::keeper_status()`, what the pool keeper has seen
  and done.

### Changed

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
