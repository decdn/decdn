# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate, under
     [Unreleased]. `cargo release` only opens the version heading above those
     entries; git-cliff generates the GitHub release notes. Conventions:
     RELEASING.md § Changelogs. -->

All notable changes to `sponsord-core` are documented in this file. It shares
one version with every sponsord crate, released together from `vX.Y.Z`
tags; a release with no entry here changed nothing in this crate.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Changed

- **decdn v0.0.2.** The decdn crates are locked at decdn's `v0.0.2` release tag
  (`a9f8af9c655666f3a03efe073035139e6f4bdbfa`) and require `0.0.2`, which is on
  crates.io.

## [0.0.2] - 2026-10-09

### Changed

- **decdn v0.0.1.** The decdn crates are locked at decdn's `v0.0.1` release
  tag (`454e5137ecb1bd5a57be2888dfcbfb7323719329`) and require `0.0.1`, which
  is on crates.io, so this crate can be published there.

## [0.0.1] - 2026-10-09

### Changed (BREAKING)

- **Any hash-signing alloy signer.** `Sponsor::connect(signer, ChainConfig,
  Limits)` takes a cloneable signer that signs raw hashes and transactions
  (local key, AWS/GCP KMS) instead of a keystore path and password;
  `SponsorConfig` is gone. Remote signers' high-s signatures are normalized.
  Hardware wallets are not supported yet (#26).
- **Typed terms and money.** `TermsRequest` / `Terms` / `Limits` replace
  tuples and bare `u64`s, and `MicroUsdc` (now from `sponsord-api`) is used
  for all money, including `Authorization`. `issue` and `info` return the
  `sponsord-api` types.
- **Renames.** `Treasury` → `PoolChain`, `DecdnTreasury` → `ChainPool`
  (`ChainPool::connect`), `pool_watch` → `keeper`, `FakeTreasury` → `FakePool`.
- **Keeper.** `Sponsor::keeper(KeeperConfig, CancellationToken)` runs until
  cancelled; `Sponsor::keeper_status()` reports what it has seen and done.
- **`PoolChain` reads transactions and nonces.** Implementors add
  `transaction(TxHash) -> pool::TxState` and `confirmed_nonce()`, which the
  keeper uses to settle an unconfirmed top-up.
  `KeeperSnapshot` gains `topup_unconfirmed_since_unix`.

### Changed

- **MSRV is Rust 1.99.0** (was 1.95.0), matching decdn.
- **decdn from GitHub.** The decdn crates are git dependencies on
  `github.com/decdn/decdn`, at the commit `Cargo.lock` pins
  (`54deb333933633121e88fd480e9377b7cd788f30`); `decdn.ref` and the sibling
  checkout are gone.

### Added

- **Published as its own crate.** Released with every sponsord crate under one
  version (`vX.Y.Z`), with crates.io metadata, dual-licensed MIT OR
  Apache-2.0.

### Fixed

- **No second top-up while one may still mine.** When a `topUp` may have
  been broadcast but is not known to have mined (decdn's `TopUpUnconfirmed`:
  its receipt could not be read, or its submit failed in transport), the
  keeper used to send another one at the next tick, so both could mine and
  the treasury paid two refills. It now holds further top-ups until that
  transaction has a receipt, or the treasury's confirmed nonce has passed the
  nonce decdn reports it was sent with. It logs the hash, when there is one,
  and the nonce at `error`. A dropped top-up is cleared by sending 0-value
  self-transfers from the treasury
  ([decdn/decdn#2319](https://github.com/decdn/decdn/issues/2319),
  [#40](https://github.com/decdn/sponsord/issues/40)).
- **Nonces from the node.** `ChainPool` asks the node for each
  transaction's nonce instead of using alloy's cached nonce, which never
  re-syncs: a dropped transaction left every later top-up queued behind its
  nonce until a restart, and a transaction sent from the treasury elsewhere
  made the next one reuse a spent nonce.

### Security

- **No RPC URL in errors.** A malformed RPC URL is reported by length, not
  echoed, since it often carries an API key. A failed chain call no longer
  names it either: the URL reqwest puts in its errors is stripped from every
  error `ChainPool` returns and from the keeper's warnings, which now carry
  the cause (connection refused, timeout, DNS, TLS) as well. `top_up` errors
  still downcast to decdn's `TopUpUnconfirmed` and `AllowanceShortfall`.
  `ChainConfig`'s `Debug` shows the URL's length
  ([#38](https://github.com/decdn/sponsord/issues/38)).
