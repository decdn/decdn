# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate. `cargo
     release` does NOT rewrite this file; git-cliff only generates the GitHub
     release notes. Conventions: RELEASING.md § Changelogs. -->

All notable changes to `sponsord-core` are documented in this file. It is
versioned and released on its own, from `sponsord-core-vX.Y.Z` tags.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

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

### Changed

- **decdn from GitHub.** The decdn crates are git dependencies on
  `github.com/decdn/decdn`, at the commit `Cargo.lock` pins
  (`7f938e6b8437eba614e54375dcbc504de108379d`); `decdn.ref` and the sibling
  checkout are gone.

### Added

- **Published as its own crate.** Versioned and released independently of the
  binaries, with crates.io metadata, dual-licensed MIT OR Apache-2.0.

### Security

- **No RPC URL in errors.** A malformed RPC URL is reported by length, not
  echoed, since it often carries an API key. A failed chain call no longer
  names it either: the URL reqwest puts in its errors is stripped from every
  error `ChainPool` returns and from the keeper's warnings, which now carry
  the cause (connection refused, timeout, DNS, TLS) as well. `top_up` errors
  still downcast to decdn's `TopUpUnconfirmed` and `AllowanceShortfall`.
  `ChainConfig`'s `Debug` shows the URL's length
  ([#38](https://github.com/decdn/sponsord/issues/38)).
