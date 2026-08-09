# decdn-config-types

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The config *vocabulary* — the value types a deCDN node's configuration is written in
(`RetryPolicy`, `DecompressMode`, `OriginUrl`, `OriginKind`, `Hash`, `PinnedHashes`) — factored
out so that `decdn-cache` and `decdn-common` can share them without depending on each other.

A leaf crate: `serde` and `url`, nothing else. No `iroh-blobs`, no AWS SDK.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
