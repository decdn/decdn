# decdn-bao-range

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

[`bao-tree`](https://docs.rs/bao-tree)-based verified-range helpers: chunk-group alignment,
encoded-size arithmetic, and range encode/verify against an untrusted `{hash}.obao4` pre-order
outboard.

Deliberately free of `iroh-blobs`. That is what lets the publisher CLI request and verify byte
ranges without linking a blob store — see ADR 038 for the range-transfer design.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
