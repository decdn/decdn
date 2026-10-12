# decdn-bao-range

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC with off-chain vouchers backed by a shared on-chain payment pool.

> **Status: early implementation.** deCDN runs a public testnet on Arbitrum Sepolia and is pre-mainnet. Wire formats, APIs and on-chain interfaces change without compatibility shims until mainnet.

Bao verified-range helpers, built on [`bao-tree`](https://docs.rs/bao-tree): align a byte
range to bao chunk groups, encode a verified range for the wire, and size that encoding. An
origin range verifies against an untrusted `{hash}.obao4` pre-order outboard.

Deliberately free of `iroh-blobs`. That is what lets the client SDK and the `decdn` CLI request
and verify byte ranges without linking a blob store. See
[ADR 038](https://github.com/decdn/decdn/blob/main/adr/038-bao-verified-range-streaming.md) for
the range-transfer design.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
