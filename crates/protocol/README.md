# decdn-protocol

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

Shared wire types and ALPN definitions: the postcard-encoded messages exchanged over
`cdn/probe/v1` (latency and availability probing), `cdn/client/v1` (all paid delivery, both
client-to-node and node-to-node) and `cdn/dht/v1` (content discovery).

This is a leaf crate with deliberately minimal dependencies, so anything that only needs to
speak the protocol does not pull in a blob store, an Ethereum client or an AWS SDK. Its
exported surface is snapshot-tested in CI.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
