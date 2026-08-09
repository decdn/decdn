# decdn-reputation

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

Local per-peer reputation scoring (ADR 008): an exponentially weighted moving average over
observed probe and delivery outcomes, used to rank candidate peers.

Scores are strictly local. There is no gossip aggregation and no shared reputation state — a
node's view is built only from what it has itself observed.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
