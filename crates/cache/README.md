# decdn-cache

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The node's cache engine: a store wrapping [`iroh-blobs`](https://docs.rs/iroh-blobs) with
origin pull-through, range admission, partial-blob retention across GC, and prewarm.

Independent of the payment layer by design — the cache works with no payment logic wired in,
which keeps it usable for local development and testing. The paid fetch path lives in
`decdn-client-pull` instead.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
