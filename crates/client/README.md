# decdn-client

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The reusable `cdn/client/v1` paid-pull requester and the buyer-side channel open: signs the
request, verifies the signed `StreamResponse`, pays a cumulative voucher at each interval, and
assembles the blob.

Shared by both sides of the protocol, because a node serving a cache miss is itself a paying
client on its upstream leg — `decdn-node` depends on this crate and re-exports it.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
