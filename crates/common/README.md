# decdn-common

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

Types shared by the two deCDN binaries (`decdn-node` and `decdn`): the config schema and its
resolver, node identity loading, and the local admin JSON-RPC trait plus its request/response
DTOs.

Split out so the daemon and the CLI agree on config and admin wire shapes by construction
rather than by convention — see the dockerd-style two-binary rationale in the repo's
`adr/appendix-binaries.md`.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
