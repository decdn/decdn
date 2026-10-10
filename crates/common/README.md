# decdn-common

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain shared payment pools.

> **Status: early implementation.** deCDN runs a public testnet on Arbitrum Sepolia and is pre-mainnet. Wire formats, APIs and on-chain interfaces change without compatibility shims until mainnet.

Types shared by the two deCDN binaries (`decdn-node` and `decdn`): the config schema and its
resolver, the shared clap argument definitions both binaries build their command lines from,
node identity loading, and the local admin JSON-RPC trait plus its request/response DTOs.

Split out so the daemon and the CLI agree on config and admin wire shapes by construction
rather than by convention — see the dockerd-style two-binary rationale in the repo's
`adr/appendix-binaries.md`.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
