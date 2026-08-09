# decdn-cli

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The user-facing `decdn` binary: `probe`, `node` administration, `key-gen`, `config` and
`bundle` subcommands.

```sh
cargo install decdn-cli
decdn config init --chain arbitrum-sepolia
```

Deliberately links no blob store and no AWS SDK — publishing and verifying content goes
through the leaf crates (`decdn-protocol`, `decdn-config-types`, `decdn-bao-range`) only.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
