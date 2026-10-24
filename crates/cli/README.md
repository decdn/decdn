# decdn-cli

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC with off-chain vouchers backed by a shared on-chain payment pool.

> **Status: early implementation.** deCDN runs a public testnet on Arbitrum Sepolia and is pre-mainnet. Wire formats, APIs and on-chain interfaces change without compatibility shims until mainnet.

The user-facing `decdn` binary: `get`, `probe`, `node` administration, `key-gen`,
`config`, `pool`, `publish`, `origin`, `appeal`, `gateway` and `self-update`
subcommands. `gateway` serves `decdn:` URIs over HTTP: to a browser from
loopback, or with `--public-url` to anyone at a public origin, paid from the
gateway's own pool within a daily allowance. `self-update` replaces the binary with the latest release whose
`SHA256SUMS.minisig` verifies against one of the maintainers' minisign keys
embedded from the
[`minisign-*.pub`](https://github.com/decdn/decdn/tree/main/crates/cli) files.

```sh
cargo install decdn-cli
decdn config init --chain arbitrum-sepolia
```

Container images are published as `ghcr.io/decdn/decdn` and `decdn/decdn`, with `decdn` as
the entrypoint.

Deliberately links no blob store and no AWS SDK: the range verification the publish path
needs lives in the `iroh-blobs`-free `decdn-bao-range`, reached through
`decdn-client`.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
