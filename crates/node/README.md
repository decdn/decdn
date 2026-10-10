# decdn-node

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC with off-chain vouchers backed by a shared on-chain payment pool.

> **Status: early implementation.** deCDN runs a public testnet on Arbitrum Sepolia and is pre-mainnet. Wire formats, APIs and on-chain interfaces change without compatibility shims until mainnet.

The `decdn-node` daemon: runtime bring-up, protocol handlers, the local admin JSON-RPC server,
the dispatch limiter, and Prometheus metrics.

```sh
cargo install decdn-node
decdn-node run
```

Container images are published as `ghcr.io/decdn/decdn-node` and `decdn/decdn-node`. The
image also carries the `decdn` CLI, so `docker exec <container> decdn node status` works.
Configure the daemon with `decdn config init` from the `decdn-cli` crate.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
