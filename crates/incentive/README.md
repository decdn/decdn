# decdn-incentive

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The payment layer: off-chain USDC payment channels and cumulative vouchers, capacity bonding
and staking, and the signature schemes binding them together. Talks to the chain through
[`alloy`](https://docs.rs/alloy).

deCDN is dual-currency — TOKEN for staking and governance, USDC for payments. Every byte
transfer is paid, including node-to-node cache-miss pulls.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
