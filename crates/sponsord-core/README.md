# sponsord-core

The logic of [`sponsord`](https://crates.io/crates/sponsord), deCDN's
sponsorship daemon, as a library: signing capped, expiring capabilities
(`dcap1:` tokens) against a `PaymentPool`, and keeping that pool topped up from
a treasury. For Rust programs that embed the daemon's behaviour instead of
calling it over HTTP.

See the [sponsord repository](https://github.com/decdn/sponsord) for the
architecture, and [decdn](https://github.com/decdn/decdn) for the protocol.

Licensed under either of MIT or Apache-2.0, at your option.
