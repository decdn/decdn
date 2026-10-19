# sponsord

deCDN's sponsorship daemon. It holds the owner key of one `PaymentPool`, signs
capped, expiring capabilities (`dcap1:` tokens) for trusted callers over a
small HTTP API, and keeps the pool funded from the treasury. Every setting is a
flag or a `SPONSORD_*` environment variable (`sponsord --help`).

Prebuilt Linux binaries, signed, are on the
[GitHub Releases](https://github.com/decdn/sponsord/releases), and a container
image is published as `ghcr.io/decdn/sponsord`. See the
[operator guide](https://github.com/decdn/sponsord/blob/main/docs/operator.md)
for configuration and the
[integrator guide](https://github.com/decdn/sponsord/blob/main/docs/integrator.md)
for the HTTP API.

Licensed under either of MIT or Apache-2.0, at your option.
