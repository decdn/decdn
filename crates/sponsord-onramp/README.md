# sponsord-onramp

The public onramp in front of [`sponsord`](https://crates.io/crates/sponsord):
a gate page (Turnstile built in, or your own `Gate`), the `decdn.sh` and
`decdn.ps1` installers, and the API the `decdn-sponsored` CLI polls. Every
setting is a flag or an `ONRAMP_*` environment variable
(`sponsord-onramp --help`).

Prebuilt Linux binaries, signed, are on the
[GitHub Releases](https://github.com/decdn/sponsord/releases), and a container
image is published as `ghcr.io/decdn/sponsord-onramp`. See the
[operator guide](https://github.com/decdn/sponsord/blob/main/docs/operator.md)
for configuration.

Licensed under either of MIT or Apache-2.0, at your option.
