# decdn-sponsored

Download from deCDN, paid for by a sponsor. `decdn-sponsored pull <hash>`
gives each download a throwaway key, gets a capability for it through the
sponsor's onramp (one check in the browser per download), and hands the pull to the
[`decdn`](https://github.com/decdn/decdn) CLI. There is no wallet, key or
password to manage.

It reads `~/.decdn/sponsor.toml`, which the onramp's installer writes, and
the chain and contracts from the onramp itself. The usual way to install it is
that installer, which also installs `decdn` and runs the pull:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- pull b3:<hash> --namespace <id>
```

Licensed under either of MIT or Apache-2.0, at your option.
