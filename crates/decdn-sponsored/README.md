# decdn-sponsored

Download from deCDN, paid for by a sponsor. `decdn-sponsored pull <hash>`
gives each download a throwaway key, gets a capability for it through the
sponsor's onramp (one check in the browser per download), and hands the pull to the
[`decdn`](https://github.com/decdn/decdn) CLI. There is no wallet, key or
password to manage.

It reads `~/.decdn/sponsor.toml`, which the onramp's installer writes (or
the environment, see [Run in a container](#run-in-a-container)), and the
chain and contracts from the onramp itself. The usual way to install it is
that installer, which also installs `decdn` and runs the pull:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- pull b3:<hash> --namespace <id>
```

## Run in a container

`ghcr.io/decdn/decdn-sponsored` (also `decdn/decdn-sponsored` on Docker Hub;
amd64 and arm64) carries the CLI and the `decdn` it spawns. With no installer
to write `~/.decdn/sponsor.toml`, it is configured by environment:

| Variable | Meaning | Default |
|---|---|---|
| `DECDN_SPONSOR_ONRAMP_URL` | The onramp's base URL. Set, the file is not read at all | — (required) |
| `DECDN_SPONSOR_DECDN_BIN` | The `decdn` binary, a path or a name on `PATH` | `decdn` |
| `DECDN_SPONSOR_DATA_DIR` | Root for per-download state | `~/.decdn/sponsored` |

The other two are read only when `DECDN_SPONSOR_ONRAMP_URL` is set. An empty
value counts as unset.

```bash
docker run --rm -it -v "$PWD:/out" \
  -e DECDN_SPONSOR_ONRAMP_URL=https://up.decdn.org \
  ghcr.io/decdn/decdn-sponsored pull b3:<hash> -o /out --namespace <id>
```

`-it` is for a terminal; drop it where there is none (`-t` fails without
one). No browser opens inside a container: open the printed link yourself.
The container runs as uid 1000, which needs write access to the output
directory. A download's state lives under `/home/sponsord/.decdn` (or
`DECDN_SPONSOR_DATA_DIR`); mount a volume there
(`-v decdn-sponsored:/home/sponsord/.decdn`) to resume an interrupted download
or keep the peer cache between runs.

The same variables work outside a container.

Licensed under either of MIT or Apache-2.0, at your option.
