<!-- description: deCDN sponsored-download onramp: serves the decdn.sh / decdn.ps1 installers behind a gate. -->
# sponsord-onramp

The public onramp in front of the [deCDN](https://github.com/decdn/decdn)
[sponsorship daemon](https://hub.docker.com/r/decdn/sponsord). It serves the
`decdn.sh` and `decdn.ps1` installers and hands each user a sponsored download
capability once they pass its gate (Cloudflare Turnstile by default), so they download
without a wallet of their own.

The network is a public testnet on Arbitrum Sepolia. Wire, config and storage formats
change without compatibility shims until the mainnet deployment.

## Quick start

The onramp is stateless and configured by `ONRAMP_*` environment variables, with its
secrets in mounted files. The
[operator guide](https://github.com/decdn/decdn/blob/main/docs/sponsord/operator.md)
walks through them, and
[`deploy/sponsord/compose.yaml`](https://github.com/decdn/decdn/blob/main/deploy/sponsord/compose.yaml)
runs the daemon and the onramp together:

```bash
docker pull decdn/sponsord-onramp
docker run --rm decdn/sponsord-onramp --help
```

The image listens on `8080` (`/healthz`, `/decdn.sh`, `/decdn.ps1`, `/fund`, `/v1/fund`,
`/v1/capability`) and runs as uid 1000. Put a TLS reverse proxy in front of it and set
`ONRAMP_PUBLIC_URL` to the address users reach.

## Tags

| Tag | Points at |
|-----|-----------|
| `latest` | the newest stable release |
| `<version>`, e.g. `0.2.1` | that release |
| `<major>.<minor>`, e.g. `0.2` | the newest stable release in that minor line |

A prerelease (for example `0.3.0-rc1`) gets only its exact version tag. The version is
the deCDN release the onramp shipped with.

This image is built for linux/amd64 and linux/arm64. It is also on GitHub Container
Registry as
[`ghcr.io/decdn/sponsord-onramp`](https://github.com/orgs/decdn/packages/container/package/sponsord-onramp),
with the same digest.

## Verify the image

A maintainer signs each release's image digest with GPG, and every tag is created only
after that signature. To pin the exact bytes, import the maintainer keys from
[`KEYS`](https://github.com/decdn/decdn/blob/main/KEYS) (`gpg --import KEYS`). Download
`sponsord-onramp-image-digest.txt` and its `.asc` from the
[release](https://github.com/decdn/decdn/releases/latest), then run:

```bash
gpg --verify sponsord-onramp-image-digest.txt.asc sponsord-onramp-image-digest.txt
docker pull "decdn/sponsord-onramp@$(cut -d@ -f2 sponsord-onramp-image-digest.txt)"
```

[SECURITY.md](https://github.com/decdn/decdn/blob/main/SECURITY.md#verify-the-container-images)
has the key fingerprints to check against and the full steps.

## Related images

- [`decdn/sponsord`](https://hub.docker.com/r/decdn/sponsord): the daemon this onramp
  fronts.
- [`decdn/decdn`](https://hub.docker.com/r/decdn/decdn): the CLI the installers install.

## Links

- [Source and README](https://github.com/decdn/decdn/blob/main/docs/sponsord/README.md)
- [Documentation](https://docs.decdn.org)
- [Releases](https://github.com/decdn/decdn/releases)
- License: MIT OR Apache-2.0
