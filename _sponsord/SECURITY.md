# Security

## Reporting a vulnerability

Email `security@decdn.org`. Please do not open a public issue for security
reports.

`sponsord` holds the key that owns a funded `PaymentPool`, so reports about
any of the following are especially welcome:

- a way to get a capability from `sponsord` without its bearer token, or from
  `sponsord-onramp` without passing its gate;
- a way to get a capability with terms above the daemon's maximum;
- anything that leaks the treasury key, its password, or the API token;
- installer behaviour that runs or installs something other than the pinned,
  checksum-verified release binaries.

Only the latest release of each crate receives fixes. The operator-facing
security model (what each key can do, what bounds a loss) is in
[docs/security.md](docs/security.md).

## Release signing

Releases are cut and signed by a maintainer, on their own machine, with their
own GPG key. No signing key exists in CI. Every key trusted to sign a release
is committed to this repository as [`KEYS`](KEYS) — the same keys that sign
decdn's releases:

```
Ant Somers <ant@decdn.org>
Fingerprint: DA75 1570 6F18 73D2 74D8  A369 9E11 A9FF D62D AADB

Alper Gundogdu <alper@decdn.org>
Fingerprint: E27B 9A2D 2519 1E8E C90B  F8AE 57E2 823C 16CC D376
```

A good signature from **any** key listed above is authentic. Adding or removing
a maintainer is a change to `KEYS`, visible in this repository's history.

**Pin the fingerprint, not the file.** The signature protects you because you
check it against a fingerprint you already trust, not because of anything the
release pipeline enforces. CI does verify that a release tag is signed by a key
published on `main`, but that check lives in the repository it is checking.
Record the fingerprint above out of band the first time you verify a release.

Each crate is released on its own, from a `<crate>-vX.Y.Z` tag
(`decdn-sponsored-v0.1.0`, `sponsord-v0.1.0`, …). A release carries these
signatures:

- the tag, always;
- the version-bump commit, for every release after a crate's first (which tags
  the version the crate already carried);
- the `SHA256SUMS` manifest covering every archive, for `decdn-sponsored`,
  `sponsord` and `sponsord-onramp`;
- `<image>-image-digest.txt` and the SBOM, for the servers' images
  (`sponsord`, `sponsord-onramp`).

`sponsord-core` and `sponsord-api` ship no binaries; their signed tags are the
whole attestation.
Individual archives carry no `.asc` of their own — verify them through the
signed manifest.

### Verify a release tag

```bash
# Import the maintainer keys (one time)
gpg --import KEYS

# Fetch tags and verify the one you're installing
git fetch --tags
git verify-tag decdn-sponsored-v0.1.1
git verify-commit decdn-sponsored-v0.1.1^{commit}   # not for a crate's first release
```

`git verify-tag` must report a **Good signature** from one of the keys above.
(GPG will also warn that the key is not certified with a trusted signature;
that is expected, and is why the fingerprints are published here.)

### Verify a binary archive

```bash
gpg --verify SHA256SUMS.asc SHA256SUMS
sha256sum --ignore-missing --check SHA256SUMS
```

The onramp's installers pin a release by the SHA-256 of this same
`SHA256SUMS` file (`ONRAMP_CLI_SUMS_SHA256`) and check each download
against it.

### Verify a container image

The release publishes each image's pinned digest and a detached signature over
it. Confirm the signature, then pull by that exact digest:

```bash
gpg --verify sponsord-image-digest.txt.asc sponsord-image-digest.txt
docker pull "$(cat sponsord-image-digest.txt)"
```

(and likewise for `sponsord-onramp-image-digest.txt`).

The images are published to `ghcr.io/decdn/<image>` and mirrored to
`decdn/<image>` on Docker Hub. The digest file records the GHCR reference, but
the mirror is a verbatim manifest copy, so both registries serve the *same*
digest and one signature covers both:

```bash
DIGEST="$(cut -d@ -f2 sponsord-image-digest.txt)"
docker pull "ghcr.io/decdn/sponsord@${DIGEST}"
docker pull "decdn/sponsord@${DIGEST}"        # identical bytes
```

An image's version tags are its server crate's version (`sponsord-v0.2.1` is
`ghcr.io/decdn/sponsord:0.2.1`). Every tag you can pull — `latest`,
`<version>` and `<major>.<minor>`, on either registry — is created only after
that digest has been signed. Pulling by digest is still stronger: it pins the
exact bytes you verified. A prerelease is published only as its exact version
tag.

### What a signature does and does not tell you

CI builds the release archives on GitHub runners; a maintainer verifies the
published checksums, then signs them. The signature means a named maintainer
vouches that these are the release artifacts. It is not a reproducible-build
attestation. To verify the binaries against the source, build from the signed
tag with `cargo build --release --locked`, which fetches the decdn commit its
`Cargo.lock` pins.

Each image is assembled from the same archives rather than compiled
separately, so its binary is byte-identical to the archived one:

```bash
docker run --rm --entrypoint sha256sum ghcr.io/decdn/sponsord:<version> \
  /usr/local/bin/sponsord
tar xzOf sponsord-<version>-x86_64-unknown-linux-gnu.tar.gz | sha256sum
```

### Crates published to crates.io

`sponsord-api`, `sponsord-core`, `sponsord`, `sponsord-onramp` and
`decdn-sponsored` are also published to crates.io. **Those artifacts carry no maintainer signature** —
crates.io has no detached-signature mechanism, and the `.crate` files are built
and uploaded from a maintainer's machine after the GitHub Release is signed.
For the guarantees above, use the GitHub Release.

To tie a `.crate` back to the signed tag, read the commit out of the
`.cargo_vcs_info.json` cargo embeds and check it is the one the tag points at:

```bash
tar xzOf sponsord-<version>.crate sponsord-<version>/.cargo_vcs_info.json
git rev-parse sponsord-v<version>^{commit}
```
