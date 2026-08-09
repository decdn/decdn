# Security

## Reporting a vulnerability

Email `security@decdn.org`. Please do not open a public issue for security
reports.

## Release signing

Releases are cut and signed by a maintainer, on their own machine, with their
own GPG key. No signing key exists in CI. Every key trusted to sign a release
is committed to this repository as [`KEYS`](KEYS):

```
Ant Somers <ant@decdn.org>
Fingerprint: DA75 1570 6F18 73D2 74D8  A369 9E11 A9FF D62D AADB
```

A good signature from **any** key listed above is authentic. Adding or removing
a maintainer is a change to `KEYS`, visible in this repository's history.

Each release carries five signatures: the tag, the version-bump commit, the
`SHA256SUMS` manifest covering every archive, `image-digest.txt` naming the
container image, and the SBOM. Individual archives carry no `.asc` of their
own — verify them through the signed manifest.

### Verify a release tag

```bash
# Import the maintainer keys (one time)
gpg --import KEYS

# Fetch tags and verify the one you're installing
git fetch --tags
git verify-tag v0.1.0
git verify-commit v0.1.0^{commit}
```

`git verify-tag` must report a **Good signature** from one of the keys above. A
missing or bad signature means the tag was not produced by the project — do not
trust it. (GPG will also warn that the key is not certified with a trusted
signature; that is expected, and is why the fingerprints are published here.)

### Verify a binary archive

Verify the signed checksum manifest, then check your archive against it:

```bash
gpg --verify SHA256SUMS.asc SHA256SUMS
sha256sum --ignore-missing --check SHA256SUMS
```

### Verify the container image

GPG has no native OCI-image signature, so the release publishes the pinned
image digest and a detached signature over it. Confirm the signature, then pull
by that exact digest:

```bash
gpg --verify image-digest.txt.asc image-digest.txt
docker pull "$(cat image-digest.txt)"
```

Every tag you can pull — `latest`, `<version>` and `<major>.<minor>` — is
created only after that digest has been signed, so an unpinned
`docker pull ghcr.io/decdn/decdn` also resolves to a signed image. Pulling by
digest is still stronger: it pins the exact bytes you verified.

CI stages each build under a `staging-v<version>` tag before it is signed.
Those tags are build scratch space, are not part of any release, and should
never be pulled.

The SBOM (`decdn-<version>-sbom.spdx.json`) ships with a matching `.asc`.

> Container-image signing here is a GPG-over-digest attestation. A future
> release will add in-registry signatures (cosign); this section will change
> when that lands.

### What a signature does and does not tell you

CI builds the release archives and the container image on GitHub runners; the
maintainer verifies the published checksums, then signs them. The signature
means a named maintainer vouches that these are the release artifacts. It is
not a reproducible-build attestation, and it does not prove the binaries were
compiled from the tagged source. To verify that yourself, build from the signed
tag.
