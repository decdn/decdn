# Security

## Reporting a vulnerability

Email `security@decdn.org`. Please do not open a public issue for security
reports.

## Release signing

Release tags and the version-bump commit for each release are GPG-signed by the
organization release key:

```
decdn <security@decdn.org>
Fingerprint: D015 6386 7CEE 34A3 6C44  736B 9BC3 D940 1928 D2B9
```

The private key lives only in the release pipeline's Actions secrets. It is not
any individual maintainer's personal key. The public key is committed to this
repository as [`KEYS`](KEYS).

### Verify a release tag

```bash
# Import the org public key (one time)
gpg --import KEYS

# Fetch tags and verify the one you're installing
git fetch --tags
git verify-tag v0.1.0
git verify-commit v0.1.0^{commit}
```

`git verify-tag` must report a **Good signature** from
`decdn <security@decdn.org>` matching the fingerprint above. A missing or bad
signature means the tag was not produced by the project — do not trust it.

### Verify a binary archive

Every release archive ships with a detached `.asc` signature, plus a signed
`SHA256SUMS` covering all archives:

```bash
# Verify one archive directly
gpg --verify decdn-0.1.0-x86_64-unknown-linux-gnu.tar.gz.asc \
             decdn-0.1.0-x86_64-unknown-linux-gnu.tar.gz

# Or verify the checksum manifest, then check the file against it
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

The SBOM (`decdn-<version>-sbom.spdx.json`) ships with a matching `.asc`.

> Container-image signing here is a GPG-over-digest attestation. A future
> release will add in-registry signatures (cosign); this section will change
> when that lands.
