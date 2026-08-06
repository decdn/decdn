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

> Binary archives and the GHCR container image are not yet individually signed;
> that is tracked as a follow-up. For now, verify the signed tag and build/pull
> from that verified revision.
