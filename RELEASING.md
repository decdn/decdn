# Releasing deCDN

Releases are cut locally and signed with a maintainer's own GPG key. CI builds
the artifacts but signs nothing and publishes nothing — there is no signing key
in Actions secrets.

A release is therefore two commands with a CI build between them:

```bash
cargo release patch --execute            # cut, sign and push the tag
# ...wait for the tag-push run to finish...
.github/scripts/sign-release.sh v0.1.2   # verify, sign, publish
```

## One-time setup

1. **A GPG key in [`KEYS`](KEYS).** Your public key must be merged to `main`
   before the tag you sign will be accepted — CI reads `KEYS` from `origin/main`
   (not from your tag) and runs `git verify-tag` before it builds anything.
   `sign-release.sh` independently refuses to sign with a key that is not in
   `KEYS`, because consumers following [SECURITY.md](SECURITY.md) would reject
   the result.

   ```bash
   gpg --armor --export <your-fingerprint>   # append the block to KEYS
   gpg --fingerprint <your-key-id>           # add the fingerprint to SECURITY.md
   ```

2. **Git configured to sign.** `cargo release` relies on git's own signing
   config; `release.toml` sets `sign-commit` and `sign-tag`. Set this
   explicitly — git otherwise selects a key by matching `user.email`, which may
   not be the key you published.

   ```bash
   git config user.signingkey <your-fingerprint>
   ```

3. **Tools.** `cargo install cargo-release git-cliff`, plus `gh auth login` and
   `docker login ghcr.io` with a token carrying `write:packages`.

## Cutting a version

`cargo release <patch|minor|major> --execute` does all of the following, driven
by [`release.toml`](release.toml):

- bumps every crate in the workspace to the same version (`shared-version`)
- commits as `chore(release): v<version>`, **signed**
- tags `v<version>`, **signed**
- pushes both to `origin`; it does not publish to crates.io (`publish = false`)

Cut from `main`, with a clean tree and CI green on the commit you are tagging.

`CHANGELOG.md` is **not** touched by `cargo release` — it is maintained by hand,
one entry per PR. git-cliff is used only to generate the GitHub release notes,
inside the workflow. Write the changelog entry as part of the work, not at
release time.

To preview a bump without changing anything, drop `--execute`:

```bash
cargo release patch --workspace --no-confirm
```

Dry-run is cargo-release's default — `--execute` is what makes it act — so the
preview writes nothing and needs no cleanup. Do **not** follow it with
`git reset --hard`; that only risks discarding unrelated work.

## What CI does with the tag

Pushing `v*` starts [`.github/workflows/release.yml`](.github/workflows/release.yml),
which:

1. rejects the tag unless it is strict semver (the trigger glob is looser than
   it looks — `v0$(whoami)` matches it);
2. imports `KEYS` **from `origin/main`** and refuses the tag if it is not signed
   by a key published there;
3. checks all twelve crate versions match the tag;
4. re-runs `cargo fmt`, `clippy` and the test suite, plus `cargo semver-checks`
   against the previous tag — skipped for major bumps, for minor bumps while the
   major is `0`, and for the first release (no prior tag to diff against);
5. creates the GitHub Release as a **draft**;
6. builds ten archives (`decdn-node` and `decdn`, five targets each) and a
   `SHA256SUMS` manifest, asserting all ten are present;
7. assembles the multi-arch image from those archives — it does not compile
   from source, so the binary in the image is byte-identical to the archived
   one — and pushes the manifest **untagged**, attaching the SBOM and
   `image-digest.txt`.

Draft assets need authentication to download, and no pullable image tag exists
yet, so nothing a user is meant to consume is reachable until the release is
signed.

## Signing and publishing

Once the run is green:

```bash
.github/scripts/sign-release.sh v0.1.2
```

It resolves your signing key to a fingerprint and confirms it is published in
`KEYS`; force-fetches tags and checks your local tag matches `origin`'s;
verifies the tag signature; downloads the draft's assets; checks `SHA256SUMS`
strictly against them **and** that every published archive appears in the
manifest; signs `SHA256SUMS`, `image-digest.txt` and the SBOM; verifies each
signature against a keyring built only from `KEYS`; uploads the three `.asc`
files; promotes `:latest`, `:<version>` and `:<major>.<minor>` from the signed
digest; and flips the release out of draft.

Useful environment overrides:

| Variable | Effect |
|----------|--------|
| `DECDN_SIGNING_KEY` | key to sign with, when your default key is not the one in `KEYS` |
| `DECDN_SKIP_IMAGE_TAGS` | set to `1` to publish with **no** pullable image tag at all — the release then ships only the signed digest |
| `DECDN_REPO` | target a fork instead of `decdn/decdn` |

For a stable version the promoted tags are `latest`, `<version>` and
`<major>.<minor>`. A prerelease (`v1.2.0-rc1`) gets only its exact version tag:
moving `latest` to a candidate would hand it to every unpinned pull, and
`<major>.<minor>` would clobber the stable minor tag.

Re-running is safe at any point before the release is published — signatures
are re-uploaded with `--clobber` and the tag promotion is idempotent. Once the
release is out of draft the script refuses to run again; re-signing a published
release means deleting and re-cutting it.

On failure the script keeps its download directory and prints the path, so you
can inspect the bytes rather than re-downloading several hundred megabytes.

## Recovery

**CI failed.** No signed artifact escaped and no pullable image tag was
created, but the tag is on `origin` and an untagged image manifest may exist.
Delete the draft and the tag, fix the problem, and cut again:

```bash
gh release delete v0.1.2 --yes
git push --delete origin v0.1.2
git tag -d v0.1.2
```

The version-bump commit is already on `origin/main` (`release.toml` sets
`push = true`), so it cannot simply be reset away. Either `git revert` it, or
force-push `main` if nothing else has landed — and say which you did, because
one rewrites published history.

Re-running the workflow on the same tag is also fine: the draft creation and
both asset uploads are idempotent. It refuses to touch an already-published
release.

**The tag is pushed but the release was never published.** A public tag with no
release behind it is the one state this flow leaves lying around. Either run the
signing script, or delete the tag as above.

**Abandoned image manifests.** A run whose release is never signed leaves an
untagged manifest in the GHCR package. It is not pullable by name and costs only
storage, so cleanup is optional. If you do delete one, take care: GHCR keys a
package version by digest and treats tags as metadata on it, so deleting the
wrong version removes the image behind every tag pointing at it, `latest`
included. Match the digest against `image-digest.txt` before deleting anything.

## Verifying a release as a consumer would

Worth doing once after the first release signed by a new maintainer's key. The
commands are in [SECURITY.md](SECURITY.md#verify-a-release-tag).
