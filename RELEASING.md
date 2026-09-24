# Releasing deCDN

Releases are cut locally and signed with a maintainer's own GPG key. CI builds
the artifacts but signs nothing and publishes nothing — there is no signing key
and no registry credential in Actions secrets.

A release is therefore three commands with a CI build between the first two:

```bash
cargo release patch --execute              # cut, sign and push the tag
                                           # (the FIRST release is `minor`: 0.0.0 -> 0.1.0)
# ...wait for the tag-push run to finish...
.github/scripts/sign-release.sh v0.1.2     # verify, sign, publish, tag images
.github/scripts/publish-crates.sh v0.1.2   # publish the crates to crates.io
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

3. **Tools.** `cargo install cargo-release git-cliff`, plus `gh auth login`.

4. **Registry logins.** `docker login ghcr.io` with a token carrying
   `write:packages`, and `docker login docker.io` — the image is published to
   both. Skip the second with `DECDN_SKIP_DOCKERHUB=1` if you only need GHCR.

5. **A crates.io token.** `cargo login`, with a token scoped to
   publish-update (and publish-new for the first release). Membership of the
   `decdn` crates.io owner set is what makes it work; the token itself is
   personal and never leaves your machine. That owner set does not exist until
   the first publish creates it — see [Crate ownership](#crate-ownership).

6. **Before the *first* release only — clear the new-crate rate limit.**
   crates.io limits crate *creation* far harder than updates: `PublishNew`
   allows a burst of 5, then roughly one per 10 minutes. The first release
   creates **ten** crates at once, so a single run would be rate-limited
   partway through — and since a published version is immutable, that leaves
   some crates uploaded, the version spent, and no clean retry.

   `publish-crates.sh` refuses to start in that situation, but clearing it is a
   manual step. Either ask the crates.io team to raise this repo's publish-new
   limit (the documented route, and the tidier one), or publish the ten
   crates by hand ahead of the tag, in dependency order, spacing them out. Once
   the names exist, every later release is `PublishUpdate` and unconstrained.
   Set `DECDN_ALLOW_RATE_LIMIT=1` to proceed once the limit has been raised.

## Cutting a version

`cargo release <patch|minor|major> --execute` does all of the following, driven
by [`release.toml`](release.toml):

- bumps every crate in the workspace to the same version (`shared-version`)
- commits as `chore(release): v<version>`, **signed**
- tags `v<version>`, **signed**
- pushes both to `origin`

`release.toml` sets `publish = false`, so this step uploads nothing: it runs
before CI has built anything and before a human has verified anything, which is
the wrong moment to make an irreversible crates.io upload. Publishing is the
separate, later `publish-crates.sh` step below.

Cut from `main`, with a clean tree and CI green on the commit you are tagging.

The workspace reads `0.0.0` until the first release is cut. That is a
statement, not a placeholder: `0.0.0` is never tagged, and the first cut is
`cargo release minor --execute`, which makes it `0.1.0`. From then on the
version moves only under `cargo release`. The `packaging` CI job and the
`workspace-manifests` pre-commit hook refuse a member that restates the
version or an internal `[workspace.dependencies]` alias that disagrees with
it, so a hand edit fails before it ships.

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
3. checks all eleven crate versions match the tag;
4. re-runs `cargo fmt`, `clippy` and the test suite, plus `cargo semver-checks`
   against the previous tag — skipped for major bumps, for minor bumps while the
   major is `0`, and for the first release (no prior tag to diff against);
   then `cargo publish --workspace --dry-run`, which packages and verify-builds
   every crate. That runs here, before the draft exists, because a packaging
   error found during the real publish has no clean recovery;
5. creates the GitHub Release as a **draft**;
6. builds eleven archives (`decdn-node` on five targets, `decdn` on six: the
   same five plus Windows ARM64) and a
   `SHA256SUMS` manifest, asserting all eleven are present;
7. assembles the multi-arch image from those archives — it does not compile
   from source, so the binary in the image is byte-identical to the archived
   one — and pushes the manifest **untagged**, attaching the SBOM and
   `image-digest.txt`.

Draft assets need authentication to download, and no image tag exists yet, so
nothing resolves by name until the release is signed. The image manifest is
fetchable by digest in that window — untagged is not unreachable — but the
digest is not advertised anywhere a user would look, and no tag ever points at
unsigned bytes.

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
digest on GHCR; mirrors that same digest to Docker Hub; and flips the release
out of draft.

The mirror is a manifest copy, not a rebuild: `docker buildx imagetools create`
copies the manifest bytes verbatim, so `docker.io/decdn/decdn-node` and
`ghcr.io/decdn/decdn-node` serve one identical digest and the single signature
over `image-digest.txt` vouches for both. The script re-reads every tag on both
registries afterwards and refuses to publish if any resolves to a different
digest, so this is checked rather than assumed.

Useful environment overrides:

| Variable | Effect |
|----------|--------|
| `DECDN_SIGNING_KEY` | key to sign with, when your default key is not the one in `KEYS` |
| `DECDN_SKIP_IMAGE_TAGS` | set to `1` to publish with **no** pullable image tag at all — the release then ships only the signed digest |
| `DECDN_SKIP_DOCKERHUB` | set to `1` to tag on GHCR only (implied by `DECDN_SKIP_IMAGE_TAGS`) |
| `DECDN_DOCKERHUB_REPO` | override the Docker Hub repository (defaults to `<DECDN_REPO>-node`) |
| `DECDN_REPO` | target a fork instead of `decdn/decdn` — both registries follow it |

The skip variables take `1`/`true`/`yes` or `0`/`false`/`no`; anything else is
rejected rather than guessed, so `DECDN_SKIP_DOCKERHUB=0` means *don't* skip.

Both image repositories derive from `DECDN_REPO`, so a fork rehearsal stays
entirely on the fork. Point `DECDN_DOCKERHUB_REPO` somewhere else only if the
Docker Hub namespace genuinely differs from the GitHub one.

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

## Publishing to crates.io

Last, once the GitHub Release is out of draft:

```bash
.github/scripts/publish-crates.sh v0.1.2
```

It verifies the tag against `KEYS` and against `origin` the way
`sign-release.sh` does, and adds two checks of its own. It **requires the
release to be published, not a draft**, and — because leaving draft does not by
itself prove anything was signed — it **requires `SHA256SUMS.asc` to be
attached**. A crates.io version can never be replaced, so nothing reaches the
registry that no maintainer signature vouches for.

`DECDN_SIGNING_KEY` works here too, and accepts the same forms as in
`sign-release.sh` (fingerprint, key id or uid). It is resolved against `KEYS`
and compared as a full primary-key fingerprint; an ambiguous value is rejected
rather than resolved to whichever key sorts first.

It then checks out the tag into a detached worktree and publishes from there,
not from your working copy, so uncommitted edits or a different branch cannot
leak into the upload. `cargo publish --workspace` resolves the order itself and
waits for each crate to reach the index before publishing its dependents. After
a dry run it prints the crate list and asks you to type the version to confirm.

Ten crates are published; `decdn-e2e` is `publish = false` (test fixtures)
and cargo skips it. The workspace has eleven members, and the CI version check
covers all eleven — only the upload is ten.

**This step is not re-runnable.** A published version is immutable — yanking
hides it from new resolutions but never frees the version. If it fails partway,
the crates already uploaded stay uploaded; re-running would fail immediately on
the first of them. The script prints what to do: publish only the remainder,
in dependency order, with `cargo publish -p <crate> --locked`.

A failure here does not invalidate the release. The GitHub Release, the
signatures and the container images are all already published and stand on
their own; crates.io is an additional distribution channel, so the recovery is
to finish the remaining crates, not to re-cut the version.

### Crate ownership

crates.io gives a new crate to whoever publishes the name first, and
`publish-crates.sh` does nothing about ownership. The first release therefore
leaves all ten crates owned by one person — whoever ran it. Hand them to the
org straight afterwards, in the same sitting.

The new owner has to be a GitHub team: crates.io has no organisation account,
so a team is the only owner that outlives an individual. The `crates-io` team
under the `decdn` org is that owner. Confirm you are in it, then:

```bash
TEAM=github:decdn:crates-io
for crate in decdn-protocol decdn-config-types decdn-bao-range decdn-common \
             decdn-cache decdn-incentive decdn-reputation decdn-client \
             decdn-node decdn-cli; do
  cargo owner --add "$TEAM" "$crate"
done
```

crates.io resolves the team through your own GitHub authorisation, so it fails
unless you are a member of `crates-io`. The team is `closed`, not secret, so
crates.io can read it with the `read:org` scope it asks for at login.

A team owner publishes updates but cannot change the owner list. Only an
individual owner does that, so do not remove yourself once the team is added —
that would leave nobody able to grant or revoke access again.

This runs once. Later releases publish updates to names that already exist and
add no crates, which is also why step 5's owner-set membership only starts
meaning something after the first release. `cargo owner --list <crate>`
confirms the result.

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

**Crates are already on crates.io.** Then the version is spent: it cannot be
re-cut, because `cargo publish` will refuse to replace it and no amount of
deleting tags or releases frees it. Do not delete the tag. `cargo yank` the
affected crates so nothing new resolves to them, then cut the *next* patch
version with the fix. This is why `publish-crates.sh` runs last, after every
other artifact is verified and published — everything before it is reversible.

## Before open-sourcing

Two of this workflow's protections are **not enforceable while the repository is
private**, because branch and tag protection are unavailable on the current plan
— the API returns `403 Upgrade to GitHub Pro or make this repository public`.
Making the repository public enables both. Configure them at that point:

1. **Tag protection on `v*`.** Without it, anyone with push access can create a
   release tag. Worse, a `push`-triggered workflow runs the workflow definition
   *from the pushed ref*, so a tag can carry a `release.yml` with the signature
   gate deleted and still receive `contents: write` and `packages: write`.
2. **Branch protection on `main`.** The gate reads `KEYS` from `origin/main`
   precisely so the tag cannot supply its own trust root. That only means
   something if `main` is protected.

Until both are set, treat the CI gate as defence in depth. The guarantee that
does hold is the maintainer signature, because consumers verify it off-platform
against the fingerprints in [SECURITY.md](SECURITY.md) rather than against
anything GitHub enforces.

## Verifying a release as a consumer would

Worth doing once after the first release signed by a new maintainer's key. The
commands are in [SECURITY.md](SECURITY.md#verify-a-release-tag).
