# Releasing sponsord

The workspace is released as a whole: `sponsord-api`, `sponsord-core`,
`sponsord`, `sponsord-onramp` and `decdn-sponsored` share one version
(`[workspace.package]` in the root `Cargo.toml`) and are released together,
from one `vX.Y.Z` tag, as decdn's crates are. A release with no change to a
crate still carries it at the new version.

Releases are cut locally and signed with a maintainer's own GPG key, the same
way decdn's are. CI builds the artifacts but signs nothing and publishes
nothing — there is no signing key and no registry credential in Actions
secrets.

A release is three commands with a CI build between the first two:

```bash
cargo release patch --execute                  # cut, sign and push the tag
# ...wait for the tag-push run to finish...
.github/scripts/sign-release.sh v0.1.2         # verify, sign, tag images, publish
.github/scripts/publish-crates.sh v0.1.2       # publish every crate to crates.io
# then pin it in the onramp (ONRAMP_CLI_RELEASE / _SUMS_SHA256)
```

## What a release carries

[`.github/scripts/release_plan.py`](.github/scripts/release_plan.py) is the one
table the workflow and both scripts read. Every `vX.Y.Z` release carries all
of it:

| Crate | Archives (one `SHA256SUMS` covers all 10) | Image | crates.io |
|---|---|---|---|
| `decdn-sponsored` | Linux, macOS and Windows, x86_64 and aarch64 each (6) | — | yes |
| `sponsord` | Linux, x86_64 and aarch64 (2) | `sponsord` | yes |
| `sponsord-onramp` | Linux, x86_64 and aarch64 (2) | `sponsord-onramp` | yes |
| `sponsord-core` | — (library) | — | yes |
| `sponsord-api` | — (library) | — | yes |

**The archive names are a contract.** The installers the onramp serves fetch
`<binary>-<version>-<target>.tar.gz` (`.zip` on Windows) from a release and
look it up in its `SHA256SUMS`, with the binary at the archive root. Renaming
either breaks every installer already served.

## One-time setup

1. **A GPG key in [`KEYS`](KEYS).** `KEYS` holds the same maintainer keys as
   decdn's. Your public key must be merged to `main` before the tag you sign
   will be accepted — CI reads `KEYS` from `origin/main` (not from your tag)
   and runs `git verify-tag` before it builds anything. `sign-release.sh`
   independently refuses to sign with a key that is not in `KEYS`, because
   consumers following [SECURITY.md](SECURITY.md) would reject the result.

   ```bash
   gpg --armor --export <your-fingerprint>   # append the block to KEYS
   gpg --fingerprint <your-key-id>           # add the fingerprint to SECURITY.md
   ```

2. **Git configured to sign.** `release.toml` sets `sign-commit` and
   `sign-tag`, and `cargo release` relies on git's own signing config. Set the
   key explicitly — git otherwise picks one by matching `user.email`, which
   may not be the key you published.

   ```bash
   git config user.signingkey <your-fingerprint>
   ```

3. **Tools.** `cargo install cargo-release git-cliff`, plus `gh auth login`.

4. **Registry logins.** `docker login ghcr.io` with a token carrying
   `write:packages`, and `docker login docker.io` — the server images are
   published to both. Skip the second with `SPONSORD_SKIP_DOCKERHUB=1` if you
   only need GHCR.

5. **A crates.io token.** `cargo login`, with a token scoped to publish-update
   (and publish-new for the first release).

6. **Repository protection.** Protect the release tags (`v*`) and the `main`
   branch (a ruleset). `cargo release` pushes its release commit straight to
   `main`, so whoever cuts a release must be able to bypass the `main`
   ruleset's pull-request rule (an organization admin, today); otherwise the
   push is refused after the tag is signed. Without tag protection, anyone with push access can
   create a release tag, and a `push`-triggered workflow runs the workflow
   definition *from the pushed ref*, so a tag can carry a `release.yml` with
   the signature gate deleted. The gate reads `KEYS` from `origin/main`
   precisely so a tag cannot supply its own trust root, which only means
   something while `main` is protected. Until both are set, treat the CI gate
   as defence in depth; the maintainer signature, verified off-platform, is
   what holds.

## Pinning decdn

The decdn crates are git dependencies on
[decdn/decdn](https://github.com/decdn/decdn), at a release `tag` in the root
`Cargo.toml`, and two places say which decdn a release builds against. They
must agree:

- `Cargo.lock`: the one decdn commit every build compiles. Every CI job and
  every release build is `--locked`, so it is what ships.
- The `version` on each `decdn-*` entry in the root `Cargo.toml`'s
  `[workspace.dependencies]`: what the published crates require from
  crates.io, where the `git` source is dropped. It must equal decdn's
  workspace version at the locked commit.

`.github/scripts/check-decdn-pin.sh` checks them against each other; CI runs it
on every PR, and `release.yml` runs it on the tag. Every gating CI job builds
the locked commit, so a decdn change never turns an unrelated sponsord PR red.
The non-blocking `decdn main canary` job moves the lock to decdn `main` and
builds and tests there; when it goes red, the next bump needs work.

**A release meant for crates.io must lock a decdn release**: the locked commit
must be the commit of decdn's `v<version>` tag, and that version must be on
crates.io. `publish-crates.sh` checks both and refuses until then. Any other
commit still releases binaries and images; it just cannot go to crates.io.

To move the lock to another decdn release, set `tag = "v<version>"` on every
`decdn-*` entry in the root `Cargo.toml`, and `version = "<version>"` on each
entry that has one, then:

```bash
cargo update -p decdn-client -p decdn-common -p decdn-incentive -p decdn-e2e
cargo metadata --locked --format-version 1 > /dev/null
.github/scripts/check-decdn-pin.sh
```

To build against decdn's current `main` instead, set `branch = "main"` in
place of the `tag`, and `version` to decdn main's workspace version, and run
the same commands. That lock can ship binaries and images, not crates.

Land the move as its own PR before cutting a release against it, with a
changelog entry naming the new decdn commit in every crate it reaches.

## Cutting a version

`cargo release <patch|minor|major> --execute` does all of the following for
the whole workspace, driven by [`release.toml`](release.toml):

- bumps `version` in `[workspace.package]`, which every crate inherits, and
  the `version` on each internal `[workspace.dependencies]` alias
  (`dependent-version = "upgrade"`)
- opens a `## [<version>] - <date>` heading under `## [Unreleased]` in every
  crate's `CHANGELOG.md`, so the entries gathered there become the release's
- rewrites `info.version` in `docs/openapi/*.json`, which carry
  `sponsord-api`'s version and are snapshot-tested against it
  (`crates/sponsord-api/Cargo.toml`, `[package.metadata.release]`)
- commits as `chore(release): v<version>`, **signed**
- tags `v<version>`, **signed**
- pushes both to `origin`

`sponsord-e2e` inherits the version but is never released
(`release = false` in its manifest).

`release.toml` sets `publish = false`, so this step uploads nothing: it runs
before CI has built anything and before anyone has verified anything, the
wrong moment for an irreversible crates.io upload. Publishing is the separate,
later `publish-crates.sh` step.

Cut from `main`, with a clean tree and CI green on the commit you are tagging.

**The first release** bumps from the `0.0.0` placeholder the workspace carries
until then, so it names its level like every other: `cargo release minor
--execute` cuts `v0.1.0`.

To preview without changing anything, drop `--execute` (with the level you
will cut, `minor` for the first release):

```bash
cargo release patch --no-confirm
```

Dry-run is cargo-release's default, so the preview writes nothing and needs no
cleanup.

## Changelogs

Each crate keeps its own `crates/<dir>/CHANGELOG.md`, maintained by hand, one
entry per PR that changes it, under `## [Unreleased]`
([Keep a Changelog](https://keepachangelog.com/en/1.1.0/)). `cargo release`
opens the version heading above those entries in every crate's file, so a
crate with nothing new gets an empty heading: that version changed nothing in
it. git-cliff only generates the GitHub release notes: the conventional commit
subjects since the previous release tag (the `pr-title` check keeps
squash-merge subjects conventional; others are left out).

- Entries group under **Changed (BREAKING)**, **Added**, **Changed**,
  **Fixed**, **Removed**, **Security**, each a bullet that opens with a bold
  one-line summary.
- **Contract-breaking** changes are called out explicitly: the release archive
  names, the `~/.decdn/sponsor.toml` schema, and both servers' HTTP APIs
  (`sponsord-api`) are read by installers and clients already deployed.
- **Config-breaking** changes name the `SPONSORD_*` or `ONRAMP_*` variable
  (rename, new required variable, default shift).
- A move of the locked decdn names the new commit in every crate it reaches.
- Security advisories cite the `RUSTSEC-YYYY-NNNN` id.

## What CI does with the tag

Pushing a release tag starts
[`.github/workflows/release.yml`](.github/workflows/release.yml). Its `tag`
job, which compiles nothing, gates everything else:

1. rejects the tag unless `release_plan.py` reads it as
   `vMAJOR.MINOR.PATCH[-pre]` (the trigger glob is looser than it looks —
   `v0$(whoami)` matches it);
2. imports `KEYS` **from `origin/main`** and refuses the tag if it is not
   signed by a key published there, or if it no longer points at the commit
   the run was triggered for (every later job builds that commit, never the
   tag name, so a tag re-pushed mid-run cannot get one commit checked and
   another built);
3. checks every crate's version matches the tag, and runs `check-decdn-pin`;
4. picks the semver baseline, the highest release below the tag, and refuses
   it unless it too carries a signature from `KEYS`. "Below" is in semver
   order, so a re-run after a newer release, or a maintenance release, never
   compares against a newer version. The first release has none.

Then two jobs run side by side:

1. **`verify`** re-runs `cargo fmt`, clippy and the test suite on the whole
   workspace; runs `cargo semver-checks --workspace` on every published
   crate's public API (default features) against the baseline, failing if a
   change needs a bigger bump than the version says (in 0.x, a breaking change
   needs a minor bump; to run it before tagging, use
   `cargo semver-checks --workspace --baseline-rev "$(.github/scripts/release_plan.py <tag> --baseline)" --default-features`);
   then creates the GitHub Release as a **draft**, with git-cliff notes and
   the locked decdn commit it was built against.
2. **`build`** runs one job per target, each building every binary released
   for it, and fails a Linux leg whose binaries need a glibc above 2.35
   (Ubuntu 22.04).

Only once both pass:

1. `upload-assets` attaches all 10 archives and one `SHA256SUMS` manifest,
   asserting all of them are present, and appends the onramp pin values to
   the notes;
2. after it, for each server, `docker` assembles the multi-arch image from
   those archives — it does not compile from source, so the image's binary is
   byte-identical to the archived one — and pushes the manifest **untagged**,
   attaching the SBOM and `<image>-image-digest.txt`.

A release that fails verification therefore leaves no asset on the draft and
pushes no image.

One check decdn's release run has is absent here: `cargo publish --dry-run`
resolves the decdn crates from crates.io, where they may not be yet, so it
runs in `publish-crates.sh` instead.

Draft assets need authentication to download, and no image tag exists yet, so
nothing resolves by name until the release is signed.

## Signing and publishing

Once the run is green:

```bash
.github/scripts/sign-release.sh v0.1.2
```

It resolves your signing key to a fingerprint and confirms it is published in
`KEYS`; force-fetches tags and checks your local tag matches `origin`'s;
verifies the tag signature; then:

- **archives:** downloads them, checks `SHA256SUMS` strictly against them,
  that every published archive appears in it, **and** that the release carries
  exactly the 10 archives the plan lists, then signs `SHA256SUMS`;
- **each image:** validates `<image>-image-digest.txt`, signs it and the SBOM,
  promotes `:<version>` and `:<major>.<minor>` from the signed digest on
  GHCR, and `:latest` too unless a higher release exists, then mirrors that
  digest to Docker Hub.

Every signature is verified against a keyring built only from `KEYS` before
the `.asc` files are uploaded. Last, it takes the release out of draft; a
stable release with no higher stable release above it becomes the
repository's "Latest release", so a maintenance release never takes it.

The images' version tags are the release's version. The mirror is a
manifest copy, not a rebuild (`docker buildx imagetools create`), so
`docker.io/decdn/<image>` and `ghcr.io/decdn/<image>` serve one identical
digest and one signature covers both. The script re-reads every tag afterwards
and refuses to publish if any resolves to a different digest.

| Variable | Effect |
|----------|--------|
| `SPONSORD_SIGNING_KEY` | key to sign with, when your default key is not the one in `KEYS` |
| `SPONSORD_SKIP_IMAGE_TAGS` | `1` publishes with **no** pullable image tag — the release then ships only the signed digest |
| `SPONSORD_SKIP_DOCKERHUB` | `1` tags on GHCR only (implied by `SPONSORD_SKIP_IMAGE_TAGS`) |
| `SPONSORD_DOCKERHUB_NAMESPACE` | override the Docker Hub namespace (defaults to the repo owner) |
| `SPONSORD_REPO` | target a fork instead of `decdn/sponsord` — both registries follow it |

The skip variables take `1`/`true`/`yes` or `0`/`false`/`no`; anything else is
rejected rather than guessed.

A prerelease (`v1.2.0-rc.1`) is drafted as a GitHub prerelease, is
never the repository's "Latest release", and gets only its exact version image
tag: moving `latest` to a candidate would hand it to every unpinned pull.

Re-running is safe at any point before the release is published. Once it is
out of draft the script refuses to run again.

## Pinning a release in the onramp

Signing adds `SHA256SUMS.asc` beside `SHA256SUMS` and changes nothing else, so
the values the workflow appended to the release notes stay valid. Set them on
the onramp to make its installers serve the release:

```bash
ONRAMP_CLI_RELEASE=v0.1.2
ONRAMP_CLI_SUMS_SHA256=<from the release notes>
```

The onramp refuses a pin that is not `vX.Y.Z`, and passes the installers the
version alongside the tag, so they fetch `decdn-sponsored-0.1.2-<target>.*`.
`decdn` releases are pinned the same way with decdn's own `vX.Y.Z` tags
(`ONRAMP_DECDN_RELEASE`, and `ONRAMP_DECDN_SUMS_SHA256` = the SHA-256 of that
release's `SHA256SUMS`). Both pins share the form, so check which repository's
release notes each value came from.

Users who already installed keep their CLI until they re-run the installer.
When a release changes what the onramp and the CLI exchange, set
`ONRAMP_MIN_CLI_VERSION` on the onramp once the new pin is live, so older CLIs
tell their users to re-run it.

## Publishing to crates.io

Last, once the GitHub Release is out of draft:

```bash
.github/scripts/publish-crates.sh v0.1.2
```

It verifies the tag against `KEYS` and against `origin` the way
`sign-release.sh` does, and requires the release to be published **and** to
carry `SHA256SUMS.asc`. It then lists the decdn crates the uploaded manifests
require ([`registry_deps.py`](.github/scripts/registry_deps.py)): the decdn
commit `Cargo.lock` locks at the tag must be decdn's `v<version>` tag, and that
release must be on crates.io. Until it is, the script stops there, with
nothing uploaded. The sibling crates need no such check: they go up in the
same run.

It then makes a detached worktree of this repo at the tag and publishes from
there — not from your working copy. It checks every publishable crate is at
the tag's version, and refuses if more than 5 of them are new to crates.io
(the publish-new rate limit would stop the run partway; set
`SPONSORD_ALLOW_RATE_LIMIT=1` once crates.io has raised it). After
`cargo publish --workspace --dry-run` it asks you to type the version to
confirm, publishes every crate in dependency order, and confirms crates.io
serves each.

**This step is not re-runnable once an upload succeeds.** A published version
is immutable. If it fails partway, the crates already uploaded stay uploaded;
the script prints which to check and how to publish the rest by hand. A
failure here does not invalidate the release: the GitHub Release, signatures
and images stand on their own.

### Crate ownership

A crate's first publish leaves it owned by whoever ran it. Hand it to the org's
crates.io team straight afterwards:

```bash
cargo owner --add github:decdn:crates-io <crate>
```

Do not remove yourself afterwards: a team owner cannot change the owner list.

## Recovery

**CI failed.** No signed artifact escaped and no pullable image tag exists, but
the tag is on `origin` and an untagged image manifest may exist. Delete the
draft and the tag, fix the problem, and cut again:

```bash
gh release delete v0.1.2 --yes
git push --delete origin v0.1.2
git tag -d v0.1.2
```

The version-bump commit is already on `origin/main` (`push = true`). Either
`git revert` it, or force-push `main` if nothing else has landed — and say
which you did.

Re-running the workflow on the same tag is also fine: draft creation and the
asset uploads are idempotent, and it refuses to touch a published release.

**A crate is already on crates.io.** The version is spent. Do not delete the
tag. `cargo yank` what was published and cut the next patch version.

## Verifying a release as a consumer would

Worth doing once after the first release signed by a new key. The commands are
in [SECURITY.md](SECURITY.md#verify-a-release-tag).
