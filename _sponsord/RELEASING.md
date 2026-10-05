# Releasing sponsord

Each crate is versioned and released on its own: `sponsord-api`,
`sponsord-core`, `sponsord`, `sponsord-onramp` and `decdn-sponsored` each
carry their own version and are released from their own `<crate>-vX.Y.Z` tags. Releasing one never bumps,
rebuilds or republishes another.

Releases are cut locally and signed with a maintainer's own GPG key, the same
way decdn's are. CI builds the artifacts but signs nothing and publishes
nothing — there is no signing key and no registry credential in Actions
secrets.

A release is three commands with a CI build between the first two:

```bash
cargo release -p decdn-sponsored patch --execute       # cut, sign and push the tag
# ...wait for the tag-push run to finish...
.github/scripts/sign-release.sh decdn-sponsored-v0.1.2  # verify, sign, tag images, publish
.github/scripts/publish-crates.sh decdn-sponsored-v0.1.2  # publish the crate to crates.io
# decdn-sponsored only: pin it in the onramp (ONRAMP_CLI_RELEASE / _SUMS_SHA256)
```

## What each crate's release carries

[`.github/scripts/release_plan.py`](.github/scripts/release_plan.py) is the one
table the workflow and both scripts read:

| Tag | Archives (+ `SHA256SUMS`) | Image | crates.io |
|---|---|---|---|
| `decdn-sponsored-vX.Y.Z` | `decdn-sponsored` for Linux, macOS and Windows, x86_64 and aarch64 each (6) | — | yes |
| `sponsord-vX.Y.Z` | `sponsord` for Linux, x86_64 and aarch64 (2) | `sponsord` | yes |
| `sponsord-onramp-vX.Y.Z` | `sponsord-onramp` for Linux, x86_64 and aarch64 (2) | `sponsord-onramp` | yes |
| `sponsord-core-vX.Y.Z` | — (library; the release carries notes only) | — | yes |
| `sponsord-api-vX.Y.Z` | — (library; the release carries notes only) | — | yes |

**The archive names are a contract.** The installers the onramp serves fetch
`<binary>-<version>-<target>.tar.gz` (`.zip` on Windows) from a
`decdn-sponsored` release and look it up in its `SHA256SUMS`, with the binary
at the archive root. Renaming either breaks every installer already served.

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
   (and publish-new for a crate's first release).

6. **Repository protection.** Protect the release tags (`sponsord-api-v*`,
   `sponsord-core-v*`, `sponsord-v*`, `sponsord-onramp-v*`,
   `decdn-sponsored-v*`) and the `main`
   branch (a ruleset). Without tag protection, anyone with push access can
   create a release tag, and a `push`-triggered workflow runs the workflow
   definition *from the pushed ref*, so a tag can carry a `release.yml` with
   the signature gate deleted. The gate reads `KEYS` from `origin/main`
   precisely so a tag cannot supply its own trust root, which only means
   something while `main` is protected. Until both are set, treat the CI gate
   as defence in depth; the maintainer signature, verified off-platform, is
   what holds.

## Pinning decdn

The decdn crates are git dependencies on
[decdn/decdn](https://github.com/decdn/decdn) (`branch = "main"` in the root
`Cargo.toml`), and two places say which decdn a release builds against. They
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

To move the lock to decdn's current `main`:

```bash
cargo update -p decdn-client -p decdn-common -p decdn-incentive -p decdn-e2e
# if decdn's workspace version moved, set version = "<it>" on each decdn-*
# entry in Cargo.toml, then:
cargo metadata --locked --format-version 1 > /dev/null
.github/scripts/check-decdn-pin.sh
```

To lock a decdn release for crates.io, point the aliases at its tag for the
update (`tag = "v0.1.0"` in place of `branch = "main"`), run the same
commands, and keep the tag there while sponsord ships against that release.

Land the move as its own PR before cutting releases against it, with a
changelog entry naming the new decdn commit in every crate it reaches.

## Cutting a version

`cargo release -p <crate> <patch|minor|major> --execute` does all of the
following for that one crate, driven by [`release.toml`](release.toml):

- bumps the crate's own `version`, and if another crate depends on it, the
  `version` on its `[workspace.dependencies]` alias
  (`dependent-version = "upgrade"`) — the dependents themselves are not
  re-released
- commits as `chore(release): <crate> v<version>`, **signed**
- tags `<crate>-v<version>`, **signed**
- pushes both to `origin`

`release.toml` sets `publish = false`, so this step uploads nothing: it runs
before CI has built anything and before anyone has verified anything, the
wrong moment for an irreversible crates.io upload. Publishing is the separate,
later `publish-crates.sh` step.

Cut from `main`, with a clean tree and CI green on the commit you are tagging.

**A crate's first release** is the version it already carries (every crate
starts at `0.1.0`), so it takes no bump level:
`cargo release -p <crate> --execute` tags the current version as-is, with no
commit. Every later release names its level.

**Order across crates.** When a change spans crates, release the dependency
first, so its version is on crates.io when the dependent publishes:
`sponsord-api` first (every other crate depends on it), then `sponsord-core`,
then `sponsord`. `sponsord-onramp` and `decdn-sponsored` depend only on
`sponsord-api`.

**`sponsord-api` carries the OpenAPI documents' version.** Releasing it
changes `docs/openapi/*.json`: regenerate them in the release PR with
`UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi`, or CI fails the
snapshot test. `publish-crates.sh`
refuses a crate whose sibling dependency is not on crates.io yet, and says
which release to publish first.

To preview without changing anything, drop `--execute`:

```bash
cargo release -p sponsord patch --no-confirm
```

Dry-run is cargo-release's default, so the preview writes nothing and needs no
cleanup.

## Changelogs

Each crate keeps its own `crates/<dir>/CHANGELOG.md`, maintained by hand, one
entry per PR that changes it ([Keep a Changelog](https://keepachangelog.com/en/1.1.0/)).
`cargo release` does **not** touch it — move the `[Unreleased]` entries under
the new version as part of the PR before the cut. git-cliff only generates the
GitHub release notes: the conventional commit subjects that touched the crate's
directory since its previous tag (the `pr-title` check keeps squash-merge
subjects conventional; others are left out).

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
[`.github/workflows/release.yml`](.github/workflows/release.yml), which:

1. rejects the tag unless `release_plan.py` reads it as
   `<crate>-vMAJOR.MINOR.PATCH[-pre]` for a known crate (the trigger globs are
   looser than they look — `sponsord-v0$(whoami)` matches them);
2. imports `KEYS` **from `origin/main`** and refuses the tag if it is not
   signed by a key published there;
3. checks the crate's version matches the tag, and runs `check-decdn-pin`;
4. re-runs `cargo fmt`, clippy and the test suite on the whole workspace;
5. creates the GitHub Release as a **draft**, with git-cliff notes for that
   crate and the locked decdn commit it was built against;
6. builds the crate's archives and a `SHA256SUMS` manifest, asserting all of
   them are present, and for `decdn-sponsored` appends the onramp pin values to
   the notes;
7. for a server, assembles its multi-arch image from those archives — it does
   not compile from source, so the image's binary is byte-identical to the
   archived one — and pushes the manifest **untagged**, attaching the SBOM and
   `<image>-image-digest.txt`.

One check decdn's release run has is absent here: `cargo publish --dry-run`
resolves the decdn crates from crates.io, where they may not be yet, so it
runs in `publish-crates.sh` instead.

Draft assets need authentication to download, and no image tag exists yet, so
nothing resolves by name until the release is signed.

## Signing and publishing

Once the run is green:

```bash
.github/scripts/sign-release.sh sponsord-v0.1.2
```

It resolves your signing key to a fingerprint and confirms it is published in
`KEYS`; force-fetches tags and checks your local tag matches `origin`'s;
verifies the tag signature; then, for what the crate's release carries:

- **archives:** downloads them, checks `SHA256SUMS` strictly against them,
  that every published archive appears in it, **and** that the release carries
  exactly the number of archives the crate ships, then signs `SHA256SUMS`;
- **an image:** validates `<image>-image-digest.txt`, signs it and the SBOM,
  promotes `:latest`, `:<version>` and `:<major>.<minor>` from the signed
  digest on GHCR, and mirrors that digest to Docker Hub;
- **nothing (`sponsord-core`):** the signed tag is the attestation.

Every signature is verified against a keyring built only from `KEYS` before
the `.asc` files are uploaded. Last, it takes the release out of draft; only
stable `decdn-sponsored` releases become the repository's "Latest release", the
CLI being what users install.

The image's version tags are the server crate's own version. The mirror is a
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

A prerelease (`sponsord-v1.2.0-rc.1`) is drafted as a GitHub prerelease, is
never the repository's "Latest release", and gets only its exact version image
tag: moving `latest` to a candidate would hand it to every unpinned pull.

Re-running is safe at any point before the release is published. Once it is
out of draft the script refuses to run again.

## Pinning a `decdn-sponsored` release in the onramp

Signing adds `SHA256SUMS.asc` beside `SHA256SUMS` and changes nothing else, so
the values the workflow appended to the release notes stay valid. Set them on
the onramp to make its installers serve the release:

```bash
ONRAMP_CLI_RELEASE=decdn-sponsored-v0.1.2
ONRAMP_CLI_SUMS_SHA256=<from the release notes>
```

The onramp refuses a CLI pin that is not `decdn-sponsored-vX.Y.Z`, and
passes the installers the version alongside the tag, so they fetch
`decdn-sponsored-0.1.2-<target>.*`. `decdn` releases are pinned the same way
with decdn's own `vX.Y.Z` tags (`ONRAMP_DECDN_RELEASE`, and
`ONRAMP_DECDN_SUMS_SHA256` = the SHA-256 of that release's `SHA256SUMS`).

Users who already installed keep their CLI until they re-run the installer.
When a release changes what the onramp and the CLI exchange, set
`ONRAMP_MIN_CLI_VERSION` on the onramp once the new pin is live, so older CLIs
tell their users to re-run it.

## Publishing to crates.io

Last, once the GitHub Release is out of draft:

```bash
.github/scripts/publish-crates.sh sponsord-v0.1.2
```

It verifies the tag against `KEYS` and against `origin` the way
`sign-release.sh` does, and requires the release to be published **and**, for a
crate with archives, to carry `SHA256SUMS.asc`. It then lists the crate's path
and git dependencies that survive into the uploaded manifest
([`registry_deps.py`](.github/scripts/registry_deps.py)) and requires each on
crates.io at the version it needs:

- a **sibling** (`sponsord-api` for every other crate; `sponsord-core` for
  `sponsord`) — publish its release first;
- the **decdn** crates — the decdn commit `Cargo.lock` locks at the tag must
  be decdn's `v<version>` tag, and that release must be on crates.io. Until it
  is, the crates that link decdn (`sponsord-core`, `sponsord`,
  `decdn-sponsored`) stop here, with nothing uploaded.

It then makes a detached worktree of this repo at the tag and publishes from
there — not from your working copy.
After `cargo publish -p <crate> --dry-run` it asks you to type the version to
confirm, publishes, and confirms crates.io serves it.

**This step is not re-runnable once the upload succeeds.** A published version
is immutable. A failure here does not invalidate the release: the GitHub
Release, signatures and image stand on their own.

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
gh release delete sponsord-v0.1.2 --yes
git push --delete origin sponsord-v0.1.2
git tag -d sponsord-v0.1.2
```

The version-bump commit is already on `origin/main` (`push = true`). Either
`git revert` it, or force-push `main` if nothing else has landed — and say
which you did.

Re-running the workflow on the same tag is also fine: draft creation and the
asset uploads are idempotent, and it refuses to touch a published release.

**The crate is already on crates.io.** The version is spent. Do not delete the
tag. `cargo yank` it and cut the crate's next patch version.

## Verifying a release as a consumer would

Worth doing once after the first release signed by a new key. The commands are
in [SECURITY.md](SECURITY.md#verify-a-release-tag).
