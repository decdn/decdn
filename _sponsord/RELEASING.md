# Releasing sponsord

The workspace is released as a whole: `sponsord-api`, `sponsord-core`,
`sponsord` and `sponsord-onramp` share one version
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
scripts/sign-release.sh v0.1.2                 # verify, sign, tag images, publish
scripts/publish-crates.sh v0.1.2               # publish every crate to crates.io
```

## What a release carries

[`.github/scripts/release_plan.py`](.github/scripts/release_plan.py) is the one
table the workflow and both scripts read. Every `vX.Y.Z` release carries all
of it:

| Crate | Archives (one `SHA256SUMS` covers all 4) | Image | crates.io |
|---|---|---|---|
| `sponsord` | Linux, x86_64 and aarch64 (2) | `sponsord` | yes |
| `sponsord-onramp` | Linux, x86_64 and aarch64 (2) | `sponsord-onramp` | yes |
| `sponsord-core` | — (library) | — | yes |
| `sponsord-api` | — (library) | — | yes |

The installers the onramp serves download nothing from these releases: they
install `decdn` from decdn's own release (`ONRAMP_DECDN_RELEASE`).

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
   `write:packages`, and `docker login docker.io` — the images are
   published to both. Skip the second with `SPONSORD_SKIP_DOCKERHUB=1` if you
   only need GHCR. Each image needs its `decdn/<image>` repository on Docker
   Hub before the first release that carries it, and its GHCR package made
   public after its first push.

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

The decdn crates come from crates.io, at a caret requirement in the root
`Cargo.toml` (`decdn-client = "0.1"`): a published sponsord takes every
compatible decdn release, which decdn's `cargo semver-checks` keeps
compatible. Three places say which decdn a release builds against:

- `Cargo.lock`: the decdn release every build compiles. Every CI job and
  every release build is `--locked`, so it is what ships.
- `decdn-e2e`, the anvil fixtures, at the decdn release `tag` of that
  version. decdn does not publish it, and it builds decdn's contracts from
  its checkout, so it is a git dependency, used only by tests. Its tag must
  match the locked version, so the contracts it deploys match the ABI of the
  crates.
- A `[patch."https://github.com/decdn/decdn"]` that takes the decdn crates
  decdn-e2e shares with the crates.io dependencies (decdn-client and what it
  pulls in) from crates.io too, so each builds once. A git copy beside the
  crates.io one is a different crate, whose types do not match. The crates
  only decdn-e2e uses (decdn-node, decdn-cache, decdn-reputation) stay on git
  at its tag.

`.github/scripts/check-decdn-pin.sh` checks them against each other; CI runs it
on every PR, and `release.yml` runs it on the tag. Every gating CI job builds
the locked release, so a decdn change never turns an unrelated sponsord PR
red. The non-blocking `decdn main canary` job takes every decdn crate from
decdn `main` instead (`bump-decdn.sh canary`; `canary-check` fails it if any
stayed on crates.io), and builds and tests there;
when it goes red, the next bump needs work.

### The bump PR

[`open-decdn-bump.sh`](scripts/open-decdn-bump.sh) opens the PR that
moves the lock to a decdn release. decdn's `publish-crates.sh` runs it with
the tag it just published. To run it by hand you need git, cargo, Python 3.11+
and `gh` logged in to github.com, in a clone whose `origin` is decdn/sponsord
and which you can push to:

```bash
scripts/open-decdn-bump.sh            # decdn's latest release
scripts/open-decdn-bump.sh v0.1.2     # one release
scripts/open-decdn-bump.sh --dry-run  # print the diff, push nothing
```

It works in a temporary worktree at `origin/main`, so your checkout and
branch stay as they are. It checks the crates.io index (the one cargo
resolves from) up to 10 times, a minute apart, until it lists every decdn
crate the workspace takes from there. Then it moves decdn-e2e's tag, pins the
requirements to `=<version>` while it runs `cargo update` on every decdn
crate (so each moves to exactly that release, not to crates.io's newest), and
sets the requirements back to their caret range, moved when the release
leaves it (0.1 to 0.2). It pushes
`bump/decdn-v<version>` and opens `build: pin decdn to v<version>`.
The commit adds a changelog entry to every crate the move reaches, replacing
an unreleased one from an earlier bump
([`bump_decdn.py`](.github/scripts/bump_decdn.py)). It commits as your git
identity without the pre-commit hooks, pushes with your `origin` credentials
and opens the PR with your `gh` login; CI checks the PR.

It does nothing when `main` already locks that release or a later one (by
SemVer precedence), or when a PR from that branch exists in any state: a
closed bump PR is never reopened, so close one to skip that release. When
`cargo metadata --locked` or `check-decdn-pin.sh` fails on the new lock, it
still opens the PR, says so in the body, and exits 1. Any other exit 1
opened no PR, and the last line says which step failed. Exit 3 means
the crates.io index does not list the release yet; run it again later.

Dependabot leaves the decdn crates alone (`.github/dependabot.yml`), since it
would move the lock without the e2e tag.

### Moving the lock by hand

To move the lock to another decdn release, point decdn-e2e's `tag` at it and
pin every `decdn-*` requirement, in `[workspace.dependencies]` and in the
`[patch]`, to exactly that version for the update; then put the caret
requirements back (`"0.2"`, or `"1"` from 1.0, when the release leaves the
range):

```bash
.github/scripts/bump-decdn.sh rewrite v<version> --exact
.github/scripts/bump-decdn.sh locked-crates | sed 's/^/--package=/' | xargs cargo update
.github/scripts/bump-decdn.sh rewrite v<version>
cargo metadata --locked --format-version 1 > /dev/null
.github/scripts/check-decdn-pin.sh
```

Without the `--exact` step, `cargo update` takes crates.io's newest release in
the range, which the e2e tag then does not match.

Land the move as its own PR before cutting a release against it, with a
changelog entry naming the new decdn release in every crate it reaches.

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
- **Contract-breaking** changes are called out explicitly: the installers'
  arguments, their state layout under `~/.decdn/sponsored/`, and both
  servers' HTTP APIs (`sponsord-api`) are read by commands already published,
  downloads in progress and clients already deployed.
- **Config-breaking** changes name the `SPONSORD_*` or `ONRAMP_*` variable
  (rename, new required variable, default shift).
- A move of the locked decdn names the new release in every crate it reaches.
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
   compares against a newer version. The first release has none, and nor
   does a `0.0.z` tag: Cargo treats every `0.0.z` bump as breaking, so the
   check has nothing to find.

Then two jobs run side by side:

1. **`verify`** re-runs `cargo fmt`, clippy and the test suite on the whole
   workspace; when there is a baseline, runs `cargo semver-checks
   --workspace` on every published crate's public API (default features)
   against it, failing if a change needs a bigger bump than the version says
   (in `0.y.z` with `y` above 0, a breaking change needs a minor bump; every
   `0.0.z` bump may break, hence no baseline there). To run it before tagging,
   when `release_plan.py <tag> --baseline` prints a tag, use
   `cargo semver-checks --workspace --baseline-rev <that tag> --default-features`.
   It then creates the GitHub Release as a **draft**, with git-cliff notes and
   the locked decdn release it was built against.
2. **`build`** runs one job per target, each building every binary released
   for it, and fails a Linux leg whose binaries need a glibc above 2.35
   (Ubuntu 22.04).

Only once both pass:

1. `upload-assets` attaches all 4 archives and one `SHA256SUMS` manifest,
   asserting all of them are present;
2. after it, for each image, `docker` assembles the multi-arch image from
   those archives — it does not compile from source, so the image's binary is
   byte-identical to the archived one — and pushes the manifest **untagged**,
   attaching the SBOM and `<image>-image-digest.txt`.

A release that fails verification therefore leaves no asset on the draft and
pushes no image.

One check decdn's release run has is absent here: `cargo publish --dry-run`
verify-builds every packaged crate, a second workspace build, so it runs in
`publish-crates.sh` instead.

Draft assets need authentication to download, and no image tag exists yet, so
nothing resolves by name until the release is signed.

## Signing and publishing

Once the run is green:

```bash
scripts/sign-release.sh v0.1.2
```

It resolves your signing key to a fingerprint — `SPONSORD_SIGNING_KEY` if set,
else your one secret key with a live `@decdn.org` uid that is in `KEYS`, else
your one secret key that is in `KEYS` at all; two or more candidates are
refused, not guessed between — and confirms `KEYS` publishes it and does not
mark it revoked or expired; force-fetches tags and checks your local tag matches `origin`'s;
verifies the tag signature; then:

- **archives:** downloads them, checks `SHA256SUMS` strictly against them,
  that every published archive appears in it, **and** that the release carries
  exactly the 4 archives the plan lists, then signs `SHA256SUMS`;
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
| `SPONSORD_SIGNING_KEY` | key to sign with. Unset, the script uses your one secret key with a live `@decdn.org` uid that is in `KEYS`, else your one secret key that is in `KEYS`. Set it when you hold two or more such keys (for example during a key rotation). gpg.conf's `default-key` is not read |
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

## Pinning decdn in the onramp

The installers install `decdn` from the decdn release the onramp pins: its
`vX.Y.Z` tag (`ONRAMP_DECDN_RELEASE`) and the SHA-256 of that release's
`SHA256SUMS` (`ONRAMP_DECDN_SUMS_SHA256`). The onramp refuses a pin that is
not `vX.Y.Z`, and passes the installers the version alongside the tag, so
they fetch `decdn-<version>-<target>.*`. Every download runs the script the
onramp serves at that moment, so a new pin reaches the next download.

## Publishing to crates.io

Last, once the GitHub Release is out of draft:

```bash
scripts/publish-crates.sh v0.1.2
```

It verifies the tag against `KEYS` and against `origin` the way
`sign-release.sh` does, and requires the release to be published **and** to
carry `SHA256SUMS.asc`. The decdn crates need no check: `Cargo.lock` at the
tag already resolved them from crates.io. The sibling crates go up in the
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
