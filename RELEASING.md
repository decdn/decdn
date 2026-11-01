# Releasing deCDN

Releases are cut locally and signed with a maintainer's own GPG key. CI builds
the artifacts but signs nothing — there is no signing key in Actions secrets.
Once a maintainer has signed and published the release, CI uploads the crates
to crates.io through trusted publishing, which needs no stored credential
either.

A release is therefore two commands with a CI build between them, and a CI
publish after:

```bash
cargo release patch --execute              # cut, sign and push the tag
                                           # (the FIRST release is `minor`: 0.0.0 -> 0.1.0)
# ...wait for the tag-push run to finish...
scripts/sign-release.sh v0.1.2             # verify, sign, publish, tag images, open the devops bump
# ...publish-crates.yml then uploads the crates to crates.io
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

2. **A minisign key in `crates/cli/`.** `sign-release.sh` also signs
   `SHA256SUMS` with minisign, the signature `decdn self-update` checks. The
   binary trusts only the public keys committed as
   `crates/cli/minisign-<maintainer>.pub`, one file per maintainer, so the key
   you sign with must be yours among them. Generate the pair once and keep
   the secret half out of the repository:

   ```bash
   minisign -G -p crates/cli/minisign-<you>.pub -s ~/.minisign/minisign.key
   ```

   Commit the `.pub`, add its `include_str!` line to `TRUSTED_KEYS` in
   `crates/cli/src/commands/self_update/verify.rs` (a unit test keeps that
   list equal to the files), and add the key id beside your fingerprint in
   SECURITY.md. Installed binaries trust only the keys of the release they
   came from, so the release that first carries your key is signed by a
   maintainer whose key they already trust; yours signs from the release
   after it. `sign-release.sh` enforces this against the previous tag's
   keys. The script reads the secret key from `DECDN_MINISIGN_KEY`,
   defaulting to `~/.minisign/minisign.key`. See
   [The minisign keys](#the-minisign-keys) for adding, removing and rotating
   keys.

3. **Git configured to sign.** `cargo release` relies on git's own signing
   config; `release.toml` sets `sign-commit` and `sign-tag`. Set this
   explicitly — git otherwise selects a key by matching `user.email`, which may
   not be the key you published.

   ```bash
   git config user.signingkey <your-fingerprint>
   ```

4. **Tools.** `cargo install --locked cargo-release git-cliff@2.14.2` (the
   git-cliff version `release.yml` installs, so `CHANGELOG.md` and the release
   notes render alike), `minisign` (a package on every platform), plus
   `gh auth login`. Your minisign must sign in the
   prehashed format: `sign-release.sh` verifies with `-H` and stops on a
   legacy-format signature, which neither `decdn self-update` nor
   `cargo binstall` accepts. `python3` for the Docker Hub overview push.

5. **Registry logins.** `docker login ghcr.io` with a token carrying
   `write:packages`, and `docker login docker.io` — all four images go to both
   registries. The Docker Hub repositories `decdn/decdn-node`, `decdn/decdn`,
   `decdn/sponsord` and `decdn/sponsord-onramp` must exist first. The same Docker Hub login writes each
   repository's overview, through the Hub API. That API accepts a password or
   a personal access token with the *Read, Write, Delete* scope. If your
   `docker login` stored something else (a browser-based login can), set
   `DECDN_DOCKERHUB_USERNAME` and `DECDN_DOCKERHUB_TOKEN` to such a token.
   Skip the Docker Hub login with `DECDN_SKIP_DOCKERHUB=1` if you only need
   GHCR.

6. **Public GHCR packages.** The first push of an image creates its GHCR
   package as **private**, and a private package denies every anonymous
   `docker pull`. After the first release run, set `decdn-node` and `decdn`
   to public (package settings, *Change visibility*). `sign-release.sh`
   checks every promoted repository without credentials and refuses to
   publish while one is private.

   The `sponsord` and `sponsord-onramp` packages are named under the
   organisation, and a workflow may push to an org package only from a
   repository the package grants access to. Before the first release that
   ships them, an org admin adds `decdn/decdn` with the **Write** role under
   each package's *Manage Actions access* (package settings); otherwise the
   `docker` job fails with `permission_denied: write_package` and
   `sign-release.sh` refuses the draft for its missing digests.

7. **Push access to decdn/devops.** `sign-release.sh` ends by opening the
   devops PR that pins this release (see
   [The devops bump](#the-devops-bump)). Over https it needs a git credential
   helper, such as `gh auth setup-git`.

No step needs a crates.io token. The repository-level setup for the crates.io
upload is in [Trusted publishing](#trusted-publishing).

## Cutting a version

`cargo release <patch|minor|major> --execute` does all of the following, driven
by [`release.toml`](release.toml):

- bumps every crate in the workspace to the same version (`shared-version`)
- regenerates `CHANGELOG.md` with git-cliff (`scripts/release-changelog.sh`, the
  `pre-release-hook`), so the release commit carries the new version's section
- commits as `chore(release): v<version>`, **signed**
- tags `v<version>`, **signed**
- pushes both to `origin`

`release.toml` sets `publish = false`, so this step uploads nothing: it runs
before CI has built anything and before a human has verified anything, which is
the wrong moment to make an irreversible crates.io upload. The upload is the
separate, later `publish-crates.yml` run below.

Cut from `main`, with a clean tree and CI green on the commit you are tagging.

The workspace reads `0.0.0` until the first release is cut. That is a
statement, not a placeholder: `0.0.0` is never tagged, and the first cut is
`cargo release minor --execute`, which makes it `0.1.0`. From then on the
version moves only under `cargo release`. The `packaging` CI job and the
`workspace-manifests` pre-commit hook refuse a member that restates the
version or an internal `[workspace.dependencies]` alias that disagrees with
it, so a hand edit fails before it ships.

`CHANGELOG.md` is git-cliff output and nothing else writes it. The hook renders
the whole history, with the version being cut as the newest section, from
[`cliff.toml`](cliff.toml). The workflow renders the GitHub release notes from
the same config, so the two agree line for line, and a PR's squash-merge
subject is its line in both. The hook fails when `git-cliff` is not on `PATH`,
when `cliff.toml` is missing, or when the render has no section for the new or
the previous version. A preview run (no `--execute`) also runs the hook, with
`DRY_RUN=true`: it renders and checks the changelog but writes nothing, so those
failures show up in the preview, not after the version bump.

To preview a bump without changing anything, drop `--execute`:

```bash
cargo release patch --workspace --no-confirm
```

Dry-run is cargo-release's default — `--execute` is what makes it act — so the
preview writes nothing and needs no cleanup. Do **not** follow it with
`git reset --hard`; that only risks discarding unrelated work.

## What CI does with the tag

Pushing `v*` starts [`.github/workflows/release.yml`](.github/workflows/release.yml).
Steps 1–3 gate everything else. After them, the archive builds (step 6) run in
parallel with the verification (step 4). The draft (step 5) waits for the
verification, the asset upload waits for the draft and the builds, and the
image pushes wait for the asset upload. Every job after step 3 works on the
commit the tag push named; if the tag has since moved, the run fails rather
than build another commit.

Each job holds the narrowest token it needs. The jobs that compile or install
tools can only read. The three jobs that write to the draft (`draft`,
`upload-assets`, `attach`) use no action but GitHub's own `actions/*`. The
image push (`docker`) is the one job that can write packages, and it runs the
`docker/*` actions and the QEMU and BuildKit images under that token; it
cannot write release contents. No checkout keeps its token, every action is
pinned by commit SHA, and every image an action runs is pinned by digest.
`cross`, `git-cliff` and `cargo-nextest` install at fixed versions with a
checked download. The floating downloads are the `stable` toolchain and the
newest cargo-semver-checks release, which cargo-semver-checks-action fetches on
every run, under `verify`'s read-only token. The `packaging` CI job holds the pins, the checkouts and which job may
write with `.github/scripts/check-workflow-hardening.sh`. The workflow:

1. rejects the tag unless it is strict semver (the trigger glob is looser than
   it looks — `v0$(whoami)` matches it);
2. imports `KEYS` **from `origin/main`** and refuses the tag if it is not signed
   by a key published there, or if it no longer points at the commit that
   triggered the run;
3. checks all sixteen crate versions match the tag;
4. re-runs `cargo fmt`, `clippy` and the test suite; then
   `cargo publish --workspace --dry-run`, which packages and verify-builds
   every crate. That runs here, before the draft exists, because a packaging
   error found during the real publish has no clean recovery. Then
   `cargo semver-checks` against the previous tag — skipped for major bumps,
   for minor bumps while the major is `0`, and for the first release (no prior
   tag to diff against);
5. creates the GitHub Release as a **draft**, with notes git-cliff generated in
   the job that runs steps 1–3;
6. builds fifteen archives (`decdn-node` on five targets, `decdn` on six: the
   same five plus Windows ARM64; `sponsord` and `sponsord-onramp` on the two
   Linux targets) with `--locked`, one job per target, failing a Linux target
   whose binaries need glibc newer than the 2.31 floor
   ([ADR 000](adr/000-language.md)). Once verification passes it uploads them
   with a `SHA256SUMS` manifest, asserting all fifteen are present;
7. once the archives are uploaded, assembles four multi-arch images from the
   same Linux archives — `decdn-node` (the daemon, with the `decdn` CLI beside
   it), `decdn` (the CLI only), `sponsord` and `sponsord-onramp` (one server
   each). It does not compile from source, so every
   binary in an image is byte-identical to the archived one. It pushes each
   manifest **untagged**, scans it for an SBOM in a separate job that cannot
   write, and attaches each image's SBOM (`<stage>-<version>-sbom.spdx.json`)
   and digest (`<stage>-image-digest.txt`), named after the image's
   Dockerfile stage.

Draft assets need authentication to download, and no image tag exists yet, so
nothing resolves by name until the release is signed. Each image manifest is
fetchable by digest in that window — untagged is not unreachable — but the
digest is not advertised anywhere a user would look, and no tag ever points at
unsigned bytes.

## Signing and publishing

Once the run is green:

```bash
scripts/sign-release.sh v0.1.2
```

It resolves your signing key to a fingerprint — `DECDN_SIGNING_KEY` if set,
else your one secret key with a live `@decdn.org` uid that is in `KEYS`, else
your one secret key that is in `KEYS` at all; two or more candidates are
refused, not guessed between — and confirms `KEYS` publishes it and does not
mark it revoked or expired; force-fetches tags and checks your local tag matches `origin`'s;
verifies the tag signature; downloads the draft's assets; checks `SHA256SUMS`
strictly against them **and** that every published archive appears in the
manifest; signs `SHA256SUMS` and each image's digest file and SBOM; verifies
each signature against a keyring built only from `KEYS`; signs `SHA256SUMS` a
second time with minisign and verifies that against the
`crates/cli/minisign-*.pub` in the tag's tree, the maintainers' keys the
released binary embeds; in the same minisign run (one password prompt) signs
every archive for `cargo binstall` and verifies each `<archive>.minisig` against
the `pubkey` the crates name at the tag, and that its signed trusted comment
names that archive; uploads the nine `.asc` files,
`SHA256SUMS.minisig` and the archives' `.minisig` files; promotes `:latest`, `:<version>` and `:<major>.<minor>` from each
signed digest on GHCR; mirrors those same digests to Docker Hub; for a stable
release, pushes each Docker Hub overview; checks that every promoted repository
is pullable without credentials; and flips the release out of draft. It then
dispatches the crates.io upload on `main`
([Publishing to crates.io](#publishing-to-cratesio)), prints the command that
finds the run, and opens the devops bump PR
([The devops bump](#the-devops-bump)).

The mirror is a manifest copy, not a rebuild: `docker buildx imagetools create`
copies the manifest bytes verbatim, so `docker.io/decdn/decdn-node` and
`ghcr.io/decdn/decdn-node` serve one identical digest, and the single signature
over `decdn-node-image-digest.txt` vouches for both. The same holds for
`decdn/decdn` and `decdn-image-digest.txt`. The script re-reads every tag on both
registries afterwards and refuses to publish if any resolves to a different
digest, so this is checked rather than assumed.

Docker Hub does not read an image's OCI labels, and the mirror copies only the
manifest. Each Hub repository's short description and overview therefore come
from [`docker/README.<stage>.md`](docker/): line 1 holds the short description
as `<!-- description: … -->` (100 characters at most), and the rest is the
overview. The script pushes the files as committed at the tag, then reads each
repository back without credentials and compares. A prerelease leaves the
overviews alone, as it leaves `latest` alone. A failed push only warns: the
release goes on, and the script prints, at the failure and again at the end,
the command that repeats the push:

```bash
scripts/dockerhub-description.py --ref v0.1.2 \
  decdn/decdn-node=decdn-node decdn/decdn=decdn
```

To push an edit to an overview between releases, run the same command with
`--ref origin/main` after `git fetch`. The Hub page then describes `main`
rather than the image `latest` serves, so keep such an edit true of the
current release.

Useful environment overrides:

| Variable | Effect |
|----------|--------|
| `DECDN_SIGNING_KEY` | key to sign with. Unset, the script uses your one secret key with a live `@decdn.org` uid that is in `KEYS`, else your one secret key that is in `KEYS`. Set it when you hold two or more such keys (for example during a key rotation). gpg.conf's `default-key` is not read |
| `DECDN_MINISIGN_KEY` | your minisign secret key, which signs `SHA256SUMS` for `decdn self-update` and each archive for `cargo binstall` (default `~/.minisign/minisign.key`). Its public half must be one of the `crates/cli/minisign-*.pub` maintainer keys, and the `pubkey` the binstall crates name at the tag |
| `DECDN_SKIP_IMAGE_TAGS` | set to `1` to publish with **no** pullable image tag at all — the release then ships only the signed digests |
| `DECDN_SKIP_DOCKERHUB` | set to `1` to tag on GHCR only, with no Docker Hub mirror or overview (implied by `DECDN_SKIP_IMAGE_TAGS`) |
| `DECDN_DOCKERHUB_REPO` | the Docker Hub counterpart of `DECDN_REPO` (defaults to it); the images are `<it>-node` and `<it>` |
| `DECDN_DOCKERHUB_USERNAME`, `DECDN_DOCKERHUB_TOKEN` | a Docker Hub personal access token for the overview push, used instead of the `docker login` credential |
| `DECDN_SKIP_DEVOPS_BUMP` | set to `1` to open no devops bump PR after publishing |
| `DECDN_REPO` | target a fork instead of `decdn/decdn` — both registries follow it |

The skip variables take `1`/`true`/`yes` or `0`/`false`/`no`; anything else is
rejected rather than guessed, so `DECDN_SKIP_DOCKERHUB=0` means *don't* skip.

Both image repositories derive from `DECDN_REPO`, so a fork rehearsal stays
entirely on the fork. Point `DECDN_DOCKERHUB_REPO` somewhere else only if the
Docker Hub namespace genuinely differs from the GitHub one.

For a stable version the promoted tags are `latest`, `<version>` and
`<major>.<minor>`. A prerelease (`v1.2.0-rc1`) gets only its exact version tag:
moving `latest` to a candidate would hand it to every unpinned pull, and
`<major>.<minor>` would clobber the stable minor tag. It is also published with
`--prerelease`, so it is never GitHub's "latest" release (what
`decdn self-update` resolves) and the devops release-line resolver, which drops
any release flagged pre-release, skips it.

Re-running is safe at any point before the release is published — signatures
are re-uploaded with `--clobber` and the tag promotion is idempotent. Once the
release is out of draft the script refuses to run again; re-signing a published
release means deleting and re-cutting it.

On failure the script keeps its download directory and prints the path, so you
can inspect the bytes rather than re-downloading several hundred megabytes.

### The devops bump

Last, once the release is published, the script opens the PR that pins the
downstream repository to this release. It clones
[decdn/devops](https://github.com/decdn/devops) into a temporary directory and
runs its `scripts/open-decdn-bump.sh <tag>`, which moves the Ansible role, Helm
chart, Compose, sponsord and onramp pins. It takes the `SHA256SUMS` digest and
the `decdn-node`, `sponsord` and `sponsord-onramp` image digests from the
release's assets once their signatures verify against its vendored `KEYS`, regenerates its mirrors from the tag, and lists in the PR
body what a reviewer still has to do. It needs `python3`, gpg and
`sha256sum`, and opens nothing for a pre-release or when `DECDN_REPO` is not
`decdn/decdn`.

It commits as your git identity, pushes with your git credentials for
github.com, and opens the PR with your `gh` login. It needs push access to
devops and, over https, a git credential helper such as `gh auth setup-git`.

When it fails, or you press Ctrl-C while it runs, the script warns and says how
to open the PR later; it never fails the script, since the release is already
out. It runs devops's `main` as you, without the cargo
registry tokens in the environment. The devops ruleset requires a reviewed PR
for a change to `main`, but org admins can bypass it. To run the bump script
and its helpers from a checkout you have reviewed instead, set
`DECDN_SKIP_DEVOPS_BUMP=1`.

## The minisign keys

`decdn self-update` trusts a release only when `SHA256SUMS.minisig` verifies
against a key the running binary embeds: the `crates/cli/minisign-*.pub`
files, one per maintainer, included at build time. A release signed by any
of them is accepted, the rule `KEYS` sets for GPG. GPG stays the channel
people verify by hand ([SECURITY.md](SECURITY.md#verify-a-binary-archive));
minisign is the one the binary carries, because a full OpenPGP implementation
has no place in the client crate graph.

A binary can only ever accept a key it already embeds, so every change to the
set reaches users through a release:

- **Adding a maintainer** is the one-time setup step above. The release that
  introduces their key is signed by a key already in the field; theirs signs
  from the release after it. `sign-release.sh` refuses a signature that no
  key in the previous tag's tree verifies.
- **Removing a maintainer** deletes their file and its `TRUSTED_KEYS` entry.
  Binaries that never update keep trusting the removed key, as they keep an
  old `KEYS`; the release that drops it is the one to call out in its notes.
- **Rotating your own key** is an add followed by a remove: commit the new
  file, cut and sign a release with the **old** key so every installed binary
  accepts it and the binary it ships trusts both, sign the releases after it
  with the new key, and delete the old file once no supported binary predates
  that release. A binary older than it refuses the newer releases ("not
  signed by a key this decdn build trusts"); say in the release notes that
  such binaries must reinstall by hand.

The script verifies the signature twice: against the `minisign-*.pub` files
in the tag's tree, which are exactly the keys the binary being released
embeds, and against those in the previous tag's tree, which are what the
binaries in the field embed. A release that fails either stops before
anything is uploaded.

### `cargo binstall` and the crates' `pubkey`

`cargo binstall` never reads `SHA256SUMS`. It checks each archive against its
own `<archive>.minisig`, under the one key in the crate's
`[package.metadata.binstall.signing] pubkey`. binstall reads that key from the
`Cargo.toml` crates.io serves for the version, which is the one at the tag.
`decdn-cli`, `decdn-node`, `sponsord` and `sponsord-onramp` each carry the
field. It names the key line (the second line) of the releasing maintainer's
`crates/cli/minisign-*.pub`, on one line of the form `pubkey = "<key>"`.

The field holds one key, not a set, so it follows whoever signs. The rules
above still decide which key may sign a release; this field only names it:

- **A release signed by another maintainer** changes `pubkey` in all four
  manifests to that maintainer's key **before** the tag is cut. Their key must
  already be in the previous tag's tree, as for `SHA256SUMS.minisig`.
- **Rotating your own key**: the release that adds your new key is signed with
  the old one, so its `pubkey` stays on the old key. The first release you
  sign with the new key changes `pubkey`, before you tag it.
- **Removing a maintainer** whose key the crates name moves `pubkey` to a
  remaining maintainer's key in the same change; the guard below refuses a
  `pubkey` that no `minisign-*.pub` file holds.

`sign-release.sh` reads `pubkey` from the tag's tree before it signs anything.
It refuses when the crates disagree or name none, and when the archive
signatures do not verify under that key, so a mismatch stops before anything
is uploaded. `.github/scripts/check-binstall-metadata.sh` checks the field on
every PR, and `publish-crates.yml` runs it again at the tag before anything
reaches crates.io: the four crates name the same key, the key is one of the
`minisign-*.pub` files, it sits on the one line form `sign-release.sh` reads,
and no override carries a `signing` table of its own.

## Publishing to crates.io

[`.github/workflows/publish-crates.yml`](.github/workflows/publish-crates.yml)
uploads the fourteen publishable crates. Once the release is out of draft,
`sign-release.sh` dispatches it on `main` with the tag as its input, using your
`gh` login. No crates.io token exists outside the run: crates.io [trusted publishing](https://crates.io/docs/trusted-publishing)
trades the job's GitHub OIDC token for a crates.io token that lasts 30 minutes,
and the auth action revokes it when the job ends.

The checks below hold only if the workflow that runs them is the reviewed one,
since the workflow is what holds the crates.io token:

- The `release` environment allows deployments from `main` only. A run from a
  tag or another branch runs that ref's copy of `publish-crates.yml`, and never
  gets the token: crates.io matches the workflow by file name and environment,
  not by ref.
- The workflow, `KEYS` and every script and action it calls come from the
  `main` commit the run started on. The tag is checked out beside it, in
  `src/`, and only packaged.

`main` itself is guarded by the `Protect Main` ruleset, which org admins
bypass.

`decdn-e2e` (test fixtures) and `sponsord-e2e` (the sponsord stack tests) are
`publish = false` and cargo skips them. The workspace has sixteen members, and
the CI version check covers all sixteen — only the upload is fourteen.

The `prepare` job uploads nothing. It:

1. checks the run is on `main`, that the tag is strict semver, and verifies the
   tag's signature against `KEYS` **from `main`**, as `release.yml` does;
2. runs `.github/scripts/check-release-signed.sh`. The release must be out of
   draft. `SHA256SUMS.asc` must be a good signature, against that same `KEYS`,
   by a key that has neither expired nor been revoked. Every archive
   `SHA256SUMS` names must carry this version, and must be attached with its
   `.minisig`, as must `SHA256SUMS.minisig`. Leaving draft does not by itself
   prove anything was signed, so `SHA256SUMS.asc` is verified again;
   `sign-release.sh` verified the minisign signatures, and this checks they are
   attached;
3. runs `check_binstall_metadata.py` at the tag;
4. runs `.github/scripts/crates_to_publish.py`. Every crate must be at the
   tag's version and must already exist on crates.io (trusted publishing
   cannot create a crate). The crates crates.io already serves at the version
   are excluded from the upload. An answer from crates.io other than 200 or
   404, or a 200 whose JSON does not describe the crate asked about, stops the
   run;
5. builds the `decdn dashboard` page, which `decdn-cli`'s crate carries, and
   runs `cargo publish --workspace --dry-run`, the verify build, on what is
   left.

The `publish` job alone holds `id-token: write`, in the `release` environment.
Any step in that job could ask for an OIDC token, so it runs no build script,
proc macro or package install: past the pinned toolchain and auth actions, it
takes the dashboard build from `prepare` and runs
`cargo publish --workspace --no-verify`, which resolves the dependency order
and waits for each crate to reach the index before its dependents. It then
asks crates.io for every crate, for up to five minutes, and warns about any it
does not serve yet (normally index lag) or could not ask about.

Pre-releases go to crates.io too: `sign-release.sh` dispatches the upload for
every release, so `v1.2.0-rc1` is uploaded as `1.2.0-rc1`. cargo resolves a
pre-release version only for a requirement that names a pre-release, but the
version is as immutable as any other.

**Re-running is safe.** A crates.io version is immutable, so a run that fails
partway leaves the crates it uploaded published. Re-run all jobs: the run
excludes those crates and uploads the rest. (Re-running only `publish` works
for seven days, while `prepare`'s dashboard build is kept.) To start it again
from scratch — after a fix to the workflow lands on `main`, or when
`sign-release.sh` could not dispatch it — dispatch it on `main`:

```bash
gh workflow run publish-crates.yml --ref main -f tag=v0.1.2
```

A failure here does not invalidate the release. The GitHub Release, the
signatures and the container images are all already published and stand on
their own; crates.io is an additional distribution channel, so the recovery is
to finish the remaining crates, not to re-cut the version.

### Trusted publishing

crates.io trusts `publish-crates.yml` per crate. Set this up once, and again
for each new crate:

1. **The `release` environment.** Under *Settings → Environments*, it allows
   deployments from the `main` branch only, with no required reviewers: your
   signature on the release is the human gate. crates.io matches the workflow
   by file name only, so this one ref is what keeps a tag's or a branch's copy
   of the workflow from publishing. Add no tag pattern.

   ```bash
   gh api -X PUT repos/decdn/decdn/environments/release \
     -F 'deployment_branch_policy[protected_branches]=false' \
     -F 'deployment_branch_policy[custom_branch_policies]=true'
   gh api repos/decdn/decdn/environments/release/deployment-branch-policies \
     -f name=main -f type=branch
   ```

2. **A trusted publisher on each crate.** On crates.io, under each crate's
   *Settings → Trusted Publishing*, add GitHub with owner `decdn`, repository
   `decdn`, workflow `publish-crates.yml` and environment `release`. An
   individual owner of the crate does this; see
   [Crate ownership](#crate-ownership).

3. **Trusted publishing only.** Once a release has gone up through the
   workflow, turn on *Require trusted publishing* in each crate's settings, so
   that no API token can publish a version of it.

**A new crate** cannot go up through the workflow: trusted publishing needs the
crate to exist, and `crates_to_publish.py` stops the run, before any upload,
when one does not. Publish the new crate's first version by hand, ahead of the
release that first carries it, with a personal token (`cargo login`, scoped to
publish-new):

```bash
cargo publish -p <new-crate> --locked
```

Then hand it to the team (below) and give it the trusted publisher and the
setting above. crates.io limits crate *creation* far harder than updates (a
burst of 5, then roughly one per 10 minutes), so space out several new crates.

### Crate ownership

crates.io gives a new crate to whoever publishes the name first. Hand each new
crate to the org straight afterwards, in the same sitting.

The new owner has to be a GitHub team: crates.io has no organisation account,
so a team is the only owner that outlives an individual. The `crates-io` team
under the `decdn` org is that owner. Confirm you are in it, then:

```bash
cargo owner --add github:decdn:crates-io <new-crate>
```

crates.io resolves the team through your own GitHub authorisation, so it fails
unless you are a member of `crates-io`. The team is `closed`, not secret, so
crates.io can read it with the `read:org` scope it asks for at login.

A team owner publishes updates but cannot change the owner list or a crate's
trusted publishers. Only an individual owner does that, so do not remove
yourself once the team is added — that would leave nobody able to grant or
revoke access again. `cargo owner --list <crate>` confirms the result.

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
untagged manifest in each GHCR package. So does re-running `upload-assets` or
`docker` before signing: the image job pushes fresh manifests, `attach` replaces
the `<stage>-image-digest.txt` files, and the earlier digests stay behind
untagged. They are not pullable by name and cost only
storage, so cleanup is optional. If you do delete one, take care: GHCR keys a
package version by digest and treats tags as metadata on it, so deleting the
wrong version removes the image behind every tag pointing at it, `latest`
included. Match the digest against that image's digest file before deleting anything.

**Crates are already on crates.io.** Then the version is spent: it cannot be
re-cut, because `cargo publish` will refuse to replace it and no amount of
deleting tags or releases frees it. Do not delete the tag. Yank the affected
crates (`cargo yank`, or each crate's page on crates.io) so nothing new
resolves to them, then cut the *next* patch version with the fix. This is why
the crates.io upload is the last irreversible step, after every other artifact
is verified and published — everything before it is reversible.

## Before open-sourcing

Two of this workflow's protections are **not enforceable while the repository is
private**, because branch and tag protection are unavailable on the current plan
— the API returns `403 Upgrade to GitHub Pro or make this repository public`.
Making the repository public enables both. Configure them at that point:

1. **Tag protection on `v*`.** Without it, anyone with push access can create a
   release tag. Worse, a `push`-triggered workflow runs the workflow definition
   *from the pushed ref*, so a tag can carry a `release.yml` with the signature
   gate deleted, and that workflow can still request `contents: write` and
   `packages: write`.
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
