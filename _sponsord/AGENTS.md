# AGENTS.md

This file provides guidance to coding agents working in this repository.

sponsord lets a publisher pay for their users' deCDN downloads. It is a cargo
workspace (Rust 2024, toolchain pinned to 1.95.0 in `rust-toolchain.toml`)
that depends on [decdn/decdn](https://github.com/decdn/decdn) through git
dependencies. Read `docs/architecture.md` first. It covers the flow and the
trust boundaries, and this file does not repeat them.

## Commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace                     # CI's runner; `cargo test --workspace` also works
cargo test --doc --workspace                      # nextest skips doctests; CI runs them separately
RUSTDOCFLAGS=-D\ warnings cargo doc --workspace --no-deps --document-private-items

# One test: pick the package, the test binary and a name filter
cargo nextest run -p sponsord-onramp --test http_contract <name_substring>
cargo test -p sponsord-core issuer::            # unit tests in one module

# Chain tests against a local anvil (needs `anvil` and `forge` on PATH)
cargo nextest run --features sponsord-core/anvil-e2e,sponsord/anvil-e2e \
  -p sponsord-core -p sponsord --test chain_pool --test binary_anvil

# After changing any type in sponsord-api: regenerate docs/openapi/*.json
UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi

# Release/CI tooling (Python) and the repo consistency checks CI runs
python3 -m pytest .github/scripts/tests
.github/scripts/check-{toolchain-pin,workspace-manifests,decdn-pin,package-embeds,image-name}.sh
.github/scripts/cargo-deny-warn.sh
```

If the anvil tests time out, the cause is usually the first cold compile of
decdn's contracts. CI builds them with `forge build` in cargo's git checkout of
decdn before it runs the tests (see the `anvil-e2e` job in `ci.yml`).

## Crates and how they fit

- **`sponsord-api`** holds the wire contract: request and response bodies,
  route paths, `ErrorCode`, and `MicroUsdc`. Its features add the
  `DaemonClient`/`OnrampClient` (`client`), axum `IntoResponse` (`axum`),
  OpenAPI generation (`openapi`) and `Secret` (`secret`). Every other crate
  depends on it. The committed OpenAPI documents are snapshot-tested against
  it.
- **`sponsord-core`** is the daemon's logic as a library. `Issuer` turns
  `Limits` and a `TermsRequest` into `Terms` and signs the EIP-712
  capability. `Sponsor` ties the issuer to a `PoolChain` (`ChainPool` in
  production, `test_support::FakePool` in tests), and `keeper` tops the pool
  up when it falls below a low-water mark. `Sponsor::issue` re-signs the
  terms already registered on-chain for a known signer, and refuses with
  `signer_expired` once that registration has expired.
- **`sponsord`** is the daemon binary: config from `SPONSORD_*` env vars,
  an axum bearer-token HTTP API (`http.rs`), and metrics. It is stateless and
  makes no decision about who gets a capability.
- **`sponsord-onramp`** is the public server. A `Gate` trait (only
  `TurnstileGate` is built in) sits in front of a `CapabilitySource` (the
  daemon client). `GrantCache` is an in-memory hand-off from the browser's
  `POST /v1/fund` to the CLI's polling `GET /v1/capability`. Installers are
  rendered once at startup from `assets/decdn.{sh,ps1}`, with the onramp URL
  and the pinned release tags and `SHA256SUMS` digests baked in. Config comes
  from `ONRAMP_*` env vars.
- **`decdn-sponsored`** is the end-user CLI (Linux, macOS and Windows). It
  creates a throwaway key per download under `~/.decdn/sponsored/`, polls the
  onramp, then spawns `decdn bundle pull` with the capability. The spawned
  `decdn` handles discovery, payment and verification.
- **`crates/e2e`** (`sponsord-e2e`, never released) runs the real daemon
  router behind the real onramp, driven by the CLI's pull flow, against
  `FakePool`.

`test-support` features expose fakes (`FakePool`, `fake_sponsor`,
`FakeCapabilitySource`, `FakeGate`, `test_config`). Crates enable them for
their own tests through a path dev-dependency on themselves. Use these fakes
rather than writing new ones.

## Constraints that aren't obvious from the code

- **Lints**: the workspace runs decdn's lint set (root `Cargo.toml`
  `[workspace.lints]`): clippy `pedantic`, `deny` on `unwrap`, `expect`,
  `panic!`, slice indexing, truncating casts and `print!`/`eprintln!`
  outside tests, `unsafe_code` forbidden, and a doc comment on every public
  item (on `sponsord-api` types those comments are the OpenAPI
  descriptions, so regenerate the snapshots after editing them). The
  `decdn-sponsored` CLI allows the print lints at its crate roots. Silence a
  lint at the site with `#[expect(…, reason = "…")]`. Every member must keep
  `[lints] workspace = true` (CI checks this).
- **Unit tests live in their own file**: every test module is declared
  `mod <name>;` with its body in `foo/<name>.rs` for `foo.rs`, or `<name>.rs`
  beside a `lib.rs`/`main.rs`/`mod.rs`. Never inline, and never in
  `crates/*/tests/` for tests that need private access. A test module is one
  gated on `test` or `feature = "test-support"` only. CodeQL and coverage skip
  these files by name (`tests.rs`, `*_tests.rs`, `proptests.rs`,
  `test_support.rs`, `tests_support.rs`), so name a new one to match.
  `crates/e2e/tests/no_inline_test_modules.rs` fails on an inline body, and on
  a file with one of those names that is not a test module.
- **Contracts with deployed software**: the release archive names, the
  `~/.decdn/sponsor.toml` schema and both HTTP APIs (`sponsord-api`) are read
  by installers and CLIs already in the field. A change to any of them must be
  called out as contract-breaking in the changelog. If the onramp and the CLI
  exchange something new, `ONRAMP_MIN_CLI_VERSION` controls when older CLIs
  are told to upgrade.
- **Single-tenant and stateless**: each publisher runs their own daemon and
  pool, so there are no per-caller budgets or tokens. The onramp keeps nothing
  on disk, and CI fails if a Docker image declares a volume.
- **`include_str!` must stay inside its crate**. A published `.crate`
  contains only its own directory, so an embed that reaches outside it builds
  in-tree but breaks `cargo install` (`check-package-embeds.sh`).
- **One version for the workspace**. Every member has
  `version.workspace = true`, and each internal alias in the root
  `[workspace.dependencies]` carries that version. `cargo release <level>`
  releases every crate together under one signed `vX.Y.Z` tag (see
  `RELEASING.md`). Changelogs stay per crate.
- **The decdn pin**: `Cargo.lock` decides which decdn commit gets built. The
  `version` on each `decdn-*` git dependency must equal decdn's workspace
  version at that commit (`check-decdn-pin.sh`). To move it, run
  `cargo update -p decdn-client -p decdn-common -p decdn-incentive -p decdn-e2e`
  in a PR of its own. To build against a local `../decdn`, use an untracked
  `.cargo/config.toml` `[patch]` (see `CONTRIBUTING.md`), and do not commit
  the lockfile changes it causes.
- **The toolchain version is written in four places**:
  `rust-toolchain.toml`, `rust-version` in the root `Cargo.toml`, every
  `dtolnay/rust-toolchain@…` ref under `.github/`, and the MSRV badge in
  `README.md`. Change all four together. CI checks only the first three.

## Pull requests

- PR titles use Conventional Commits with a lowercase subject (`feat:`,
  `fix:`, `docs:`, `chore:`, `refactor:`, `perf:`, `test:`, `build:`, `ci:`,
  `revert:`). The squash subject feeds git-cliff's release notes.
- A change users or operators can see needs an entry under `## [Unreleased]`
  in `crates/<dir>/CHANGELOG.md` for each crate it touches, following
  `RELEASING.md` § Changelogs.
- Markdown is linted (`.markdownlint.yaml`).
- `.github/pull_request_template.md` has the verification checklist.
