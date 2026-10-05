# Contributing to sponsord

## Toolchain

`rust-toolchain.toml` pins Rust 1.95.0 with `rustfmt` and `clippy`; a
rustup-managed `cargo` installs it on first use. The pin lives in three kinds
of place, each read by a different tool: `rust-toolchain.toml`'s channel, the
root `Cargo.toml`'s `rust-version`, and every `dtolnay/rust-toolchain@…` ref
under `.github/`. Move them together in one change;
`.github/scripts/check-toolchain-pin.sh` fails CI when they drift. The MSRV is
the pinned toolchain.

Also useful: [`cargo-nextest`](https://nexte.st) (the test runner CI uses),
[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny),
[pre-commit](https://pre-commit.com), and [Foundry](https://book.getfoundry.sh)
(`anvil`, `forge`) for the chain tests.

## Checks

CI runs everything behind one required `CI success` check. Locally:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace          # or: cargo test --workspace
# Against a local anvil chain (needs anvil + forge): the pool client and
# Sponsor::connect, then the sponsord binary end to end.
cargo nextest run --features sponsord-core/anvil-e2e,sponsord/anvil-e2e \
  -p sponsord-core -p sponsord --test chain_pool --test binary_anvil
pre-commit install --hook-type pre-commit --hook-type pre-push
```

The workspace denies `unwrap`, `expect`, `panic!` and slice indexing outside
tests (`[workspace.lints.clippy]`). `.github/scripts/cargo-deny-warn.sh` runs
`cargo deny check` the way CI does, and `python3 -m pytest .github/scripts/tests`
tests the release tooling.

If you change a type in `sponsord-api`, regenerate the OpenAPI documents:
`UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi`.

## Building against a local deCDN checkout

The deCDN crates are git dependencies on `github.com/decdn/decdn`, at the
commit `Cargo.lock` pins ([RELEASING.md § Pinning decdn](RELEASING.md#pinning-decdn)
covers moving it). To build against a local checkout instead (say
`../decdn`), create an untracked `.cargo/config.toml`:

```toml
[patch."https://github.com/decdn/decdn"]
decdn-client = { path = "../decdn/crates/client" }
decdn-common = { path = "../decdn/crates/common" }
decdn-incentive = { path = "../decdn/crates/incentive" }
decdn-e2e = { path = "../decdn/crates/e2e" }
```

The override rewrites those packages' sources in `Cargo.lock`; don't commit
that. Delete the file and run `cargo metadata` to get the pinned lock back.

## Pull requests

PR titles follow [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `refactor:`, ...; the `pr-title` check enforces
it): the squash-merge subject becomes the commit git-cliff builds the release
notes from. User- or operator-visible changes get an entry under
`## [Unreleased]` in the changed crate's `crates/<dir>/CHANGELOG.md` in the
same PR ([RELEASING.md § Changelogs](RELEASING.md#changelogs)).

By contributing you agree that your contributions are licensed under the
project's dual MIT OR Apache-2.0 license.
