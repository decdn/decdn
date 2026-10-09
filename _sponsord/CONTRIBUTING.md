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

## Unit test placement

Every unit-test module lives in its own file. Declare it in the source file and
put the body in a child file: `foo/tests.rs` for `foo.rs`, or `tests.rs` beside
a `lib.rs`, `main.rs` or `mod.rs`.

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
```

The rule has no size threshold. It covers every module gated only on test code:
`cfg(test)`, `cfg(all(test, …))`, and the `test_support` fakes gated on
`cfg(any(test, feature = "test-support"))`. The tests stay child modules, so
they keep access to private items. Do not move them to `crates/*/tests/`: an
integration test reaches only `pub` items.
`crates/e2e/tests/no_inline_test_modules.rs` parses every source file under
`crates/` and fails on any test module with an inline body. It runs in the
default `cargo nextest run`.

CodeQL skips these files by name (`.github/codeql/codeql-config.yml`):
`tests.rs`, `*_tests.rs`, `proptests.rs`, `test_support.rs` and
`tests_support.rs` under `src/`. The coverage job skips the same names. Name a
new test module to match, or its fixture keys raise alerts. The same test fails
if a file with one of those names is not a test module, so a production file
never drops out of the analysis.

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
