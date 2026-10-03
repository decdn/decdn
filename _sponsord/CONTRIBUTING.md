# Contributing to sponsord

## Toolchain

`rust-toolchain.toml` pins Rust 1.95 with `rustfmt` and `clippy`; a
rustup-managed `cargo` installs it on first use. The anvil-backed tests also
need [Foundry](https://book.getfoundry.sh) (`anvil`, `forge`) on `PATH`.

## Checks

Run these before opening a pull request; CI runs the same:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# Treasury against a local anvil chain (needs anvil + forge):
cargo test -p sponsord-core --features anvil-e2e
```

The workspace denies `unwrap`, `expect`, `panic!` and slice indexing outside
tests (`[workspace.lints.clippy]`). `cargo deny check` audits licenses and
advisories.

## Building against a local deCDN checkout

The deCDN crates come from `github.com/decdn/decdn` at the commit pinned in
`Cargo.lock`. Move the pin with:

```bash
cargo update -p decdn-incentive
```

To build against a local checkout instead (say `../decdn`), create an
untracked `.cargo/config.toml`:

```toml
[patch."https://github.com/decdn/decdn"]
decdn-client = { path = "../decdn/crates/client" }
decdn-incentive = { path = "../decdn/crates/incentive" }
decdn-e2e = { path = "../decdn/crates/e2e" }
```

The override rewrites those packages' sources in `Cargo.lock`; don't commit
that. Delete the file and run `cargo metadata` to get the pinned lock back.

## Commits and pull requests

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `refactor:`, ...). Add a line under
`## [Unreleased]` in `CHANGELOG.md` for any user- or operator-visible change.

By contributing you agree that your contributions are licensed under the
project's dual MIT OR Apache-2.0 license.
