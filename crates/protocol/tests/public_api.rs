//! Public-API surface snapshot for `decdn-protocol` (issue #304).
//!
//! `decdn-protocol` is the leaf crate every other workspace crate (and,
//! eventually, external consumers) depends on — a silent change to its exported
//! wire types or ALPN definitions is exactly the regression this guards against.
//! The snapshot below captures the crate's public surface; any diff to it fails
//! CI and shows up as a reviewable `.snap` change.
//!
//! Gated behind the `public-api-test` feature (see this crate's `Cargo.toml`)
//! so it stays out of the default `cargo nextest --workspace` coverage run: it
//! shells out to `cargo rustdoc` to build the JSON and is a touch slow. Run /
//! update it with:
//!
//! ```text
//! INSTA_UPDATE=always cargo nextest run -p decdn-protocol --features public-api-test
//! ```
//!
//! ## No nightly toolchain
//!
//! `public-api` parses rustdoc's JSON output, which is nominally a nightly-only
//! feature (`-Z unstable-options --output-format json`). We avoid pulling a
//! nightly into the project by generating the JSON with the workspace's pinned
//! **stable** toolchain (`rust-toolchain.toml`, currently 1.95.0) and unlocking
//! the unstable flag via `RUSTC_BOOTSTRAP=1` — passed only to the spawned
//! `cargo rustdoc`, not to anything else. This is deterministic and *more*
//! stable than tracking `nightly`: the rustdoc-JSON `format_version` is frozen
//! to whatever our pinned stable emits (currently 57, which matches the
//! `rustdoc-types` 0.57.x that `public-api` parses). The coupling to watch: a
//! deliberate workspace toolchain bump can bump `format_version`, which may
//! require bumping `public-api` and regenerating this `.snap` in the same PR.
//!
//! Written with `?` / `Result` rather than the upstream `.unwrap()` example so
//! it complies with the workspace anti-panic clippy lints (`unwrap_used` et al.)
//! without a per-module allow.

use std::error::Error;

#[test]
fn public_api() -> Result<(), Box<dyn Error>> {
    let rustdoc_json = rustdoc_json::Builder::default()
        // Use the active (pinned stable) toolchain; `RUSTC_BOOTSTRAP=1` lets it
        // accept the otherwise nightly-only `--output-format json`.
        .clear_toolchain()
        .env("RUSTC_BOOTSTRAP", "1")
        .all_features(true)
        .build()?;

    let public_api = public_api::Builder::from_rustdoc_json(rustdoc_json)
        .omit_auto_derived_impls(true)
        .omit_auto_trait_impls(true)
        .omit_blanket_impls(true)
        .build()?;

    insta::assert_snapshot!(public_api.to_string());

    Ok(())
}
