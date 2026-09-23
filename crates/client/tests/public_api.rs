//! Public-API surface snapshot for `decdn-client`, the client SDK (#1150).
//!
//! The CLI and the node's cache-miss leg both build on this crate, and a third
//! party builds on the same surface. The snapshot below captures that surface;
//! any diff to it fails CI and shows up as a reviewable `.snap` change. An item
//! earns a place in it only when a caller outside the crate needs it. Anything
//! else stays `pub(crate)`.
//!
//! The snapshot renders the default feature set. The `test-util` doubles are not
//! part of the SDK contract, so they stay out of it.
//!
//! Gated behind the `public-api-test` feature (see this crate's `Cargo.toml`)
//! so it stays out of the default `cargo nextest --workspace` coverage run. Run
//! or update it with:
//!
//! ```text
//! INSTA_UPDATE=always cargo nextest run -p decdn-client --features public-api-test
//! ```
//!
//! The toolchain notes in `crates/protocol/tests/public_api.rs` apply here too:
//! the rustdoc JSON comes from the pinned stable toolchain via
//! `RUSTC_BOOTSTRAP=1`, so a toolchain bump can require a `public-api` bump and a
//! regenerated `.snap` in the same PR.

use std::error::Error;

#[test]
fn public_api() -> Result<(), Box<dyn Error>> {
    let rustdoc_json = rustdoc_json::Builder::default()
        // Use the active (pinned stable) toolchain; `RUSTC_BOOTSTRAP=1` lets it
        // accept the otherwise nightly-only `--output-format json`.
        .clear_toolchain()
        .env("RUSTC_BOOTSTRAP", "1")
        .build()?;

    let public_api = public_api::Builder::from_rustdoc_json(rustdoc_json)
        .omit_auto_derived_impls(true)
        .omit_auto_trait_impls(true)
        .omit_blanket_impls(true)
        .build()?;

    insta::assert_snapshot!(public_api.to_string());

    Ok(())
}
