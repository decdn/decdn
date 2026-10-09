//! The committed `OpenAPI` documents under `docs/openapi/` match the types.
//! `UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi` rewrites them.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

fn check(name: &str, doc: &utoipa::openapi::OpenApi) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/openapi")
        .join(format!("{name}.json"));
    let fresh = format!("{}\n", doc.to_pretty_json().unwrap());
    if std::env::var_os("UPDATE_OPENAPI").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &fresh).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == fresh,
        "{} is stale; regenerate with UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi",
        path.display()
    );
}

#[test]
fn daemon_document_is_current() {
    check("sponsord", &sponsord_api::openapi::daemon());
}

#[test]
fn onramp_document_is_current() {
    check("sponsord-onramp", &sponsord_api::openapi::onramp());
}
