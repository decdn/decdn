//! Source guard: no crate opens a QUIC connection that carries early data.
//!
//! Every ALPN this workspace speaks — `cdn/probe/v1` included — completes a
//! full handshake before any application byte is written. That property cannot
//! be asserted at the wire, and the gap is a property of the transport, not of
//! this repo: iroh sets `max_early_data_size = u32::MAX` on every server TLS
//! config, so a peer's early data is accepted regardless of what the handler
//! does, and a server that accepts a stream *after* its handshake completes
//! cannot tell whether the bytes on it were sent early. (Measured on loopback,
//! `RecvStream::is_0rtt` reported deliberately-sent early data as ordinary
//! stream data in the large majority of runs — it loses the race against
//! handshake completion, so it cannot be a gate.)
//!
//! What is left is the emitter side, and there the invariant is nearly
//! type-level already: `Endpoint::connect` hands back a `Connection`, and iroh
//! builds one only by driving the handshake to completion or by going through
//! `into_0rtt`. Naming iroh's two 0-RTT entry points and asserting that no
//! source file mentions them closes the remaining gap.
//!
//! Note what this guard deliberately does *not* police: `Endpoint::max_tls_tickets`
//! sizes the `rustls` client session cache, which backs ordinary 1-RTT session
//! resumption — the thing that survives 0-RTT's removal. Banning it here would
//! forbid tuning a mechanism this workspace still relies on.

use std::path::{Path, PathBuf};

/// Repo-relative path of this file, used to exclude it from its own scan.
/// `Path::ends_with` matches whole components, so this cannot collide with a
/// same-named file elsewhere in the tree.
const THIS_FILE: &str = "crates/node/tests/no_early_data_emitters.rs";

/// iroh's two 0-RTT entry points, plus the status enum a client must match on
/// to use one. Assembled from fragments so the scan does not match the literals
/// in this file — belt-and-braces beside [`THIS_FILE`], because a guard that
/// only ever finds itself is indistinguishable from one that finds nothing.
fn forbidden_calls() -> [String; 2] {
    let rtt = format!("{}rtt", 0);
    [format!("into_{rtt}"), format!("Zero{}Status", "Rtt")]
}

/// Compile-time anchor for [`forbidden_calls`].
///
/// The needles above are upstream API names matched as plain text, so an iroh
/// rename would leave the scan passing while guarding nothing. These signatures
/// pin the same names to the type system: rename either entry point upstream
/// and this test binary stops compiling, forcing the needle list to be updated
/// alongside. Nothing calls them — existing is the whole job.
#[expect(dead_code, reason = "compile-time anchor; see the doc comment")]
mod api_anchor {
    use iroh::endpoint::{
        Accepting, Connecting, Connection, IncomingZeroRttConnection, OutgoingZeroRttConnection,
        ZeroRttStatus,
    };

    /// Client side: `Connecting::into_0rtt`.
    fn client(connecting: Connecting) -> Result<OutgoingZeroRttConnection, Connecting> {
        connecting.into_0rtt()
    }

    /// Server side: `Accepting::into_0rtt`, the call a handler's `on_accepting`
    /// override would use.
    fn server(accepting: Accepting) -> IncomingZeroRttConnection {
        accepting.into_0rtt()
    }

    /// The status a 0-RTT client must match on once the handshake lands.
    fn status(status: ZeroRttStatus) -> Connection {
        match status {
            ZeroRttStatus::Accepted(conn) | ZeroRttStatus::Rejected(conn) => conn,
        }
    }
}

/// Workspace root: `CARGO_MANIFEST_DIR` is `<root>/crates/node`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Directory names that hold no first-party source.
fn is_skipped_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == "target" || n.starts_with('.'))
}

/// Every `.rs` file under `dir`, recursively, skipping build output and dot-dirs.
fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if !is_skipped_dir(&path) {
                rust_sources(&path, out)?;
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Which forbidden needles `text` contains.
fn hits_in<'a>(text: &str, forbidden: &'a [String]) -> Vec<&'a String> {
    forbidden
        .iter()
        .filter(|needle| text.contains(needle.as_str()))
        .collect()
}

#[test]
fn no_crate_requests_quic_early_data() -> anyhow::Result<()> {
    let mut sources = Vec::new();
    rust_sources(&workspace_root(), &mut sources)?;
    anyhow::ensure!(
        sources.len() > 50,
        "expected to scan the whole workspace, found only {} files",
        sources.len()
    );

    let forbidden = forbidden_calls();
    let mut hits = Vec::new();
    for path in sources {
        if path.ends_with(THIS_FILE) {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        for needle in hits_in(&text, &forbidden) {
            hits.push(format!("{}: {needle}", path.display()));
        }
    }

    anyhow::ensure!(
        hits.is_empty(),
        "QUIC early data is not used anywhere in this workspace; \
         the following would emit it:\n{}",
        hits.join("\n")
    );
    Ok(())
}

/// The scan above is only worth its runtime if it fails on a real hit. Pin that
/// against a fixture, so a needle that can never match (an assembly typo, say)
/// is caught here rather than by the outage it was meant to prevent.
#[test]
fn every_needle_matches_the_call_it_names() {
    let forbidden = forbidden_calls();
    let planted = [
        "let conn = connecting.into_0rtt().map_err(drop)?;",
        "if let ZeroRttStatus::Accepted(conn) = zrtt.handshake_completed().await? {}",
    ];
    for (needle, source) in forbidden.iter().zip(planted) {
        assert!(
            hits_in(source, std::slice::from_ref(needle)).len() == 1,
            "needle {needle:?} does not match the call it names: {source:?}"
        );
    }

    // ...and stays quiet on source that merely talks about the mechanism.
    let benign = "// This connection never carries early data.";
    assert!(
        hits_in(benign, &forbidden).is_empty(),
        "guard matched prose that names no forbidden call"
    );
}
