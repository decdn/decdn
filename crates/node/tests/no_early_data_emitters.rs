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
//! What is left is the emitter side, and there the invariant is exact: a
//! connection can only carry early data if some code asks for it. This test
//! asserts nothing does.

use std::path::{Path, PathBuf};

/// Transport calls that would put application bytes on the wire before the
/// handshake completes (`into_0rtt`, its status enum) or re-tune the TLS ticket
/// budget that only mattered for them. Assembled from fragments so the guard
/// does not match its own source.
fn forbidden_calls() -> [String; 3] {
    let rtt = format!("{}rtt", 0);
    [
        format!("into_0{rtt}"),
        format!("Zero{}Status", "Rtt"),
        format!("max_tls_{}", "tickets"),
    ]
}

fn workspace_crates() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/crates/node`.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Every `.rs` file under `dir`, recursively.
fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            rust_sources(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

#[test]
fn no_crate_requests_quic_early_data() -> anyhow::Result<()> {
    let mut sources = Vec::new();
    rust_sources(&workspace_crates(), &mut sources)?;
    anyhow::ensure!(
        sources.len() > 50,
        "expected to scan the whole workspace, found only {} files",
        sources.len()
    );

    let forbidden = forbidden_calls();
    let mut hits = Vec::new();
    for path in sources {
        // This guard names the calls it forbids.
        if path.ends_with("no_early_data_emitters.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        for needle in &forbidden {
            if text.contains(needle.as_str()) {
                hits.push(format!("{}: {needle}", path.display()));
            }
        }
    }

    anyhow::ensure!(
        hits.is_empty(),
        "QUIC early data is not used anywhere in this workspace; \
         the following would emit or size it:\n{}",
        hits.join("\n")
    );
    Ok(())
}
