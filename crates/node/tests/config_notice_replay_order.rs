//! Source guard: the daemon replays config-resolve notices *after* it installs
//! a `tracing` subscriber, and it logs its node key the same way.
//!
//! `resolve_config` records notices instead of emitting them precisely because
//! it runs before `init_tracing`, so nothing at that point has a sink. The
//! replay in `commands::run` closes that gap, and its correctness is entirely
//! positional: `emit_config_notices` above `init_tracing` compiles, type-checks
//! and passes every other test in this repo while emitting into no subscriber
//! at all — which is the original defect, restored.
//!
//! No runtime assertion can see this. A test that captures the daemon's log
//! stream has to install its own subscriber first, which is the very thing
//! whose absence is under test. So the guard reads the source and checks the
//! order of the two calls.
//!
//! The node key has the same shape. `load_or_create` runs before
//! `init_tracing` so the JSON formatter can stamp `node_id` on every event, and
//! `log_node_key` logs its outcome afterwards. `log_node_key` above
//! `init_tracing` drops the "generated new node secret key" line. The OTLP
//! provider, the one fallible step of tracing bring-up, runs before the key
//! load, so a bad endpoint cannot fail the start after a first key is written.

use std::path::PathBuf;

/// The daemon's startup path, relative to the workspace root.
const RUN_PATH: &str = "crates/node/src/commands/mod.rs";

/// Workspace root: `CARGO_MANIFEST_DIR` is `<root>/crates/node`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Both names are matched as plain text and both items are crate-private, so
/// an integration test cannot pin them to the type system the way
/// `no_early_data_emitters` pins iroh's public API. Renaming either one
/// therefore has to keep this file in step: the scan fails loudly on a missing
/// needle rather than passing silently, which is the failure mode that matters.
#[test]
fn notices_are_replayed_after_the_subscriber_exists() -> anyhow::Result<()> {
    let path = workspace_root().join(RUN_PATH);
    let text =
        std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;

    // Both needles appear in prose above the calls, so match the call sites:
    // an identifier followed immediately by `(`.
    let call_of = |name: &str| text.find(&format!("{name}("));

    let init = call_of("init_tracing")
        .ok_or_else(|| anyhow::anyhow!("no init_tracing call in {RUN_PATH}"))?;
    let emit = call_of("emit_config_notices")
        .ok_or_else(|| anyhow::anyhow!("no emit_config_notices call in {RUN_PATH}"))?;

    anyhow::ensure!(
        emit > init,
        "{RUN_PATH} replays config notices at byte {emit}, before init_tracing at \
         byte {init}: with no subscriber installed yet the events are discarded and \
         the operator is told nothing"
    );
    Ok(())
}

/// The node key is loaded after the OTLP provider and before `init_tracing`,
/// and logged after `init_tracing`. The same plain-text caveat as above
/// applies to all four needles.
#[test]
fn node_key_is_loaded_before_and_logged_after_the_subscriber() -> anyhow::Result<()> {
    let path = workspace_root().join(RUN_PATH);
    let text =
        std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    let call_of = |name: &str| {
        text.find(&format!("{name}("))
            .ok_or_else(|| anyhow::anyhow!("no {name} call in {RUN_PATH}"))
    };

    let otlp = call_of("init_otlp_provider")?;
    let load = call_of("load_or_create")?;
    let init = call_of("init_tracing")?;
    let log = call_of("log_node_key")?;

    anyhow::ensure!(
        otlp < load,
        "{RUN_PATH} builds the OTLP provider at byte {otlp}, after the key load at \
         byte {load}: a bad endpoint then fails a first start after the key is \
         written, and the \"generated\" line is never logged"
    );
    anyhow::ensure!(
        load < init,
        "{RUN_PATH} loads the node key at byte {load}, after init_tracing at byte \
         {init}: the JSON formatter has no node_id for its events"
    );
    anyhow::ensure!(
        log > init,
        "{RUN_PATH} logs the node key at byte {log}, before init_tracing at byte \
         {init}: with no subscriber installed yet the lines are discarded"
    );
    Ok(())
}
