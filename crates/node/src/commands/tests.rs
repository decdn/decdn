use super::*;

#[test]
fn reload_directive_keeps_a_rust_log_filter_and_applies_the_file_level_otherwise() {
    for lvl in [
        cli::common::LogLevel::Error,
        cli::common::LogLevel::Warn,
        cli::common::LogLevel::Info,
        cli::common::LogLevel::Debug,
        cli::common::LogLevel::Trace,
    ] {
        assert_eq!(
            reload_directive(true, lvl),
            None,
            "RUST_LOG pins the filter"
        );
        assert_eq!(reload_directive(false, lvl), Some(lvl.to_string()));
    }
}

/// A shared in-memory log sink.
#[derive(Clone, Default)]
struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Buf {
    fn text(&self) -> anyhow::Result<String> {
        let bytes = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .clone();
        Ok(String::from_utf8(bytes)?)
    }
}

/// The reload handle still swaps the level in the stack `init_tracing`
/// installs, where the `EnvFilter` is a per-layer filter on the fmt layer:
/// a debug event is dropped at `info` and written after the swap to
/// `debug`.
#[test]
fn per_layer_reload_filter_applies_a_new_level() -> anyhow::Result<()> {
    let buf = Buf::default();
    let writer = buf.clone();
    let (filter, handle) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
    let node_id = iroh::SecretKey::generate().public();
    let subscriber = subscriber(
        fmt_layer(cli::LogFormat::Pretty, &node_id, move || writer.clone()),
        filter,
        None,
    );
    let _guard = tracing::subscriber::set_default(subscriber);

    tracing::debug!("before the swap");
    handle.modify(|f| *f = tracing_subscriber::EnvFilter::new("debug"))?;
    tracing::debug!("after the swap");

    let logs = buf.text()?;
    anyhow::ensure!(!logs.contains("before the swap"), "logs: {logs}");
    anyhow::ensure!(logs.contains("after the swap"), "logs: {logs}");
    // The pretty format is plain text for a terminal: no JSON, no node id.
    anyhow::ensure!(!logs.starts_with('{'), "logs: {logs}");
    anyhow::ensure!(!logs.contains("node_id"), "logs: {logs}");
    Ok(())
}

/// Every JSON event, inside a span or not, is one valid JSON object that
/// carries the ADR appendix-observability mandatory top-level keys,
/// `node_id` among them. An event field named `node_id` stays under
/// `fields` and does not replace the node's own.
#[test]
fn json_events_carry_the_mandatory_top_level_fields() -> anyhow::Result<()> {
    let buf = Buf::default();
    let writer = buf.clone();
    let (filter, _handle) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
    let node_id = iroh::SecretKey::generate().public();
    let subscriber = subscriber(
        fmt_layer(cli::LogFormat::Json, &node_id, move || writer.clone()),
        filter,
        None,
    );
    let _guard = tracing::subscriber::set_default(subscriber);

    tracing::info!(hash = "ab", "outside a span");
    tracing::info_span!("serve_stream", peer = "cd").in_scope(|| {
        tracing::warn!("inside a span");
    });
    tracing::info!(node_id = "other", "with a node_id field");

    let logs = buf.text()?;
    let events = logs
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(events.len() == 3, "logs: {logs}");
    let want_id = node_id.to_string();
    anyhow::ensure!(
        want_id.len() == 64
            && want_id
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "node_id is not 64 lowercase hex digits: {want_id}"
    );
    let str_at = |event: &serde_json::Value, pointer: &str| {
        event
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let messages = ["outside a span", "inside a span", "with a node_id field"];
    for (event, message) in events.iter().zip(messages) {
        anyhow::ensure!(
            str_at(event, "/node_id") == Some(want_id.clone()),
            "event: {event}"
        );
        for key in ["/timestamp", "/level", "/target"] {
            anyhow::ensure!(str_at(event, key).is_some(), "no {key}: {event}");
        }
        anyhow::ensure!(
            str_at(event, "/fields/message").as_deref() == Some(message),
            "event: {event}"
        );
    }
    anyhow::ensure!(
        events
            .get(1)
            .and_then(|e| str_at(e, "/span/name"))
            .as_deref()
            == Some("serve_stream"),
        "logs: {logs}"
    );
    anyhow::ensure!(
        events
            .get(2)
            .and_then(|e| str_at(e, "/fields/node_id"))
            .as_deref()
            == Some("other"),
        "logs: {logs}"
    );
    Ok(())
}
