//! Implementations of the `decdn` CLI subcommands.
//!
//! The bin target (`src/main.rs`) does nothing but parse `Cli` and dispatch to
//! the functions here. Keeping them in the library target lets integration
//! tests and downstream tools depend on them without `#[path]` tricks.

pub mod node;

pub use node::{node_dispatch, peers};

use crate::{cli, config, identity, runtime};

/// Run the deCDN node with resolved configuration.
pub async fn run(
    config_path: Option<&std::path::Path>,
    run_args: &cli::RunArgs,
) -> anyhow::Result<()> {
    let resolved = config::resolve_config(config_path, run_args)?;

    // Initialize tracing — RUST_LOG env var takes precedence over resolved log level.
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(e) => {
            // Only warn if RUST_LOG was actually set (not just absent).
            if std::env::var_os("RUST_LOG").is_some() {
                eprintln!("warning: ignoring malformed RUST_LOG: {e}");
            }
            tracing_subscriber::EnvFilter::new(resolved.observability.log_level.to_string())
        }
    };

    let log_level_setter = init_tracing(filter, &resolved)?;

    tracing::info!("deCDN node starting");
    tracing::debug!(
        data_dir = %resolved.identity.data_dir.display(),
        bind_port = resolved.network.bind_port,
        rpc_url = "<redacted>",
        cache_dir = %resolved.cache.cache_dir.display(),
        cache_size_mb = resolved.cache.cache_size_mb,
        rate_per_mb = resolved.payment.rate_per_mb,
        metrics_port = resolved.observability.metrics_port,
        "resolved configuration"
    );

    let reload_state = std::sync::Arc::new(runtime::RuntimeReloadState::new(
        run_args.payment.clone(),
        run_args.observability.clone(),
        &resolved,
        log_level_setter,
    ));

    runtime::run(
        resolved,
        config_path.map(std::path::Path::to_path_buf),
        reload_state,
    )
    .await
}

/// Initialize the tracing subscriber with fmt layer and optional OTLP layer.
///
/// Returns a [`runtime::LogLevelSetter`] closure that swaps the live
/// `EnvFilter` to one matching a new `LogLevel` — used by the SIGHUP
/// hot-reload path (#236). The closure captures a `reload::Handle` to the
/// `EnvFilter` layer; calls to `modify` must respect any errors from the
/// handle (e.g. the registry was dropped) by surfacing them.
#[allow(clippy::unnecessary_wraps)] // Returns Result only when otlp feature is enabled.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    resolved: &config::ResolvedConfig,
) -> anyhow::Result<runtime::LogLevelSetter> {
    use tracing_subscriber::prelude::*;

    let fmt_layer = match resolved.observability.log_format {
        cli::LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        cli::LogFormat::Pretty => tracing_subscriber::fmt::layer().boxed(),
    };

    // Wrap the EnvFilter in a `reload::Layer` so the SIGHUP reload path
    // can swap it without rebuilding the rest of the subscriber stack.
    let (reload_filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let registry = tracing_subscriber::registry()
        .with(reload_filter)
        .with(fmt_layer);

    #[cfg(feature = "otlp")]
    {
        if let Some(ref endpoint) = resolved.observability.otlp_endpoint {
            let tracer = init_otlp_tracer(endpoint)?;
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            registry.with(otel_layer).init();
        } else {
            registry.init();
        }
    }

    #[cfg(not(feature = "otlp"))]
    {
        if resolved.observability.otlp_endpoint.is_some() {
            eprintln!("warning: --otlp-endpoint ignored (binary not built with 'otlp' feature)");
        }
        registry.init();
    }

    let setter: runtime::LogLevelSetter = Box::new(move |lvl| {
        // Build a fresh EnvFilter from the level's lowercase name. This
        // matches the startup default-filter construction above; we don't
        // attempt to honour `RUST_LOG` here because reload is driven by
        // the file, not the launching shell.
        let new_filter = tracing_subscriber::EnvFilter::try_new(lvl.to_string())
            .map_err(|e| anyhow::anyhow!("invalid log_level {lvl}: {e}"))?;
        reload_handle
            .modify(|f| *f = new_filter)
            .map_err(|e| anyhow::anyhow!("failed to swap tracing filter: {e}"))?;
        Ok(())
    });

    Ok(setter)
}

/// Build an OTLP span exporter and tracer provider.
#[cfg(feature = "otlp")]
fn init_otlp_tracer(endpoint: &str) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracer> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::SdkTracerProvider;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build OTLP exporter: {e}"))?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attributes([KeyValue::new("service.name", "decdn")])
                .build(),
        )
        .build();

    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "decdn");
    opentelemetry::global::set_tracer_provider(provider);

    Ok(tracer)
}

/// Generate (or reuse) the persistent Ed25519 node key.
pub fn key_gen(args: &cli::KeyGenArgs) -> anyhow::Result<()> {
    let output_dir = args
        .output_dir
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine output directory: home dir not found"))?;

    let key_path = identity::key_path(&output_dir);
    if key_path.exists() {
        if !args.force {
            anyhow::bail!(
                "node key already exists at {}; pass --force to overwrite",
                key_path.display()
            );
        }
        std::fs::remove_file(&key_path)
            .map_err(|e| anyhow::anyhow!("failed to remove {}: {e}", key_path.display()))?;
    }

    let key = identity::load_or_generate(&output_dir)?;
    println!("node id: {}", key.public());
    println!("wrote secret key to {}", key_path.display());
    Ok(())
}

/// Send a `cdn/probe/v1` request to a running node and print the response.
pub async fn probe(args: &cli::ProbeArgs) -> anyhow::Result<()> {
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    use decdn_protocol::{
        ALPN_PROBE, ProbeMessage, decode_message, encode_message,
        message::{ProbeRequest, ProbeResponse},
        read_frame, write_frame,
    };
    use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMap, RelayMode, RelayUrl};
    use rand::Rng;

    use crate::identity::fresh_secret_key;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;

    let relay_url = match args.relay_url.as_deref() {
        Some(s) => Some(
            RelayUrl::from_str(s).map_err(|e| anyhow::anyhow!("invalid --relay-url {s:?}: {e}"))?,
        ),
        None => None,
    };

    // Bind to an unspecified IPv4 address in both cases. Loopback-only binding
    // prevents the probe client from reaching a non-loopback `--addr`, which
    // is the whole point of the subcommand. `0.0.0.0:0` lets the OS pick an
    // ephemeral port on any interface; we're a client, nothing listens here.
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0);
    let relay_mode = match relay_url.clone() {
        Some(url) => RelayMode::Custom(RelayMap::from_iter([url])),
        None => RelayMode::Disabled,
    };

    let client_sk = fresh_secret_key();
    let endpoint = Endpoint::empty_builder()
        .secret_key(client_sk)
        .relay_mode(relay_mode)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))?;

    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relay_url {
        target = target.with_relay_url(url);
    }

    let timeout = Duration::from_millis(args.timeout_ms);
    let nonce: u64 = rand::rng().next_u64();

    let started = Instant::now();
    let result = tokio::time::timeout(timeout, async {
        let conn = endpoint
            .connect(target, ALPN_PROBE)
            .await
            .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

        let payload = encode_message(&ProbeMessage::Request(ProbeRequest { nonce }))
            .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;

        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
        let (msg, _rest) = decode_message::<ProbeMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;
        let resp: ProbeResponse = match msg {
            ProbeMessage::Response(r) => r,
            ProbeMessage::Request(_) => {
                anyhow::bail!("unexpected ProbeMessage::Request from server");
            }
        };

        conn.close(0u32.into(), b"probe-done");
        Ok::<_, anyhow::Error>(resp)
    })
    .await;

    let rtt_ms = started.elapsed().as_secs_f64() * 1000.0;
    endpoint.close().await;

    let resp = match result {
        Ok(inner) => inner?,
        Err(_) => anyhow::bail!("probe timed out after {} ms", args.timeout_ms),
    };

    if resp.nonce != nonce {
        anyhow::bail!(
            "nonce mismatch: sent 0x{nonce:016x}, received 0x{:016x}",
            resp.nonce
        );
    }

    print_probe_response(&resp, rtt_ms, nonce, args.json);
    Ok(())
}

/// Render a successful probe response to stdout in pretty or JSON form.
fn print_probe_response(
    resp: &decdn_protocol::message::ProbeResponse,
    rtt_ms: f64,
    nonce: u64,
    json: bool,
) {
    // Format as the canonical iroh node-id string (z-base-32 via PublicKey's
    // Display impl) — matches what the server logs on startup. Fall back to
    // raw hex if the key fails to parse; the fallback stays alphanumeric so
    // downstream `--json` consumers never see non-conforming output.
    let node_id = iroh::PublicKey::from_bytes(&resp.node_id).map_or_else(
        |_| {
            use std::fmt::Write as _;
            let mut s = String::with_capacity(2 + 64);
            s.push_str("0x");
            for b in resp.node_id {
                let _ = write!(s, "{b:02x}");
            }
            s
        },
        |pk| pk.to_string(),
    );
    if json {
        println!(
            "{{\"node_id\":\"{node_id}\",\"rate_per_mb\":{},\"measured_at_unix_ms\":{},\"rtt_ms\":{:.3},\"nonce\":\"0x{nonce:016x}\"}}",
            resp.rate_per_mb, resp.measured_at_unix_ms, rtt_ms,
        );
    } else {
        println!("node_id:       {node_id}");
        println!("rate_per_mb:   {} (base units)", resp.rate_per_mb);
        println!("measured_at:   {} (unix ms)", resp.measured_at_unix_ms);
        println!("rtt:           {rtt_ms:.3} ms");
        println!("nonce:         0x{nonce:016x} (echoed ok)");
    }
}

/// Validate a configuration and print a resolved summary. Performs no I/O
/// beyond reading the config file — does not open sockets or touch the RPC.
pub fn config_validate(
    config_path: Option<&std::path::Path>,
    args: &cli::ConfigValidateArgs,
) -> anyhow::Result<()> {
    let resolved = config::resolve_config(config_path, &args.run)?;
    let source = effective_source(config_path, cli::common::default_config_path)?;
    let mut stdout = std::io::stdout().lock();
    write_validate_summary(&mut stdout, source.as_deref(), &resolved)
        .map_err(|e| anyhow::anyhow!("failed to write summary: {e}"))
}

/// Resolve the config source actually consulted. When `--config` is omitted,
/// the default path is auto-loaded only if present, so report the file that
/// `resolve_config` will actually read — not just the explicit flag.
///
/// Uses `try_exists` rather than `exists`: a permission error on the default
/// path must be surfaced, not silently reported as "no config file" — the
/// whole point of `validate` is telling the operator what the node sees.
pub fn effective_source(
    config_path: Option<&std::path::Path>,
    default: impl FnOnce() -> Option<std::path::PathBuf>,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    if let Some(p) = config_path {
        return Ok(Some(p.to_path_buf()));
    }
    let Some(default) = default() else {
        return Ok(None);
    };
    match default.try_exists() {
        Ok(true) => Ok(Some(default)),
        Ok(false) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(
            "cannot determine whether default config {} exists: {e}",
            default.display()
        )),
    }
}

/// Render the validation summary. Separated from [`config_validate`] so tests
/// can capture the output into a buffer and assert on the printed contract —
/// in particular that `rpc_url` and `otlp_endpoint` values never appear.
pub fn write_validate_summary<W: std::io::Write>(
    w: &mut W,
    source: Option<&std::path::Path>,
    resolved: &config::ResolvedConfig,
) -> std::io::Result<()> {
    writeln!(w, "config valid")?;
    match source {
        Some(path) => writeln!(w, "  source:                   {}", path.display())?,
        None => writeln!(
            w,
            "  source:                   (defaults + env only, no config file)"
        )?,
    }
    writeln!(
        w,
        "  data_dir:                 {}",
        resolved.identity.data_dir.display()
    )?;
    if let Some(region) = &resolved.identity.region {
        writeln!(w, "  region:                   {region}")?;
    }
    writeln!(
        w,
        "  bind_port:                {}",
        resolved.network.bind_port
    )?;
    if let Some(relay) = &resolved.network.relay_url {
        writeln!(w, "  relay_url:                {relay}")?;
    }
    writeln!(
        w,
        "  rpc_url:                  <redacted> ({} chars)",
        resolved.blockchain.rpc_url.len()
    )?;
    writeln!(
        w,
        "  eth_keystore:             {}",
        resolved.blockchain.eth_keystore.display()
    )?;
    writeln!(
        w,
        "  payment_channel_address:  {}",
        resolved.blockchain.payment_channel_address
    )?;
    writeln!(
        w,
        "  staking_registry_address: {}",
        resolved.blockchain.staking_registry_address
    )?;
    writeln!(
        w,
        "  rpc_watchdog_interval_sec: {}",
        resolved.blockchain.rpc_watchdog_interval_sec
    )?;
    writeln!(
        w,
        "  cache_dir:                {}",
        resolved.cache.cache_dir.display()
    )?;
    writeln!(
        w,
        "  cache_size_mb:            {}",
        resolved.cache.cache_size_mb
    )?;
    writeln!(
        w,
        "  max_blob_size_mb:         {}",
        resolved.cache.max_blob_size_mb
    )?;
    writeln!(
        w,
        "  rate_per_mb:              {}",
        resolved.payment.rate_per_mb
    )?;
    writeln!(
        w,
        "  log_level:                {}",
        resolved.observability.log_level
    )?;
    writeln!(
        w,
        "  metrics_port:             {}",
        resolved.observability.metrics_port
    )?;
    match resolved.observability.admin_port {
        Some(p) => writeln!(w, "  admin_port:               {p}")?,
        None => writeln!(w, "  admin_port:               disabled")?,
    }
    // otlp_endpoint URLs commonly carry bearer tokens or API keys in the
    // path or query, so redact like rpc_url.
    if let Some(otlp) = &resolved.observability.otlp_endpoint {
        writeln!(
            w,
            "  otlp_endpoint:            <redacted> ({} chars)",
            otlp.len()
        )?;
    }
    Ok(())
}

/// Write a default TOML configuration file.
pub fn config_init(args: &cli::ConfigInitArgs) -> anyhow::Result<()> {
    let output = args
        .output
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_config_path)
        .ok_or_else(|| anyhow::anyhow!("cannot determine config path: home dir not found"))?;

    if output.exists() && !args.force {
        anyhow::bail!(
            "config file already exists at {}; use --force to overwrite",
            output.display()
        );
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("failed to create directory {}: {e}", parent.display()))?;
    }

    std::fs::write(&output, DEFAULT_CONFIG)
        .map_err(|e| anyhow::anyhow!("failed to write config file {}: {e}", output.display()))?;

    println!("wrote default config to {}", output.display());
    Ok(())
}

/// Default TOML config file content.
const DEFAULT_CONFIG: &str = r#"# deCDN node configuration
# CLI flags override values in this file.

[identity]
# data_dir = "~/.decdn"
# region = "US"

[network]
# bind_port = 4433
# relay_url = "https://relay.iroh.network."

[blockchain]
# rpc_url = ""                       # REQUIRED: Arbitrum Sepolia JSON-RPC URL
# eth_keystore = "~/.decdn/keystore.json"
# payment_channel_address = ""       # REQUIRED: 0x-prefixed hex
# staking_registry_address = ""      # REQUIRED: 0x-prefixed hex
# rpc_watchdog_interval_sec = 30     # 0 disables the connectivity watchdog

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 1024

[payment]
# rate_per_mb = 10

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
# admin_port = 9191                        # loopback-only; 0 disables (ADR 025)
# otlp_endpoint = "http://localhost:4317"  # requires --features otlp
"#;
