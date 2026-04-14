//! Implementations of the `decdn` CLI subcommands.
//!
//! The bin target (`src/main.rs`) does nothing but parse `Cli` and dispatch to
//! the functions here. Keeping them in the library target lets integration
//! tests and downstream tools depend on them without `#[path]` tricks.

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
            tracing_subscriber::EnvFilter::new(resolved.log_level.to_string())
        }
    };

    init_tracing(filter, &resolved)?;

    tracing::info!("deCDN node starting");
    tracing::debug!(
        data_dir = %resolved.data_dir.display(),
        bind_port = resolved.bind_port,
        rpc_url = "<redacted>",
        cache_dir = %resolved.cache_dir.display(),
        cache_size_mb = resolved.cache_size_mb,
        rate_per_mb = resolved.rate_per_mb,
        metrics_port = resolved.metrics_port,
        "resolved configuration"
    );

    runtime::run(resolved).await
}

/// Initialize the tracing subscriber with fmt layer and optional OTLP layer.
#[allow(clippy::unnecessary_wraps)] // Returns Result only when otlp feature is enabled.
fn init_tracing(
    filter: tracing_subscriber::EnvFilter,
    resolved: &config::ResolvedConfig,
) -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;

    let fmt_layer = match resolved.log_format {
        cli::LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        cli::LogFormat::Pretty => tracing_subscriber::fmt::layer().boxed(),
    };

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    #[cfg(feature = "otlp")]
    {
        if let Some(ref endpoint) = resolved.otlp_endpoint {
            let tracer = init_otlp_tracer(endpoint)?;
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            registry.with(otel_layer).init();
        } else {
            registry.init();
        }
    }

    #[cfg(not(feature = "otlp"))]
    {
        if resolved.otlp_endpoint.is_some() {
            eprintln!("warning: --otlp-endpoint ignored (binary not built with 'otlp' feature)");
        }
        registry.init();
    }

    Ok(())
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
        ALPN_PROBE,
        message::{ProbeRequest, ProbeResponse},
    };
    use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey};
    use rand::RngCore;

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

    let client_sk = SecretKey::generate(&mut rand::rng());
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

        let bytes = postcard::to_allocvec(&ProbeRequest { nonce })
            .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
        send.write_all(&bytes)
            .await
            .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;

        let resp_bytes = recv
            .read_to_end(4096)
            .await
            .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
        let resp: ProbeResponse = postcard::from_bytes(&resp_bytes)
            .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;

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
    // Display impl) — matches what the server logs on startup.
    let node_id = iroh::PublicKey::from_bytes(&resp.node_id)
        .map_or_else(|_| "<invalid node id>".to_string(), |pk| pk.to_string());
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

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 10240

[payment]
# rate_per_mb = 10

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
# otlp_endpoint = "http://localhost:4317"  # requires --features otlp
"#;
