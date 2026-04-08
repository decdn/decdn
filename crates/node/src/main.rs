//! deCDN node — CLI entry point.

mod cli;
mod config;

use clap::Parser;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(run_args) => cmd_run(cli.config.as_deref(), &run_args),
        Command::KeyGen(args) => cmd_key_gen(&args),
        Command::Config(args) => cmd_config_init(&args),
    }
}

/// Run the deCDN node with resolved configuration.
fn cmd_run(config_path: Option<&std::path::Path>, run_args: &cli::RunArgs) -> anyhow::Result<()> {
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

    // TODO: block on node runtime here; for now just exit cleanly.

    Ok(())
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

/// Generate Ed25519 node key and Ethereum keystore.
fn cmd_key_gen(args: &cli::KeyGenArgs) -> anyhow::Result<()> {
    let output_dir = args
        .output_dir
        .clone()
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine output directory: home dir not found"))?;

    // TODO: implement key generation
    println!("would generate keys in {}", output_dir.display());
    if args.force {
        println!("(overwrite mode enabled)");
    }
    Ok(())
}

/// Write a default TOML configuration file.
fn cmd_config_init(args: &cli::ConfigInitArgs) -> anyhow::Result<()> {
    let output = args
        .output
        .clone()
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
