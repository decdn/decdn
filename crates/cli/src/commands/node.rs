//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback JSON-RPC admin surface (`appendix-local-admin-http`).

use std::io;
use std::io::Write as _;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use jsonrpsee::core::client::Error as JsonRpcClientError;
use jsonrpsee::http_client::HttpClientBuilder;
use serde::Deserialize;

use decdn_common::admin::{
    AdminRpcClient, AnnounceResponse, ChannelSnapshot, ChannelsResponse, DrainRequest,
    DrainResponse, EvictRequest, EvictResponse, HealthResponse, PeerView, PeersResponse,
    RegionStatsResponse, ReloadResponse, ReputationRequest, ReputationResponse, StatusResponse,
};
use decdn_common::cli;
use decdn_common::cli::ConfigPathSource;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_ADMIN_PORT;

/// Partial deserializer for the TOML config — only the path
/// `observability.admin_port` is interesting to `decdn node peers`.
/// Kept private here (rather than reusing `decdn_common::config::FileConfig`)
/// so an operator's typo in an unrelated section can't make peer
/// listing unusable. `serde(default)` and serde-toml's default
/// "ignore unknown fields" together guarantee that any other valid
/// TOML — including missing tables — round-trips through with no
/// effect.
#[derive(Debug, Default, Deserialize)]
struct AdminPortConfig {
    observability: Option<AdminPortObservability>,
}

#[derive(Debug, Default, Deserialize)]
struct AdminPortObservability {
    admin_port: Option<u16>,
}

/// Dispatch a `decdn node <sub>` invocation.
///
/// `global_config` is the path (if any) from the top-level `decdn
/// --config` flag. It's consulted as a fallback when the subcommand
/// didn't set its own `--config`, so `decdn --config foo.toml node
/// peers` works the way the help text implies.
pub async fn node_dispatch(
    args: &cli::NodeArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.cmd {
        cli::NodeCommand::Peers(p) => peers(p, global_config).await,
        cli::NodeCommand::Health(h) => health(h, global_config).await,
        cli::NodeCommand::Status(s) => status(s, global_config).await,
        cli::NodeCommand::Channels(c) => channels(c, global_config).await,
        cli::NodeCommand::RegionStats(r) => region_stats(r, global_config).await,
        cli::NodeCommand::Reputation(r) => reputation(r, global_config).await,
        cli::NodeCommand::Evict(e) => evict(e, global_config).await,
        cli::NodeCommand::Announce(a) => announce(a, global_config).await,
        cli::NodeCommand::Reload(r) => reload(r, global_config).await,
        cli::NodeCommand::Drain(d) => drain(d, global_config).await,
        cli::NodeCommand::Top(t) => crate::commands::node_top::run(t, global_config).await,
        cli::NodeCommand::Register(r) => crate::commands::register::run(r, global_config).await,
        cli::NodeCommand::Bond(b) => crate::commands::bond::run(b, global_config).await,
    }
}

/// `decdn node health`: call `admin_v1_health` on the running node and
/// print the result. Two-line plain text by default (`node_id=…` /
/// `uptime_s=…`), or pretty JSON with `--json`.
pub async fn health(args: &cli::HealthArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: HealthResponse = client
        .health()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode health as JSON")?;
        println!("{pretty}");
    } else {
        // Two stable, grep-friendly lines so `decdn node health | grep
        // node_id=` works in operator scripts without `--json`.
        println!("node_id={}", resp.node_id);
        println!("uptime_s={}", resp.uptime_s);
    }

    Ok(())
}

/// `decdn node evict`: call `admin_v1_evict` on the running node to
/// forcibly remove a single blob from the local cache (issue #279), or
/// preview what the evict would touch when `--dry-run` is set (issue
/// #379).
pub async fn evict(args: &cli::EvictArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: EvictResponse = client
        .evict(EvictRequest {
            hash: args.hash.clone(),
            dry_run: args.dry_run,
        })
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode evict response")?;
        println!("{pretty}");
    } else if args.dry_run {
        // Multi-line, grep-friendly so operators can pipe through
        // `grep size_bytes=` / `grep already_evicted=` in scripts. Each
        // line stands on its own; the "dry_run=true" tag is the
        // load-bearing indicator that no state was mutated.
        write_dry_run_human(&mut io::stdout().lock(), &args.hash, &resp)
            .context("failed to write dry-run preview")?;
    } else {
        let presence = if resp.was_present {
            "evicted"
        } else {
            // Operator ran evict on a hash the node never held — log it
            // explicitly rather than printing nothing, otherwise scripts
            // can't tell success-with-no-effect from a hung command.
            "not present"
        };
        println!("hash={} status={presence}", args.hash);
    }

    Ok(())
}

/// Format the dry-run preview as a multi-line plain-text block. Pure
/// function (takes `&mut impl Write`) so unit tests can assert exact
/// output without an HTTP round-trip — same pattern as
/// [`write_peers_table`].
fn write_dry_run_human(w: &mut impl io::Write, hash: &str, resp: &EvictResponse) -> io::Result<()> {
    writeln!(w, "hash={hash}")?;
    writeln!(w, "dry_run=true")?;
    writeln!(w, "was_present={}", resp.was_present)?;
    writeln!(w, "pinned={}", resp.preview.pinned)?;
    writeln!(w, "already_evicted={}", resp.preview.already_evicted)?;
    match resp.preview.size_bytes {
        Some(b) => writeln!(w, "size_bytes={b}")?,
        // Distinct from "0" (a legitimately empty blob); operators
        // seeing `not_stored` know the iroh-blobs status reported
        // `NotFound`, not `Complete { size: 0 }`.
        None => writeln!(w, "size_bytes=not_stored")?,
    }
    match resp.preview.last_accessed_us_ago {
        Some(us) => writeln!(w, "last_accessed={}", format_age(us))?,
        // Distinct from "<1s ago"; operators want to know the
        // engine has *no* access record vs a very recent one.
        None => writeln!(w, "last_accessed=never")?,
    }
    // Origin egress-cost cue (#439). Distinct from omitting the line
    // when the engine has no origins: operators evaluating disk-reclaim
    // potential against re-fetch cost want this signal explicitly,
    // not buried in "the field is missing because there's no origin
    // at all". `none` matches the JSON serialisation skip-condition
    // semantically (`Vec::is_empty` is omitted in JSON, surfaced as
    // `none` here). Multi-origin chains (#284) render as a
    // comma-separated list in declared order.
    if resp.preview.origin_kinds.is_empty() {
        writeln!(w, "origin_kinds=none")?;
    } else {
        let joined = resp
            .preview
            .origin_kinds
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        writeln!(w, "origin_kinds={joined}")?;
    }
    Ok(())
}

/// `decdn node announce`: call `admin_v1_announce` on the running node to
/// publish a one-shot `NodeAnnounce` outside the periodic interval (issue
/// #280).
pub async fn announce(
    args: &cli::AnnounceArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: AnnounceResponse = client
        .announce()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    // `triggered` is `true` on every well-formed non-error response — the
    // publisher-disabled case returns a distinct error code, not `false`. A
    // `false` therefore means the node accepted the RPC but reported the
    // announce was *not* queued; treat that as a failure (stderr + non-zero
    // exit) instead of silently exiting 0 with `announce_queued=false` (#845).
    // Checked *before* any stdout emission so a failing run never prints a
    // contradictory `announce_queued=false` / `"triggered": false` line —
    // stdout carries only the success result.
    anyhow::ensure!(
        resp.triggered,
        "node accepted the request but reported the announce was not queued \
         (triggered=false)"
    );

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode announce response")?;
        println!("{pretty}");
    } else {
        // The trigger is fire-and-forget on the publisher side, so this
        // confirms only that the node accepted the request — `queued`
        // rather than `triggered` so a script reader can't mistake this
        // for "broadcast hit the wire". Actual peer delivery is
        // observable via `decdn node peers` on a peer; broadcast failures
        // (no neighbors, transport error) surface as `warn!` lines in the
        // node's own log.
        println!("announce_queued={}", resp.triggered);
    }

    Ok(())
}

/// `decdn node reload`: call `admin_v1_reload` on the running node to
/// re-read the config file it was started with and apply the
/// hot-reloadable subset (issue #373). Equivalent to `kill -HUP <pid>`
/// for operators who'd rather not stat the PID — and shares the same
/// internal mutex, so concurrent SIGHUPs queue rather than race.
pub async fn reload(args: &cli::ReloadArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: ReloadResponse = client
        .reload()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode reload response")?;
        println!("{pretty}");
    } else {
        // Two stable, grep-friendly lines so an operator can do
        // `decdn node reload | grep rate_per_mb=` without `--json`.
        // Same shape as `decdn node health`'s plain output.
        println!("rate_per_mb={}", resp.rate_per_mb);
        println!("log_level={}", resp.log_level);
    }

    Ok(())
}

/// `decdn node drain`: call `admin_v1_drain` on the running node to trigger
/// graceful shutdown (issue #244, ADR 025).
///
/// Without `--wait`: fire-and-forget — returns `drain_initiated=true` as
/// soon as the trigger is queued; observe completion via process exit
/// or `decdn node health` until ECONNREFUSED.
///
/// With `--wait` (issue #604): asks the runtime to keep the admin
/// server alive through `router.shutdown` (`DrainRequest { wait_admin:
/// true }`), then polls `admin_v1_health` for `in_flight_streams == 0`.
/// Returns `Ok` once the count reaches 0 or admin closes
/// (ECONNREFUSED — drain finished and tore admin down). Returns `Err`
/// with `drain_timeout=true in_flight_streams=N` on stderr if
/// `--wait-timeout-secs` elapses first.
pub async fn drain(args: &cli::DrainArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );
    // `wait_timeout_secs` and `wait_poll_ms` are `NonZeroU64` — clap
    // rejects `0` at parse time, so no further runtime check needed.

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: DrainResponse = client
        .drain(Some(DrainRequest {
            wait_admin: args.wait,
        }))
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if !args.wait {
        if args.json {
            let pretty =
                serde_json::to_string_pretty(&resp).context("failed to encode drain response")?;
            println!("{pretty}");
        } else {
            // One stable, grep-friendly line. "Initiated", not "completed":
            // fire-and-forget semantics mean the process is still running when
            // this prints. Observe completion via `decdn node health` until
            // ECONNREFUSED, or let the process supervisor (systemd/K8s) notify.
            println!("drain_initiated={}", resp.initiated);
        }
        return Ok(());
    }

    // Cross-version safety: `--wait` is only safe when the server is
    // actually keeping admin alive through `router.shutdown`. A server
    // that doesn't honor `wait_admin` (older binary, future bug,
    // anything in between) lacks the `wait_admin_honored` field, which
    // serde-defaults to `false` on this side. Refuse to enter the
    // polling loop in that case — otherwise the imminent ECONNREFUSED
    // from the early admin tear-down would be misread as drain
    // completion while in-flight streams are still running, defeating
    // the zero-payment-loss guarantee.
    if !resp.wait_admin_honored {
        // The drain has *already fired* server-side at this point —
        // fire-and-forget semantics, so the node is now mid-shutdown
        // via the legacy SIGTERM-equivalent ordering. The operator
        // can't undo that; the actionable advice is to either let it
        // finish via the legacy path (process exit / health-until-
        // ECONNREFUSED) or upgrade the node so `--wait` works next time.
        return Err(anyhow::anyhow!(
            "admin at {url} did not honor --wait (wait_admin_honored=false). \
             Drain has already been initiated server-side and the node is \
             shutting down via the legacy SIGTERM-equivalent path; observe \
             completion via process exit or `decdn node health` until \
             ECONNREFUSED. The server is likely older than the CLI or does \
             not implement #604 — upgrade the node to use --wait safely",
        ));
    }

    // `--wait` path: drain has been initiated with `wait_admin=true`,
    // and the server acked it. Poll until `in_flight_streams == 0`,
    // admin closes (ECONNREFUSED — the runtime tore admin down after
    // `router.shutdown` returned), or the wall-clock budget elapses.
    // Both terminal-success paths emit the same JSON shape so machine
    // consumers see a single schema regardless of which branch fires.
    let deadline = std::time::Instant::now() + Duration::from_secs(args.wait_timeout_secs.get());
    let poll_interval = Duration::from_millis(args.wait_poll_ms.get());
    loop {
        let h = match client.health().await {
            Ok(h) => h,
            Err(JsonRpcClientError::Transport(inner))
                if is_admin_closed_transport_error(inner.as_ref()) =>
            {
                // Admin closed — runtime finished `router.shutdown` and
                // moved on. In-flight streams are necessarily zero
                // (router awaited them all). Treat
                // ConnectionRefused/Reset/Aborted and UnexpectedEof
                // all the same: each is a way for a TCP connection to
                // observe "the listener / accepted socket went away."
                emit_drain_complete(&mut io::stdout().lock(), args.json, true)
                    .context("failed to write drain-complete output")?;
                return Ok(());
            }
            // Transient errors during polling shouldn't end the wait
            // — the polling loop has its own wall-clock deadline. A
            // single 5s RPC timeout against a momentarily-busy admin
            // would otherwise collapse a 30s drain budget after one
            // tick. Retry until the deadline check fires; the
            // deadline is the single authority on "give up".
            // Bundling `RequestTimeout` and non-close `Transport`
            // into the same arm — they have the same retry policy.
            Err(JsonRpcClientError::RequestTimeout | JsonRpcClientError::Transport(_)) => {
                check_deadline_and_sleep(deadline, poll_interval, args.wait_timeout_secs).await?;
                continue;
            }
            // Server-side JSON-RPC errors during polling indicate
            // something genuinely unexpected (MethodNotFound here
            // would have been caught upstream by the
            // `wait_admin_honored=false` guard, so any `Call(...)` is
            // novel). Bail with a clear diagnostic.
            Err(JsonRpcClientError::Call(obj)) => {
                return Err(anyhow::anyhow!(
                    "admin at {url} returned JSON-RPC error {} during --wait poll: {}",
                    obj.code(),
                    obj.message(),
                ));
            }
            Err(other) => {
                return Err(classify_client_error(&url, args.timeout_ms, other));
            }
        };
        if h.in_flight_streams == 0 {
            emit_drain_complete(&mut io::stdout().lock(), args.json, false)
                .context("failed to write drain-complete output")?;
            return Ok(());
        }
        check_deadline_and_sleep_with_count(
            deadline,
            poll_interval,
            args.wait_timeout_secs,
            h.in_flight_streams,
        )
        .await?;
    }
}

/// Sleep until `poll_interval` elapses or `deadline` is reached,
/// whichever fires first; on deadline overrun, return `Err` with the
/// "no in-flight count known" timeout shape. Sleep is racy with
/// `ctrl_c` so an operator can interrupt a long wait without
/// `SIGKILL`ing the process.
async fn check_deadline_and_sleep(
    deadline: std::time::Instant,
    poll_interval: Duration,
    wait_timeout_secs: NonZeroU64,
) -> anyhow::Result<()> {
    if std::time::Instant::now() >= deadline {
        let _ = writeln!(
            io::stderr().lock(),
            "drain_timeout=true wait_timeout_secs={wait_timeout_secs}"
        );
        return Err(anyhow::anyhow!(
            "drain --wait timed out after {wait_timeout_secs}s"
        ));
    }
    sleep_or_ctrl_c(poll_interval).await
}

/// Variant of [`check_deadline_and_sleep`] used when the last
/// successful health response is available, so the timeout diagnostic
/// can report the last-known in-flight count to the operator.
async fn check_deadline_and_sleep_with_count(
    deadline: std::time::Instant,
    poll_interval: Duration,
    wait_timeout_secs: NonZeroU64,
    in_flight: u64,
) -> anyhow::Result<()> {
    if std::time::Instant::now() >= deadline {
        let _ = writeln!(
            io::stderr().lock(),
            "drain_timeout=true in_flight_streams={in_flight} \
             wait_timeout_secs={wait_timeout_secs}"
        );
        return Err(anyhow::anyhow!(
            "drain --wait timed out after {wait_timeout_secs}s; \
             {in_flight} stream(s) still in flight"
        ));
    }
    sleep_or_ctrl_c(poll_interval).await
}

/// Sleep for `dur` but resolve early on Ctrl-C, returning `Err` so
/// the polling loop can short-circuit with a clean exit. Without the
/// `ctrl_c` race, a long `--wait-poll-ms` would leave the operator
/// wedged for the remainder of the tick before they could escape.
async fn sleep_or_ctrl_c(dur: Duration) -> anyhow::Result<()> {
    tokio::select! {
        () = tokio::time::sleep(dur) => Ok(()),
        result = tokio::signal::ctrl_c() => {
            result.context("ctrl_c handler failed")?;
            let _ = writeln!(
                io::stderr().lock(),
                "drain --wait interrupted by Ctrl-C; drain continues server-side \
                 (use `decdn node health` to observe completion)",
            );
            Err(anyhow::anyhow!("drain --wait interrupted"))
        }
    }
}

/// Write the unified terminal-success record for `decdn node drain
/// --wait` to `w`. Both the polled-to-zero and ECONNREFUSED branches
/// share the same `{drain_complete, in_flight_streams, admin_closed}`
/// shape so machine consumers don't have to parse two unrelated
/// schemas depending on which path fired (#662 review). Same
/// `&mut impl io::Write` pattern as [`write_peers_table`] so tests
/// can assert exact bytes without a stdout capture.
fn emit_drain_complete(w: &mut impl io::Write, json: bool, admin_closed: bool) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "drain_complete": true,
            "in_flight_streams": 0,
            "admin_closed": admin_closed,
        });
        writeln!(w, "{value}")
    } else {
        writeln!(
            w,
            "drain_complete=true in_flight_streams=0 admin_closed={admin_closed}"
        )
    }
}

/// `decdn node peers`: call `admin_v1_peersList` on the running node and
/// print the result.
pub async fn peers(args: &cli::PeersArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: PeersResponse = client
        .peers_list()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    let filtered = filter_peers(parsed.peers, args.region.as_deref());

    if args.json {
        let pretty = render_json(&filtered).context("failed to encode filtered peers as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_peers_table(&mut stdout, &filtered, wall_clock_us())
            .context("failed to write peers table")?;
    }

    Ok(())
}

/// `decdn node status`: call `admin_v1_status` on the running node and
/// print a snapshot of its DHT participation health (issue #741).
pub async fn status(args: &cli::StatusArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: StatusResponse = client
        .status()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode status as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_status(&mut stdout, &parsed, wall_clock_us())
            .context("failed to write status report")?;
    }

    Ok(())
}

/// `decdn node channels`: call `admin_v1_channels` on the running node and
/// print a snapshot of its open payment channels (issue #749).
pub async fn channels(
    args: &cli::ChannelsArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: ChannelsResponse = client
        .channels()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode channels as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_channels_table(&mut stdout, &parsed).context("failed to write channels table")?;
    }

    Ok(())
}

/// Write the payment-channel table to `w`. Pure function (takes `&mut impl
/// Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_peers_table`] / [`write_status`]. A summary line
/// carries the redemption threshold as a stable `key=value` token; the
/// per-channel table follows. USDC amounts are rendered from micro-USDC.
fn write_channels_table(w: &mut impl io::Write, resp: &ChannelsResponse) -> io::Result<()> {
    writeln!(
        w,
        "redeem_threshold={} channels={}",
        format_usdc(resp.redeem_threshold_micro_usdc),
        resp.channels.len(),
    )?;
    if resp.channels.is_empty() {
        return writeln!(w, "(no open channels)");
    }
    // Fixed-column layout: channel preview | counterparty preview | nonce |
    // outstanding | deposit | last-voucher age | eligible.
    writeln!(
        w,
        "{:<14} {:<14} {:>6} {:>12} {:>12} {:>12} ELIGIBLE",
        "CHANNEL", "COUNTERPARTY", "NONCE", "OUTSTANDING", "DEPOSIT", "LAST_VOUCHER",
    )?;
    for c in &resp.channels {
        write_channel_row(w, c)?;
    }
    Ok(())
}

/// Render one channel as a fixed-column row. Split out so the column
/// formatting stays in one place and the loop body reads as a single call.
fn write_channel_row(w: &mut impl io::Write, c: &ChannelSnapshot) -> io::Result<()> {
    let channel = short_node_id(&c.channel_id);
    let counterparty = short_node_id(&c.counterparty);
    let last_voucher = match c.seconds_since_last_voucher {
        // No voucher seen since this process started — distinct from
        // "<1s ago" so operators know the activity clock has no record
        // (a freshly-restarted node, or a channel that has never billed).
        None => "never".to_string(),
        // Reuse `format_age` (microsecond input) by scaling the whole-second
        // wire value; `saturating_mul` clamps the (unrealistic) overflow on an
        // absurd age rather than panicking. Boundary buckets are identical
        // (covered by `format_age_units`).
        Some(secs) => format_age(secs.saturating_mul(1_000_000)),
    };
    let eligible = if c.settlement_eligible { "yes" } else { "no" };
    writeln!(
        w,
        "{channel:<14} {counterparty:<14} {:>6} {:>12} {:>12} {last_voucher:>12} {eligible}",
        c.last_nonce,
        format_usdc(c.outstanding_micro_usdc),
        format_usdc(c.deposit_micro_usdc),
    )
}

/// `decdn node region-stats`: call `admin_v1_regionStats` on the running node
/// and print cumulative per-region bandwidth (issue #750).
pub async fn region_stats(
    args: &cli::RegionStatsArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: RegionStatsResponse = client
        .region_stats()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty = serde_json::to_string_pretty(&parsed)
            .context("failed to encode region stats as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_region_stats_table(&mut stdout, &parsed)
            .context("failed to write region stats table")?;
    }

    Ok(())
}

/// `decdn node reputation <node-id>`: call `admin_v1_reputation` and print the
/// queried peer's network score, scored-flag, and per-region coverage (#326).
pub async fn reputation(
    args: &cli::ReputationArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: ReputationResponse = client
        .reputation(ReputationRequest {
            node_id: args.node_id.clone(),
        })
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode reputation as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_reputation(&mut stdout, &parsed).context("failed to write reputation report")?;
    }

    Ok(())
}

/// Write the reputation report to `w`. Pure function so the formatting is
/// unit-testable without an HTTP hop.
fn write_reputation(w: &mut impl io::Write, resp: &ReputationResponse) -> io::Result<()> {
    writeln!(w, "network_score={:.6}", resp.network_score)?;
    writeln!(w, "scored={}", resp.scored)?;
    writeln!(w, "regions={}", resp.regions.len())?;
    if resp.regions.is_empty() {
        return writeln!(w, "(no coverage signal)");
    }
    writeln!(w, "{:<8} {:>10}", "REGION", "COVERAGE")?;
    for r in &resp.regions {
        writeln!(w, "{:<8} {:>10.6}", r.region, r.score)?;
    }
    Ok(())
}

/// Write the per-region bandwidth table to `w`. Pure function (takes
/// `&mut impl Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_channels_table`]. Byte counts are raw decimal so a
/// scraping script sees a stable shape.
fn write_region_stats_table(w: &mut impl io::Write, resp: &RegionStatsResponse) -> io::Result<()> {
    writeln!(w, "regions={}", resp.regions.len())?;
    if resp.regions.is_empty() {
        return writeln!(w, "(no region data)");
    }
    writeln!(w, "{:<8} {:>16} {:>16}", "REGION", "BYTES_IN", "BYTES_OUT")?;
    for r in &resp.regions {
        writeln!(w, "{:<8} {:>16} {:>16}", r.region, r.bytes_in, r.bytes_out)?;
    }
    Ok(())
}

/// Format a micro-USDC amount as a `"N.NNNNNN"` USDC string. USDC has 6
/// decimals (ADR 003), so 1 USDC == `1_000_000` micro-USDC. Trailing
/// fractional zeros are kept fixed-width (six places) so columns align and
/// a script parsing the value sees a stable shape.
fn format_usdc(micro: u64) -> String {
    let whole = micro / 1_000_000;
    let frac = micro % 1_000_000;
    format!("{whole}.{frac:06}")
}

/// Map a `jsonrpsee` client error into the three operator-actionable
/// classes the previous reqwest path exposed:
///
/// - `Transport` with an `ECONNREFUSED` in the source chain → "admin
///   isn't there" (check it's running / port).
/// - `RequestTimeout` → "admin is slow" (stuck lock, overloaded).
/// - `Call` → the server returned a JSON-RPC application error; surface
///   the code and message so the operator can tell a misconfigured
///   method name from a real server failure.
/// - Anything else → passed through with the URL as context.
fn classify_client_error(url: &str, timeout_ms: u64, err: JsonRpcClientError) -> anyhow::Error {
    match err {
        JsonRpcClientError::RequestTimeout => anyhow::anyhow!(
            "admin at {url} did not respond within {timeout_ms}ms; \
             the node may be overloaded or blocked on a long lock hold",
        ),
        JsonRpcClientError::Transport(inner) => {
            if is_connection_refused(inner.as_ref()) {
                anyhow::anyhow!(
                    "admin at {url} refused the connection ({inner}); is the node \
                     running, and is admin_port configured correctly?",
                )
            } else {
                anyhow::anyhow!("admin request to {url} failed: {inner}")
            }
        }
        JsonRpcClientError::Call(obj) => anyhow::anyhow!(
            "admin at {url} returned JSON-RPC error {code}: {msg}",
            code = obj.code(),
            msg = obj.message(),
        ),
        other => anyhow::Error::new(other).context(format!("admin request to {url} failed")),
    }
}

/// Walk the `source()` chain looking for an `std::io::Error` of kind
/// `ConnectionRefused`. The hyper/jsonrpsee error hierarchy is several
/// layers deep and the exact intermediate types are implementation
/// details, so match on the innermost `io::Error` kind instead of any
/// particular transport type.
fn is_connection_refused(err: &(dyn std::error::Error + 'static)) -> bool {
    io_error_kind_in_source_chain(err, |kind| kind == std::io::ErrorKind::ConnectionRefused)
}

/// Walk the `source()` chain looking for any `std::io::Error` whose
/// kind indicates the admin server's listener (or an already-accepted
/// connection) has gone away. Used by the `--wait` polling loop to
/// detect "admin closed after `router.shutdown` returned" — which is
/// the late-stop success terminal. `ConnectionRefused` is the most
/// common (new connection rejected because listener dropped), but
/// `ConnectionReset` / `ConnectionAborted` / `UnexpectedEof` also
/// occur when a poll lands mid-response as the server tears down.
/// Without these, the polling loop would treat a race-window error
/// as fatal and turn drain completion into a non-zero exit.
fn is_admin_closed_transport_error(err: &(dyn std::error::Error + 'static)) -> bool {
    io_error_kind_in_source_chain(err, |kind| {
        matches!(
            kind,
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        )
    })
}

/// Helper: walk the `source()` chain, return `true` if any layer is
/// an `io::Error` whose kind matches `predicate`.
fn io_error_kind_in_source_chain(
    err: &(dyn std::error::Error + 'static),
    predicate: impl Fn(std::io::ErrorKind) -> bool,
) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>()
            && predicate(io_err.kind())
        {
            return true;
        }
        current = e.source();
    }
    false
}

/// Resolve the admin URL in this precedence order:
///
/// 1. `--admin-url` flag or the `DECDN_ADMIN_URL` env var (clap folds the
///    env into `args.admin_url`).
/// 2. `DECDN_ADMIN_PORT` — the same env var the daemon reads for its admin
///    bind (`common/src/cli/run.rs`). An env-only deployment (port set via
///    env, no config file) would otherwise be unreachable: the CLI would
///    dial the default 9191 and report "is the node running?" (#864). A
///    malformed or `0` value errors rather than silently falling through.
/// 3. `observability.admin_port` from the TOML config file — either the
///    caller's explicit path or the default `~/.decdn/node.toml`. An
///    explicit path that doesn't exist is an error; a missing default
///    path falls through. `admin_port = 0` in the file is an operator
///    opt-out and errors here rather than silently probing the default.
/// 4. Default `http://127.0.0.1:9191`.
fn resolve_admin_url(flag: Option<&str>, config_path: Option<&Path>) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    if let Some(url) = admin_url_from_env(std::env::var("DECDN_ADMIN_PORT").ok())? {
        return Ok(url);
    }
    let (resolved_path, source) = match config_path {
        Some(p) => (Some(expand_tilde(p)), ConfigPathSource::Explicit),
        None => (
            cli::common::default_config_path(),
            ConfigPathSource::Default,
        ),
    };
    let port =
        port_from_config_file(resolved_path.as_deref(), source)?.unwrap_or(DEFAULT_ADMIN_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

/// Build the loopback admin URL from the raw `DECDN_ADMIN_PORT` env value, or
/// `None` when the var is unset. Pure (takes the value rather than reading the
/// environment) so the parse/validation logic is testable without mutating
/// process-global state. A non-numeric value or `0` (the daemon's "admin
/// disabled" sentinel) is an error — both indicate misconfiguration the
/// operator should see rather than have masked by a fall-through to the
/// config file or the built-in default.
fn admin_url_from_env(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let port: u16 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("DECDN_ADMIN_PORT is not a valid port number: {raw:?}"))?;
    if port == 0 {
        anyhow::bail!(
            "DECDN_ADMIN_PORT=0 disables the admin server; pass --admin-url or set a non-zero port"
        );
    }
    Ok(Some(format!("http://127.0.0.1:{port}")))
}

/// Read `observability.admin_port` from a TOML config file. Returns:
/// - `Ok(None)` when `path` is `None`, or when `path` is the *default*
///   path and the file doesn't exist (operator hasn't set up a config
///   file yet — fall through to the built-in default).
/// - `Ok(Some(port))` for a positive port value.
/// - `Err` if the file exists but can't be parsed, if the operator
///   explicitly set `admin_port = 0` (which disables the server), or if
///   `source` is `Explicit` and the file is missing/unreadable.
///
/// Callers are expected to have already tilde-expanded the path;
/// `resolve_admin_url` is the only intended caller and does so via
/// [`expand_tilde`] / [`cli::common::default_config_path`].
fn port_from_config_file(
    path: Option<&Path>,
    source: ConfigPathSource,
) -> anyhow::Result<Option<u16>> {
    let Some(path) = path else { return Ok(None) };
    let path: PathBuf = path.to_path_buf();
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            // Deliberately a partial deserializer: parsing the full
            // `FileConfig` would couple `decdn node peers` to every
            // unrelated field's well-formedness. An operator with a
            // typo'd `[payments]` table shouldn't lose the ability to
            // list peers. Serde's TOML mode ignores unknown fields by
            // default, so this only fails on (a) genuinely malformed
            // TOML or (b) a wrong type for `observability.admin_port`
            // itself — both of which we genuinely want to surface.
            let parsed: AdminPortConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", path.display()))?;
            match parsed.observability.and_then(|o| o.admin_port) {
                Some(0) => anyhow::bail!(
                    "config {} disables the admin server (observability.admin_port = 0); \
                     pass --admin-url or enable the admin port",
                    path.display()
                ),
                other => Ok(other),
            }
        }
        Err(err) => match (err.kind(), source) {
            // Default path not yet created: fall through to the built-in.
            (io::ErrorKind::NotFound, ConfigPathSource::Default) => Ok(None),
            // Permission problems usually point at file mode / ownership,
            // not a wrong path — give the operator that nudge rather than
            // a generic "failed to read".
            (io::ErrorKind::PermissionDenied, _) => Err(anyhow::anyhow!(
                "cannot read config file {}: permission denied; check file mode and ownership",
                path.display()
            )),
            _ => Err(anyhow::anyhow!(
                "failed to read config file {}: {err}",
                path.display()
            )),
        },
    }
}

fn filter_peers(peers: Vec<PeerView>, region: Option<&str>) -> Vec<PeerView> {
    match region {
        None => peers,
        Some(want) => peers
            .into_iter()
            .filter(|p| p.region.eq_ignore_ascii_case(want))
            .collect(),
    }
}

/// Render the (possibly filtered) peer list as pretty JSON.
///
/// Kept as a pure function so tests can round-trip filter-then-render
/// without an HTTP hop; the filter's effect on the `--json` output is
/// otherwise only observable at the shell level.
fn render_json(peers: &[PeerView]) -> anyhow::Result<String> {
    let out = PeersResponse {
        peers: peers.to_vec(),
    };
    serde_json::to_string_pretty(&out).context("encode peers as JSON")
}

/// Write the peer table to `w`. Taking `&mut impl Write` instead of
/// writing directly to `stdout` makes the formatter testable and makes
/// it a straightforward component for future TUI consumers.
fn write_peers_table(w: &mut impl io::Write, peers: &[PeerView], now_us: u64) -> io::Result<()> {
    if peers.is_empty() {
        return writeln!(w, "(no peers known)");
    }
    // Fixed-column layout: 14 (node_id preview) | 8 (region) | rest (last_seen).
    let (node_hdr, region_hdr, last_hdr) = ("NODE_ID", "REGION", "LAST_SEEN");
    writeln!(w, "{node_hdr:<14} {region_hdr:<8} {last_hdr}")?;
    for p in peers {
        let preview = short_node_id(&p.node_id);
        let age = relative_age(now_us, p.last_seen_us);
        let region = truncate(&p.region, 8);
        writeln!(w, "{preview:<14} {region:<8} {age}")?;
    }
    Ok(())
}

fn short_node_id(hex: &str) -> String {
    // Unicode '…' (U+2026) rather than "..." so a pasted preview is
    // unambiguously a preview and never parses as hex.
    let mut chars = hex.chars();
    let prefix: String = chars.by_ref().take(12).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn truncate(s: &str, max: usize) -> String {
    let mut chars = s.chars();
    let mut out: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        out.push('…');
    }
    out
}

fn wall_clock_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// Coarse operator-facing age bucket. On-call use case is "is this peer
/// fresh in the last N {s,m,h,d}?"; sub-second precision would only add
/// noise to the table.
fn relative_age(now_us: u64, last_seen_us: u64) -> String {
    if last_seen_us == 0 {
        return "unknown".to_string();
    }
    if last_seen_us > now_us {
        // Clock skew: report as "in the future" rather than a huge negative
        // unsigned delta. Honest about the condition without panicking.
        return "in future".to_string();
    }
    let delta_us = now_us - last_seen_us;
    format_age(delta_us)
}

fn format_age(delta_us: u64) -> String {
    const US_PER_SEC: u64 = 1_000_000;
    const US_PER_MIN: u64 = 60 * US_PER_SEC;
    const US_PER_HOUR: u64 = 60 * US_PER_MIN;
    const US_PER_DAY: u64 = 24 * US_PER_HOUR;
    if delta_us < US_PER_SEC {
        return "<1s ago".to_string();
    }
    if delta_us < US_PER_MIN {
        return format!("{}s ago", delta_us / US_PER_SEC);
    }
    if delta_us < US_PER_HOUR {
        return format!("{}m ago", delta_us / US_PER_MIN);
    }
    if delta_us < US_PER_DAY {
        return format!("{}h ago", delta_us / US_PER_HOUR);
    }
    format!("{}d ago", delta_us / US_PER_DAY)
}

/// Write the DHT status report to `w`. Pure function (takes `&mut impl
/// Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_peers_table`]. The summary block uses stable
/// `key=value` tokens so operator scripts can `grep` them without
/// `--json`; the per-non-empty-bucket fill table follows.
fn write_status(w: &mut impl io::Write, s: &StatusResponse, now_us: u64) -> io::Result<()> {
    writeln!(w, "node_id={}", s.node_id)?;
    writeln!(w, "known_stakers={}", s.known_stakers)?;
    let last_refresh = match s.routing.last_refresh_us {
        // No bucket-refresh pass has completed yet (node up < one interval).
        None => "never".to_string(),
        Some(us) => relative_age(now_us, us),
    };
    writeln!(
        w,
        "routing total_peers={} non_empty_buckets={} refresh_interval={} last_refresh={}",
        s.routing.total_peers,
        s.routing.non_empty_buckets,
        format_interval(s.routing.refresh_interval_s),
        last_refresh,
    )?;
    writeln!(
        w,
        "record_store records={}/{} ({})",
        s.record_store.records,
        s.record_store.capacity,
        percent(s.record_store.records, s.record_store.capacity),
    )?;
    writeln!(
        w,
        "republish scheduled_records={}",
        s.republish.scheduled_records
    )?;

    if s.routing.buckets.is_empty() {
        return writeln!(w, "(routing table empty — no buckets populated)");
    }
    // Capacity is the same K for every bucket — carried once on RoutingHealth.
    let capacity = s.routing.bucket_capacity;
    writeln!(w)?;
    writeln!(w, "{:<8} {:<8} FILL%", "BUCKET", "FILL")?;
    for b in &s.routing.buckets {
        let fill = format!("{}/{capacity}", b.fill);
        let pct = percent(u64::from(b.fill), u64::from(capacity));
        writeln!(w, "{:<8} {fill:<8} {pct}", b.index)?;
    }
    Ok(())
}

/// Integer percentage of `num/den` as an `"NN%"` string. A `den` of 0
/// (which should never happen for a Kademlia bucket capacity or the
/// record-store global cap) renders `"n/a"` rather than dividing by zero.
/// `saturating_mul` guards the (unrealistic) `num * 100` overflow.
fn percent(num: u64, den: u64) -> String {
    if den == 0 {
        return "n/a".to_string();
    }
    format!("{}%", num.saturating_mul(100) / den)
}

/// Format a whole-second interval as a coarse human string (e.g. `"1h"`,
/// `"30m"`, `"45s"`). Uses the same unit set as `format_age` (s/m/h/d) but
/// selects the coarsest unit that divides *exactly* — so a non-round
/// interval like 90 minutes renders `"90m"`, not `"1h"` — keeping the
/// reported bucket-refresh cadence precise.
fn format_interval(secs: u64) -> String {
    const SEC_PER_MIN: u64 = 60;
    const SEC_PER_HOUR: u64 = 60 * SEC_PER_MIN;
    const SEC_PER_DAY: u64 = 24 * SEC_PER_HOUR;
    if secs == 0 {
        return "0s".to_string();
    }
    if secs.is_multiple_of(SEC_PER_DAY) {
        return format!("{}d", secs / SEC_PER_DAY);
    }
    if secs.is_multiple_of(SEC_PER_HOUR) {
        return format!("{}h", secs / SEC_PER_HOUR);
    }
    if secs.is_multiple_of(SEC_PER_MIN) {
        return format!("{}m", secs / SEC_PER_MIN);
    }
    format!("{secs}s")
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    /// `emit_drain_complete` JSON shape, polled-to-zero branch:
    /// `admin_closed=false`. Asserts the exact JSON object so
    /// machine consumers can rely on a fixed schema across both
    /// terminal-success paths (#662 review).
    #[test]
    fn emit_drain_complete_json_admin_closed_false() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, true, false).expect("write succeeds");
        let s = String::from_utf8(buf).expect("utf8");
        let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "drain_complete": true,
                "in_flight_streams": 0,
                "admin_closed": false,
            })
        );
    }

    /// JSON shape, ECONNREFUSED branch: `admin_closed=true`.
    #[test]
    fn emit_drain_complete_json_admin_closed_true() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, true, true).expect("write succeeds");
        let s = String::from_utf8(buf).expect("utf8");
        let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "drain_complete": true,
                "in_flight_streams": 0,
                "admin_closed": true,
            })
        );
    }

    /// Plain-text shape, both branches. Operator scripts parse the
    /// `key=value` form via grep, so the exact byte layout is a
    /// public contract — guard it with a literal assertion.
    #[test]
    fn emit_drain_complete_plain_shapes() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, false, false).expect("write succeeds");
        assert_eq!(
            String::from_utf8(buf).expect("utf8"),
            "drain_complete=true in_flight_streams=0 admin_closed=false\n",
        );

        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, false, true).expect("write succeeds");
        assert_eq!(
            String::from_utf8(buf).expect("utf8"),
            "drain_complete=true in_flight_streams=0 admin_closed=true\n",
        );
    }

    /// `is_admin_closed_transport_error` walks the `source()` chain
    /// and matches the four io kinds that indicate the admin
    /// listener / accepted socket went away. A regression that
    /// dropped one of the kinds would falsely turn a race-window
    /// close into a non-zero exit from the `--wait` polling loop.
    #[test]
    fn is_admin_closed_transport_error_matches_close_like_kinds() {
        use std::io::{Error, ErrorKind};
        for kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::UnexpectedEof,
        ] {
            let io_err = Error::new(kind, "boom");
            // Wrap to exercise the source-chain walk, not a direct
            // downcast — production errors are typically several
            // layers deep (hyper / jsonrpsee / tower).
            let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
            assert!(
                is_admin_closed_transport_error(wrapped.as_ref()),
                "kind {kind:?} must be classified as admin-closed",
            );
        }
        // Unrelated kinds must not match.
        for kind in [
            ErrorKind::Other,
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
        ] {
            let io_err = Error::new(kind, "boom");
            let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
            assert!(
                !is_admin_closed_transport_error(wrapped.as_ref()),
                "kind {kind:?} must not be classified as admin-closed",
            );
        }
    }

    /// `is_connection_refused` keeps the original narrow semantics
    /// — only `ConnectionRefused` matches. Used by the non-wait
    /// `classify_client_error` path; the wider helper is only for
    /// the `--wait` poll loop.
    #[test]
    fn is_connection_refused_only_matches_refused() {
        use std::io::{Error, ErrorKind};
        let refused: Box<dyn std::error::Error + 'static> =
            Box::new(Error::new(ErrorKind::ConnectionRefused, "x"));
        assert!(is_connection_refused(refused.as_ref()));
        let reset: Box<dyn std::error::Error + 'static> =
            Box::new(Error::new(ErrorKind::ConnectionReset, "x"));
        assert!(!is_connection_refused(reset.as_ref()));
    }

    fn mk_peer(node_id: &str, region: &str, last_seen_us: u64) -> PeerView {
        PeerView {
            node_id: node_id.to_string(),
            region: region.to_string(),
            first_seen_us: last_seen_us,
            last_seen_us,
            announced_at_us: last_seen_us,
        }
    }

    #[test]
    fn filter_by_region_is_case_insensitive() {
        let peers = vec![
            mk_peer("aa", "US", 10),
            mk_peer("bb", "us", 20),
            mk_peer("cc", "EU", 30),
        ];
        let filtered = filter_peers(peers, Some("US"));
        // Assert exact surviving ids so a regression that filtered on the
        // wrong field (e.g. node_id vs region) can't produce a matching
        // count by accident.
        let ids: Vec<&str> = filtered.iter().map(|p| p.node_id.as_str()).collect();
        assert_eq!(ids, vec!["aa", "bb"]);
    }

    #[test]
    fn filter_none_is_passthrough() {
        let peers = vec![mk_peer("aa", "US", 10)];
        assert_eq!(filter_peers(peers.clone(), None).len(), peers.len());
    }

    #[test]
    fn render_json_roundtrips_through_filter() -> anyhow::Result<()> {
        let peers = vec![
            mk_peer("aa", "US", 10),
            mk_peer("bb", "EU", 20),
            mk_peer("cc", "US", 30),
        ];
        let filtered = filter_peers(peers, Some("US"));
        let pretty = render_json(&filtered)?;
        let value: serde_json::Value = serde_json::from_str(&pretty)?;
        let out_peers = value["peers"].as_array().expect("peers array");
        assert_eq!(out_peers.len(), 2);
        let out_ids: Vec<&str> = out_peers
            .iter()
            .map(|p| p["node_id"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(out_ids, vec!["aa", "cc"]);
        Ok(())
    }

    #[test]
    fn write_peers_table_empty_emits_sentinel() -> anyhow::Result<()> {
        let mut buf = Vec::<u8>::new();
        write_peers_table(&mut buf, &[], 1_000)?;
        let s = String::from_utf8(buf)?;
        assert_eq!(s, "(no peers known)\n");
        Ok(())
    }

    #[test]
    fn write_peers_table_renders_header_and_row() -> anyhow::Result<()> {
        let peer = mk_peer(&"a".repeat(64), "US", 1_000_000); // last_seen = 1s past epoch
        let now_us = 2_000_000; // 1s after last_seen → "1s ago"
        let mut buf = Vec::<u8>::new();
        write_peers_table(&mut buf, &[peer], now_us)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("NODE_ID"), "header missing: {s}");
        assert!(s.contains("LAST_SEEN"), "header missing: {s}");
        assert!(s.contains("aaaaaaaaaaaa…"), "node_id preview missing: {s}");
        assert!(s.contains("US"), "region missing: {s}");
        assert!(s.contains("1s ago"), "relative age missing: {s}");
        Ok(())
    }

    fn mk_status(last_refresh_us: Option<u64>) -> StatusResponse {
        use decdn_common::admin::{BucketStat, RecordStoreHealth, RepublishHealth, RoutingHealth};
        StatusResponse {
            node_id: "ab".repeat(32),
            routing: RoutingHealth {
                total_peers: 21,
                non_empty_buckets: 2,
                buckets: vec![
                    BucketStat { index: 0, fill: 1 },
                    BucketStat {
                        index: 255,
                        fill: 20,
                    },
                ],
                bucket_capacity: 20,
                refresh_interval_s: 3_600,
                last_refresh_us,
            },
            known_stakers: 7,
            record_store: RecordStoreHealth {
                records: 50_000,
                capacity: 100_000,
            },
            republish: RepublishHealth {
                scheduled_records: 5,
            },
        }
    }

    #[test]
    fn write_status_renders_summary_and_bucket_table() -> anyhow::Result<()> {
        // last_seen 1s past epoch, now 1s later → "1s ago".
        let status = mk_status(Some(1_000_000));
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains(&format!("node_id={}", "ab".repeat(32))), "{s}");
        assert!(s.contains("known_stakers=7"), "{s}");
        assert!(s.contains("total_peers=21"), "{s}");
        assert!(s.contains("non_empty_buckets=2"), "{s}");
        assert!(s.contains("refresh_interval=1h"), "{s}");
        assert!(s.contains("last_refresh=1s ago"), "{s}");
        // Record-store utilization: 50000/100000 → 50%.
        assert!(s.contains("record_store records=50000/100000 (50%)"), "{s}");
        assert!(s.contains("republish scheduled_records=5"), "{s}");
        // Bucket table: header + a full bucket at 100%.
        assert!(s.contains("BUCKET"), "{s}");
        assert!(s.contains("FILL%"), "{s}");
        assert!(s.contains("20/20"), "{s}");
        assert!(s.contains("100%"), "{s}");
        Ok(())
    }

    #[test]
    fn write_status_never_refreshed_renders_never() -> anyhow::Result<()> {
        let status = mk_status(None);
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("last_refresh=never"), "{s}");
        Ok(())
    }

    /// Cold-start: a node that has joined no buckets yet renders the
    /// empty-table sentinel and omits the bucket table header entirely —
    /// the precise scenario `decdn node status` exists to diagnose.
    #[test]
    fn write_status_empty_routing_table_renders_sentinel() -> anyhow::Result<()> {
        let mut status = mk_status(None);
        status.routing.buckets.clear();
        status.routing.non_empty_buckets = 0;
        status.routing.total_peers = 0;
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("(routing table empty"), "{s}");
        assert!(
            !s.contains("BUCKET"),
            "empty table must omit the header: {s}"
        );
        assert!(!s.contains("FILL%"), "{s}");
        Ok(())
    }

    /// Clock skew between the node's stamp and the CLI's wall clock must
    /// render "in future" via `relative_age`, not a huge wrapped age.
    #[test]
    fn write_status_future_last_refresh_renders_in_future() -> anyhow::Result<()> {
        let status = mk_status(Some(5_000_000));
        let mut buf = Vec::<u8>::new();
        // now_us earlier than the stamp.
        write_status(&mut buf, &status, 1_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("last_refresh=in future"), "{s}");
        Ok(())
    }

    #[test]
    fn percent_handles_zero_denominator() {
        assert_eq!(percent(0, 0), "n/a");
        assert_eq!(percent(5, 0), "n/a");
        assert_eq!(percent(1, 20), "5%");
        assert_eq!(percent(20, 20), "100%");
    }

    #[test]
    fn format_interval_uses_coarsest_exact_unit() {
        assert_eq!(format_interval(0), "0s");
        assert_eq!(format_interval(45), "45s");
        assert_eq!(format_interval(30 * 60), "30m");
        assert_eq!(format_interval(3_600), "1h");
        assert_eq!(format_interval(2 * 86_400), "2d");
        // 90 minutes isn't a whole number of hours → falls back to minutes.
        assert_eq!(format_interval(90 * 60), "90m");
    }

    #[test]
    fn resolve_admin_url_prefers_flag() {
        let got = resolve_admin_url(Some("http://custom:1234"), None).expect("flag path ok");
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn admin_url_from_env_unset_is_none() {
        assert_eq!(admin_url_from_env(None).expect("unset ok"), None);
    }

    #[test]
    fn admin_url_from_env_valid_port_builds_loopback_url() {
        assert_eq!(
            admin_url_from_env(Some("9999".to_string())).expect("valid ok"),
            Some("http://127.0.0.1:9999".to_string())
        );
        // Surrounding whitespace is tolerated (env values can carry it).
        assert_eq!(
            admin_url_from_env(Some(" 9999 ".to_string())).expect("trimmed ok"),
            Some("http://127.0.0.1:9999".to_string())
        );
    }

    #[test]
    fn admin_url_from_env_zero_and_malformed_error() {
        assert!(admin_url_from_env(Some("0".to_string())).is_err());
        assert!(admin_url_from_env(Some("notaport".to_string())).is_err());
        // Out of u16 range.
        assert!(admin_url_from_env(Some("70000".to_string())).is_err());
    }

    #[test]
    fn port_from_config_file_missing_default_returns_none() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            None
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_missing_explicit_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        let err = port_from_config_file(Some(&path), ConfigPathSource::Explicit)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected explicit-missing error"))?
            .to_string();
        assert!(
            err.contains("failed to read config file"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_default_path() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 12345\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(12345)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_explicit_path() -> anyhow::Result<()> {
        // Explicit-source + valid file must succeed the same way as the
        // default-source case. Without this test, a regression that
        // broadened the explicit-source error arm to swallow successes
        // would still pass CI.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 7777\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Explicit)?,
            Some(7777)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_explicit_zero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 0\n")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error for explicit 0"))?
            .to_string();
        assert!(
            err.contains("disables the admin server"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_invalid_toml() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, b"not = valid = toml")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse error"))?
            .to_string();
        assert!(err.contains("parse"), "missing context: {err}");
        Ok(())
    }

    // Locks in the partial-deserializer choice: a wrong type in some
    // unrelated section (here, a malformed `[network]` field that the
    // full FileConfig would reject) must not stop `decdn node peers`
    // from resolving the admin port. If a future refactor reverts to
    // parsing FileConfig, this test fails.
    #[test]
    fn port_from_config_file_ignores_unrelated_field_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        // `network.bind_addr` would be a string in the real schema;
        // making it an integer is a guaranteed type-mismatch for the
        // full FileConfig, but the narrow AdminPortConfig deserializer
        // never sees `network` so it must round-trip fine.
        std::fs::write(
            &path,
            b"[observability]\nadmin_port = 4242\n[network]\nbind_addr = 7\n",
        )?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(4242)
        );
        Ok(())
    }

    fn mk_channel(
        channel_id: &str,
        counterparty: &str,
        last_nonce: u64,
        outstanding: u64,
        deposit: u64,
        secs_since: Option<u64>,
        eligible: bool,
    ) -> ChannelSnapshot {
        ChannelSnapshot {
            channel_id: channel_id.to_string(),
            counterparty: counterparty.to_string(),
            last_nonce,
            outstanding_micro_usdc: outstanding,
            deposit_micro_usdc: deposit,
            seconds_since_last_voucher: secs_since,
            settlement_eligible: eligible,
        }
    }

    #[test]
    fn region_stats_table_renders_rows_and_empty_sentinel() {
        use decdn_common::admin::{RegionBytes, RegionStatsResponse};

        let mut buf = Vec::new();
        write_region_stats_table(
            &mut buf,
            &RegionStatsResponse {
                regions: vec![
                    RegionBytes {
                        region: "DE".to_string(),
                        bytes_in: 0,
                        bytes_out: 1_048_576,
                    },
                    RegionBytes {
                        region: "UNKNOWN".to_string(),
                        bytes_in: 2_097_152,
                        bytes_out: 0,
                    },
                ],
            },
        )
        .expect("write table");
        let out = String::from_utf8(buf).expect("utf8");
        assert!(out.contains("regions=2"), "summary count line: {out}");
        assert!(
            out.contains("REGION") && out.contains("BYTES_IN") && out.contains("BYTES_OUT"),
            "column headers: {out}"
        );
        assert!(out.contains("DE"), "table must list DE: {out}");
        assert!(out.contains("UNKNOWN"), "table must list UNKNOWN: {out}");
        // Raw decimal byte values are rendered verbatim (stable for scrapers).
        assert!(out.contains("1048576"), "DE bytes_out value: {out}");
        assert!(out.contains("2097152"), "UNKNOWN bytes_in value: {out}");

        let mut empty = Vec::new();
        write_region_stats_table(&mut empty, &RegionStatsResponse { regions: vec![] })
            .expect("write empty");
        let out = String::from_utf8(empty).expect("utf8");
        assert!(out.contains("(no region data)"), "empty sentinel: {out}");
    }

    #[test]
    fn write_reputation_renders_score_and_regions() {
        use decdn_common::admin::ReputationCoverage;
        let mut buf = Vec::new();
        write_reputation(
            &mut buf,
            &ReputationResponse {
                network_score: 0.625,
                scored: true,
                regions: vec![
                    ReputationCoverage {
                        region: "DE".to_string(),
                        score: 0.8,
                    },
                    ReputationCoverage {
                        region: "US".to_string(),
                        score: 0.55,
                    },
                ],
            },
        )
        .expect("write reputation");
        let out = String::from_utf8(buf).expect("utf8");
        assert!(out.contains("network_score=0.625000"), "score line: {out}");
        assert!(out.contains("scored=true"), "scored line: {out}");
        assert!(out.contains("regions=2"), "region count: {out}");
        assert!(
            out.contains("REGION") && out.contains("COVERAGE"),
            "headers: {out}"
        );
        assert!(
            out.contains("DE") && out.contains("US"),
            "region rows: {out}"
        );
    }

    #[test]
    fn write_reputation_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_reputation(
            &mut buf,
            &ReputationResponse {
                network_score: 0.5,
                scored: false,
                regions: vec![],
            },
        )
        .expect("write reputation");
        let out = String::from_utf8(buf).expect("utf8");
        assert!(
            out.contains("(no coverage signal)"),
            "empty sentinel: {out}"
        );
    }

    #[test]
    fn write_channels_table_empty_emits_sentinel() -> anyhow::Result<()> {
        let resp = ChannelsResponse {
            channels: Vec::new(),
            redeem_threshold_micro_usdc: 1_000_000,
        };
        let mut buf = Vec::<u8>::new();
        write_channels_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        // Summary line still prints the threshold, then the sentinel.
        assert!(s.contains("redeem_threshold=1.000000"), "{s}");
        assert!(s.contains("channels=0"), "{s}");
        assert!(s.contains("(no open channels)"), "{s}");
        // No table header when there are no rows.
        assert!(!s.contains("CHANNEL"), "header must be omitted: {s}");
        Ok(())
    }

    #[test]
    fn write_channels_table_renders_header_and_rows() -> anyhow::Result<()> {
        // The DTO documents `counterparty` as an EIP-55 mixed-case
        // checksummed address (`alloy`'s `Address` Display), so the
        // fixture must be a real checksummed string — a lowercase
        // placeholder wouldn't exercise the mixed-case rendering the
        // table inherits verbatim. Derive it via `alloy` so the literal
        // is provably the canonical checksum, not a hand-typed guess.
        let counterparty =
            alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
        // Sanity-guard the fixture itself: the checksum is genuinely
        // mixed-case (some hex letters upper, some lower), so a regression
        // that lowercased it before rendering would be caught below.
        assert_ne!(
            counterparty,
            counterparty.to_lowercase(),
            "fixture must be a mixed-case EIP-55 address: {counterparty}"
        );
        let resp = ChannelsResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            channels: vec![
                mk_channel(
                    &format!("0x{}", "a".repeat(64)),
                    &counterparty,
                    7,
                    2_500_000,
                    10_000_000,
                    Some(90),
                    true,
                ),
                mk_channel(
                    &format!("0x{}", "c".repeat(64)),
                    &format!("0x{}", "d".repeat(40)),
                    0,
                    0,
                    5_000_000,
                    None,
                    false,
                ),
            ],
        };
        let mut buf = Vec::<u8>::new();
        write_channels_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("channels=2"), "{s}");
        assert!(s.contains("CHANNEL"), "header missing: {s}");
        assert!(s.contains("OUTSTANDING"), "header missing: {s}");
        assert!(s.contains("ELIGIBLE"), "header missing: {s}");
        // First row: USDC-formatted amounts, 90s → "1m ago", eligible "yes".
        assert!(s.contains("2.500000"), "outstanding USDC missing: {s}");
        assert!(s.contains("10.000000"), "deposit USDC missing: {s}");
        assert!(s.contains("1m ago"), "last-voucher age missing: {s}");
        // Channel id preview is the short form.
        assert!(s.contains("0xaaaaaaaaaa"), "channel preview missing: {s}");
        // Counterparty preview is the short form AND preserves the EIP-55
        // mixed case verbatim — `short_node_id` truncates to 12 chars, so
        // assert the row carries that checksummed prefix unchanged (a
        // regression that lowercased the address would miss this).
        let cp_preview = short_node_id(&counterparty);
        assert!(
            cp_preview.chars().any(|ch| ch.is_ascii_uppercase()),
            "expected mixed-case counterparty preview: {cp_preview}"
        );
        assert!(
            s.contains(&cp_preview),
            "checksummed counterparty preview missing: {s}"
        );
        // Second row: no activity → "never", not eligible → "no".
        assert!(s.contains("never"), "never sentinel missing: {s}");
        Ok(())
    }

    #[test]
    fn format_usdc_renders_six_decimals() {
        assert_eq!(format_usdc(0), "0.000000");
        assert_eq!(format_usdc(1_000_000), "1.000000");
        assert_eq!(format_usdc(2_500_000), "2.500000");
        assert_eq!(format_usdc(1), "0.000001");
        assert_eq!(format_usdc(12_345_678), "12.345678");
    }

    /// `decdn node channels --json` serializes the `ChannelsResponse`
    /// DTO with `serde_json::to_string_pretty` (the seam the `--json`
    /// branch in [`channels`] uses). Assert the pretty encoding (a)
    /// round-trips back to the same value and (b) carries every
    /// load-bearing field with its wire key, so a rename or a
    /// skipped-field regression on the DTO breaks here rather than only
    /// at the shell. Mirrors `render_json_roundtrips_through_filter` for
    /// the peers `--json` path. The counterparty is a real EIP-55
    /// checksummed address so the JSON reflects production output.
    #[test]
    fn channels_json_pretty_roundtrips_and_carries_fields() -> anyhow::Result<()> {
        let counterparty =
            alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
        let resp = ChannelsResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            channels: vec![mk_channel(
                &format!("0x{}", "a".repeat(64)),
                &counterparty,
                7,
                2_500_000,
                10_000_000,
                Some(42),
                true,
            )],
        };

        // Exact seam the `--json` branch uses.
        let pretty = serde_json::to_string_pretty(&resp)?;
        // Pretty form is multi-line (indented) — guards against an
        // accidental switch to the compact encoder.
        assert!(
            pretty.contains('\n'),
            "pretty JSON must be multi-line: {pretty}"
        );

        // Round-trips back through the DTO with no lossy field — the
        // generated client deserializes this exact shape. (The DTO doesn't
        // derive `PartialEq`, so assert the reconstructed fields directly
        // rather than comparing whole structs.)
        let back: ChannelsResponse = serde_json::from_str(&pretty)?;
        assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
        assert_eq!(back.channels.len(), 1);
        let bc = back.channels.first().expect("one channel");
        assert_eq!(bc.counterparty, counterparty);
        assert_eq!(bc.last_nonce, 7);
        assert_eq!(bc.outstanding_micro_usdc, 2_500_000);
        assert_eq!(bc.deposit_micro_usdc, 10_000_000);
        assert_eq!(bc.seconds_since_last_voucher, Some(42));
        assert!(bc.settlement_eligible);

        // Each wire key is present with the expected value, including the
        // checksummed counterparty verbatim (mixed-case preserved).
        let value: serde_json::Value = serde_json::from_str(&pretty)?;
        assert_eq!(value["redeem_threshold_micro_usdc"], 1_000_000);
        let chans = value["channels"].as_array().expect("channels array");
        assert_eq!(chans.len(), 1);
        let c0 = &chans[0];
        assert_eq!(c0["counterparty"].as_str(), Some(counterparty.as_str()));
        assert_eq!(c0["last_nonce"], 7);
        assert_eq!(c0["outstanding_micro_usdc"], 2_500_000);
        assert_eq!(c0["deposit_micro_usdc"], 10_000_000);
        assert_eq!(c0["seconds_since_last_voucher"], 42);
        assert_eq!(c0["settlement_eligible"], true);
        Ok(())
    }

    #[test]
    fn format_age_units() {
        assert_eq!(format_age(500_000), "<1s ago");
        assert_eq!(format_age(2_000_000), "2s ago");
        assert_eq!(format_age(90 * 1_000_000), "1m ago");
        assert_eq!(format_age(2 * 3600 * 1_000_000), "2h ago");
        assert_eq!(format_age(36 * 3600 * 1_000_000), "1d ago");
    }

    #[test]
    fn relative_age_handles_future_and_zero() {
        assert_eq!(relative_age(100, 200), "in future");
        assert_eq!(relative_age(100, 0), "unknown");
    }

    #[test]
    fn short_node_id_trims_long_hex() {
        let full = "a".repeat(64);
        let s = short_node_id(&full);
        assert_eq!(s.chars().count(), 13); // 12 hex + ellipsis
        assert!(s.ends_with('…'));
    }

    #[test]
    fn short_node_id_passthrough_when_already_short() {
        let s = short_node_id("abcd");
        assert_eq!(s, "abcd");
    }

    /// `--dry-run` plain output is multi-line, key=value, grep-friendly.
    /// Asserts every load-bearing field appears on its own line so a
    /// regression that collapsed the table back to one line (or dropped
    /// e.g. `pinned=`) breaks here rather than silently eating the
    /// information the operator needs to decide whether to run the real
    /// evict.
    #[test]
    fn write_dry_run_human_emits_all_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: true,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: Some(1024),
                last_accessed_us_ago: Some(2_000_000), // 2s ago via format_age
                pinned: true,
                already_evicted: false,
                origin_kinds: vec![decdn_cache::OriginKind::Http],
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "abcd", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("hash=abcd"), "missing hash: {s}");
        assert!(s.contains("dry_run=true"), "missing dry_run tag: {s}");
        assert!(s.contains("was_present=true"), "missing was_present: {s}");
        assert!(s.contains("pinned=true"), "missing pinned: {s}");
        assert!(
            s.contains("already_evicted=false"),
            "missing already_evicted: {s}"
        );
        assert!(s.contains("size_bytes=1024"), "missing size_bytes: {s}");
        assert!(
            s.contains("last_accessed=2s ago"),
            "expected formatted last_accessed, got: {s}"
        );
        assert!(
            s.contains("origin_kinds=http"),
            "missing origin_kinds (#439, #284): {s}"
        );
        Ok(())
    }

    /// Sentinels for the "no information available" cases:
    /// `size_bytes=not_stored` and `last_accessed=never`. Distinct from
    /// "0" / "<1s ago" so an operator can tell "the engine has no
    /// record" from "the record is at the floor".
    #[test]
    fn write_dry_run_human_uses_sentinels_for_absent_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: false,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: None,
                last_accessed_us_ago: None,
                pinned: false,
                already_evicted: false,
                // Cache-only mode: no origin configured, so the
                // dry-run reports `none` rather than omitting the
                // line entirely (#439).
                origin_kinds: Vec::new(),
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "deadbeef", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(
            s.contains("size_bytes=not_stored"),
            "expected not_stored sentinel, got: {s}"
        );
        assert!(
            s.contains("origin_kinds=none"),
            "expected origin_kinds=none sentinel for cache-only mode, got: {s}"
        );
        assert!(
            s.contains("last_accessed=never"),
            "expected never sentinel, got: {s}"
        );
        Ok(())
    }

    /// Multi-origin chain (#284): the dry-run preview surfaces every
    /// configured backend kind, comma-separated in declared order, so
    /// operators evaluating worst-case egress cost across a fallback
    /// chain see the full chain length and composition rather than
    /// only the primary's kind.
    #[test]
    fn write_dry_run_human_renders_multi_origin_kinds_in_declared_order() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: true,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: Some(512),
                last_accessed_us_ago: Some(1_500_000),
                pinned: false,
                already_evicted: false,
                origin_kinds: vec![
                    decdn_cache::OriginKind::Http,
                    decdn_cache::OriginKind::S3,
                    decdn_cache::OriginKind::Filesystem,
                ],
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "cafebabe", &resp)?;
        let s = String::from_utf8(buf)?;
        // Order is operator-controlled and load-bearing — assert the
        // exact comma-separated sequence rather than just substring
        // matches, so a regression that sorts or dedupes the list is
        // caught here.
        assert!(
            s.contains("origin_kinds=http,s3,filesystem"),
            "expected ordered comma-separated chain, got: {s}"
        );
        Ok(())
    }
}
