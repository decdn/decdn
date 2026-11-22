//! `decdn probe`: ask a running node about one blob with a voucher-less
//! `cdn/client/v1` open, print its signed answer, and cancel the stream.

use std::time::Duration;

use decdn_client::OpenedStream;
use decdn_common::cli;
use decdn_incentive::capacity_bond_domain;
use iroh::EndpointAddr;

use super::fetch::{self, FetchHost, Payer};

/// Parse a user-supplied BLAKE3 hash (64 hex chars, optional `0x` prefix —
/// the same form `cache.pinned_hashes` accepts) into raw bytes.
fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!("invalid --hash {s:?}: expected 64 hex chars (BLAKE3 digest): {e}")
    })?;
    Ok(*h.as_bytes())
}

/// What `decdn probe` prints of one node's answer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProbeAnswer {
    /// The hash the node answered for, echoed from the request.
    pub(crate) hash: [u8; 32],
    /// The signed `ok`: the node serves the blob.
    pub(crate) ok: bool,
    /// The signed per-MiB rate.
    pub(crate) rate_per_mb: u64,
    /// The blob size the answer proves; `None` for a refusal.
    pub(crate) total_bytes: Option<u64>,
    /// How many discovery blocks the node says it holds.
    pub(crate) coverage_blocks: usize,
    /// The request's timestamp, echoed.
    pub(crate) timestamp_us: u64,
    /// The connection's path RTT, in milliseconds.
    pub(crate) rtt_ms: f64,
    /// The answer's `slash_sig`.
    pub(crate) slash_sig: Vec<u8>,
}

impl ProbeAnswer {
    /// The answer `opened` carries. Its RTT is `0` when the open measured
    /// none.
    fn of(opened: &OpenedStream) -> Self {
        let resp = opened.response();
        Self {
            hash: resp.body.hash,
            ok: resp.body.ok,
            rate_per_mb: resp.body.rate_per_mb,
            total_bytes: opened.proven_size().map(decdn_client::ProvenSize::get),
            coverage_blocks: opened.coverage().covered_blocks().count(),
            timestamp_us: resp.body.timestamp_us,
            rtt_ms: opened.rtt().map_or(0.0, |(ms, _)| ms),
            slash_sig: resp.slash_sig.clone(),
        }
    }
}

/// Open one `cdn/client/v1` stream to the node `--node-id` names for the
/// blob `--hash` names, print its answer, and cancel the stream.
///
/// The open is a pull's discovery open ([`decdn_client::discovery_open`]): it
/// names the payer's pool, lane key, binding and capability, asks for the
/// whole blob, and asks no pull-through. The keystore unlocks and the pool is
/// named as for `decdn get` (`Payer::discovery_ctx`); no voucher is sent and
/// no transaction. The answer is verified before it prints: its `slash_sig`
/// must recover to `--provider-address` (ADR 005 § Signer binding), its echoed hash and
/// timestamp must match the request, a size it states must carry its proof,
/// and its coverage must agree with its signed `ok`.
///
/// # Errors
///
/// A malformed hash, node id or address, unresolvable chain
/// coordinates, a keystore that does not unlock, an open that fails or times
/// out, or an answer that does not verify.
pub async fn probe(
    args: &cli::ProbeArgs,
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let common = &fetch_args(args)?;
    let pinned = fetch::discovery_options(common)?
        .pinned
        .ok_or_else(|| anyhow::anyhow!("decdn probe asks one node: pass --node-id"))?;
    let (host, grant) = FetchHost::open(common, config_path, false).await?;
    let payer = Payer::keystore(grant);
    let answered = async {
        let ctx = payer.discovery_ctx(&host).await?;
        // The endpoint is discovery-enabled, so the node id resolves even
        // with no `--addr` or relay; a direct `--addr` pins the address.
        let mut target = EndpointAddr::new(pinned.node_id);
        if let Some(addr) = pinned.addr {
            target = target.with_ip_addr(addr);
        }
        if let Some(url) = host.relays().first() {
            target = target.with_relay_url(url.clone());
        }
        let chain = &host.chain().config;
        let slash_dom =
            capacity_bond_domain(chain.governance.chain_id, chain.governance.capacity_bond);
        let opened = decdn_client::discovery_open(
            host.connections(),
            &target,
            pinned.provider,
            &ctx,
            &slash_dom,
            hash,
            decdn_protocol::client::NO_NAMESPACE,
            (0, 0),
            false,
            Duration::from_millis(args.timeout_ms),
        )
        .await?;
        let answer = ProbeAnswer::of(&opened);
        opened.cancel();
        anyhow::Ok(answer)
    }
    .await;
    host.close().await;
    let answer = answered?;

    let mut stdout = std::io::stdout().lock();
    write_probe_answer(&mut stdout, &answer, args.json)
        .map_err(|e| anyhow::anyhow!("failed to write the answer: {e}"))?;
    Ok(())
}

/// `decdn get`'s retrieval flags for `args`: the node, chain, keystore,
/// data-dir and capability flags `decdn probe` takes, and every other flag at
/// its default ([`fetch::default_fetch_args`]).
///
/// # Errors
///
/// Never in practice: the defaults parse.
pub(crate) fn fetch_args(args: &cli::ProbeArgs) -> anyhow::Result<cli::ClientFetchArgs> {
    let mut common = fetch::default_fetch_args()?;
    common.node_id = Some(args.node_id.clone());
    common.provider_address = Some(args.provider_address.clone());
    common.addr = args.addr;
    common.relay_url.clone_from(&args.relay_url);
    common.governance.clone_from(&args.governance);
    common.payment.clone_from(&args.payment);
    common
        .capacity_bond_address
        .clone_from(&args.capacity_bond_address);
    common.keystore.clone_from(&args.keystore);
    common
        .keystore_password_file
        .clone_from(&args.keystore_password_file);
    common.data_dir.clone_from(&args.data_dir);
    common.capability.clone_from(&args.capability);
    common.capability_file.clone_from(&args.capability_file);
    Ok(common)
}

/// Render an answer to `w` in pretty or JSON form.
///
/// Public-in-crate so unit tests can capture the output into a buffer and
/// assert on the wire contract — in particular the `--json` shape (key set,
/// `rtt_ms` quantization, `slash_sig` hex encoding).
pub(crate) fn write_probe_answer(
    w: &mut impl std::io::Write,
    answer: &ProbeAnswer,
    json: bool,
) -> std::io::Result<()> {
    let hash_hex = alloy::primitives::hex::encode(answer.hash);
    let slash_sig_hex = alloy::primitives::hex::encode(&answer.slash_sig);
    if json {
        // Quantize `rtt_ms` to three decimals before serialization; serde_json
        // drops insignificant trailing zeros, so the field carries at most
        // microsecond precision.
        let rtt_ms_quantized = (answer.rtt_ms * 1000.0).round() / 1000.0;
        let output = serde_json::json!({
            "hash": hash_hex,
            "ok": answer.ok,
            "rate_per_mb": answer.rate_per_mb,
            "total_bytes": answer.total_bytes,
            "coverage_blocks": answer.coverage_blocks,
            "timestamp_us": answer.timestamp_us,
            "rtt_ms": rtt_ms_quantized,
            "slash_sig": slash_sig_hex,
        });
        writeln!(w, "{output}")
    } else {
        writeln!(w, "hash:            {hash_hex}")?;
        writeln!(w, "ok:              {}", answer.ok)?;
        writeln!(w, "rate_per_mb:     {} (base units)", answer.rate_per_mb)?;
        match answer.total_bytes {
            Some(n) => writeln!(w, "total_bytes:     {n}")?,
            None => writeln!(w, "total_bytes:     (unknown)")?,
        }
        writeln!(w, "coverage_blocks: {}", answer.coverage_blocks)?;
        writeln!(w, "timestamp_us:    {} (echoed ok)", answer.timestamp_us)?;
        writeln!(w, "rtt:             {:.3} ms", answer.rtt_ms)?;
        writeln!(w, "slash_sig:       {slash_sig_hex}")
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
