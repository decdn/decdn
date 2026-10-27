//! Pull receipts (#2599): the block `decdn get` ends with, the file
//! `--receipt` writes, and `decdn receipt {verify,settle}`.
//!
//! The receipt is [`decdn_client::receipt::PullReceipt`]: per paid lane, the
//! node's first signed answer for each hash it served, the voucher the lane's
//! claim rests on and the deepest revealed preimage. `verify` re-checks every
//! signature offline. `settle` verifies the receipt, then reads
//! `PaymentPool.PoolRedeemed` for each lane and records the redemption that
//! covers its claim, or the highest one short of it; a node redeems in its own
//! sweep, so that lands some time after the pull
//! (`adr/appendix-pull-receipts.md`).

use std::io::Write;
use std::num::NonZeroU64;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Context as _;
use decdn_client::receipt::{
    ContentKind, PullReceipt, ReceiptChain, ReceiptEvidence, ReceiptLane, ReceiptSettlement,
    ReceiptSummary, ReceiptWindow,
};
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::{
    GetArgs, ReceiptArgs, ReceiptCommand, ReceiptSettleArgs, ReceiptVerifyArgs,
};
use decdn_common::config::DEFAULT_GET_LOGS_MAX_BLOCK_SPAN;
use decdn_incentive::log_scan::{ScanEvent, scan_logs};
use decdn_incentive::payment_pool::PaymentPool;

use super::bundle_pull::human_bytes;
use super::chain_ctx;
use super::civil_time::{format_rfc3339, parse_rfc3339};
use super::node::earnings_chain::{block_at_or_after, bounded, print_retry, scan_failed};
use super::quote::usdc_short;
use crate::known_chains;

/// How far before a receipt's start `settle` begins its scan by default, for
/// a client clock that runs ahead of the chain's.
const CLOCK_SKEW_SECS: u64 = 3_600;

/// Run `decdn receipt`.
///
/// # Errors
///
/// The receipt cannot be read, a check fails, or the settlement scan fails.
pub async fn receipt_dispatch(
    args: &ReceiptArgs,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        ReceiptCommand::Verify(verify) => verify_file(verify),
        ReceiptCommand::Settle(settle) => settle_file(settle, config_path).await,
    }
}

/// The current Unix time; `0` on a clock set before 1970.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The directory a receipt at `path` is written in.
fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Check that a receipt can be written at `path`, before the pull pays
/// anything: the receipt's evidence lives only in memory, and a run that
/// repeats a finished pull pays nothing and writes none.
///
/// # Errors
///
/// `path` is a directory, or its directory cannot take a temp file.
pub(crate) fn check_receipt_path(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        !path.is_dir(),
        "--receipt {} is a directory; name a file",
        path.display()
    );
    let dir = parent_dir(path);
    tempfile::NamedTempFile::new_in(dir)
        .map(drop)
        .with_context(|| {
            format!(
                "--receipt {}: cannot write a file in {}",
                path.display(),
                dir.display()
            )
        })
}

/// Whether `a` and `b` name one file: the same file on disk when both exist,
/// else the same absolute path.
fn same_path(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => match (std::path::absolute(a), std::path::absolute(b)) {
            (Ok(a), Ok(b)) => a == b,
            _ => a == b,
        },
    }
}

/// Refuse a `--receipt` path that names `output`, the file `what` describes
/// (for example "the blob lands"): the receipt would replace it, and a run
/// that paid nothing would remove it.
///
/// # Errors
///
/// `receipt` and `output` name one file.
pub(crate) fn refuse_collision(receipt: &Path, output: &Path, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !same_path(receipt, output),
        "--receipt {} is where {what}; name another file",
        receipt.display()
    );
    Ok(())
}

/// End a `decdn get` run that started at `started`: build its receipt from
/// `evidence`, print the receipt block, and write `--receipt` ([`finish_with`]).
/// Returns the run's result, as [`finish_with`] leaves it, beside the
/// receipt's summary, `None` when the run paid nothing.
///
/// The block goes to stdout after the run's summary, or to stderr when
/// stdout carries the data (`-o -`) or the JSON summary (`--json`).
pub(crate) async fn finish_get(
    args: &GetArgs,
    uri: &DecdnUri,
    evidence: &ReceiptEvidence,
    started: u64,
    result: anyhow::Result<()>,
) -> (anyhow::Result<()>, Option<ReceiptSummary>) {
    let kind = match uri.target {
        UriTarget::Blob => ContentKind::Blob,
        _ => ContentKind::Bundle,
    };
    let window = ReceiptWindow {
        started: format_rfc3339(started),
        finished: format_rfc3339(unix_now()),
    };
    let receipt = evidence
        .receipt(uri.hash, kind, uri.namespace, result.is_ok(), window)
        .await
        .filter(|r| !r.lanes.is_empty());
    let path = args.receipt.as_deref();
    let summary = receipt.as_ref().map(PullReceipt::summary);
    let result = if args.json || args.output.as_deref() == Some(Path::new("-")) {
        finish_with(
            receipt.as_ref(),
            path,
            result,
            &mut std::io::stderr().lock(),
        )
    } else {
        finish_with(
            receipt.as_ref(),
            path,
            result,
            &mut std::io::stdout().lock(),
        )
    };
    (result, summary)
}

/// Print `receipt`'s block to `out` and write it to `path`.
///
/// A run that paid nothing has no receipt and prints no block. With `path`,
/// a run that succeeded says on stderr that no receipt was written, and
/// removes a file an earlier run left at `path`, so that a script that
/// verifies `path` next does not verify the earlier run; a run that failed
/// leaves the file alone, since no script goes on to verify it. A run that
/// failed after paying still has a receipt for what it paid, marked
/// incomplete.
///
/// # Errors
///
/// `result`, or for a run that succeeded, a receipt that cannot be written to
/// `path`. A failed run keeps its own error and warns about the file. A block
/// that cannot be printed (a closed stdout) is no error: the file and the
/// pull's own result do not depend on it.
pub(crate) fn finish_with(
    receipt: Option<&PullReceipt>,
    path: Option<&Path>,
    result: anyhow::Result<()>,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let Some(receipt) = receipt else {
        if let Some(path) = path.filter(|_| result.is_ok()) {
            let removed = path.is_file() && std::fs::remove_file(path).is_ok();
            eprintln!(
                "no receipt: this run paid nothing, so {} was not written{}",
                path.display(),
                if removed {
                    " (the file an earlier run left there is removed)"
                } else {
                    ""
                }
            );
        }
        return result;
    };
    let saved = path.map(|path| (path, save(path, receipt)));
    let file = match &saved {
        None => ReceiptFile::Unrequested,
        Some((path, Ok(()))) => ReceiptFile::Saved(path),
        Some((path, Err(_))) => ReceiptFile::Failed(path),
    };
    let _ = write_receipt(out, receipt, file);
    match (result, saved) {
        (Ok(()), Some((_, Err(err)))) => Err(err),
        (Err(err), Some((_, Err(write)))) => {
            eprintln!("warning: {write:#}");
            Err(err)
        }
        (result, _) => result,
    }
}

/// Write `receipt` to `path` as pretty JSON, atomically: a temp file in the
/// same directory, then a rename.
///
/// # Errors
///
/// The directory cannot take the temp file, or the write or the rename fails.
pub(crate) fn save(path: &Path, receipt: &PullReceipt) -> anyhow::Result<()> {
    let dir = parent_dir(path);
    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("create a temp file for the receipt in {}", dir.display()))?;
    serde_json::to_writer_pretty(&mut tmp, receipt).context("serialize the receipt")?;
    tmp.write_all(b"\n").context("write the receipt")?;
    tmp.persist(path)
        .map_err(|e| e.error)
        .with_context(|| format!("write the receipt to {}", path.display()))?;
    Ok(())
}

/// Read the receipt at `path`.
fn load(path: &Path) -> anyhow::Result<PullReceipt> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read the receipt {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse the receipt {}", path.display()))
}

/// What became of `--receipt`, for the block's last line.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReceiptFile<'a> {
    /// No `--receipt` was given.
    Unrequested,
    /// The receipt was written here.
    Saved(&'a Path),
    /// The receipt could not be written here.
    Failed(&'a Path),
}

/// The receipt block: what the pull fetched, who served it, what it paid,
/// and how many signatures back it. `file` says where the receipt went, for
/// the `verify` hint.
///
/// # Errors
///
/// A write to `w` fails.
pub(crate) fn write_receipt(
    w: &mut impl Write,
    receipt: &PullReceipt,
    file: ReceiptFile<'_>,
) -> std::io::Result<()> {
    let summary = receipt.summary();
    let content = &receipt.content;
    let entries = content
        .entries
        .iter()
        .filter(|e| content.kind == ContentKind::Blob || e.hash != content.hash)
        .count();
    let landed = human_bytes(summary.landed_bytes);
    let shape = match (content.kind, entries) {
        (ContentKind::Blob, 0) => "blob, no whole blob landed".to_owned(),
        (ContentKind::Blob, _) => format!("blob, {landed}"),
        (ContentKind::Bundle, n) => format!(
            "bundle, {landed}, {n} {}",
            if n == 1 { "entry" } else { "entries" }
        ),
    };
    writeln!(w, "decdn receipt v1")?;
    writeln!(w, "hash        {}  ({shape})", short_hash(content.hash))?;
    if !content.complete {
        write!(
            w,
            "status      incomplete: the pull ended early; this is what it paid for"
        )?;
        match content.unlanded.len() {
            0 => writeln!(w)?,
            n => writeln!(w, " ({n} started blob(s) did not land)")?,
        }
    }
    if let Some(ns) = content.namespace {
        writeln!(w, "namespace   {ns}")?;
    }
    writeln!(
        w,
        "bytes       {} verified · {} paid",
        grouped(summary.landed_bytes),
        grouped(summary.paid.bytes)
    )?;
    let mut nodes = summary.per_provider.clone();
    nodes.sort_by_key(|(_, paid)| std::cmp::Reverse(paid.bytes));
    let listed: Vec<String> = nodes
        .iter()
        .map(|(provider, paid)| format!("{} {}", short_address(*provider), human_bytes(paid.bytes)))
        .collect();
    writeln!(w, "nodes       {}   {}", nodes.len(), listed.join(" · "))?;
    let pools = match summary.pools.as_slice() {
        [pool] => format!("pool {}", short_hex(pool.as_slice())),
        pools => format!("{} pools", pools.len()),
    };
    writeln!(
        w,
        "paid        {} USDC from {pools}",
        usdc_short(U256::from(summary.paid.amount))
    )?;
    writeln!(
        w,
        "started     {}   finished {}",
        receipt.window.started, receipt.window.finished
    )?;
    if summary.settled_lanes > 0 {
        writeln!(
            w,
            "settled     {} of {} lanes",
            summary.settled_lanes,
            receipt.lanes.len()
        )?;
    }
    write!(
        w,
        "evidence    {} node + {} payer signatures",
        summary.node_signatures, summary.payer_signatures
    )?;
    if summary.unproved_lanes > 0 {
        let lanes = if summary.unproved_lanes == 1 {
            "lane"
        } else {
            "lanes"
        };
        write!(w, " · {} {lanes} without a voucher", summary.unproved_lanes)?;
    }
    match file {
        ReceiptFile::Saved(path) => writeln!(w, " · decdn receipt verify {}", path.display()),
        ReceiptFile::Failed(path) => writeln!(w, " · receipt NOT written to {}", path.display()),
        ReceiptFile::Unrequested => writeln!(w, " (--receipt <FILE> keeps them)"),
    }
}

/// What this build knows of a receipt's chain.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChainCheck {
    /// A chain this build knows: the faults where the receipt's contracts
    /// differ from this build's, empty when they match.
    Known(Vec<String>),
    /// A chain this build does not know: its contract addresses are unchecked.
    Unknown,
}

/// Check the receipt's contracts against the ones this build knows for its
/// chain.
///
/// # Errors
///
/// This build's manifest for the chain does not read, or names an address
/// that does not parse: a build fault, never a pass.
pub(crate) fn check_chain(chain: &ReceiptChain) -> anyhow::Result<ChainCheck> {
    let Some(known) = known_chains::by_chain_id(chain.chain_id) else {
        return Ok(ChainCheck::Unknown);
    };
    let addresses = known.addresses().with_context(|| {
        format!(
            "this build's deployment manifest for chain {} does not read",
            chain.chain_id
        )
    })?;
    let mut faults = Vec::new();
    for (name, stated, known) in [
        ("payment_pool", chain.payment_pool, &addresses.payment_pool),
        ("slash_judge", chain.slash_judge, &addresses.slash_judge),
    ] {
        let known: Address = known.parse().with_context(|| {
            format!(
                "this build's {name} for chain {} is not an address: {known:?}",
                chain.chain_id
            )
        })?;
        if known != stated {
            faults.push(format!(
                "{name} {stated} is not chain {}'s contract {known} in this build",
                chain.chain_id
            ));
        }
    }
    Ok(ChainCheck::Known(faults))
}

/// Every fault in `receipt`: its own checks, then the chain check's.
fn receipt_faults(receipt: &PullReceipt) -> anyhow::Result<(Vec<String>, bool)> {
    let mut faults: Vec<String> = receipt.verify().iter().map(ToString::to_string).collect();
    let known = match check_chain(&receipt.chain)? {
        ChainCheck::Known(mismatched) => {
            faults.extend(mismatched);
            true
        }
        ChainCheck::Unknown => false,
    };
    Ok((faults, known))
}

/// The note for a chain this build does not know.
fn unknown_chain_note(chain_id: u64) -> String {
    format!(
        "chain {chain_id} is not one this build knows: its PaymentPool and SlashJudge \
         addresses are unchecked"
    )
}

/// `decdn receipt verify`: re-check the file's signatures offline.
fn verify_file(args: &ReceiptVerifyArgs) -> anyhow::Result<()> {
    let receipt = load(&args.file)?;
    let (faults, chain_known) = receipt_faults(&receipt)?;
    let summary = receipt.summary();
    if args.json {
        let out = serde_json::json!({
            "ok": faults.is_empty(),
            "faults": faults,
            "chain_id": receipt.chain.chain_id,
            "chain_known": chain_known,
            "node_signatures": summary.node_signatures,
            "payer_signatures": summary.payer_signatures,
            "unproved_lanes": summary.unproved_lanes,
            "settled_lanes": summary.settled_lanes,
            "lanes": receipt.lanes.len(),
        });
        println!("{out}");
    } else if faults.is_empty() {
        println!(
            "receipt verified: {} node and {} payer signatures hold on chain {}",
            summary.node_signatures, summary.payer_signatures, receipt.chain.chain_id
        );
        if !chain_known {
            println!("{}", unknown_chain_note(receipt.chain.chain_id));
        }
        if summary.unproved_lanes > 0 {
            println!(
                "{} lane(s) paid but carry no voucher, so their claim is not proved",
                summary.unproved_lanes
            );
        }
        if receipt.lanes.iter().any(|l| l.settlement.is_some()) {
            println!(
                "settlement: {} of {} lanes recorded as settled (read from the chain by \
                 `decdn receipt settle`; verify does not check it)",
                summary.settled_lanes,
                receipt.lanes.len()
            );
        }
    } else {
        for fault in &faults {
            eprintln!("  {fault}");
        }
    }
    anyhow::ensure!(
        faults.is_empty(),
        "{} check(s) failed in {}",
        faults.len(),
        args.file.display()
    );
    Ok(())
}

/// `decdn receipt settle`: verify the file, scan each lane's redemptions and
/// rewrite it.
async fn settle_file(args: &ReceiptSettleArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let mut receipt = load(&args.file)?;
    // A receipt that does not verify would have settle read another
    // contract's events, or mark a forged claim settled.
    let (faults, chain_known) = receipt_faults(&receipt)?;
    if !faults.is_empty() {
        for fault in &faults {
            eprintln!("  {fault}");
        }
        anyhow::bail!(
            "{} does not verify ({} check(s) failed); settle reads only a receipt that does",
            args.file.display(),
            faults.len()
        );
    }
    let chain_id = receipt.chain.chain_id;
    if !chain_known {
        eprintln!("note: {}", unknown_chain_note(chain_id));
    }
    let known = known_chains::by_chain_id(chain_id);
    let (rpc_url, span) = settle_coordinates(args, chain_id, config_path)?;
    let provider = decdn_client::provider::build_read_provider(&rpc_url)?;
    let rpc_chain_id = bounded("eth_chainId", provider.get_chain_id()).await?;
    anyhow::ensure!(
        rpc_chain_id == chain_id,
        "the RPC serves chain {rpc_chain_id}, but the receipt is for chain {chain_id}; pass \
         --rpc-url for that chain"
    );
    let head = bounded("the latest block number", provider.get_block_number()).await?;
    let from = if let Some(block) = args.from_block {
        block
    } else {
        let started = parse_rfc3339(&receipt.window.started).ok_or_else(|| {
            anyhow::anyhow!(
                "the receipt's start {:?} is not a time this build reads; pass --from-block",
                receipt.window.started
            )
        })?;
        let floor = known.map_or(0, |k| k.l2_start_block);
        block_at_or_after(
            &provider,
            started.saturating_sub(CLOCK_SKEW_SECS),
            floor,
            head,
        )
        .await?
    };
    let mut pools: Vec<B256> = Vec::new();
    for lane in receipt.lanes.iter().filter(|l| l.proof.is_some()) {
        if !pools.contains(&lane.pool_id) {
            pools.push(lane.pool_id);
        }
    }
    let mut redemptions = Vec::new();
    for pool in pools {
        let filter = Filter::new()
            .address(receipt.chain.payment_pool)
            .event_signature(PaymentPool::PoolRedeemed::SIGNATURE_HASH)
            .topic1(pool);
        eprintln!("scanning PaymentPool.PoolRedeemed for pool {pool} over blocks {from}..={head}");
        let logs = scan_logs(&provider, &filter, from, head, span, |event| {
            if let ScanEvent::Retry { .. } = event {
                print_retry(&event);
            }
        })
        .await
        .with_context(|| scan_failed("PaymentPool.PoolRedeemed", span))?;
        redemptions.extend(decode_redemptions(&logs)?);
    }
    apply_redemptions(&mut receipt, &redemptions);
    save(&args.file, &receipt)?;
    if args.json {
        println!("{}", serde_json::to_string(&receipt)?);
        return Ok(());
    }
    for lane in &receipt.lanes {
        println!("{}", settlement_line(lane));
    }
    let summary = receipt.summary();
    println!(
        "settled {} of {} lanes; wrote {}",
        summary.settled_lanes,
        receipt.lanes.len(),
        args.file.display()
    );
    Ok(())
}

/// The RPC endpoint and the first `eth_getLogs` window span `settle` scans
/// chain `chain_id` with: `--rpc-url`, else `blockchain.rpc_url` when the
/// config names the same chain, else the chain's public RPC when this build
/// knows it.
fn settle_coordinates(
    args: &ReceiptSettleArgs,
    chain_id: u64,
    config_path: Option<&Path>,
) -> anyhow::Result<(String, NonZeroU64)> {
    let file = chain_ctx::load_optional_config(config_path)?;
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| {
            (file.chain_id() == Some(chain_id))
                .then(|| file.rpc_url().map(str::to_owned))
                .flatten()
        })
        .or_else(|| known_chains::by_chain_id(chain_id).map(|k| k.public_rpc.to_owned()))
        .ok_or_else(|| anyhow::anyhow!("no RPC is known for chain {chain_id}; pass --rpc-url"))?;
    let span = NonZeroU64::new(
        file.get_logs_max_block_span()
            .unwrap_or(DEFAULT_GET_LOGS_MAX_BLOCK_SPAN),
    )
    .ok_or_else(|| {
        anyhow::anyhow!(
            "blockchain.get_logs_max_block_span = 0 would scan one block per request; use at \
             least 1"
        )
    })?;
    Ok((rpc_url, span))
}

/// One `PoolRedeemed` log: where it landed and what it paid.
#[derive(Clone)]
pub(crate) struct Redemption {
    pub(crate) tx_hash: B256,
    pub(crate) block: u64,
    pub(crate) event: PaymentPool::PoolRedeemed,
}

/// Decode `logs` as `PoolRedeemed`, in chain order.
///
/// # Errors
///
/// A log that does not decode, or that has no transaction hash or block.
pub(crate) fn decode_redemptions(logs: &[Log]) -> anyhow::Result<Vec<Redemption>> {
    let mut out = Vec::with_capacity(logs.len());
    for log in logs {
        let (Some(tx_hash), Some(block)) = (log.transaction_hash, log.block_number) else {
            anyhow::bail!("the RPC returned a PoolRedeemed log without a transaction or block");
        };
        let event =
            PaymentPool::PoolRedeemed::decode_log_data(&log.inner.data).with_context(|| {
                format!(
                    "PaymentPool.PoolRedeemed in tx {tx_hash} does not decode as this binary's ABI"
                )
            })?;
        out.push(Redemption {
            tx_hash,
            block,
            event,
        });
    }
    Ok(out)
}

/// Replace each lane's settlement with what `redemptions` show: for a lane
/// with a proof, the first of them (chain order) whose paid cumulative for the
/// lane's signer reaches the claim, or else the highest one past the lane's
/// prior watermark; for any other lane, none. A settlement already in the
/// file is not kept: nothing signs it, so only the chain the scan read can
/// state it, and a reorganization can have removed it.
pub(crate) fn apply_redemptions(receipt: &mut PullReceipt, redemptions: &[Redemption]) {
    for lane in &mut receipt.lanes {
        lane.settlement = None;
        if lane.proof.is_none() {
            continue;
        }
        for r in redemptions {
            if r.event.poolId != lane.pool_id || r.event.provider != lane.provider {
                continue;
            }
            for settled in r.event.lanes.iter().filter(|s| s.signer == lane.signer) {
                let paid = settled.newPaidCumulative;
                let higher = lane.settlement.is_none_or(|s| paid > s.new_paid_cumulative);
                if lane.is_settled() || paid <= lane.prior.amount || !higher {
                    continue;
                }
                lane.settlement = Some(ReceiptSettlement {
                    tx_hash: r.tx_hash,
                    block: r.block,
                    new_paid_cumulative: paid,
                });
            }
        }
    }
}

/// The `settle` line for one lane.
fn settlement_line(lane: &ReceiptLane) -> String {
    let node = short_address(lane.provider);
    match (&lane.proof, lane.settlement) {
        (None, _) => format!("{node}  no voucher, so no claim to settle"),
        (Some(_), None) => format!("{node}  not redeemed yet"),
        (Some(_), Some(s)) if lane.is_settled() => format!(
            "{node}  settled at block {} (tx {})",
            s.block,
            short_hex(s.tx_hash.as_slice())
        ),
        (Some(proof), Some(s)) => format!(
            "{node}  partly redeemed: {} of {} USDC claimed by block {} (tx {})",
            usdc_short(U256::from(
                s.new_paid_cumulative.saturating_sub(lane.prior.amount)
            )),
            usdc_short(U256::from(
                proof.claim.amount.saturating_sub(lane.prior.amount)
            )),
            s.block,
            short_hex(s.tx_hash.as_slice())
        ),
    }
}

/// `n` with thousands separators: `28,995,469,846`.
pub(crate) fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// `bytes` as `0x` hex cut to its first and last two bytes: `0x2a3b…09f1`.
pub(crate) fn short_hex(bytes: &[u8]) -> String {
    format!("0x{}", short(&alloy::hex::encode(bytes)))
}

/// An address cut like [`short_hex`].
fn short_address(address: Address) -> String {
    short_hex(address.as_slice())
}

/// A content hash as bare hex cut to its first four and last two bytes:
/// `9194000d…69e5`.
fn short_hash(hash: B256) -> String {
    let hex = alloy::hex::encode(hash.as_slice());
    match (hex.get(..8), hex.get(hex.len().saturating_sub(4)..)) {
        (Some(head), Some(tail)) => format!("{head}…{tail}"),
        _ => hex,
    }
}

/// `hex` cut to its first and last four digits, or whole when that is no
/// shorter.
fn short(hex: &str) -> String {
    if hex.len() <= 9 {
        return hex.to_owned();
    }
    match (hex.get(..4), hex.get(hex.len() - 4..)) {
        (Some(head), Some(tail)) => format!("{head}…{tail}"),
        _ => hex.to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
