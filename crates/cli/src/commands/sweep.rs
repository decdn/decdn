//! `decdn node sweep` and `decdn node receive-sweeps`: move the operator's
//! protocol share from the payment chains to the governance chain (ADR 003 §
//! Redemption-path interface, ADR 016 § Bridge adapters).
//!
//! Each `redeemMany` leaves the protocol share of its payout in the pool, as
//! `pending(operator)`. `sweep` sends a batch of shares: the operator's own,
//! plus any `--with` operators that agree to share the governance-chain
//! receive. The pool burns the batch total over CCTP V2 to the governance
//! chain's `CctpSweepReceiver` once it reaches `minSweep`, with one
//! `SweepShare` (operator, served bytes, amount) per swept operator as the
//! hook data. CCTP then needs an attestation from Circle's attestation
//! service (Iris). `receive-sweeps` finds every batch that carried the
//! operator's share, fetches its message and attestation once, waits while
//! the attestation is pending, and calls `CctpSweepReceiver.receiveSweep`
//! once per message, which mints the USDC and deposits each entry's share
//! into `FeeRouter`. Both calls are permissionless; the operator runs them by
//! hand, and no daemon task does.

use std::path::{Path, PathBuf};

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol_types::{SolEvent, SolValue};
use anyhow::Context as _;
use decdn_client::session::PaymentPoolChain;
use decdn_common::cli;
use decdn_common::config::DEFAULT_CHAIN_ID;
use decdn_incentive::eth_identity;
use decdn_incentive::log_scan::scan_logs;
use decdn_incentive::payment_pool::{CctpSweepReceiver, MessageTransmitterV2, PaymentPool};
use decdn_incentive::tx::{SendOutcome, send_for_receipt};
use serde::{Deserialize, Serialize};

use super::chain_ctx;
use super::node::earnings_chain::{bounded, scan_failed, scan_progress, scan_span, scan_targets};
use super::node::format_usdc;

mod iris;

pub(crate) use iris::{IrisWait, MessageStatus, fetch_messages};

/// One operator's pending protocol share in a pool, read before a sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct PendingShare {
    /// The operator.
    pub(crate) operator: Address,
    /// Its pending share, micro-USDC.
    pub(crate) micro_usdc: u64,
}

/// What `sweep` does with one pool, from the batch's pending shares and
/// `minSweep`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SweepPlan {
    /// The batch total has reached `minSweep`: send `sweep(operators)`.
    Send {
        /// The operators with a share, in input order.
        operators: Vec<Address>,
        /// The batch total, micro-USDC.
        total: u64,
    },
    /// The batch total is under `minSweep`, or zero: the pool would revert
    /// `BelowMinSweep`, so nothing is sent.
    Below {
        /// The batch total, micro-USDC.
        total: u64,
        /// The pool's `minSweep`, micro-USDC.
        min_sweep: u64,
    },
}

/// The plan for the shares `shares` against `min_sweep`. The batch keeps
/// each operator with a share once, in input order, and leaves out the rest,
/// as the pool does. The pool sends a total at or above `minSweep` and never
/// an empty one.
///
/// # Errors
///
/// The batch holds more than `max_batch` operators: the pool reverts
/// `BadSweepBatch`.
pub(crate) fn plan_sweep(
    shares: &[PendingShare],
    min_sweep: u64,
    max_batch: usize,
) -> anyhow::Result<SweepPlan> {
    let mut operators: Vec<Address> = Vec::with_capacity(shares.len());
    let mut total = 0u64;
    for share in shares {
        if share.micro_usdc == 0 || operators.contains(&share.operator) {
            continue;
        }
        operators.push(share.operator);
        total = total.saturating_add(share.micro_usdc);
    }
    anyhow::ensure!(
        operators.len() <= max_batch,
        "{} operators have a share, but one sweep carries at most {max_batch}; pass fewer --with",
        operators.len()
    );
    Ok(if total == 0 || total < min_sweep {
        SweepPlan::Below { total, min_sweep }
    } else {
        SweepPlan::Send { operators, total }
    })
}

/// The `Swept` events `pool` emitted in `receipt`, one per swept operator in
/// batch order.
pub(crate) fn swept_from_receipt(
    receipt: &TransactionReceipt,
    pool: Address,
) -> Vec<PaymentPool::Swept> {
    receipt
        .inner
        .logs()
        .iter()
        .filter(|log| log.address() == pool)
        .filter_map(|log| PaymentPool::Swept::decode_log(&log.inner).ok())
        .map(|log| log.data)
        .collect()
}

/// The `SweepSent` event `pool` emitted in `receipt`: the batch total and
/// the bridge fee it allowed.
pub(crate) fn sweep_sent_from_receipt(
    receipt: &TransactionReceipt,
    pool: Address,
) -> Option<PaymentPool::SweepSent> {
    receipt.inner.logs().iter().find_map(|log| {
        (log.address() == pool)
            .then(|| PaymentPool::SweepSent::decode_log(&log.inner).ok())
            .flatten()
            .map(|log| log.data)
    })
}

/// The CCTP message the first `MessageSent` log of `receipt` carries.
pub(crate) fn message_sent(receipt: &TransactionReceipt) -> Option<Bytes> {
    receipt.inner.logs().iter().find_map(|log| {
        (log.topic0() == Some(&MessageTransmitterV2::MessageSent::SIGNATURE_HASH))
            .then(|| MessageTransmitterV2::MessageSent::decode_log_data(&log.inner.data).ok())
            .flatten()
            .map(|sent| sent.message)
    })
}

/// Byte offsets of the CCTP V2 message header fields read here
/// (circlefin/evm-cctp-contracts `MessageV2`).
const SOURCE_DOMAIN: std::ops::Range<usize> = 4..8;
const NONCE: std::ops::Range<usize> = 12..44;
const DESTINATION_CALLER: std::ops::Range<usize> = 108..140;
/// Where the burn body's hook data starts: the `MessageV2` body offset (148)
/// plus the `BurnMessageV2` hook-data offset (228).
const HOOK_DATA: usize = 148 + 228;

/// The `SweepShare[]` a sweep message's hook data carries: one entry per
/// swept operator, in batch order. `None` when the message is too short or
/// its hook data does not decode.
pub(crate) fn sweep_shares(message: &[u8]) -> Option<Vec<PaymentPool::SweepShare>> {
    let hook = message.get(HOOK_DATA..)?;
    <Vec<PaymentPool::SweepShare>>::abi_decode(hook).ok()
}

/// The source domain in a CCTP message header.
pub(crate) fn source_domain(message: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = message.get(SOURCE_DOMAIN)?.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

/// The nonce in a CCTP message header: the key `usedNonces` marks once the
/// message is received.
pub(crate) fn message_nonce(message: &[u8]) -> Option<B256> {
    message.get(NONCE).map(B256::from_slice)
}

/// The destination caller in a CCTP message header: the only address that
/// may receive it.
pub(crate) fn destination_caller(message: &[u8]) -> Option<Address> {
    let word = message.get(DESTINATION_CALLER)?;
    Some(Address::from_slice(word.get(12..)?))
}

/// The operator key, data dir and config file of a sweep command.
struct Operator {
    file: chain_ctx::FileConfig,
    keystore: PathBuf,
    password_file: Option<PathBuf>,
}

/// Resolve the operator's keystore: flag > `blockchain.eth_keystore` > under
/// the data dir.
fn resolve_operator(
    args: &cli::OperatorKeyArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<Operator> {
    let file = chain_ctx::load_optional_config(args.config.as_deref().or(global_config))?;
    let data_dir = args
        .data_dir
        .clone()
        .or_else(|| file.data_dir().map(Path::to_path_buf))
        .map(|p| cli::common::expand_tilde(&p))
        .or_else(cli::default_data_dir)
        .context("data_dir not set and no default available (pass --data-dir)")?;
    let keystore = args
        .keystore
        .clone()
        .or_else(|| file.eth_keystore().map(Path::to_path_buf))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| cli::common::expand_tilde(&p),
        );
    Ok(Operator {
        file,
        keystore,
        password_file: args.keystore_password_file.clone(),
    })
}

/// One swept operator's share, from the pool's `Swept` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SweptShare {
    /// The operator.
    pub(crate) operator: Address,
    /// The share sent, micro-USDC.
    pub(crate) micro_usdc: u64,
    /// The served bytes sent with it.
    pub(crate) served_bytes: u64,
}

/// One pool's line of `decdn node sweep` output.
#[derive(Debug, Serialize)]
struct SweepLine {
    /// The pool, as a CAIP-10 account.
    pool: String,
    /// `swept`, `below_min_sweep` or `dry_run`.
    outcome: &'static str,
    /// Each listed operator's pending share before the sweep: this operator
    /// first, then each `--with` operator.
    pending: Vec<PendingShare>,
    /// The batch total before the sweep, micro-USDC.
    total_micro_usdc: u64,
    /// The pool's `minSweep`, micro-USDC.
    min_sweep_micro_usdc: u64,
    /// The sweep transaction, when one was sent.
    tx: Option<String>,
    /// Each swept operator's share, from the `Swept` events.
    swept: Vec<SweptShare>,
    /// The batch total the `SweepSent` event says was burned, micro-USDC.
    sent_micro_usdc: Option<u64>,
    /// The bridge fee the `SweepSent` event says the sweep allowed,
    /// micro-USDC.
    max_fee_micro_usdc: Option<u64>,
}

impl SweepLine {
    /// The human-readable form: one line for the pool, then one per swept
    /// operator.
    fn text(&self) -> String {
        let base = format!(
            "pool={} total_usdc={} min_sweep_usdc={} operators={}",
            self.pool,
            format_usdc(self.total_micro_usdc),
            format_usdc(self.min_sweep_micro_usdc),
            self.pending.iter().filter(|p| p.micro_usdc > 0).count()
        );
        match (self.outcome, &self.tx) {
            ("swept", Some(tx)) => std::iter::once(format!(
                "{base} tx={tx} sent_usdc={} max_fee_usdc={}",
                format_usdc(self.sent_micro_usdc.unwrap_or(0)),
                format_usdc(self.max_fee_micro_usdc.unwrap_or(0))
            ))
            .chain(self.swept.iter().map(|share| {
                format!(
                    "  operator={} swept_usdc={} served_bytes={}",
                    share.operator,
                    format_usdc(share.micro_usdc),
                    share.served_bytes
                )
            }))
            .collect::<Vec<_>>()
            .join("\n"),
            ("dry_run", _) => format!("{base} (dry run: would sweep)"),
            _ => format!("{base} (below minSweep; nothing sent)"),
        }
    }
}

/// `decdn node sweep`.
///
/// # Errors
///
/// A pool, keystore or `--with` address that does not resolve, an RPC on
/// another chain, a failed read, a batch over `MAX_SWEEP_BATCH()`, or a sweep
/// that reverts or does not confirm.
pub async fn sweep(args: &cli::SweepArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let op = resolve_operator(&args.operator, global_config)?;
    let pools = chain_pools(&args.payment, &op.file)?;
    let others = args
        .with
        .iter()
        .map(|raw| chain_ctx::parse_nonzero_address(raw, "--with"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let signer =
        chain_ctx::load_signer_with_password_file(op.password_file.as_deref(), &op.keystore)
            .await?;
    let operators: Vec<Address> = std::iter::once(signer.address()).chain(others).collect();
    let mut lines = Vec::with_capacity(pools.len());
    for pool in &pools {
        lines.push(sweep_pool(pool, &signer, &operators, args.operator.dry_run).await?);
    }
    print_lines(&lines, args.operator.json, SweepLine::text)
}

/// Read the listed operators' shares in one pool and send the batch's sweep
/// when the plan says so.
async fn sweep_pool(
    pool: &PaymentPoolChain,
    signer: &alloy::signers::local::PrivateKeySigner,
    operators: &[Address],
    dry_run: bool,
) -> anyhow::Result<SweepLine> {
    let account = pool.deployment.pool_account().to_string();
    let provider = decdn_client::provider::build_provider(&pool.rpc_url, signer)?;
    ensure_chain(&provider, pool.deployment.chain_id, &account).await?;
    let contract = PaymentPool::new(pool.deployment.payment_pool, &provider);
    let mut pending = Vec::with_capacity(operators.len());
    for &operator in operators {
        let share = bounded(
            "PaymentPool.pending(operator)",
            contract.pending(operator).call(),
        )
        .await?;
        pending.push(PendingShare {
            operator,
            micro_usdc: u64::try_from(share.usdcAmount).unwrap_or(u64::MAX),
        });
    }
    let min_sweep = bounded("PaymentPool.minSweep()", contract.minSweep().call()).await?;
    let max_batch = bounded(
        "PaymentPool.MAX_SWEEP_BATCH()",
        contract.MAX_SWEEP_BATCH().call(),
    )
    .await?;
    let plan = plan_sweep(
        &pending,
        min_sweep,
        usize::try_from(max_batch).unwrap_or(usize::MAX),
    )
    .with_context(|| format!("sweeping {account}"))?;
    let total = match &plan {
        SweepPlan::Send { total, .. } | SweepPlan::Below { total, .. } => *total,
    };
    let mut line = SweepLine {
        pool: account.clone(),
        outcome: "below_min_sweep",
        pending,
        total_micro_usdc: total,
        min_sweep_micro_usdc: min_sweep,
        tx: None,
        swept: Vec::new(),
        sent_micro_usdc: None,
        max_fee_micro_usdc: None,
    };
    let SweepPlan::Send { operators, .. } = plan else {
        return Ok(line);
    };
    if dry_run {
        line.outcome = "dry_run";
        return Ok(line);
    }
    let mut outcome = SendOutcome::NotSent;
    let receipt = send_for_receipt(
        contract.sweep(operators),
        &format!("PaymentPool.sweep on {account}"),
        Some("re-run `decdn node sweep`: a share already swept reads as nothing pending"),
        &mut outcome,
    )
    .await?;
    let sent = sweep_sent_from_receipt(&receipt, pool.deployment.payment_pool)
        .context("the sweep landed but its receipt carries no SweepSent event")?;
    line.outcome = "swept";
    line.tx = Some(format!("{:#x}", receipt.transaction_hash));
    line.swept = swept_from_receipt(&receipt, pool.deployment.payment_pool)
        .into_iter()
        .map(|swept| SweptShare {
            operator: swept.operator,
            micro_usdc: u64::try_from(swept.amount).unwrap_or(u64::MAX),
            served_bytes: u64::try_from(swept.servedBytes).unwrap_or(u64::MAX),
        })
        .collect();
    line.sent_micro_usdc = Some(u64::try_from(sent.amount).unwrap_or(u64::MAX));
    line.max_fee_micro_usdc = Some(u64::try_from(sent.maxFee).unwrap_or(u64::MAX));
    Ok(line)
}

/// The pools a sweep command acts on: `--pool`, else every configured pool,
/// else the known target's.
fn chain_pools(
    select: &cli::PoolSelectArgs,
    file: &chain_ctx::FileConfig,
) -> anyhow::Result<Vec<PaymentPoolChain>> {
    // The first block only matters to a scan; `0` keeps a pool whose chain
    // has no known first block.
    Ok(scan_targets(select, Some(0), file)?
        .into_iter()
        .map(|target| target.pool)
        .collect())
}

/// Fail unless `provider` serves `chain_id`.
async fn ensure_chain<P: Provider>(provider: &P, chain_id: u64, what: &str) -> anyhow::Result<()> {
    let served = bounded("eth_chainId", provider.get_chain_id()).await?;
    anyhow::ensure!(
        served == chain_id,
        "the RPC serves chain {served}, but {what} is on chain {chain_id}; pass the RPC of chain \
         {chain_id}"
    );
    Ok(())
}

/// Print `lines` as one JSON array, or one text line each.
fn print_lines<T: Serialize>(
    lines: &[T],
    json: bool,
    text: impl Fn(&T) -> String,
) -> anyhow::Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(lines).context("encode the result as JSON")?
        );
    } else {
        for line in lines {
            println!("{}", text(line));
        }
    }
    Ok(())
}

/// One sweep transaction to complete on the governance chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SweepTx {
    /// The pool that sent it.
    pub(crate) pool: PaymentPoolChain,
    /// The sweep transaction on the pool's payment chain.
    pub(crate) tx: B256,
}

/// One sweep's line of `decdn node receive-sweeps` output.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReceiveLine {
    /// The pool that sent the sweep, as a CAIP-10 account.
    pub(crate) pool: String,
    /// The sweep transaction on the payment chain.
    pub(crate) sweep_tx: String,
    /// `received`, `already_received`, `pending`, `dry_run` or
    /// `not_for_receiver`.
    pub(crate) outcome: String,
    /// How many operators the sweep message carries, from its hook data.
    pub(crate) batch_operators: Option<usize>,
    /// This operator's entry in the message's hook data, micro-USDC, before
    /// any bridge fee.
    pub(crate) share_micro_usdc: Option<u64>,
    /// The `receiveSweep` transaction, when one was sent.
    pub(crate) receive_tx: Option<String>,
}

/// `decdn node receive-sweeps`.
///
/// # Errors
///
/// A pool, keystore, receiver or attestation service that does not resolve, an
/// RPC on another chain, a failed scan or read, an Iris answer that does not
/// decode, or a `receiveSweep` that reverts for a reason other than an
/// already-used nonce.
pub async fn receive_sweeps(
    args: &cli::ReceiveSweepsArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let op = resolve_operator(&args.operator, global_config)?;
    let governance_chain_id = args
        .governance
        .governance_chain_id
        .or_else(|| op.file.chain_id())
        .unwrap_or(DEFAULT_CHAIN_ID);
    let known = crate::known_chains::by_chain_id(governance_chain_id);
    let governance_rpc = args
        .governance
        .governance_rpc_url
        .clone()
        .or_else(|| op.file.rpc_url().map(str::to_owned))
        .or_else(|| known.map(|k| k.public_rpc.to_owned()))
        .context(
            "governance rpc_url not set (--governance-rpc-url or blockchain.rpc_url): sweeps \
             are received on the governance chain",
        )?;
    let receiver_raw = match args
        .cctp_sweep_receiver_address
        .clone()
        .or_else(|| op.file.cctp_sweep_receiver_address().map(str::to_owned))
    {
        Some(raw) => raw,
        None => known
            .map(|k| k.addresses().map(|a| a.cctp_sweep_receiver))
            .transpose()?
            .context(
                "cctp_sweep_receiver_address not set (--cctp-sweep-receiver-address or \
                 blockchain.cctp_sweep_receiver_address)",
            )?,
    };
    let receiver = chain_ctx::parse_nonzero_address(&receiver_raw, "cctp_sweep_receiver_address")?;
    let iris = args
        .iris
        .clone()
        .or_else(|| op.file.cctp_iris_url().map(str::to_owned))
        .or_else(|| known.map(|k| k.cctp_iris_url.to_owned()))
        .context(
            "the CCTP attestation service is not set (--iris or blockchain.cctp_iris_url), such \
             as https://iris-api-sandbox.circle.com for a testnet",
        )?;

    let signer =
        chain_ctx::load_signer_with_password_file(op.password_file.as_deref(), &op.keystore)
            .await?;
    let operator = signer.address();
    let sweeps = if args.txs.is_empty() {
        find_sweeps(&args.payment, args.from_block, &op.file, operator).await?
    } else {
        let pool = chain_pools(&args.payment, &op.file)?
            .into_iter()
            .next()
            .context("--tx needs the pool that sent it (--pool)")?;
        args.txs
            .iter()
            .map(|raw| {
                Ok(SweepTx {
                    pool: pool.clone(),
                    tx: raw
                        .parse()
                        .map_err(|e| anyhow::anyhow!("invalid --tx {raw:?}: {e}"))?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };
    let sweeps = distinct(sweeps);
    if sweeps.is_empty() {
        println!("no sweep transactions found for {operator}");
        return Ok(());
    }

    let governance = decdn_client::provider::build_provider(&governance_rpc, &signer)?;
    ensure_chain(&governance, governance_chain_id, "the governance chain").await?;
    let client = iris::client()?;
    let lines = receive_all(
        &sweeps,
        &Receiving {
            governance: &governance,
            operator,
            receiver,
            iris: &iris,
            client: &client,
            wait: &IrisWait::default(),
            dry_run: args.operator.dry_run,
        },
    )
    .await?;
    print_lines(&lines, args.operator.json, ReceiveLine::text)
}

impl ReceiveLine {
    /// The human-readable form.
    fn text(&self) -> String {
        let tx = self
            .receive_tx
            .as_deref()
            .map(|tx| format!(" receive_tx={tx}"))
            .unwrap_or_default();
        let batch = self
            .batch_operators
            .map(|n| format!(" batch_operators={n}"))
            .unwrap_or_default();
        let share = self
            .share_micro_usdc
            .map(|micro| format!(" share_usdc={}", format_usdc(micro)))
            .unwrap_or_default();
        format!(
            "pool={} sweep_tx={} outcome={}{batch}{share}{tx}",
            self.pool, self.sweep_tx, self.outcome
        )
    }
}

/// `sweeps` with each `(pool, tx)` kept once, in first-seen order. A batch
/// transaction is one CCTP message whatever the operators it carries, so it
/// is received once.
pub(crate) fn distinct(sweeps: Vec<SweepTx>) -> Vec<SweepTx> {
    let mut out: Vec<SweepTx> = Vec::with_capacity(sweeps.len());
    for sweep in sweeps {
        if !out.contains(&sweep) {
            out.push(sweep);
        }
    }
    out
}

/// Scan every pool for the operator's `Swept` events and return their
/// transactions in chain order, pool by pool. A batch that carried the
/// operator's share is found whoever sent it, since `Swept` indexes the
/// operator.
async fn find_sweeps(
    select: &cli::PoolSelectArgs,
    from_block: Option<u64>,
    file: &chain_ctx::FileConfig,
    operator: Address,
) -> anyhow::Result<Vec<SweepTx>> {
    let span = scan_span(file)?;
    let mut sweeps = Vec::new();
    for target in scan_targets(select, from_block, file)? {
        let deployment = target.pool.deployment;
        let account = deployment.pool_account().to_string();
        let provider = decdn_client::provider::build_read_provider(&target.pool.rpc_url)?;
        ensure_chain(&provider, deployment.chain_id, &account).await?;
        let head = bounded("eth_blockNumber", provider.get_block_number()).await?;
        eprintln!(
            "scanning {account} for {operator}'s Swept events over blocks {}..={head}",
            target.from_block
        );
        let filter = Filter::new()
            .address(deployment.payment_pool)
            .event_signature(PaymentPool::Swept::SIGNATURE_HASH)
            .topic1(operator.into_word());
        let logs = scan_logs(
            &provider,
            &filter,
            target.from_block,
            head,
            span,
            scan_progress(),
        )
        .await
        .with_context(|| scan_failed("PaymentPool.Swept", span))?;
        for log in logs {
            let tx = log
                .transaction_hash
                .context("a Swept log carries no transaction hash")?;
            sweeps.push(SweepTx {
                pool: target.pool.clone(),
                tx,
            });
        }
    }
    Ok(sweeps)
}

/// The governance-chain side of `receive-sweeps`.
pub(crate) struct Receiving<'a, P> {
    /// Wallet provider on the governance chain.
    pub(crate) governance: &'a P,
    /// The operator whose share each line reports.
    pub(crate) operator: Address,
    /// The `CctpSweepReceiver`.
    pub(crate) receiver: Address,
    /// The Iris base URL.
    pub(crate) iris: &'a str,
    /// The HTTP client Iris is read with.
    pub(crate) client: &'a reqwest::Client,
    /// How long to wait on a pending attestation.
    pub(crate) wait: &'a IrisWait,
    /// Read and report, but send nothing.
    pub(crate) dry_run: bool,
}

/// Complete every sweep in `sweeps`, one line each.
///
/// # Errors
///
/// A failed read, an Iris answer that does not decode, or a `receiveSweep`
/// that fails for a reason other than an already-used nonce.
pub(crate) async fn receive_all<P: Provider + Clone>(
    sweeps: &[SweepTx],
    on: &Receiving<'_, P>,
) -> anyhow::Result<Vec<ReceiveLine>> {
    let transmitter = bounded(
        "CctpSweepReceiver.messageTransmitter()",
        CctpSweepReceiver::new(on.receiver, on.governance)
            .messageTransmitter()
            .call(),
    )
    .await?;
    let mut lines = Vec::new();
    for sweep in sweeps {
        let account = sweep.pool.deployment.pool_account().to_string();
        let payment = decdn_client::provider::build_read_provider(&sweep.pool.rpc_url)?;
        let receipt = bounded(
            &format!("the receipt of sweep {:#x}", sweep.tx),
            payment.get_transaction_receipt(sweep.tx),
        )
        .await?
        .with_context(|| format!("sweep {:#x} has no receipt on {account}", sweep.tx))?;
        lines.extend(receive_one(sweep, &receipt, transmitter, on).await?);
    }
    Ok(lines)
}

/// Complete the sweep `sweep`, whose payment-chain receipt is `receipt`:
/// one line per CCTP message of the transaction. `transmitter` is the
/// receiver's `MessageTransmitterV2`.
///
/// # Errors
///
/// The receipt carries no CCTP message, a failed read, an Iris answer that
/// does not decode, or a `receiveSweep` that fails for a reason other than an
/// already-used nonce.
pub(crate) async fn receive_one<P: Provider + Clone>(
    sweep: &SweepTx,
    receipt: &TransactionReceipt,
    transmitter: Address,
    on: &Receiving<'_, P>,
) -> anyhow::Result<Vec<ReceiveLine>> {
    let account = sweep.pool.deployment.pool_account().to_string();
    let domain = message_sent(receipt)
        .and_then(|message| source_domain(&message))
        .with_context(|| {
            format!(
                "sweep {:#x} carries no CCTP MessageSent; is it a sweep of {account}?",
                sweep.tx
            )
        })?;
    let shares = message_sent(receipt).and_then(|message| sweep_shares(&message));
    let line = |outcome: &str, receive_tx: Option<String>| ReceiveLine {
        pool: account.clone(),
        sweep_tx: format!("{:#x}", sweep.tx),
        outcome: outcome.to_owned(),
        batch_operators: shares.as_ref().map(Vec::len),
        share_micro_usdc: shares.as_ref().and_then(|shares| {
            shares
                .iter()
                .find(|share| share.operator == on.operator)
                .map(|share| u64::try_from(share.amount).unwrap_or(u64::MAX))
        }),
        receive_tx,
    };
    let messages = match fetch_messages(on.client, on.iris, domain, sweep.tx, on.wait).await? {
        MessageStatus::Complete(messages) => messages,
        MessageStatus::Pending => {
            eprintln!(
                "sweep {:#x}: the attestation is still pending; re-run later",
                sweep.tx
            );
            return Ok(vec![line("pending", None)]);
        }
    };
    let used = |nonce: B256| async move {
        bounded(
            "MessageTransmitterV2.usedNonces",
            MessageTransmitterV2::new(transmitter, on.governance)
                .usedNonces(nonce)
                .call(),
        )
        .await
        .map(|used| used != U256::ZERO)
    };
    let mut lines = Vec::with_capacity(messages.len());
    for attested in messages {
        if destination_caller(&attested.message) != Some(on.receiver) {
            lines.push(line("not_for_receiver", None));
            continue;
        }
        let nonce = message_nonce(&attested.message).context("the CCTP message has no nonce")?;
        if used(nonce).await? {
            lines.push(line("already_received", None));
            continue;
        }
        if on.dry_run {
            lines.push(line("dry_run", None));
            continue;
        }
        let mut outcome = SendOutcome::NotSent;
        let sent = send_for_receipt(
            CctpSweepReceiver::new(on.receiver, on.governance)
                .receiveSweep(attested.message, attested.attestation),
            "CctpSweepReceiver.receiveSweep",
            Some("re-run `decdn node receive-sweeps`: a received sweep is skipped"),
            &mut outcome,
        )
        .await;
        match sent {
            Ok(receipt) => lines.push(line(
                "received",
                Some(format!("{:#x}", receipt.transaction_hash)),
            )),
            // Another caller may have received it between the `usedNonces`
            // read and the send: receiving is permissionless.
            Err(_) if used(nonce).await.unwrap_or(false) => {
                lines.push(line("already_received", None));
            }
            Err(err) => {
                return Err(err.context(format!("receiving sweep {:#x} of {account}", sweep.tx)));
            }
        }
    }
    Ok(lines)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
