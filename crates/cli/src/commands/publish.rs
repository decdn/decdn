//! `decdn publish` — origin-publisher control plane (issues #1029 / #1491).
//!
//! Submits the `PublisherRegistry` / `OriginAssignment` writes that create
//! namespaces and then seat or unseat a publisher's authorized origins. Vetting
//! is not a CLI step: a `VETTER_ROLE` holder on the installed vetting policy (an
//! operator, multisig, or governance) vets the wallet out-of-band, after which
//! `assign` and `revoke` take effect in the transaction that carries them
//! (ADR 011 § Origin Assignment Authority).
//! Content is bound to a namespace off-chain by the requester at fetch time
//! (ADR 002 § Hash-to-namespace association), so there is no per-hash on-chain
//! claim. Mirrors the
//! on-chain-write pattern of `super::register`: resolve chain coordinates and
//! parse the target contract address, then — on the submit path only — load
//! the keystore signer, build a wallet-filled provider, submit, and print a
//! JSON or `key=value` receipt. `--dry-run` stops after resolution: it loads
//! no keystore and sends no transaction, printing just the resolved parameters.

use std::io;
// For `flush` on the stdout lock: a `LineWriter` can buffer a partial write and
// fail at drop, where the error is unobservable.
use std::io::Write as _;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::publisher_registry::PublisherRegistry;
use decdn_incentive::tx::SendOutcome;

use crate::commands::chain_ctx;
use crate::commands::chain_ctx::ResolvedPublish;

/// Entry point for `decdn publish`.
pub async fn publish_dispatch(
    args: &cli::PublishArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        cli::PublishCommand::Namespace(ns) => match &ns.command {
            cli::NamespaceCommand::Create(a) => namespace_create(a, global_config).await,
        },
        cli::PublishCommand::Assign(a) => assign(a, global_config).await,
        cli::PublishCommand::Revoke(a) => revoke(a, global_config).await,
    }
}

/// Resolve the publisher coordinates and parse the `PublisherRegistry`
/// address. Used by `namespace create`; the signer and provider are built on
/// the submit path by [`signer_and_provider`].
fn registry_ctx(
    chain: &cli::PublishChainArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<(ResolvedPublish, Address)> {
    let config_path = chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve_publish(chain, &file)?;
    let registry = resolved.publisher_registry_address.ok_or_else(|| {
        anyhow::anyhow!(
            "publisher_registry_address not set (pass \
             --publisher-registry-address or set \
             blockchain.publisher_registry_address)"
        )
    })?;
    Ok((resolved, registry))
}

/// Resolve the publisher coordinates and parse the `OriginAssignment` address.
/// The `assign` parallel of [`registry_ctx`]; the signer and provider are built
/// on the submit path by [`signer_and_provider`].
fn assignment_ctx(
    chain: &cli::PublishChainArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<(ResolvedPublish, Address)> {
    let config_path = chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve_publish(chain, &file)?;
    let origin_assignment = resolved.origin_assignment_address.ok_or_else(|| {
        anyhow::anyhow!(
            "origin_assignment_address not set (pass --origin-assignment-address \
             or set blockchain.origin_assignment_address)"
        )
    })?;
    Ok((resolved, origin_assignment))
}

/// Preflight the RPC network, decrypt the keystore signer, and build the
/// wallet-filled provider — the byte-identical submit preamble shared by every
/// submitting subcommand. Called only on the submit path
/// (never a dry run), so it always decrypts. The network is verified *before*
/// touching secrets, so a wrong `--rpc-url` fails here rather than after an
/// interactive password prompt + scrypt KDF.
///
/// Returns owned `(signer, provider)`: `build_provider` clones the signer into
/// the wallet (`+ use<>`), so the provider borrows nothing and the caller can
/// hold both as independent locals.
async fn signer_and_provider(
    resolved: &ResolvedPublish,
    chain: &cli::PublishChainArgs,
) -> anyhow::Result<(PrivateKeySigner, impl Provider + Clone)> {
    preflight_chain_id(&resolved.rpc_url, resolved.chain_id).await?;
    let signer = chain_ctx::load_operator_signer(&chain.common, &resolved.keystore).await?;
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
    Ok((signer, provider))
}

/// Reject duplicate operator addresses before submitting — the contract
/// reverts `DuplicateOperator` (`OriginAssignment.addOrigin`) on an operator
/// that is already seated, so failing fast on the client saves the gas of a
/// doomed transaction *and* stops a duplicate from stranding the run half-way
/// through the per-operator loop.
fn ensure_unique_operators(ops: &[Address]) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::with_capacity(ops.len());
    for op in ops {
        if !seen.insert(*op) {
            anyhow::bail!("duplicate operator address: {op:#x}");
        }
    }
    Ok(())
}

/// Fail if the RPC reports a different chain id than the resolved one, so a
/// mis-pointed `--rpc-url` cannot submit to the wrong network. Pure so it is
/// unit-testable; [`ensure_rpc_chain_id`] does the single network read.
fn chain_id_guard(expected: u64, rpc: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected == rpc,
        "chain id mismatch: --chain-id/config expects {expected} but the RPC reports {rpc} \
         (check --rpc-url points at the right network, or pass --chain-id {rpc})",
    );
    Ok(())
}

/// Read the RPC's chain id and enforce it matches `expected` before submitting.
async fn ensure_rpc_chain_id<P: alloy::providers::Provider>(
    provider: &P,
    expected: u64,
) -> anyhow::Result<()> {
    let rpc = provider
        .get_chain_id()
        .await
        .context("failed to read chainId from the RPC (is --rpc-url reachable?)")?;
    chain_id_guard(expected, rpc)
}

/// Verify the RPC network matches `expected` using a read-only (wallet-less)
/// provider, *before* any keystore decryption — so a mis-pointed `--rpc-url`
/// fails fast without an interactive password prompt or the scrypt KDF. The
/// `rpc_url` value is never echoed into the parse error (it commonly carries an
/// API key), matching [`decdn_client_pull::provider::build_provider`].
async fn preflight_chain_id(rpc_url: &str, expected: u64) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().with_context(|| {
        format!(
            "rpc_url is not a valid URL (<redacted>, {} chars)",
            rpc_url.len()
        )
    })?);
    ensure_rpc_chain_id(&provider, expected).await
}

// -------------------------------------------------------------------------
// namespace create
// -------------------------------------------------------------------------

/// The outcome of `namespace create`'s single transaction — the five mutually
/// exclusive states as one value, so text and JSON rendering derive from one
/// source of truth and cannot contradict each other. Modeled
/// on [`SeatOutcome`]: an illegal combination (a hash on a dry run, a `created`
/// with no hash) is unrepresentable rather than merely unreached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceStatus {
    /// `--dry-run`: nothing was sent, and no keystore was loaded.
    DryRun,
    /// Nothing was minted: the node rejected the send before broadcast. Distinct
    /// from `Reverted` (which mined) and `MaybeBroadcast` (whose fate is unknown).
    Failed,
    /// A transport-class send failure (timeout, reset, 5xx). The request may have
    /// reached the node and minted a namespace, but no hash was captured, so the
    /// outcome is unknown and a blind retry can burn quota against
    /// `maxNamespacesPerPublisher` (#1577).
    MaybeBroadcast,
    /// The transaction was broadcast but its receipt was unreadable, so a
    /// namespace may or may not exist. Keeps the hash — the only handle on it.
    InFlight(B256),
    /// Mined, but the call reverted: it burned gas and minted nothing. Keeps its
    /// hash as a permanent handle, so a gas-reconciling consumer can find it —
    /// distinct from `Failed`, which never broadcast (#1550).
    Reverted(B256),
    /// `createNamespace` confirmed. `id` is `None` only when the receipt carried
    /// no decodable `NamespaceCreated` log; the tx hash is then the sole handle
    /// on the live namespace, so it is never dropped.
    Created { tx: B256, id: Option<u64> },
}

impl NamespaceStatus {
    /// The transaction hash, for any state that has one.
    const fn tx(self) -> Option<B256> {
        match self {
            Self::InFlight(hash) | Self::Reverted(hash) | Self::Created { tx: hash, .. } => {
                Some(hash)
            }
            Self::DryRun | Self::Failed | Self::MaybeBroadcast => None,
        }
    }

    /// The minted namespace id, known only when the create confirmed and its
    /// `NamespaceCreated` log decoded. `Some` therefore implies `status ==
    /// "created"`, which is the invariant a `--json` consumer relies on.
    const fn namespace_id(self) -> Option<u64> {
        match self {
            Self::Created { id, .. } => id,
            Self::DryRun
            | Self::Failed
            | Self::MaybeBroadcast
            | Self::InFlight(_)
            | Self::Reverted(_) => None,
        }
    }

    /// Whether the transaction was broadcast, for the `submitted` field:
    /// `Some(true)`/`Some(false)` when it is known, and `None` for
    /// `MaybeBroadcast`, where it is exactly what cannot be answered. A flat
    /// `false` there is the #1577 lie.
    const fn submitted(self) -> Option<bool> {
        match self {
            Self::MaybeBroadcast => None,
            Self::InFlight(_) | Self::Reverted(_) | Self::Created { .. } => Some(true),
            Self::DryRun | Self::Failed => Some(false),
        }
    }

    /// The `status=` token in the receipt.
    const fn label(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Failed => "failed",
            Self::MaybeBroadcast => "maybe_broadcast",
            Self::InFlight(_) => "unknown",
            Self::Reverted(_) => "reverted",
            Self::Created { .. } => "created",
        }
    }
}

/// The `submitted=` token for a single-tx text receipt whose transaction has no
/// hash: a definite `true`/`false`, or `unknown` for a transport failure whose
/// broadcast cannot be determined.
const fn submitted_token(submitted: Option<bool>) -> &'static str {
    match submitted {
        Some(true) => "true",
        Some(false) => "false",
        None => "unknown",
    }
}

/// Result of `namespace create`. `operator` is `None` on a dry run (which loads
/// no keystore, so the signer address is unknown).
pub(crate) struct NamespaceOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) registry: Address,
    pub(crate) status: NamespaceStatus,
}

pub(crate) fn write_namespace_outcome(
    w: &mut impl io::Write,
    o: &NamespaceOutcome,
    json: bool,
) -> io::Result<()> {
    let tx = o.status.tx();
    let namespace_id = o.status.namespace_id();
    if json {
        let value = serde_json::json!({
            "submitted": o.status.submitted(),
            "tx": tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "publisher_registry": format!("{:#x}", o.registry),
            "namespace_id": namespace_id,
            "status": o.status.label(),
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "publisher_registry={:#x}", o.registry)?;
    // Its own line, like `ready_at`: the id is absent whenever it could not be
    // decoded, and `status` + `tx` still have to print without it.
    if let Some(id) = namespace_id {
        writeln!(w, "namespace_id={id}")?;
    }
    match tx {
        Some(h) => writeln!(w, "status={} tx={h:#x}", o.status.label()),
        None => writeln!(
            w,
            "status={} submitted={}",
            o.status.label(),
            submitted_token(o.status.submitted())
        ),
    }
}

async fn namespace_create(
    args: &cli::NamespaceCreateArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let (resolved, registry) = registry_ctx(&args.chain, global_config)?;
    let dry_run = args.chain.common.dry_run;
    let mut operator = None;
    let mut status = NamespaceStatus::DryRun;
    // `Ok` on the dry-run path: nothing was sent, so there is nothing to decode
    // and nothing to report as failed. The 0 is never read — only `created.err()`
    // is — which matters because 0 is the registry's reserved "no namespace" id,
    // so it would be a plausible-looking lie if it ever reached the receipt.
    let mut created: anyhow::Result<u64> = Ok(0);

    if !dry_run {
        // The keystore is decrypted only when actually submitting — a dry run
        // needs no secrets.
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        operator = Some(signer.address());
        let contract = PublisherRegistry::new(registry, &provider);
        let mut recorded = SendOutcome::NotSent;
        let sent = decdn_incentive::tx::send_for_receipt(
            contract.createNamespace(),
            "createNamespace",
            Some("check the RPC, gas, and the registry address"),
            &mut recorded,
        )
        .await;

        status = match sent {
            // Every decode failure below happens AFTER the namespace exists
            // on-chain, so it must not short-circuit the receipt — the tx hash is
            // the only handle the publisher has on a namespace whose id was lost,
            // and a blind retry mints a second one against
            // `maxNamespacesPerPublisher`. The receipt's hash always exists on a
            // confirmed create, so `Created` carries it unconditionally.
            Ok(receipt) => {
                created = decode_created_namespace(&receipt);
                NamespaceStatus::Created {
                    tx: receipt.transaction_hash,
                    id: created.as_ref().ok().copied(),
                }
            }
            Err(err) => {
                created = Err(err);
                // `recorded` already distinguishes the failure shapes: a broadcast
                // whose receipt was unreadable (`InFlight`, keeps its hash), a
                // transport failure that may still have minted (`MaybeBroadcast`),
                // a confirmed revert (`Reverted`, minted nothing but kept its
                // hash), or a clean rejection (`Rejected` → `Failed`).
                match recorded {
                    SendOutcome::InFlight(hash) => NamespaceStatus::InFlight(hash),
                    SendOutcome::Reverted(hash) => NamespaceStatus::Reverted(hash),
                    SendOutcome::MaybeBroadcast => NamespaceStatus::MaybeBroadcast,
                    SendOutcome::Rejected | SendOutcome::Confirmed(_) | SendOutcome::NotSent => {
                        NamespaceStatus::Failed
                    }
                }
            }
        };
    }

    let outcome = NamespaceOutcome {
        operator,
        registry,
        status,
    };
    let mut out = io::stdout().lock();
    // `flush` explicitly: `Stdout` is a `LineWriter`, so a partial write can
    // buffer the tail and fail at `drop`, where the result is unobservable — a
    // truncated receipt reported as a clean one.
    let write_err = write_namespace_outcome(&mut out, &outcome, args.chain.common.json)
        .and_then(|()| out.flush())
        .err();
    drop(out);
    let context = namespace_failure_context(outcome.status);
    propagate(
        created.err().map(|err| match context {
            Some(note) => err.context(note),
            None => err,
        }),
        write_err,
        "failed to write namespace-create output",
        &single_tx_receipt(
            outcome
                .status
                .namespace_id()
                .map(|id| format!("namespace_id={id}")),
            outcome.status.tx(),
        ),
    )
}

/// Extra guidance to attach to a `namespace create` failure, or `None` when the
/// underlying error already says everything true.
///
/// A namespace cannot be un-created, and creating it already incremented
/// `namespaceCount` against `maxNamespacesPerPublisher` — so an operator who
/// retries blind burns quota to mint a second one while the first stays owned
/// with its id still unrecovered. That warning is only honest once something
/// reached the chain: a rejected send minted nothing and has no hash to point
/// at. It is also the modal failure, since `NamespaceCapReached` is
/// `createNamespace`'s only revert and so surfaces from the pre-flight gas
/// estimate, before anything is broadcast.
const fn namespace_failure_context(status: NamespaceStatus) -> Option<&'static str> {
    match status {
        NamespaceStatus::InFlight(_) => Some(
            "the transaction was broadcast — check the tx above before re-running, since a \
             create that landed already counts against maxNamespacesPerPublisher",
        ),
        // A transport failure may have minted too, but has no hash to point at,
        // so the note sends the operator to this signer's own transactions.
        NamespaceStatus::MaybeBroadcast => Some(
            "the request may have reached the node and broadcast the transaction — check this \
             signer's pending and mined transactions before re-running, since a create that \
             landed already counts against maxNamespacesPerPublisher",
        ),
        // Names a log query rather than the receipt's own log, and deliberately
        // not `ownerOf`. `ownerOf` maps id → owner, so it needs the very id that
        // was lost — it can confirm a guess, never produce one. An owner-filtered
        // `eth_getLogs` can: `createNamespace` emits `NamespaceCreated`
        // unconditionally and indexes both parameters, so the id arrives in a
        // topic with no data decode. That holds for every failure this note rides
        // along with, including a receipt that came back without the log — the
        // log is still on-chain, it just was not in what the RPC returned.
        NamespaceStatus::Created { .. } => Some(
            "the namespace exists and is owned by the signer — recover its id by filtering the \
             registry's NamespaceCreated logs for this signer rather than re-running, which \
             mints a second namespace",
        ),
        // A rejected send or a confirmed revert minted nothing.
        NamespaceStatus::DryRun | NamespaceStatus::Failed | NamespaceStatus::Reverted(_) => None,
    }
}

/// Pull the minted id out of a confirmed `createNamespace` receipt.
///
/// The id comes from the emitted event: the return value is not in a receipt,
/// and a static pre-call would race a concurrent create (`_nextNamespaceId` is
/// global across publishers).
///
/// Match on `topic0` first, then decode, so a log that IS `NamespaceCreated` but
/// whose payload does not match reports a decode failure instead of vanishing
/// into "no such log". That covers an `indexed` change, which leaves the
/// signature hash untouched; a drift that renames the event or changes a
/// parameter type changes the hash itself and lands in the missing-log arm
/// below, which says so.
///
/// Split out so the caller can print its receipt before propagating any of these
/// failures, all of which happen after the namespace has already been minted.
fn decode_created_namespace(
    receipt: &alloy::rpc::types::TransactionReceipt,
) -> anyhow::Result<u64> {
    let created = receipt
        .inner
        .logs()
        .iter()
        .find(|log| log.topic0() == Some(&PublisherRegistry::NamespaceCreated::SIGNATURE_HASH))
        .context(
            "createNamespace succeeded on-chain but its receipt carried no NamespaceCreated \
             log — either the RPC returned an incomplete receipt, or an ABI drift changed the \
             event signature (the namespace id could not be recovered; check the tx on a block \
             explorer)",
        )?
        .log_decode::<PublisherRegistry::NamespaceCreated>()
        .context(
            "createNamespace emitted a NamespaceCreated log that failed to decode (ABI \
             mismatch between this CLI and the deployed PublisherRegistry?)",
        )?;
    u64::try_from(created.inner.data.namespaceId)
        .context("namespace id exceeds u64 (unexpected on this chain)")
}

/// Return the on-chain failure if there was one, otherwise the stdout-write
/// failure, otherwise success.
///
/// The order is the point: a broken pipe must never outrank — and so hide — a
/// chain error the operator has to act on. Printing the receipt first is what
/// makes a partial or ambiguous on-chain effect recoverable, and that guarantee
/// is worthless if the write's own `?` returns before the real error is built.
///
/// `receipt` carries that same guarantee across the one case where printing
/// first is not enough: the write itself failed, so whatever the command learned
/// on-chain never reached stdout. It rides in the error context instead, which
/// goes to stderr — a different descriptor, still writable when stdout is a
/// closed pipe or a full disk. Without it, `cmd | head -1` can mint a namespace
/// and report only that the output could not be written.
fn propagate(
    chain_err: Option<anyhow::Error>,
    write_err: Option<io::Error>,
    write_context: &'static str,
    receipt: &str,
) -> anyhow::Result<()> {
    match (chain_err, write_err) {
        (Some(err), None) => Err(err),
        // `anyhow` renders the outermost context first, so this wording has to
        // lead with the chain failure — the write error is the aside, not the
        // headline.
        (Some(err), Some(w)) => Err(err.context(format!(
            "the command failed on-chain (cause below), and its receipt could not be written \
             to stdout ({receipt}): {w}"
        ))),
        (None, Some(w)) => {
            Err(anyhow::Error::from(w).context(format!("{write_context} ({receipt})")))
        }
        (None, None) => Ok(()),
    }
}

/// The receipt facts for a command that submits exactly ONE transaction, in the
/// shape [`propagate`] needs them: what landed, and the hash that is its handle.
///
/// `decoded` is the command's own decoded value (`namespace_id=9`,
/// `ready_at=…`), already rendered, or `None` when there was nothing to decode
/// or the decode is what failed.
fn single_tx_receipt(decoded: Option<String>, tx: Option<B256>) -> String {
    match (decoded, tx) {
        (Some(d), Some(h)) => format!("{d} tx={h:#x}"),
        (None, Some(h)) => format!("tx={h:#x}"),
        (Some(d), None) => d,
        (None, None) => "nothing was sent".to_string(),
    }
}

// -------------------------------------------------------------------------
// assign (instant, one addOrigin per operator)
// -------------------------------------------------------------------------

/// The `addOrigin` failure hint: every guard the contract applies, led by the one
/// a publisher hits first. `removeOrigin` has its own, since none of the operator
/// guards below apply to unseating.
const ADD_ORIGIN_HINT: &str = "the signer must be a vetted publisher (a VETTER_ROLE holder on \
     the installed vetting policy vets it out-of-band) and own the namespace; the operator must \
     be an active bonded node, not blacklisted, not already seated, and must fit under \
     maxOriginsPerNamespace";

/// What happened to one operator in the seating loop.
///
/// Seating is one transaction per operator, so a run that stops half-way leaves
/// the namespace genuinely part-seated. The receipt has to say which operators
/// are live, which definitely are not, and — the case that matters most — which
/// one was broadcast without a readable outcome, because that one must not be
/// blindly re-sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeatOutcome {
    /// The `addOrigin` transaction confirmed. The operator is authorized.
    Seated(B256),
    /// The transaction was broadcast but its outcome could not be read (the
    /// receipt fetch failed). It may still confirm. `send_for_receipt` records
    /// the hash before awaiting the receipt precisely so this case can carry it.
    InFlight(B256),
    /// Mined, but the transaction reverted: it burned gas and did not seat the
    /// operator. Keeps its hash as a permanent handle — distinct from `Failed`,
    /// which never broadcast (#1550).
    Reverted(B256),
    /// The send failed with a transport-class error, so whether it broadcast is
    /// unknown and no hash was captured. Re-sending can double-submit, so this is
    /// distinct from `Failed` (#1577).
    MaybeBroadcast,
    /// The node rejected the send — nothing was ever broadcast. The usual shape,
    /// since the `addOrigin` guards surface from the pre-flight gas estimate.
    Failed,
    /// The loop stopped before this operator was tried.
    NotAttempted,
}

impl SeatOutcome {
    /// Derive a seat's outcome from the `send_for_receipt` result and the
    /// [`SendOutcome`] it recorded. `Ok` always carries the confirmed hash from
    /// the receipt; the `Err` arms map straight across from `send`.
    const fn from_send(
        sent: &anyhow::Result<alloy::rpc::types::TransactionReceipt>,
        recorded: SendOutcome,
    ) -> Self {
        match sent {
            Ok(receipt) => Self::Seated(receipt.transaction_hash),
            Err(_) => match recorded {
                SendOutcome::InFlight(h) => Self::InFlight(h),
                SendOutcome::Reverted(h) => Self::Reverted(h),
                SendOutcome::MaybeBroadcast => Self::MaybeBroadcast,
                // A rejected send, or the unreachable `Confirmed`/`NotSent` on an
                // `Err`: nothing the operator can chase, so `Failed`.
                SendOutcome::Rejected | SendOutcome::Confirmed(_) | SendOutcome::NotSent => {
                    Self::Failed
                }
            },
        }
    }

    /// The transaction hash, for any state that captured one.
    const fn tx(self) -> Option<B256> {
        match self {
            Self::Seated(hash) | Self::InFlight(hash) | Self::Reverted(hash) => Some(hash),
            Self::MaybeBroadcast | Self::Failed | Self::NotAttempted => None,
        }
    }

    /// The `state=` token in the receipt.
    const fn label(self) -> &'static str {
        match self {
            Self::Seated(_) => "seated",
            Self::InFlight(_) => "in_flight",
            Self::Reverted(_) => "reverted",
            Self::MaybeBroadcast => "maybe_broadcast",
            Self::Failed => "failed",
            Self::NotAttempted => "not_attempted",
        }
    }

    /// Whether this seat's outcome is uncertain — a broadcast whose receipt could
    /// not be read, or a send whose broadcast is unknown. Either is a state the
    /// operator must resolve before re-running, so it outranks every other status
    /// in [`AssignOutcome::status`].
    const fn is_uncertain(self) -> bool {
        matches!(self, Self::InFlight(_) | Self::MaybeBroadcast)
    }
}

/// Receipt for `publish assign`.
///
/// `seats` carries every requested operator in the order given, each paired with
/// what happened to it — one entry per operator, always, so the receipt cannot
/// misattribute a transaction or silently omit an operator. `dry_run` is a field
/// rather than inferred from the absence of transactions because "nothing was
/// sent" and "the first send failed" are different answers that both leave zero
/// seats — the same reason `RevokeOutcome` carries it too.
pub(crate) struct AssignOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    pub(crate) seats: Vec<(Address, SeatOutcome)>,
    pub(crate) dry_run: bool,
}

impl AssignOutcome {
    fn seated_count(&self) -> usize {
        self.seats
            .iter()
            .filter(|(_, s)| matches!(s, SeatOutcome::Seated(_)))
            .count()
    }

    /// `dry_run` before anything is sent; `unknown` whenever a transaction was
    /// broadcast without a readable outcome (it outranks everything else — it is
    /// the state the operator must resolve before re-running); then `seated`,
    /// `failed`, or `partial` by how many seats landed.
    fn status(&self) -> &'static str {
        if self.dry_run {
            return "dry_run";
        }
        if self.seats.iter().any(|(_, s)| s.is_uncertain()) {
            return "unknown";
        }
        // `0` first: an empty `seats` would otherwise satisfy `n == len` and
        // report every operator seated when none were.
        match self.seated_count() {
            0 => "failed",
            n if n == self.seats.len() => "seated",
            _ => "partial",
        }
    }
}

pub(crate) fn write_assign_outcome(
    w: &mut impl io::Write,
    o: &AssignOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.seats.iter().any(|(_, s)| s.tx().is_some()),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "operators": o.seats.iter().map(|(a, _)| format!("{a:#x}")).collect::<Vec<_>>(),
            "origins": o.seats.iter().map(|(a, s)| serde_json::json!({
                "operator": format!("{a:#x}"),
                "tx": s.tx().map(|h| format!("{h:#x}")),
                "state": s.label(),
            })).collect::<Vec<_>>(),
            "status": o.status(),
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "origin_assignment={:#x}", o.origin_assignment)?;
    writeln!(w, "namespace_id={}", o.namespace_id)?;
    writeln!(w, "operators={}", o.seats.len())?;
    for (a, seat) in &o.seats {
        match seat.tx() {
            Some(tx) => writeln!(w, "  operator={a:#x} tx={tx:#x} state={}", seat.label())?,
            None => writeln!(w, "  operator={a:#x} state={}", seat.label())?,
        }
    }
    if o.dry_run {
        writeln!(w, "status=dry_run submitted=false")
    } else {
        writeln!(w, "status={} seated={}", o.status(), o.seated_count())
    }
}

async fn assign(args: &cli::AssignArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let (resolved, oa_addr) = assignment_ctx(&args.chain, global_config)?;
    let operators = args
        .operators
        .iter()
        .map(|s| chain_ctx::parse_address(s, "operator"))
        .collect::<anyhow::Result<Vec<Address>>>()?;
    ensure_unique_operators(&operators)?;

    let mut outcome = AssignOutcome {
        operator: None,
        origin_assignment: oa_addr,
        namespace_id: args.namespace,
        seats: Vec::with_capacity(operators.len()),
        dry_run: args.chain.common.dry_run,
    };

    // Seed one slot per requested operator up front, so "one entry per operator,
    // in order" is a property of this line rather than of the loop's bookkeeping.
    // The loop only ever OVERWRITES a slot — it cannot drop, duplicate, or
    // reorder an operator, and everything it never reaches stays `NotAttempted`.
    outcome.seats = operators
        .iter()
        .map(|a| (*a, SeatOutcome::NotAttempted))
        .collect();

    // Seating is a per-operator delta, so a set of N operators is N
    // transactions. They run in the order given and stop at the first failure:
    // the remaining operators are usually doomed for the same reason (an
    // unvetted signer, a namespace the signer does not own), and continuing
    // would burn gas to collect the identical error N times.
    let mut failure = None;
    if !outcome.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        for (operator, seat) in &mut outcome.seats {
            let mut recorded = SendOutcome::NotSent;
            let sent = decdn_incentive::tx::send_for_receipt(
                contract.addOrigin(U256::from(args.namespace), *operator),
                "addOrigin",
                Some(ADD_ORIGIN_HINT),
                &mut recorded,
            )
            .await;
            // `from_send` keeps the two uncertain shapes (`in_flight`,
            // `maybe_broadcast`) and the mined revert distinct from a clean
            // rejection, so the receipt never tells an operator to blindly
            // re-send a transaction that may still be pending (#1550, #1577).
            *seat = SeatOutcome::from_send(&sent, recorded);
            if let Err(err) = sent {
                failure = Some((*operator, err));
                break;
            }
        }
    }

    // Print BEFORE propagating: the seats that already landed are on-chain, and
    // an operator who only sees the error has no way to tell which.
    let mut out = io::stdout().lock();
    let write_err = write_assign_outcome(&mut out, &outcome, args.chain.common.json)
        .and_then(|()| out.flush())
        .err();
    drop(out);

    // Every seat that reached the chain, so a lost receipt still names them: this
    // command's whole hazard is not knowing which operators are already live.
    let seats = outcome
        .seats
        .iter()
        .filter_map(|(operator, seat)| seat.tx().map(|h| format!("{operator:#x}={h:#x}")))
        .collect::<Vec<_>>()
        .join(" ");
    let receipt = if seats.is_empty() {
        "nothing was sent".to_string()
    } else {
        seats
    };

    let chain_err = failure.map(|(operator, err)| {
        err.context(format!(
            "addOrigin failed for operator {operator:#x} after seating {} of {} (the seated \
             operators are live; once the cause is fixed, re-run with the operators marked \
             failed or not_attempted, and check any marked in_flight or maybe_broadcast — the \
             first on a block explorer, the second among this signer's own pending and mined \
             transactions — before re-sending them)",
            outcome.seated_count(),
            outcome.seats.len(),
        ))
    });
    propagate(
        chain_err,
        write_err,
        "failed to write assign output",
        &receipt,
    )
}

// -------------------------------------------------------------------------
// revoke
// -------------------------------------------------------------------------

/// The outcome of `publish revoke`'s single transaction — the four mutually
/// exclusive states as one value, the `removeOrigin` parallel of
/// [`NamespaceStatus`]. It has no decoded payload, so `Revoked` is a bare hash
/// rather than a struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RevokeStatus {
    /// `--dry-run`: nothing was sent, and no keystore was loaded.
    DryRun,
    /// Nothing was unseated: the node rejected the send before broadcast.
    /// Distinct from `Reverted` (mined) and `MaybeBroadcast` (fate unknown).
    Failed,
    /// A transport-class send failure (timeout, reset, 5xx). The request may have
    /// reached the node and unseated the operator, but no hash was captured, so
    /// the outcome is unknown and re-running can double-submit (#1577).
    MaybeBroadcast,
    /// The transaction was broadcast but its receipt was unreadable, so the
    /// operator may or may not still be seated. Keeps the hash — the only handle
    /// on it.
    InFlight(B256),
    /// Mined, but the call reverted: it burned gas and unseated nothing. Keeps
    /// its hash — distinct from `Failed`, which never broadcast (#1550).
    Reverted(B256),
    /// `removeOrigin` confirmed. The operator is unseated.
    Revoked(B256),
}

impl RevokeStatus {
    /// The transaction hash, for any state that has one.
    const fn tx(self) -> Option<B256> {
        match self {
            Self::InFlight(hash) | Self::Reverted(hash) | Self::Revoked(hash) => Some(hash),
            Self::DryRun | Self::Failed | Self::MaybeBroadcast => None,
        }
    }

    /// Whether the transaction was broadcast, for the `submitted` field. `None`
    /// for `MaybeBroadcast`, where it cannot be answered — a flat `false` there
    /// is the #1577 lie.
    const fn submitted(self) -> Option<bool> {
        match self {
            Self::MaybeBroadcast => None,
            Self::InFlight(_) | Self::Reverted(_) | Self::Revoked(_) => Some(true),
            Self::DryRun | Self::Failed => Some(false),
        }
    }

    /// The `status=` token in the receipt.
    const fn label(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Failed => "failed",
            Self::MaybeBroadcast => "maybe_broadcast",
            Self::InFlight(_) => "unknown",
            Self::Reverted(_) => "reverted",
            Self::Revoked(_) => "revoked",
        }
    }
}

/// Receipt for `publish revoke`. One transaction, so the shape is simpler than
/// [`AssignOutcome`] — but it needs the same honesty about a broadcast whose
/// outcome could not be read.
pub(crate) struct RevokeOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    /// The operator being unseated.
    pub(crate) revoked: Address,
    pub(crate) status: RevokeStatus,
}

pub(crate) fn write_revoke_outcome(
    w: &mut impl io::Write,
    o: &RevokeOutcome,
    json: bool,
) -> io::Result<()> {
    let tx = o.status.tx();
    if json {
        let value = serde_json::json!({
            "submitted": o.status.submitted(),
            "tx": tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "revoked_operator": format!("{:#x}", o.revoked),
            "status": o.status.label(),
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "origin_assignment={:#x}", o.origin_assignment)?;
    writeln!(w, "namespace_id={}", o.namespace_id)?;
    writeln!(w, "revoked_operator={:#x}", o.revoked)?;
    match tx {
        Some(h) => writeln!(w, "status={} tx={h:#x}", o.status.label()),
        None => writeln!(
            w,
            "status={} submitted={}",
            o.status.label(),
            submitted_token(o.status.submitted())
        ),
    }
}

async fn revoke(args: &cli::RevokeArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let (resolved, oa_addr) = assignment_ctx(&args.chain, global_config)?;
    let revoked = chain_ctx::parse_address(&args.operator, "operator")?;

    let dry_run = args.chain.common.dry_run;
    let mut operator = None;
    let mut status = RevokeStatus::DryRun;

    // Print before propagating, for the same reason `assign` does: a broadcast
    // whose receipt could not be read leaves the hash set, and a `--json`
    // consumer that only sees the error loses its one handle on the removal.
    let mut chain_err = None;
    if !dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        let mut send = SendOutcome::NotSent;
        match decdn_incentive::tx::send_for_receipt(
            contract.removeOrigin(U256::from(args.namespace), revoked),
            "removeOrigin",
            Some(
                "the signer must own the namespace (or hold GOVERNANCE_ROLE) and the operator \
                 must currently be seated as an authorized origin for it",
            ),
            &mut send,
        )
        .await
        {
            Ok(receipt) => status = RevokeStatus::Revoked(receipt.transaction_hash),
            Err(err) => {
                // `send` distinguishes a broadcast whose receipt was unreadable
                // (`InFlight`), a transport failure that may have unseated
                // (`MaybeBroadcast`), a confirmed revert (`Reverted`, unseated
                // nothing), and a clean rejection (`Rejected` → `Failed`).
                status = match send {
                    SendOutcome::InFlight(hash) => RevokeStatus::InFlight(hash),
                    SendOutcome::Reverted(hash) => RevokeStatus::Reverted(hash),
                    SendOutcome::MaybeBroadcast => RevokeStatus::MaybeBroadcast,
                    SendOutcome::Rejected | SendOutcome::Confirmed(_) | SendOutcome::NotSent => {
                        RevokeStatus::Failed
                    }
                };
                chain_err = Some(match revoke_failure_context(status) {
                    Some(note) => err.context(note),
                    None => err,
                });
            }
        }
    }

    let outcome = RevokeOutcome {
        operator,
        origin_assignment: oa_addr,
        namespace_id: args.namespace,
        revoked,
        status,
    };
    let mut out = io::stdout().lock();
    let write_err = write_revoke_outcome(&mut out, &outcome, args.chain.common.json)
        .and_then(|()| out.flush())
        .err();
    drop(out);
    propagate(
        chain_err,
        write_err,
        "failed to write revoke output",
        &single_tx_receipt(None, outcome.status.tx()),
    )
}

/// Extra guidance to attach to a `revoke` failure, or `None` when the underlying
/// error already says everything true. The parallel of
/// [`namespace_failure_context`].
///
/// Only the broadcast case needs it: a removal that landed makes the retry
/// revert `NotAuthorizedOrigin`, which reads like a permissions problem rather
/// than "this already worked". A rejected send unseated nothing and has no hash
/// to cite, so `send_for_receipt`'s own error is already complete.
const fn revoke_failure_context(status: RevokeStatus) -> Option<&'static str> {
    match status {
        RevokeStatus::InFlight(_) => Some(
            "the transaction was broadcast — check the tx above before re-running, since a \
             removal that landed makes the retry revert NotAuthorizedOrigin",
        ),
        // A transport failure may have unseated too, but has no hash to cite.
        RevokeStatus::MaybeBroadcast => Some(
            "the request may have reached the node and broadcast the transaction — check this \
             signer's pending and mined transactions before re-running, since a removal that \
             landed makes the retry revert NotAuthorizedOrigin",
        ),
        // A rejected send or a confirmed revert unseated nothing.
        RevokeStatus::DryRun
        | RevokeStatus::Failed
        | RevokeStatus::Reverted(_)
        | RevokeStatus::Revoked(_) => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn namespace_dry_run_and_submitted_formats() {
        // Dry run: no signer loaded, so no operator line is printed.
        let base = NamespaceOutcome {
            operator: None,
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::DryRun,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &base, false).unwrap();
        let dry = String::from_utf8(buf).unwrap();
        assert!(dry.contains("status=dry_run submitted=false"), "{dry}");
        assert!(!dry.contains("operator="), "{dry}");
        assert!(!dry.contains("namespace_id="), "{dry}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            status: NamespaceStatus::Created {
                tx: B256::repeat_byte(0x55),
                id: Some(9),
            },
            ..base
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operator=0xcdcd"), "{s}");
        assert!(s.contains("namespace_id=9"), "{s}");
        assert!(s.contains("status=created tx=0x5555"), "{s}");
    }

    /// The enum makes an illegal pair unrepresentable: a `dry_run: true`
    /// alongside a `tx: Some` cannot print `status=dry_run tx=…` or emit
    /// `"submitted": true` next to `"status": "dry_run"`. `DryRun` carries no hash,
    /// so `tx()` is `None` and text and JSON agree on both fields — which this pins.
    #[test]
    fn namespace_dry_run_carries_no_tx_and_is_not_submitted() {
        let dry = NamespaceOutcome {
            operator: None,
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::DryRun,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &dry, false).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(!text.contains("tx=0x"), "{text}");
        assert!(text.contains("status=dry_run submitted=false"), "{text}");

        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &dry, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("dry_run"));
        assert_eq!(v["submitted"], serde_json::json!(false));
        assert!(v["tx"].is_null(), "{v}");
    }

    /// The state this receipt exists for: `createNamespace` confirmed, so the
    /// namespace is live and already counts against the publisher's cap, but its
    /// id could not be decoded. The tx hash is then the operator's ONLY handle on
    /// it — dropping the receipt here is what would make the id unrecoverable and
    /// a retry quota-burning. It is also not a dry run, and must never say so.
    ///
    /// Then walks the other two non-dry-run statuses off the same fixture: a
    /// broadcast whose receipt could not be read is `unknown`, and no hash at all
    /// is `failed`.
    #[test]
    fn namespace_output_distinguishes_created_unknown_and_failed() {
        let o = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::Created {
                tx: B256::repeat_byte(0x55),
                id: None,
            },
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=created tx=0x5555"), "{s}");
        assert!(!s.contains("dry_run"), "{s}");
        assert!(!s.contains("namespace_id="), "{s}");

        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        // `created` + a null id is what separates this from "not created": a
        // `--json` consumer switching on `status` cannot confuse the two.
        assert_eq!(v["status"], serde_json::json!("created"));
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert!(v["namespace_id"].is_null(), "{v}");
        assert_eq!(
            v["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );

        // A broadcast whose receipt could not be read is UNKNOWN, not created —
        // but it keeps its hash, because a namespace that may exist needs the
        // same handle as one that does.
        let unknown = NamespaceOutcome {
            status: NamespaceStatus::InFlight(B256::repeat_byte(0x55)),
            ..o
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &unknown, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("unknown"));
        assert_eq!(
            v["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );

        // Nothing reached the chain: `failed`, and distinguishable from a dry run.
        let failed = NamespaceOutcome {
            status: NamespaceStatus::Failed,
            ..o
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &failed, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("failed"));
        assert_eq!(v["submitted"], serde_json::json!(false));
    }

    /// A mined revert (#1550) reports `reverted` and keeps its hash; a lost send
    /// response (#1577) reports `maybe_broadcast` with a NULL `submitted` and no
    /// hash. Neither may collapse into `failed` — the first has a gas-burning tx
    /// to reconcile, the second may still mint a namespace against the cap.
    #[test]
    fn namespace_output_surfaces_reverted_and_maybe_broadcast() {
        let base = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::Reverted(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &base, false).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("status=reverted tx=0x5555"), "{text}");

        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &base, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("reverted"));
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(
            v["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );

        let maybe = NamespaceOutcome {
            status: NamespaceStatus::MaybeBroadcast,
            ..base
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &maybe, false).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            text.contains("status=maybe_broadcast submitted=unknown"),
            "{text}"
        );

        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &maybe, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("maybe_broadcast"));
        assert!(
            v["submitted"].is_null(),
            "submitted must be null, not false: {v}"
        );
        assert!(v["tx"].is_null(), "{v}");
    }

    /// A `created` with a decoded id prints and emits the id, distinguishing it
    /// from the id-less `created` above — the fifth of the five states, and the
    /// one whose `namespace_id` a `--json` consumer reads back.
    #[test]
    fn namespace_created_with_id_emits_the_id() {
        let o = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::Created {
                tx: B256::repeat_byte(0x55),
                id: Some(9),
            },
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("namespace_id=9"), "{s}");
        assert!(s.contains("status=created tx=0x5555"), "{s}");

        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("created"));
        assert_eq!(v["namespace_id"], serde_json::json!(9));
    }

    /// A `namespace create` failure must only warn about the burnt quota when a
    /// namespace was actually minted. Claiming it on the path where the send was
    /// rejected — the modal failure, since `NamespaceCapReached` surfaces from
    /// the pre-flight gas estimate — sends the publisher hunting for a namespace
    /// that does not exist, and contradicts the `status=failed` receipt printed
    /// one line above.
    #[test]
    fn namespace_failure_context_only_claims_a_namespace_when_one_exists() {
        // Broadcast, receipt unreadable: it may exist, so say so.
        let in_flight =
            namespace_failure_context(NamespaceStatus::InFlight(B256::repeat_byte(0x55))).unwrap();
        assert!(in_flight.contains("was broadcast"), "{in_flight}");
        assert!(
            in_flight.contains("maxNamespacesPerPublisher"),
            "{in_flight}"
        );

        // Confirmed, but the id could not be decoded: the namespace IS owned.
        let landed = namespace_failure_context(NamespaceStatus::Created {
            tx: B256::repeat_byte(0x55),
            id: None,
        })
        .unwrap();
        assert!(landed.contains("exists and is owned"), "{landed}");
        // The recovery path has to work for every decode failure, including the
        // one where the receipt came back WITHOUT the log — so it points at an
        // owner-filtered log query (both event params are indexed), not at the
        // receipt's own log, and never at `ownerOf`, which maps id → owner and
        // so needs the id that was just lost.
        assert!(
            landed.contains("NamespaceCreated logs for this signer"),
            "{landed}"
        );
        assert!(!landed.contains("ownerOf"), "{landed}");

        // A lost send response may have minted too, but has no hash to point at,
        // so the note routes the operator to this signer's own transactions.
        let maybe = namespace_failure_context(NamespaceStatus::MaybeBroadcast).unwrap();
        assert!(maybe.contains("may have reached the node"), "{maybe}");
        assert!(maybe.contains("this signer's pending"), "{maybe}");
        assert!(maybe.contains("maxNamespacesPerPublisher"), "{maybe}");

        // Nothing reached the chain — nothing minted, and no hash to cite.
        assert!(
            namespace_failure_context(NamespaceStatus::Failed).is_none(),
            "a rejected send must not claim a created namespace"
        );
        // A confirmed revert minted nothing — the create had no effect.
        assert!(
            namespace_failure_context(NamespaceStatus::Reverted(B256::repeat_byte(0x55))).is_none(),
            "a reverted create minted nothing"
        );
        // A dry run minted nothing either.
        assert!(
            namespace_failure_context(NamespaceStatus::DryRun).is_none(),
            "a dry run must not claim a created namespace"
        );
    }

    /// A confirmed receipt carrying `logs`.
    ///
    /// The decode helpers read only `inner.logs()`, so every other field is
    /// filler. It exists because `TransactionReceipt` has no constructor, and
    /// without it the decode failures below are reachable only against a chain
    /// whose deployed ABI has drifted from this CLI's — which is to say, never
    /// in a test.
    fn receipt_with_logs(
        logs: Vec<alloy::rpc::types::Log>,
    ) -> alloy::rpc::types::TransactionReceipt {
        alloy::rpc::types::TransactionReceipt {
            inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
                receipt: alloy::consensus::Receipt {
                    status: alloy::consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: 0,
                    logs,
                },
                logs_bloom: alloy::primitives::Bloom::ZERO,
            }),
            transaction_hash: B256::repeat_byte(0x55),
            transaction_index: Some(0),
            block_hash: None,
            block_number: None,
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: None,
            contract_address: None,
        }
    }

    /// A log with `topics` and no data — enough to drive `topic0` matching and
    /// the decode that follows it.
    fn log_with_topics(topics: Vec<B256>) -> alloy::rpc::types::Log {
        alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x01),
                data: alloy::primitives::LogData::new_unchecked(
                    topics,
                    alloy::primitives::Bytes::new(),
                ),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    /// A `NamespaceCreated` log as the registry actually emits it: both
    /// parameters are `indexed`, so the id and the owner arrive as topics and
    /// the data is empty.
    fn namespace_created_log(id: U256, owner: Address) -> alloy::rpc::types::Log {
        log_with_topics(vec![
            PublisherRegistry::NamespaceCreated::SIGNATURE_HASH,
            B256::from(id),
            owner.into_word(),
        ])
    }

    #[test]
    fn decode_created_namespace_reads_the_id_out_of_the_log() {
        let receipt = receipt_with_logs(vec![namespace_created_log(
            U256::from(9),
            Address::repeat_byte(0xCD),
        )]);
        assert_eq!(decode_created_namespace(&receipt).unwrap(), 9);
    }

    /// The failure that makes a live namespace's id unrecoverable, and so the
    /// one that must reach the operator intact rather than as a bare `?`.
    #[test]
    fn decode_created_namespace_reports_a_receipt_with_no_matching_log() {
        // A log from some other event: the receipt is not empty, it just does
        // not carry the one being looked for.
        let receipt = receipt_with_logs(vec![log_with_topics(vec![B256::repeat_byte(0xAB)])]);
        let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
        assert!(err.contains("carried no NamespaceCreated"), "{err}");
    }

    /// `topic0` matches but the payload does not — the case the match-then-decode
    /// ordering exists for. Collapsing the two steps into a single
    /// `find_map(|l| l.log_decode().ok())` would report this as a missing log and
    /// send the operator hunting for an event the chain did emit.
    #[test]
    fn decode_created_namespace_separates_an_undecodable_log_from_a_missing_one() {
        let receipt = receipt_with_logs(vec![log_with_topics(vec![
            PublisherRegistry::NamespaceCreated::SIGNATURE_HASH,
        ])]);
        let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
        assert!(err.contains("failed to decode"), "{err}");
        assert!(!err.contains("carried no NamespaceCreated"), "{err}");
    }

    /// Unreachable against this registry — it allocates from a `uint64` counter —
    /// so this pins that an impossible id is reported rather than truncated into
    /// a plausible one.
    #[test]
    fn decode_created_namespace_rejects_an_id_that_does_not_fit_u64() {
        let receipt = receipt_with_logs(vec![namespace_created_log(
            U256::from(u64::MAX) + U256::from(1),
            Address::repeat_byte(0xCD),
        )]);
        let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
        assert!(err.contains("exceeds u64"), "{err}");
    }

    #[test]
    fn assign_output_lists_every_seat_with_its_tx() {
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![
                (a, SeatOutcome::Seated(B256::repeat_byte(0x55))),
                (b, SeatOutcome::Seated(B256::repeat_byte(0x66))),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operators=2"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
        assert!(s.contains("tx=0x6666"), "{s}");
        assert!(s.contains("status=seated seated=2"), "{s}");
    }

    /// Seating is one transaction per operator, so a mid-run failure leaves some
    /// operators live and the rest not. The receipt must say which — that is the
    /// whole reason it is printed before the error propagates.
    #[test]
    fn assign_output_distinguishes_partial_from_seated() {
        let landed = Address::repeat_byte(0x11);
        let failed = Address::repeat_byte(0x22);
        let untried = Address::repeat_byte(0x33);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![
                (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
                (failed, SeatOutcome::Failed),
                (untried, SeatOutcome::NotAttempted),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains(&format!("  operator={landed:#x} tx=")), "{s}");
        assert!(
            s.contains(&format!("  operator={failed:#x} state=failed")),
            "{s}"
        );
        assert!(
            s.contains(&format!("  operator={untried:#x} state=not_attempted")),
            "{s}"
        );
        assert!(s.contains("status=partial seated=1"), "{s}");
    }

    /// Nothing landed is NOT "partial" — that would read as if a seat exists.
    /// This is the modal failure (an unvetted signer), so the status a machine
    /// consumer switches on has to be right for it.
    #[test]
    fn assign_output_reports_failed_when_no_seat_landed() {
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![
                (Address::repeat_byte(0x11), SeatOutcome::Failed),
                (Address::repeat_byte(0x22), SeatOutcome::NotAttempted),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=failed seated=0"), "{s}");

        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("failed"));
        assert_eq!(v["submitted"], serde_json::json!(false));
    }

    /// A transaction that was broadcast but whose receipt could not be read has
    /// an UNKNOWN outcome. Reporting it as reverted is how an operator gets told
    /// to re-send a transaction that is still pending, so it gets its own state,
    /// keeps its hash, and outranks every other status.
    #[test]
    fn assign_output_surfaces_an_in_flight_transaction() {
        let landed = Address::repeat_byte(0x11);
        let unknown = Address::repeat_byte(0x22);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![
                (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
                (unknown, SeatOutcome::InFlight(B256::repeat_byte(0x66))),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains(&format!("  operator={unknown:#x} tx=0x6666")),
            "the in-flight hash must reach the receipt, not just the error chain: {s}"
        );
        assert!(s.contains("state=in_flight"), "{s}");
        assert!(s.contains("status=unknown"), "{s}");

        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("unknown"));
        assert_eq!(v["submitted"], serde_json::json!(true));
        let origins = v["origins"].as_array().unwrap();
        assert_eq!(origins[1]["state"], serde_json::json!("in_flight"));
        assert_eq!(
            origins[1]["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x66)))
        );
    }

    /// A mined revert (#1550) keeps its hash as `reverted`; a lost send response
    /// (#1577) reports `maybe_broadcast` with no hash. Neither collapses into a
    /// bare `failed`. `maybe_broadcast` is uncertain, so it drives `status=unknown`.
    #[test]
    fn assign_seats_distinguish_reverted_and_maybe_broadcast_from_failed() {
        // A run whose only send reverted: the seat cites its hash, and with no
        // uncertain seat the run's status is the definite `failed` (0 seated).
        let reverted_only = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![(
                Address::repeat_byte(0x11),
                SeatOutcome::Reverted(B256::repeat_byte(0x66)),
            )],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &reverted_only, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("failed"));
        let origins = v["origins"].as_array().unwrap();
        assert_eq!(origins[0]["state"], serde_json::json!("reverted"));
        assert_eq!(
            origins[0]["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x66))),
            "a mined revert must keep its hash: {v}"
        );

        // A lost send response: uncertain, no hash, so `status=unknown`.
        let maybe = AssignOutcome {
            seats: vec![(Address::repeat_byte(0x22), SeatOutcome::MaybeBroadcast)],
            ..reverted_only
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &maybe, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("unknown"));
        let origins = v["origins"].as_array().unwrap();
        assert_eq!(origins[0]["state"], serde_json::json!("maybe_broadcast"));
        assert!(origins[0]["tx"].is_null(), "no hash was captured: {v}");
    }

    /// `seated` means every requested operator landed. An empty `seats` satisfies
    /// `seated_count == len` vacuously, so the zero case has to be answered first
    /// or a receipt with no operators at all claims success.
    #[test]
    fn assign_status_does_not_call_an_empty_run_seated() {
        let empty = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: Vec::new(),
            dry_run: false,
        };
        assert_eq!(empty.status(), "failed");
    }

    #[test]
    fn assign_dry_run_omits_signer_and_txs() {
        let o = AssignOutcome {
            operator: None,
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![(Address::repeat_byte(0x11), SeatOutcome::NotAttempted)],
            dry_run: true,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=dry_run submitted=false"), "{s}");
        // The signer `operator=` line sits at column 0; the operator-set lines
        // are indented (`  operator=`). Only the signer line must be absent.
        assert!(!s.lines().any(|l| l.starts_with("operator=")), "{s}");
        assert!(!s.contains("tx="), "{s}");
    }

    /// The write error must never outrank — and so hide — a chain error the
    /// operator has to act on. `| head -1` on a failed run is the real case.
    #[test]
    fn propagate_prefers_the_chain_error_over_a_broken_pipe() {
        let broken = || io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
        let err = propagate(
            Some(anyhow::anyhow!("addOrigin reverted")),
            Some(broken()),
            "w",
            "nothing was sent",
        )
        .unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("addOrigin reverted"), "{rendered}");
        assert!(rendered.contains("could not be written"), "{rendered}");

        // Write failure alone still surfaces, with its own context.
        let err = propagate(
            None,
            Some(broken()),
            "failed to write assign output",
            "nothing was sent",
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to write assign output"),
            "{err:#}"
        );

        assert!(propagate(None, None, "w", "nothing was sent").is_ok());
    }

    /// A broken pipe must not be able to eat the receipt. Both write-failure arms
    /// carry the facts into the error chain, which goes to stderr — a different
    /// descriptor, still writable when stdout is a closed pipe or a full disk.
    /// The success + broken-pipe arm is the sharp one: the namespace was minted
    /// AND its id decoded, and without this the operator learns neither.
    #[test]
    fn propagate_carries_the_receipt_when_stdout_could_not_take_it() {
        let broken = || io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
        let receipt = single_tx_receipt(
            Some("namespace_id=9".to_string()),
            Some(B256::repeat_byte(0x55)),
        );

        let err = propagate(None, Some(broken()), "failed to write output", &receipt).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("namespace_id=9"), "{rendered}");
        assert!(rendered.contains("tx=0x5555"), "{rendered}");

        // And alongside a chain failure, where the receipt is the only handle on
        // a namespace whose id could not be decoded.
        let err = propagate(
            Some(anyhow::anyhow!("no NamespaceCreated log")),
            Some(broken()),
            "failed to write output",
            &single_tx_receipt(None, Some(B256::repeat_byte(0x55))),
        )
        .unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("no NamespaceCreated log"), "{rendered}");
        assert!(rendered.contains("tx=0x5555"), "{rendered}");
    }

    #[test]
    fn single_tx_receipt_names_only_what_is_known() {
        let hash = B256::repeat_byte(0x55);
        assert_eq!(
            single_tx_receipt(Some("namespace_id=9".to_string()), Some(hash)),
            format!("namespace_id=9 tx={hash:#x}")
        );
        assert_eq!(single_tx_receipt(None, Some(hash)), format!("tx={hash:#x}"));
        // A rejected send: there is no hash, and saying so beats an empty note.
        assert_eq!(single_tx_receipt(None, None), "nothing was sent");
    }

    #[test]
    fn revoke_failure_context_only_warns_once_something_was_broadcast() {
        let in_flight =
            revoke_failure_context(RevokeStatus::InFlight(B256::repeat_byte(0x55))).unwrap();
        assert!(in_flight.contains("was broadcast"), "{in_flight}");
        // The retry's revert reads like a permissions problem, so it has to be
        // named — that is the whole reason this note exists.
        assert!(in_flight.contains("NotAuthorizedOrigin"), "{in_flight}");

        // A lost send response warns too, but points at the signer's own txs
        // since there is no hash to cite.
        let maybe = revoke_failure_context(RevokeStatus::MaybeBroadcast).unwrap();
        assert!(maybe.contains("may have reached the node"), "{maybe}");
        assert!(maybe.contains("NotAuthorizedOrigin"), "{maybe}");

        assert!(
            revoke_failure_context(RevokeStatus::Failed).is_none(),
            "a rejected send unseated nothing and has no hash to cite"
        );
        assert!(
            revoke_failure_context(RevokeStatus::Reverted(B256::repeat_byte(0x55))).is_none(),
            "a confirmed revert unseated nothing"
        );
        assert!(
            revoke_failure_context(RevokeStatus::Revoked(B256::repeat_byte(0x55))).is_none(),
            "a confirmed removal needs no extra warning"
        );
        assert!(
            revoke_failure_context(RevokeStatus::DryRun).is_none(),
            "a dry run sent nothing"
        );
    }

    #[test]
    fn revoke_output_names_the_unseated_operator() {
        let o = RevokeOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            status: RevokeStatus::Revoked(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("namespace_id=7"), "{s}");
        assert!(s.contains("revoked_operator=0x1111"), "{s}");
        assert!(s.contains("status=revoked tx=0x5555"), "{s}");

        let dry = RevokeOutcome {
            operator: None,
            status: RevokeStatus::DryRun,
            ..o
        };
        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &dry, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=dry_run submitted=false"), "{s}");
    }

    // JSON writers are the machine-consumable contract — round-trip each so a
    // renamed key or wrong-shaped value fails loudly.
    #[test]
    fn namespace_json_round_trips() {
        let dry = NamespaceOutcome {
            operator: None,
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::DryRun,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &dry, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(false));
        assert_eq!(v["status"], serde_json::json!("dry_run"));
        assert!(v["tx"].is_null(), "{v}");
        assert!(v["operator"].is_null(), "{v}");
        assert!(v["namespace_id"].is_null(), "{v}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            status: NamespaceStatus::Created {
                tx: B256::repeat_byte(0x55),
                id: Some(9),
            },
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(v["status"], serde_json::json!("created"));
        assert_eq!(v["namespace_id"], serde_json::json!(9)); // number, not string
        assert_eq!(
            v["operator"],
            serde_json::json!(format!("{:#x}", done.operator.unwrap()))
        );
    }

    #[test]
    fn assign_json_round_trips() {
        let landed = Address::repeat_byte(0x11);
        let untried = Address::repeat_byte(0x22);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            seats: vec![
                (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
                (untried, SeatOutcome::NotAttempted),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("partial"));
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(v["operators"].as_array().unwrap().len(), 2);
        let origins = v["origins"].as_array().unwrap();
        // Every requested operator appears, in order, with its state — so a
        // consumer can never mistake "not tried" for "did not happen".
        assert_eq!(origins.len(), 2);
        assert_eq!(
            origins[0]["operator"],
            serde_json::json!(format!("{landed:#x}"))
        );
        assert_eq!(origins[0]["state"], serde_json::json!("seated"));
        assert_eq!(
            origins[0]["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );
        assert_eq!(origins[1]["state"], serde_json::json!("not_attempted"));
        assert!(origins[1]["tx"].is_null(), "{v}");
    }

    /// A `removeOrigin` that was broadcast without a readable receipt must not
    /// print the success label — the operator has to check the hash before
    /// retrying, because a retry after a landed removal reverts.
    #[test]
    fn single_tx_commands_report_an_unreadable_broadcast_as_unknown() {
        let revoke = RevokeOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            status: RevokeStatus::InFlight(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &revoke, false).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("status=unknown tx=0x5555"), "{text}");
        assert!(!text.contains("status=revoked"), "{text}");

        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &revoke, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("unknown"));
        // The hash still reaches the receipt — it is the operator's only handle.
        assert_eq!(
            v["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );
    }

    /// The label/hash derivation for every `NamespaceStatus` state, walked once
    /// so the two renderers can never disagree with each other about a state.
    /// `dry_run` and `failed` are distinct, hash-free states the enum keeps apart.
    #[test]
    fn namespace_status_labels_and_hashes_match_their_states() {
        let hash = B256::repeat_byte(0x55);
        assert_eq!(NamespaceStatus::DryRun.label(), "dry_run");
        assert_eq!(NamespaceStatus::DryRun.tx(), None);
        assert_eq!(NamespaceStatus::DryRun.namespace_id(), None);

        assert_eq!(NamespaceStatus::Failed.label(), "failed");
        assert_eq!(NamespaceStatus::Failed.tx(), None);
        assert_eq!(NamespaceStatus::Failed.namespace_id(), None);

        assert_eq!(NamespaceStatus::InFlight(hash).label(), "unknown");
        assert_eq!(NamespaceStatus::InFlight(hash).tx(), Some(hash));
        // A broadcast whose receipt was unreadable never carries a decoded id.
        assert_eq!(NamespaceStatus::InFlight(hash).namespace_id(), None);

        let created = NamespaceStatus::Created {
            tx: hash,
            id: Some(9),
        };
        assert_eq!(created.label(), "created");
        assert_eq!(created.tx(), Some(hash));
        assert_eq!(created.namespace_id(), Some(9));
        // Confirmed but the id could not be decoded: still `created`, still keeps
        // its hash, but has no id — `Some(id)` therefore implies `created`.
        let created_no_id = NamespaceStatus::Created { tx: hash, id: None };
        assert_eq!(created_no_id.label(), "created");
        assert_eq!(created_no_id.tx(), Some(hash));
        assert_eq!(created_no_id.namespace_id(), None);
    }

    /// The `RevokeStatus` parallel: a send that never made it on-chain is
    /// `failed`, not `dry_run` — the two were indistinguishable while the status
    /// was derived from `tx` alone.
    #[test]
    fn revoke_status_labels_and_hashes_match_their_states() {
        let hash = B256::repeat_byte(0x55);
        assert_eq!(RevokeStatus::DryRun.label(), "dry_run");
        assert_eq!(RevokeStatus::DryRun.tx(), None);

        assert_eq!(RevokeStatus::Failed.label(), "failed");
        assert_eq!(RevokeStatus::Failed.tx(), None);

        assert_eq!(RevokeStatus::InFlight(hash).label(), "unknown");
        assert_eq!(RevokeStatus::InFlight(hash).tx(), Some(hash));

        assert_eq!(RevokeStatus::Revoked(hash).label(), "revoked");
        assert_eq!(RevokeStatus::Revoked(hash).tx(), Some(hash));

        // A mined revert keeps its hash (#1550); a lost send response has none
        // and reports `submitted: null` rather than a definite `false` (#1577).
        assert_eq!(RevokeStatus::Reverted(hash).label(), "reverted");
        assert_eq!(RevokeStatus::Reverted(hash).tx(), Some(hash));
        assert_eq!(RevokeStatus::Reverted(hash).submitted(), Some(true));

        assert_eq!(RevokeStatus::MaybeBroadcast.label(), "maybe_broadcast");
        assert_eq!(RevokeStatus::MaybeBroadcast.tx(), None);
        assert_eq!(RevokeStatus::MaybeBroadcast.submitted(), None);
    }

    #[test]
    fn revoke_json_round_trip() {
        let revoke = RevokeOutcome {
            operator: None,
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            status: RevokeStatus::DryRun,
        };
        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &revoke, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("dry_run"));
        assert_eq!(v["submitted"], serde_json::json!(false));
        assert!(v["tx"].is_null(), "{v}");
        assert_eq!(
            v["revoked_operator"],
            serde_json::json!(format!("{:#x}", revoke.revoked))
        );
    }

    #[test]
    fn unique_operators_accepts_distinct_rejects_dupes() {
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        assert!(ensure_unique_operators(&[a, b]).is_ok());
        // Same address, however the user spelled it, parses to one `Address`.
        let err = ensure_unique_operators(&[a, b, a]).unwrap_err().to_string();
        assert!(err.contains("duplicate operator address"), "{err}");
        assert!(err.contains(&format!("{a:#x}")), "{err}");
    }

    #[test]
    fn chain_id_guard_matches_and_mismatches() {
        assert!(chain_id_guard(421_614, 421_614).is_ok());
        let err = chain_id_guard(421_614, 31_337).unwrap_err().to_string();
        assert!(err.contains("421614"), "{err}");
        assert!(err.contains("31337"), "{err}");
    }
}
