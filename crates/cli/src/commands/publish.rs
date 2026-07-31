//! `decdn publish` — origin-publisher control plane (issues #1029 / #1491).
//!
//! Submits the `PublisherRegistry` / `OriginAssignment` writes that create
//! namespaces, ask governance to vet the publisher wallet, and then seat or
//! unseat that publisher's authorized origins. Vetting is the only step that
//! waits on governance; once it lands, `assign` and `revoke` take effect in the
//! transaction that carries them (ADR 011 § Origin Assignment Authority).
//! Content is bound to a namespace off-chain by the requester at fetch time
//! (ADR 002 § Hash-to-namespace association), so there is no per-hash on-chain
//! claim. Mirrors the
//! on-chain-write pattern of `super::register`: resolve chain coordinates and
//! parse the target contract address, then — on the submit path only — load
//! the keystore signer, build a wallet-filled provider, submit, and print a
//! JSON or `key=value` receipt. `--dry-run` stops after resolution: it loads
//! no keystore and sends no transaction, printing just the resolved parameters.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::publisher_registry::PublisherRegistry;

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
        cli::PublishCommand::RequestVetting(a) => request_vetting(a, global_config).await,
        cli::PublishCommand::Assign(a) => assign(a, global_config).await,
        cli::PublishCommand::Revoke(a) => revoke(a, global_config).await,
    }
}

/// Resolve the publisher coordinates and parse the `PublisherRegistry`
/// address. Shared by `namespace create` and `claim`; the signer and provider
/// are built on the submit path by [`signer_and_provider`].
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
/// wallet-filled provider — the byte-identical submit preamble shared by
/// `namespace create`, `claim`, and `assign`. Called only on the submit path
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

/// Result of `namespace create`. `operator`/`namespace_id`/`tx` are `None` on
/// a dry run (which loads no keystore, so the signer address is unknown).
pub(crate) struct NamespaceOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) registry: Address,
    pub(crate) namespace_id: Option<u64>,
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_namespace_outcome(
    w: &mut impl io::Write,
    o: &NamespaceOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "publisher_registry": format!("{:#x}", o.registry),
            "namespace_id": o.namespace_id,
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "publisher_registry={:#x}", o.registry)?;
    match (o.namespace_id, o.tx) {
        (Some(id), Some(h)) => writeln!(w, "submitted=true namespace_id={id} tx={h:#x}"),
        _ => writeln!(w, "submitted=false dry_run=true"),
    }
}

async fn namespace_create(
    args: &cli::NamespaceCreateArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let (resolved, registry) = registry_ctx(&args.chain, global_config)?;
    let mut outcome = NamespaceOutcome {
        operator: None,
        registry,
        namespace_id: None,
        tx: None,
    };

    if !args.chain.common.dry_run {
        // The keystore is decrypted only when actually submitting — a dry run
        // needs no secrets.
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = PublisherRegistry::new(registry, &provider);
        let receipt = decdn_incentive::tx::send_for_receipt(
            contract.createNamespace(),
            "createNamespace",
            Some("check the RPC, gas, and the registry address"),
            &mut outcome.tx,
        )
        .await?;
        let tx = receipt.transaction_hash;
        // The new id comes from the emitted event: the return value is not in a
        // receipt, and a static pre-call would race a concurrent create
        // (`_nextNamespaceId` is global across publishers). Match the log by
        // event signature first, then decode — so an ABI drift surfaces as a
        // decode error rather than a misleading "missing event".
        let created = receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&PublisherRegistry::NamespaceCreated::SIGNATURE_HASH))
            .context(
                "createNamespace succeeded on-chain but its receipt carried no NamespaceCreated \
                 log (the namespace id could not be recovered; check the tx on a block explorer)",
            )?
            .log_decode::<PublisherRegistry::NamespaceCreated>()
            .context(
                "createNamespace emitted a NamespaceCreated log that failed to decode (ABI \
                 mismatch between this CLI and the deployed PublisherRegistry?)",
            )?;
        let id_u64 = u64::try_from(created.inner.data.namespaceId)
            .context("namespace id exceeds u64 (unexpected on this chain)")?;
        outcome.namespace_id = Some(id_u64);
        outcome.tx = Some(tx);
    }

    let mut out = io::stdout().lock();
    write_namespace_outcome(&mut out, &outcome, args.chain.common.json)
        .context("failed to write namespace-create output")?;
    Ok(())
}

// -------------------------------------------------------------------------
// request-vetting
// -------------------------------------------------------------------------

/// Result of `request-vetting`. `operator`/`tx`/`ready_at` are `None` on a dry
/// run (which loads no keystore, so the signer address is unknown).
pub(crate) struct VettingOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    /// Unix time the vetting timelock elapses (earliest governance grant), from
    /// the `VettingRequested` event. `None` on a dry run, and `None` when the
    /// request landed but its deadline could not be recovered.
    pub(crate) ready_at: Option<u64>,
    pub(crate) tx: Option<B256>,
    /// The transaction was broadcast but its outcome could not be read, so the
    /// request is neither confirmed queued nor known to have failed.
    pub(crate) in_flight: bool,
    pub(crate) dry_run: bool,
}

pub(crate) fn write_vetting_outcome(
    w: &mut impl io::Write,
    o: &VettingOutcome,
    json: bool,
) -> io::Result<()> {
    let status = single_tx_status(o.dry_run, o.in_flight, o.tx, "vetting_requested");
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "ready_at": o.ready_at,
            "status": status,
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "origin_assignment={:#x}", o.origin_assignment)?;
    if let Some(ready_at) = o.ready_at {
        writeln!(w, "ready_at={ready_at}")?;
    }
    // Requested only: the grant is a separate governance action, and until it
    // lands `assign` still reverts `PublisherNotVetted`.
    match o.tx {
        Some(h) => writeln!(w, "status={status} tx={h:#x}"),
        None => writeln!(w, "status={status} submitted=false"),
    }
}

async fn request_vetting(
    args: &cli::RequestVettingArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let (resolved, oa_addr) = assignment_ctx(&args.chain, global_config)?;
    let mut outcome = VettingOutcome {
        operator: None,
        origin_assignment: oa_addr,
        ready_at: None,
        tx: None,
        in_flight: false,
        dry_run: args.chain.common.dry_run,
    };
    // `Ok` on the dry-run path: nothing was sent, so there is nothing to decode
    // and nothing to report as failed.
    let mut deadline: anyhow::Result<u64> = Ok(0);

    if !outcome.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        let sent = decdn_incentive::tx::send_for_receipt(
            contract.requestVetting(),
            "requestVetting",
            Some(
                "the signer must own at least one namespace (run `decdn publish namespace \
                 create` first), must not already be vetted, and must not already have a \
                 request pending",
            ),
            &mut outcome.tx,
        )
        .await;

        match sent {
            // Surface the timelock deadline (`readyAt`), matching the honest
            // decode in `namespace_create`: match by signature, then decode.
            // Every failure here happens AFTER the request is queued on-chain,
            // so it must not short-circuit the receipt — the tx hash is the only
            // handle the publisher has on a request that now blocks re-running
            // this command (`VettingRequestPending`).
            Ok(receipt) => {
                deadline = decode_vetting_deadline(&receipt);
                if let Ok(ready_at) = deadline {
                    outcome.ready_at = Some(ready_at);
                }
            }
            Err(err) => {
                // A hash that survived the error means the transaction was
                // broadcast and only its receipt was unreadable: the request may
                // well be queued, so report it rather than implying nothing
                // happened.
                outcome.in_flight = outcome.tx.is_some();
                deadline = Err(err);
            }
        }
    }

    let mut out = io::stdout().lock();
    let write_err = write_vetting_outcome(&mut out, &outcome, args.chain.common.json).err();
    drop(out);
    let in_flight = outcome.in_flight;
    propagate(
        deadline.err().map(|err| {
            err.context(if in_flight {
                "the transaction was broadcast — check the tx above before re-running, since a \
                 queued request makes `request-vetting` revert VettingRequestPending"
            } else {
                "the request is queued on-chain — the tx above is its only handle"
            })
        }),
        write_err,
        "failed to write request-vetting output",
    )
}

/// Pull `readyAt` out of a confirmed `requestVetting` receipt. Split out so the
/// caller can print its receipt before propagating any of these failures, all of
/// which happen after the transaction has already taken effect.
fn decode_vetting_deadline(receipt: &alloy::rpc::types::TransactionReceipt) -> anyhow::Result<u64> {
    let requested = receipt
        .inner
        .logs()
        .iter()
        .find(|log| log.topic0() == Some(&OriginAssignment::VettingRequested::SIGNATURE_HASH))
        .context(
            "requestVetting succeeded but its receipt carried no VettingRequested log (the \
             timelock deadline could not be recovered; check the tx on a block explorer)",
        )?
        .log_decode::<OriginAssignment::VettingRequested>()
        .context(
            "requestVetting emitted a VettingRequested log that failed to decode (ABI \
             mismatch between this CLI and the deployed OriginAssignment?)",
        )?;
    u64::try_from(requested.inner.data.readyAt)
        .context("vetting readyAt exceeds u64 (unexpected on this chain)")
}

/// The `status=` token for a command that submits exactly ONE transaction.
///
/// `in_flight` is the case that needs its own answer: the transaction was
/// broadcast and its outcome could not be read, so neither the success label nor
/// "failed" is honest. `send_for_receipt` clears the hash on a confirmed revert
/// and leaves it set when only the receipt fetch failed, which is what makes the
/// two distinguishable here.
const fn single_tx_status(
    dry_run: bool,
    in_flight: bool,
    tx: Option<B256>,
    confirmed: &'static str,
) -> &'static str {
    if dry_run {
        "dry_run"
    } else if in_flight {
        "unknown"
    } else if tx.is_some() {
        confirmed
    } else {
        "failed"
    }
}

/// Return the on-chain failure if there was one, otherwise the stdout-write
/// failure, otherwise success.
///
/// The order is the point: a broken pipe must never outrank — and so hide — a
/// chain error the operator has to act on. Printing the receipt first is what
/// makes a partial or ambiguous on-chain effect recoverable, and that guarantee
/// is worthless if the write's own `?` returns before the real error is built.
fn propagate(
    chain_err: Option<anyhow::Error>,
    write_err: Option<io::Error>,
    write_context: &'static str,
) -> anyhow::Result<()> {
    match (chain_err, write_err) {
        (Some(err), None) => Err(err),
        (Some(err), Some(w)) => {
            Err(err.context(format!("(the receipt could not be written to stdout: {w})")))
        }
        (None, Some(w)) => Err(anyhow::Error::from(w).context(write_context)),
        (None, None) => Ok(()),
    }
}

// -------------------------------------------------------------------------
// assign (instant, one addOrigin per operator)
// -------------------------------------------------------------------------

/// The failure hint shared by `addOrigin` and `removeOrigin`: every guard the
/// contract applies, led by the one a publisher hits first.
const ADD_ORIGIN_HINT: &str = "the signer must be a vetted publisher (run `decdn publish \
     request-vetting`, then wait for governance to grant it) and own the namespace; the \
     operator must be an active bonded node, not blacklisted, not already seated, and must \
     fit under maxOriginsPerNamespace";

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
    /// the hash before awaiting the receipt precisely so this case can be
    /// reported; the hash is `None` only in the shape it documents as
    /// impossible.
    InFlight(Option<B256>),
    /// Attempted and definitively did not take effect — the send was rejected,
    /// or the transaction confirmed as a revert.
    Reverted,
    /// The loop stopped before this operator was tried.
    NotAttempted,
}

impl SeatOutcome {
    /// The transaction hash, for any state that has one.
    const fn tx(self) -> Option<B256> {
        match self {
            Self::Seated(hash) => Some(hash),
            Self::InFlight(hash) => hash,
            Self::Reverted | Self::NotAttempted => None,
        }
    }

    /// The `state=` token in the receipt.
    const fn label(self) -> &'static str {
        match self {
            Self::Seated(_) => "seated",
            Self::InFlight(_) => "in_flight",
            Self::Reverted => "reverted",
            Self::NotAttempted => "not_attempted",
        }
    }
}

/// Receipt for `publish assign`.
///
/// `seats` carries every requested operator in the order given, each paired with
/// what happened to it — one entry per operator, always, so the receipt cannot
/// misattribute a transaction or silently omit an operator. `dry_run` is a field
/// rather than inferred from the absence of transactions (the convention its
/// sibling outcome types use) because "nothing was sent" and "the first send
/// reverted" are different answers that both leave zero seats.
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
        if self
            .seats
            .iter()
            .any(|(_, s)| matches!(s, SeatOutcome::InFlight(_)))
        {
            return "unknown";
        }
        match self.seated_count() {
            n if n == self.seats.len() => "seated",
            0 => "failed",
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

    // Seating is a per-operator delta, so a set of N operators is N
    // transactions. They run in the order given and stop at the first failure:
    // the remaining operators are usually doomed for the same reason (an
    // unvetted signer, a namespace the signer does not own), and continuing
    // would burn gas to collect the identical error N times. The untried
    // operators are recorded as such rather than dropped.
    let mut failure = None;
    if outcome.dry_run {
        outcome
            .seats
            .extend(operators.iter().map(|a| (*a, SeatOutcome::NotAttempted)));
    } else {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        let mut stopped_at = operators.len();
        for (i, operator) in operators.iter().enumerate() {
            let mut tx = None;
            let sent = decdn_incentive::tx::send_for_receipt(
                contract.addOrigin(U256::from(args.namespace), *operator),
                "addOrigin",
                Some(ADD_ORIGIN_HINT),
                &mut tx,
            )
            .await;
            match (sent, tx) {
                (Ok(_), Some(hash)) => outcome.seats.push((*operator, SeatOutcome::Seated(hash))),
                // `send_for_receipt` records the hash before awaiting the
                // receipt, so a success with no hash should be unreachable.
                // Report it as in-flight rather than as a seat we cannot cite.
                (Ok(_), None) => {
                    outcome.seats.push((*operator, SeatOutcome::InFlight(None)));
                    failure = Some((
                        *operator,
                        anyhow::anyhow!("addOrigin succeeded but recorded no transaction hash"),
                    ));
                    stopped_at = i + 1;
                    break;
                }
                // A hash with an `Err` means the transaction was broadcast and
                // its outcome could not be read — NOT that it failed. Saying
                // "reverted" here is how an operator gets told to re-send a
                // transaction that is still pending.
                (Err(err), Some(hash)) => {
                    outcome
                        .seats
                        .push((*operator, SeatOutcome::InFlight(Some(hash))));
                    failure = Some((*operator, err));
                    stopped_at = i + 1;
                    break;
                }
                (Err(err), None) => {
                    outcome.seats.push((*operator, SeatOutcome::Reverted));
                    failure = Some((*operator, err));
                    stopped_at = i + 1;
                    break;
                }
            }
        }
        outcome.seats.extend(
            operators
                .get(stopped_at..)
                .unwrap_or_default()
                .iter()
                .map(|a| (*a, SeatOutcome::NotAttempted)),
        );
    }

    // Print BEFORE propagating: the seats that already landed are on-chain, and
    // an operator who only sees the error has no way to tell which.
    let mut out = io::stdout().lock();
    let write_err = write_assign_outcome(&mut out, &outcome, args.chain.common.json).err();
    drop(out);

    let chain_err = failure.map(|(operator, err)| {
        err.context(format!(
            "addOrigin failed for operator {operator:#x} after seating {} of {} (the seated \
             operators are live; re-run with the operators still marked not_attempted, and \
             check any marked in_flight on a block explorer before re-sending them)",
            outcome.seated_count(),
            outcome.seats.len(),
        ))
    });
    propagate(chain_err, write_err, "failed to write assign output")
}

// -------------------------------------------------------------------------
// revoke
// -------------------------------------------------------------------------

/// Receipt for `publish revoke`. One transaction, so the shape is simpler than
/// [`AssignOutcome`] — but it needs the same honesty about a broadcast whose
/// outcome could not be read.
pub(crate) struct RevokeOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    /// The operator being unseated.
    pub(crate) revoked: Address,
    pub(crate) tx: Option<B256>,
    /// The transaction was broadcast but its outcome could not be read, so the
    /// operator may or may not still be seated.
    pub(crate) in_flight: bool,
    pub(crate) dry_run: bool,
}

pub(crate) fn write_revoke_outcome(
    w: &mut impl io::Write,
    o: &RevokeOutcome,
    json: bool,
) -> io::Result<()> {
    let status = single_tx_status(o.dry_run, o.in_flight, o.tx, "revoked");
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "revoked_operator": format!("{:#x}", o.revoked),
            "status": status,
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "origin_assignment={:#x}", o.origin_assignment)?;
    writeln!(w, "namespace_id={}", o.namespace_id)?;
    writeln!(w, "revoked_operator={:#x}", o.revoked)?;
    match o.tx {
        Some(h) => writeln!(w, "status={status} tx={h:#x}"),
        None => writeln!(w, "status={status} submitted=false"),
    }
}

async fn revoke(args: &cli::RevokeArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let (resolved, oa_addr) = assignment_ctx(&args.chain, global_config)?;
    let revoked = chain_ctx::parse_address(&args.operator, "operator")?;

    let mut outcome = RevokeOutcome {
        operator: None,
        origin_assignment: oa_addr,
        namespace_id: args.namespace,
        revoked,
        tx: None,
        in_flight: false,
        dry_run: args.chain.common.dry_run,
    };

    // Print before propagating, for the same reason `assign` does: a broadcast
    // whose receipt could not be read leaves the hash set, and a `--json`
    // consumer that only sees the error loses its one handle on the removal.
    let mut chain_err = None;
    if !outcome.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        if let Err(err) = decdn_incentive::tx::send_for_receipt(
            contract.removeOrigin(U256::from(args.namespace), revoked),
            "removeOrigin",
            Some(
                "the signer must own the namespace (or hold GOVERNANCE_ROLE) and the operator \
                 must currently be seated as an authorized origin for it",
            ),
            &mut outcome.tx,
        )
        .await
        {
            outcome.in_flight = outcome.tx.is_some();
            chain_err = Some(if outcome.in_flight {
                err.context(
                    "the transaction was broadcast — check the tx above before re-running, \
                     since a removal that landed makes the retry revert NotAuthorizedOrigin",
                )
            } else {
                err
            });
        }
    }

    let mut out = io::stdout().lock();
    let write_err = write_revoke_outcome(&mut out, &outcome, args.chain.common.json).err();
    drop(out);
    propagate(chain_err, write_err, "failed to write revoke output")
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
            namespace_id: None,
            tx: None,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &base, false).unwrap();
        let dry = String::from_utf8(buf).unwrap();
        assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
        assert!(!dry.contains("operator="), "{dry}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            namespace_id: Some(9),
            tx: Some(B256::repeat_byte(0x55)),
            ..base
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operator=0xcdcd"), "{s}");
        assert!(s.contains("namespace_id=9"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
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
                (failed, SeatOutcome::Reverted),
                (untried, SeatOutcome::NotAttempted),
            ],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains(&format!("  operator={landed:#x} tx=")), "{s}");
        assert!(
            s.contains(&format!("  operator={failed:#x} state=reverted")),
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
                (Address::repeat_byte(0x11), SeatOutcome::Reverted),
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
                (
                    unknown,
                    SeatOutcome::InFlight(Some(B256::repeat_byte(0x66))),
                ),
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
        )
        .unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("addOrigin reverted"), "{rendered}");
        assert!(rendered.contains("could not be written"), "{rendered}");

        // Write failure alone still surfaces, with its own context.
        let err = propagate(None, Some(broken()), "failed to write assign output").unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to write assign output"),
            "{err:#}"
        );

        assert!(propagate(None, None, "w").is_ok());
    }

    #[test]
    fn vetting_output_states_requested_then_dry_run() {
        let done = VettingOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            ready_at: Some(1_700_000_000),
            tx: Some(B256::repeat_byte(0x55)),
            in_flight: false,
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_vetting_outcome(&mut buf, &done, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("ready_at=1700000000"), "{s}");
        assert!(s.contains("status=vetting_requested tx=0x5555"), "{s}");

        let dry = VettingOutcome {
            operator: None,
            ready_at: None,
            tx: None,
            dry_run: true,
            ..done
        };
        let mut buf = Vec::new();
        write_vetting_outcome(&mut buf, &dry, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=dry_run submitted=false"), "{s}");
        assert!(!s.contains("ready_at="), "{s}");
        assert!(!s.contains("operator="), "{s}");
    }

    #[test]
    fn revoke_output_names_the_unseated_operator() {
        let o = RevokeOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            tx: Some(B256::repeat_byte(0x55)),
            in_flight: false,
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_revoke_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("namespace_id=7"), "{s}");
        assert!(s.contains("revoked_operator=0x1111"), "{s}");
        assert!(s.contains("status=revoked tx=0x5555"), "{s}");

        let dry = RevokeOutcome {
            operator: None,
            tx: None,
            dry_run: true,
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
            namespace_id: None,
            tx: None,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &dry, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(false));
        assert!(v["tx"].is_null(), "{v}");
        assert!(v["operator"].is_null(), "{v}");
        assert!(v["namespace_id"].is_null(), "{v}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            namespace_id: Some(9),
            tx: Some(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(true));
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

    /// A `removeOrigin` / `requestVetting` that was broadcast without a readable
    /// receipt must not print the success label — the operator has to check the
    /// hash before retrying, because a retry after a landed removal reverts.
    #[test]
    fn single_tx_commands_report_an_unreadable_broadcast_as_unknown() {
        let revoke = RevokeOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            tx: Some(B256::repeat_byte(0x55)),
            in_flight: true,
            dry_run: false,
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

        let vetting = VettingOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            ready_at: None,
            tx: Some(B256::repeat_byte(0x66)),
            in_flight: true,
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_vetting_outcome(&mut buf, &vetting, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("unknown"));
        assert!(v["ready_at"].is_null(), "{v}");
    }

    /// A send that never made it on-chain is `failed`, not `dry_run` — the two
    /// were indistinguishable while the status was derived from `tx` alone.
    #[test]
    fn single_tx_status_separates_failed_from_dry_run() {
        assert_eq!(single_tx_status(true, false, None, "revoked"), "dry_run");
        assert_eq!(single_tx_status(false, false, None, "revoked"), "failed");
        assert_eq!(
            single_tx_status(false, false, Some(B256::repeat_byte(1)), "revoked"),
            "revoked"
        );
        assert_eq!(
            single_tx_status(false, true, Some(B256::repeat_byte(1)), "revoked"),
            "unknown"
        );
    }

    #[test]
    fn vetting_and_revoke_json_round_trip() {
        let vetting = VettingOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            ready_at: Some(1_700_000_000),
            tx: Some(B256::repeat_byte(0x55)),
            in_flight: false,
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_vetting_outcome(&mut buf, &vetting, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("vetting_requested"));
        assert_eq!(v["ready_at"], serde_json::json!(1_700_000_000)); // number, not string

        let revoke = RevokeOutcome {
            operator: None,
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            revoked: Address::repeat_byte(0x11),
            tx: None,
            in_flight: false,
            dry_run: true,
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
