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
    /// the `VettingRequested` event. `None` on a dry run.
    pub(crate) ready_at: Option<u64>,
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_vetting_outcome(
    w: &mut impl io::Write,
    o: &VettingOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "ready_at": o.ready_at,
            "status": if o.tx.is_some() { "vetting_requested" } else { "dry_run" },
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
    match o.tx {
        // Requested only: the grant is a separate governance action, and until
        // it lands `assign` still reverts `PublisherNotVetted`.
        Some(h) => writeln!(w, "status=vetting_requested tx={h:#x}"),
        None => writeln!(w, "status=dry_run submitted=false"),
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
    };

    if !args.chain.common.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        let receipt = decdn_incentive::tx::send_for_receipt(
            contract.requestVetting(),
            "requestVetting",
            Some(
                "the signer must own at least one namespace (run `decdn publish namespace \
                 create` first), must not already be vetted, and must not already have a \
                 request pending",
            ),
            &mut outcome.tx,
        )
        .await?;

        // Surface the timelock deadline (`readyAt`), matching the honest decode
        // in `namespace_create`: match by signature, then decode.
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
        outcome.ready_at = Some(
            u64::try_from(requested.inner.data.readyAt)
                .context("vetting readyAt exceeds u64 (unexpected on this chain)")?,
        );
    }

    let mut out = io::stdout().lock();
    write_vetting_outcome(&mut out, &outcome, args.chain.common.json)
        .context("failed to write request-vetting output")?;
    Ok(())
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

pub(crate) struct AssignOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    /// Every operator the invocation asked to seat, in the order given.
    pub(crate) operators: Vec<Address>,
    /// The operators actually seated, each with the transaction that seated it.
    /// Seating is one transaction per operator, so a mid-run revert leaves this
    /// a prefix of `operators` — which is exactly what the receipt must show.
    pub(crate) seated: Vec<(Address, B256)>,
    pub(crate) dry_run: bool,
}

impl AssignOutcome {
    /// `dry_run` before anything is sent, `seated` once every requested operator
    /// landed, `partial` when a mid-run revert stopped the loop early.
    const fn status(&self) -> &'static str {
        if self.dry_run {
            "dry_run"
        } else if self.seated.len() == self.operators.len() {
            "seated"
        } else {
            "partial"
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
            "submitted": !o.seated.is_empty(),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "operators": o.operators.iter().map(|a| format!("{a:#x}")).collect::<Vec<_>>(),
            "origins": o.seated.iter().map(|(a, tx)| serde_json::json!({
                "operator": format!("{a:#x}"),
                "tx": format!("{tx:#x}"),
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
    writeln!(w, "operators={}", o.operators.len())?;
    for a in &o.operators {
        match o.seated.iter().find(|(seated, _)| seated == a) {
            Some((_, tx)) => writeln!(w, "  operator={a:#x} tx={tx:#x}")?,
            None => writeln!(w, "  operator={a:#x} unseated")?,
        }
    }
    if o.dry_run {
        writeln!(w, "status=dry_run submitted=false")
    } else {
        writeln!(w, "status={} seated={}", o.status(), o.seated.len())
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
        // Moved in (not cloned): the dry-run path never submits, so it does no
        // extra allocation.
        operators,
        seated: Vec::new(),
        dry_run: args.chain.common.dry_run,
    };

    // Seating is a per-operator delta, so a set of N operators is N
    // transactions. They run in the order given and stop at the first revert:
    // the remaining operators are almost certainly doomed for the same reason
    // (an unvetted signer, a namespace the signer does not own), and continuing
    // would burn gas to collect the identical error N times.
    let mut failure = None;
    if !outcome.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        for operator in &outcome.operators {
            let mut tx = None;
            let sent = decdn_incentive::tx::send_for_receipt(
                contract.addOrigin(U256::from(args.namespace), *operator),
                "addOrigin",
                Some(ADD_ORIGIN_HINT),
                &mut tx,
            )
            .await;
            match (sent, tx) {
                (Ok(_), Some(hash)) => outcome.seated.push((*operator, hash)),
                (Ok(_), None) => {
                    // `send_for_receipt` records the hash before awaiting the
                    // receipt, so a success with no hash is impossible; treat it
                    // as a failure rather than reporting a seat we cannot cite.
                    failure = Some((
                        *operator,
                        anyhow::anyhow!("addOrigin succeeded but recorded no transaction hash"),
                    ));
                    break;
                }
                (Err(err), _) => {
                    failure = Some((*operator, err));
                    break;
                }
            }
        }
    }

    // Print BEFORE propagating: the seats that already landed are on-chain, and
    // an operator who only sees the error has no way to tell which.
    let mut out = io::stdout().lock();
    write_assign_outcome(&mut out, &outcome, args.chain.common.json)
        .context("failed to write assign output")?;
    drop(out);

    if let Some((operator, err)) = failure {
        return Err(err.context(format!(
            "addOrigin failed for operator {operator:#x} after seating {} of {} \
             (the seated operators are live; re-run with the remaining ones)",
            outcome.seated.len(),
            outcome.operators.len(),
        )));
    }
    Ok(())
}

// -------------------------------------------------------------------------
// revoke
// -------------------------------------------------------------------------

pub(crate) struct RevokeOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    /// The operator being unseated.
    pub(crate) revoked: Address,
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_revoke_outcome(
    w: &mut impl io::Write,
    o: &RevokeOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "revoked_operator": format!("{:#x}", o.revoked),
            "status": if o.tx.is_some() { "revoked" } else { "dry_run" },
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
        Some(h) => writeln!(w, "status=revoked tx={h:#x}"),
        None => writeln!(w, "status=dry_run submitted=false"),
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
    };

    if !args.chain.common.dry_run {
        let (signer, provider) = signer_and_provider(&resolved, &args.chain).await?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        decdn_incentive::tx::send_for_receipt(
            contract.removeOrigin(U256::from(args.namespace), revoked),
            "removeOrigin",
            Some(
                "the signer must own the namespace (or hold GOVERNANCE_ROLE) and the operator \
                 must currently be seated as an authorized origin for it",
            ),
            &mut outcome.tx,
        )
        .await?;
    }

    let mut out = io::stdout().lock();
    write_revoke_outcome(&mut out, &outcome, args.chain.common.json)
        .context("failed to write revoke output")?;
    Ok(())
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
            operators: vec![a, b],
            seated: vec![(a, B256::repeat_byte(0x55)), (b, B256::repeat_byte(0x66))],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operators=2"), "{s}");
        assert!(s.contains("  operator=0x1111"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
        assert!(s.contains("tx=0x6666"), "{s}");
        assert!(s.contains("status=seated seated=2"), "{s}");
    }

    /// Seating is one transaction per operator, so a mid-run revert leaves some
    /// operators live and the rest not. The receipt must say which — that is the
    /// whole reason it is printed before the error propagates.
    #[test]
    fn assign_output_distinguishes_partial_from_seated() {
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![a, b],
            seated: vec![(a, B256::repeat_byte(0x55))],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains(&format!("  operator={a:#x} tx=")), "{s}");
        assert!(s.contains(&format!("  operator={b:#x} unseated")), "{s}");
        assert!(s.contains("status=partial seated=1"), "{s}");
    }

    #[test]
    fn assign_dry_run_omits_signer_and_txs() {
        let o = AssignOutcome {
            operator: None,
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![Address::repeat_byte(0x11)],
            seated: Vec::new(),
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

    #[test]
    fn vetting_output_states_requested_then_dry_run() {
        let done = VettingOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            ready_at: Some(1_700_000_000),
            tx: Some(B256::repeat_byte(0x55)),
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
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![a, b],
            seated: vec![(a, B256::repeat_byte(0x55))],
            dry_run: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("partial"));
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(v["operators"].as_array().unwrap().len(), 2);
        let origins = v["origins"].as_array().unwrap();
        assert_eq!(origins.len(), 1);
        assert_eq!(origins[0]["operator"], serde_json::json!(format!("{a:#x}")));
        assert_eq!(
            origins[0]["tx"],
            serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
        );
    }

    #[test]
    fn vetting_and_revoke_json_round_trip() {
        let vetting = VettingOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            ready_at: Some(1_700_000_000),
            tx: Some(B256::repeat_byte(0x55)),
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
