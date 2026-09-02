//! Startup self-check: is the local iroh key the one bound on-chain? (#1034)
//!
//! `SlashJudge._checkRegistered` resolves an accused node through
//! `CapacityBond.nodeIdOf`, so slashability follows the *binding*, not the key
//! the daemon happens to serve under. A node whose `node.secret` was replaced
//! without a matching `bindNodeId` therefore keeps serving and keeps getting
//! paid while its bond is unreachable — it is unslashable. Nothing else in the
//! runtime notices: the node is healthy, its channels settle, and its peers
//! simply stop routing to an id no longer in the staker set, which reads like
//! ordinary churn.
//!
//! That is the gap this check closes, and the reason it exists on the daemon at
//! all rather than only in `decdn node rotate-key`. The command is written so
//! the state is unreachable through it — the key file is committed only after
//! the bind confirms — but the state is reachable *around* it, by an operator
//! copying a key file, restoring the wrong backup, or running `decdn key-gen
//! --force` in a data dir that was already registered.
//!
//! # Advisory, deliberately
//!
//! The result never blocks startup. A daemon that refused to boot on a
//! mismatch would be brickable by a transient RPC failure, and — worse —
//! unable to run during the very window an operator needs it up to diagnose
//! and repair the binding. So the check reports: a `WARN` line at bring-up and
//! a field on `admin_v1_health`, both naming the repair.
//!
//! # Sampled once
//!
//! The binding changes only by an explicit operator transaction, so this runs
//! once during bring-up rather than per health poll — `admin_v1_health` is on
//! the readiness path, and putting a chain round trip behind it would make
//! every probe pay for a value that almost never moves.

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use decdn_common::admin::BindingStatus;
use decdn_incentive::capacity_bond::CapacityBond;

/// Outcome of the bring-up binding check, as carried into `AdminState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingReport {
    /// Whether the operator address is bound, unbound, or could not be read.
    pub status: BindingStatus,
    /// The node id the operator is bound to, when it could be read and is
    /// non-zero. `None` for [`BindingStatus::Unbound`] and
    /// [`BindingStatus::Unknown`].
    pub bound_node_id: Option<[u8; 32]>,
}

impl BindingReport {
    /// The report for a node with nothing to check against — no chain
    /// configuration, or a read that failed.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            status: BindingStatus::Unknown,
            bound_node_id: None,
        }
    }
}

/// Classify a binding read. Pure so every arm is reachable from a unit test —
/// the chain read that produces `bound` cannot be steered to these values from
/// a fixture without deploying a contract per case.
#[must_use]
pub fn classify(bound: B256, local: B256) -> BindingReport {
    if bound == B256::ZERO {
        return BindingReport {
            status: BindingStatus::Unbound,
            bound_node_id: None,
        };
    }
    let status = if bound == local {
        BindingStatus::Bound
    } else {
        BindingStatus::Mismatch
    };
    BindingReport {
        status,
        bound_node_id: Some(bound.into()),
    }
}

/// Read `CapacityBond.addressToNodeId(operator)` and compare it with the key
/// the daemon is about to serve under, logging the outcome.
///
/// Never fails: a read error is [`BindingStatus::Unknown`], which is reported
/// as its own state rather than folded into "fine" — an operator debugging an
/// unslashable node has to be able to tell "we checked and it is bound" from
/// "we could not check".
pub async fn check<P: Provider>(
    provider: P,
    capacity_bond: Address,
    operator: Address,
    local_node_id: [u8; 32],
) -> BindingReport {
    let local = B256::from(local_node_id);
    let bound = match CapacityBond::new(capacity_bond, provider)
        .addressToNodeId(operator)
        .call()
        .await
    {
        Ok(bound) => bound,
        Err(err) => {
            tracing::warn!(
                %err,
                %operator,
                %capacity_bond,
                "could not read the on-chain node-id binding at startup; slashability of this \
                 node's key is UNVERIFIED. Check it with `decdn node health`."
            );
            return BindingReport::unknown();
        }
    };

    let report = classify(bound, local);
    log(report.status, local, bound, operator);
    report
}

/// Emit the bring-up line for a classified binding.
///
/// Split from [`check`] so each arm's wording is one small function's worth of
/// branching rather than a fourth branch inside the read-and-classify flow.
fn log(status: BindingStatus, local: B256, bound: B256, operator: Address) {
    match status {
        BindingStatus::Bound => tracing::info!(
            node_id = %local,
            %operator,
            "local node key matches the on-chain binding"
        ),
        // Both remaining arms are the unslashable state, and both name the fix.
        // They are split because the repairs differ: a mismatch has a key to
        // choose between, an unbound operator has no registration at all.
        BindingStatus::Mismatch => tracing::warn!(
            local_node_id = %local,
            bound_node_id = %bound,
            %operator,
            "this node is serving under a key that is NOT the one bound on-chain, so it cannot be \
             slashed and peers will not route to it. Either bind the local key with `decdn node \
             rotate-key --key iroh --bind-existing`, or restore the key the binding names."
        ),
        BindingStatus::Unbound => tracing::warn!(
            local_node_id = %local,
            %operator,
            "this operator address has no on-chain node-id binding, so this node cannot be \
             slashed and peers will not route to it. Register it with `decdn node register` \
             (after `decdn node bond`)."
        ),
        // `check` never produces `Unknown` past its early return.
        BindingStatus::Unknown => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const LOCAL: B256 = B256::repeat_byte(0xAA);

    #[test]
    fn matching_binding_is_bound_and_echoes_the_id() {
        let r = classify(LOCAL, LOCAL);
        assert_eq!(r.status, BindingStatus::Bound);
        assert_eq!(r.bound_node_id, Some(LOCAL.into()));
    }

    /// The whole point of the check: a different bound id is the unslashable
    /// state, and the report has to carry the id to restore.
    #[test]
    fn different_binding_is_a_mismatch_that_names_the_bound_id() {
        let other = B256::repeat_byte(0xBB);
        let r = classify(other, LOCAL);
        assert_eq!(r.status, BindingStatus::Mismatch);
        assert_eq!(
            r.bound_node_id,
            Some(other.into()),
            "the operator needs the id they are bound to, not the one they are running"
        );
    }

    /// A zero binding is `Unbound`, never `Mismatch`: reporting it as a
    /// mismatch would send the operator hunting for a key to restore that was
    /// never bound.
    #[test]
    fn zero_binding_is_unbound_with_no_id() {
        let r = classify(B256::ZERO, LOCAL);
        assert_eq!(r.status, BindingStatus::Unbound);
        assert_eq!(r.bound_node_id, None);
    }

    /// `Unknown` must not be reachable from a successful read — it means "we
    /// could not check", and conflating it with a checked state is exactly the
    /// ambiguity the status enum exists to remove.
    #[test]
    fn classify_never_reports_unknown() {
        for bound in [B256::ZERO, LOCAL, B256::repeat_byte(0x01)] {
            assert_ne!(classify(bound, LOCAL).status, BindingStatus::Unknown);
        }
        assert_eq!(BindingReport::unknown().status, BindingStatus::Unknown);
    }
}
