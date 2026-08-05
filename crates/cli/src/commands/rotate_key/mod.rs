//! `decdn node rotate-key` — the operator key-rotation runbook
//! (`adr/appendix-operator-key-rotation.md`) as a command (#1034).
//!
//! The runbook's decision tree branches on *which* key is being rotated, and
//! the branches share almost nothing: one is a single transaction that changes
//! no economic state, the other moves the entire on-chain identity across a
//! 14-day window. So `--key` selects a submodule rather than a code path
//! inside one function:
//!
//! - `iroh` — rebind the wire `NodeId` through `CapacityBond.bindNodeId`.
//! - `eth` — migrate the Ethereum address: deregister → unbond → re-onboard.
//!
//! What they do share is the failure this command exists to prevent. A node
//! whose local key is not the on-chain-bound one is **un-slashable**
//! (`SlashJudge._checkRegistered` reads `nodeIdOf`, so an unbound key has no
//! operator to charge), which is a protocol fault rather than an outage: the
//! node keeps serving and keeps getting paid while its bond is unreachable. No
//! ordering of steps here may produce that state even transiently, which is
//! what fixes the sequencing in both submodules — the iroh path commits
//! `node.secret` only after the bind confirms, and the eth path reads chain
//! state to pick its phase instead of trusting a flag about what already ran.
//!
//! Neither path restarts the daemon. The iroh key is not hot-reloadable
//! (`crates/node/src/runtime/reload.rs` covers `payment.rate_per_mb`,
//! `observability.log_level`, `cache.pinned_hashes`, and `security.*` — nothing
//! that rebuilds the iroh endpoint), and a command that must keep working with
//! the daemon *down* cannot depend on its admin RPC. Drain, stop, rotate,
//! restart stays the operator's sequence; the command names the steps it did
//! not take.

pub(crate) mod eth;
pub(crate) mod iroh;

use std::io::{self, IsTerminal, Write};
use std::path::Path;

use decdn_common::cli;

/// Dispatch a `decdn node rotate-key` invocation to the path `--key` names.
pub async fn run(args: &cli::RotateKeyArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    match args.key {
        cli::RotateKeyTarget::Iroh => iroh::run(args, global_config).await,
        cli::RotateKeyTarget::Eth => eth::run(args, global_config).await,
    }
}

/// What the confirmation gate decides, before any IO. Split from
/// [`confirm_or_bail`] the way `deregister::decide_confirmation` is, so the
/// gate is testable without a TTY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Confirmation {
    /// `--yes` supplied: disclose the consequences, but skip the prompt.
    Bypassed,
    /// Ask on the terminal.
    Prompt,
    /// Headless and unconfirmed: refuse rather than assume consent.
    NeedFlag,
}

/// Decide whether to prompt. Same rule as `decdn node deregister`: every run
/// that reaches the gate is about to submit, so there is no "not needed" arm.
pub(crate) const fn decide_confirmation(yes: bool, interactive: bool) -> Confirmation {
    if yes {
        Confirmation::Bypassed
    } else if interactive {
        Confirmation::Prompt
    } else {
        Confirmation::NeedFlag
    }
}

/// Disclose the consequences and, on a terminal, wait for `y`.
///
/// `--yes` suppresses the *prompt*, never the disclosure — "don't ask" must not
/// silently become "don't tell", the rule `unbond` and `deregister` both
/// follow. `disclosure` writes the path-specific wording; `subject` names the
/// operation in the two refusal messages ("rotation cancelled" and so on).
///
/// # Errors
///
/// Fails if the disclosure or prompt cannot be written, if stdin cannot be
/// read, if the run is headless without `--yes`, or if the operator declines.
pub(crate) fn confirm_or_bail<F>(disclosure: F, subject: &str, yes: bool) -> anyhow::Result<()>
where
    F: FnOnce(&mut dyn io::Write) -> io::Result<()>,
{
    // Interactive only when BOTH streams are terminals: the warning goes to
    // stderr and the answer is read from stdin, so if either is redirected the
    // operator cannot see what they would be agreeing to.
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    let decision = decide_confirmation(yes, interactive);

    let mut err = io::stderr().lock();
    disclosure(&mut err)?;

    match decision {
        // Disclosed above; the operator asked not to be asked.
        Confirmation::Bypassed => Ok(()),
        Confirmation::NeedFlag => anyhow::bail!(
            "{subject} not confirmed: no interactive terminal detected — re-run with `--yes` \
             (or `--dry-run` to preview)."
        ),
        Confirmation::Prompt => {
            write!(err, "Proceed? [y/N] ")?;
            err.flush()?;
            let mut line = String::new();
            io::stdin().read_line(&mut line)?;
            // Allowlist, not denylist: an empty line (Ctrl-D / EOF) lands on
            // the deny side rather than being read as assent.
            let answer = line.trim().to_ascii_lowercase();
            anyhow::ensure!(answer == "y" || answer == "yes", "{subject} cancelled.");
            Ok(())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn headless_without_yes_refuses() {
        assert_eq!(decide_confirmation(false, false), Confirmation::NeedFlag);
        assert_eq!(decide_confirmation(false, true), Confirmation::Prompt);
        // `--yes` bypasses the prompt on a terminal AND headless — the flag is
        // what makes a scripted rotation legal.
        assert_eq!(decide_confirmation(true, false), Confirmation::Bypassed);
        assert_eq!(decide_confirmation(true, true), Confirmation::Bypassed);
    }
}
