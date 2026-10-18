//! Graceful Ctrl-C for `decdn get`, `decdn pool open` and `decdn pool top-up`.
//!
//! The first Ctrl-C resolves `Interrupt::wait`: the command stops its
//! transfers, writes what it must keep (the bundle skip-cache, the progress
//! indicators' cleanup), and returns [`Interrupted`], which exits with status
//! 130. A second Ctrl-C, or one that arrives after the command stops watching,
//! exits at once with the same status.
//!
//! Stopping mid-transfer loses nothing already paid for: every fetch resumes
//! from its on-disk `.partial` on the next run. Stopping an `openPool` or a
//! `topUp` before its record commits does not cancel the transaction, so the
//! command names it (`name_held_escrows`, and the immediate exit)
//! instead of telling the user to run it again: a re-run before it confirms
//! escrows a second time. An escrow that mined with no local record crediting
//! it is named the same way: a re-run reads the uncredited row and escrows
//! again.

use decdn_client::GaveUp;
use decdn_client::buyer_pool::{
    EscrowUnconfirmed, OpenUnconfirmed, TopUpUnconfirmed, UnconfirmedEscrows, UntrackedEscrow,
};

/// The error a command returns when Ctrl-C stopped it. `main` prints
/// [`stop_message`] and exits with status 130 (128 + SIGINT), the shell
/// convention for an interrupted command.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted")
    }
}

impl std::error::Error for Interrupted {}

/// The `openPool` and `topUp` transactions a command ended with that someone
/// must reconcile against the chain: those it did not see confirm, which may
/// still escrow USDC, and those that escrowed with no local record crediting
/// them. Displays one per line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreconciledEscrows {
    /// The transactions no local record credits yet
    /// ([`UnconfirmedEscrows::pending`]).
    pending: Vec<EscrowUnconfirmed>,
    /// The transactions that escrowed with no local record crediting them
    /// ([`UnconfirmedEscrows::untracked`]). The handle releases each from the
    /// pending list once it holds it.
    untracked: Vec<UntrackedEscrow>,
}

impl std::fmt::Display for UnreconciledEscrows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending = self.pending.iter().map(ToString::to_string);
        let untracked = self.untracked.iter().map(ToString::to_string);
        for (i, escrow) in pending.chain(untracked).enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            f.write_str(&escrow)?;
        }
        Ok(())
    }
}

impl std::error::Error for UnreconciledEscrows {}

/// End a command with every `openPool` and `topUp` that `escrows` still holds
/// named: [`name_unconfirmed_escrows`] over what the handle holds.
pub(crate) fn name_held_escrows<T>(
    result: anyhow::Result<T>,
    escrows: &UnconfirmedEscrows,
) -> anyhow::Result<T> {
    name_unconfirmed_escrows(result, &escrows.pending(), &escrows.untracked())
}

/// End a command that may have dropped an `openPool` or a `topUp` before its
/// receipt was read, or that holds one that escrowed with no local record.
/// `escrows` are the transactions it did not see confirm
/// ([`UnconfirmedEscrows::pending`]), and `untracked` those that escrowed
/// untracked ([`UnconfirmedEscrows::untracked`]).
///
/// A pending transaction the error carries by type, or whose line the
/// error's text holds, or an untracked one whose tx the error's text names, is
/// not named twice. A stop (an [`Interrupted`]
/// or a [`GaveUp`]) never counts as naming an escrow by its text:
/// `main` prints only the stop's own line. With none left, `result` passes
/// through. Otherwise they are attached to the result as
/// [`UnreconciledEscrows`], so the command names each tx. An `Ok` becomes an
/// error: the USDC may escrow, or has escrowed, with no local record, and only
/// a non-zero exit makes sure someone reconciles it. An error keeps its own
/// type underneath, so an [`Interrupted`] still exits 130.
pub(crate) fn name_unconfirmed_escrows<T>(
    result: anyhow::Result<T>,
    escrows: &[EscrowUnconfirmed],
    untracked: &[UntrackedEscrow],
) -> anyhow::Result<T> {
    let err = result.as_ref().err();
    let carried = err.and_then(carried);
    // The error that reported an escrow can sit inside one that hides it from
    // a downcast, so it is looked for in the text too: a bundle pull folds
    // every escrow but the first in as text.
    let named = err
        .filter(|err| err.downcast_ref::<Interrupted>().is_none())
        .filter(|err| err.downcast_ref::<GaveUp>().is_none())
        .map(|err| format!("{err:#}"));
    let pending: Vec<EscrowUnconfirmed> = escrows
        .iter()
        .copied()
        .filter(|escrow| Some(*escrow) != carried)
        .filter(|escrow| {
            named
                .as_ref()
                .is_none_or(|named| !named.contains(&escrow.to_string()))
        })
        .collect();
    let untracked: Vec<UntrackedEscrow> = untracked
        .iter()
        .filter(|held| {
            named
                .as_ref()
                .is_none_or(|named| !named.contains(&held.tx.to_string()))
        })
        .cloned()
        .collect();
    if pending.is_empty() && untracked.is_empty() {
        return result;
    }
    let left = UnreconciledEscrows { pending, untracked };
    match result {
        Ok(_) => Err(anyhow::Error::new(left)),
        Err(err) => Err(err.context(left)),
    }
}

/// The unconfirmed `openPool` or `topUp` that `err` itself carries, from a
/// submit lost in transport, a receipt read that failed or lapsed, or a
/// refusal that names an earlier one.
fn carried(err: &anyhow::Error) -> Option<EscrowUnconfirmed> {
    err.downcast_ref::<OpenUnconfirmed>()
        .copied()
        .map(EscrowUnconfirmed::from)
        .or_else(|| {
            err.downcast_ref::<TopUpUnconfirmed>()
                .copied()
                .map(EscrowUnconfirmed::from)
        })
}

/// The `openPool` and `topUp` transactions `err` names as possibly escrowing,
/// or as escrowed with no local record, as text: the one it carries, then the
/// [`UnreconciledEscrows`] attached to it.
fn in_flight(err: &anyhow::Error) -> Option<String> {
    let lines: Vec<String> = carried(err)
        .map(|escrow| escrow.to_string())
        .into_iter()
        .chain(
            err.downcast_ref::<UnreconciledEscrows>()
                .map(ToString::to_string),
        )
        .collect();
    (!lines.is_empty()).then(|| lines.join("\n"))
}

/// The line `main` prints for a command that stopped rather than failed: a
/// Ctrl-C, or a give-up after the no-progress limit. `None` for any other
/// error. An `openPool` or `topUp` in flight or untracked replaces the advice
/// to run the command again, because a re-run before it is reconciled escrows
/// again.
#[must_use]
pub fn stop_message(err: &anyhow::Error) -> Option<String> {
    let in_flight = in_flight(err);
    if err.downcast_ref::<Interrupted>().is_some() {
        return Some(match in_flight {
            Some(escrows) => format!("interrupted: {escrows}"),
            None => "interrupted; run the same command again to resume".to_owned(),
        });
    }
    let gave_up = err.downcast_ref::<GaveUp>()?;
    Some(match in_flight {
        Some(escrows) => format!("no progress for {}s: {escrows}", gave_up.idle.as_secs()),
        None => gave_up.to_string(),
    })
}

/// The exit status for a command that stopped rather than failed:
/// [`INTERRUPTED_EXIT`] for a Ctrl-C, and [`GAVE_UP_EXIT`] for a give-up.
/// `None` for any other error, and for a give-up with an `openPool` or `topUp`
/// in flight or untracked: `EX_TEMPFAIL` tells a wrapper that a re-run is
/// safe, and that one is not.
#[must_use]
pub fn stop_exit(err: &anyhow::Error) -> Option<u8> {
    if err.downcast_ref::<Interrupted>().is_some() {
        return Some(INTERRUPTED_EXIT);
    }
    (err.downcast_ref::<GaveUp>().is_some() && in_flight(err).is_none()).then_some(GAVE_UP_EXIT)
}

/// The lines the immediate exit of a second Ctrl-C prints: one per
/// `openPool` and `topUp` that `escrows` holds, pending or untracked.
fn exit_lines(escrows: &UnconfirmedEscrows) -> Vec<String> {
    let pending = escrows
        .pending()
        .into_iter()
        .map(|escrow| escrow.to_string());
    let untracked = escrows
        .untracked()
        .into_iter()
        .map(|escrow| escrow.to_string());
    pending
        .chain(untracked)
        .map(|escrow| format!("interrupted: {escrow}"))
        .collect()
}

/// The exit status for an interrupted command: 128 + SIGINT.
pub const INTERRUPTED_EXIT: u8 = 130;

/// The exit status for a fetch that gave up after its no-progress limit:
/// `EX_TEMPFAIL`, since a rerun resumes it.
pub const GAVE_UP_EXIT: u8 = 75;

/// A handle that resolves on the first Ctrl-C. Dropping it hands Ctrl-C back
/// to an immediate exit. The immediate exit first names every `openPool` and
/// `topUp` in the command's [`UnconfirmedEscrows`], pending or untracked: it
/// skips the command's own return, which is where the first Ctrl-C names them.
pub(crate) struct Interrupt {
    /// Fires on the first Ctrl-C while this handle lives.
    rx: tokio::sync::oneshot::Receiver<()>,
    /// Whether `rx` has fired; a oneshot is not polled again after it resolves.
    fired: bool,
}

impl Interrupt {
    /// Start watching Ctrl-C for the rest of the process, for a command whose
    /// `openPool` and `topUp` transactions `escrows` holds.
    pub(crate) fn watch(escrows: UnconfirmedEscrows) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut tx = Some(tx);
            // An `Err` means no handler could be installed, so SIGINT keeps its
            // default action and there is nothing to watch.
            while tokio::signal::ctrl_c().await.is_ok() {
                if let Some(tx) = tx.take()
                    && tx.send(()).is_ok()
                {
                    continue;
                }
                // Nothing graceful left to do: remove the tab indicator that
                // `exit` would skip, name what may still escrow or escrowed
                // untracked, then quit.
                super::tab_progress::remove_now();
                for line in exit_lines(&escrows) {
                    eprintln!("{line}");
                }
                std::process::exit(i32::from(INTERRUPTED_EXIT));
            }
        });
        Self { rx, fired: false }
    }

    /// A handle fired by the returned sender in place of a Ctrl-C.
    #[cfg(test)]
    pub(crate) fn manual() -> (tokio::sync::oneshot::Sender<()>, Self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (tx, Self { rx, fired: false })
    }

    /// Resolve once the first Ctrl-C has arrived. Pends forever when no handler
    /// could be installed.
    pub(crate) async fn wait(&mut self) {
        if self.fired {
            return;
        }
        match (&mut self.rx).await {
            Ok(()) => self.fired = true,
            Err(_) => std::future::pending::<()>().await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
