//! Graceful Ctrl-C for `decdn fetch` and `decdn bundle pull`.
//!
//! The first Ctrl-C resolves `Interrupt::wait`: the command stops its
//! transfers, writes what it must keep (the bundle skip-cache, the progress
//! indicators' cleanup), and returns [`Interrupted`], which exits with status
//! 130. A second Ctrl-C, or one that arrives after the command stops watching,
//! exits at once with the same status.
//!
//! Stopping mid-transfer loses nothing already paid for: every fetch resumes
//! from its on-disk `.partial` on the next run.

/// The error a command returns when Ctrl-C stopped it. `main` prints a resume
/// hint and exits with status 130 (128 + SIGINT), the shell convention for an
/// interrupted command.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted")
    }
}

impl std::error::Error for Interrupted {}

/// The exit status for an interrupted command: 128 + SIGINT.
pub const INTERRUPTED_EXIT: u8 = 130;

/// A handle that resolves on the first Ctrl-C. Dropping it hands Ctrl-C back
/// to an immediate exit.
pub(crate) struct Interrupt {
    /// Fires on the first Ctrl-C while this handle lives.
    rx: tokio::sync::oneshot::Receiver<()>,
    /// Whether `rx` has fired; a oneshot is not polled again after it resolves.
    fired: bool,
}

impl Interrupt {
    /// Start watching Ctrl-C for the rest of the process.
    pub(crate) fn watch() -> Self {
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
                // `exit` would skip, then quit.
                super::tab_progress::remove_now();
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn wait_resolves_again_after_it_fired() {
        let (fire, mut interrupt) = Interrupt::manual();
        fire.send(()).unwrap();
        interrupt.wait().await;
        // A second wait (a later `select!`) must not poll the spent oneshot.
        interrupt.wait().await;
    }

    #[tokio::test]
    async fn wait_pends_while_nothing_fired() {
        let (_fire, mut interrupt) = Interrupt::manual();
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), interrupt.wait()).await;
        assert!(waited.is_err());
    }
}
