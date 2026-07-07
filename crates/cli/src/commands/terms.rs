//! Operator-terms acceptance at registration (ADR 019 § Terms Acceptance).
//!
//! The canonical operator terms ship embedded in the binary ([`TERMS_TEXT`]);
//! their `keccak256` is the exact preimage the on-chain `currentTermsHash`
//! commits to. Before signing `registerNode`, the CLI shows the operator the
//! terms and records an explicit affirmative acknowledgement — interactively on
//! a terminal, or via `--accept-terms` for headless/automated provisioning.
//!
//! Enforcement is deliberately split: [`decide`] is the pure policy (unit
//! tested), and [`ensure_accepted`] is the thin IO wrapper that displays the
//! terms and prompts.

use std::io::{self, IsTerminal, Write};

use alloy::primitives::{B256, keccak256};

/// The canonical operator terms, embedded from the repo-root `TERMS.md` so the
/// displayed text is the exact preimage of the recorded hash — there is no
/// external document to fetch or substitute (ADR 019 § Terms Acceptance).
pub const TERMS_TEXT: &str = include_str!("../../../../TERMS.md");

/// `keccak256` of the embedded terms — the value the operator's signature
/// commits to and which must equal the network's on-chain `currentTermsHash`.
#[must_use]
pub fn terms_hash() -> B256 {
    keccak256(TERMS_TEXT.as_bytes())
}

/// Human-readable version pulled from the `**Version:**` line of `TERMS.md`
/// (display only — the hash is the authoritative identifier).
#[must_use]
pub fn terms_version() -> &'static str {
    TERMS_TEXT
        .lines()
        .find_map(|line| line.strip_prefix("**Version:**"))
        .map_or("unknown", str::trim)
}

/// Outcome of the acceptance policy given the invocation context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Terms accepted (explicit `--accept-terms`); proceed without a prompt.
    Proceed,
    /// Interactive terminal, no flag — show the terms and prompt `[y/N]`.
    Prompt,
    /// No terminal and no `--accept-terms` — cannot record consent; abort.
    NeedFlag,
    /// The embedded terms do not match the network's current terms; the client
    /// is stale and must be updated before it can register (ADR 019 § 281).
    StaleClient,
}

/// Pure acceptance policy. `local` is [`terms_hash`]; `chain` is the network's
/// `currentTermsHash`. The staleness check takes precedence over everything —
/// we never sign a hash the operator was not shown, even with `--accept-terms`.
#[must_use]
pub fn decide(accept_flag: bool, is_tty: bool, local: B256, chain: B256) -> Decision {
    if local != chain {
        Decision::StaleClient
    } else if accept_flag {
        Decision::Proceed
    } else if is_tty {
        Decision::Prompt
    } else {
        Decision::NeedFlag
    }
}

/// Enforce operator-terms acceptance before registration signs `registerNode`.
///
/// `chain_terms_hash` is the network's on-chain `currentTermsHash`;
/// `accept_flag` is `--accept-terms`. Returns `Ok(())` only when the operator
/// has accepted the current terms; otherwise returns a descriptive error and
/// registration must not proceed.
///
/// # Errors
///
/// - Client stale (embedded terms ≠ on-chain terms).
/// - No terminal and `--accept-terms` not passed.
/// - The operator answered no at the interactive prompt.
pub fn ensure_accepted(chain_terms_hash: B256, accept_flag: bool) -> anyhow::Result<()> {
    let local = terms_hash();
    // Interactive only when BOTH streams are terminals: the terms + prompt go to
    // stderr and the answer is read from stdin, so if either is redirected the
    // operator can't see the prompt — fall back to requiring `--accept-terms`
    // rather than blocking on invisible input.
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    match decide(accept_flag, interactive, local, chain_terms_hash) {
        Decision::StaleClient => anyhow::bail!(
            "operator terms mismatch: this build ships terms {} ({:#x}) but the network's \
             current terms are {:#x}. Update `decdn` to a build whose terms match the network \
             (ADR 019 § Terms Acceptance), then retry.",
            terms_version(),
            local,
            chain_terms_hash,
        ),
        Decision::Proceed => {
            // Flag path: the operator asserts prior review; record a one-line
            // confirmation rather than re-dumping the full text.
            let mut err = io::stderr().lock();
            writeln!(
                err,
                "Accepting deCDN operator terms {} ({:#x}) per --accept-terms.",
                terms_version(),
                local,
            )?;
            Ok(())
        }
        Decision::NeedFlag => {
            print_terms()?;
            anyhow::bail!(
                "operator terms not accepted: no interactive terminal detected — re-run with \
                 `--accept-terms` after reviewing the terms above."
            )
        }
        Decision::Prompt => {
            print_terms()?;
            if prompt_yes_no()? {
                Ok(())
            } else {
                anyhow::bail!("registration cancelled: operator terms not accepted.")
            }
        }
    }
}

/// Render the full terms + version + hash to stderr (stdout stays reserved for
/// the machine-readable registration outcome / `--json`).
fn print_terms() -> io::Result<()> {
    let mut err = io::stderr().lock();
    let rule = "─".repeat(64);
    writeln!(
        err,
        "\n{rule}\ndeCDN Operator Terms {} ({:#x})\n{rule}",
        terms_version(),
        terms_hash(),
    )?;
    writeln!(err, "{}", TERMS_TEXT.trim_end())?;
    writeln!(err, "{rule}")?;
    Ok(())
}

/// Prompt for acceptance on the controlling terminal. Defaults to no: only an
/// explicit `y` / `yes` accepts.
fn prompt_yes_no() -> anyhow::Result<bool> {
    let mut err = io::stderr().lock();
    write!(err, "Accept these operator terms? [y/N] ")?;
    err.flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(answer == "y" || answer == "yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    #[test]
    fn stale_client_beats_every_other_signal() {
        // Even with the flag set on a TTY, a hash mismatch is always stale.
        assert_eq!(decide(true, true, h(0xAA), h(0xBB)), Decision::StaleClient);
        assert_eq!(
            decide(false, false, h(0xAA), h(0xBB)),
            Decision::StaleClient
        );
    }

    #[test]
    fn matching_hash_with_flag_proceeds() {
        assert_eq!(decide(true, true, h(0x11), h(0x11)), Decision::Proceed);
        assert_eq!(decide(true, false, h(0x11), h(0x11)), Decision::Proceed);
    }

    #[test]
    fn matching_hash_on_tty_without_flag_prompts() {
        assert_eq!(decide(false, true, h(0x11), h(0x11)), Decision::Prompt);
    }

    #[test]
    fn matching_hash_headless_without_flag_needs_flag() {
        assert_eq!(decide(false, false, h(0x11), h(0x11)), Decision::NeedFlag);
    }

    #[test]
    fn embedded_terms_hash_is_nonzero_and_versioned() {
        assert_ne!(terms_hash(), B256::ZERO, "TERMS.md must embed and hash");
        assert_ne!(
            terms_version(),
            "unknown",
            "TERMS.md must carry a Version line"
        );
    }

    /// Locks the embedded terms hash to `keccak256` of the exact `TERMS.md`
    /// bytes (verify out-of-band: `cast keccak 0x$(xxd -p -c1000000 TERMS.md)`).
    /// Editing TERMS.md deliberately flips this — that is intended: the hash is
    /// consensus-critical, so a terms change is a gated event that must be paired
    /// with a governance `setCurrentTermsHash` bump and a new deploy value.
    #[test]
    fn embedded_terms_hash_matches_committed_terms_md() {
        let expected = alloy::primitives::b256!(
            "7da33feb48d0b64ca41a4478ea67aeb13832b56b7598cca803bc2b9f5ec37f4b"
        );
        assert_eq!(
            terms_hash(),
            expected,
            "TERMS.md changed — update this lock AND the network's currentTermsHash \
             (governance setCurrentTermsHash + CURRENT_TERMS_HASH deploy value)"
        );
    }

    // The test harness has no controlling terminal, so `ensure_accepted`
    // exercises the headless branches for real (is_terminal() == false).

    #[test]
    fn ensure_accepted_headless_requires_the_flag() {
        // Matching terms but no flag and no TTY → must refuse (NeedFlag).
        assert!(ensure_accepted(terms_hash(), false).is_err());
    }

    #[test]
    fn ensure_accepted_with_flag_and_matching_terms_ok() {
        assert!(ensure_accepted(terms_hash(), true).is_ok());
    }

    #[test]
    fn ensure_accepted_stale_terms_refused_even_with_flag() {
        // A network terms hash that differs from the embedded one is stale and
        // must be refused regardless of the flag — never sign unseen terms.
        assert!(ensure_accepted(B256::repeat_byte(0x99), true).is_err());
    }
}
