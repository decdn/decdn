//! `decdn key-gen` — write or refresh the persistent Ed25519 node key and
//! the Ethereum keystore.

use anyhow::Context;
use decdn_common::{cli, identity};
use decdn_incentive::eth_identity;

/// Generate (or reuse) the persistent Ed25519 node key, and a fresh
/// Ethereum keystore alongside it.
pub fn key_gen(args: &cli::KeyGenArgs) -> anyhow::Result<()> {
    let output_dir = args
        .output_dir
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine output directory: home dir not found"))?;

    let key_path = identity::key_path(&output_dir);
    let keystore_path = eth_identity::keystore_path(&output_dir);

    // Pre-flight check: error before any writes if either target exists and
    // --force was not passed. Halving the work into "check then act" keeps
    // partial-success states out of the operator's data_dir.
    if !args.force {
        if key_path.exists() {
            anyhow::bail!(
                "node key already exists at {}; pass --force to overwrite",
                key_path.display()
            );
        }
        if keystore_path.exists() {
            anyhow::bail!(
                "eth keystore already exists at {}; pass --force to overwrite",
                keystore_path.display()
            );
        }
    }

    // Source the keystore password before any disk writes — interactive
    // prompts that abort (Ctrl-C, mismatch retries exhausted) shouldn't
    // leave a half-written `node.secret` behind. `confirm: true` because this
    // command CREATES the keystore: an entry typed once has nothing to check it
    // against, so a typo would be sealed into the file. `confirm` reaches the
    // prompt alone, which is why the empty-password warning below is
    // unconditional.
    let password = eth_identity::read_password(
        &super::chain_ctx::password_sources(args.keystore_password_file.as_deref(), true),
        "eth keystore password",
    )?;

    // An empty password is representable on purpose (#1931), but creating a
    // keystore with one is unverifiable from the operator's side — a shell
    // expanding an unset variable into `DECDN_KEYSTORE_PASSWORD` looks the same
    // as a deliberate choice. Say so once, loudly. Loading needs no such
    // warning: the wrong password fails to decrypt.
    if password.is_empty() {
        eprintln!(
            "warning: creating {} with an EMPTY password; \
             anyone who can read the file can use the key",
            keystore_path.display()
        );
    }

    // Two-phase rotation so a `--force` overwrite can't half-rotate the key
    // pair (#844). STAGE both secrets first — all the expensive, failure-prone
    // work (RNG, scrypt encryption, temp-file writes) lands in temp files and
    // touches neither canonical file. Only once BOTH stages succeed do we
    // COMMIT (archive-old + atomic rename), so a failure generating the second
    // secret leaves the first untouched rather than mismatched. If
    // `stage_keystore` fails, `staged_key` drops and its temp is cleaned up.
    let mut staged_key = identity::stage_node_key(&output_dir)?;
    let staged_keystore = eth_identity::stage_keystore(&output_dir, &password)?;

    // Commit phase: only fast metadata ops (archive + atomic rename) run here —
    // all key generation, encryption, and RNG already succeeded during staging,
    // so the half-rotation hazard is bounded to this window. Existing files are
    // archived rather than destroyed so the operator key-rotation runbook
    // (`appendix-operator-key-rotation.md` §1 step 9 / §5 rollback) has the prior
    // key material to fall back on.
    //
    // The window is narrow but NOT crash-only: a commit can still return an error
    // (a `move_aside` archive collision, or a rename hitting ENOSPC/EXDEV/EACCES).
    // If the node key commits and the keystore commit then fails, the pair is
    // half-rotated; the keystore error below is annotated with that fact and the
    // recovery path so the operator isn't misled into thinking nothing changed.
    println!("node id: {}", staged_key.public());
    // A node-key commit failure here is not half-rotated: the keystore hasn't been
    // touched yet, and `commit` is fail-safe — on a failed install rename it
    // restores the archived prior key in place (or, if that restore itself fails,
    // its error names the `.bak` archive to restore manually). Either way the eth
    // side is untouched, so we let it propagate as-is.
    if let Some(bak) = staged_key.commit()? {
        println!("archived previous node key -> {}", bak.display());
    }
    println!("wrote secret key to {}", key_path.display());

    let address = staged_keystore.address();
    let keystore_bak = staged_keystore.commit().context(
        "the node key was already rotated but committing the new eth keystore failed — the key \
         pair is now HALF-ROTATED. Restore node.secret from its `.bak` archive per \
         appendix-operator-key-rotation.md §5, or re-run `decdn key-gen --force` once the cause \
         (e.g. disk space) is resolved",
    )?;
    if let Some(bak) = keystore_bak {
        println!("archived previous eth keystore -> {}", bak.display());
    }
    println!("eth address: {address}");
    println!("wrote eth keystore to {}", keystore_path.display());

    Ok(())
}
