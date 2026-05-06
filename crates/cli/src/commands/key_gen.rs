//! `decdn key-gen` — write or refresh the persistent Ed25519 node key and
//! the Ethereum keystore.

use decdn_common::{cli, eth_identity, identity};

const KEYSTORE_PASSWORD_ENV: &str = "DECDN_KEYSTORE_PASSWORD";

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
    // leave a half-written `node.secret` behind. Build the source list
    // dynamically so `--password-file` only appears when the operator
    // passed one (an absent `--password-file` should fall through to the
    // env var or the prompt, not error on a missing file).
    let mut sources = vec![eth_identity::PasswordSource::Env(KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = args.password_file.as_deref() {
        sources.push(eth_identity::PasswordSource::File(path.to_path_buf()));
    }
    sources.push(eth_identity::PasswordSource::Prompt { confirm: true });
    let password = eth_identity::read_password(&sources, "eth keystore password")?;

    // Force-replace order: the node key write is also non-atomic across the
    // two files, but each file's own write is atomic and the ordering
    // mirrors `decdn run` startup (Ed25519 first, ETH keystore second).
    if args.force && key_path.exists() {
        std::fs::remove_file(&key_path)
            .map_err(|e| anyhow::anyhow!("failed to remove {}: {e}", key_path.display()))?;
    }
    let key = identity::load_or_generate(&output_dir)?;
    println!("node id: {}", key.public());
    println!("wrote secret key to {}", key_path.display());

    let address = eth_identity::generate_and_persist(&output_dir, &password, args.force)?;
    println!("eth address: {address}");
    println!("wrote eth keystore to {}", keystore_path.display());

    Ok(())
}
