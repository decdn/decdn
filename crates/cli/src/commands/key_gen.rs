//! `decdn key-gen` — write or refresh the persistent Ed25519 node key.

use decdn_common::{cli, identity};

/// Generate (or reuse) the persistent Ed25519 node key.
pub fn key_gen(args: &cli::KeyGenArgs) -> anyhow::Result<()> {
    let output_dir = args
        .output_dir
        .as_deref()
        .map(cli::common::expand_tilde)
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine output directory: home dir not found"))?;

    let key_path = identity::key_path(&output_dir);
    if key_path.exists() {
        if !args.force {
            anyhow::bail!(
                "node key already exists at {}; pass --force to overwrite",
                key_path.display()
            );
        }
        std::fs::remove_file(&key_path)
            .map_err(|e| anyhow::anyhow!("failed to remove {}: {e}", key_path.display()))?;
    }

    let key = identity::load_or_generate(&output_dir)?;
    println!("node id: {}", key.public());
    println!("wrote secret key to {}", key_path.display());
    Ok(())
}
