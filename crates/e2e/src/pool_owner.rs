//! A pool owner that funds a `decdn gateway` through a capability, as an
//! operator's controller does: [`PoolOwner::open`] opens a pool from a
//! funded keystore, [`PoolOwner::assign`] issues a `dcap1:` capability from
//! it, and [`register_capability`] registers one to a gateway's pending key.
//! A gateway holds no keystore and no pool of its own, so every journey that
//! runs one funds it this way.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_incentive::eth_identity;
use reqwest::StatusCode;

use crate::chain::ChainFixture;
use crate::cli::decdn_command;
use crate::node::NodeFixture;

/// An owner's pool deposit: 10 USDC.
pub const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// A capability's cap: 5 USDC, far above what a journey pulls.
pub const CAP_MICRO_USDC: u64 = 5_000_000;
/// The password of the owner's keystore.
const KEYSTORE_PASSWORD: &str = "pool-owner-e2e-password";

/// A pool owner: a funded keystore under its own data dir, and the pool it
/// opened.
#[derive(Debug)]
pub struct PoolOwner {
    dir: tempfile::TempDir,
    keystore: PathBuf,
    /// The pool the owner opened.
    pub pool_id: B256,
}

impl PoolOwner {
    /// A funded owner whose pool `node` already serves: the owner's own fetch
    /// of `warm` waits out the node's view of the fresh pool, so a gateway
    /// never takes that race for a funding stop.
    ///
    /// # Errors
    ///
    /// The keystore, the funding, the pool open or the warm-up fails.
    pub async fn open(
        chain: &ChainFixture,
        node: &NodeFixture,
        warm: &Hash,
    ) -> anyhow::Result<Self> {
        let dir = private_tempdir("owner")?;
        eth_identity::generate_and_persist(dir.path(), KEYSTORE_PASSWORD, false)
            .context("generate the owner keystore")?;
        let keystore = eth_identity::keystore_path(dir.path());
        let owner = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD)?.address();
        chain.fund_eth(owner, 100).await?;
        chain
            .mint_usdc(owner, U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64))
            .await?;
        let opened = decdn_ok(
            dir.path(),
            &[
                strs(&[
                    "pool",
                    "open",
                    "--deposit-micro-usdc",
                    &DEPOSIT_MICRO_USDC.to_string(),
                ]),
                chain_flags(chain),
                owner_flags(&keystore, dir.path()),
            ]
            .concat(),
        )
        .await?;
        let pool_id = parse_pool_id(&opened)?;
        let owner = Self {
            dir,
            keystore,
            pool_id,
        };
        owner.warm_up(chain, node, warm).await?;
        Ok(owner)
    }

    /// The `dcap1:` capability this owner issues `signer` from its pool.
    ///
    /// # Errors
    ///
    /// `decdn pool assign` fails or prints no capability.
    pub async fn assign(&self, chain: &ChainFixture, signer: &str) -> anyhow::Result<String> {
        let assigned = self.pool_assign(chain, signer, &[]).await?;
        assigned
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("dcap1:"))
            .map(str::to_owned)
            .with_context(|| format!("the dcap1: token in `pool assign` output for {signer}"))
    }

    /// The `PUT /_capability` body that registers a capability this owner
    /// issues `signer`, as `decdn pool assign` prints it: scoped to
    /// `namespaces`, or for any content when `namespaces` is empty.
    ///
    /// # Errors
    ///
    /// `decdn pool assign` fails or prints no registration body.
    pub async fn registration(
        &self,
        chain: &ChainFixture,
        signer: &str,
        namespaces: &[u64],
    ) -> anyhow::Result<String> {
        let scope = if namespaces.is_empty() {
            vec!["--any-namespace".to_owned()]
        } else {
            namespaces
                .iter()
                .flat_map(|ns| ["--namespace".to_owned(), ns.to_string()])
                .collect()
        };
        let assigned = self.pool_assign(chain, signer, &scope).await?;
        assigned
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with('{'))
            .map(str::to_owned)
            .with_context(|| format!("the registration body in `pool assign` output for {signer}"))
    }

    /// `decdn pool assign` from this owner's pool to `signer`, with `extra`
    /// flags: its stdout.
    async fn pool_assign(
        &self,
        chain: &ChainFixture,
        signer: &str,
        extra: &[String],
    ) -> anyhow::Result<String> {
        decdn_ok(
            self.dir.path(),
            &[
                strs(&[
                    "pool",
                    "assign",
                    "--pool",
                    &format!("{:#x}", self.pool_id),
                    "--signer",
                    signer,
                    "--cap-micro-usdc",
                    &CAP_MICRO_USDC.to_string(),
                    "--expiry-secs",
                    "3600",
                ]),
                extra.to_vec(),
                chain_flags(chain),
                owner_flags(&self.keystore, self.dir.path()),
            ]
            .concat(),
        )
        .await
    }

    /// The owner's own `decdn get` of `hash`, retried until the node serves
    /// its fresh pool.
    async fn warm_up(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: &Hash,
    ) -> anyhow::Result<()> {
        let home = self.dir.path();
        let out = home.join("warm.bin");
        let args = [
            vec![
                "get".to_owned(),
                format!("decdn:{}", hash.to_hex()),
                "-o".to_owned(),
                out.display().to_string(),
            ],
            chain_flags(chain),
            node_flags(chain, node),
            owner_flags(&self.keystore, home),
        ]
        .concat();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
                .args(&args)
                .output()
                .await
                .context("spawn the owner's decdn get")?;
            if output.status.success() {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "the owner's warm-up get never succeeded: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    }
}

/// Register a capability from `owner`'s pool on the gateway at `base`, as an
/// operator's controller does: read the pending key from `GET /_delegate`
/// once the gateway answers, have `owner` issue it a capability, and
/// `PUT /_capability`. The capability pays for the content of `namespaces`,
/// or for any content when `namespaces` is empty. Every request carries
/// `host` as its `Host`. Returns the key the capability claimed.
///
/// # Errors
///
/// The gateway does not answer within a minute, the assignment fails, or the
/// gateway refuses the capability.
pub async fn register_capability(
    http: &reqwest::Client,
    base: &str,
    host: &str,
    chain: &ChainFixture,
    owner: &PoolOwner,
    namespaces: &[u64],
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let pending = loop {
        let answer = http
            .get(format!("{base}/_delegate"))
            .header("host", host)
            .send()
            .await;
        if let Ok(answer) = answer
            && answer.status() == StatusCode::OK
        {
            break answer.json::<serde_json::Value>().await?;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the gateway never answered GET /_delegate on {base}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let delegate = pending
        .get("address")
        .and_then(serde_json::Value::as_str)
        .context("the pending key's address")?
        .to_owned();
    let body = owner.registration(chain, &delegate, namespaces).await?;
    let registered = http
        .put(format!("{base}/_capability"))
        .header("host", host)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .context("register the capability")?;
    let status = registered.status();
    anyhow::ensure!(
        status == StatusCode::CREATED,
        "the gateway refused the capability ({status}): {}",
        registered.text().await.unwrap_or_default()
    );
    Ok(delegate)
}

/// The chain coordinates as flags, so no config file is needed.
#[must_use]
pub fn chain_flags(chain: &ChainFixture) -> Vec<String> {
    vec![
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--chain-id".into(),
        chain.chain_id().to_string(),
    ]
}

/// The paid-pull flags that pin `node`. `--capacity-bond-address` is the
/// EIP-712 `verifyingContract` of the client identity binding a node requires.
#[must_use]
pub fn node_flags(chain: &ChainFixture, node: &NodeFixture) -> Vec<String> {
    vec![
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
    ]
}

/// A fresh owner-only temp dir: the buyer store refuses one others can read.
///
/// # Errors
///
/// The directory cannot be made or its mode set.
pub fn private_tempdir(role: &str) -> anyhow::Result<tempfile::TempDir> {
    let dir = tempfile::tempdir().with_context(|| format!("{role} tempdir"))?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .with_context(|| format!("chmod {role} dir 0o700"))?;
    Ok(dir)
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

fn owner_flags(keystore: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        "--keystore".into(),
        keystore.display().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
    ]
}

/// Run `decdn <args>` from `home`, requiring success, and return its stdout.
async fn decdn_ok(home: &Path, args: &[String]) -> anyhow::Result<String> {
    let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
        .args(args)
        .output()
        .await
        .context("spawn decdn")?;
    anyhow::ensure!(
        output.status.success(),
        "decdn {} failed: {}",
        args.first().map_or("", String::as_str),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

/// Parse the `{"poolId": "0x…"}` document `decdn pool open` prints.
fn parse_pool_id(stdout: &str) -> anyhow::Result<B256> {
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("pool open output was not JSON: {stdout:?}"))?;
    let raw = parsed
        .get("poolId")
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("no poolId in {stdout:?}"))?;
    B256::from_str(raw).context("poolId is not a 32-byte hash")
}
