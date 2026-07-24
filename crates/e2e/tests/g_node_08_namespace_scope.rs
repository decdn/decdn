//! G-NODE-08 companion: the authorized-origin gate is namespace-scoped, not
//! operator-scoped (#1368).
//!
//! `pull_origin_gate_blocks` asks whether the request's **namespace** has any
//! currently-authorized active origin — it never compares this node's own
//! operator against the assignment. The surprising half of that, made executable
//! here: a node that holds `H` in its opaque backend and has the gate armed will
//! serve `H` opaquely as soon as the namespace has an authorized origin **even
//! when the DAO assigned that namespace to a *different* operator**.
//!
//! Setup: node A runs the armed gate and holds `H` backend-only. A second
//! operator B (onboarded, active, but never running a daemon) is the *sole*
//! authorized origin of namespace N. The assertion is that A still serves `H`
//! under N. The negative control before ratification (N has no origin, A refuses)
//! keeps the positive from passing vacuously — together they pin the ADR 022 /
//! ADR 037 §85 clarification that authorization is a property of the namespace,
//! not of the operator holding the bytes.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::time::Duration;

use alloy::primitives::B256;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_client_pull::UpstreamRefused;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_protocol::client::StreamError;

const KIB: usize = 1024;
const CACHED_LEN: usize = 96 * KIB;
const ORIGIN_LEN: usize = 160 * KIB;
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);

#[tokio::test(flavor = "multi_thread")]
async fn armed_gate_serves_a_namespace_assigned_to_another_operator() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-NODE-08 namespace-scope exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    let cached_payload = payload(0xC1, CACHED_LEN);
    let origin_payload = payload(0x88, ORIGIN_LEN);

    // Node A: armed authorized-origin gate, opaque `fs` backend, no discovery
    // peers — a served backend-only blob can only have come from its own backend.
    let (node, cached) =
        NodeFixture::launch_authorized_origin(&chain, "US", &[&cached_payload]).await?;
    let cached_hash = *cached.first().context("launch returned no cached hash")?;
    // `H` exists ONLY in A's backend — serving it needs the gate open.
    let origin_hash = node.seed_origin_blob(&origin_payload)?;
    anyhow::ensure!(cached_hash != origin_hash, "fixture payloads must differ");

    // Operator B: onboarded (bonded + registered node) so it is on-chain active,
    // but it runs no daemon. It is a DIFFERENT operator from node A.
    let operator_b = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x2B))
        .context("build operator B signer")?;
    let node_secret_b = iroh::SecretKey::from_bytes(&[0x2Bu8; 32]);
    chain
        .onboard_operator(
            &operator_b,
            &node_secret_b,
            "US",
            "/ip4/127.0.0.1/udp/1/quic-v1",
        )
        .await
        .context("onboard operator B")?;
    anyhow::ensure!(
        operator_b.address() != node.operator_addr(),
        "operator B must be distinct from node A's operator"
    );

    let client = ClientFixture::new(&chain).await?;

    // The cache role is always open: A serves the blob it already holds. This also
    // warms the session used for the single-shot refusal below.
    let (mut session, from_cache) = client.open_session(&chain, &node, cached_hash).await?;
    assert_eq!(from_cache, cached_payload, "A must serve a blob it holds");

    // -------------------------------------------------------------- negative
    // The publisher creates the namespace but has not yet had an origin ratified,
    // so it has no authorized origin and A's gate refuses to reach into its backend.
    let publisher = PrivateKeySigner::random();
    let namespace = chain.create_namespace(&publisher).await?;
    assert!(
        chain.origins(namespace).await?.is_empty(),
        "a fresh namespace has no authorized origin"
    );
    let refused = client
        .fetch_once(&mut session, origin_hash, 0, namespace)
        .await
        .err()
        .context("A must refuse a backend-only hash for an unauthorized namespace")?;
    let code = refused
        .downcast_ref::<UpstreamRefused>()
        .map(|r| r.error().clone())
        .with_context(|| format!("expected a signed refusal, got: {refused:#}"))?;
    assert_eq!(
        code,
        StreamError::NotFound,
        "unauthorized-namespace refusal code"
    );

    // ---------------------------------------------------------- ratify to B
    // Assign the namespace to operator B ONLY — never to node A's operator.
    chain
        .propose_assignment(&publisher, namespace, &[operator_b.address()])
        .await?;
    chain.activate_assignment_after_timelock(namespace).await?;
    assert_eq!(
        chain.origins(namespace).await?,
        vec![operator_b.address()],
        "the namespace's sole authorized origin must be operator B"
    );

    // -------------------------------------------------------------- positive
    // The namespace-wide reading (#1368): the namespace now has an active
    // authorized origin (B), so A's gate opens — even though A is NOT that origin
    // — and A serves `H` opaquely from its own backend. `fetch` retries until A's
    // directory watcher has observed both B's onboarding and the activation.
    let served = client.fetch(&chain, &node, origin_hash, namespace).await?;
    assert_eq!(
        served.bytes, origin_payload,
        "A must serve H even though the namespace is assigned to a DIFFERENT operator"
    );
    assert!(
        client.probe(&node, origin_hash).await?.body.has_blob,
        "the reactive backend fill must have landed the blob in A's store"
    );

    Ok(())
}

/// A deterministic payload whose every byte is >= 0x80 (mirrors G-NODE-08).
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let step = u8::try_from(i % 251).expect("i % 251 fits in u8");
            0x80 | (seed ^ step).wrapping_mul(7) >> 1
        })
        .collect()
}
