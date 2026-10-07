use alloy::primitives::{Address, B256, U256};
use alloy::sol_types::SolEvent;

use super::{PaymentPool, to_pool_u64};

/// Pins the `PoolRedeemed` event signature (and therefore its topic-0
/// selector) to `contracts/src/PaymentPool.sol`'s field list, so a drift
/// between the Solidity source and this binding fails the build instead
/// of silently mis-decoding a node's settlement log.
///
/// The ABI signature string does not encode which params are `indexed`
/// — see `pool_redeemed_indexed_layout_matches_contract` below for the
/// companion pin on topic/data placement.
#[test]
fn pool_redeemed_signature_matches_contract() {
    assert_eq!(
        PaymentPool::PoolRedeemed::SIGNATURE,
        "PoolRedeemed(bytes32,address,(address,uint64,uint64)[])"
    );
}

/// Pins which `PoolRedeemed` params are `indexed`: `poolId` and
/// `provider` land in the log's topics (topic0 is the selector, topics
/// 1-2 the indexed params); the `lanes` array lands in the log data,
/// carrying `signer` per entry rather than as a topic. The
/// signature-string pin above is blind to this split, so a future edit
/// that moved `indexed` onto the wrong param would pass that test while
/// silently breaking a node's `provider`-topic filter for its settlement
/// watcher. This test builds the event with distinct sentinel values and
/// checks each one landed where the ABI encoding puts it.
#[test]
fn pool_redeemed_indexed_layout_matches_contract() {
    let pool_id = B256::repeat_byte(0x11);
    let provider = Address::repeat_byte(0x33);
    let signer_a = Address::repeat_byte(0x22);
    let signer_b = Address::repeat_byte(0x44);

    let event = PaymentPool::PoolRedeemed {
        poolId: pool_id,
        provider,
        lanes: vec![
            PaymentPool::LaneSettled {
                signer: signer_a,
                newPaidCumulative: 3_000,
                bytesPaid: 2_000,
            },
            PaymentPool::LaneSettled {
                signer: signer_b,
                newPaidCumulative: 7_000,
                bytesPaid: 5_000,
            },
        ],
    };
    let log = event.encode_log_data();

    // topic0 (selector) + exactly the 2 indexed params. `signer` is per
    // lane now, so it cannot be a topic — a node keys the lane from the
    // decoded entry instead.
    assert_eq!(log.topics().len(), 3);
    assert_eq!(
        log.topics().first(),
        Some(&PaymentPool::PoolRedeemed::SIGNATURE_HASH)
    );
    assert_eq!(log.topics().get(1), Some(&pool_id));
    assert_eq!(log.topics().get(2), Some(&provider.into_word()));

    // The dynamic array is head-encoded (offset, then length, then the
    // static entries), so the whole payload is 2 framing words plus 3
    // words per lane — which is the shape the per-lane log cost rests on.
    assert_eq!(log.data.len(), 32 * (2 + 3 * 2));

    // Round-trip through the decoder recovers the same lanes, proving the
    // topic/data split above is exactly what a real watcher decodes.
    let decoded = PaymentPool::PoolRedeemed::decode_log_data(&log).unwrap();
    assert_eq!(decoded.poolId, pool_id);
    assert_eq!(decoded.provider, provider);
    assert_eq!(decoded.lanes.len(), 2);
    assert_eq!(decoded.lanes[0].signer, signer_a);
    assert_eq!(decoded.lanes[0].newPaidCumulative, 3_000);
    assert_eq!(decoded.lanes[0].bytesPaid, 2_000);
    assert_eq!(decoded.lanes[1].signer, signer_b);
    assert_eq!(decoded.lanes[1].newPaidCumulative, 7_000);
}

/// A `PaymentPool` whose `eth_call`s are answered in order from `calls`: the
/// first answers `getPools`, each later one a `getPool`. `None` faults the
/// call, as a transient RPC error does.
fn mocked(
    calls: Vec<Option<alloy::primitives::Bytes>>,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone + 'static> {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    let asserter = Asserter::new();
    for call in calls {
        match call {
            Some(response) => asserter.push_success(&response),
            None => asserter.push_failure_msg("transient rpc fault"),
        }
    }
    PaymentPool::new(
        Address::ZERO,
        ProviderBuilder::new().connect_mocked_client(asserter),
    )
}

fn pool(status: PaymentPool::Status, deposit: u64, redeemed: u64) -> PaymentPool::Pool {
    PaymentPool::Pool {
        owner: Address::repeat_byte(1),
        status,
        disputeDeadline: 0,
        deposit,
        totalRedeemed: redeemed,
    }
}

/// Of two pools that can both still pay, the newer is adopted.
///
/// Both answers are solvent, so a walk in the wrong direction returns the
/// other id however the mock's positional answers line up — this pins the
/// order itself, not an accident of which answer each read happened to get.
#[tokio::test]
async fn adopts_the_newer_of_two_solvent_pools() -> anyhow::Result<()> {
    use alloy::sol_types::SolValue;
    let older = B256::repeat_byte(0xAA);
    let newer = B256::repeat_byte(0xBB);
    let contract = mocked(vec![
        Some(vec![older, newer].abi_encode().into()),
        Some(
            pool(PaymentPool::Status::Open, 10_000_000, 2_000_000)
                .abi_encode()
                .into(),
        ),
        Some(
            pool(PaymentPool::Status::Open, 10_000_000, 3_000_000)
                .abi_encode()
                .into(),
        ),
    ]);
    let (id, _) = super::newest_solvent_owned_pool(&contract, Address::repeat_byte(1))
        .await?
        .ok_or_else(|| anyhow::anyhow!("two solvent open pools are on chain"))?;
    anyhow::ensure!(id == newer, "the walk must go newest-first, got {id}");
    Ok(())
}

/// The newest pool that can still pay wins, and a newer one that cannot is
/// passed over rather than adopted.
///
/// A fully-redeemed pool is still `Open` on chain. Adopting it would hand the
/// buyer a pool with nothing to spend, and — since nothing opens beside an
/// adopted pool — leave it unable to pay anyone.
#[tokio::test]
async fn adopts_the_newest_pool_that_can_still_pay() -> anyhow::Result<()> {
    use alloy::sol_types::SolValue;
    let older = B256::repeat_byte(0xAA);
    let newer = B256::repeat_byte(0xBB);
    // `getPools` is oldest-first; the walk reads the newer one first.
    let contract = mocked(vec![
        Some(vec![older, newer].abi_encode().into()),
        Some(
            pool(PaymentPool::Status::Open, 10_000_000, 10_000_000)
                .abi_encode()
                .into(),
        ),
        Some(
            pool(PaymentPool::Status::Open, 10_000_000, 1_106_908)
                .abi_encode()
                .into(),
        ),
    ]);
    let (id, adopted) = super::newest_solvent_owned_pool(&contract, Address::repeat_byte(1))
        .await?
        .ok_or_else(|| anyhow::anyhow!("a solvent open pool is on chain"))?;
    anyhow::ensure!(id == older, "the fully-redeemed newer pool must be skipped");
    anyhow::ensure!(adopted.totalRedeemed == 1_106_908);
    Ok(())
}

/// Nothing left to adopt is `None`, which is the one answer that licenses
/// opening a fresh pool.
#[tokio::test]
async fn answers_none_when_no_owned_pool_can_pay() -> anyhow::Result<()> {
    use alloy::sol_types::SolValue;
    let contract = mocked(vec![
        Some(vec![B256::repeat_byte(0xAA)].abi_encode().into()),
        Some(
            pool(PaymentPool::Status::Closing, 10_000_000, 0)
                .abi_encode()
                .into(),
        ),
    ]);
    anyhow::ensure!(
        super::newest_solvent_owned_pool(&contract, Address::repeat_byte(1))
            .await?
            .is_none()
    );
    Ok(())
}

/// A pool read that faults is an error, never `None`.
///
/// `None` means "you own nothing you can pay from — open one", and a caller
/// acts on it by escrowing a deposit. A faulted read says nothing about
/// whether that pool is live, so answering `None` on it would escrow a second
/// deposit beside a pool the wallet already funded — which, on a wallet
/// whose USDC is all in the first, reverts outright.
#[tokio::test]
async fn a_faulted_pool_read_is_an_error_not_an_empty_answer() {
    use alloy::sol_types::SolValue;
    let contract = mocked(vec![
        Some(vec![B256::repeat_byte(0xAA)].abi_encode().into()),
        None,
    ]);
    assert!(
        super::newest_solvent_owned_pool(&contract, Address::repeat_byte(1))
            .await
            .is_err(),
        "\"could not tell\" must not become \"no pool, go open one\""
    );
}

/// The narrowing guard on the money path: every USDC amount crosses into
/// the contract through here, so the ceiling is worth pinning at the exact
/// boundary rather than trusting the `try_from`. `u64::MAX` is a legal
/// deposit (~$18.4 trillion at six decimals); one base unit more is not
/// expressible on-chain and must be refused rather than truncated — a
/// silent wrap here would understate a pool's deposit.
#[test]
fn to_pool_u64_accepts_the_ceiling_and_refuses_one_past_it() -> anyhow::Result<()> {
    let ceiling = U256::from(u64::MAX);
    assert_eq!(to_pool_u64(ceiling, "deposit")?, u64::MAX);

    let over = ceiling + U256::from(1u8);
    let Err(err) = to_pool_u64(over, "deposit") else {
        anyhow::bail!("one base unit past the ceiling must be refused, not truncated");
    };
    assert!(
        err.to_string().contains("deposit"),
        "the error names the field that overflowed, got: {err}"
    );
    Ok(())
}

/// Pins the field list of every struct the read surface decodes into, so
/// a drift between `contracts/src/PaymentPool.sol` and this binding fails
/// the build.
///
/// The event pins above do not cover these: a `sol!` block is a *hand-written
/// restatement* of the ABI, not something derived from the Solidity source,
/// so a struct that gains, loses or narrows a field here compiles perfectly
/// against a contract that disagrees. `getPool` then decodes a `Pool` whose
/// fields have silently shifted, the node reads a garbage `owner`, and the
/// only symptom is a node refusing to serve — no compile error, no decode
/// error, nothing until an anvil run. Pinning the layout is what turns that
/// into a build failure.
///
/// `eip712_encode_type` is used purely as a stable printer for the field
/// list; none of these structs is EIP-712 signed.
#[test]
fn read_surface_struct_layouts_match_contract() {
    use alloy::sol_types::SolStruct;

    assert_eq!(
        PaymentPool::Pool::eip712_encode_type(),
        "Pool(address owner,uint8 status,uint64 disputeDeadline,uint64 deposit,uint64 totalRedeemed)",
        "getPool"
    );
    assert_eq!(
        PaymentPool::Authorization::eip712_encode_type(),
        "Authorization(uint64 cap,uint64 expiry,uint64 spent)",
        "getAuthorization"
    );
    assert_eq!(
        PaymentPool::Lane::eip712_encode_type(),
        "Lane(uint64 amount,uint64 bytesDelivered)",
        "getWatermark / getWatermarks"
    );
}

/// The same pin for the two structs `redeemMany` takes as calldata. A
/// silent drift here would encode a batch the contract cannot decode.
#[test]
fn redeem_call_struct_layouts_match_contract() {
    use alloy::sol_types::SolStruct;

    assert_eq!(
        PaymentPool::CapabilityReg::eip712_encode_type(),
        "CapabilityReg(address signer,uint64 spendingCap,uint64 expiry,bytes ownerSig)"
    );
    assert_eq!(
        PaymentPool::LaneVoucher::eip712_encode_type(),
        "LaneVoucher(address signer,uint64 cumulative,uint64 bytesDelivered,bytes32 r,bytes32 vs,bytes32 chainRoot,bytes32 preimage,uint256 chainMeter)"
    );
}

/// Same pin for the other four ABI-tuple-carrying events consumed by
/// the node's watchers.
#[test]
fn lifecycle_event_signatures_match_contract() {
    assert_eq!(
        PaymentPool::PoolOpened::SIGNATURE,
        "PoolOpened(bytes32,address,uint256)"
    );
    assert_eq!(
        PaymentPool::PoolToppedUp::SIGNATURE,
        "PoolToppedUp(bytes32,uint256,uint256)"
    );
    assert_eq!(
        PaymentPool::PoolCloseInitiated::SIGNATURE,
        "PoolCloseInitiated(bytes32,address,uint256)"
    );
    assert_eq!(
        PaymentPool::PoolReclaimed::SIGNATURE,
        "PoolReclaimed(bytes32,address,uint256)"
    );
    assert_eq!(
        PaymentPool::RateBoundsUpdated::SIGNATURE,
        "RateBoundsUpdated(uint256)"
    );
}

/// Batch companion selector for `getAuthorization` (ADR 003 § redeemer
/// batching).
#[test]
fn get_authorizations_signature_matches_contract() {
    use alloy::sol_types::SolCall;
    assert_eq!(
        PaymentPool::getAuthorizationsCall::SIGNATURE,
        "getAuthorizations(bytes32[],address[])"
    );
}

/// Batch companion selector for `getWatermark` (the seller's pre-redeem
/// reconciliation read).
#[test]
fn get_watermarks_signature_matches_contract() {
    use alloy::sol_types::SolCall;
    assert_eq!(
        PaymentPool::getWatermarksCall::SIGNATURE,
        "getWatermarks(bytes32[],address[],address[])"
    );
}

/// The all-zero row is unregistered; any non-zero `cap` or `expiry` is a
/// registration, a zero-cap one included.
#[test]
fn signer_authorization_reads_registration_from_cap_or_expiry() {
    use super::SignerAuthorization;
    let row = |cap, expiry, spent| PaymentPool::Authorization { cap, expiry, spent };
    assert_eq!(
        SignerAuthorization::from_onchain(&row(0, 0, 0)),
        SignerAuthorization::Unregistered
    );
    assert_eq!(
        SignerAuthorization::from_onchain(&row(0, 9, 0)),
        SignerAuthorization::Registered {
            cap: 0,
            expiry: 9,
            spent: 0
        }
    );
}

/// A registration covers a floor while it is live and `cap − spent`
/// reaches the floor; an unregistered signer always covers.
#[test]
fn signer_authorization_covers_a_floor_within_headroom_and_expiry() {
    use super::SignerAuthorization;
    let auth = SignerAuthorization::Registered {
        cap: 40,
        expiry: 100,
        spent: 30,
    };
    assert!(auth.covers(U256::from(10u64), 99));
    assert!(!auth.covers(U256::from(11u64), 99));
    assert!(!auth.covers(U256::from(1u64), 100), "expired at its expiry");
    assert!(SignerAuthorization::Unregistered.covers(U256::MAX, u64::MAX));
}
