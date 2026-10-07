use super::{DIRECT_PATH_GRACE, PathRtt, exchange_within, verify_probe_response};
use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::Eip712Domain;
use decdn_incentive::{ProbeSlashData, slash_judge_domain};
use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
use std::time::Duration;

const HASH: [u8; 32] = [0x11u8; 32];
const TS: u64 = 1_700_000_000_000_000;

fn domain() -> Eip712Domain {
    slash_judge_domain(421_614, Address::repeat_byte(0x0D))
}

fn signed(signer: &PrivateKeySigner, rate_per_mb: u64) -> anyhow::Result<ProbeResponse> {
    let body = ProbeResponseBody {
        hash: HASH,
        has_blob: true,
        rate_per_mb,
        timestamp_us: TS,
    };
    let slash_sig = ProbeSlashData {
        hash: body.hash.into(),
        has_blob: body.has_blob,
        rate_per_mb: body.rate_per_mb,
        timestamp_us: body.timestamp_us,
    }
    .sign(signer, &domain())
    .map_err(|e| anyhow::anyhow!("sign: {e}"))?
    .as_bytes()
    .to_vec();
    Ok(ProbeResponse { body, slash_sig })
}

#[test]
fn an_honestly_signed_response_verifies() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let resp = signed(&signer, 10)?;
    verify_probe_response(&resp, signer.address(), &domain(), HASH, TS)
}

/// The gap this closes: a well-formed 65-byte signature that simply is not
/// the expected operator's. The length check passes it; only recovery catches
/// it. Without recovery a node wins selection on a rate it never committed to
/// and faces nothing for abandoning it.
#[test]
fn a_signature_from_the_wrong_operator_is_rejected() -> anyhow::Result<()> {
    let impostor = PrivateKeySigner::random();
    let expected = PrivateKeySigner::random();
    let resp = signed(&impostor, 1)?;
    anyhow::ensure!(
        resp.slash_sig.len() == decdn_protocol::SLASH_SIG_LEN,
        "the fixture must be well-formed, or this proves nothing about recovery"
    );
    let err = verify_probe_response(&resp, expected.address(), &domain(), HASH, TS)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a signature from another key must not verify"))?;
    anyhow::ensure!(
        err.to_string().contains("verification failed"),
        "expected a verification failure, got: {err}"
    );
    Ok(())
}

/// Signing covers `rate_per_mb`, so re-quoting after signing breaks recovery.
/// That is what makes a quote non-repudiable rather than advisory.
#[test]
fn a_tampered_rate_breaks_recovery() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let mut resp = signed(&signer, 10)?;
    resp.body.rate_per_mb = 1;
    anyhow::ensure!(
        verify_probe_response(&resp, signer.address(), &domain(), HASH, TS).is_err(),
        "a rate edited after signing must not verify"
    );
    Ok(())
}

/// Correlation runs before recovery: a validly-signed answer to a DIFFERENT
/// request must not satisfy this one, or one cheap quote could be replayed
/// across every hash the node is asked about.
#[test]
fn a_validly_signed_answer_to_another_request_is_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let resp = signed(&signer, 10)?;
    anyhow::ensure!(
        verify_probe_response(&resp, signer.address(), &domain(), [0x22u8; 32], TS).is_err(),
        "a mismatched hash must be rejected"
    );
    anyhow::ensure!(
        verify_probe_response(&resp, signer.address(), &domain(), HASH, TS + 1).is_err(),
        "an unechoed timestamp must be rejected"
    );
    Ok(())
}

/// An exchange that answers after `after` with exchange RTT `rtt`.
async fn answers(after: Duration, rtt: Duration) -> anyhow::Result<((), (), Duration)> {
    tokio::time::sleep(after).await;
    Ok(((), (), rtt))
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// The node probes its upstream with a tight budget (decdn-node
/// `PROBE_TIMEOUT`, 500 ms). A cold dial that answers on the relay must keep
/// its answer when hole punching does not select a direct path before the
/// budget runs out.
#[tokio::test(start_paused = true)]
async fn an_answered_probe_survives_a_direct_path_that_never_comes() -> anyhow::Result<()> {
    let budget = ms(500);
    let started = tokio::time::Instant::now();
    let ((), (), rtt) = exchange_within(budget, answers(ms(300), ms(120)), async |()| {
        std::future::pending().await
    })
    .await?;
    let want = PathRtt {
        rtt: ms(120),
        direct: false,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    anyhow::ensure!(
        started.elapsed() == budget,
        "expected the direct-path wait to end at the budget, took {:?}",
        started.elapsed()
    );
    Ok(())
}

/// A caller with a long budget, such as the CLI's 5 s probe, still waits
/// at most `DIRECT_PATH_GRACE` for a direct path.
#[tokio::test(start_paused = true)]
async fn the_direct_path_wait_stops_at_the_grace_under_a_long_budget() -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();
    let ((), (), rtt) = exchange_within(ms(5000), answers(ms(300), ms(120)), async |()| {
        std::future::pending().await
    })
    .await?;
    let want = PathRtt {
        rtt: ms(120),
        direct: false,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    let want_elapsed = ms(300) + DIRECT_PATH_GRACE;
    anyhow::ensure!(
        started.elapsed() == want_elapsed,
        "expected the wait to stop at {want_elapsed:?}, took {:?}",
        started.elapsed()
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_lower_direct_path_rtt_replaces_the_exchange_rtt() -> anyhow::Result<()> {
    let ((), (), rtt) = exchange_within(ms(5000), answers(ms(300), ms(250)), async |()| {
        tokio::time::sleep(ms(100)).await;
        Some(ms(40))
    })
    .await?;
    let want = PathRtt {
        rtt: ms(40),
        direct: true,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_higher_direct_path_rtt_keeps_the_exchange_rtt() -> anyhow::Result<()> {
    let ((), (), rtt) = exchange_within(ms(5000), answers(ms(300), ms(50)), async |()| {
        tokio::time::sleep(ms(10)).await;
        Some(ms(200))
    })
    .await?;
    let want = PathRtt {
        rtt: ms(50),
        direct: true,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    Ok(())
}

/// An exchange that answers exactly at the deadline leaves no time to wait.
/// The probe still keeps its answer, and a direct path that is already
/// selected still counts, because the zero-length wait polls once.
#[tokio::test(start_paused = true)]
async fn an_answer_at_the_deadline_keeps_its_answer() -> anyhow::Result<()> {
    let ((), (), rtt) =
        exchange_within(ms(500), answers(ms(500), ms(120)), async |()| Some(ms(40))).await?;
    let want = PathRtt {
        rtt: ms(40),
        direct: true,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    let ((), (), rtt) = exchange_within(ms(500), answers(ms(500), ms(120)), async |()| {
        std::future::pending().await
    })
    .await?;
    let want = PathRtt {
        rtt: ms(120),
        direct: false,
    };
    anyhow::ensure!(rtt == want, "expected {want:?}, got {rtt:?}");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_exchange_past_the_budget_times_out() -> anyhow::Result<()> {
    let err = exchange_within(ms(500), answers(ms(600), ms(1)), async |()| Some(ms(1)))
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("an exchange past the budget must fail"))?;
    anyhow::ensure!(
        err.to_string().contains("probe timed out after 500 ms"),
        "expected the timeout, got: {err}"
    );
    Ok(())
}

/// Callers tell a peer that sheds the probe from an unreachable one by
/// downcasting the error, so the helper must pass an exchange error on
/// unwrapped.
#[tokio::test(start_paused = true)]
async fn an_exchange_error_keeps_the_rate_limit_sentinel() -> anyhow::Result<()> {
    let err = exchange_within(
        ms(500),
        async {
            Err::<((), (), Duration), _>(anyhow::Error::new(crate::UpstreamRateLimited {
                label: None,
            }))
        },
        async |()| Some(ms(1)),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("an exchange error must fail the probe"))?;
    anyhow::ensure!(
        err.downcast_ref::<crate::UpstreamRateLimited>().is_some(),
        "expected the rate-limit sentinel, got: {err}"
    );
    Ok(())
}
