//! Reusable `cdn/probe/v1` client.
//!
//! [`probe_once`](crate::probe::probe_once) performs one probe round trip against a remote node
//! over a plain QUIC handshake. The transport lives here rather than inline
//! in the `decdn probe` command so the node's cache-miss probe-collection loop
//! (DHT `FIND_VALUE` → parallel probes, [ADR 001]) can reuse it; that loop
//! runs in `decdn_node::node_origin`. The reused mechanism is
//! **transport-only**:
//! echoed-field correlation (ADR 005) and `slash_sig` validation (ADR 014
//! §1) are the caller's responsibility, not performed here — see
//! [`probe_once`](crate::probe::probe_once).
//!
//! [ADR 001]: ../../../adr/001-network.md

use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, Signature};
use alloy::sol_types::Eip712Domain;
use decdn_incentive::ProbeSlashData;
use decdn_protocol::{
    ALPN_PROBE, ProbeMessage, ProbeResponseExt, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    parse_probe_response_ext, read_frame, write_frame,
};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

use crate::rate_limited::transport_error;

/// Run one probe round trip and return the decoded response together with
/// the measured round-trip time in milliseconds.
///
/// `timeout` bounds the whole connect+request+response exchange.
///
/// The decoded [`ProbeResponse`] is returned without echoed-field
/// correlation or `slash_sig` validation: the requester-side obligations —
/// echoed `hash`/`timestamp_us` correlation (ADR 005) and the mandatory
/// `slash_sig` shape check (`ProbeResponse::validate`, ADR 014 §1) — are the
/// caller's, so a future node-side probe-collection loop can apply its own
/// policy on the same transport. The only check performed internally is the
/// protocol-level one that the server sent a `Response` (not a `Request`)
/// variant.
///
/// # Errors
///
/// A node that sheds the probe at the transport with `APP_ERR_RATE_LIMITED`
/// surfaces as the [`UpstreamRateLimited`](crate::UpstreamRateLimited) sentinel
/// (recoverable with `downcast_ref` through any added context), so a caller can
/// tell a peer refusing work from a peer that could not be reached. Every other
/// failure is a plain message naming the stage.
pub async fn probe_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt, f64)> {
    let started = Instant::now();

    let (conn, resp) = tokio::time::timeout(timeout, async {
        let conn = endpoint
            .connect(target, ALPN_PROBE)
            .await
            .map_err(|e| transport_error("connect failed", e))?;
        let resp = exchange(&conn, hash, timestamp_us).await?;
        Ok::<_, anyhow::Error>((conn, resp))
    })
    .await
    .map_err(|_| anyhow::anyhow!("probe timed out after {} ms", timeout.as_millis()))??;

    let rtt_ms = started.elapsed().as_secs_f64() * 1000.0;
    conn.close(0u32.into(), b"probe-done");

    Ok((resp.0, resp.1, rtt_ms))
}

/// Verify a [`ProbeResponse`] the way a requester must before acting on it
/// (ADR 005 §Correlation, ADR 014 §1).
///
/// Three checks, in the order a cheap one should precede an expensive one:
/// the value invariants (`ProbeResponse::validate` — non-zero rate, `slash_sig`
/// length), the echoed-field correlation that ties the answer to *this* request,
/// and finally the secp256k1 recovery of `slash_sig` to `expected_signer`.
///
/// The recovery is the load-bearing one and the reason this exists. `slash_sig`
/// is what makes a quoted rate non-repudiable: a node that quotes `R₁` on probe
/// and then charges `R₂ > R₁` within the 30-second window is slashable on the
/// strength of these two signed messages alone. A response whose signature is
/// never recovered is not evidence of anything — it is an unattributed claim, so
/// a node could win selection on a rate it never committed to and face nothing
/// when it fails to honour it.
///
/// Requester-local policy, not an attributable fault: a caller that gets an
/// `Err` here MUST drop the response and move on, exactly as it would for a
/// timeout, and MUST NOT feed it to reputation. The signature is the only thing
/// binding a response to a peer, so a failed verification cannot attribute
/// anything to that peer — an unverified message is precisely a message with no
/// established author.
///
/// # Errors
///
/// A failed invariant, a mismatched echoed `hash`/`timestamp_us`, an unparseable
/// signature, or a signature that recovers to any address but `expected_signer`.
pub fn verify_probe_response(
    resp: &ProbeResponse,
    expected_signer: Address,
    slash_domain: &Eip712Domain,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<()> {
    resp.validate()
        .map_err(|e| anyhow::anyhow!("invalid probe response: {e}"))?;
    if resp.body.hash != hash {
        anyhow::bail!("probe response hash does not match the request");
    }
    if resp.body.timestamp_us != timestamp_us {
        anyhow::bail!("probe response timestamp_us not echoed");
    }
    let sig = Signature::try_from(resp.slash_sig.as_slice())
        .map_err(|e| anyhow::anyhow!("slash_sig parse: {e}"))?;
    ProbeSlashData {
        hash: B256::from(resp.body.hash),
        has_blob: resp.body.has_blob,
        rate_per_mb: resp.body.rate_per_mb,
        timestamp_us: resp.body.timestamp_us,
    }
    .verify_signer(&sig, expected_signer, slash_domain)
    .map_err(|e| anyhow::anyhow!("probe slash_sig verification failed: {e}"))?;
    Ok(())
}

/// Open a fresh bidirectional stream, send the request, read the response.
async fn exchange(
    conn: &Connection,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
    let (mut send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| transport_error("open_bi failed", e))?;
    write_request(&mut send, hash, timestamp_us).await?;
    read_response(recv).await
}

/// Frame and write a `ProbeMessage::Request`, then finish the send half.
async fn write_request(
    send: &mut SendStream,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<()> {
    let payload = encode_message(&ProbeMessage::Request(ProbeRequest { hash, timestamp_us }))
        .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(send, &payload)
        .await
        .map_err(|e| transport_error("write request", e))?;
    send.finish()
        .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;
    Ok(())
}

/// Read and decode one framed `ProbeMessage::Response`. Echoed-field
/// correlation (`hash`/`timestamp_us`) and the mandatory `slash_sig` shape
/// check are the caller's obligation (ADR 005 / ADR 014 §1), not enforced
/// here — see [`probe_once`].
async fn read_response(mut recv: RecvStream) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| transport_error("read response", e))?;
    let (msg, rest) = decode_message::<ProbeMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;
    match msg {
        ProbeMessage::Response(r) => {
            // Trailing bytes are the Tier-1 extension, not junk: parse what this
            // build knows and skip the rest (ADR 013 §Tier 1).
            let ext = parse_probe_response_ext(rest)
                .map_err(|e| anyhow::anyhow!("decode response ext: {e}"))?;
            Ok((r, ext))
        }
        ProbeMessage::Request(_) => {
            anyhow::bail!("unexpected ProbeMessage::Request from server");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::verify_probe_response;
    use alloy::primitives::Address;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::Eip712Domain;
    use decdn_incentive::{ProbeSlashData, slash_judge_domain};
    use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};

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
}
