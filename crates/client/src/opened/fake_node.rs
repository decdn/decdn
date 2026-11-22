//! A loopback `cdn/client/v1` node for tests: it answers every stream open
//! with a signed answer, and counts the opens and how each stream ended.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, Bytes};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::Eip712Domain;
use decdn_incentive::stream_sig::StreamSigData;
use decdn_protocol::client::{
    ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseBody, StreamResponseExt,
};
use decdn_protocol::{ALPN_CLIENT, Coverage, SizeProof};
use iroh::endpoint::{Connection, ReadError, ReadToEndError, presets};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use tokio_util::task::AbortOnDropHandle;

use crate::discovery::NodeCandidate;

/// How a [`FakeNode`] answers every open.
#[derive(Clone)]
pub(crate) enum Answer {
    /// `ok`, with a size proof and full coverage of `blob`, at `rate`.
    Serve {
        /// The blob the node holds.
        blob: Vec<u8>,
        /// The per-MiB rate it signs.
        rate: u64,
    },
    /// A signed refusal of `error`, at `rate`, with empty coverage.
    Refuse {
        /// The refusal class.
        error: StreamError,
        /// The per-MiB rate it signs.
        rate: u64,
    },
    /// A signed `NotFound` with `coverage`: a partial holder that does not
    /// cover the span.
    Partial {
        /// The blocks the node holds.
        coverage: Coverage,
        /// The per-MiB rate it signs.
        rate: u64,
    },
    /// `ok` as [`Answer::Serve`], at `first` for the node's first open and
    /// at `then` for every later one: a node that raises its rate.
    Reprice {
        /// The blob the node holds.
        blob: Vec<u8>,
        /// The per-MiB rate of its first answer.
        first: u64,
        /// The per-MiB rate of every later answer.
        then: u64,
    },
    /// `ok` with no coverage: an answer whose coverage disagrees with it.
    Inconsistent {
        /// The blob the node holds.
        blob: Vec<u8>,
    },
    /// A signed `Declined` of the request's payment pool, naming the pools
    /// the node accepts instead.
    DeclinePool {
        /// The pools the node accepts.
        accepted: Vec<decdn_protocol::PoolAccount>,
        /// The per-MiB rate it signs.
        rate: u64,
    },
}

/// What a [`FakeNode`] saw.
#[derive(Default)]
pub(crate) struct Seen {
    /// Streams the node answered.
    pub(crate) opens: AtomicUsize,
    /// Streams the payer reset after the answer, with no frame sent.
    pub(crate) resets: AtomicUsize,
    /// Streams on which a frame followed the answer.
    pub(crate) paid: AtomicUsize,
    /// `ok` streams that ended after the answer, however they ended.
    pub(crate) ended: AtomicUsize,
    /// Every request, with its extension.
    pub(crate) requests: Mutex<Vec<(StreamRequest, StreamRequestExt)>>,
    /// Every connection the node accepted.
    pub(crate) conns: Mutex<Vec<Connection>>,
}

impl Seen {
    /// The count `of` reads, now.
    pub(crate) fn count(of: &AtomicUsize) -> usize {
        of.load(Ordering::SeqCst)
    }
}

/// A loopback node on its own endpoint, answering `cdn/client/v1` opens.
pub(crate) struct FakeNode {
    pub(crate) endpoint: Endpoint,
    pub(crate) addr: EndpointAddr,
    /// The operator key the node signs its answers with.
    pub(crate) operator: PrivateKeySigner,
    pub(crate) seen: Arc<Seen>,
    _task: AbortOnDropHandle<()>,
}

impl FakeNode {
    /// Spawn a node that answers every open with `answer`, signing under
    /// `domain`.
    pub(crate) async fn spawn(answer: Answer, domain: Eip712Domain) -> anyhow::Result<Self> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN_CLIENT.to_vec()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))?
            .bind()
            .await?;
        let socket = endpoint
            .bound_sockets()
            .into_iter()
            .find(std::net::SocketAddr::is_ipv4)
            .ok_or_else(|| anyhow::anyhow!("an IPv4 bound socket"))?;
        let addr = EndpointAddr::new(endpoint.id()).with_ip_addr(socket);
        let operator = PrivateKeySigner::random();
        let seen = Arc::new(Seen::default());
        let task = AbortOnDropHandle::new(tokio::spawn(serve(
            endpoint.clone(),
            answer,
            operator.clone(),
            domain,
            Arc::clone(&seen),
        )));
        Ok(Self {
            endpoint,
            addr,
            operator,
            seen,
            _task: task,
        })
    }

    /// The node as a discovery candidate: its id, its operator address, and
    /// its loopback socket as a registry dial address.
    pub(crate) fn candidate(&self) -> anyhow::Result<NodeCandidate> {
        let socket = self
            .addr
            .ip_addrs()
            .next()
            .ok_or_else(|| anyhow::anyhow!("the node has a socket"))?;
        let packed = decdn_incentive::node_register::pack_multiaddrs(&[format!(
            "/ip4/{}/udp/{}/quic-v1",
            socket.ip(),
            socket.port()
        )])?;
        Ok(NodeCandidate {
            node_id: self.endpoint.id(),
            eth_address: self.operator.address(),
            region_hint: None,
            multiaddrs: Bytes::from(packed),
        })
    }

    /// The operator address the node signs under.
    pub(crate) fn operator_address(&self) -> Address {
        self.operator.address()
    }

    /// Whether every connection the node accepted is still open.
    pub(crate) fn connections_open(&self) -> bool {
        self.seen
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|c| c.close_reason().is_none())
    }
}

/// Accept connections and answer every stream on them.
async fn serve(
    endpoint: Endpoint,
    answer: Answer,
    operator: PrivateKeySigner,
    domain: Eip712Domain,
    seen: Arc<Seen>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connecting) = incoming.accept() else {
            continue;
        };
        let Ok(conn) = connecting.await else {
            continue;
        };
        seen.conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(conn.clone());
        let (answer, operator, domain, seen) = (
            answer.clone(),
            operator.clone(),
            domain.clone(),
            Arc::clone(&seen),
        );
        tokio::spawn(async move {
            while let Ok((send, recv)) = conn.accept_bi().await {
                tokio::spawn(answer_stream(
                    send,
                    recv,
                    answer.clone(),
                    operator.clone(),
                    domain.clone(),
                    Arc::clone(&seen),
                ));
            }
        });
    }
}

/// Answer one stream, then wait for how it ends.
async fn answer_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    answer: Answer,
    operator: PrivateKeySigner,
    domain: Eip712Domain,
    seen: Arc<Seen>,
) -> anyhow::Result<()> {
    let frame = decdn_protocol::read_frame(&mut recv, 4096).await?;
    let (msg, tail) = decdn_protocol::decode_message::<ClientMessage>(&frame)?;
    let ClientMessage::StreamRequest(req) = msg else {
        anyhow::bail!("expected a request");
    };
    let ext = decdn_protocol::parse_stream_request_ext(tail)?;
    let nth = {
        let mut requests = seen
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        requests.push((req.clone(), ext));
        requests.len()
    };
    let (ok, rate, resp_ext) = match answer {
        Answer::Serve { blob, rate } => (true, rate, served(&blob, true)?),
        Answer::Reprice { blob, first, then } => {
            let rate = if nth == 1 { first } else { then };
            (true, rate, served(&blob, true)?)
        }
        Answer::Refuse { error, rate } => (
            false,
            rate,
            StreamResponseExt::refused(error, Coverage::empty()),
        ),
        Answer::Partial { coverage, rate } => (
            false,
            rate,
            StreamResponseExt::refused(StreamError::NotFound, coverage),
        ),
        Answer::Inconsistent { blob } => (true, 10, served(&blob, false)?),
        Answer::DeclinePool { accepted, rate } => {
            (false, rate, StreamResponseExt::pool_declined(accepted))
        }
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok,
        rate_per_mb: rate,
        timestamp_us: req.timestamp_us,
    };
    let sig = StreamSigData::from_response_body(&body).sign(&operator, &domain)?;
    let resp = StreamResponse {
        body,
        operator_sig: sig.as_bytes().to_vec(),
    };
    let payload = decdn_protocol::client::encode_stream_response(&resp, Some(&resp_ext))?;
    decdn_protocol::write_frame(&mut send, &payload).await?;
    seen.opens.fetch_add(1, Ordering::SeqCst);
    if !ok {
        let _ = send.finish();
        return Ok(());
    }
    match recv.read_to_end(1 << 20).await {
        Err(ReadToEndError::Read(ReadError::Reset(_))) => {
            seen.resets.fetch_add(1, Ordering::SeqCst);
        }
        Ok(bytes) if !bytes.is_empty() => {
            seen.paid.fetch_add(1, Ordering::SeqCst);
        }
        _ => {}
    }
    seen.ended.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// A served extension for `blob`: its size proof, with full coverage when
/// `covered`, else none.
fn served(blob: &[u8], covered: bool) -> anyhow::Result<StreamResponseExt> {
    let size = u64::try_from(blob.len())?;
    let proof = decdn_bao_range::encode_outboard(blob, size)?
        .size_proof
        .to_vec();
    let coverage = if covered {
        Coverage::full(decdn_protocol::num_blocks(size))
    } else {
        Coverage::empty()
    };
    Ok(StreamResponseExt::served(
        SizeProof::new(size, proof)?,
        None,
        coverage,
    ))
}
