//! Pull receipts (#2599): the signed messages one pull already exchanged, kept
//! so that anyone can check offline what the pull paid for.
//!
//! A [`PullReceipt`] holds, per paid lane, the first accepting
//! [`StreamResponse`] the node signed for each hash it served, the
//! payer-signed voucher the lane's cumulative claim rests on, and the deepest
//! hash-chain preimage released under it. [`PullReceipt::verify`] re-checks
//! every signature against the deployment the receipt names, with no network
//! access. The rest of the receipt — the namespace, the wall-clock window, the
//! watermark before the pull, and the size of an entry no response covers — is
//! the client's own statement and proves nothing
//! (`adr/appendix-pull-receipts.md`).
//!
//! Collection is opt-in. A [`ReceiptEvidence`] collector passed to the lane
//! builders attaches a slot to every lane ledger it sees; a ledger built
//! without one keeps nothing, so a node's long-lived upstream lanes do not
//! grow with every response they read.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use alloy::primitives::{Address, B256, Bytes};
use alloy::signers::Signature;
use alloy::sol_types::Eip712Domain;
use decdn_incentive::chain::CHUNK_BYTES;
use decdn_incentive::stream_sig::StreamSlashData;
use decdn_incentive::{
    LaneKey, slash_judge_domain, verify_forward, voucher_domain, wire_voucher_to_signed,
};
use decdn_protocol::client::{StreamResponse, StreamResponseBody, Voucher as WireVoucher};
use serde::{Deserialize, Serialize};

use crate::ledger::{PoolLedger, ProvedFrontier};
use crate::session::ChainConfig;

/// How many signed vouchers a lane keeps. A rejection rewinds the lane by one
/// voucher, so an anchor this pull signed is one of the last few it signed.
/// An anchor a reseed or rebase took from a node's watermark has no signature
/// of this pull at all.
const SIGNED_HISTORY: usize = 4;

/// One lane's receipt evidence: the vouchers it signed most recently and the
/// first accepting response each hash drew. Attached to a [`PoolLedger`] by a
/// [`ReceiptEvidence`] collector.
#[derive(Debug, Default)]
pub(crate) struct LaneEvidence {
    inner: Mutex<LaneEvidenceInner>,
}

#[derive(Debug, Default)]
struct LaneEvidenceInner {
    signed: VecDeque<WireVoucher>,
    responses: Vec<StreamResponse>,
}

impl LaneEvidence {
    fn inner(&self) -> std::sync::MutexGuard<'_, LaneEvidenceInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keep `voucher`, the lane's newest signature, in place of the oldest one
    /// kept.
    pub(crate) fn note_voucher(&self, voucher: &WireVoucher) {
        let mut inner = self.inner();
        if inner.signed.len() >= SIGNED_HISTORY {
            inner.signed.pop_front();
        }
        inner.signed.push_back(voucher.clone());
    }

    /// Keep `response` if it accepts and is the first for its hash on this lane.
    pub(crate) fn note_response(&self, response: &StreamResponse) {
        if !response.body.ok {
            return;
        }
        let mut inner = self.inner();
        if inner
            .responses
            .iter()
            .all(|kept| kept.body.hash != response.body.hash)
        {
            inner.responses.push(response.clone());
        }
    }

    /// The proof of `frontier`'s anchor: the newest kept voucher that signs
    /// it, with the frontier's reveal when a voucher on the reveal's chain
    /// signs it. When only a voucher on another chain signs the anchor, the
    /// proof drops the reveal and claims the anchor alone, which is less than
    /// the lane holds and never more. `None` when no kept voucher signs the
    /// anchor, or the claim does not fit the wire width.
    fn proof_for(&self, frontier: ProvedFrontier) -> Option<ReceiptProof> {
        let amount = u64::try_from(frontier.anchor.amount).ok()?;
        let bytes = u64::try_from(frontier.anchor.bytes).ok()?;
        let inner = self.inner();
        let mut signing = inner
            .signed
            .iter()
            .rev()
            .filter(|v| v.amount == amount && v.bytes_delivered == bytes);
        let (voucher, reveal) = match frontier.reveal {
            Some(reveal) => {
                let root: [u8; 32] = reveal.chain_root.into();
                match signing.clone().find(|v| v.chain_root == root) {
                    Some(v) => (
                        v,
                        Some(ReceiptReveal {
                            preimage: reveal.preimage,
                            index: reveal.index,
                        }),
                    ),
                    None => (signing.next()?, None),
                }
            }
            None => (signing.next()?, None),
        };
        let voucher = ReceiptVoucher::from(voucher);
        let claim = voucher.claim(reveal.map_or(0, |r| r.index))?;
        Some(ReceiptProof {
            voucher,
            reveal,
            claim,
        })
    }

    fn responses(&self) -> Vec<StreamResponse> {
        self.inner().responses.clone()
    }
}

/// A pull's receipt collector. Pass one to the lane builders
/// ([`crate::lanes::LaneDeps::evidence`], or the bundle puller's builder) and
/// read [`Self::receipt`] once the pull ends.
#[derive(Debug, Default)]
pub struct ReceiptEvidence {
    lanes: Mutex<Vec<CollectedLane>>,
    /// The deployment the first attached lane pays on. Every lane of one pull
    /// pays on the same one.
    chain: Mutex<Option<ReceiptChain>>,
    /// Each blob the pull landed and verified, in landing order.
    landed: Mutex<Vec<ReceiptEntry>>,
}

#[derive(Debug, Clone)]
struct CollectedLane {
    key: LaneKey,
    ledger: Arc<PoolLedger>,
    evidence: Arc<LaneEvidence>,
}

impl ReceiptEvidence {
    /// An empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect for `lane` on `chain`, paid through `ledger`. A ledger already
    /// collected for keeps its first entry.
    pub(crate) fn attach(&self, chain: &ChainConfig, lane: LaneKey, ledger: &Arc<PoolLedger>) {
        self.chain
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(ReceiptChain {
                chain_id: chain.chain_id,
                payment_pool: chain.payment_pool,
                slash_judge: chain.slash_judge,
            });
        let evidence = ledger.attach_evidence();
        let mut lanes = self.lanes.lock().unwrap_or_else(PoisonError::into_inner);
        if lanes.iter().any(|l| Arc::ptr_eq(&l.ledger, ledger)) {
            return;
        }
        lanes.push(CollectedLane {
            key: lane,
            ledger: Arc::clone(ledger),
            evidence,
        });
    }

    /// The receipt lanes of every lane that paid during the pull.
    ///
    /// Each lane's prior watermark is its ledger's baseline: the seed it was
    /// built from, moved by every reseed and rebase, so a jump to a node's
    /// watermark does not count as this pull's payment. A lane whose anchor
    /// did not move past its baseline, and that released no preimage, paid
    /// nothing and is left out; so is a lane whose baseline does not fit the
    /// wire width, which no voucher can have been signed over.
    ///
    /// A lane that paid but kept no voucher signing its anchor (the voucher
    /// left the kept history) appears with no proof. Two ledgers on one lane
    /// key merge: the higher claim's proof wins, the responses join, and the
    /// merged lane pays the sum of what each ledger paid itself.
    pub async fn lanes(&self) -> Vec<ReceiptLane> {
        let collected = self
            .lanes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut out: Vec<ReceiptLane> = Vec::new();
        for lane in collected {
            let frontier = lane.ledger.proved_frontier().await;
            if frontier.anchor.amount <= frontier.baseline.amount && frontier.reveal.is_none() {
                continue;
            }
            let Ok(prior) = ReceiptAmount::try_from(frontier.baseline) else {
                continue;
            };
            let responses = lane
                .evidence
                .responses()
                .iter()
                .map(ReceiptResponse::from)
                .collect();
            merge_lane(
                &mut out,
                ReceiptLane {
                    pool_id: lane.key.pool_id,
                    signer: lane.key.signer,
                    provider: lane.key.provider,
                    responses,
                    proof: lane.evidence.proof_for(frontier),
                    prior,
                    settlement: None,
                },
            );
        }
        out
    }

    /// Record that the pull landed and verified `size` bytes of `hash`. A hash
    /// landed twice counts once.
    pub fn note_landed(&self, hash: [u8; 32], size: u64) {
        let hash = B256::from(hash);
        let mut landed = self.landed.lock().unwrap_or_else(PoisonError::into_inner);
        if landed.iter().all(|e| e.hash != hash) {
            landed.push(ReceiptEntry { hash, size });
        }
    }

    /// The receipt of a pull of `hash` that ran over `window`: every lane that
    /// paid, every blob [`Self::note_landed`] recorded, and as `unlanded` every
    /// hash a node accepted that did not land, the requested one included. `None` when no lane was built, so nothing
    /// was paid and no deployment is known.
    pub async fn receipt(
        &self,
        hash: [u8; 32],
        kind: ContentKind,
        namespace: Option<u64>,
        complete: bool,
        window: ReceiptWindow,
    ) -> Option<PullReceipt> {
        let chain = (*self.chain.lock().unwrap_or_else(PoisonError::into_inner))?;
        let entries = self
            .landed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let lanes = self.lanes().await;
        let hash = B256::from(hash);
        let mut unlanded: Vec<B256> = Vec::new();
        for r in lanes.iter().flat_map(|l| &l.responses) {
            if entries.iter().all(|e| e.hash != r.hash) && !unlanded.contains(&r.hash) {
                unlanded.push(r.hash);
            }
        }
        let content = ReceiptContent {
            hash,
            kind,
            namespace,
            entries,
            unlanded,
            complete,
        };
        Some(PullReceipt::new(chain, content, window, lanes))
    }
}

/// Add `lane` to `out`, merging it into a lane already there under the same
/// key: the higher claim's proof wins, responses join, and the prior is set
/// so that the merged lane pays what the two ledgers paid between them.
///
/// Each ledger's prior is its own baseline, which already leaves out the
/// jumps that ledger healed across. Taking the lower prior under the higher
/// claim would count a jump the second ledger took as payment, so the prior
/// is the winning claim less the sum of both ledgers' own payments.
fn merge_lane(out: &mut Vec<ReceiptLane>, lane: ReceiptLane) {
    let Some(kept) = out.iter_mut().find(|l| l.key() == lane.key()) else {
        out.push(lane);
        return;
    };
    // A lane with no proof ranks below every lane with one.
    let claim_of = |l: &ReceiptLane| l.proof.as_ref().map(|p| p.claim.amount);
    let (kept_paid, lane_paid) = (kept.paid(), lane.paid());
    let paid = ReceiptAmount {
        amount: kept_paid.amount.saturating_add(lane_paid.amount),
        bytes: kept_paid.bytes.saturating_add(lane_paid.bytes),
    };
    let lowest = if lane.prior.amount < kept.prior.amount {
        lane.prior
    } else {
        kept.prior
    };
    let mut responses = std::mem::take(&mut kept.responses);
    for r in &lane.responses {
        if responses.iter().all(|k| k.hash != r.hash) {
            responses.push(r.clone());
        }
    }
    if claim_of(&lane) > claim_of(kept) {
        *kept = lane;
    }
    kept.prior = kept.proof.as_ref().map_or(lowest, |proof| ReceiptAmount {
        amount: proof.claim.amount.saturating_sub(paid.amount),
        bytes: proof.claim.bytes.saturating_sub(paid.bytes),
    });
    kept.responses = responses;
}

/// The receipt format. Serialized as `"decdn-receipt/1"`; a reader rejects any
/// other value when it parses the receipt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptFormat {
    /// `decdn-receipt/1`.
    #[default]
    #[serde(rename = "decdn-receipt/1")]
    V1,
}

/// A pull receipt: what one pull paid for, and the signatures that prove it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullReceipt {
    /// The receipt format.
    pub format: ReceiptFormat,
    /// The deployment every signature in the receipt is bound to.
    pub chain: ReceiptChain,
    /// What the pull fetched.
    pub content: ReceiptContent,
    /// When the pull ran. Client-stated.
    pub window: ReceiptWindow,
    /// One entry per `(pool, signer, provider)` lane that paid.
    pub lanes: Vec<ReceiptLane>,
}

/// The deployment a receipt's signatures are bound to: the chain and the two
/// contracts whose EIP-712 domains they sign under.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptChain {
    /// The chain id both domains carry.
    pub chain_id: u64,
    /// The `PaymentPool` contract, the vouchers' `verifyingContract`.
    pub payment_pool: Address,
    /// The `SlashJudge` contract, the stream responses' `verifyingContract`.
    pub slash_judge: Address,
}

/// What a pull fetched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptContent {
    /// The hash the pull was asked for: the blob, or the bundle's manifest.
    pub hash: B256,
    /// Whether `hash` names a blob or a bundle.
    pub kind: ContentKind,
    /// The namespace the request routed under, if any. Client-stated: no node
    /// signs it.
    pub namespace: Option<u64>,
    /// The whole blobs the pull landed and verified, a bundle's manifest
    /// included when the pull fetched it. A blob pull lists its one blob; a
    /// blob pull that failed, or that fetched a byte window, lists none. Sizes
    /// are client-stated; a response for the same hash signs its size.
    pub entries: Vec<ReceiptEntry>,
    /// The hashes a node accepted on a lane of the pull that did not land: a
    /// blob, a manifest or entries that a failed or interrupted pull had
    /// started. Empty when `complete`.
    pub unlanded: Vec<B256>,
    /// `false` when the pull ended with an error: a blob that did not land, or
    /// a bundle with entries it did not land.
    pub complete: bool,
}

/// Whether a receipt's content hash names a blob or a bundle manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    /// One blob.
    Blob,
    /// A bundle manifest and the entries it lists.
    Bundle,
}

/// One blob a pull landed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptEntry {
    /// The blob's BLAKE3 hash.
    pub hash: B256,
    /// Its size in bytes.
    pub size: u64,
}

/// When a pull ran, as RFC 3339 UTC timestamps. Client-stated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptWindow {
    /// When the pull started.
    pub started: String,
    /// When the pull ended.
    pub finished: String,
}

/// One paid lane: a node, the key that paid it, and the pool it drew on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptLane {
    /// The pool the lane's vouchers draw on.
    pub pool_id: B256,
    /// The key the lane's vouchers sign under.
    pub signer: Address,
    /// The node's operator address: the vouchers' `provider` and the key its
    /// stream responses sign under.
    pub provider: Address,
    /// The first accepting response the node signed for each hash it served.
    pub responses: Vec<ReceiptResponse>,
    /// The voucher the lane's claim rests on, with its reveal and the claim
    /// they prove, or `None` when the pull kept no signature for the lane's
    /// anchor.
    pub proof: Option<ReceiptProof>,
    /// The lane's cumulative before the pull. Client-stated.
    pub prior: ReceiptAmount,
    /// The on-chain redemption that covers the claim, or the highest one short
    /// of it, once `decdn receipt settle` finds one.
    pub settlement: Option<ReceiptSettlement>,
}

impl ReceiptLane {
    /// The lane's `(pool, signer, provider)` key.
    #[must_use]
    pub const fn key(&self) -> LaneKey {
        LaneKey {
            pool_id: self.pool_id,
            signer: self.signer,
            provider: self.provider,
        }
    }

    /// What this pull paid on the lane: the claim past the prior watermark.
    /// Zero without a proof.
    #[must_use]
    pub fn paid(&self) -> ReceiptAmount {
        self.proof
            .as_ref()
            .map_or_else(ReceiptAmount::default, |proof| ReceiptAmount {
                amount: proof.claim.amount.saturating_sub(self.prior.amount),
                bytes: proof.claim.bytes.saturating_sub(self.prior.bytes),
            })
    }

    /// Whether the recorded redemption reaches the lane's claim. `false`
    /// without a proof or a settlement.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        match (&self.proof, self.settlement) {
            (Some(proof), Some(settlement)) => settlement.new_paid_cumulative >= proof.claim.amount,
            _ => false,
        }
    }
}

/// What a lane's claim rests on: the voucher that signs its anchor, the
/// deepest reveal under that voucher's chain, and the claim the two prove.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProof {
    /// The payer-signed voucher.
    pub voucher: ReceiptVoucher,
    /// The deepest preimage released under `voucher`'s chain, if any.
    pub reveal: Option<ReceiptReveal>,
    /// `voucher.claim(reveal.index)`: stored for readers that do not compute
    /// the redemption formula; [`PullReceipt::verify`] computes it again.
    pub claim: ReceiptAmount,
}

/// An `(amount, bytes)` pair on a lane: a cumulative position, or the
/// difference between two.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptAmount {
    /// Token base units (`µUSDC`).
    pub amount: u64,
    /// Bytes.
    pub bytes: u64,
}

impl TryFrom<crate::ledger::Cumulative> for ReceiptAmount {
    type Error = alloy::primitives::ruint::FromUintError<u64>;

    fn try_from(c: crate::ledger::Cumulative) -> Result<Self, Self::Error> {
        Ok(Self {
            amount: u64::try_from(c.amount)?,
            bytes: u64::try_from(c.bytes)?,
        })
    }
}

/// A node's signed answer to one stream request: the [`StreamResponseBody`]
/// fields and the `SlashJudge`-domain signature over them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptResponse {
    /// The hash the response answers for.
    pub hash: B256,
    /// Whether the node agreed to serve.
    pub ok: bool,
    /// The quoted rate, in token base units per MB.
    pub rate_per_mb: u64,
    /// The blob size the node signed.
    pub total_bytes: u64,
    /// The pool the request named.
    pub pool_id: B256,
    /// The requester's timestamp, echoed and signed by the node.
    pub timestamp_us: u64,
    /// The node's 65-byte EIP-712 signature.
    pub slash_sig: Bytes,
}

impl From<&StreamResponse> for ReceiptResponse {
    fn from(r: &StreamResponse) -> Self {
        Self {
            hash: B256::from(r.body.hash),
            ok: r.body.ok,
            rate_per_mb: r.body.rate_per_mb,
            total_bytes: r.body.total_bytes,
            pool_id: B256::from(r.body.pool_id),
            timestamp_us: r.body.timestamp_us,
            slash_sig: Bytes::copy_from_slice(&r.slash_sig),
        }
    }
}

impl ReceiptResponse {
    fn to_wire(&self) -> StreamResponse {
        StreamResponse {
            body: StreamResponseBody {
                hash: self.hash.into(),
                ok: self.ok,
                rate_per_mb: self.rate_per_mb,
                total_bytes: self.total_bytes,
                pool_id: self.pool_id.into(),
                timestamp_us: self.timestamp_us,
            },
            slash_sig: self.slash_sig.to_vec(),
        }
    }
}

/// A payer-signed cumulative voucher, as it went on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptVoucher {
    /// Cumulative amount, in token base units.
    pub amount: u64,
    /// Cumulative bytes.
    pub bytes_delivered: u64,
    /// The hash chain the voucher opens, or zero when it seals.
    pub chain_root: B256,
    /// What one chunk adds over `amount`.
    pub chunk_price: u64,
    /// The payer's 65-byte EIP-712 signature.
    pub signature: Bytes,
}

impl From<&WireVoucher> for ReceiptVoucher {
    fn from(v: &WireVoucher) -> Self {
        Self {
            amount: v.amount,
            bytes_delivered: v.bytes_delivered,
            chain_root: B256::from(v.chain_root),
            chunk_price: v.chunk_price,
            signature: Bytes::copy_from_slice(&v.signature),
        }
    }
}

impl ReceiptVoucher {
    fn to_wire(&self) -> WireVoucher {
        WireVoucher {
            signature: self.signature.to_vec(),
            amount: self.amount,
            bytes_delivered: self.bytes_delivered,
            chain_root: self.chain_root.into(),
            chunk_price: self.chunk_price,
        }
    }

    /// The claim this voucher proves with `index` chunks revealed past it:
    /// `amount + index × chunk_price` over `bytes_delivered + index ×
    /// CHUNK_BYTES`, the redemption formula. `None` past `u64`.
    #[must_use]
    pub fn claim(&self, index: u8) -> Option<ReceiptAmount> {
        let steps = u64::from(index);
        Some(ReceiptAmount {
            amount: self
                .amount
                .checked_add(steps.checked_mul(self.chunk_price)?)?,
            bytes: self
                .bytes_delivered
                .checked_add(steps.checked_mul(CHUNK_BYTES)?)?,
        })
    }
}

/// A released hash-chain preimage and its depth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptReveal {
    /// The released value.
    pub preimage: B256,
    /// Its depth below the chain root.
    pub index: u8,
}

/// The `PoolRedeemed` event that covers, or comes closest to covering, a
/// lane's claim ([`ReceiptLane::is_settled`]). Read from the chain; not signed
/// by anyone in the receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptSettlement {
    /// The redemption transaction.
    pub tx_hash: B256,
    /// The block it landed in.
    pub block: u64,
    /// The lane's paid cumulative after it.
    pub new_paid_cumulative: u64,
}

/// A receipt's stated totals. Arithmetic over the stated fields only: run
/// [`PullReceipt::verify`] to check them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReceiptSummary {
    /// The bytes of every landed entry, a fetched manifest included. The
    /// client states them.
    pub landed_bytes: u64,
    /// What every lane's claim adds past its prior watermark.
    pub paid: ReceiptAmount,
    /// Each node's paid bytes and amount, in lane order, merged per provider.
    pub per_provider: Vec<(Address, ReceiptAmount)>,
    /// The distinct pools the lanes drew on, in lane order.
    pub pools: Vec<B256>,
    /// Node signatures in the receipt.
    pub node_signatures: usize,
    /// Payer signatures in the receipt.
    pub payer_signatures: usize,
    /// Lanes that paid but carry no proof for their claim.
    pub unproved_lanes: usize,
    /// Lanes whose claim a recorded redemption reaches.
    pub settled_lanes: usize,
}

/// Why a receipt fails [`PullReceipt::verify`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReceiptFault {
    /// A response breaks a wire rule (zero rate, bad signature length) or its
    /// signature does not parse.
    ResponseMalformed {
        /// The node.
        provider: Address,
        /// The hash.
        hash: B256,
        /// The rule it breaks.
        reason: String,
    },
    /// A response refuses (`ok = false`); a receipt keeps only acceptances.
    ResponseRefused {
        /// The node.
        provider: Address,
        /// The hash.
        hash: B256,
    },
    /// A response names a pool other than its lane's.
    ResponsePool {
        /// The node.
        provider: Address,
        /// The hash.
        hash: B256,
    },
    /// A response answers for a hash the receipt does not list.
    ResponseHash {
        /// The node.
        provider: Address,
        /// The hash.
        hash: B256,
    },
    /// A response's signature recovers to an address other than the lane's
    /// provider.
    ResponseSignature {
        /// The node.
        provider: Address,
        /// The hash.
        hash: B256,
    },
    /// An entry's size differs from the size a node signed for it.
    EntrySize {
        /// The entry.
        hash: B256,
        /// The size the receipt states.
        stated: u64,
        /// The size the node signed.
        signed: u64,
    },
    /// A complete receipt lists a hash that did not land.
    Unlanded {
        /// The hash.
        hash: B256,
    },
    /// A voucher's signature does not parse.
    VoucherMalformed {
        /// The node the voucher pays.
        provider: Address,
        /// Why it does not parse.
        reason: String,
    },
    /// A voucher's signature recovers to an address other than the lane's
    /// signer.
    VoucherSignature {
        /// The node the voucher pays.
        provider: Address,
    },
    /// A reveal does not hash forward to its voucher's chain root, sits at
    /// index 0, or extends a sealed voucher.
    Reveal {
        /// The node.
        provider: Address,
    },
    /// The stated claim is not what the voucher and reveal prove.
    Claim {
        /// The node.
        provider: Address,
        /// The claim the receipt states.
        stated: ReceiptAmount,
        /// The claim the voucher and reveal prove, or `None` past `u64`.
        proved: Option<ReceiptAmount>,
    },
    /// The claim sits below the prior watermark it is said to extend, on
    /// either axis.
    ClaimBelowPrior {
        /// The node.
        provider: Address,
    },
}

impl std::fmt::Display for ReceiptFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ResponseMalformed {
                provider,
                hash,
                reason,
            } => write!(
                f,
                "node {provider}: response for {hash} is malformed: {reason}"
            ),
            Self::ResponseRefused { provider, hash } => {
                write!(
                    f,
                    "node {provider}: response for {hash} refuses (ok = false)"
                )
            }
            Self::ResponsePool { provider, hash } => {
                write!(f, "node {provider}: response for {hash} names another pool")
            }
            Self::ResponseHash { provider, hash } => {
                write!(
                    f,
                    "node {provider}: response for {hash}, which the receipt does not list"
                )
            }
            Self::ResponseSignature { provider, hash } => write!(
                f,
                "node {provider}: response for {hash} is not signed by the node"
            ),
            Self::EntrySize {
                hash,
                stated,
                signed,
            } => write!(
                f,
                "entry {hash}: size {stated} differs from the {signed} a node signed"
            ),
            Self::Unlanded { hash } => write!(
                f,
                "{hash} is listed as not landed in a receipt marked complete"
            ),
            Self::VoucherMalformed { provider, reason } => {
                write!(f, "node {provider}: voucher is malformed: {reason}")
            }
            Self::VoucherSignature { provider } => write!(
                f,
                "node {provider}: voucher is not signed by the lane's signer"
            ),
            Self::Reveal { provider } => write!(
                f,
                "node {provider}: reveal does not extend the voucher's hash chain"
            ),
            Self::Claim {
                provider,
                stated,
                proved,
            } => match proved {
                Some(proved) => write!(
                    f,
                    "node {provider}: claim {} µUSDC / {} B differs from the {} µUSDC / {} B the \
                     voucher proves",
                    stated.amount, stated.bytes, proved.amount, proved.bytes
                ),
                None => write!(
                    f,
                    "node {provider}: the voucher's claim does not fit 64 bits"
                ),
            },
            Self::ClaimBelowPrior { provider } => {
                write!(f, "node {provider}: claim sits below the prior watermark")
            }
        }
    }
}

impl PullReceipt {
    /// A receipt for a pull of `content` that ran over `window` and paid on
    /// `lanes`.
    #[must_use]
    pub const fn new(
        chain: ReceiptChain,
        content: ReceiptContent,
        window: ReceiptWindow,
        lanes: Vec<ReceiptLane>,
    ) -> Self {
        Self {
            format: ReceiptFormat::V1,
            chain,
            content,
            window,
            lanes,
        }
    }

    /// The receipt's stated totals.
    #[must_use]
    pub fn summary(&self) -> ReceiptSummary {
        let mut s = ReceiptSummary {
            landed_bytes: self
                .content
                .entries
                .iter()
                .fold(0u64, |acc, e| acc.saturating_add(e.size)),
            ..ReceiptSummary::default()
        };
        for lane in &self.lanes {
            let paid = lane.paid();
            s.paid.amount = s.paid.amount.saturating_add(paid.amount);
            s.paid.bytes = s.paid.bytes.saturating_add(paid.bytes);
            match s.per_provider.iter_mut().find(|(p, _)| *p == lane.provider) {
                Some((_, total)) => {
                    total.amount = total.amount.saturating_add(paid.amount);
                    total.bytes = total.bytes.saturating_add(paid.bytes);
                }
                None => s.per_provider.push((lane.provider, paid)),
            }
            if !s.pools.contains(&lane.pool_id) {
                s.pools.push(lane.pool_id);
            }
            s.node_signatures += lane.responses.len();
            if lane.proof.is_some() {
                s.payer_signatures += 1;
            } else {
                s.unproved_lanes += 1;
            }
            if lane.is_settled() {
                s.settled_lanes += 1;
            }
        }
        s
    }

    /// Re-check every signature in the receipt against the deployment it
    /// names, with no network access. An empty result means every signed
    /// statement holds.
    ///
    /// Checks every response (well-formed, accepting, on its lane's pool, for
    /// a listed hash, signed by the lane's provider), that each entry a
    /// response covers has the size the response signed, that a complete
    /// receipt lists no unlanded hash, every voucher (signed by the lane's
    /// signer), every reveal (hashes forward to its voucher's root), and that
    /// each stated claim is the one its voucher and reveal prove and does not
    /// sit below the prior. The namespace, the window, the prior itself and
    /// the size of an entry no response covers are client-stated and are not
    /// checked; nor is a settlement, which `decdn receipt settle` reads from
    /// the chain.
    #[must_use]
    pub fn verify(&self) -> Vec<ReceiptFault> {
        let slash_dom = slash_judge_domain(self.chain.chain_id, self.chain.slash_judge);
        let voucher_dom = voucher_domain(self.chain.chain_id, self.chain.payment_pool);
        let content = &self.content;
        let listed = |hash: &B256| {
            *hash == content.hash
                || content.entries.iter().any(|e| e.hash == *hash)
                || content.unlanded.contains(hash)
        };
        let mut faults = Vec::new();
        if content.complete {
            faults.extend(
                content
                    .unlanded
                    .iter()
                    .map(|&hash| ReceiptFault::Unlanded { hash }),
            );
        }
        for lane in &self.lanes {
            for r in &lane.responses {
                faults.extend(verify_response(lane, r, content, &listed, &slash_dom));
            }
            faults.extend(verify_payment(lane, &voucher_dom));
        }
        faults
    }
}

/// The faults in one of `lane`'s responses.
fn verify_response(
    lane: &ReceiptLane,
    r: &ReceiptResponse,
    content: &ReceiptContent,
    listed: &impl Fn(&B256) -> bool,
    slash_dom: &Eip712Domain,
) -> Vec<ReceiptFault> {
    let (provider, hash) = (lane.provider, r.hash);
    let wire = r.to_wire();
    let malformed = |reason: String| {
        vec![ReceiptFault::ResponseMalformed {
            provider,
            hash,
            reason,
        }]
    };
    if let Err(e) = wire.validate() {
        return malformed(e.to_string());
    }
    let sig = match Signature::from_raw(&wire.slash_sig) {
        Ok(sig) => sig,
        Err(e) => return malformed(format!("slash_sig does not parse: {e}")),
    };
    let mut faults = Vec::new();
    if !r.ok {
        faults.push(ReceiptFault::ResponseRefused { provider, hash });
    }
    if r.pool_id != lane.pool_id {
        faults.push(ReceiptFault::ResponsePool { provider, hash });
    }
    if !listed(&hash) {
        faults.push(ReceiptFault::ResponseHash { provider, hash });
    }
    if StreamSlashData::from_response_body(&wire.body)
        .verify_signer(&sig, provider, slash_dom)
        .is_err()
    {
        faults.push(ReceiptFault::ResponseSignature { provider, hash });
    }
    for e in &content.entries {
        if e.hash == hash && e.size != r.total_bytes {
            faults.push(ReceiptFault::EntrySize {
                hash,
                stated: e.size,
                signed: r.total_bytes,
            });
        }
    }
    faults
}

/// The faults in `lane`'s proof.
fn verify_payment(lane: &ReceiptLane, voucher_dom: &Eip712Domain) -> Vec<ReceiptFault> {
    let provider = lane.provider;
    let Some(proof) = &lane.proof else {
        return Vec::new();
    };
    let mut faults = Vec::new();
    let v = &proof.voucher;
    match wire_voucher_to_signed(&v.to_wire(), lane.pool_id, lane.signer, provider) {
        Err(e) => faults.push(ReceiptFault::VoucherMalformed {
            provider,
            reason: e.to_string(),
        }),
        Ok(signed) => {
            if signed.verify_signer(lane.signer, voucher_dom).is_err() {
                faults.push(ReceiptFault::VoucherSignature { provider });
            }
        }
    }
    let index = match proof.reveal {
        None => 0,
        Some(r) => {
            let extends = r.index > 0
                && v.chain_root != B256::ZERO
                && verify_forward(r.preimage, r.index, v.chain_root);
            if !extends {
                faults.push(ReceiptFault::Reveal { provider });
            }
            r.index
        }
    };
    let proved = v.claim(index);
    if proved != Some(proof.claim) {
        faults.push(ReceiptFault::Claim {
            provider,
            stated: proof.claim,
            proved,
        });
    }
    if proof.claim.amount < lane.prior.amount || proof.claim.bytes < lane.prior.bytes {
        faults.push(ReceiptFault::ClaimBelowPrior { provider });
    }
    faults
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
