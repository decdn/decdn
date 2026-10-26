# Appendix: Pull Receipts

> **This is an appendix, not a core protocol ADR.** A pull receipt is a file
> the client writes from messages the pull already exchanged. Nodes, pools and
> contracts do not see it, and nothing in the protocol reads it. A client can
> write no receipt, or a different one.

**Date:** 2026-10-26 — **Status:** Accepted (`decdn get --receipt` and
`decdn receipt {verify,settle}` ship). **Touches:** `decdn` CLI (`get`,
`receipt`), `decdn-client` (`receipt`).

## Context

A client verifies every byte it receives against the BLAKE3 hash, and pays
for every byte with a signed voucher. After the pull, the client keeps the
file and nothing else. A user cannot show a publisher what the pull fetched,
from which nodes, and for how much. A bug report cannot name the signed answers a
node gave.

Signed messages already answer these questions. Each node signs
a `StreamResponse` for each stream
([ADR 014](014-on-chain-verification.md#eip-712-type-definitions)). The payer
signs each cumulative voucher
([ADR 003](003-payments.md#eip-712-voucher-signature)). A pull receipt keeps
these messages instead of dropping them.

## Contents

A receipt is a JSON document with `"format": "decdn-receipt/1"`. A reader
rejects any other format when it parses the document. It has these parts:

| Part | Content | Signed by |
|------|---------|-----------|
| `chain` | The chain id, the `PaymentPool` address and the `SlashJudge` address. | Nobody. The EIP-712 domain of each signature below binds it to these values. |
| `content` | The requested hash, `blob` or `bundle`, the namespace, the whole blobs that landed with their sizes, the hashes that a node accepted but that did not land (`unlanded`), and whether the pull completed. | Nobody. The client states it. |
| `window` | The start time and the end time of the pull. | Nobody. The client states it. |
| `lanes[].responses` | For each hash a node served on the lane, the first accepting `StreamResponse` body and its `slash_sig`. | The node's operator key, under the `SlashJudge` domain. |
| `lanes[].proof.voucher` | The voucher that signs the lane's anchor at the end of the pull. | The lane signer, under the `PaymentPool` domain. |
| `lanes[].proof.reveal` | The deepest preimage released under that voucher's chain. | Nobody. The preimage proves itself against the voucher's `chain_root` ([ADR 003](003-payments.md#hash-chain-metering-payword)). |
| `lanes[].proof.claim` | `amount + index × chunk_price` over `bytes_delivered + index × CHUNK_BYTES`. | Nobody. A reader computes it again from the voucher and the reveal. |
| `lanes[].prior` | The lane's cumulative before the pull. A jump to a node's watermark during the pull moves it, so the jump does not count as the pull's payment. | Nobody. The client states it. |
| `lanes[].settlement` | The `PoolRedeemed` event that covers the claim, or the highest one that does not. | Nobody. `decdn receipt settle` reads it from the chain. |

A lane that paid but has no voucher for its anchor has no `proof`. For
example, the voucher can leave the short history of vouchers that the client
keeps for each lane. A lane that did not move past its prior watermark, and
that released no preimage, is not in the receipt.

A failed or interrupted pull also writes a receipt, with `complete = false`.
It lists in `unlanded` each hash that a node accepted and that did not land.

The receipt contains no signature that the client makes for the receipt
itself. Such a signature proves only that the client wrote the fields. The
client can write any fields. The signatures that prove something are the
node's and the payer's, and the receipt already holds them.

## Verification

`decdn receipt verify` checks the receipt with no network access:

1. Each response is valid on the wire, accepts (`ok = true`), names its
   lane's pool, and answers for a hash that the receipt lists.
2. Each response's `slash_sig` recovers to the lane's provider.
3. Each entry that a response covers has the size that the response signs.
   `verify` does not check an entry that no response covers, for example an
   entry that the client built from local data.
4. A receipt with `complete = true` lists no `unlanded` hash.
5. Each voucher recovers to the lane's signer.
6. Each reveal hashes forward to its voucher's `chain_root`.
7. Each claim is equal to the claim that its voucher and reveal prove, and is
   not below the prior watermark on either axis.
8. On a chain that the build knows, the receipt's `PaymentPool` and
   `SlashJudge` addresses are equal to the build's addresses. On a chain that
   the build does not know, `verify` says that it did not check them.

`verify` does not check the settlement. A reader checks it against the chain.
`decdn receipt settle` reads the chain only for a receipt that passes these
checks. Each run of `settle` replaces every settlement with the events that
it reads, so a settlement that the chain does not show stays out.

## What a Receipt Proves

- The node with operator address X quotes rate R and size S for hash H on
  pool P. The client chooses the timestamp, and the node signs it.
- The payer signs a cumulative claim C to provider X on pool P. A node that
  holds that voucher and that preimage can redeem up to C. Redemption pays
  only what the pool balance and the signer's capability allow
  ([ADR 003](003-payments.md#redemption-and-close)).
- After `settle` finds a covering event, the chain shows that the lane is
  paid at least C.

## What a Receipt Does Not Prove

- **That the bytes arrived.** The client checks the bytes against the hash.
  The receipt contains only the client's statement about this check.
- **Which bytes each node served.** The request's `byte_offset` and
  `byte_len` are not signed.
- **That the downloads are distinct people, or that the payer is not the
  publisher.** A publisher can pay its own nodes from its own pool and get a
  valid receipt. A count of receipts is a count of paid pulls.
- **The namespace or the wall-clock time.** No node signs either value.
- **Settlement before `settle` runs.** A node redeems in its own sweep
  ([ADR 003](003-payments.md#redemption-and-close)), so the claim is not
  settled when the pull ends.

## Slashing

A receipt adds no offense and no evidence path. The two on-chain offenses
stay the same: rate manipulation and blacklist violation
([ADR 014](014-on-chain-verification.md#evidence-verification-per-offense-type)).
A blacklist challenge accepts one `StreamResponse`, within the evidence age.
A rate challenge also needs a `ProbeResponse`, and a receipt holds no
`ProbeResponse`. A person who holds a receipt has no power that the holder of
the responses did not already have. Vouchers are not slashing evidence.

## Privacy

A receipt links a content hash to a lane signer, a pool and the nodes that
served it. The chain already links the signer, the pool and the provider at
redemption ([ADR 017](017-privacy.md#payment-and-identity-linkability-p-02-p-03-p-07)).
The chain does not link them to a hash. A published receipt adds that link.
A node's `slash_sig` already proves what that node holds
([ADR 017](017-privacy.md#slash_sig-as-content-inventory-proof-p-23)). A
published receipt shows that proof to everyone.

`decdn get` writes a receipt only to the local file that `--receipt` names.
To publish a receipt is the user's decision.

## Relation to the Node's Download Receipts

A node writes `download_receipts.jsonl` in its data directory. That file is
the operator's audit log of what the node served and was paid for. A pull
receipt is the client's record of one pull. The two files have different
formats, and neither one reads the other.
