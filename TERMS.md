<!--
  CANONICAL OPERATOR TERMS — single source of the in-binary terms constant.
  The node software embeds this file's bytes verbatim; the registration
  clickwrap displays them and records keccak256(bytes) as `termsHash`
  on-chain (ADR 019 § Operator Safety Obligations).

  Editing this file changes the hash. A new version becomes canonical only
  when governance sets `currentTermsHash` to its hash (DecdnGovernor +
  timelock). Do not paraphrase clauses in code or docs — reference by
  version.
-->

# deCDN Operator Terms

**Version:** 1.0.0-draft
**Status:** DRAFT — not yet validated by counsel. Shipped pre-testnet to establish the mechanism and posture; a counsel-reviewed successor is adopted later by a governance `currentTermsHash` bump, with no contract change.

By registering a node on the deCDN network you, the operator, accept these
terms. Acceptance is a precondition of registration: the node software shows
you this text and records your assent on-chain as the hash of these exact
bytes. These are duties you take on as an operator. They are not a statement
by the network that any content is screened.

## 1. Compliance with applicable law

You are aware of, and will comply with, the laws that apply to you and to the
operation of your node — including any obligation to report illegal content,
such as child sexual abuse material (CSAM), that applies in your jurisdiction.
These terms do not name or substitute for the law that applies to you; they
require that you follow it.

## 2. Regional obligations

The region you declare at registration (`regionHint`) determines which
regional obligations and which regional governance body apply to your node.
You will keep your declared region truthful, and you accept the content-removal
and enforcement scope of your declared region as described in the network's
content-takedown rules. Obligations that are specific to a jurisdiction attach
to you through your declared region.

## 3. Acting on knowledge of illegal content

If you become aware that specific content your node serves is illegal, you
will:

1. stop serving that content promptly, and
2. report it where the law that applies to you requires reporting.

This is a duty to act on actual knowledge. These terms do not, by themselves,
require you to run any particular automated scanning system — what screening
you must perform, if any, is governed by the law that applies to you (§1) and
your regional obligations (§2). Where you do operate screening, you remain
responsible for acting on what it surfaces.

## 4. No network guarantee

The network does not inspect content and makes no representation that content
is lawful or screened. Nothing in these terms creates such a guarantee. Each
operator is independently responsible for the duties above; another operator's
compliance or non-compliance does not alter yours.

## 5. Consequences of breach

If you breach these terms, the network's governance may act against your node
under the content-takedown and slashing rules — including blacklisting and
ejection — and you remain subject to enforcement by the authorities of your
own jurisdiction. The on-chain record of your acceptance is evidence that you
were on notice of these duties; it is not evidence that you performed them.

## 6. Changes to these terms

These terms may be revised through the network's canonical governance process.
You agree to be bound by the version that governance has made canonical from
time to time, not only the version you accepted when you registered. The
network records the version you assented to at registration; a later
governance-adopted revision applies to your continued operation of a node,
whether or not you re-record your acceptance on-chain. If you do not wish to be
bound by a revision, your remedy is to stop operating your node and exit under
the unbonding process.

## Operator guidance (not part of these terms)

Practical guidance for meeting these duties — for example, content hash-match
databases and the reporting endpoint or hotline for a given jurisdiction — is
maintained as region-scoped operator documentation outside this document, so
that this text and its hash remain stable across jurisdictions. That guidance
is a help, not an additional contractual term, and is not covered by your
acceptance hash.

---

*Accepting these terms records `termsHash = keccak256(<the bytes of this file>)`
against your node registration. See ADR 019 § Operator Safety Obligations.*
