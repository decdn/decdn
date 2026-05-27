```{=typst}
#pagebreak(weak: true)
```

# Chapter 6 — Governance & contracts

The governance model that sets the parameters earlier chapters consume, and the cross-contract interaction map that consolidates the on-chain surface. An admin key governs the initial network; a bootstrap multisig handles the first 6–12 months post-launch; a served-bytes-weighted Governor (`FeeRouter.bytesInWindow × age_ramp` per [ADR 036](../036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) with a Timelock and safety bounds governs production once the operator-count and capacity transition thresholds are met.

```{=typst}
#pagebreak()
```
