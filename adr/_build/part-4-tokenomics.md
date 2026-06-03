```{=typst}
#pagebreak(weak: true)
```

# Chapter 4 — Tokenomics & incentives

The economic model that ties the protocol together: a fixed-supply TOKEN, the FeeRouter three-bucket split (60/30/10), the `CapacityBond` lock-to-capacity curve (`bond = k × Mbps^α`) — no ongoing service emission — the App Incentives demand-side bucket (19%), and a Balancer V3 protocol-owned-liquidity strategy (10% POL + 5% MM = 15% combined Liquidity Provision). Per-byte FeeRouter settlement and served-bytes voting weight are the wash-trading defense. The launch contract surface is forward-compatible, so future economic-layer products land as additive contracts without changing existing ones.

```{=typst}
#pagebreak()
```
