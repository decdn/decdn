# Integrator guide

The onramp's Turnstile gate is one way to decide who gets a sponsored
download. If you'd rather sponsor your signed-in users, or users who bought
something, you have three options, from least to most code:

1. **Call the daemon from your backend.** Your app already knows who the user
   is; once it decides they qualify, it asks `sponsord` for a capability.
2. **Implement a gate for the onramp.** Keep the onramp's installers,
   hand-off and rate limits, and swap the check.
3. **Embed `sponsord-core`.** Sign capabilities in your own Rust service,
   with no daemon.

## 1. The daemon API

Every `/v1` route needs `Authorization: Bearer <SPONSORD_API_TOKEN>`. The
OpenAPI document is [`docs/openapi/sponsord.json`](openapi/sponsord.json).

| Route | Request | Success |
|---|---|---|
| `POST /v1/capabilities` | `{"signer": "0x..", "spending_cap"?: u64, "ttl_secs"?: u64}` | `200 {"token": "dcap1:..", "spending_cap": u64, "expiry": u64, "registered": bool}` |
| `GET /v1/info` | | `200 {"chain_id": u64, "payment_pool": "0x..", "max_spending_cap": u64, "max_ttl_secs": u64}` |
| `GET /healthz` | | `200 {"ok": true}` (no token) |
| `GET /metrics` | | Prometheus text (no token) |

`spending_cap` is in micro-USDC. An omitted cap or TTL is the daemon's
maximum. Errors are `{"error": "<code>"}`, plus the fields noted:

| Status | `error` | When |
|---|---|---|
| 400 | `bad_request` | malformed body or signer |
| 400 | `exceeds_max` | cap or TTL above the maximum; adds `max_spending_cap` and `max_ttl_secs` |
| 400 | `zero` | cap or TTL of 0 |
| 401 | `unauthorized` | missing or wrong token |
| 409 | `signer_expired` | the signer is registered on-chain and its registration has expired; adds `expiry` |
| 500 | `internal` | signing failed |
| 503 | `chain_unavailable` | the signer's registration could not be read |

**One signer, one set of terms.** The first capability redeemed for a signer
fixes its cap and expiry on-chain. For a registered signer, `POST
/v1/capabilities` returns that capability (`registered: true`, with its real
terms) whatever you ask for, and `409 signer_expired` once it has expired.
To give someone different terms, use a new signer key.

The minimal gate is one call after your check passes:

```bash
curl -fsS -X POST http://127.0.0.1:8090/v1/capabilities \
  -H "Authorization: Bearer $SPONSORD_API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"signer":"0x...","spending_cap":1000000,"ttl_secs":86400}'
```

From Rust, `sponsord-api` (feature `client`) has a typed client:

```rust
use sponsord_api::client::{DaemonClient, DaemonError};
use sponsord_api::daemon::IssueRequest;
use sponsord_api::MicroUsdc;

let daemon = DaemonClient::new("http://127.0.0.1:8090", token, reqwest::Client::new());
let req = IssueRequest {
    spending_cap: Some(MicroUsdc(1_000_000)),
    ..IssueRequest::new(signer)
};
match daemon.issue(&req).await {
    Ok(resp) => hand_to_user(resp.capability.token),
    Err(DaemonError::SignerExpired { .. }) => ask_for_a_new_key(),
    Err(e) => return Err(e.into()),
}
```

The signer is the user's download key. With your own gate you also decide
how the token reaches the user's `decdn`: write it to a file and run
`decdn bundle pull --capability-file <file> --keystore <their key> ...`, or
keep using `decdn-sponsored` by putting it behind an onramp (option 2).

## 2. A gate for the onramp

The onramp's gate is the `sponsord_onramp::gate::Gate` trait:

```rust
#[async_trait]
pub trait Gate: Send + Sync {
    /// The HTML page the CLI sends the person to, for download key `client`.
    /// Once its check passes it must POST {"client", "proof"} to /v1/fund.
    fn page(&self, client: Address) -> String;

    /// Whether `proof` lets `client` have a capability.
    async fn verify(&self, client: Address, proof: &str, client_ip: Option<IpAddr>)
        -> anyhow::Result<bool>;
}
```

Build the onramp around yours the way its `main.rs` does: load an
`OnrampConfig` with `ONRAMP_GATE=custom` (which drops the Turnstile settings
from what is required), build `state::build(&cfg, daemon, Arc::new(YourGate))`,
and serve `http::router(state)`. The stock `sponsord-onramp` binary refuses
`custom`, since it has no gate of yours to run. Everything else (installers, `/v1/profile`, the
hand-off, rate limits) stays as it is. `TurnstileGate`
(`crates/sponsord-onramp/src/gate/turnstile.rs`) is a complete example.

Webhook ([#22](https://github.com/decdn/sponsord/issues/22)), signed-JWT
([#23](https://github.com/decdn/sponsord/issues/23)) and allow-all
([#24](https://github.com/decdn/sponsord/issues/24)) gates are planned as
built-ins.

The onramp's own API (what the CLI and the gate page call) is in
[`docs/openapi/sponsord-onramp.json`](openapi/sponsord-onramp.json).

## 3. Embed `sponsord-core`

`sponsord-core` is the daemon's logic without the HTTP server:

```rust
use sponsord_core::{ChainConfig, KeeperConfig, Limits, MicroUsdc, Sponsor, TermsRequest};

let sponsor = Sponsor::connect(signer, ChainConfig { rpc_url, chain_id, payment_pool, pool_id },
    Limits { max_spending_cap: MicroUsdc(5_000_000), max_ttl_secs: 172_800 }).await?;
tokio::spawn(sponsor.keeper(KeeperConfig { low_water, refill, interval }, shutdown.clone()));
let issued = sponsor.issue(user_key, &TermsRequest::default(), now_unix).await?;
```

`signer` is any cloneable alloy signer that signs raw hashes and
transactions: a local `PrivateKeySigner`, or a remote signer such as AWS or
GCP KMS. Hardware wallets aren't supported yet: alloy's `LedgerSigner` signs
only typed data and isn't `Clone` (tracked in
[#26](https://github.com/decdn/sponsord/issues/26)).
`Sponsor::connect` checks that it owns the pool on-chain. With a remote
signer, re-issuing for a registered key gives an equally valid but not
byte-identical token.
