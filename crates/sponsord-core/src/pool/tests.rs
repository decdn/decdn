use alloy::signers::local::PrivateKeySigner;

use super::*;

#[tokio::test]
async fn a_malformed_rpc_url_stays_out_of_the_error() {
    let err = ChainPool::connect(
        "htt p://rpc.example/v2/SECRET-API-KEY",
        Address::ZERO,
        PrivateKeySigner::random(),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(!err.contains("SECRET-API-KEY"), "{err}");
    assert!(err.contains("not a valid URL"), "{err}");
}

/// reqwest names the URL in a transport error; the key in its path must
/// not survive into `ChainPool`'s error (#38).
#[tokio::test]
async fn an_unreachable_rpc_url_stays_out_of_the_error() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let err = ChainPool::connect(
        &format!("http://127.0.0.1:{port}/v3/SECRET-API-KEY"),
        Address::ZERO,
        PrivateKeySigner::random(),
    )
    .await
    .err()
    .unwrap();
    let err = format!("{err:#}");
    assert!(err.contains("usdc()"), "{err}");
    assert!(
        err.contains("tcp connect error"),
        "the cause is kept: {err}"
    );
    assert!(!err.contains("SECRET-API-KEY"), "{err}");
    assert!(!err.contains(&format!(":{port}")), "{err}");
}

/// One link of a hand-built error chain.
#[derive(Debug)]
struct Link(&'static str, Option<Box<Link>>);

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Link {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.1.as_deref().map(|l| l as _)
    }
}

fn chain(links: &[&'static str]) -> Link {
    let mut iter = links.iter().rev();
    let mut out = Link(iter.next().unwrap(), None);
    for l in iter {
        out = Link(l, Some(Box::new(out)));
    }
    out
}

#[test]
fn redacted_strips_each_link_and_skips_a_repeat() {
    const REQWEST: &str = "error sending request for url (http://rpc.example/v3/KEY)";
    let err = chain(&[REQWEST, REQWEST, "client error (Connect)", "refused"]);
    assert_eq!(
        redacted(&err),
        "error sending request: client error (Connect): refused"
    );
    // Only a repeat of the link before is dropped.
    assert_eq!(redacted(&chain(&["a", "b", "a"])), "a: b: a");
    assert_eq!(redacted(&chain(&["a"])), "a");
}

#[test]
fn a_top_up_error_keeps_its_markers_but_not_the_url() {
    const REQWEST: &str = "error sending request for url (http://rpc.example/v3/KEY)";
    let marker = TopUpUnconfirmed {
        tx: Some(alloy::primitives::TxHash::repeat_byte(0xAB)),
        nonce: 7,
    };
    let unconfirmed = anyhow::Error::new(chain(&[REQWEST, "timed out"]))
        .context("await topUp receipt")
        .context(marker);
    let err = redacted_top_up("top up 0x11".to_owned(), &unconfirmed);
    let kept = err.downcast_ref::<TopUpUnconfirmed>().unwrap();
    assert_eq!((kept.tx, kept.nonce), (marker.tx, marker.nonce));
    let shown = format!("{err:#}");
    assert!(!shown.contains("rpc.example"), "{shown}");
    assert!(!shown.contains("KEY"), "{shown}");
    assert_eq!(
        shown,
        format!("top up 0x11: {marker}: await topUp receipt: error sending request: timed out")
    );

    let short = anyhow::Error::new(chain(&[REQWEST]))
        .context("submit topUp")
        .context(AllowanceShortfall);
    let err = redacted_top_up("top up 0x11".to_owned(), &short);
    assert!(err.downcast_ref::<AllowanceShortfall>().is_some());
    assert!(err.downcast_ref::<TopUpUnconfirmed>().is_none());
    assert!(!format!("{err:#}").contains("rpc.example"));

    let plain = redacted_top_up("top up 0x11".to_owned(), &anyhow::anyhow!("{REQWEST}"));
    assert!(plain.downcast_ref::<AllowanceShortfall>().is_none());
    assert_eq!(format!("{plain:#}"), "top up 0x11: error sending request");
}

/// Every chain call names the RPC URL when it fails; none of
/// `ChainPool`'s errors may carry it (#38).
#[tokio::test]
async fn no_chain_pool_error_names_the_rpc_url() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}/v3/SECRET-API-KEY");
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let provider = treasury_provider(url.parse().unwrap(), signer);
    let pool = ChainPool {
        contract: PaymentPool::new(Address::ZERO, provider.clone()),
        provider,
        owner,
        token: Address::ZERO,
        payment_pool: Address::ZERO,
    };
    let id = B256::ZERO;
    let errors = [
        pool.remaining(id).await.err().unwrap(),
        pool.top_up(id, MicroUsdc(1)).await.err().unwrap(),
        pool.pool_owner(id).await.err().unwrap(),
        pool.authorization(id, owner).await.err().unwrap(),
        pool.transaction(TxHash::ZERO).await.err().unwrap(),
        pool.confirmed_nonce().await.err().unwrap(),
    ];
    for err in errors {
        let err = format!("{err:#}");
        assert!(
            err.contains("tcp connect error"),
            "the cause is kept: {err}"
        );
        assert!(!err.contains("SECRET-API-KEY"), "{err}");
        assert!(!err.contains(&format!(":{port}")), "{err}");
    }
}

#[test]
fn zero_cap_and_expiry_is_unregistered() {
    let auth = |cap, expiry| Authorization {
        spending_cap: MicroUsdc(cap),
        expiry,
    };
    assert_eq!(registration(0, 0), None);
    assert_eq!(registration(5, 10), Some(auth(5, 10)));
    assert_eq!(registration(0, 10), Some(auth(0, 10)));
}
