use serde_json::json;

use super::*;

#[test]
fn issue_request_omits_unset_terms() {
    let req = IssueRequest::new(Address::repeat_byte(0xaa));
    assert_eq!(
        serde_json::to_value(&req).unwrap(),
        json!({"signer": Address::repeat_byte(0xaa)})
    );
    let parsed: IssueRequest =
        serde_json::from_value(json!({"signer": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}))
            .unwrap();
    assert_eq!(parsed, req);
}

#[test]
fn issue_response_is_flat() {
    let resp = IssueResponse {
        capability: IssuedCapability {
            token: "dcap1:X".into(),
            spending_cap: MicroUsdc(5),
            expiry: 9,
        },
        registered: true,
    };
    assert_eq!(
        serde_json::to_value(&resp).unwrap(),
        json!({"token": "dcap1:X", "spending_cap": 5, "expiry": 9, "registered": true})
    );
}
