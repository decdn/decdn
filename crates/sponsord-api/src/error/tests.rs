use serde_json::json;

use super::*;

#[test]
fn codes_are_snake_case_and_unknown_ones_parse() {
    let all = [
        ErrorCode::BadRequest,
        ErrorCode::ExceedsMax,
        ErrorCode::Zero,
        ErrorCode::Unauthorized,
        ErrorCode::GateFailed,
        ErrorCode::SignerExpired,
        ErrorCode::RateLimited,
        ErrorCode::Internal,
        ErrorCode::Upstream,
        ErrorCode::ChainUnavailable,
    ];
    for code in all {
        assert_eq!(serde_json::to_value(code).unwrap(), json!(code.as_str()));
    }
    let parsed: ErrorBody = serde_json::from_value(json!({"error": "brand_new"})).unwrap();
    assert_eq!(parsed.error, ErrorCode::Unknown);
}

#[test]
fn body_carries_only_the_fields_set() {
    let body = ErrorBody {
        expiry: Some(1_000),
        ..ErrorBody::new(ErrorCode::SignerExpired)
    };
    assert_eq!(
        serde_json::to_value(body).unwrap(),
        json!({"error": "signer_expired", "expiry": 1_000})
    );
}
