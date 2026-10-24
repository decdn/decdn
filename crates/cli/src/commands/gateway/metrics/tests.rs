use super::*;

/// Every series exports from the start, at zero, with its type line.
#[test]
fn every_series_exports_at_zero() {
    let metrics = Metrics::default();
    let text = metrics.render();
    for name in [
        "decdn_gateway_bytes_served_total",
        "decdn_gateway_requests_ok_total",
        "decdn_gateway_requests_not_found_total",
        "decdn_gateway_requests_refused_total",
        "decdn_gateway_requests_allowance_exhausted_total",
        "decdn_gateway_pull_failures_funding_total",
        "decdn_gateway_pull_failures_other_total",
        "decdn_gateway_pulls_active",
    ] {
        assert!(text.contains(&format!("# TYPE {name} ")), "{name}: {text}");
        assert!(
            text.contains(&format!("\n{name} 0\n")),
            "{name} at zero: {text}"
        );
    }
}

/// A request counts under the sibling its status names, bytes count as they
/// go out, and pulls in flight are a gauge.
#[test]
fn outcomes_count_under_their_siblings() {
    let metrics = Metrics::default();
    for status in [
        StatusCode::OK,
        StatusCode::PARTIAL_CONTENT,
        StatusCode::FOUND,
        StatusCode::NOT_FOUND,
        StatusCode::PAYMENT_REQUIRED,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::BAD_GATEWAY,
        StatusCode::FORBIDDEN,
        StatusCode::RANGE_NOT_SATISFIABLE,
        StatusCode::METHOD_NOT_ALLOWED,
    ] {
        metrics.request(status);
    }
    metrics.bytes(100);
    metrics.bytes(23);
    metrics.pull_started();
    metrics.pull_started();
    metrics.pull_ended();
    let text = metrics.render();
    for line in [
        "decdn_gateway_requests_ok_total 3",
        "decdn_gateway_requests_not_found_total 1",
        "decdn_gateway_requests_allowance_exhausted_total 1",
        "decdn_gateway_pull_failures_funding_total 1",
        "decdn_gateway_pull_failures_other_total 1",
        "decdn_gateway_requests_refused_total 3",
        "decdn_gateway_bytes_served_total 123",
        "decdn_gateway_pulls_active 1",
    ] {
        assert!(text.contains(&format!("\n{line}\n")), "{line}: {text}");
    }
}
