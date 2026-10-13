use super::*;

#[test]
fn allowed_hosts_name_the_listen_port_under_every_loopback_name() {
    let hosts = allowed_hosts("127.0.0.1:8484".parse().unwrap());
    for host in ["127.0.0.1:8484", "localhost:8484", "[::1]:8484"] {
        assert!(hosts.iter().any(|h| h == host), "{host}");
    }
    assert!(!hosts.iter().any(|h| h == "evil.example:8484"));
    assert!(!hosts.iter().any(|h| h == "localhost:9999"));
}

#[test]
fn content_type_by_extension_and_markup_as_text() {
    assert_eq!(content_type("Sintel.2010.4k.MKV"), "video/x-matroska");
    assert_eq!(content_type("clip.mp4"), "video/mp4");
    assert_eq!(
        content_type("weights.safetensors"),
        "application/octet-stream"
    );
    assert_eq!(content_type("page.html"), "text/plain; charset=utf-8");
    assert_eq!(content_type("logo.svg"), "text/plain; charset=utf-8");
    assert_eq!(content_type("no-extension"), "application/octet-stream");
}

#[test]
fn escape_covers_markup_and_quotes() {
    assert_eq!(
        escape(r#"<a href="x">'&'</a>"#),
        "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;"
    );
}

#[test]
fn human_secs_rounds_to_the_largest_unit() {
    assert_eq!(human_secs(59), "0 minutes");
    assert_eq!(human_secs(60), "1 minute");
    assert_eq!(human_secs(3_599), "59 minutes");
    assert_eq!(human_secs(3_600), "1 hour");
    assert_eq!(human_secs(7_200), "2 hours");
    assert_eq!(human_secs(86_400), "1 day");
    assert_eq!(human_secs(172_800), "2 days");
}

#[test]
fn sponsor_reads_a_saved_allowance() {
    let dir = tempfile::tempdir().unwrap();
    let sponsor = Sponsor {
        dir: dir.path().to_path_buf(),
        onramp: "http://127.0.0.1:1".to_owned(),
        http: reqwest::Client::new(),
    };
    assert!(sponsor.saved_allowance().is_none());
    write_private(&dir.path().join("capability"), "dcap1:abc\n").unwrap();
    write_private(&dir.path().join("expiry"), "1792065079").unwrap();
    let allowance = sponsor.saved_allowance().unwrap();
    assert_eq!(allowance.token, "dcap1:abc");
    assert_eq!(allowance.expiry, 1_792_065_079);
    assert_eq!(allowance.keystore, dir.path().join("keystore.json"));
}
