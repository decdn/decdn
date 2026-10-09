use super::html_escape;

#[test]
fn escapes_html_metacharacters() {
    assert_eq!(
        html_escape(r#"<a href="x">Tom & 'Jerry'</a>"#),
        "&lt;a href=&quot;x&quot;&gt;Tom &amp; &#39;Jerry&#39;&lt;/a&gt;"
    );
}
