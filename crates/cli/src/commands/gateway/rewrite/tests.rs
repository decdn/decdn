use super::*;

fn hex() -> String {
    "ab".repeat(32)
}

/// The whole of `input` through `rewrite`, fed in chunks of `size` bytes.
fn chunked(mut rewrite: Rewrite, input: &str, size: usize) -> String {
    let mut out = Vec::new();
    for chunk in input.as_bytes().chunks(size) {
        out.extend_from_slice(&rewrite.feed(chunk).unwrap());
    }
    out.extend_from_slice(&rewrite.finish().unwrap());
    String::from_utf8(out).unwrap()
}

#[test]
fn a_uri_becomes_a_root_relative_path() {
    let h = hex();
    assert_eq!(
        rewrite_str(&format!("decdn:{h}/a/b.css?ns=7")).unwrap(),
        format!("/_decdn/{h}/a/b.css?ns=7")
    );
    assert_eq!(
        rewrite_str(&format!("a.png 1x, decdn:{h} 2x")).unwrap(),
        format!("a.png 1x, /_decdn/{h} 2x")
    );
    assert_eq!(
        rewrite_str(&format!("0; url=decdn:{h}/")).unwrap(),
        format!("0; url=/_decdn/{h}/")
    );
}

#[test]
fn only_a_whole_uri_is_rewritten() {
    let h = hex();
    for kept in [
        "page.html".to_owned(),
        format!("xdecdn:{h}"),
        format!("decdn:{}", &h[..63]),
        format!("decdn:{}zz", &h[..62]),
        "decdn:".to_owned(),
    ] {
        assert_eq!(rewrite_str(&kept), None, "{kept}");
    }
}

#[test]
fn css_urls_and_imports_are_rewritten_across_any_chunking() {
    let h = hex();
    let css = format!(
        "@import \"decdn:{h}/base.css\";\nbody {{ background: url(decdn:{h}) }}\n\
         .a {{ background: url('img.png') }}"
    );
    let want = css.replace("decdn:", "/_decdn/");
    for size in [1, 2, 7, 69, 70, 71, 4096] {
        let rewrite = Rewrite::for_type("text/css").unwrap();
        assert_eq!(chunked(rewrite, &css, size), want, "chunk size {size}");
    }
}

#[test]
fn html_attributes_and_style_are_rewritten_across_any_chunking() {
    let h = hex();
    let page = format!(
        "<!doctype html><html><head><link rel=stylesheet href=\"decdn:{h}/a.css\">\
         <style>body {{ background: url(decdn:{h}) }}</style></head><body>\
         <a href=\"decdn:{h}/\">site</a><img src=\"decdn:{h}\" srcset=\"decdn:{h} 2x\">\
         <div style=\"background: url(decdn:{h})\"></div><a href=\"rel.html\">rel</a>\
         <p>decdn:{h} in text stays</p><script>let u = \"decdn:{h}\";</script>\
         </body></html>"
    );
    for size in [1, 3, 64, 4096] {
        let rewrite = Rewrite::for_type("text/html").unwrap();
        let out = chunked(rewrite, &page, size);
        assert_eq!(
            out.matches("/_decdn/").count(),
            6,
            "chunk size {size}: {out}"
        );
        assert!(
            out.contains(&format!("<p>decdn:{h} in text stays</p>")),
            "{out}"
        );
        assert!(out.contains(&format!("let u = \"decdn:{h}\"")), "{out}");
        assert!(out.contains("href=\"rel.html\""), "{out}");
    }
}

#[test]
fn other_types_are_not_rewritten() {
    for essence in ["text/plain", "application/javascript", "image/svg+xml"] {
        assert!(Rewrite::for_type(essence).is_none(), "{essence}");
    }
}
