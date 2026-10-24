use super::*;

const ROOT: [u8; 32] = [0xab; 32];

#[test]
fn the_commit_is_the_first_forty_hex_of_the_root() {
    assert_eq!(commit(&ROOT), "ab".repeat(20));
}

/// `/<a>/<b>[/…]/resolve/<rev>/<file…>` names the entry `<a>/<b>[/…]/<file…>`
/// under `main` or the root's commit; another revision is unknown; anything
/// else is not a resolve path and falls through to the entry lookup.
#[test]
fn resolve_paths_name_their_entry() {
    let commit = commit(&ROOT);
    for (path, entry) in [
        (
            "/org/repo/resolve/main/model.safetensors",
            "org/repo/model.safetensors",
        ),
        (
            "/datasets/org/repo/resolve/main/data/train.parquet",
            "datasets/org/repo/data/train.parquet",
        ),
        (
            "/org/repo/resolve/main/dir/sub/file",
            "org/repo/dir/sub/file",
        ),
        ("/org/repo/resolve/main/a%20b.txt", "org/repo/a%20b.txt"),
        (
            "/spaces/org/demo/resolve/main/index.html",
            "spaces/org/demo/index.html",
        ),
        // The delimiter comes after the repo id, so a repo named `resolve`
        // is a repo.
        ("/org/resolve/resolve/main/f.bin", "org/resolve/f.bin"),
        (
            "/datasets/org/resolve/resolve/main/f.bin",
            "datasets/org/resolve/f.bin",
        ),
        (
            "/resolve/resolve/resolve/main/f.bin",
            "resolve/resolve/f.bin",
        ),
    ] {
        assert_eq!(
            resolve(path, &ROOT),
            HfPath::Entry {
                entry: entry.to_owned(),
                main: true
            },
            "{path}"
        );
    }
    assert_eq!(
        resolve(&format!("/org/repo/resolve/{commit}/f.bin"), &ROOT),
        HfPath::Entry {
            entry: "org/repo/f.bin".to_owned(),
            main: false
        }
    );
    for path in [
        "/org/repo/resolve/v1.0/f.bin",
        "/org/repo/resolve/refs%2Fpr%2F1/f.bin",
        &format!("/org/repo/resolve/{}/f.bin", "ab".repeat(32)),
        &format!("/org/repo/resolve/{}/f.bin", commit.to_ascii_uppercase()),
    ] {
        assert_eq!(resolve(path, &ROOT), HfPath::UnknownRevision, "{path}");
    }
    for path in [
        "/",
        "/f.bin",
        "/org/resolve/main/f.bin",
        "/org/repo/resolve/main",
        "/org/repo/resolve/main/",
        "/org/repo/resolve//f.bin",
        "/org/repo/f.bin",
        "/resolve/main/f.bin",
        // A repo id is `[<type>/]<org>/<name>`: a deeper path before the
        // delimiter is an entry path.
        "/a/b/c/resolve/main/f.bin",
        "/datasets/a/b/c/resolve/main/f.bin",
        "/datasets/org/resolve/main/f.bin",
    ] {
        assert_eq!(resolve(path, &ROOT), HfPath::NotResolve, "{path}");
    }
}

/// The commit path a `HEAD` under `main` sends the client to: the repo id,
/// typed or not, then `resolve`, the commit and the file.
#[test]
fn the_pinned_path_puts_the_commit_after_the_repo() {
    let commit = commit(&ROOT);
    assert_eq!(
        pinned("org/repo/dir/f.bin", &ROOT),
        format!("/org/repo/resolve/{commit}/dir/f.bin")
    );
    assert_eq!(
        pinned("datasets/org/repo/f.bin", &ROOT),
        format!("/datasets/org/repo/resolve/{commit}/f.bin")
    );
    assert_eq!(
        pinned("org/resolve/f.bin", &ROOT),
        format!("/org/resolve/resolve/{commit}/f.bin")
    );
}

/// The headers `huggingface_hub` reads: the commit, the entry hash as the
/// linked etag, the full size as the linked size (from `Content-Range` on a
/// partial answer, else `Content-Length`).
#[test]
fn decoration_names_the_commit_etag_and_size() {
    let mut full = hyper::Response::builder()
        .header(hyper::header::ETAG, "\"ee\"")
        .header(hyper::header::CONTENT_LENGTH, "12")
        .body(())
        .unwrap();
    decorate(full.headers_mut(), &ROOT);
    let h = full.headers();
    assert_eq!(h.get("x-repo-commit").unwrap(), &*"ab".repeat(20));
    assert_eq!(h.get("x-linked-etag").unwrap(), "\"ee\"");
    assert_eq!(h.get("x-linked-size").unwrap(), "12");

    let mut partial = hyper::Response::builder()
        .header(hyper::header::ETAG, "\"ee\"")
        .header(hyper::header::CONTENT_LENGTH, "5")
        .header(hyper::header::CONTENT_RANGE, "bytes 2-6/40")
        .body(())
        .unwrap();
    decorate(partial.headers_mut(), &ROOT);
    assert_eq!(partial.headers().get("x-linked-size").unwrap(), "40");

    let mut bare = hyper::Response::builder().body(()).unwrap();
    decorate(bare.headers_mut(), &ROOT);
    assert!(bare.headers().get("x-linked-size").is_none());
    assert!(bare.headers().get("x-linked-etag").is_none());
    assert!(bare.headers().get("x-repo-commit").is_some());
}
