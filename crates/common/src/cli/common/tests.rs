use super::{default_client_data_dir, default_data_dir};
use std::ffi::OsStr;

#[test]
fn client_data_dir_is_client_subdir_of_data_dir() {
    // Home-independent: whatever the base data dir resolves to (or `None`),
    // the client dir is its `client` subdirectory. Guards against a silent
    // revert to the node-shaped data dir or a wrong subdir name.
    match (default_data_dir(), default_client_data_dir()) {
        (Some(base), Some(client)) => {
            assert_eq!(client.file_name(), Some(OsStr::new("client")));
            assert_eq!(client.parent(), Some(base.as_path()));
        }
        (None, None) => {} // no home available; both absent, consistent
        other => panic!("data-dir/client-dir availability mismatch: {other:?}"),
    }
}
