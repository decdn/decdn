use super::*;
use crate::commands::doctor::Severity;
use std::path::Path;

#[test]
fn fs_origin_dir_passes_missing_fails() {
    assert_eq!(
        evaluate_fs_origin(0, Path::new("/x"), true, true).severity,
        Severity::Pass
    );
    assert_eq!(
        evaluate_fs_origin(0, Path::new("/x"), false, false).severity,
        Severity::Fail
    );
    assert_eq!(
        evaluate_fs_origin(0, Path::new("/x"), true, false).severity,
        Severity::Fail
    );
}
