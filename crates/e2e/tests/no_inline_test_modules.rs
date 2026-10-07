//! Every unit-test module in `crates/` lives in its own file (#2352).
//!
//! A `#[cfg(test)]` or `#[cfg(all(test, …))]` module is declared with
//! `mod <name>;` and its body sits in the child file `foo/<name>.rs` (for
//! `foo.rs`) or `<name>.rs` beside a `lib.rs`, `main.rs` or `mod.rs`. The tests
//! stay child modules, so they keep private access. They do not move to
//! `crates/*/tests/`: an integration test reaches only `pub` items.
//!
//! No rustc or clippy lint expresses this rule, so this test holds it. It
//! parses every `.rs` file under `crates/` with `syn` and fails on any
//! test-only module with an inline body, at any nesting depth. A module gated
//! on `cfg(any(test, …))` also compiles outside tests, so it is not a test
//! module and stays inline.
//!
//! The test lives in `decdn-e2e` because that crate is `publish = false`: it
//! reads the whole workspace tree, which a packaged crate does not ship.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Attribute, ItemMod, Meta, Token};

/// Whether `meta` (the argument of a `cfg`) holds only under `cfg(test)`.
fn cfg_requires_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") => list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|args| args.iter().any(cfg_requires_test)),
        _ => false,
    }
}

/// Whether `attrs` gate the item on `cfg(test)`.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<Meta>()
                .is_ok_and(|meta| cfg_requires_test(&meta))
    })
}

/// Collects the test-only modules with an inline body in one file.
struct InlineTestModules<'a> {
    /// The directory that holds this file's child module files.
    child_dir: PathBuf,
    /// The names of the enclosing inline modules, outermost first.
    nesting: Vec<String>,
    /// One message per offending module.
    offenders: Vec<String>,
    /// The file being checked, as reported.
    file: &'a Path,
}

impl<'ast> Visit<'ast> for InlineTestModules<'_> {
    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if item.content.is_none() {
            return;
        }
        let name = item.ident.to_string();
        if is_test_only(&item.attrs) {
            let mut target = self.child_dir.clone();
            target.extend(&self.nesting);
            target.push(format!("{name}.rs"));
            self.offenders.push(format!(
                "{}: module `{name}` has an inline body; move it to {}",
                self.file.display(),
                target.display(),
            ));
        }
        self.nesting.push(name);
        syn::visit::visit_item_mod(self, item);
        self.nesting.pop();
    }
}

/// Returns one message per test-only module with an inline body in `source`,
/// the contents of the file at `file`.
fn inline_test_modules(file: &Path, source: &str) -> Result<Vec<String>, String> {
    let ast = syn::parse_file(source).map_err(|e| format!("{}: {e}", file.display()))?;
    let parent = file.parent().unwrap_or(Path::new(""));
    let is_dir_owner = file
        .file_name()
        .is_some_and(|f| f == "lib.rs" || f == "main.rs" || f == "mod.rs");
    let child_dir = match file.file_stem() {
        Some(stem) if !is_dir_owner => parent.join(stem),
        _ => parent.to_path_buf(),
    };
    let mut visitor = InlineTestModules {
        child_dir,
        nesting: Vec::new(),
        offenders: Vec::new(),
        file,
    };
    visitor.visit_file(&ast);
    Ok(visitor.offenders)
}

#[test]
fn no_crate_has_an_inline_test_module() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let crates = workspace.join("crates");
    let mut checked = 0_usize;
    let mut offenders = Vec::new();
    let walk = walkdir::WalkDir::new(&crates)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| e.file_name() != "target");
    for entry in walk {
        let entry = entry.expect("walk crates/");
        let path = entry.path();
        if !entry.file_type().is_file() || path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let source = std::fs::read_to_string(path).expect("read source file");
        let relative = path.strip_prefix(&workspace).unwrap_or(path);
        match inline_test_modules(relative, &source) {
            Ok(found) => offenders.extend(found),
            Err(e) => offenders.push(format!("cannot parse {e}")),
        }
        checked += 1;
    }
    assert!(checked > 0, "found no .rs files under {}", crates.display());
    assert!(
        offenders.is_empty(),
        "{} inline test modules (#2352: each test module lives in its own file):\n{}",
        offenders.len(),
        offenders.join("\n"),
    );
}

/// Runs the checker on `source` as if it were `crates/x/src/foo.rs`.
fn check(source: &str) -> Vec<String> {
    inline_test_modules(Path::new("crates/x/src/foo.rs"), source).expect("parse")
}

#[test]
fn flags_an_inline_tests_module() {
    let found = check("#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {}\n}\n");
    assert_eq!(
        found,
        ["crates/x/src/foo.rs: module `tests` has an inline body; \
          move it to crates/x/src/foo/tests.rs"],
    );
}

#[test]
fn accepts_an_out_of_line_tests_module() {
    assert!(check("#[cfg(test)]\nmod tests;\n").is_empty());
}

#[test]
fn reads_past_stacked_and_multi_line_attributes() {
    let source = "#[cfg(test)]\n// note\n#[allow(\n    clippy::unwrap_used,\n    \
                  clippy::expect_used,\n)]\n#[expect(clippy::panic, reason = \"x\")]\n\
                  mod tests {}\n";
    assert_eq!(check(source).len(), 1);
}

#[test]
fn flags_cfg_all_test_and_a_feature() {
    assert_eq!(
        check("#[cfg(all(test, feature = \"anvil-e2e\"))]\nmod e2e_tests {}\n").len(),
        1,
    );
    assert_eq!(
        check("#[cfg(all(feature = \"x\", all(unix, test)))]\nmod t {}\n").len(),
        1,
    );
}

#[test]
fn flags_any_module_name_and_visibility() {
    let found = check("#[cfg(test)]\npub(crate) mod test_support {}\n");
    assert_eq!(
        found,
        [
            "crates/x/src/foo.rs: module `test_support` has an inline body; \
          move it to crates/x/src/foo/test_support.rs"
        ],
    );
}

#[test]
fn flags_a_test_module_nested_in_an_inline_module() {
    let found = check("mod inner {\n    #[cfg(test)]\n    mod tests {}\n}\n");
    assert_eq!(
        found,
        ["crates/x/src/foo.rs: module `tests` has an inline body; \
          move it to crates/x/src/foo/inner/tests.rs"],
    );
}

#[test]
fn places_children_of_a_directory_owner_beside_it() {
    for owner in ["lib.rs", "main.rs", "mod.rs"] {
        let file = PathBuf::from("crates/x/src").join(owner);
        let found = inline_test_modules(&file, "#[cfg(test)]\nmod tests {}\n").expect("parse");
        assert_eq!(
            found,
            [format!(
                "{}: module `tests` has an inline body; move it to crates/x/src/tests.rs",
                file.display()
            )],
        );
    }
}

#[test]
fn ignores_modules_that_also_compile_outside_tests() {
    assert!(check("#[cfg(any(test, feature = \"test-util\"))]\nmod doubles {}\n").is_empty());
    assert!(check("#[cfg(not(test))]\nmod real {}\n").is_empty());
    assert!(check("#[cfg(feature = \"x\")]\nmod gated {}\n").is_empty());
    assert!(check("mod plain {}\n").is_empty());
}

#[test]
fn ignores_test_gated_items_that_are_not_modules() {
    let source = "#[cfg(test)]\nuse std::fmt;\n#[cfg(test)]\nfn helper() {}\n\
                  #[cfg(test)]\nimpl Foo {}\n";
    assert!(check(source).is_empty());
}

#[test]
fn ignores_module_text_inside_string_literals() {
    assert!(check("const S: &str = \"#[cfg(test)]\\nmod tests {}\";\n").is_empty());
}
