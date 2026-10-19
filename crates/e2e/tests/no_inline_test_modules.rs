//! Every unit-test module in `crates/` lives in its own file (#2352).
//!
//! A test module is declared with `mod <name>;` and its body sits in the
//! child file `foo/<name>.rs` (for `foo.rs`) or `<name>.rs` beside a `lib.rs`,
//! `main.rs` or `mod.rs`. The tests stay child modules, so they keep private
//! access. They do not move to `crates/*/tests/`: an integration test reaches
//! only `pub` items.
//!
//! A test module is one whose `cfg` holds only in test code: `test`,
//! `feature = "test-support"`, an `all(…)` with one of those, or an `any(…)`
//! made only of them. `test-support` is the workspace's name for a test-only
//! feature: `decdn-protocol`'s overridable discovery-block size (and
//! `decdn-node`'s pass-through to it) and the fakes in the sponsord crates'
//! `test_support.rs`. Only tests and dev-dependencies enable it; no release
//! build does, and a third test below holds that. Any other
//! `cfg(any(test, …))` also compiles outside tests, so that module is not
//! test code and stays inline.
//!
//! No rustc or clippy lint expresses this rule, so this test holds it. It
//! parses every `.rs` file under `crates/` with `syn` and fails on any
//! test-only module with an inline body, at any nesting depth.
//!
//! Because every test module has its own file, `CodeQL` skips them by file name
//! (`.github/codeql/codeql-config.yml`). A second test holds that skip list to
//! test code: every file under `src/` that a `paths-ignore` pattern matches
//! must be declared as a test module, so a production file never drops out of
//! the analysis because of its name.
//!
//! The tests live in `decdn-e2e` because that crate is `publish = false`: they
//! read the whole workspace tree, which a packaged crate does not ship.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use globset::{GlobBuilder, GlobSetBuilder};

use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Attribute, Expr, ExprLit, ItemMod, Lit, Meta, MetaList, Token};

/// Whether `meta` (the argument of a `cfg`) holds only under `cfg(test)` or
/// the `test-support` feature.
fn cfg_requires_test(meta: &Meta) -> bool {
    let args =
        |list: &MetaList| list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated);
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::NameValue(nv) => {
            nv.path.is_ident("feature")
                && matches!(
                    &nv.value,
                    Expr::Lit(ExprLit { lit: Lit::Str(s), .. })
                        if s.value() == "test-support"
                )
        }
        Meta::List(list) if list.path.is_ident("all") => {
            args(list).is_ok_and(|args| args.iter().any(cfg_requires_test))
        }
        Meta::List(list) if list.path.is_ident("any") => {
            args(list).is_ok_and(|args| !args.is_empty() && args.iter().all(cfg_requires_test))
        }
        Meta::List(_) => false,
    }
}

/// Whether `attrs` gate the item on test code only.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<Meta>()
                .is_ok_and(|meta| cfg_requires_test(&meta))
    })
}

/// The test-only modules one file declares.
#[derive(Default)]
struct TestModules {
    /// One message per test-only module with an inline body.
    inline: Vec<String>,
    /// The file of each test-only module declared with `mod <name>;`.
    files: Vec<PathBuf>,
}

/// Walks one file's module tree, collecting its test-only modules.
struct TestModuleVisitor<'a> {
    /// The directory that holds this file's child module files.
    child_dir: PathBuf,
    /// The names of the enclosing inline modules, outermost first.
    nesting: Vec<String>,
    /// What the walk has found so far.
    found: TestModules,
    /// The file being checked, as reported.
    file: &'a Path,
}

impl TestModuleVisitor<'_> {
    /// The file that holds the body of child module `name` at this depth.
    fn child_file(&self, name: &str) -> PathBuf {
        let mut target = self.child_dir.clone();
        target.extend(&self.nesting);
        target.push(format!("{name}.rs"));
        target
    }
}

impl<'ast> Visit<'ast> for TestModuleVisitor<'_> {
    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        let name = item.ident.to_string();
        let test_only = is_test_only(&item.attrs);
        if item.content.is_none() {
            if test_only {
                let target = self.child_file(&name);
                self.found.files.push(target);
            }
            return;
        }
        if test_only {
            let target = self.child_file(&name);
            self.found.inline.push(format!(
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

/// Collects the test-only modules declared in `source`, the contents of the
/// file at `file`.
fn test_modules(file: &Path, source: &str) -> Result<TestModules, String> {
    let ast = syn::parse_file(source).map_err(|e| format!("{}: {e}", file.display()))?;
    let parent = file.parent().unwrap_or(Path::new(""));
    let is_dir_owner = file
        .file_name()
        .is_some_and(|f| f == "lib.rs" || f == "main.rs" || f == "mod.rs");
    let child_dir = match file.file_stem() {
        Some(stem) if !is_dir_owner => parent.join(stem),
        _ => parent.to_path_buf(),
    };
    let mut visitor = TestModuleVisitor {
        child_dir,
        nesting: Vec::new(),
        found: TestModules::default(),
        file,
    };
    visitor.visit_file(&ast);
    Ok(visitor.found)
}

/// The workspace root.
fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every `.rs` file under `crates/`, relative to the workspace root, and the
/// test-only modules they declare. A file that does not parse is reported as
/// an inline module, so it fails the inline check rather than passing it.
fn scan_crates() -> (Vec<PathBuf>, TestModules) {
    let workspace = workspace();
    let crates = workspace.join("crates");
    let mut sources = Vec::new();
    let mut all = TestModules::default();
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
        let relative = path.strip_prefix(&workspace).unwrap_or(path).to_path_buf();
        match test_modules(&relative, &source) {
            Ok(found) => {
                all.inline.extend(found.inline);
                all.files.extend(found.files);
            }
            Err(e) => all.inline.push(format!("cannot parse {e}")),
        }
        sources.push(relative);
    }
    assert!(
        !sources.is_empty(),
        "found no .rs files under {}",
        crates.display()
    );
    (sources, all)
}

#[test]
fn no_crate_has_an_inline_test_module() {
    let (_, found) = scan_crates();
    assert!(
        found.inline.is_empty(),
        "{} inline test modules (#2352: each test module lives in its own file):\n{}",
        found.inline.len(),
        found.inline.join("\n"),
    );
}

/// The `paths-ignore` entries of `config` that reach into a crate's `src/`.
fn codeql_src_ignores(config: &str) -> Vec<String> {
    config
        .lines()
        .skip_while(|line| line.trim_end() != "paths-ignore:")
        .skip(1)
        .map_while(|line| line.trim_start().strip_prefix("- "))
        .map(str::trim)
        .filter(|pattern| pattern.contains("/src/"))
        .map(String::from)
        .collect()
}

#[test]
fn codeql_skips_only_test_modules() {
    let config_path = workspace().join(".github/codeql/codeql-config.yml");
    let config = std::fs::read_to_string(&config_path).expect("read the CodeQL config");
    let patterns = codeql_src_ignores(&config);
    assert!(
        !patterns.is_empty(),
        "no `src/` paths-ignore entries in {}",
        config_path.display()
    );
    let mut globs = GlobSetBuilder::new();
    for pattern in &patterns {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .expect("valid glob");
        globs.add(glob);
    }
    let globs = globs.build().expect("glob set");
    let (sources, found) = scan_crates();
    let test_files: BTreeSet<PathBuf> = found.files.into_iter().collect();
    let skipped: Vec<&PathBuf> = sources.iter().filter(|f| globs.is_match(f)).collect();
    assert!(!skipped.is_empty(), "no file matches {patterns:?}");
    let production: Vec<String> = skipped
        .into_iter()
        .filter(|f| !test_files.contains(*f))
        .map(|f| format!("{}", f.display()))
        .collect();
    assert!(
        production.is_empty(),
        "CodeQL skips these files by name, but they are not test modules; \
         rename them or narrow the pattern in .github/codeql/codeql-config.yml:\n{}",
        production.join("\n"),
    );
}

/// The ways a `Cargo.toml` could turn `test-support` on outside tests: as a
/// default feature, through a normal or build dependency's feature list, or
/// from a feature with another name. Only tests and dev-dependencies may
/// enable it, or the classifier above would treat shipped code as test code
/// and `CodeQL` would skip it by name.
fn test_support_leaks(manifest: &toml::Table) -> Vec<String> {
    let mut leaks = Vec::new();
    let enables_test_support = |value: &toml::Value| {
        value.as_array().is_some_and(|items| {
            items.iter().any(|item| {
                item.as_str()
                    .is_some_and(|s| s == "test-support" || s.ends_with("/test-support"))
            })
        })
    };
    if let Some(features) = manifest.get("features").and_then(toml::Value::as_table) {
        for (name, value) in features {
            if name != "test-support" && enables_test_support(value) {
                leaks.push(format!("feature `{name}` enables test-support"));
            }
        }
    }
    let mut dep_tables: Vec<(&str, &toml::Table)> = Vec::new();
    for table in ["dependencies", "build-dependencies"] {
        if let Some(deps) = manifest.get(table).and_then(toml::Value::as_table) {
            dep_tables.push((table, deps));
        }
        if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
            for target in targets.values() {
                if let Some(deps) = target.get(table).and_then(toml::Value::as_table) {
                    dep_tables.push((table, deps));
                }
            }
        }
    }
    for (table, deps) in dep_tables {
        for (dep, spec) in deps {
            if spec.get("features").is_some_and(enables_test_support) {
                leaks.push(format!("[{table}] {dep} enables its test-support feature"));
            }
        }
    }
    leaks
}

#[test]
fn the_test_support_feature_is_never_on_outside_tests() {
    let crates = workspace().join("crates");
    let mut checked = 0;
    let mut leaks = Vec::new();
    for entry in std::fs::read_dir(&crates).expect("read crates/") {
        let manifest_path = entry.expect("crate dir").path().join("Cargo.toml");
        if !manifest_path.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&manifest_path).expect("read Cargo.toml");
        let manifest: toml::Table = toml::from_str(&text).expect("parse Cargo.toml");
        checked += 1;
        let name = manifest_path
            .strip_prefix(workspace())
            .unwrap_or(&manifest_path)
            .display()
            .to_string();
        leaks.extend(
            test_support_leaks(&manifest)
                .into_iter()
                .map(|l| format!("{name}: {l}")),
        );
    }
    assert!(checked > 0, "no Cargo.toml under {}", crates.display());
    assert!(
        leaks.is_empty(),
        "`test-support` is a test-only feature; a module gated on it is a test module the \
         CodeQL skip list may ignore by name, so nothing but tests and dev-dependencies may \
         enable it:\n{}",
        leaks.join("\n"),
    );
}

#[test]
fn test_support_leak_shapes_are_detected() {
    let parse = |s: &str| toml::from_str::<toml::Table>(s).unwrap();
    assert!(test_support_leaks(&parse("[features]\ndefault = []\ntest-support = []\n")).is_empty());
    assert!(
        test_support_leaks(&parse(
            "[features]\ntest-support = [\"decdn-protocol/test-support\"]\n"
        ))
        .is_empty()
    );
    assert!(
        test_support_leaks(&parse(
            "[dev-dependencies]\nx = { path = \".\", features = [\"test-support\"] }\n"
        ))
        .is_empty()
    );
    assert_eq!(
        test_support_leaks(&parse("[features]\ndefault = [\"test-support\"]\n")).len(),
        1
    );
    assert_eq!(
        test_support_leaks(&parse(
            "[features]\nfakes = [\"decdn-protocol/test-support\"]\n"
        ))
        .len(),
        1
    );
    assert_eq!(
        test_support_leaks(&parse(
            "[dependencies]\nx = { path = \"../x\", features = [\"test-support\"] }\n"
        ))
        .len(),
        1
    );
    assert_eq!(
        test_support_leaks(&parse(
            "[target.'cfg(unix)'.build-dependencies]\nx = { version = \"1\", features = [\"test-support\"] }\n"
        ))
        .len(),
        1
    );
}

/// Runs the checker on `source` as if it were `crates/x/src/foo.rs`.
fn check(source: &str) -> Vec<String> {
    test_modules(Path::new("crates/x/src/foo.rs"), source)
        .expect("parse")
        .inline
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
        let found = test_modules(&file, "#[cfg(test)]\nmod tests {}\n")
            .expect("parse")
            .inline;
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
fn flags_a_module_gated_on_test_or_the_test_support_feature() {
    let found = check("#[cfg(any(test, feature = \"test-support\"))]\npub mod test_support {}\n");
    assert_eq!(
        found,
        [
            "crates/x/src/foo.rs: module `test_support` has an inline body; \
          move it to crates/x/src/foo/test_support.rs"
        ],
    );
    assert_eq!(
        check("#[cfg(feature = \"test-support\")]\nmod fakes {}\n").len(),
        1
    );
}

#[test]
fn nested_cfg_combinations_follow_the_same_rule() {
    assert_eq!(
        check("#[cfg(any(all(test, unix), feature = \"test-support\"))]\nmod doubles {}\n").len(),
        1
    );
    assert!(
        check("#[cfg(all(any(test, feature = \"other\"), unix))]\nmod doubles {}\n").is_empty()
    );
}

#[test]
fn ignores_a_module_that_any_other_feature_compiles() {
    assert!(check("#[cfg(any(test, feature = \"other\"))]\nmod doubles {}\n").is_empty());
    assert!(check("#[cfg(any(feature = \"test-support\", unix))]\nmod doubles {}\n").is_empty());
    assert!(check("#[cfg(any())]\nmod never {}\n").is_empty());
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

#[test]
fn records_the_file_of_each_out_of_line_test_module() {
    let source = "#[cfg(test)]\nmod tests;\nmod inner {\n    #[cfg(all(test, unix))]\n    \
                  pub(crate) mod test_support;\n}\nmod production;\n";
    let found = test_modules(Path::new("crates/x/src/foo.rs"), source).expect("parse");
    assert_eq!(
        found.files,
        [
            PathBuf::from("crates/x/src/foo/tests.rs"),
            PathBuf::from("crates/x/src/foo/inner/test_support.rs"),
        ],
    );
}

#[test]
fn reads_the_src_patterns_of_paths_ignore() {
    let config = "name: x\n\n# note\npaths-ignore:\n  - crates/*/tests/**\n  \
                  - crates/*/src/**/tests.rs\n  - crates/*/src/**/*_tests.rs\nqueries: []\n";
    assert_eq!(
        codeql_src_ignores(config),
        ["crates/*/src/**/tests.rs", "crates/*/src/**/*_tests.rs"],
    );
}
