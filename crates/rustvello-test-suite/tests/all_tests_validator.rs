//! Validates that all shared test functions from `rustvello-test-suite`
//! are consumed by at least one backend crate (mem or sqlite).
//!
//! This is the Rust equivalent of pynenc's `test_all_tests_for_plugins.py`
//! which uses AST analysis to ensure no test is forgotten.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Collect all `pub async fn test_*` function names from a source file.
fn extract_test_functions(path: &Path) -> Vec<String> {
    let content = fs::read_to_string(path).expect("failed to read file");
    let mut names = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("pub async fn test_") {
            if let Some(name) = trimmed
                .strip_prefix("pub async fn ")
                .and_then(|s| s.split('(').next())
            {
                names.push(name.to_string());
            }
        }
    }
    names
}

/// Collect all test function names referenced in a consumer test file.
/// Looks for patterns like `::test_xxx(` or `test_xxx(&` which is how
/// the suite macros and manual invocations reference the shared functions.
fn extract_referenced_tests(path: &Path) -> HashSet<String> {
    let content = fs::read_to_string(path).expect("failed to read consumer file");
    let mut refs = HashSet::new();
    for line in content.lines() {
        // Match patterns like `::test_xxx(` from both macro-generated and manual tests
        let haystack = line.trim();
        if let Some(idx) = haystack.find("::test_") {
            let after = &haystack[idx + 2..]; // skip "::"
            if let Some(name) = after.split('(').next() {
                refs.insert(name.to_string());
            }
        }
    }
    refs
}

#[test]
fn all_shared_tests_are_consumed() {
    let suite_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mem_suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("rustvello-mem/tests/suite.rs");
    let sqlite_suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("rustvello-sqlite/tests/suite.rs");

    // Collect all shared test function names
    let mut all_tests = Vec::new();
    let modules = [
        "broker.rs",
        "orchestrator.rs",
        "state_backend.rs",
        "trigger.rs",
        "client_data_store.rs",
        "lifecycle.rs",
    ];
    for module in &modules {
        let path = suite_dir.join(module);
        if path.exists() {
            let tests = extract_test_functions(&path);
            for t in tests {
                all_tests.push((module.to_string(), t));
            }
        }
    }

    // Collect all references from consumers
    let mut all_refs = HashSet::new();
    for consumer in [&mem_suite, &sqlite_suite] {
        if consumer.exists() {
            all_refs.extend(extract_referenced_tests(consumer));
        }
    }

    // Also scan macro definitions to find which tests are included in macros
    let mut macro_tests = HashSet::new();
    for module in &modules {
        let path = suite_dir.join(module);
        if path.exists() {
            let content = fs::read_to_string(&path).unwrap();
            // Inside macro_rules! blocks, references like $crate::module::test_xxx
            for line in content.lines() {
                if line.contains("$crate::") && line.contains("::test_") {
                    if let Some(idx) = line.find("::test_") {
                        let after = &line[idx + 2..];
                        if let Some(name) = after.split('(').next() {
                            macro_tests.insert(name.to_string());
                        }
                    }
                }
            }
        }
    }

    // Every shared test function must either be referenced directly OR be in a macro
    let mut missing = Vec::new();
    for (module, test_name) in &all_tests {
        let is_referenced = all_refs.contains(test_name.as_str());
        let is_in_macro = macro_tests.contains(test_name.as_str());
        if !is_referenced && !is_in_macro {
            missing.push(format!("{module}::{test_name}"));
        }
    }

    assert!(
        missing.is_empty(),
        "The following shared test functions are not consumed by any backend crate \
         or referenced in any suite macro:\n  {}\n\n\
         Add them to the appropriate suite macro or wire them manually in \
         rustvello-mem/tests/suite.rs and/or rustvello-sqlite/tests/suite.rs.",
        missing.join("\n  ")
    );

    // Sanity check: we should have found a reasonable number of tests
    assert!(
        all_tests.len() >= 30,
        "Expected at least 30 shared test functions, found {}. \
         Check that the test suite source files exist.",
        all_tests.len()
    );
}
