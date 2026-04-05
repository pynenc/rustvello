//! CLI integration tests.
//!
//! These invoke the `rustvello-cli` binary via `assert_cmd` and check
//! stdout / exit-code without needing any database.

use assert_cmd::Command;
use predicates::prelude::*;

fn cli() -> Command {
    Command::cargo_bin("rustvello-cli").expect("binary should exist")
}

#[test]
fn cli_info_shows_version() {
    cli()
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("Rustvello v"));
}

#[test]
fn cli_info_shows_homepage() {
    cli()
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("pynenc.org"));
}

#[test]
fn cli_config_default() {
    cli()
        .args(["config", "--app-id", "test-cli"])
        .assert()
        .success()
        .stdout(predicate::str::contains("test-cli"))
        .stdout(predicate::str::contains("Effective Configuration"));
}

#[test]
fn cli_help_flag() {
    cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Rustvello distributed task system CLI",
        ));
}

#[test]
fn cli_version_flag() {
    cli()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("rustvello"));
}

#[test]
fn cli_purge_requires_yes() {
    cli()
        .arg("purge")
        .assert()
        .success()
        .stdout(predicate::str::contains("Use --yes to confirm"));
}

#[test]
fn cli_status_invalid_uuid() {
    cli()
        .args(["status", "not-a-uuid"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid invocation ID"));
}

#[test]
fn cli_unknown_subcommand() {
    cli().arg("nonexistent").assert().failure();
}
