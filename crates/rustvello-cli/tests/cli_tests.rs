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
        ))
        .stdout(predicate::str::contains("investigate"));
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
fn cli_investigate_invalid_uuid() {
    cli()
        .args(["investigate", "not-a-uuid"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid UUID for InvocationId"));
}

#[test]
fn cli_unknown_subcommand() {
    cli().arg("nonexistent").assert().failure();
}

#[test]
fn cli_cancel_queued_invocation_then_reports_already_final() {
    use rustvello::prelude::*;

    let dir = std::env::temp_dir().join(format!("rustvello-cli-cancel-{}", InvocationId::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("cli.db");
    let db_path = db.to_str().unwrap().to_owned();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let invocation_id = runtime.block_on(async {
        let mut app = Rustvello::builder()
            .app_id("cli")
            .sqlite(&db_path, "cli")
            .build()
            .await
            .unwrap();
        let task = TaskId::new("cli", "never_run");
        app.register_task(
            task.clone(),
            TaskConfig::default(),
            std::sync::Arc::new(|_| Ok("null".to_owned())),
        )
        .unwrap();
        app.submit(&task, SerializedArguments::new()).await.unwrap()
    });

    cli()
        .args([
            "cancel",
            invocation_id.as_str(),
            "--app-id",
            "cli",
            "--db-path",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));
    cli()
        .args([
            "cancel",
            invocation_id.as_str(),
            "--app-id",
            "cli",
            "--db-path",
        ])
        .arg(&db_path)
        .assert()
        .code(3)
        .stdout(predicate::str::contains("CANCELLED"));
    let _ = std::fs::remove_dir_all(dir);
}
