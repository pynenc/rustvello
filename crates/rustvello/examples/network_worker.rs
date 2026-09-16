//! Public Rust-only consumer. DSN stays in the environment, never argv or receipts.
use rustvello::prelude::*;
use std::time::Duration;

#[rustvello::task(module = "network_example", max_retries = 1)]
fn double(value: u64) -> u64 {
    value * 2
}

#[rustvello::task(module = "network_example", max_retries = 1)]
fn delayed(value: u64, millis: u64) -> u64 {
    std::thread::sleep(Duration::from_millis(millis.min(5_000)));
    value * 2
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("RUSTVELLO_POSTGRES_DSN")?;
    let name = std::env::var("RUSTVELLO_APP_ID").unwrap_or_else(|_| "network_example".into());
    let postgres_options = rustvello::postgres::db::PostgresOptions {
        delivery_lease_ms: 5_000,
        ..Default::default()
    };
    let builder = Rustvello::builder().app_id(&name);
    let builder = match std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME") {
        Ok(hostname) => {
            let ca_path = std::env::var("RUSTVELLO_POSTGRES_TLS_CA")?;
            let ca_pem = std::fs::read(ca_path)?;
            let tls =
                rustvello::postgres::db::PostgresTlsOptions::private_ca_pem(hostname, ca_pem)?;
            builder.postgres_tls_with_options(&dsn, &name, postgres_options, tls)
        }
        Err(_) => builder.postgres_with_options(&dsn, &name, postgres_options),
    };
    let mut app = builder
        .heartbeat_interval(1)
        .runner_dead_after_seconds(10)
        .max_pending_seconds(10)
        .recovery_check_interval(1)
        .build()
        .await?;
    app.require_crash_consistent_publication()?;
    app.register(DoubleTask::new())?;
    app.register(DelayedTask::new())?;
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("submit-delayed") => {
            let id =
                InvocationId::from_string(args.get(2).ok_or("invocation ID required")?.clone());
            let handle = app
                .submit_call_with_id(
                    id,
                    &DelayedTask::new(),
                    DelayedParams {
                        value: 21,
                        millis: 3_000,
                    },
                    None,
                )
                .await?;
            println!("{}", handle.invocation_id());
        }
        Some("history") => {
            let id =
                InvocationId::from_string(args.get(2).ok_or("invocation ID required")?.clone());
            println!(
                "{}",
                serde_json::to_string(&app.state_backend().get_history(&id).await?)?
            );
        }
        Some("submit") => {
            let id = InvocationId::from_string(
                args.get(2)
                    .ok_or("submit requires a durable operation ID")?
                    .clone(),
            );
            let handle = app
                .submit_call_with_id(id, &DoubleTask::new(), DoubleParams { value: 21 }, None)
                .await?;
            println!("{}", handle.invocation_id());
        }
        Some("worker") => {
            app.into_runner()
                .with_num_workers(2)
                .with_bounded_shutdown(
                    async {
                        let _ = tokio::signal::ctrl_c().await;
                    },
                    Duration::from_secs(10),
                )
                .await?;
        }
        Some("run-one") => {
            println!("{}", app.into_runner().run_one().await?);
        }
        Some("result") => {
            let id = InvocationId::from_string(
                args.get(2)
                    .ok_or("result requires an invocation ID")?
                    .clone(),
            );
            println!("{:?}", app.get_result(&id).await?);
        }
        _ => return Err(
            "expected submit ID | submit-delayed ID | worker | run-one | result ID | history ID"
                .into(),
        ),
    }
    Ok(())
}
