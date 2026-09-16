//! Real SQLite/PostgreSQL retry relocation and nested execution evidence.
//! Build with `cargo build -p rustvello-otel --example fail_once --features rustvello/sqlite`.

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustvello::app::RustvelloApp;
use rustvello::builder::Rustvello;
use rustvello_core::context::{get_invocation_context, get_or_create_runner_context};
use rustvello_core::error::RustvelloError;
use rustvello_core::observability::{
    capture_w3c_trace_context, AsyncExportConfig, AsyncExportStats, BoundedAsyncEmitter, EventLevel,
};
use rustvello_core::runner::Runner;
use rustvello_otel::{OtlpExportStats, OtlpLifecycleConfig, OtlpLifecycleExporter};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::TraceContextCarrier;
use rustvello_proto::status::InvocationStatus;
use serde_json::{json, Value};

const APP_ID: &str = "lc04-rust";
const TRACE_ID: &str = "11111111111111111111111111111111";
const INCOMING_SPAN_ID: &str = "1111111111111111";
const TRACEPARENT: &str = "00-11111111111111111111111111111111-1111111111111111-01";
const UNSAMPLED_TRACE_ID: &str = "33333333333333333333333333333333";

#[cfg_attr(feature = "network-acceptance", allow(clippy::unnecessary_wraps))]
fn runtime_builder(
    database: &str,
) -> Result<rustvello::builder::RustvelloBuilder, Box<dyn std::error::Error>> {
    let builder = Rustvello::builder().app_id(APP_ID);
    if let Ok(dsn) = std::env::var("RUSTVELLO_NETWORK_DSN") {
        #[cfg(feature = "network-acceptance")]
        {
            let builder = match std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME") {
                Ok(hostname) => {
                    let ca_path = std::env::var("RUSTVELLO_POSTGRES_TLS_CA")?;
                    let tls = rustvello::postgres::db::PostgresTlsOptions::private_ca_pem(
                        hostname,
                        std::fs::read(ca_path)?,
                    )?;
                    builder.postgres_tls_with_options(
                        &dsn,
                        APP_ID,
                        rustvello::postgres::db::PostgresOptions::default(),
                        tls,
                    )
                }
                Err(_) => builder.postgres(&dsn, APP_ID),
            };
            return Ok(builder);
        }
        #[cfg(not(feature = "network-acceptance"))]
        {
            let _ = dsn;
            return Err("rebuild producer with network-acceptance".into());
        }
    }
    Ok(builder.sqlite(database, APP_ID))
}

fn stats_json(stats: AsyncExportStats, delivery: OtlpExportStats) -> Value {
    let mut result = json!({
        "accepted": stats.accepted,
        "dropped": stats.dropped,
        "exported": stats.exported,
        "export_failed": stats.export_failed,
        "rejected_after_shutdown": stats.rejected_after_shutdown,
        "enqueue_nanos_total": stats.enqueue_nanos_total,
        "enqueue_nanos_max": stats.enqueue_nanos_max,
        "otlp_failed_events": delivery.failed_events,
        "otlp_incomplete_attempts": delivery.incomplete_attempts,
        "otlp_duplicate_events": delivery.duplicate_events,
        "otlp_unsampled_attempts": delivery.unsampled_attempts,
        "otlp_response_warnings": delivery.response_warnings,
        "otlp_unprocessed_events": delivery.unprocessed_events,
    });
    for (signal, stats) in [
        ("traces", delivery.traces),
        ("logs", delivery.logs),
        ("metrics", delivery.metrics),
    ] {
        for (field, value) in [
            ("attempted", stats.attempted),
            ("acknowledged", stats.acknowledged),
            ("rejected", stats.rejected),
            ("failed", stats.failed),
            ("not_sent", stats.not_sent),
        ] {
            result[format!("otlp_{signal}_{field}")] = json!(value);
        }
    }
    result
}

fn record_execution() -> Value {
    let context = get_invocation_context().expect("task invocation context");
    let runner = get_or_create_runner_context();
    assert_eq!(runner.pid, std::process::id());
    assert!(
        runner.parent_ctx.is_some(),
        "task is not running in a worker"
    );
    let carrier = capture_w3c_trace_context();
    let traceparent = carrier.traceparent.expect("active execution span");
    let parts: Vec<_> = traceparent.split('-').collect();
    json!({
        "invocation_id": context.invocation_id.to_string(),
        "task_id": context.task_id.to_string(),
        "attempt": context.num_retries,
        "pid": std::process::id(),
        "worker_id": runner.runner_id.to_string(),
        "trace_id": parts[1],
        "execute_span_id": parts[2],
        "execute_traceparent": traceparent,
        "child_invocation_ids": [],
    })
}

async fn make_app(
    database: &str,
    emitter: &BoundedAsyncEmitter,
    executions: Arc<Mutex<Vec<Value>>>,
) -> Result<RustvelloApp, Box<dyn std::error::Error>> {
    let mut child_app = runtime_builder(database)?
        .build()
        .await?
        .with_event_emitter(EventLevel::TaskLifecycle, emitter.clone());
    let child_executions = Arc::clone(&executions);
    child_app.register_task(
        TaskId::new("lc04", "rust_child"),
        TaskConfig::default(),
        Arc::new(move |_| {
            child_executions.lock().unwrap().push(record_execution());
            Ok("\"child-ok\"".to_owned())
        }),
    )?;
    let child_app = Arc::new(child_app);
    let mut app = runtime_builder(database)?
        .build()
        .await?
        .with_event_emitter(EventLevel::TaskLifecycle, emitter.clone());
    let child_task = child_app
        .task_registry()
        .get(&TaskId::new("lc04", "rust_child"))
        .expect("registered child");
    app.register_task(
        child_task.task_id.clone(),
        child_task.config.clone(),
        Arc::clone(&child_task.func),
    )?;
    let unsampled_executions = Arc::clone(&executions);
    app.register_task(
        TaskId::new("lc04", "rust_unsampled"),
        TaskConfig::default(),
        Arc::new(move |_| {
            unsampled_executions
                .lock()
                .unwrap()
                .push(record_execution());
            Ok("\"unsampled-ok\"".to_owned())
        }),
    )?;
    let mut retry_config = TaskConfig::default();
    retry_config.max_retries = 1;
    retry_config.blocking = true;
    app.register_task(
        TaskId::new("lc04", "rust_fail_once"),
        retry_config,
        Arc::new(move |_| {
            let mut execution = record_execution();
            let attempt = get_invocation_context().expect("task context").num_retries;
            if attempt == 0 {
                executions.lock().unwrap().push(execution);
                return Err(RustvelloError::TaskExecution {
                    error_type: "RuntimeError".to_owned(),
                    message: "password=must-not-leave-rustvello".to_owned(),
                    traceback: None,
                });
            }
            // The blocking task thread retains the real active execution carrier
            // while the normal submission API persists the child's incoming parent.
            let child_id = tokio::runtime::Handle::current().block_on(child_app.submit(
                &TaskId::new("lc04", "rust_child"),
                SerializedArguments::new(),
            ))?;
            execution["child_invocation_ids"] = json!([child_id.to_string()]);
            executions.lock().unwrap().push(execution);
            Ok("\"ok\"".to_owned())
        }),
    )?;
    Ok(app)
}

fn aggregate_export(processes: &[Value]) -> Value {
    let mut totals = serde_json::Map::new();
    for process in processes {
        for (key, value) in process["export"]["lifecycle"].as_object().unwrap() {
            let value = value.as_u64().unwrap();
            let previous = totals.get(key).and_then(Value::as_u64).unwrap_or(0);
            totals.insert(
                key.clone(),
                json!(if key.ends_with("_max") {
                    previous.max(value)
                } else {
                    previous + value
                }),
            );
        }
    }
    json!({"lifecycle": totals})
}

fn check_export(export: &Value) {
    for stats in export.as_object().unwrap().values() {
        assert_eq!(stats["accepted"], stats["exported"], "{stats}");
        for key in ["dropped", "export_failed", "rejected_after_shutdown"] {
            assert_eq!(stats[key], 0, "{stats}");
        }
        for signal in ["traces", "logs", "metrics"] {
            assert_eq!(
                stats[format!("otlp_{signal}_attempted")],
                stats[format!("otlp_{signal}_acknowledged")]
            );
            assert_eq!(stats[format!("otlp_{signal}_rejected")], 0);
            assert_eq!(stats[format!("otlp_{signal}_failed")], 0);
            assert_eq!(stats[format!("otlp_{signal}_not_sent")], 0);
        }
        assert_eq!(stats["otlp_failed_events"], 0);
        assert_eq!(stats["otlp_incomplete_attempts"], 0);
        assert_eq!(stats["otlp_unprocessed_events"], 0);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let started_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let worker = std::env::args().any(|arg| arg == "--worker");
    let database = match std::env::var("RUSTVELLO_LC_DATABASE") {
        Ok(path) => path,
        Err(_) if worker => return Err("worker requires RUSTVELLO_LC_DATABASE".into()),
        Err(_) => {
            let directory = std::env::temp_dir().join(format!(
                "rustvello-lc04-{}-{started_ns}",
                std::process::id()
            ));
            std::fs::create_dir(&directory)?;
            directory.join("rust.sqlite").to_string_lossy().into_owned()
        }
    };
    let database = if Path::new(&database).is_absolute() {
        database
    } else {
        std::env::current_dir()?
            .join(database)
            .to_string_lossy()
            .into_owned()
    };
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let database_path = Path::new(&database);
    let stem = database_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let extension = database_path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();
    let effective_database = database_path.with_file_name(format!("{stem}_{APP_ID}{extension}"));
    let token = std::env::var("OTLP_BEARER_TOKEN").or_else(|_| std::env::var("POET_OTLP_TOKEN"))?;
    let exporter = OtlpLifecycleExporter::new(OtlpLifecycleConfig::new(endpoint, token))?;
    let accounting = exporter.accounting();
    let emitter = BoundedAsyncEmitter::new(
        AsyncExportConfig {
            scheduled_delay: Duration::from_millis(10),
            ..AsyncExportConfig::default()
        },
        exporter,
    );
    let executions = Arc::new(Mutex::new(Vec::new()));
    let app = make_app(&database, &emitter, Arc::clone(&executions)).await?;
    app.require_crash_consistent_publication()?;
    if std::env::var_os("RUSTVELLO_NETWORK_DSN").is_none() {
        assert!(
            effective_database.is_file(),
            "SQLite app-qualified file is missing"
        );
    } else {
        assert!(
            !effective_database.is_file(),
            "network fixture must not create a task database file"
        );
    }
    let network = std::env::var_os("RUSTVELLO_NETWORK_DSN").is_some();
    let public_database = if network {
        Value::Null
    } else {
        json!(database)
    };
    let public_effective_database = if network {
        Value::Null
    } else {
        json!(effective_database)
    };
    let backend = if network { "postgres" } else { "sqlite" };
    let mut process = json!({
        "role": if worker { "worker" } else { "submitter" },
        "pid": std::process::id(),
        "started_unix_nano": started_ns,
        "backend": backend,
        "database": public_database,
        "effective_database": public_effective_database,
        "executions": [],
        "export": {},
    });
    if worker {
        let runner = app.into_runner();
        process["runner_id"] = json!(runner.runner_id().to_string());
        let result = tokio::time::timeout_at(deadline, runner.run_one()).await?;
        runner.shutdown().await?;
        process["export"] = json!({"lifecycle": stats_json(emitter.shutdown(Duration::from_secs(5))?, accounting.stats())});
        assert!(result?, "worker found no queued invocation");
        process["executions"] = json!(*executions.lock().unwrap());
        assert_eq!(process["executions"].as_array().unwrap().len(), 1);
        check_export(&process["export"]);
        println!("{process}");
        return Ok(());
    }

    let task_id = TaskId::new("lc04", "rust_fail_once");
    let invocation_id = app
        .submit_with_trace_context(
            &task_id,
            SerializedArguments::new(),
            Some(TraceContextCarrier {
                traceparent: Some(TRACEPARENT.to_owned()),
                tracestate: Some("ih=rust".to_owned()),
            }),
        )
        .await?;
    let mut processes = vec![process];
    let mut unsampled_id = None;
    for index in 0..4 {
        if index == 3 {
            unsampled_id = Some(
                app.submit_with_trace_context(
                    &TaskId::new("lc04", "rust_unsampled"),
                    SerializedArguments::new(),
                    Some(TraceContextCarrier {
                        traceparent: Some(format!("00-{UNSAMPLED_TRACE_ID}-{INCOMING_SPAN_ID}-00")),
                        tracestate: Some("ih=rust".to_owned()),
                    }),
                )
                .await?,
            );
        }
        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .arg("--worker")
            .env("RUSTVELLO_LC_DATABASE", &database)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().expect("launched worker PID");
        // Drain both pipes while waiting, so diagnostics cannot deadlock the worker.
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let read = |mut pipe: Box<dyn tokio::io::AsyncRead + Unpin + Send>| async move {
            let mut bytes = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut pipe, &mut bytes).await?;
            Ok::<_, std::io::Error>(bytes)
        };
        let output = tokio::time::timeout_at(deadline, async {
            tokio::try_join!(child.wait(), read(Box::new(stdout)), read(Box::new(stderr)),)
        })
        .await;
        let (status, stdout, stderr) = match output {
            Ok(result) => result?,
            Err(error) => {
                child.kill().await?;
                child.wait().await?;
                return Err(error.into());
            }
        };
        if !status.success() {
            let token = std::env::var("OTLP_BEARER_TOKEN")
                .or_else(|_| std::env::var("POET_OTLP_TOKEN"))
                .unwrap_or_default();
            let stderr = String::from_utf8_lossy(&stderr).replace(&token, "<redacted>");
            return Err(format!("worker {pid} failed: {stderr}").into());
        }
        let evidence: Value = serde_json::from_slice(&stdout)?;
        assert_eq!(evidence["pid"], pid);
        assert_eq!(evidence["effective_database"], public_effective_database);
        assert_eq!(evidence["backend"], backend);
        processes.push(evidence);
    }
    processes[0]["export"] = json!({"lifecycle": stats_json(emitter.shutdown(Duration::from_secs(5))?, accounting.stats())});
    let process_ids: Vec<_> = processes[1..3].iter().map(|p| p["pid"].clone()).collect();
    let worker_process_ids: Vec<_> = processes[1..].iter().map(|p| p["pid"].clone()).collect();
    let pids: std::collections::HashSet<_> = processes
        .iter()
        .map(|p| p["pid"].as_u64().unwrap())
        .collect();
    assert_eq!(pids.len(), 5);
    let attempts = vec![
        processes[1]["executions"][0].clone(),
        processes[2]["executions"][0].clone(),
    ];
    for (attempt, execution) in attempts.iter().enumerate() {
        assert_eq!(execution["attempt"], attempt);
        assert_eq!(execution["invocation_id"], invocation_id.to_string());
        assert_ne!(execution["execute_traceparent"], TRACEPARENT);
    }
    assert_ne!(
        attempts[0]["execute_span_id"],
        attempts[1]["execute_span_id"]
    );
    assert_eq!(
        app.get_status(&invocation_id).await?,
        InvocationStatus::Success
    );
    assert_eq!(
        app.get_result(&invocation_id).await?.as_deref(),
        Some("\"ok\"")
    );
    let child_ids = attempts[1]["child_invocation_ids"].as_array().unwrap();
    assert_eq!(child_ids.len(), 1);
    let child_id = InvocationId::from_string(child_ids[0].as_str().unwrap());
    let stored = app.state_backend().get_invocation(&invocation_id).await?;
    assert_eq!(
        stored.trace_context.traceparent.as_deref(),
        Some(TRACEPARENT)
    );
    let child_stored = app.state_backend().get_invocation(&child_id).await?;
    assert_eq!(
        child_stored.parent_invocation_id.as_ref(),
        Some(&invocation_id)
    );
    assert_eq!(
        child_stored.trace_context.traceparent.as_deref(),
        attempts[1]["execute_traceparent"].as_str(),
    );
    assert_eq!(app.get_status(&child_id).await?, InvocationStatus::Success);
    assert_eq!(
        app.get_result(&child_id).await?.as_deref(),
        Some("\"child-ok\"")
    );
    let mut child = processes[3]["executions"][0].clone();
    assert_eq!(child["invocation_id"], child_id.to_string());
    child["parent_invocation_id"] = json!(invocation_id.to_string());
    child["incoming_traceparent"] = json!(child_stored.trace_context.traceparent);
    let unsampled_id = unsampled_id.expect("unsampled invocation submitted");
    assert_eq!(
        app.get_status(&unsampled_id).await?,
        InvocationStatus::Success
    );
    assert_eq!(
        app.get_result(&unsampled_id).await?.as_deref(),
        Some("\"unsampled-ok\"")
    );
    let unsampled = &processes[4]["executions"][0];
    assert_eq!(unsampled["invocation_id"], unsampled_id.to_string());
    assert_eq!(unsampled["trace_id"], UNSAMPLED_TRACE_ID);
    assert_ne!(unsampled["execute_span_id"], INCOMING_SPAN_ID);
    assert!(unsampled["execute_traceparent"]
        .as_str()
        .unwrap()
        .ends_with("-00"));
    let export = aggregate_export(&processes);
    check_export(&export);
    println!(
        "{}",
        json!({
            "app_id": APP_ID,
            "invocation_id": invocation_id.to_string(),
            "task_id": task_id.to_string(),
            "trace_id": TRACE_ID,
            "traceparent": TRACEPARENT,
            "incoming_parent_span_id": INCOMING_SPAN_ID,
            "backend": backend,
            "database": public_database,
            "effective_database": public_effective_database,
            "child_invocation_ids": child_ids,
            "children": [child],
            "attempts": attempts,
            "process_ids": process_ids,
            "worker_process_ids": worker_process_ids,
            "unsampled_invocation_id": unsampled_id.to_string(),
            "unsampled_trace_id": UNSAMPLED_TRACE_ID,
            "unsampled_incoming_parent_span_id": INCOMING_SPAN_ID,
            "unsampled": unsampled,
            "processes": processes,
            "export": export,
        })
    );
    Ok(())
}
