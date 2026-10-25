//! Executor that runs task code in a pool of external worker processes.
//!
//! Built for Python, where one interpreter means one GIL: N worker processes
//! give N independent interpreters, so CPU-bound Python tasks run in parallel
//! while the control plane (polling, admission, heartbeats, recovery) stays in
//! this process. The protocol is language-agnostic JSON lines over the worker's
//! stdin/stdout:
//!
//! - worker start: the worker prints `{"ready": true, ...}` once it can execute;
//! - request: `{"protocol": 1, "invocation_id", "task_id", "language", "module",
//!   "name", "args": {key: json}, "num_retries", "parent_invocation_id",
//!   "traceparent", "tracestate", "workflow": {...} | null,
//!   "is_workflow_defining"}`;
//! - response: `{"ok": true, "result": "<json>"}` or
//!   `{"ok": false, "error_type", "message", "traceback"}`.
//!
//! A worker that exits or breaks the protocol is dropped and replaced on the
//! next request; the in-flight invocation fails with error type
//! `WorkerProcessCrashed` so the task's retry policy decides what happens.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, Semaphore};

use rustvello_core::context::{InvocationContext, RunnerContext};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::task::DynTask;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::ExecutorKind;

use super::TaskExecutor;

/// How worker processes are launched.
#[derive(Clone, Debug)]
pub struct SubprocessSpec {
    /// Program and arguments of one worker process.
    pub command: Vec<String>,
    /// Extra environment variables for the worker processes.
    pub env: Vec<(String, String)>,
    /// Executor kind reported in runner contexts and monitoring.
    pub kind: ExecutorKind,
    /// Canonical ids (`language::module.name`) of the tasks this runner knows.
    /// A worker whose ready line lists `tasks` must register every one of
    /// them, or it is refused with the missing tasks named. Filled from the
    /// runner's registry by `with_subprocess_executor` when left empty.
    pub expected_tasks: Vec<String>,
}

struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Clone)]
pub(crate) struct SubprocessExecutor {
    spec: Arc<SubprocessSpec>,
    idle: Arc<Mutex<Vec<WorkerProcess>>>,
    permits: Arc<Semaphore>,
    size: usize,
}

impl std::fmt::Debug for SubprocessExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubprocessExecutor")
            .field("command", &self.spec.command)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

fn worker_crash(message: impl Into<String>) -> RustvelloError {
    RustvelloError::TaskExecution {
        error_type: "WorkerProcessCrashed".to_owned(),
        message: message.into(),
        traceback: None,
    }
}

/// Tasks of `expected` that a worker's ready line does not list. `None` when
/// nothing is missing or the worker announces no `tasks` (an older protocol).
fn missing_tasks(expected: &[String], announcement: &serde_json::Value) -> Option<Vec<String>> {
    let announced = announcement.get("tasks")?.as_array()?;
    let announced: std::collections::HashSet<&str> = announced
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    let missing: Vec<String> = expected
        .iter()
        .filter(|task| !announced.contains(task.as_str()))
        .cloned()
        .collect();
    (!missing.is_empty()).then_some(missing)
}

impl SubprocessExecutor {
    pub(crate) fn new(spec: SubprocessSpec, size: usize) -> Self {
        let size = size.max(1);
        Self {
            spec: Arc::new(spec),
            idle: Arc::new(Mutex::new(Vec::with_capacity(size))),
            permits: Arc::new(Semaphore::new(size)),
            size,
        }
    }

    async fn spawn_worker(&self) -> RustvelloResult<WorkerProcess> {
        let (program, args) =
            self.spec
                .command
                .split_first()
                .ok_or_else(|| RustvelloError::Configuration {
                    message: "subprocess executor needs a worker command".to_owned(),
                })?;
        let mut child = Command::new(program)
            .args(args)
            .envs(self.spec.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| RustvelloError::Configuration {
                message: format!("cannot start worker process {program:?}: {error}"),
            })?;
        let stdin = child.stdin.take().ok_or_else(|| RustvelloError::Internal {
            message: "worker stdin not piped".to_owned(),
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RustvelloError::Internal {
                message: "worker stdout not piped".to_owned(),
            })?;
        let mut worker = WorkerProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        };
        // The worker announces readiness after importing the application, so an
        // import failure surfaces here instead of failing every invocation.
        let mut ready = String::new();
        let read = worker.stdout.read_line(&mut ready).await;
        let announcement = match read {
            Ok(n) if n > 0 => serde_json::from_str::<serde_json::Value>(ready.trim()).ok(),
            _ => None,
        };
        let announced = announcement
            .as_ref()
            .and_then(|value| value.get("ready").and_then(serde_json::Value::as_bool))
            .unwrap_or(false);
        if !announced {
            let _ = worker.child.kill().await;
            return Err(RustvelloError::Configuration {
                message: format!(
                    "worker process {program:?} did not announce readiness (got {ready:?})"
                ),
            });
        }
        let announcement = announcement.unwrap_or_default();
        if let Some(missing) = missing_tasks(&self.spec.expected_tasks, &announcement) {
            let _ = worker.child.kill().await;
            let app = announcement
                .get("app")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unknown>");
            let registered = announcement
                .get("tasks")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len);
            return Err(RustvelloError::Configuration {
                message: format!(
                    "worker process {program:?} (app {app:?}) registered {registered} task(s) but this \
                     runner knows {}; missing: {}. A worker process imports only its import_path, so \
                     that module must import every module that registers tasks",
                    self.spec.expected_tasks.len(),
                    missing.join(", ")
                ),
            });
        }
        tracing::info!(pid = ?worker.child.id(), "worker process ready");
        Ok(worker)
    }

    async fn checkout(&self) -> RustvelloResult<WorkerProcess> {
        if let Some(worker) = self.idle.lock().await.pop() {
            return Ok(worker);
        }
        self.spawn_worker().await
    }

    fn request_json(
        task: &dyn DynTask,
        args: &SerializedArguments,
        invocation: &InvocationContext,
    ) -> String {
        let task_id = task.task_id();
        let args: serde_json::Map<String, serde_json::Value> = args
            .0
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        let workflow = invocation.workflow.as_ref().map(|workflow| {
            serde_json::json!({
                "workflow_id": workflow.workflow_id.to_string(),
                "workflow_type": workflow.workflow_type.to_string(),
                "parent_id": workflow.parent_id.as_ref().map(ToString::to_string),
            })
        });
        serde_json::json!({
            "protocol": 1,
            "invocation_id": invocation.invocation_id.to_string(),
            "task_id": task_id.to_string(),
            "language": task_id.language().as_str(),
            "module": task_id.module(),
            "name": task_id.name(),
            "args": args,
            "num_retries": invocation.num_retries,
            "parent_invocation_id": invocation.parent_invocation_id.as_ref().map(ToString::to_string),
            "traceparent": invocation.trace_context.traceparent,
            "tracestate": invocation.trace_context.tracestate,
            "workflow": workflow,
            "is_workflow_defining": invocation.is_workflow_defining,
        })
        .to_string()
    }

    async fn round_trip(worker: &mut WorkerProcess, request: &str) -> RustvelloResult<String> {
        worker
            .stdin
            .write_all(request.as_bytes())
            .await
            .map_err(|error| worker_crash(format!("cannot write to worker: {error}")))?;
        worker
            .stdin
            .write_all(b"\n")
            .await
            .map_err(|error| worker_crash(format!("cannot write to worker: {error}")))?;
        worker
            .stdin
            .flush()
            .await
            .map_err(|error| worker_crash(format!("cannot flush worker stdin: {error}")))?;
        let mut line = String::new();
        let read = worker
            .stdout
            .read_line(&mut line)
            .await
            .map_err(|error| worker_crash(format!("cannot read from worker: {error}")))?;
        if read == 0 {
            return Err(worker_crash(
                "worker process exited while executing the task",
            ));
        }
        let response: serde_json::Value = serde_json::from_str(line.trim()).map_err(|error| {
            worker_crash(format!("worker returned invalid JSON ({error}): {line:?}"))
        })?;
        if response.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
            return Ok(response
                .get("result")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("null")
                .to_owned());
        }
        let field = |key: &str| {
            response
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        };
        Err(RustvelloError::TaskExecution {
            error_type: field("error_type").unwrap_or_else(|| "TaskExecutionError".to_owned()),
            message: field("message").unwrap_or_default(),
            traceback: field("traceback"),
        })
    }
}

#[async_trait]
impl TaskExecutor for SubprocessExecutor {
    fn kind(&self) -> ExecutorKind {
        self.spec.kind
    }

    /// Spawn the first worker now: its ready line proves the command starts,
    /// the application imports, and the worker registers every task this
    /// runner knows. The worker is kept for the first invocation.
    async fn start(&self) -> RustvelloResult<()> {
        let worker = self.spawn_worker().await?;
        self.idle.lock().await.push(worker);
        Ok(())
    }

    async fn execute(
        &self,
        task: Arc<dyn DynTask>,
        args: SerializedArguments,
        invocation_context: InvocationContext,
        _runner_context: RunnerContext,
    ) -> RustvelloResult<String> {
        let _permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|error| RustvelloError::Internal {
                message: format!("subprocess executor closed: {error}"),
            })?;
        let mut worker = self.checkout().await?;
        let request = Self::request_json(task.as_ref(), &args, &invocation_context);
        let outcome = Self::round_trip(&mut worker, &request).await;
        match &outcome {
            // Protocol or process failures leave the worker unusable: drop it (kill_on_drop).
            Err(RustvelloError::TaskExecution { error_type, .. })
                if error_type == "WorkerProcessCrashed" =>
            {
                let _ = worker.child.kill().await;
            }
            _ => self.idle.lock().await.push(worker),
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustvello_core::task::{TaskDefinition, TaskRegistry};
    use rustvello_proto::config::TaskConfig;
    use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId, TaskLanguage};

    use super::*;

    /// A tiny protocol-speaking worker written in Python; skips when python3 is absent.
    fn python_worker(body: &str) -> Option<Vec<String>> {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .ok()?;
        let script =
            format!("import sys, json\nprint(json.dumps({{'ready': True}}), flush=True)\n{body}\n");
        Some(vec!["python3".to_owned(), "-c".to_owned(), script])
    }

    /// A worker whose ready line is `announcement` (a Python dict literal) and that then exits.
    fn python_worker_announcing(announcement: &str) -> Option<Vec<String>> {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .ok()?;
        let script = format!("import sys, json\nprint(json.dumps({announcement}), flush=True)\n");
        Some(vec!["python3".to_owned(), "-c".to_owned(), script])
    }

    fn echo_worker() -> Option<Vec<String>> {
        python_worker(
            "for line in sys.stdin:\n    req = json.loads(line)\n    out = {'ok': True, 'result': json.dumps({'echo': req['args'], 'task': req['task_id'], 'retries': req['num_retries']})}\n    print(json.dumps(out), flush=True)",
        )
    }

    fn task() -> Arc<dyn DynTask> {
        let task_id = TaskId::for_language(TaskLanguage::Python, "mod", "echo");
        let mut registry = TaskRegistry::new();
        registry
            .register(TaskDefinition::new(
                task_id.clone(),
                TaskConfig::default(),
                Arc::new(|_| {
                    panic!("in-process function must not run under the subprocess executor")
                }),
            ))
            .unwrap();
        registry.get_dyn(&task_id).unwrap()
    }

    fn invocation(num_retries: u32) -> InvocationContext {
        InvocationContext {
            invocation_id: InvocationId::new(),
            task_id: TaskId::for_language(TaskLanguage::Python, "mod", "echo"),
            workflow: None,
            is_workflow_defining: false,
            state_backend: None,
            parent_invocation_id: None,
            num_retries,
            trace_context: Default::default(),
        }
    }

    fn runner() -> RunnerContext {
        RunnerContext::new(
            RunnerId::new(),
            Arc::from("subprocess-test"),
            "SubprocessTest",
        )
    }

    fn spec(command: Vec<String>) -> SubprocessSpec {
        SubprocessSpec {
            command,
            expected_tasks: Vec::new(),
            env: vec![],
            kind: ExecutorKind::Python,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trips_arguments_and_context() {
        let Some(command) = echo_worker() else { return };
        let executor = SubprocessExecutor::new(spec(command), 2);
        let mut args = SerializedArguments::new();
        args.insert("x".to_owned(), "1".to_owned());
        let result = executor
            .execute(task(), args, invocation(3), runner())
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(value["echo"]["x"], "1");
        assert_eq!(value["task"], "python::mod.echo");
        assert_eq!(value["retries"], 3);
        assert_eq!(executor.kind(), ExecutorKind::Python);
        // the worker went back to the pool
        assert_eq!(executor.idle.lock().await.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_errors_keep_type_message_and_worker() {
        let Some(command) = python_worker(
            "for line in sys.stdin:\n    print(json.dumps({'ok': False, 'error_type': 'ValueError', 'message': 'bad input', 'traceback': 'tb'}), flush=True)",
        ) else {
            return;
        };
        let executor = SubprocessExecutor::new(spec(command), 1);
        let error = executor
            .execute(task(), SerializedArguments::new(), invocation(0), runner())
            .await
            .unwrap_err();
        match error {
            RustvelloError::TaskExecution {
                error_type,
                message,
                traceback,
            } => {
                assert_eq!(error_type, "ValueError");
                assert_eq!(message, "bad input");
                assert_eq!(traceback.as_deref(), Some("tb"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(executor.idle.lock().await.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crashed_worker_is_reported_and_replaced() {
        let Some(command) = python_worker("sys.stdin.readline()\nsys.exit(3)") else {
            return;
        };
        let executor = SubprocessExecutor::new(spec(command), 1);
        let error = executor
            .execute(task(), SerializedArguments::new(), invocation(0), runner())
            .await
            .unwrap_err();
        assert!(
            matches!(error, RustvelloError::TaskExecution { ref error_type, .. } if error_type == "WorkerProcessCrashed"),
            "{error:?}"
        );
        assert!(executor.idle.lock().await.is_empty());
        // the next call spawns a fresh worker instead of failing permanently
        let error = executor
            .execute(task(), SerializedArguments::new(), invocation(0), runner())
            .await
            .unwrap_err();
        assert!(matches!(error, RustvelloError::TaskExecution { .. }));
    }

    #[test]
    fn missing_tasks_compares_announced_with_expected() {
        let expected = vec![
            "python::mod.echo".to_owned(),
            "python::mod.other".to_owned(),
        ];
        let none = serde_json::json!({"ready": true});
        assert!(
            missing_tasks(&expected, &none).is_none(),
            "no tasks key: not checked"
        );
        let all = serde_json::json!({"ready": true, "tasks": ["python::mod.echo", "python::mod.other", "python::mod.extra"]});
        assert!(missing_tasks(&expected, &all).is_none());
        let fewer = serde_json::json!({"ready": true, "tasks": ["python::mod.echo"]});
        assert_eq!(
            missing_tasks(&expected, &fewer),
            Some(vec!["python::mod.other".to_owned()])
        );
    }

    #[tokio::test]
    async fn worker_with_fewer_tasks_than_the_runner_is_refused() {
        let Some(command) =
            python_worker_announcing("{'ready': True, 'app': 'loader:app', 'tasks': []}")
        else {
            return;
        };
        let mut spec = spec(command);
        spec.expected_tasks = vec!["python::mod.echo".to_owned()];
        let executor = SubprocessExecutor::new(spec, 1);
        let error = executor
            .execute(task(), SerializedArguments::new(), invocation(0), runner())
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(
            matches!(error, RustvelloError::Configuration { .. })
                && message.contains("loader:app")
                && message.contains("python::mod.echo"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn unready_worker_is_a_configuration_error() {
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let command = vec![
            "python3".to_owned(),
            "-c".to_owned(),
            "import sys; sys.exit(1)".to_owned(),
        ];
        let executor = SubprocessExecutor::new(spec(command), 1);
        let error = executor
            .execute(task(), SerializedArguments::new(), invocation(0), runner())
            .await
            .unwrap_err();
        assert!(
            matches!(error, RustvelloError::Configuration { .. }),
            "{error:?}"
        );
    }
}
