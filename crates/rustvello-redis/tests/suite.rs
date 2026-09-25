//! Integration tests using testcontainers to run Redis suite tests against real Redis.
//!
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-redis -- --ignored          # only Docker tests
//! cargo test -p rustvello-redis -- --include-ignored   # all tests
//! ```

use std::sync::Arc;
use std::{path::Path, process::Command};

use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::OrchestratorStatus;
use rustvello_core::publication::{PublicationChange, PublicationRoute, SubmissionPublication};
use rustvello_core::state_backend::{StateBackendCore, StoredRunnerContext};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, TraceContextCarrier};
use rustvello_proto::status::InvocationStatus;
use rustvello_redis::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::runners::AsyncRunner;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    GenericImage, ImageExt,
};
use testcontainers_modules::redis::Redis;

/// Start a Redis container and return the connection URI.
async fn redis_uri() -> (testcontainers::ContainerAsync<Redis>, String) {
    let container = Redis::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let uri = format!("redis://127.0.0.1:{port}/");
    (container, uri)
}

async fn restartable_redis_uri() -> (testcontainers::ContainerAsync<Redis>, String) {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let container = Redis::default()
        .with_mapped_port(port, 6379_u16.tcp())
        .start()
        .await
        .unwrap();
    (container, format!("redis://127.0.0.1:{port}/"))
}

fn openssl(arguments: &[&str], directory: &Path) {
    let status = Command::new("openssl")
        .args(arguments)
        .current_dir(directory)
        .status()
        .expect("openssl is required for the Redis TLS acceptance");
    assert!(status.success(), "openssl command failed: {arguments:?}");
}

fn tls_fixture() -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "rustvello-redis-tls-{}",
        rustvello_proto::identifiers::InvocationId::new()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    openssl(
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.crt",
            "-days",
            "1",
            "-subj",
            "/CN=Rustvello Redis test CA",
        ],
        &directory,
    );
    openssl(
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "other-ca.key",
            "-out",
            "other-ca.crt",
            "-days",
            "1",
            "-subj",
            "/CN=Untrusted Redis test CA",
        ],
        &directory,
    );
    openssl(
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ],
        &directory,
    );
    std::fs::write(
        directory.join("server.ext"),
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    .unwrap();
    openssl(
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.crt",
            "-days",
            "1",
            "-extfile",
            "server.ext",
        ],
        &directory,
    );
    // The key is copied into the container with its mode and owned by root
    // there, while Redis runs as the unprivileged `redis` user: openssl's
    // 0600 makes it unreadable ("Failed to load private key: Permission
    // denied"). A throwaway test key may be world-readable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            directory.join("server.key"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    directory
}

fn make_pool(uri: &str) -> Arc<RedisPool> {
    Arc::new(RedisPool::new(uri, "test").unwrap())
}

async fn make_broker() -> (testcontainers::ContainerAsync<Redis>, RedisBroker) {
    let (c, uri) = redis_uri().await;
    (c, RedisBroker::new(make_pool(&uri)))
}

async fn make_orchestrator() -> (testcontainers::ContainerAsync<Redis>, RedisOrchestrator) {
    let (c, uri) = redis_uri().await;
    (c, RedisOrchestrator::new(make_pool(&uri)))
}

async fn make_state_backend() -> (testcontainers::ContainerAsync<Redis>, RedisStateBackend) {
    let (c, uri) = redis_uri().await;
    (c, RedisStateBackend::new(make_pool(&uri)))
}

async fn make_trigger_store() -> (testcontainers::ContainerAsync<Redis>, RedisTriggerStore) {
    let (c, uri) = redis_uri().await;
    (c, RedisTriggerStore::new(make_pool(&uri)))
}

async fn make_client_data_store() -> (testcontainers::ContainerAsync<Redis>, RedisClientDataStore) {
    let (c, uri) = redis_uri().await;
    (c, RedisClientDataStore::new(make_pool(&uri)))
}

async fn make_triple() -> (testcontainers::ContainerAsync<Redis>, BackendTriple) {
    let (container, uri) = redis_uri().await;
    let pool = make_pool(&uri);
    let triple = BackendTriple {
        broker: Arc::new(RedisBroker::new(Arc::clone(&pool))),
        orchestrator: Arc::new(RedisOrchestrator::new(Arc::clone(&pool))),
        state_backend: Arc::new(RedisStateBackend::new(pool)),
    };
    (container, triple)
}

mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}

mod orchestrator_suite {
    use super::*;
    rustvello_test_suite::async_orchestrator_suite!(make_orchestrator());
}

mod state_backend_suite {
    use super::*;
    rustvello_test_suite::async_state_backend_suite!(make_state_backend());
}

mod trigger_suite {
    use super::*;
    rustvello_test_suite::async_trigger_suite!(make_trigger_store());
}

mod client_data_store_suite {
    use super::*;
    rustvello_test_suite::async_client_data_store_suite!(make_client_data_store());
}

mod concurrency_suite {
    use super::*;
    rustvello_test_suite::async_concurrency_suite!(make_orchestrator());
}

mod lifecycle_suite {
    use super::*;
    rustvello_test_suite::async_lifecycle_suite!(make_triple());
}

/// Two sets of backends sharing the same Redis instance but different app_ids.
async fn make_isolation_pair() -> (
    testcontainers::ContainerAsync<Redis>,
    RedisBroker,
    RedisBroker,
    RedisOrchestrator,
    RedisOrchestrator,
    RedisStateBackend,
    RedisStateBackend,
    RedisTriggerStore,
    RedisTriggerStore,
    RedisClientDataStore,
    RedisClientDataStore,
) {
    let (container, uri) = redis_uri().await;

    let pool_a = Arc::new(RedisPool::new(&uri, "app_a").unwrap());
    let pool_b = Arc::new(RedisPool::new(&uri, "app_b").unwrap());

    (
        container,
        RedisBroker::new(Arc::clone(&pool_a)),
        RedisBroker::new(Arc::clone(&pool_b)),
        RedisOrchestrator::new(Arc::clone(&pool_a)),
        RedisOrchestrator::new(Arc::clone(&pool_b)),
        RedisStateBackend::new(Arc::clone(&pool_a)),
        RedisStateBackend::new(Arc::clone(&pool_b)),
        RedisTriggerStore::new(Arc::clone(&pool_a)),
        RedisTriggerStore::new(Arc::clone(&pool_b)),
        RedisClientDataStore::new(Arc::clone(&pool_a)),
        RedisClientDataStore::new(Arc::clone(&pool_b)),
    )
}

mod isolation_suite {
    use super::*;
    rustvello_test_suite::async_isolation_suite!(make_isolation_pair());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn crash_consistent_publication_and_delivery_lease() {
    let (container, uri) = restartable_redis_uri().await;
    let pool = Arc::new(
        RedisPool::new_with_options(
            &uri,
            "publication",
            RedisOptions {
                delivery_lease_ms: 100,
                max_queue_rows: 2,
                require_durable_server: true,
                ..RedisOptions::default()
            },
        )
        .unwrap(),
    );
    pool.verify_server_policy().await.unwrap();
    let broker = RedisBroker::new(Arc::clone(&pool));
    let orchestrator = RedisOrchestrator::new(Arc::clone(&pool));
    let state = RedisStateBackend::new(Arc::clone(&pool));
    let publication = orchestrator.runtime_publication().unwrap();
    assert_eq!(publication.domain(), broker.publication_domain().unwrap());
    assert_eq!(publication.domain(), state.publication_domain().unwrap());

    let task_id = TaskId::new("test", "durable");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let invocation_id = InvocationId::new();
    let invocation = InvocationDTO::new(invocation_id.clone(), task_id, call.call_id.clone());
    let runner = RunnerId::from_string("submitter");
    let submission = SubmissionPublication {
        invocation,
        call,
        runner_id: runner.clone(),
        runner_context: Some(StoredRunnerContext::current("submitter", "test")),
        workflow_root: false,
        cc_arguments: None,
        route: PublicationRoute {
            queue: "default".into(),
            priority: 0.0,
        },
    };
    assert!(publication.submit(submission.clone()).await.unwrap());
    assert!(!publication.submit(submission.clone()).await.unwrap());
    let mut conflicting = submission;
    conflicting.route.priority = 1.0;
    assert!(publication.submit(conflicting).await.is_err());

    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(invocation_id.clone())
    );
    tokio::time::sleep(std::time::Duration::from_millis(125)).await;
    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(invocation_id.clone())
    );

    let worker = RunnerId::from_string("worker-a");
    publication
        .change(
            &invocation_id,
            &worker,
            PublicationChange::Status(InvocationStatus::Pending),
            false,
        )
        .await
        .unwrap();
    publication
        .change(
            &invocation_id,
            &worker,
            PublicationChange::Status(InvocationStatus::Running),
            false,
        )
        .await
        .unwrap();
    let identity = publication
        .begin_execution(&invocation_id, &worker, 0, &TraceContextCarrier::default())
        .await
        .unwrap();
    assert_eq!(identity.attempt, 0);
    assert!(publication
        .change(
            &invocation_id,
            &RunnerId::from_string("stale-worker"),
            PublicationChange::Success("wrong".into()),
            false,
        )
        .await
        .is_err());
    publication
        .change(
            &invocation_id,
            &worker,
            PublicationChange::Success("ok".into()),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        state.get_result(&invocation_id).await.unwrap().as_deref(),
        Some("ok")
    );
    assert_eq!(
        orchestrator
            .get_invocation_status(&invocation_id)
            .await
            .unwrap()
            .status,
        InvocationStatus::Success
    );
    assert_eq!(broker.count_invocations(None).await.unwrap(), 0);

    let recovery_call = CallDTO::new(TaskId::new("test", "recovery"), SerializedArguments::new());
    let recovery_id = InvocationId::new();
    publication
        .submit(SubmissionPublication {
            invocation: InvocationDTO::new(
                recovery_id.clone(),
                recovery_call.task_id.clone(),
                recovery_call.call_id.clone(),
            ),
            call: recovery_call,
            runner_id: runner.clone(),
            runner_context: None,
            workflow_root: false,
            cc_arguments: None,
            route: PublicationRoute {
                queue: "default".into(),
                priority: 0.0,
            },
        })
        .await
        .unwrap();
    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(recovery_id.clone())
    );
    publication
        .change(
            &recovery_id,
            &RunnerId::from_string("lost-worker"),
            PublicationChange::Status(InvocationStatus::Pending),
            false,
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert!(publication
        .change(
            &recovery_id,
            &RunnerId::from_string("recovery-service"),
            PublicationChange::Recover {
                status: InvocationStatus::PendingRecovery,
                stale_after_seconds: 1,
                route: PublicationRoute {
                    queue: "default".into(),
                    priority: 0.0,
                },
            },
            false,
        )
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(recovery_id.clone())
    );

    container.stop().await.unwrap();
    assert!(state.get_invocation(&recovery_id).await.is_err());
    container.start().await.unwrap();
    let restarted_port = container.get_host_port_ipv4(6379).await.unwrap();
    assert_eq!(uri, format!("redis://127.0.0.1:{restarted_port}/"));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let reconnect_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match state.get_invocation(&recovery_id).await {
            Ok(value) => {
                assert_eq!(value.status, InvocationStatus::Rerouted);
                break;
            }
            Err(_) if tokio::time::Instant::now() < reconnect_deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Redis did not reconnect after restart: {error}"),
        }
    }

    let reopened = RedisStateBackend::new(Arc::new(RedisPool::new(&uri, "publication").unwrap()));
    assert_eq!(
        reopened.get_invocation(&recovery_id).await.unwrap().status,
        InvocationStatus::Rerouted
    );
}

#[tokio::test]
#[ignore = "requires Docker and openssl"]
async fn tls_authentication_and_transport_rejections() {
    let tls = tls_fixture();
    let image = GenericImage::new("redis", "alpine")
        .with_exposed_port(6379_u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_copy_to("/tls/ca.crt", tls.join("ca.crt"))
        .with_copy_to("/tls/server.crt", tls.join("server.crt"))
        .with_copy_to("/tls/server.key", tls.join("server.key"))
        .with_cmd([
            "redis-server",
            "--port",
            "0",
            "--tls-port",
            "6379",
            "--tls-cert-file",
            "/tls/server.crt",
            "--tls-key-file",
            "/tls/server.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            "no",
            "--requirepass",
            "correct-password",
            "--appendonly",
            "yes",
            "--maxmemory-policy",
            "noeviction",
        ]);
    let container = image.start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let ca = std::fs::read(tls.join("ca.crt")).unwrap();
    let options = RedisOptions {
        connection_timeout_ms: 5_000,
        operation_timeout_ms: 5_000,
        require_durable_server: true,
        ..RedisOptions::default()
    };

    let valid = RedisPool::new_tls_with_options(
        &format!("rediss://default:correct-password@127.0.0.1:{port}/"),
        "secure",
        options.clone(),
        RedisTlsOptions::private_ca_pem(ca.clone()).unwrap(),
    )
    .unwrap();
    valid.verify_server_policy().await.unwrap();

    let wrong_host = RedisPool::new_tls_with_options(
        &format!("rediss://default:correct-password@localhost:{port}/"),
        "wrong-host",
        options.clone(),
        RedisTlsOptions::private_ca_pem(ca.clone()).unwrap(),
    )
    .unwrap();
    assert!(wrong_host.conn().await.is_err());

    let wrong_ca = RedisPool::new_tls_with_options(
        &format!("rediss://default:correct-password@127.0.0.1:{port}/"),
        "wrong-ca",
        options.clone(),
        RedisTlsOptions::private_ca_pem(std::fs::read(tls.join("other-ca.crt")).unwrap()).unwrap(),
    )
    .unwrap();
    assert!(wrong_ca.conn().await.is_err());

    let wrong_credentials = RedisPool::new_tls_with_options(
        &format!("rediss://default:wrong-password@127.0.0.1:{port}/"),
        "wrong-credentials",
        options,
        RedisTlsOptions::private_ca_pem(ca).unwrap(),
    )
    .unwrap();
    assert!(wrong_credentials.conn().await.is_err());

    let plaintext = RedisPool::new(
        &format!("redis://default:correct-password@127.0.0.1:{port}/"),
        "plaintext",
    )
    .unwrap();
    assert!(plaintext.conn().await.is_err());
    std::fs::remove_dir_all(tls).unwrap();
}
