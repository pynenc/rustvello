//! Integration tests using testcontainers to run broker suite tests against real RabbitMQ.
//!
//! RabbitMQ only implements the `Broker` trait, so only the broker suite is wired.
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-rabbitmq -- --ignored          # only Docker tests
//! cargo test -p rustvello-rabbitmq -- --include-ignored   # all tests
//! ```

use rustvello_rabbitmq::prelude::*;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

/// Start a RabbitMQ container and return the AMQP URI.
async fn rabbitmq_uri() -> (testcontainers::ContainerAsync<RabbitMq>, String) {
    let container = RabbitMq::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5672).await.unwrap();
    let uri = format!("amqp://{host}:{port}");
    (container, uri)
}

async fn make_broker() -> (testcontainers::ContainerAsync<RabbitMq>, RabbitMqBroker) {
    let (c, uri) = rabbitmq_uri().await;
    (c, RabbitMqBroker::new(&uri, "test"))
}

mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}
