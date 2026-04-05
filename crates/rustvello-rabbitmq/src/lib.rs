//! RabbitMQ broker backend for Rustvello.
//!
//! Provides a [`RabbitMqBroker`] that uses AMQP queues for invocation routing.
//! This is a broker-only plugin — orchestrator, state backend, trigger store,
//! and client data store must be provided by another backend (e.g. Redis or SQLite).

pub mod broker;
mod connection;

pub mod prelude {
    pub use crate::broker::RabbitMqBroker;
}
