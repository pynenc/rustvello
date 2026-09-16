//! All port I/O shares a checkout deadline. Never return a timed-out socket to the pool.

use rustvello_core::error::{RustvelloError, RustvelloResult};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{timeout_at, Instant};
use tokio_postgres::{types::ToSql, Row, ToStatement};

pub(crate) struct Client {
    pub(crate) inner: Option<deadpool_postgres::Client>,
    pub(crate) deadline: Instant,
    poisoned: AtomicBool,
}

pub(crate) struct Transaction<'a> {
    inner: deadpool_postgres::Transaction<'a>,
    deadline: Instant,
    poisoned: &'a AtomicBool,
}

async fn bounded<T>(
    deadline: Instant,
    poisoned: &AtomicBool,
    future: impl Future<Output = Result<T, tokio_postgres::Error>>,
) -> RustvelloResult<T> {
    match timeout_at(deadline, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            poisoned.store(true, Ordering::Relaxed);
            // Server details can contain SQL parameters, credentials or task results.
            Err(RustvelloError::state_backend(format!(
                "Postgres operation failed (SQLSTATE {})",
                error.code().map_or("connection", |code| code.code())
            )))
        }
        Err(_) => {
            poisoned.store(true, Ordering::Relaxed);
            Err(RustvelloError::state_backend(
                "Postgres operation deadline exceeded; write outcome may be unknown",
            ))
        }
    }
}

macro_rules! queries {
    () => {
        pub(crate) async fn execute<T: ToStatement + ?Sized>(
            &self,
            statement: &T,
            params: &[&(dyn ToSql + Sync)],
        ) -> RustvelloResult<u64> {
            bounded(
                self.deadline,
                self.poison(),
                self.raw().execute(statement, params),
            )
            .await
        }
        pub(crate) async fn query<T: ToStatement + ?Sized>(
            &self,
            statement: &T,
            params: &[&(dyn ToSql + Sync)],
        ) -> RustvelloResult<Vec<Row>> {
            bounded(
                self.deadline,
                self.poison(),
                self.raw().query(statement, params),
            )
            .await
        }
        pub(crate) async fn query_opt<T: ToStatement + ?Sized>(
            &self,
            statement: &T,
            params: &[&(dyn ToSql + Sync)],
        ) -> RustvelloResult<Option<Row>> {
            bounded(
                self.deadline,
                self.poison(),
                self.raw().query_opt(statement, params),
            )
            .await
        }
        pub(crate) async fn query_one<T: ToStatement + ?Sized>(
            &self,
            statement: &T,
            params: &[&(dyn ToSql + Sync)],
        ) -> RustvelloResult<Row> {
            bounded(
                self.deadline,
                self.poison(),
                self.raw().query_one(statement, params),
            )
            .await
        }
        pub(crate) async fn batch_execute(&self, sql: &str) -> RustvelloResult<()> {
            bounded(self.deadline, self.poison(), self.raw().batch_execute(sql)).await
        }
    };
}

impl Client {
    pub(crate) fn new(inner: deadpool_postgres::Client, deadline: Instant) -> Self {
        Self {
            inner: Some(inner),
            deadline,
            poisoned: AtomicBool::new(false),
        }
    }
    fn raw(&self) -> &deadpool_postgres::Client {
        self.inner.as_ref().expect("owned connection")
    }
    fn poison(&self) -> &AtomicBool {
        &self.poisoned
    }
    queries!();
    pub(crate) async fn transaction(&mut self) -> RustvelloResult<Transaction<'_>> {
        let inner = bounded(
            self.deadline,
            &self.poisoned,
            self.inner.as_mut().expect("owned connection").transaction(),
        )
        .await?;
        Ok(Transaction {
            inner,
            deadline: self.deadline,
            poisoned: &self.poisoned,
        })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.poisoned.load(Ordering::Relaxed) {
            if let Some(client) = self.inner.take() {
                drop(deadpool_postgres::Object::take(client));
            }
        }
    }
}

impl Transaction<'_> {
    fn raw(&self) -> &deadpool_postgres::Transaction<'_> {
        &self.inner
    }
    fn poison(&self) -> &AtomicBool {
        self.poisoned
    }
    queries!();
    pub(crate) async fn commit(self) -> RustvelloResult<()> {
        bounded(self.deadline, self.poisoned, self.inner.commit()).await
    }
}
