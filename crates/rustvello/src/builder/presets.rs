//! Backend preset methods for [`RustvelloBuilder`].

use super::RustvelloBuilder;

impl RustvelloBuilder {
    /// Use all in-memory backends (suitable for testing and development).
    ///
    /// No connections are established — backends are created during
    /// [`build()`](Self::build).
    #[cfg(feature = "mem")]
    #[cfg_attr(docsrs, doc(cfg(feature = "mem")))]
    pub fn memory(mut self) -> Self {
        self.backend_preset = Some(super::BackendPreset::Memory);
        self
    }

    /// Use all SQLite backends with the given database path.
    ///
    /// The database file is opened during [`build()`](Self::build), not here.
    #[cfg(feature = "sqlite")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
    pub fn sqlite(self, path: &str, app_id: &str) -> Self {
        self.sqlite_with_options(path, app_id, rustvello_sqlite::db::SqliteOptions::default())
    }

    /// Co-located atomic publication with explicit connection synchronization.
    /// All producers/workers must use the same local path, app ID and FULL policy.
    #[cfg(feature = "sqlite")]
    pub fn sqlite_with_options(
        mut self,
        path: &str,
        app_id: &str,
        options: rustvello_sqlite::db::SqliteOptions,
    ) -> Self {
        self.backend_preset = Some(super::BackendPreset::Sqlite {
            path: path.to_string(),
            app_id: app_id.to_string(),
            options,
        });
        self
    }

    /// Use all Redis backends with the given connection URI.
    ///
    /// The connection pool is created during [`build()`](Self::build), not here.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .redis("redis://127.0.0.1/", "my_app")
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "redis")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
    pub fn redis(self, uri: &str, app_id: &str) -> Self {
        self.redis_with_options(
            uri,
            app_id,
            rustvello_redis::prelude::RedisOptions::default(),
        )
    }

    /// Use all Redis ports with bounded queue, lease and connection policy.
    #[cfg(feature = "redis")]
    pub fn redis_with_options(
        mut self,
        uri: &str,
        app_id: &str,
        options: rustvello_redis::prelude::RedisOptions,
    ) -> Self {
        self.backend_preset = Some(super::BackendPreset::Redis {
            uri: uri.to_string(),
            app_id: app_id.to_string(),
            options,
            tls: None,
        });
        self
    }

    /// Use all Redis ports over TLS with explicit private trust roots.
    #[cfg(feature = "redis")]
    pub fn redis_tls_with_options(
        mut self,
        uri: &str,
        app_id: &str,
        options: rustvello_redis::prelude::RedisOptions,
        tls: rustvello_redis::prelude::RedisTlsOptions,
    ) -> Self {
        self.backend_preset = Some(super::BackendPreset::Redis {
            uri: uri.to_string(),
            app_id: app_id.to_string(),
            options,
            tls: Some(tls),
        });
        self
    }

    /// Use all MongoDB backends with the given connection URI and database name.
    ///
    /// The connection pool is created during [`build()`](Self::build), not here.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .mongodb("mongodb://localhost:27017", "rustvello_db", "my_app")
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mongodb")]
    #[cfg_attr(docsrs, doc(cfg(feature = "mongodb")))]
    pub fn mongodb(mut self, uri: &str, db_name: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::MongoDB {
            uri: uri.to_string(),
            db_name: db_name.to_string(),
            app_id: app_id.to_string(),
        });
        self
    }

    /// Use a RabbitMQ broker with the given AMQP URI and queue prefix.
    ///
    /// Only sets the broker — orchestrator, state backend, client data store,
    /// and trigger store must be provided separately (e.g. via `.redis()` or
    /// `.sqlite()`). This matches the Python `pynenc_rabbitmq` plugin which
    /// only provides a broker implementation.
    ///
    /// The connection is established during [`build()`](Self::build), not here.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .rabbitmq("amqp://localhost:5672", "myapp")
    ///     .redis("redis://127.0.0.1/", "my_app")
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "rabbitmq")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
    pub fn rabbitmq(mut self, uri: &str, prefix: &str) -> Self {
        self.rabbitmq_config = Some(super::RabbitMqConfig {
            uri: uri.to_string(),
            prefix: prefix.to_string(),
        });
        self
    }

    /// Use all PostgreSQL backends with the given connection string.
    ///
    /// The connection pool is created during [`build()`](Self::build), not here.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .postgres("postgresql://user:pass@localhost/mydb", "my_app")
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "postgres")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
    pub fn postgres(self, connection_string: &str, app_id: &str) -> Self {
        self.postgres_with_options(
            connection_string,
            app_id,
            rustvello_postgres::db::PostgresOptions::default(),
        )
    }

    /// Networked atomic-publication profile with bounded I/O and admission.
    #[cfg(feature = "postgres")]
    pub fn postgres_with_options(
        mut self,
        connection_string: &str,
        app_id: &str,
        options: rustvello_postgres::db::PostgresOptions,
    ) -> Self {
        self.backend_preset = Some(super::BackendPreset::Postgres {
            connection_string: connection_string.to_string(),
            app_id: app_id.to_string(),
            options,
        });
        self
    }

    /// Use all PostgreSQL backends with TLS encryption.
    ///
    /// Requires both the `postgres` and `tls` features.
    #[cfg(all(feature = "postgres", feature = "tls"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "postgres", feature = "tls"))))]
    pub fn postgres_tls(mut self, connection_string: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::PostgresTls {
            connection_string: connection_string.to_string(),
            app_id: app_id.to_string(),
            options: rustvello_postgres::db::PostgresOptions::default(),
            tls: None,
        });
        self
    }

    /// Use verified PostgreSQL TLS with complete runtime and trust policy.
    #[cfg(all(feature = "postgres", feature = "tls"))]
    pub fn postgres_tls_with_options(
        mut self,
        connection_string: &str,
        app_id: &str,
        options: rustvello_postgres::db::PostgresOptions,
        tls: rustvello_postgres::db::PostgresTlsOptions,
    ) -> Self {
        self.backend_preset = Some(super::BackendPreset::PostgresTls {
            connection_string: connection_string.to_string(),
            app_id: app_id.to_string(),
            options,
            tls: Some(tls),
        });
        self
    }

    /// Use all MongoDB 3.6+ backends with the given connection URI and database name.
    ///
    /// The connection pool is created during [`build()`](Self::build), not here.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .mongo3("mongodb://localhost:27017", "rustvello_db", "my_app")
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mongodb3")]
    #[cfg_attr(docsrs, doc(cfg(feature = "mongodb3")))]
    pub fn mongo3(mut self, uri: &str, db_name: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::Mongo3 {
            uri: uri.to_string(),
            db_name: db_name.to_string(),
            app_id: app_id.to_string(),
        });
        self
    }
}
