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
    pub fn sqlite(mut self, path: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::Sqlite {
            path: path.to_string(),
            app_id: app_id.to_string(),
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
    pub fn redis(mut self, uri: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::Redis {
            uri: uri.to_string(),
            app_id: app_id.to_string(),
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
    pub fn postgres(mut self, connection_string: &str, app_id: &str) -> Self {
        self.backend_preset = Some(super::BackendPreset::Postgres {
            connection_string: connection_string.to_string(),
            app_id: app_id.to_string(),
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
