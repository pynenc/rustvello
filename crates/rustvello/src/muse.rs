// crates/rustvello/src/muse.rs

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::tasks;
use crate::timing;
use rustvello_client::{MockClient, PoetClient};
use rustvello_core::prelude::*;
use rustvello_proto::prelude::*;
use rustvello_record::{
    FileRecorder, FileReplayer, RecordedEvent, RecordedEventWithTime, Recorder, Replayer,
};

/// The main client for interacting with the Muse system.
///
/// The `Muse` struct provides methods to initialize the client, register elements,
/// send metrics, and replay recorded events.
pub struct Muse {
    client: Arc<dyn Transport + Send + Sync>,
    state: Arc<State>,
    pub recorder: Option<Arc<Mutex<dyn Recorder + Send + Sync>>>,
    tasks: Vec<JoinHandle<()>>,
    cancellation_token: CancellationToken,
    /// Indicates whether the Muse client has been initialized.
    pub is_initialized: Arc<AtomicBool>,
    element_buffer: Arc<ElementBuffer>,
    metric_buffer: Arc<MetricBuffer>,
    config: Config,
}

impl Drop for Muse {
    /// Cleans up resources when the `Muse` instance is dropped.
    ///
    /// Cancels any running tasks and releases resources.
    /// Flushes and Closes any running event recording.
    fn drop(&mut self) {
        // Cancel any running tasks
        self.cancellation_token.cancel();
        for task in &self.tasks {
            task.abort();
        }

        // Flush and close the recorder synchronously if it exists
        if let Some(recorder) = &self.recorder {
            let recorder = recorder.clone();
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
                rt.block_on(async {
                    let mut recorder = recorder.lock().await;
                    if let Err(e) = recorder.flush().await {
                        eprintln!("Failed to flush recorder: {:?}", e);
                    }
                    if let Err(e) = recorder.close().await {
                        eprintln!("Failed to close recorder: {:?}", e);
                    }
                });
            })
            .join();
        }
    }
}

impl Muse {
    /// Creates a new `Muse` client instance.
    ///
    /// # Arguments
    ///
    /// - `config`: A reference to the [`Config`] object.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Configuration`] if the client cannot be created with the provided configuration.
    pub fn new(config: &Config) -> MuseResult<Self> {
        let client: Arc<dyn Transport + Send + Sync> = match config.client_type {
            ClientType::Poet => Arc::new(PoetClient::new(
                &config.endpoints,
                config.max_endpoint_retries,
            )),
            ClientType::Mock => Arc::new(MockClient::new(config.default_resolution)),
        };

        let recorder: Option<Arc<Mutex<dyn Recorder + Send + Sync>>> = if config.recording_enabled {
            if let Some(path) = &config.recording_path {
                let file_recorder =
                    FileRecorder::new(Path::new(path)).expect("Failed to create FileRecorder");
                Some(Arc::new(Mutex::new(file_recorder))) // Wrap in Mutex here
            } else {
                return Err(MuseError::Configuration(
                    "Recording enabled but no recording path provided".to_string(),
                ));
            }
        } else {
            None
        };

        // Create the cancellation token
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            client,
            state: Arc::new(State::new(config.default_resolution)),
            recorder,
            tasks: Vec::new(),
            cancellation_token: cancellation_token.clone(),
            is_initialized: Arc::new(AtomicBool::new(false)),
            element_buffer: Arc::new(ElementBuffer::new(config.max_reg_elem_retries)),
            metric_buffer: Arc::new(MetricBuffer::new()),
            config: config.clone(),
        })
    }

    /// Initializes the Muse client and starts background tasks.
    ///
    /// Must be called before using other methods that interact with the Muse system.
    ///
    /// # Arguments
    ///
    /// - `timeout`: Optional timeout duration for the initialization process.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::MuseInitializationTimeout`] if initialization times out.
    pub async fn initialize(&mut self, timeout: Option<Duration>) -> MuseResult<()> {
        // Record the MuseConfig event if recording is enabled
        if let Some(recorder) = &self.recorder {
            let config_event = RecordedEvent::MuseConfig(self.config.clone());
            recorder
                .lock()
                .await
                .record(RecordedEventWithTime::new(config_event))
                .await
                .expect("Failed to record MuseConfig event");
        }
        // Start background tasks
        let init_interval = self
            .config
            .initialization_interval
            .unwrap_or(timing::INITIALIZATION_INTERVAL);
        self.start_tasks(
            self.config.element_kinds.to_vec(),
            self.config.metric_definitions.to_vec(),
            init_interval,
            self.config.cluster_monitor_interval,
        );
        // Wait for initialization to complete, with an optional timeout
        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        while !self.is_initialized() {
            if let Some(deadline) = deadline {
                if tokio::time::Instant::now() >= deadline {
                    return Err(MuseError::MuseInitializationTimeout(timeout.unwrap()));
                }
            }
            tokio::time::sleep(init_interval).await;
        }
        Ok(())
    }

    /// Retrieves a reference to the internal [`State`] object.
    ///
    /// # Returns
    ///
    /// An `Arc` pointing to the internal `State`.
    pub fn get_state(&self) -> Arc<State> {
        self.state.clone()
    }

    /// Retrieves the finest resolution of timestamps from the state.
    ///
    /// # Returns
    ///
    /// The current `TimestampResolution` as set in the state.
    pub fn get_finest_resolution(&self) -> TimestampResolution {
        self.state.get_finest_resolution()
    }

    /// Retrieves a reference to the internal transport client.
    ///
    /// # Returns
    ///
    /// An `Arc` pointing to the transport client implementing `Transport`.
    pub fn get_client(&self) -> Arc<dyn Transport + Send + Sync> {
        self.client.clone()
    }

    fn start_tasks(
        &mut self,
        element_kinds: Vec<ElementKindRegistration>,
        metric_definitions: Vec<MetricDefinition>,
        initialization_interval: Duration,
        cluster_monitor_interval: Option<Duration>,
    ) {
        let cancellation_token = self.cancellation_token.clone();
        let client = self.client.clone();
        let state = self.state.clone();
        let is_initialized = self.is_initialized.clone();

        // Start the recorded flushing task
        if let Some(recorder) = &self.recorder {
            let flush_interval = self
                .config
                .recording_flush_interval
                .unwrap_or(timing::RECORDING_FLUSH_INTERVAL);
            let flush_task = tokio::spawn(tasks::start_recorder_flush_task(
                cancellation_token.clone(),
                recorder.clone(),
                flush_interval,
            ));
            self.tasks.push(flush_task);
        }

        // Start the initialization task
        let init_task_handle = tokio::spawn(tasks::start_init_task(
            cancellation_token.clone(),
            client.clone(),
            state.clone(),
            element_kinds,
            metric_definitions,
            initialization_interval,
            is_initialized.clone(),
        ));
        self.tasks.push(init_task_handle);

        // Start the cluster monitoring task
        let cluster_monitoring_handle = tokio::spawn(tasks::start_cluster_monitor(
            cancellation_token.clone(),
            client.clone(),
            state.clone(),
            is_initialized,
            cluster_monitor_interval.unwrap_or(timing::CLUSTER_MONITOR_INTERVAL),
        ));
        self.tasks.push(cluster_monitoring_handle);

        // Start element registration task
        let elem_reg_handle = tokio::spawn(tasks::start_element_registration_task(
            cancellation_token.clone(),
            client.clone(),
            state.clone(),
            self.element_buffer.clone(),
        ));
        self.tasks.push(elem_reg_handle);

        // Start metric sender task
        let metric_sender_handle = tokio::spawn(tasks::start_metric_sender_task(
            cancellation_token.clone(),
            client,
            state,
            self.metric_buffer.clone(),
        ));
        self.tasks.push(metric_sender_handle);
    }

    /// Checks if the Muse client has been initialized.
    ///
    /// # Returns
    ///
    /// `true` if initialized, `false` otherwise.
    pub fn is_initialized(&self) -> bool {
        self.is_initialized.load(Ordering::SeqCst)
    }

    /// Registers a new element with the Muse system.
    ///
    /// # Arguments
    ///
    /// - `kind_code`: The kind code of the element.
    /// - `name`: The name of the element.
    /// - `metadata`: A map of metadata key-value pairs.
    /// - `parent_id`: Optional parent [`LocalElementId`].
    ///
    /// # Returns
    ///
    /// A [`LocalElementId`] representing the registered element.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError`] if registration fails.
    pub async fn register_element(
        &self,
        kind_code: &str,
        name: String,
        metadata: HashMap<String, String>,
        parent_id: Option<LocalElementId>,
    ) -> MuseResult<LocalElementId> {
        let local_elem_id = generate_local_element_id();
        self.register_element_inner(local_elem_id, kind_code, name, metadata, parent_id)
            .await?;
        Ok(local_elem_id)
    }

    async fn register_element_inner(
        &self,
        local_elem_id: LocalElementId,
        kind_code: &str,
        name: String,
        metadata: HashMap<String, String>,
        parent_id: Option<LocalElementId>,
    ) -> MuseResult<()> {
        // Record the event if recorder is enabled
        if let Some(recorder) = &self.recorder {
            let event = RecordedEvent::ElementRegistration {
                local_elem_id,
                kind_code: kind_code.to_string(),
                name: name.clone(),
                metadata: metadata.clone(),
                parent_id,
            };
            recorder
                .lock()
                .await
                .record(RecordedEventWithTime::new(event))
                .await?;
        }
        if !self.state.is_valid_element_kind_code(kind_code) {
            return Err(MuseError::InvalidElementKindCode(kind_code.to_string()));
        }
        let remote_parent_id = match parent_id {
            Some(p) => {
                let remote_id = self
                    .get_remote_element_id(&p)
                    .ok_or(MuseError::NotAvailableRemoteElementId(p))?;
                Some(remote_id)
            }
            None => None,
        };
        let element = ElementRegistration::new(kind_code, name, metadata, remote_parent_id);
        self.element_buffer
            .add_element(local_elem_id, element)
            .await;
        Ok(())
    }

    /// Retrieves the remote `ElementId` associated with a given `LocalElementId`.
    ///
    /// # Arguments
    ///
    /// - `local_elem_id`: The `LocalElementId` for which to retrieve the `ElementId`.
    ///
    /// # Returns
    ///
    /// An `Option<ElementId>` containing the associated `ElementId` if it exists.
    pub fn get_remote_element_id(&self, local_elem_id: &LocalElementId) -> Option<ElementId> {
        self.state.get_element_id(local_elem_id)
    }

    /// Sends a metric value associated with an element.
    ///
    /// # Arguments
    ///
    /// - `local_elem_id`: The local ID of the element.
    /// - `metric_code`: The code identifying the metric.
    /// - `value`: The metric value to send.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError`] if the metric cannot be sent.
    pub async fn send_metric(
        &self,
        local_elem_id: LocalElementId,
        metric_code: &str,
        value: MetricValue,
    ) -> MuseResult<()> {
        // Record the event if recorder is enabled
        if let Some(recorder) = &self.recorder {
            let event = RecordedEvent::SendMetric {
                local_elem_id,
                metric_code: metric_code.to_string(),
                value,
            };
            recorder
                .lock()
                .await
                .record(RecordedEventWithTime::new(event))
                .await?;
        }

        if !self.state.is_valid_metric_code(metric_code) {
            return Err(MuseError::InvalidMetricCode(metric_code.to_string()));
        }

        self.metric_buffer
            .add_metric(local_elem_id, metric_code.to_string(), value)
            .await;

        Ok(())
    }

    /// Retrieves metrics from the Muse system based on a query.
    ///
    /// **Note**: The `Muse` client is primarily intended for sending metrics to the Muse system.
    /// This method is provided mainly for testing purposes and is not recommended for use in production code.
    ///
    /// # Arguments
    ///
    /// - `query`: The [`MetricQuery`] specifying the criteria for retrieving metrics.
    ///
    /// # Returns
    ///
    /// A vector of [`MetricPayload`]s matching the query.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError`] if the metrics cannot be retrieved.
    pub async fn get_metrics(&self, query: &MetricQuery) -> MuseResult<Vec<MetricPayload>> {
        // For testing purposes, we use the client to get metrics.
        // Note that in production use, the Muse client is not intended for retrieving metrics.
        self.client.get_metrics(query, None).await
    }

    /// Replays events from a recording file.
    ///
    /// Useful for testing or replaying historical data.
    ///
    /// # Arguments
    ///
    /// - `replay_path`: The file path to the recording.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError`] if replaying fails.
    pub async fn replay(&self, replay_path: &Path) -> MuseResult<()> {
        if self.config.recording_enabled {
            return Err(MuseError::Replaying(
                "Cannot replay with recording enabled".to_string(),
            ));
        }
        let mut replayer = FileReplayer::new(replay_path).await?;

        let mut last_timestamp = None;

        while let Some(timed_event) = replayer.next_event().await? {
            if let Some(last) = last_timestamp {
                let delay = Duration::from_micros((timed_event.timestamp - last) as u64);
                tokio::time::sleep(delay).await;
            }

            last_timestamp = Some(timed_event.timestamp);

            match timed_event.event {
                RecordedEvent::MuseConfig(recorded_config) => {
                    if !self.config.is_relevantly_equal(&recorded_config) {
                        let differences = recorded_config.pretty_diff(&self.config);
                        log::warn!(
                            "Recorded config and current config do not match:\n{}",
                            differences
                        );
                    }
                }
                RecordedEvent::ElementRegistration {
                    local_elem_id,
                    kind_code,
                    name,
                    metadata,
                    parent_id,
                } => {
                    self.register_element_inner(
                        local_elem_id,
                        &kind_code,
                        name,
                        metadata,
                        parent_id,
                    )
                    .await?;
                }
                RecordedEvent::SendMetric {
                    local_elem_id,
                    metric_code,
                    value,
                } => {
                    self.send_metric(local_elem_id, &metric_code, value).await?;
                }
            }
        }
        Ok(())
    }

    /// Initializes a Muse instance using the Config recorded in a replay file.
    /// Sets `recording_enabled` in the Config to `false` to prevent re-recording during replay.
    ///
    /// # Arguments
    /// - `replay_path`: The path to the replay file containing the Config.
    ///
    /// # Returns
    /// A new Muse instance initialized with the Config extracted from the replay file.
    pub async fn from_replay(replay_path: &Path) -> MuseResult<Self> {
        let mut replayer = FileReplayer::new(replay_path).await?;
        let mut config: Option<Config> = None;

        // Extract the ConfigUpdate event from the replay file.
        if let Some(timed_event) = replayer.next_event().await? {
            if let RecordedEvent::MuseConfig(mut c) = timed_event.event {
                c.recording_enabled = false;
                config = Some(c);
            }
        }

        // Ensure the Config is present in the replay file.
        let config = config.ok_or_else(|| {
            MuseError::Replaying("No ConfigUpdate event found in the replay file.".to_string())
        })?;

        // Create and initialize a new Muse instance with the extracted Config.
        let mut muse = Muse::new(&config)?;
        muse.initialize(None).await?;
        Ok(muse)
    }

    /// Checks if a replay should start based on the presence of a replay file.
    /// If the replay file exists and contains valid events, it will start the replay process.
    ///
    /// # Arguments
    /// - `replay_path`: The path to the replay file.
    ///
    /// # Returns
    /// A Result indicating success or failure of the replay process.
    pub async fn check_and_replay(replay_path: &Path) -> MuseResult<Self> {
        if replay_path.exists() {
            log::info!("Replay file found: {:?}", replay_path);
            let muse = Muse::from_replay(replay_path).await?;
            muse.replay(replay_path).await?;
            log::info!("Replay completed successfully.");
            return Ok(muse);
        }
        log::info!("No replay file found at: {:?}", replay_path);
        Err(MuseError::Replaying(format!(
            "Replay file not found: {:?}",
            replay_path
        )))
    }
}
