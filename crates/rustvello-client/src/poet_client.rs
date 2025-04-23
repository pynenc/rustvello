// crates/rustvello-client/src/poet_client.rs

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use tokio::time::sleep;

use rustvello_core::{MuseError, MuseResult, Transport};
use rustvello_proto::*;

pub struct PoetClient {
    client: Client,
    endpoints: Vec<String>,
    max_retries: Option<usize>,
}

impl PoetClient {
    pub fn new(endpoints: &[String], max_retries: Option<usize>) -> Self {
        let client = Client::new();
        Self {
            client,
            endpoints: endpoints.to_vec(),
            max_retries,
        }
    }

    fn get_base_url(&self) -> &str {
        // TODO rotate endpoints on failure
        self.endpoints.first().unwrap()
    }

    /// Returns the base URL or constructs it from `node_addr` if provided.
    fn build_url(&self, path: &str, node_addr: Option<SocketAddr>) -> String {
        match node_addr {
            Some(addr) => format!("http://{}{}", addr, path),
            None => format!("{}{}", self.get_base_url(), path),
        }
    }

    /// Helper function to send a request with retry logic for SERVICE_UNAVAILABLE responses.
    async fn send_with_retry<F>(&self, request_builder: F) -> MuseResult<reqwest::Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut retries = 0;
        loop {
            let response = request_builder()
                .send()
                .await
                .map_err(|e| MuseError::Client(format!("Failed to send request: {}", e)))?;
            if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                if retries < self.max_retries.unwrap_or(usize::MAX) {
                    let retry_after = response
                        .headers()
                        .get("Retry-After")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(1);
                    sleep(Duration::from_secs(retry_after)).await;
                    retries += 1;
                    continue;
                } else {
                    return Err(MuseError::Client("Max retries exceeded".into()));
                }
            } else {
                return Ok(response);
            }
        }
    }
}

#[async_trait]
impl Transport for PoetClient {
    async fn health_check(&self) -> MuseResult<()> {
        let url = format!("{}/health", self.get_base_url());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to perform health check: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(MuseError::Client(format!(
                "Health check failed: HTTP {}",
                response.status()
            )))
        }
    }

    async fn get_node_state(&self) -> MuseResult<NodeState> {
        let url = format!("{}/sync/state", self.get_base_url());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to retrieve node state: {}", e)))?;

        if response.status().is_success() {
            let resp_haikus: NodeState = response.json().await.map_err(|e| {
                MuseError::Client(format!("Failed to parse response as NodeState: {e}"))
            })?;
            Ok(resp_haikus)
        } else {
            Err(MuseError::Client(format!(
                "Get Finest Resolution failed: {}",
                response.status()
            )))
        }
    }

    async fn get_finest_resolution(&self) -> MuseResult<TimestampResolution> {
        let url = format!("{}/config/finest_resolution", self.get_base_url());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to perform health check: {}", e)))?;

        if response.status().is_success() {
            let resp_haikus: TimestampResolution = response.json().await.map_err(|e| {
                MuseError::Client(format!(
                    "Failed to parse response as TimestampResolution: {e}"
                ))
            })?;
            Ok(resp_haikus)
        } else {
            Err(MuseError::Client(format!(
                "Get Finest Resolution failed: {}",
                response.status()
            )))
        }
    }

    async fn get_node_elem_ranges(
        &self,
        ini: Option<u64>,
        end: Option<u64>,
    ) -> MuseResult<Vec<NodeElementRange>> {
        let url = format!("{}/ds/elements/ranges", self.get_base_url());
        let response = self
            .client
            .get(&url)
            .json(&GetRangesRequest { ini, end })
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to retrieve node state: {}", e)))?;

        if response.status().is_success() {
            let ranges: Vec<NodeElementRange> = response.json().await.map_err(|e| {
                MuseError::Client(format!(
                    "Failed to parse response as Vec<NodeElementRange>: {e}"
                ))
            })?;
            Ok(ranges)
        } else {
            Err(MuseError::Client(format!(
                "Get All Element Ranges failed: {}",
                response.status()
            )))
        }
    }

    async fn register_metrics(&self, payload: &[MetricDefinition]) -> MuseResult<()> {
        let url = format!("{}/ds/metrics", self.get_base_url());
        let response = self
            .client
            .post(&url)
            .json(payload)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to send metric: {e}")))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(MuseError::Client(format!(
                "Failed to send metric: HTTP {}",
                response.status()
            )))
        }
    }

    async fn get_metric_order(&self) -> MuseResult<Vec<MetricDefinition>> {
        let url = format!("{}/ds/metrics", self.get_base_url());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to send metric: {}", e)))?;

        if response.status().is_success() {
            let metric_defs: Vec<MetricDefinition> = response.json().await.map_err(|e| {
                MuseError::Client(format!(
                    "Failed to parse response as Vec<MetricDefinition>: {e}"
                ))
            })?;
            Ok(metric_defs)
        } else {
            Err(MuseError::Client(format!(
                "Failed to send metric: HTTP {}",
                response.status()
            )))
        }
    }

    async fn send_metrics(
        &self,
        payload: Vec<MetricPayload>,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<()> {
        let url = self.build_url("/ds/abs_metrics", node_addr);
        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to send metric: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(MuseError::Client(format!(
                "Failed to send metric: HTTP {}",
                response.status()
            )))
        }
    }

    async fn get_metrics(
        &self,
        query: &MetricQuery,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<Vec<MetricPayload>> {
        let url = self.build_url("/ds/abs_metrics", node_addr);
        let response = self
            .client
            .get(&url)
            .json(query)
            .send()
            .await
            .map_err(|e| MuseError::Client(format!("Failed to get metrics: {}", e)))?;

        if response.status().is_success() {
            let metrics: Vec<MetricPayload> = response.json().await.map_err(|e| {
                MuseError::Client(format!("Failed to parse metrics response: {}", e))
            })?;
            Ok(metrics)
        } else {
            Err(MuseError::Client(format!(
                "Failed to get metrics: HTTP {}",
                response.status()
            )))
        }
    }

    async fn register_elements(
        &self,
        elements: &[ElementRegistration],
    ) -> MuseResult<Vec<MuseResult<ElementId>>> {
        let url = format!("{}/ds/elements", self.get_base_url());
        let response = self
            .send_with_retry(|| self.client.post(&url).json(elements))
            .await?;

        match response.status() {
            StatusCode::CREATED | StatusCode::MULTI_STATUS | StatusCode::BAD_REQUEST => {
                // Deserialize response as `NewElementsResponse` for all relevant cases
                let response_data: NewElementsResponse = response
                    .json()
                    .await
                    .map_err(|e| MuseError::Client(format!("Failed to parse response: {}", e)))?;

                // Convert Vec<Result<u64, String>> to Vec<Result<ElementId, Error>>
                let results = response_data
                    .results
                    .into_iter()
                    .map(|res| res.map_err(MuseError::Client))
                    .collect();

                Ok(results)
            }
            status => {
                // Handle any unexpected HTTP status codes
                Err(MuseError::Client(format!(
                    "Failed to register elements: HTTP {}",
                    status
                )))
            }
        }
    }

    async fn register_element_kinds(
        &self,
        element_kind: &[ElementKindRegistration],
    ) -> MuseResult<()> {
        let url = format!("{}/ds/element_kinds", self.get_base_url());
        let response = self
            .send_with_retry(|| self.client.post(&url).json(element_kind))
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(MuseError::Client(format!(
                "Failed to register element kind: HTTP {}",
                response.status()
            )))
        }
    }
}
