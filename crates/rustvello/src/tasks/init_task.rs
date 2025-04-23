// crates/rustvello/src/tasks/init_task.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use rustvello_core::{MuseResult, State, Transport};
use rustvello_proto::{ElementKindRegistration, MetricDefinition};

pub async fn start_init_task(
    cancellation_token: CancellationToken,
    client: Arc<dyn Transport + Send + Sync>,
    state: Arc<State>,
    element_kinds: Vec<ElementKindRegistration>,
    metric_definitions: Vec<MetricDefinition>,
    initialization_interval_duration: Duration,
    is_initialized: Arc<AtomicBool>,
) {
    let mut step = InitializationStep::HealthCheck;
    let mut interval = interval(initialization_interval_duration);
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                println!("Initialization task was cancelled.");
                break;
            }
            _ = interval.tick() => {
                match perform_initialization_step(
                    &client,
                    &state,
                    &element_kinds,
                    &metric_definitions,
                    &mut step,
                ).await {
                    Ok(_) => {
                        if step == InitializationStep::Done {
                            is_initialized.store(true, Ordering::SeqCst);
                            println!("Initialization completed successfully.");
                            break;
                        }
                    }
                    Err(e) => {
                        println!("Initialization error at step {:?}: {}", step, e);
                    }
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum InitializationStep {
    HealthCheck,
    RegisterElementKinds,
    RegisterMetrics,
    GetMetricOrder,
    GetFinestResolution,
    Done,
}

async fn perform_initialization_step(
    client: &Arc<dyn Transport + Send + Sync>,
    state: &Arc<State>,
    element_kinds: &[ElementKindRegistration],
    metric_definitions: &[MetricDefinition],
    step: &mut InitializationStep,
) -> MuseResult<()> {
    match *step {
        InitializationStep::HealthCheck => {
            client.health_check().await?;
            *step = InitializationStep::RegisterElementKinds;
        }
        InitializationStep::RegisterElementKinds => {
            if !element_kinds.is_empty() {
                client.register_element_kinds(element_kinds).await?;
                state.init_element_kinds(element_kinds).await;
            }
            *step = InitializationStep::RegisterMetrics;
        }
        InitializationStep::RegisterMetrics => {
            if !metric_definitions.is_empty() {
                client.register_metrics(metric_definitions).await?;
                state.init_metrics(metric_definitions).await;
            }
            *step = InitializationStep::GetMetricOrder;
        }
        InitializationStep::GetMetricOrder => {
            let metric_order = client.get_metric_order().await?;
            state.update_metric_order(metric_order).await;
            *step = InitializationStep::GetFinestResolution;
        }
        InitializationStep::GetFinestResolution => {
            let finest_resolution = client.get_finest_resolution().await?;
            state.update_finest_resolution(finest_resolution).await;
            *step = InitializationStep::Done;
        }
        InitializationStep::Done => {}
    }
    Ok(())
}
