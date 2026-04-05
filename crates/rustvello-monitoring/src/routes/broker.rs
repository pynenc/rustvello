//! Broker monitoring views.

use askama::Template;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(Template)]
#[template(path = "broker/overview.html")]
#[allow(dead_code)]
struct BrokerOverviewTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    pending_count: usize,
}

/// A row in the broker queue preview table.
struct QueueInvocationRow {
    invocation_id: String,
    short_id: String,
    task_id: String,
    task_func: String,
}

#[derive(Template)]
#[template(path = "broker/partials/queue_content.html")]
struct QueueContentPartial {
    invocations: Vec<QueueInvocationRow>,
    total_count: usize,
    displayed_count: usize,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(overview))
        .route("/refresh", axum::routing::get(refresh))
        .route("/queue", axum::routing::get(queue_preview))
        .route("/purge", axum::routing::post(purge))
}

async fn overview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let pending_count = app.broker.count_invocations(None).await.unwrap_or(0);
    Ok(HtmlTemplate(BrokerOverviewTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "broker",
        pending_count,
    }))
}

#[derive(Template)]
#[template(path = "broker/partials/info.html")]
struct BrokerInfoPartial {
    pending_count: usize,
}

async fn refresh(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let pending_count = app.broker.count_invocations(None).await.unwrap_or(0);
    Ok(HtmlTemplate(BrokerInfoPartial { pending_count }))
}

#[derive(serde::Deserialize)]
struct QueueQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_limit() -> usize {
    5
}

/// Peek at broker queue: retrieve up to `limit` invocations, get their details,
/// then re-queue them (like pynmon's queue_view).
///
/// **Note:** This is destructive: `retrieve_invocation` dequeues items, then
/// we re-enqueue them via `route_invocation`. If the server crashes between
/// dequeue and re-enqueue, those invocations are lost. FIFO order is also not
/// preserved after re-queuing. A non-destructive peek API on the `Broker`
/// trait would be needed to fix this.
async fn queue_preview(
    State(state): State<AppState>,
    Query(params): Query<QueueQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let total_count = app.broker.count_invocations(None).await.unwrap_or(0);
    let limit = params.limit.min(20);

    let mut invocations = Vec::new();
    let mut retrieved_ids = Vec::new();

    // Retrieve up to `limit` invocations from the broker
    for _ in 0..limit {
        match app.broker.retrieve_invocation(None).await {
            Ok(Some(inv_id)) => {
                retrieved_ids.push(inv_id.clone());
                // Get invocation details from state backend
                if let Ok(dto) = app.state_backend.get_invocation(&inv_id).await {
                    let id_str = inv_id.to_string();
                    let short = crate::util::formatting::short_id(&id_str);
                    let task_str = dto.task_id.to_string();
                    let task_func = task_str.rsplit('.').next().unwrap_or(&task_str).to_owned();
                    invocations.push(QueueInvocationRow {
                        invocation_id: id_str,
                        short_id: short,
                        task_id: task_str,
                        task_func,
                    });
                }
            }
            _ => break,
        }
    }

    // Re-queue all retrieved invocations
    for inv_id in &retrieved_ids {
        if let Err(e) = app.broker.route_invocation(inv_id).await {
            tracing::warn!(error = %e, invocation_id = %inv_id, "failed to re-queue invocation");
        }
    }

    let displayed_count = invocations.len();
    Ok(HtmlTemplate(QueueContentPartial {
        invocations,
        total_count,
        displayed_count,
    }))
}

async fn purge(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    if let Err(e) = app.broker.purge(None).await {
        tracing::error!(error = %e, "broker purge failed");
    }
    Ok(axum::response::Redirect::to("/broker"))
}
