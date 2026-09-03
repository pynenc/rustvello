//! Broker monitoring views.

use askama::Template;
use axum::extract::State;
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

struct QueueRow {
    name: String,
    pending_count: usize,
    consumed: bool,
}

#[derive(Template)]
#[template(path = "broker/partials/queue_content.html")]
struct QueueContentPartial {
    queues: Vec<QueueRow>,
    total_count: usize,
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

/// Show configured logical queues without consuming or reordering messages.
async fn queue_preview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let consumed_queues = if app.config.runner_queues.is_empty() {
        &app.config.broker_queues
    } else {
        &app.config.runner_queues
    };
    let mut queues = Vec::with_capacity(app.config.broker_queues.len());
    let mut total_count = 0;
    for name in &app.config.broker_queues {
        let pending_count = app
            .broker
            .count_invocations_in_queues(std::slice::from_ref(name), None)
            .await
            .unwrap_or(0);
        total_count += pending_count;
        queues.push(QueueRow {
            name: name.clone(),
            pending_count,
            consumed: consumed_queues.contains(name),
        });
    }

    Ok(HtmlTemplate(QueueContentPartial {
        queues,
        total_count,
    }))
}

async fn purge(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    if let Err(e) = app.broker.purge(None).await {
        tracing::error!(error = %e, "broker purge failed");
    }
    Ok(axum::response::Redirect::to("/broker"))
}
