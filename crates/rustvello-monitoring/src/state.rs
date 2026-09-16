//! Application state shared across all request handlers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rustvello_core::error::{RustvelloError, RustvelloResult};

use crate::AppInstance;

/// Shared application state accessible from Axum handlers via `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<RwLock<AppStateInner>>,
    workflow_histories: Arc<std::sync::Mutex<WorkflowHistoryCache>>,
}

type WorkflowHistoryCache =
    HashMap<(String, String), (std::time::Instant, Arc<WorkflowHistorySnapshot>)>;

pub(crate) struct WorkflowHistorySnapshot {
    pub entries: Vec<crate::histogram::HistogramEntry>,
    pub truncated: bool,
}

struct AppStateInner {
    apps: HashMap<String, AppInstance>,
    active_app_id: String,
}

fn state_lock_err(e: impl std::fmt::Display) -> RustvelloError {
    RustvelloError::Internal {
        message: format!("state lock poisoned: {e}"),
    }
}

impl AppState {
    /// Create a new `AppState` with the given apps and initially active app.
    pub fn new(apps: HashMap<String, AppInstance>, selected: &str) -> RustvelloResult<Self> {
        if !apps.contains_key(selected) {
            return Err(RustvelloError::Configuration {
                message: format!("selected app '{selected}' not found in apps list"),
            });
        }
        Ok(Self {
            workflow_histories: Arc::default(),
            inner: Arc::new(RwLock::new(AppStateInner {
                apps,
                active_app_id: selected.to_owned(),
            })),
        })
    }

    pub(crate) fn workflow_history(
        &self,
        app_id: &str,
        workflow_id: &str,
    ) -> Option<Arc<WorkflowHistorySnapshot>> {
        let cache = self.workflow_histories.lock().ok()?;
        let (loaded, snapshot) = cache.get(&(app_id.to_owned(), workflow_id.to_owned()))?;
        (loaded.elapsed() < std::time::Duration::from_secs(2)).then(|| Arc::clone(snapshot))
    }

    pub(crate) fn cache_workflow_history(
        &self,
        app_id: &str,
        workflow_id: &str,
        snapshot: Arc<WorkflowHistorySnapshot>,
    ) {
        let Ok(mut cache) = self.workflow_histories.lock() else {
            return;
        };
        cache.retain(|_, (loaded, _)| loaded.elapsed() < std::time::Duration::from_secs(2));
        if cache.len() >= 10 {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (loaded, _))| *loaded)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            (app_id.to_owned(), workflow_id.to_owned()),
            (std::time::Instant::now(), snapshot),
        );
    }

    /// Get the currently active application instance.
    pub fn active_app(&self) -> RustvelloResult<AppInstance> {
        let inner = self.inner.read().map_err(state_lock_err)?;
        inner
            .apps
            .get(&inner.active_app_id)
            .cloned()
            .ok_or_else(|| RustvelloError::Internal {
                message: format!("active app '{}' not found", inner.active_app_id),
            })
    }

    /// Get the active app ID.
    pub fn active_app_id(&self) -> RustvelloResult<String> {
        let inner = self.inner.read().map_err(state_lock_err)?;
        Ok(inner.active_app_id.clone())
    }

    /// List all available app IDs.
    pub fn app_ids(&self) -> RustvelloResult<Vec<String>> {
        let inner = self.inner.read().map_err(state_lock_err)?;
        Ok(inner.apps.keys().cloned().collect())
    }

    /// Switch to a different app by ID.
    pub fn switch_app(&self, app_id: &str) -> RustvelloResult<()> {
        let mut inner = self.inner.write().map_err(state_lock_err)?;
        if !inner.apps.contains_key(app_id) {
            return Err(RustvelloError::Configuration {
                message: format!("app '{app_id}' not found"),
            });
        }
        inner.active_app_id = app_id.to_owned();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_cache_is_bounded_isolated_and_expires() {
        let state = AppState {
            inner: Arc::new(RwLock::new(AppStateInner {
                apps: HashMap::new(),
                active_app_id: String::new(),
            })),
            workflow_histories: Arc::default(),
        };
        let snapshot = Arc::new(WorkflowHistorySnapshot {
            entries: Vec::new(),
            truncated: false,
        });
        state.cache_workflow_history("app-a", "run", Arc::clone(&snapshot));
        assert!(state.workflow_history("app-a", "run").is_some());
        assert!(state.workflow_history("app-b", "run").is_none());
        state
            .workflow_histories
            .lock()
            .unwrap()
            .get_mut(&("app-a".into(), "run".into()))
            .unwrap()
            .0 -= std::time::Duration::from_secs(3);
        assert!(state.workflow_history("app-a", "run").is_none());
        for id in 0..20 {
            state.cache_workflow_history("app-a", &id.to_string(), Arc::clone(&snapshot));
        }
        assert_eq!(state.workflow_histories.lock().unwrap().len(), 10);
    }
}
