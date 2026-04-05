//! Application state shared across all request handlers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rustvello_core::error::{RustvelloError, RustvelloResult};

use crate::AppInstance;

/// Shared application state accessible from Axum handlers via `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<RwLock<AppStateInner>>,
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
            inner: Arc::new(RwLock::new(AppStateInner {
                apps,
                active_app_id: selected.to_owned(),
            })),
        })
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
