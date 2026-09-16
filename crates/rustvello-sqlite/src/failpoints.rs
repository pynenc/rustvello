//! Explicitly opt-in process barriers for crash acceptance, absent in normal builds.

use rustvello_core::error::RustvelloResult;

pub const fn enabled() -> bool {
    cfg!(feature = "fault-injection")
}

pub fn boundary(name: &str, invocation: &str) -> RustvelloResult<()> {
    #[cfg(feature = "fault-injection")]
    {
        use rustvello_core::error::RustvelloError;
        use std::time::{Duration, Instant};
        if std::env::var("RUSTVELLO_SQLITE_FAILPOINT").ok().as_deref() == Some(name) {
            if std::env::var("RUSTVELLO_SQLITE_FAULT").ok().as_deref() == Some("error") {
                return Err(RustvelloError::state_backend(
                    "injected publication write failure",
                ));
            }
            let marker = std::env::var("RUSTVELLO_SQLITE_BARRIER_FILE")
                .map_err(|_| RustvelloError::state_backend("missing barrier file"))?;
            std::fs::write(&marker, invocation)
                .map_err(|_| RustvelloError::state_backend("cannot publish barrier"))?;
            let deadline = Instant::now() + Duration::from_secs(30);
            while !std::path::Path::new(&format!("{marker}.release")).exists() {
                if Instant::now() >= deadline {
                    return Err(RustvelloError::state_backend("process barrier timed out"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    let _ = (name, invocation);
    Ok(())
}
