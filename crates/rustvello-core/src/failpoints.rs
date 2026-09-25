//! Opt-in orchestration failpoints for crash acceptance tests.
//!
//! Absent from normal builds: without the `fault-injection` feature
//! [`boundary`] compiles to `Ok(())`. With it, a boundary named by
//! `RUSTVELLO_FAILPOINT` either fails (`RUSTVELLO_FAULT=error`) or parks the
//! process on a barrier file (`RUSTVELLO_BARRIER_FILE`) so a test can kill it
//! there. In-process tests can arm a failure with [`arm_error`] instead.

use crate::error::RustvelloResult;

/// Whether this build contains failpoints.
pub const fn enabled() -> bool {
    cfg!(feature = "fault-injection")
}

#[cfg(feature = "fault-injection")]
static ARMED: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Arm (or disarm with `None`) an in-process error at the named boundary.
///
/// The armed failure fires once and then disarms itself.
#[cfg(feature = "fault-injection")]
pub fn arm_error(name: Option<&str>) {
    *ARMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = name.map(str::to_owned);
}

/// Pass one named boundary; see the module docs.
pub async fn boundary(name: &str, subject: &str) -> RustvelloResult<()> {
    #[cfg(feature = "fault-injection")]
    {
        use crate::error::RustvelloError;
        {
            let mut armed = ARMED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if armed.as_deref() == Some(name) {
                *armed = None;
                return Err(RustvelloError::state_backend(format!(
                    "injected failure at {name}"
                )));
            }
        }
        if std::env::var("RUSTVELLO_FAILPOINT").ok().as_deref() == Some(name) {
            if std::env::var("RUSTVELLO_FAULT").ok().as_deref() == Some("error") {
                return Err(RustvelloError::state_backend(format!(
                    "injected failure at {name}"
                )));
            }
            let marker = std::env::var("RUSTVELLO_BARRIER_FILE")
                .map_err(|_| RustvelloError::state_backend("missing barrier file"))?;
            std::fs::write(&marker, subject)
                .map_err(|_| RustvelloError::state_backend("cannot publish barrier"))?;
            let release = format!("{marker}.release");
            let wait = async {
                while !std::path::Path::new(&release).exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            };
            tokio::time::timeout(std::time::Duration::from_secs(30), wait)
                .await
                .map_err(|_| RustvelloError::state_backend("process barrier timed out"))?;
        }
    }
    let _ = (name, subject);
    Ok(())
}
