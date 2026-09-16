//! Opt-in process barriers; production builds contain no environment-controlled pause.
use rustvello_core::error::RustvelloResult;

pub const fn enabled() -> bool {
    cfg!(feature = "fault-injection")
}

pub(crate) async fn boundary(name: &str, invocation: &str) -> RustvelloResult<()> {
    #[cfg(feature = "fault-injection")]
    if std::env::var("RUSTVELLO_POSTGRES_FAILPOINT")
        .ok()
        .as_deref()
        == Some(name)
    {
        use rustvello_core::error::RustvelloError;
        if std::env::var("RUSTVELLO_POSTGRES_FAULT").ok().as_deref() == Some("error") {
            return Err(RustvelloError::state_backend(
                "injected publication failure",
            ));
        }
        let marker = std::env::var("RUSTVELLO_POSTGRES_BARRIER_FILE")
            .map_err(|_| RustvelloError::state_backend("missing barrier file"))?;
        std::fs::write(&marker, invocation)
            .map_err(|_| RustvelloError::state_backend("cannot publish barrier"))?;
        let wait = async {
            while !std::path::Path::new(&format!("{marker}.release")).exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), wait)
            .await
            .map_err(|_| RustvelloError::state_backend("process barrier timed out"))?;
    }
    let _ = (name, invocation);
    Ok(())
}
