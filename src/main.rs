use std::sync::Arc;

use anyhow::{Context, Result};
use paddock::build_router;
use paddock::config::Config;
use paddock::state::AppState;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,paddock=debug")),
        )
        .init();

    let cfg = Config::from_env();
    tracing::info!(
        listen = %cfg.listen,
        upstream = %cfg.upstream_url,
        global_concurrency = cfg.global_concurrency,
        model_concurrency = cfg.model_concurrency,
        max_queue_per_model = cfg.max_queue_per_model,
        wait_timeout_secs = cfg.wait_timeout.as_secs(),
        "paddock starting"
    );

    let state = Arc::new(AppState::new(cfg.clone()));
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .context("failed to bind listen address")?;
    let addr = listener
        .local_addr()
        .context("listener has no local address")?;
    tracing::info!(%addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server failed")?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(%err, "failed to install ctrl-c handler");
        return;
    }
    tracing::info!("shutdown signal received; finishing in-flight requests");
}
