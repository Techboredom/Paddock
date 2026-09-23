//! Paddock: a request-queueing reverse proxy for Ollama.
//!
//! When agents fire several LLM requests at Ollama at once, the server can
//! get overwhelmed: duplicate model contexts in memory, scheduler thrash,
//! request timeouts, or outright OOM. Paddock sits in front of Ollama and
//! turns those chaotic bursts into a predictable, bounded queue:
//!
//! * **Per-model FIFO queues** — requests for the same model are served in
//!   order, one at a time by default (configurable).
//! * **Global concurrency cap** — at most N requests are in flight to Ollama
//!   across all models at once.
//! * **Backpressure** — when a model's queue is full, new requests get a fast
//!   `503` + `Retry-After` instead of piling up.
//! * **Wait deadline** — requests that sit in the queue too long are rejected
//!   with `504`, so agents fail fast instead of hanging.
//! * **Streaming-safe accounting** — concurrency slots are held until the
//!   response stream is actually delivered (or the client disconnects).
//!
//! All inference endpoints (`/api/chat`, `/api/generate`,
//! `/v1/chat/completions`, `/v1/completions`) are queued. Everything else is
//! proxied straight through to the upstream.

pub mod config;
pub mod proxy;
pub mod queue;
pub mod state;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

/// Builds the full HTTP application around `state`.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(proxy::root))
        .route("/health", get(proxy::health))
        .route("/metrics", get(proxy::metrics))
        .route("/api/chat", post(proxy::queued_inference))
        .route("/api/generate", post(proxy::queued_inference))
        .route("/v1/chat/completions", post(proxy::queued_inference))
        .route("/v1/completions", post(proxy::queued_inference))
        .fallback(proxy::passthrough)
        .layer(RequestBodyLimitLayer::new(64 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
