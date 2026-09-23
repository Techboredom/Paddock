use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;
use axum::Json;
use serde_json::json;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

use crate::queue::{Job, ModelQueue, StreamingBody};
use crate::state::{AppState, Metrics};

/// Returns the `model` field of an inference JSON body, or `"unknown"`.
pub(crate) fn extract_model(body: &Bytes) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Returns the request path (including the query string) from the URI.
pub(crate) fn path_of(uri: &Uri) -> String {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string())
}

/// Strips connection-specific headers before forwarding to the upstream.
pub(crate) fn filter_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::new();
    for (key, value) in headers {
        let connection_specific = matches!(
            key.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "host"
                | "content-length"
        );
        if !connection_specific {
            forwarded.append(key, value.clone());
        }
    }
    forwarded
}

/// Inference endpoint handler: the request is queued per model and only
/// forwarded to Ollama once a concurrency slot is free.
pub async fn queued_inference(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = path_of(&uri);
    let model = extract_model(&body);
    let queue = state.queue_for(&model);
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);

    let (respond, response) = oneshot::channel();
    let job = Job {
        method,
        path,
        model: model.clone(),
        headers: filter_headers(&headers),
        body,
        deadline: Instant::now() + state.wait_timeout,
        enqueued_at: Instant::now(),
        respond,
    };

    if !queue.try_push(job, state.max_queue_per_model) {
        state.metrics.rejected_total.fetch_add(1, Ordering::Relaxed);
        let depth = queue.depth();
        tracing::warn!(model = %model, depth, "queue full; rejecting request");
        return queue_full_response(&model, depth);
    }
    state.metrics.queued_total.fetch_add(1, Ordering::Relaxed);

    match response.await {
        Ok(response) => response,
        Err(_) => internal_error_response("queue dispatcher closed unexpectedly"),
    }
}

/// Any other endpoint (e.g. `/api/tags`, `/api/pull`, `/api/show`): forwarded
/// directly to the upstream without queueing.
pub async fn passthrough(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = path_of(&uri);
    let url = format!("{}{}", state.base_url, path);

    let mut request = state.upstream.request(method, &url);
    for (key, value) in &headers {
        request = request.header(key, value);
    }
    if !body.is_empty() {
        request = request.body(body);
    }

    match request.send().await {
        Ok(upstream) => upstream_response(
            Arc::clone(&state.metrics),
            None,
            upstream,
            Vec::new(),
            (None, None),
        ),
        Err(err) => {
            state
                .metrics
                .upstream_errors_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(path = %path, error = %err, "upstream request failed");
            upstream_error_response(&err)
        }
    }
}

/// Builds an axum response that streams the upstream body, optionally
/// holding the queue concurrency permits for the duration of the stream.
pub(crate) fn upstream_response(
    metrics: Arc<Metrics>,
    queue: Option<Arc<ModelQueue>>,
    upstream: reqwest::Response,
    extra_headers: Vec<(String, String)>,
    permits: (Option<OwnedSemaphorePermit>, Option<OwnedSemaphorePermit>),
) -> Response {
    let status = upstream.status();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let mut builder = Response::builder().status(status);
    if let Ok(content_type_value) = content_type.parse::<HeaderValue>() {
        builder = builder.header(header::CONTENT_TYPE, content_type_value);
    }
    for (name, value) in extra_headers {
        // Parsed defensively: untrusted input (e.g. a hostile `model` string)
        // must never be able to crash the proxy.
        let Ok(name) = name.parse::<HeaderName>() else {
            continue;
        };
        let Ok(value) = value.parse::<HeaderValue>() else {
            continue;
        };
        builder = builder.header(name, value);
    }

    let stream = upstream.bytes_stream();
    let body = Body::from_stream(StreamingBody::new(stream, metrics, queue, permits));
    builder
        .body(body)
        .expect("status and headers built above are always valid")
}

pub(crate) fn queue_full_response(model: &str, depth: usize) -> Response {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(header::RETRY_AFTER, "2")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(error_json_body(format!(
            "paddock queue for model '{model}' is full ({depth} requests waiting); try again shortly"
        ))))
        .expect("static response")
}

pub(crate) fn queue_wait_timeout_response() -> Response {
    error_json(
        StatusCode::GATEWAY_TIMEOUT,
        "request timed out waiting for a free slot in the paddock queue",
    )
}

pub(crate) fn upstream_error_response(err: &reqwest::Error) -> Response {
    error_json(
        StatusCode::BAD_GATEWAY,
        format!("upstream request failed: {err}"),
    )
}

pub(crate) fn internal_error_response(message: &str) -> Response {
    error_json(StatusCode::INTERNAL_SERVER_ERROR, message)
}

fn error_json(status: StatusCode, message: impl Into<String>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(error_json_body(message)))
        .expect("static response")
}

fn error_json_body(message: impl Into<String>) -> String {
    let message = message.into();
    serde_json::to_string(&json!({ "error": message })).expect("error payload always serializes")
}

/// Liveness of the proxy plus reachability of the upstream.
pub async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let probe = state
        .upstream
        .get(format!("{}/api/version", state.base_url))
        .send();
    let upstream_reachable = matches!(
        tokio::time::timeout(Duration::from_secs(3), probe).await,
        Ok(Ok(response)) if response.status().is_success()
    );
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "upstream": {
            "url": state.base_url,
            "reachable": upstream_reachable,
        }
    }))
}

/// Prometheus-style metrics.
pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    use std::fmt::Write;

    let m = &state.metrics;
    let model_stats = state.model_stats();
    let waiting: usize = model_stats.iter().map(|(_, waiting, _)| *waiting).sum();

    let mut text = String::new();
    let _ = writeln!(
        text,
        "# HELP paddock_requests_total Inference requests received on queued endpoints."
    );
    let _ = writeln!(text, "# TYPE paddock_requests_total counter");
    let _ = writeln!(
        text,
        "paddock_requests_total {}",
        m.requests_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_queued_total Requests accepted into a model queue."
    );
    let _ = writeln!(text, "# TYPE paddock_queued_total counter");
    let _ = writeln!(
        text,
        "paddock_queued_total {}",
        m.queued_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_rejected_total Requests rejected because a model queue was full."
    );
    let _ = writeln!(text, "# TYPE paddock_rejected_total counter");
    let _ = writeln!(
        text,
        "paddock_rejected_total {}",
        m.rejected_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_queue_timeouts_total Requests rejected after exceeding the queue wait timeout."
    );
    let _ = writeln!(text, "# TYPE paddock_queue_timeouts_total counter");
    let _ = writeln!(
        text,
        "paddock_queue_timeouts_total {}",
        m.queue_timeouts_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_upstream_errors_total Requests that failed at the upstream."
    );
    let _ = writeln!(text, "# TYPE paddock_upstream_errors_total counter");
    let _ = writeln!(
        text,
        "paddock_upstream_errors_total {}",
        m.upstream_errors_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_active_requests Requests currently being served by the upstream (streaming included)."
    );
    let _ = writeln!(text, "# TYPE paddock_active_requests gauge");
    let _ = writeln!(
        text,
        "paddock_active_requests {}",
        m.active.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        text,
        "# HELP paddock_waiting_requests Requests waiting across all model queues."
    );
    let _ = writeln!(text, "# TYPE paddock_waiting_requests gauge");
    let _ = writeln!(text, "paddock_waiting_requests {waiting}");
    let _ = writeln!(
        text,
        "# HELP paddock_queue_depth Requests waiting per model."
    );
    let _ = writeln!(text, "# TYPE paddock_queue_depth gauge");
    for (model, waiting, _) in &model_stats {
        let _ = writeln!(
            text,
            "paddock_queue_depth{{model=\"{}\"}} {waiting}",
            escape_label(model)
        );
    }
    let _ = writeln!(
        text,
        "# HELP paddock_model_active_requests Requests currently being served per model (streaming included)."
    );
    let _ = writeln!(text, "# TYPE paddock_model_active_requests gauge");
    for (model, _, active) in &model_stats {
        let _ = writeln!(
            text,
            "paddock_model_active_requests{{model=\"{}\"}} {active}",
            escape_label(model)
        );
    }

    Response::builder()
        .header(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(text))
        .expect("metrics response")
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Service description / landing page.
pub async fn root(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "service": "paddock",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "Request-queueing reverse proxy for Ollama",
        "upstream": state.base_url,
        "endpoints": {
            "queued inference": [
                "POST /api/chat",
                "POST /api/generate",
                "POST /v1/chat/completions",
                "POST /v1/completions"
            ],
            "direct proxy": "anything else (e.g. GET /api/tags, POST /api/pull)",
            "health": "GET /health",
            "metrics": "GET /metrics"
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_model_from_json_body() {
        let body = Bytes::from_static(br#"{"model":"llama3","messages":[]}"#);
        assert_eq!(extract_model(&body), "llama3");
    }

    #[test]
    fn model_extraction_falls_back_to_unknown() {
        assert_eq!(extract_model(&Bytes::from_static(b"not json")), "unknown");
        assert_eq!(
            extract_model(&Bytes::from_static(br#"{"model": 42}"#)),
            "unknown"
        );
        assert_eq!(extract_model(&Bytes::from_static(b"")), "unknown");
    }

    #[test]
    fn strips_connection_specific_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "example.com".parse().unwrap());
        headers.insert("content-length", "12".parse().unwrap());
        headers.insert("transfer-encoding", "chunked".parse().unwrap());
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("accept", "application/json".parse().unwrap());

        let forwarded = filter_headers(&headers);
        assert!(!forwarded.contains_key("host"));
        assert!(!forwarded.contains_key("content-length"));
        assert!(!forwarded.contains_key("transfer-encoding"));
        assert!(forwarded.contains_key("authorization"));
        assert!(forwarded.contains_key("accept"));
    }

    #[test]
    fn path_of_keeps_query_string() {
        let uri: Uri = "/api/chat?model=x&y=z".parse().unwrap();
        assert_eq!(path_of(&uri), "/api/chat?model=x&y=z");
        let absolute: Uri = "http://127.0.0.1:8080/v1/chat/completions".parse().unwrap();
        assert_eq!(path_of(&absolute), "/v1/chat/completions");
    }
}
