//! End-to-end queueing tests: a mock Ollama counts its in-flight requests
//! while bursts of traffic go through a real Paddock router.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::routing::post;
use axum::Json;
use axum::Router;
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

use paddock::build_router;
use paddock::config::Config;
use paddock::state::AppState;

/// A mock Ollama that counts concurrent in-flight requests to `/api/chat`.
fn mock_ollama(inflight: Arc<AtomicIsize>, peak: Arc<AtomicIsize>, delay: Duration) -> Router {
    Router::new().route(
        "/api/chat",
        post(move |_body: axum::body::Bytes| {
            let inflight = inflight.clone();
            let peak = peak.clone();
            async move {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                Json(json!({ "model": "test", "done": true }))
            }
        }),
    )
}

async fn spawn_mock(
    inflight: Arc<AtomicIsize>,
    peak: Arc<AtomicIsize>,
    delay: Duration,
) -> SocketAddr {
    let mock = mock_ollama(inflight, peak, delay);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("mock listener address");
    tokio::spawn(async move {
        axum::serve(listener, mock).await.expect("mock serves");
    });
    addr
}

fn test_config(addr: SocketAddr, global: usize, model: usize, max_queue: usize) -> Config {
    Config {
        listen: "127.0.0.1:0".into(),
        upstream_url: format!("http://{addr}"),
        global_concurrency: global,
        model_concurrency: model,
        max_queue_per_model: max_queue,
        wait_timeout: Duration::from_secs(30),
        upstream_timeout: Duration::from_secs(30),
    }
}

/// Sends one request through the paddock router and fully reads the
/// response body. Reading the body to the end is important: paddock keeps
/// a concurrency slot held until the response stream is delivered, so a test
/// that parks unconsumed responses would deadlock the queue.
async fn send_request(app: Router, payload: String) -> (StatusCode, String) {
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/chat")
        .header("content-type", "application/json")
        .body(Body::from(payload))
        .expect("valid request");
    let response = app.oneshot(request).await.expect("request succeeds");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    (
        status,
        String::from_utf8(bytes.to_vec()).expect("utf-8 body"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_for_one_model_are_serialized() {
    let inflight = Arc::new(AtomicIsize::new(0));
    let peak = Arc::new(AtomicIsize::new(0));
    let addr = spawn_mock(inflight.clone(), peak.clone(), Duration::from_millis(80)).await;

    let state = Arc::new(AppState::new(test_config(addr, 2, 1, 16)));
    let app = build_router(state);
    let payload = json!({ "model": "test", "messages": [], "stream": false }).to_string();

    let (first, second, third) = tokio::join!(
        tokio::spawn(send_request(app.clone(), payload.clone())),
        tokio::spawn(send_request(app.clone(), payload.clone())),
        tokio::spawn(send_request(app.clone(), payload)),
    );
    for result in [first, second, third] {
        let (status, body) = result.expect("task joins");
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"done\":true"));
    }
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "only one request per model may run at a time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_full_rejects_with_503() {
    let inflight = Arc::new(AtomicIsize::new(0));
    let peak = Arc::new(AtomicIsize::new(0));
    let addr = spawn_mock(inflight.clone(), peak.clone(), Duration::from_millis(300)).await;

    // One global slot, one model slot, a queue that fits exactly one
    // waiting request.
    let state = Arc::new(AppState::new(test_config(addr, 1, 1, 1)));
    let app = build_router(state);
    let payload = json!({ "model": "test", "messages": [], "stream": false }).to_string();

    // First request takes the only slot (in flight at the mock for 300ms).
    let first = tokio::spawn(send_request(app.clone(), payload.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second request is popped by the dispatcher immediately and waits for
    // the global slot — it is no longer in the queue.
    let second = tokio::spawn(send_request(app.clone(), payload.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Third request sits in the queue (depth 1 = max).
    let third = tokio::spawn(send_request(app.clone(), payload.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fourth request must be rejected: the queue is full.
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/chat")
        .header("content-type", "application/json")
        .body(Body::from(payload.clone()))
        .expect("valid request");
    let response = app.oneshot(request).await.expect("request succeeds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get("retry-after").is_some());
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body = String::from_utf8(bytes.to_vec()).expect("utf-8 body");
    assert!(body.contains("full"));

    // The earlier requests are all served, in order.
    assert_eq!(first.await.unwrap().0, StatusCode::OK);
    assert_eq!(second.await.unwrap().0, StatusCode::OK);
    assert_eq!(third.await.unwrap().0, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_request_exceeding_wait_deadline_is_rejected() {
    let inflight = Arc::new(AtomicIsize::new(0));
    let peak = Arc::new(AtomicIsize::new(0));
    let addr = spawn_mock(inflight.clone(), peak.clone(), Duration::from_millis(500)).await;

    let mut config = test_config(addr, 1, 1, 4);
    config.wait_timeout = Duration::from_millis(100);
    let state = Arc::new(AppState::new(config));
    let app = build_router(state);
    let payload = json!({ "model": "test", "messages": [], "stream": false }).to_string();

    // Hold the single slot for 500ms.
    let first = tokio::spawn(send_request(app.clone(), payload.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // This request's 100ms deadline expires while it waits for the slot.
    let (status, body) = send_request(app, payload).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(body.contains("timed out"));

    assert_eq!(first.await.unwrap().0, StatusCode::OK);
}
