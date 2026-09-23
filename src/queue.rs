use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use axum::response::Response;
use futures_core::stream::Stream;
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};

use crate::state::{AppState, Metrics};

/// A single inference request waiting (or about to be forwarded) for a slot.
pub struct Job {
    pub method: Method,
    /// Request path including any query string, e.g. `/api/chat`.
    pub path: String,
    /// Model the request targets (from the JSON body); used as the queue key.
    pub model: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Absolute deadline after which the request must be rejected instead of served.
    pub deadline: Instant,
    /// When the request was accepted into the queue.
    pub enqueued_at: Instant,
    pub respond: oneshot::Sender<Response>,
}

/// FIFO queue for a single model with a bounded in-flight count.
pub struct ModelQueue {
    pub name: String,
    queue: Mutex<VecDeque<Job>>,
    notified: Notify,
    sem: Arc<Semaphore>,
    /// Jobs currently waiting in the queue.
    depth: AtomicUsize,
    /// Jobs currently being served upstream (response stream not yet complete).
    active: AtomicUsize,
}

impl ModelQueue {
    pub fn new(name: String, concurrency: usize) -> Self {
        Self {
            name,
            queue: Mutex::new(VecDeque::new()),
            notified: Notify::new(),
            sem: Arc::new(Semaphore::new(concurrency.max(1))),
            depth: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        }
    }

    /// Pushes a job unless the queue already holds `max_depth` waiting requests.
    pub fn try_push(&self, job: Job, max_depth: usize) -> bool {
        {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if queue.len() >= max_depth {
                return false;
            }
            queue.push_back(job);
        }
        self.depth.fetch_add(1, Ordering::SeqCst);
        self.notified.notify_one();
        true
    }

    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::SeqCst)
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// Starts the per-model dispatcher task.
///
/// `AppState::queue_for` calls this exactly once per model. The dispatcher
/// owns the forwarding loop for that model: it pops jobs in FIFO order, waits
/// for a global *and* a per-model concurrency slot (bounded by the job's
/// deadline), and forwards the request to the upstream.
pub fn spawn_dispatcher(state: Arc<AppState>, queue: Arc<ModelQueue>) {
    tokio::spawn(async move {
        tracing::debug!(model = %queue.name, "queue dispatcher started");
        loop {
            // Create the `Notified` future *before* checking the queue so that
            // a `notify_one()` arriving between the check and the wait cannot
            // be missed.
            let notified = queue.notified.notified();
            // The guard's scope ends at the block boundary, so it is never
            // held across an `await` (the future must stay `Send`).
            let maybe_job = {
                let mut q = queue
                    .queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                q.pop_front()
            };
            let job = match maybe_job {
                Some(job) => job,
                None => {
                    notified.await;
                    continue;
                }
            };
            queue.depth.fetch_sub(1, Ordering::SeqCst);

            let remaining = job.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                reject_wait_timeout(&state, job);
                continue;
            }

            // Global cap first, then the per-model cap; both waits are
            // deadline-bounded so a job can never wait past its deadline
            // while acquiring slots.
            let global =
                match tokio::time::timeout(remaining, state.global_sem.clone().acquire_owned())
                    .await
                {
                    Ok(Ok(permit)) => permit,
                    _ => {
                        reject_wait_timeout(&state, job);
                        continue;
                    }
                };
            let model = match tokio::time::timeout(
                job.deadline.saturating_duration_since(Instant::now()),
                queue.sem.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                _ => {
                    reject_wait_timeout(&state, job);
                    continue;
                }
            };

            queue.active.fetch_add(1, Ordering::SeqCst);
            state.metrics.active.fetch_add(1, Ordering::SeqCst);
            tracing::debug!(
                model = %job.model,
                waiting = queue.depth(),
                "forwarding request to upstream"
            );
            tokio::spawn(forward(
                Arc::clone(&state),
                Arc::clone(&queue),
                job,
                global,
                model,
            ));
        }
    });
}

/// Responds to a queued job that exceeded its wait deadline, without
/// touching the upstream.
fn reject_wait_timeout(state: &AppState, job: Job) {
    state
        .metrics
        .queue_timeouts_total
        .fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        model = %job.model,
        waited_ms = (Instant::now() - job.enqueued_at).as_millis(),
        "queue wait timeout; rejecting request"
    );
    let _ = job
        .respond
        .send(crate::proxy::queue_wait_timeout_response());
}

/// Forwards one job to the upstream and streams the response back to the
/// client.
///
/// The concurrency permits are stored in the response stream
/// (`StreamingBody`), so the slots stay occupied until the client has
/// actually received the whole response (or disconnected).
async fn forward(
    state: Arc<AppState>,
    queue: Arc<ModelQueue>,
    job: Job,
    global: OwnedSemaphorePermit,
    model: OwnedSemaphorePermit,
) {
    let url = format!("{}{}", state.base_url, job.path);
    let mut request = state.upstream.request(job.method, &url);
    for (key, value) in &job.headers {
        request = request.header(key, value);
    }
    if !job.body.is_empty() {
        request = request.body(job.body);
    }

    match request.send().await {
        Ok(upstream) => {
            let wait_ms = (Instant::now() - job.enqueued_at).as_millis();
            let response = crate::proxy::upstream_response(
                Arc::clone(&state.metrics),
                Some(queue),
                upstream,
                vec![
                    ("x-paddock-queue-wait-ms".to_string(), wait_ms.to_string()),
                    ("x-paddock-model".to_string(), job.model.clone()),
                ],
                (Some(global), Some(model)),
            );
            let _ = job.respond.send(response);
        }
        Err(err) => {
            // No stream is created, so undo the accounting done by the
            // dispatcher (the stream's `Drop` will not run).
            state
                .metrics
                .upstream_errors_total
                .fetch_add(1, Ordering::Relaxed);
            state.metrics.active.fetch_sub(1, Ordering::SeqCst);
            queue.active.fetch_sub(1, Ordering::SeqCst);
            tracing::warn!(
                model = %job.model,
                path = %job.path,
                error = %err,
                "upstream request failed"
            );
            let _ = job
                .respond
                .send(crate::proxy::upstream_error_response(&err));
        }
    }
}

/// Wraps the upstream response stream and owns the concurrency permits.
///
/// Dropping this value (client finished, or disconnected) releases the
/// global and per-model slots and updates the active counters.
pub(crate) struct StreamingBody<S> {
    inner: Option<S>,
    metrics: Arc<Metrics>,
    queue: Option<Arc<ModelQueue>>,
    /// The permits are held for the full lifetime of the stream.
    _permits: (Option<OwnedSemaphorePermit>, Option<OwnedSemaphorePermit>),
}

impl<S> StreamingBody<S> {
    pub(crate) fn new(
        inner: S,
        metrics: Arc<Metrics>,
        queue: Option<Arc<ModelQueue>>,
        permits: (Option<OwnedSemaphorePermit>, Option<OwnedSemaphorePermit>),
    ) -> Self {
        Self {
            inner: Some(inner),
            metrics,
            queue,
            _permits: permits,
        }
    }
}

impl<S> Stream for StreamingBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    type Item = Result<Bytes, reqwest::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Stream::poll_next(Pin::new(inner), cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                this.inner.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for StreamingBody<S> {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.take() {
            queue.active.fetch_sub(1, Ordering::SeqCst);
        }
        if self._permits.0.is_some() {
            self.metrics.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_job() -> Job {
        let (respond, _response) = oneshot::channel();
        Job {
            method: Method::POST,
            path: "/api/chat".into(),
            model: "test".into(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            deadline: Instant::now() + Duration::from_secs(30),
            enqueued_at: Instant::now(),
            respond,
        }
    }

    #[test]
    fn try_push_respects_max_depth() {
        let queue = ModelQueue::new("test".into(), 1);
        assert!(queue.try_push(test_job(), 2));
        assert!(queue.try_push(test_job(), 2));
        assert!(!queue.try_push(test_job(), 2));
        assert_eq!(queue.depth(), 2);
    }
}
