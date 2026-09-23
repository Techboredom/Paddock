use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::queue::{spawn_dispatcher, ModelQueue};

/// Process-wide counters powering the `/metrics` endpoint.
#[derive(Debug)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub queued_total: AtomicU64,
    pub rejected_total: AtomicU64,
    pub queue_timeouts_total: AtomicU64,
    pub upstream_errors_total: AtomicU64,
    pub active: AtomicUsize,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            queued_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            queue_timeouts_total: AtomicU64::new(0),
            upstream_errors_total: AtomicU64::new(0),
            active: AtomicUsize::new(0),
        }
    }
}

/// Shared application state: upstream client, concurrency limits,
/// per-model queues, and metrics.
pub struct AppState {
    pub upstream: Client,
    pub base_url: String,
    pub global_sem: Arc<Semaphore>,
    pub model_concurrency: usize,
    pub max_queue_per_model: usize,
    pub wait_timeout: Duration,
    pub metrics: Arc<Metrics>,
    models: Mutex<HashMap<String, Arc<ModelQueue>>>,
}

impl AppState {
    pub fn new(cfg: Config) -> Self {
        let upstream = Client::builder()
            .timeout(cfg.upstream_timeout)
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("http client builds with valid config");
        Self {
            upstream,
            base_url: cfg.upstream_url,
            global_sem: Arc::new(Semaphore::new(cfg.global_concurrency.max(1))),
            model_concurrency: cfg.model_concurrency.max(1),
            max_queue_per_model: cfg.max_queue_per_model.max(1),
            wait_timeout: cfg.wait_timeout,
            metrics: Arc::new(Metrics::default()),
            models: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the FIFO queue for `model`, creating it (and its dispatcher
    /// task) on first use.
    pub fn queue_for(self: &Arc<Self>, model: &str) -> Arc<ModelQueue> {
        let mut models = self
            .models
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(queue) = models.get(model) {
            return Arc::clone(queue);
        }
        let queue = Arc::new(ModelQueue::new(model.to_string(), self.model_concurrency));
        spawn_dispatcher(Arc::clone(self), Arc::clone(&queue));
        models.insert(model.to_string(), Arc::clone(&queue));
        queue
    }

    /// Snapshot of `(model, waiting, active)` for every model seen so far.
    pub fn model_stats(&self) -> Vec<(String, usize, usize)> {
        let models = self
            .models
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        models
            .values()
            .map(|queue| (queue.name.clone(), queue.depth(), queue.active()))
            .collect()
    }
}
