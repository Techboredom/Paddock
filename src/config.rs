use std::env;
use std::time::Duration;

/// Runtime configuration, read from environment variables.
#[derive(Clone, Debug)]
pub struct Config {
    /// Address the proxy binds to (`host:port`).
    pub listen: String,
    /// Base URL of the Ollama instance (trailing slash is stripped).
    pub upstream_url: String,
    /// Maximum number of requests in flight to Ollama across all models.
    pub global_concurrency: usize,
    /// Maximum number of concurrent requests per model; extras wait in a FIFO queue.
    pub model_concurrency: usize,
    /// Maximum number of waiting requests per model before new ones are rejected (503).
    pub max_queue_per_model: usize,
    /// Maximum time a request may wait in the queue before being rejected (504).
    pub wait_timeout: Duration,
    /// Hard deadline for a single request against the upstream.
    pub upstream_timeout: Duration,
}

impl Config {
    /// Reads the configuration from the environment, falling back to defaults.
    pub fn from_env() -> Self {
        Self {
            listen: env_str("PADDOK_LISTEN", "0.0.0.0:8080"),
            upstream_url: env_str("OLLAMA_URL", "http://127.0.0.1:11434")
                .trim_end_matches('/')
                .to_string(),
            global_concurrency: env_usize("PADDOK_GLOBAL_CONCURRENCY", 4),
            model_concurrency: env_usize("PADDOK_MODEL_CONCURRENCY", 1),
            max_queue_per_model: env_usize("PADDOK_MAX_QUEUE_PER_MODEL", 32),
            wait_timeout: env_secs("PADDOK_QUEUE_WAIT_TIMEOUT_SECS", 120),
            upstream_timeout: env_secs("PADDOK_UPSTREAM_TIMEOUT_SECS", 600),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    match env::var(key).ok().and_then(|raw| raw.parse::<usize>().ok()) {
        Some(value) => value.max(1),
        None => default,
    }
}

fn env_secs(key: &str, default: u64) -> Duration {
    let secs = match env::var(key).ok().and_then(|raw| raw.parse::<u64>().ok()) {
        Some(value) => value.max(1),
        None => default,
    };
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_helpers_parse_and_clamp() {
        std::env::remove_var("PADDOK_UNIT_TEST_USIZE");
        assert_eq!(env_usize("PADDOK_UNIT_TEST_USIZE", 7), 7);

        std::env::set_var("PADDOK_UNIT_TEST_USIZE", "0");
        assert_eq!(env_usize("PADDOK_UNIT_TEST_USIZE", 7), 1);

        std::env::set_var("PADDOK_UNIT_TEST_USIZE", "not-a-number");
        assert_eq!(env_usize("PADDOK_UNIT_TEST_USIZE", 7), 7);

        std::env::remove_var("PADDOK_UNIT_TEST_SECS");
        assert_eq!(
            env_secs("PADDOK_UNIT_TEST_SECS", 30),
            Duration::from_secs(30)
        );

        std::env::set_var("PADDOK_UNIT_TEST_SECS", "10");
        assert_eq!(
            env_secs("PADDOK_UNIT_TEST_SECS", 30),
            Duration::from_secs(10)
        );

        std::env::remove_var("PADDOK_UNIT_TEST_STR");
        assert_eq!(env_str("PADDOK_UNIT_TEST_STR", "d"), "d");
        std::env::set_var("PADDOK_UNIT_TEST_STR", "x");
        assert_eq!(env_str("PADDOK_UNIT_TEST_STR", "d"), "x");
        std::env::remove_var("PADDOK_UNIT_TEST_STR");
    }
}
