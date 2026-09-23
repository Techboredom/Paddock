# Paddock

A request-queueing reverse proxy for [Ollama](https://ollama.com), written in Rust.

When coding agents (or any client) fire multiple LLM requests at Ollama simultaneously,
the server can get overwhelmed: duplicated model contexts in memory, scheduler thrash,
timeouts, or outright OOM. Paddock sits in front of Ollama and turns those chaotic
bursts into a predictable, bounded queue.

## How it works

Every inference request (`POST /api/chat`, `POST /api/generate`,
`POST /v1/chat/completions`, `POST /v1/completions`) is:

1. **Classified by model** — the `model` field of the JSON body picks the queue.
2. **Admitted only if the queue has room** — otherwise it gets a fast `503` with
   `Retry-After` (backpressure instead of unbounded pile-up).
3. **Served FIFO, one at a time per model** (or `PADDOK_MODEL_CONCURRENCY` at a time).
4. **Capped globally** — at most `PADDOK_GLOBAL_CONCURRENCY` requests are in flight
   to Ollama across all models.
5. **Deadline-checked** — a request that waits in the queue longer than
   `PADDOK_QUEUE_WAIT_TIMEOUT_SECS` is rejected with `504`, so agents fail fast
   instead of hanging.

Concurrency slots are held until the response stream has actually been delivered to the
client (or the client disconnects), so the proxy's accounting matches what Ollama is
doing server-side. All other endpoints (model listing, pull, show, …) are proxied
directly without queueing. Payloads pass through unmodified.

## Quick start

### From source

```bash
cargo run
# => listening on 0.0.0.0:8080, proxying to http://127.0.0.1:11434
```

Point your client at the proxy:

```bash
curl http://localhost:8080/api/chat -d '{
  "model": "llama3",
  "messages": [{"role": "user", "content": "hi"}],
  "stream": false
}'
```

For the OpenAI-compatible API, use `http://localhost:8080/v1` as the base URL
(e.g. `http://localhost:8080/v1/chat/completions`).

### Docker

```bash
docker build -t paddock .

# macOS / Windows (Ollama running on the host):
docker run --rm -p 8080:8080 \
  -e OLLAMA_URL=http://host.docker.internal:11434 \
  paddock

# Linux, Ollama on the same host, host networking:
docker run --rm --network host \
  -e OLLAMA_URL=http://127.0.0.1:11434 \
  paddock
```

Or use the provided compose file:

```bash
docker compose up --build
```

## Configuration

Everything is configured via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `PADDOK_LISTEN` | `0.0.0.0:8080` | Address the proxy binds to. |
| `OLLAMA_URL` | `http://127.0.0.1:11434` | Base URL of the Ollama instance. |
| `PADDOK_GLOBAL_CONCURRENCY` | `4` | Max requests in flight to Ollama across all models. |
| `PADDOK_MODEL_CONCURRENCY` | `1` | Max concurrent requests per model; the rest wait FIFO. |
| `PADDOK_MAX_QUEUE_PER_MODEL` | `32` | Queue depth per model before `503` backpressure. |
| `PADDOK_QUEUE_WAIT_TIMEOUT_SECS` | `120` | Max time a request may wait in the queue before `504`. |
| `PADDOK_UPSTREAM_TIMEOUT_SECS` | `600` | Hard deadline for a single upstream request. |
| `RUST_LOG` | `info,paddock=debug` | Log filter (e.g. `debug`, `paddock=trace`). |

## Observability

- `GET /health` — liveness plus upstream reachability.
- `GET /metrics` — Prometheus text format:

  ```text
  paddock_requests_total 148
  paddock_queued_total 139
  paddock_rejected_total 3
  paddock_queue_timeouts_total 1
  paddock_upstream_errors_total 0
  paddock_active_requests 2
  paddock_waiting_requests 1
  paddock_queue_depth{model="llama3"} 1
  paddock_model_active_requests{model="llama3"} 1
  ```

- Every queued response carries `X-Paddock-Queue-Wait-Ms` (how long it waited) and
  `X-Paddock-Model`.
- Structured `tracing` logs: dispatcher lifecycle, queue-full rejections, wait
  timeouts, upstream errors.

## Failure semantics

| Situation | Response |
| --- | --- |
| Model queue full | `503` + `Retry-After: 2` + JSON error |
| Queue wait timeout | `504` + JSON error |
| Upstream unreachable / error | `502` + JSON error |
| Ollama's own error (e.g. model not found) | passed through with Ollama's status and body |

## Tuning for agents

- **One model, chatty agent** — the defaults are right: strict per-model FIFO keeps
  Ollama from duplicating context for the same model.
- **Multiple models in parallel** — raise `PADDOK_GLOBAL_CONCURRENCY` to the number
  of models you expect to hit at once; Ollama can hold several models in memory
  concurrently (watch `keep_alive` and RAM).
- **Long generations + impatient clients** — raise `PADDOK_QUEUE_WAIT_TIMEOUT_SECS`
  so queued requests are not rejected while a long stream is in flight, and raise
  `PADDOK_MAX_QUEUE_PER_MODEL` to absorb deeper bursts.
- **Want some per-model parallelism** — set `PADDOK_MODEL_CONCURRENCY=2` if your
  GPU/VRAM can handle two streams of the same model at once.

## Development

```bash
cargo test          # unit + queueing integration tests (mock Ollama)
cargo clippy --all-targets
```

The integration tests in `tests/queueing.rs` spin up a mock Ollama and assert:

- concurrent requests for one model never overlap (peak in-flight = 1),
- the queue rejects with `503` when full,
- requests that exceed their wait deadline are rejected with `504`.

## Limitations

- The upstream is reached over plain HTTP (no TLS to Ollama). Fine for local/LAN use;
  put Paddock behind a TLS-terminating proxy if you expose it further.
- Request bodies are buffered in memory (bounded to 64 MB) — inference payloads are
  small JSON, so this is not an issue in practice.
- Queues are in-memory: restarting Paddock drops waiting requests.
