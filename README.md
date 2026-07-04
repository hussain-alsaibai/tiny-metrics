# tiny-metrics

**Zero-dependency Prometheus-compatible metrics for Python.**

Single-file library that gives you counters, gauges, histograms, summaries,
the OpenMetrics text exposition format, a built-in `/metrics` HTTP endpoint,
and default process/python collectors — without `prometheus_client`, `twisted`,
`aiohttp`, or any other dependency.

> Part of the [`tiny-*` ecosystem](https://github.com/hussain-alsaibai) — 21
> single-file Python libraries, **0 dependencies across the entire stack**.

---

## Why?

`prometheus_client` is excellent but ships ~3 KLOC of collectors, async
helpers, multiprocess shared-memory mode, and optional twisted/aiohttp
exposition. For a service that just wants a `/metrics` endpoint and a few
counters, that's overkill.

`tiny-metrics` gives you:

| Need                                          | `prometheus_client` | `tiny-metrics` |
|-----------------------------------------------|---------------------|----------------|
| Counter, Gauge, Histogram, Summary            | ✓                   | ✓              |
| OpenMetrics text format (`# HELP`, `# TYPE`, `# EOF`) | ✓             | ✓              |
| Built-in `/metrics` HTTP endpoint             | ✓                   | ✓              |
| Process collector (`process_*`)               | ✓                   | ✓              |
| Python platform collector (`python_info`)     | ✓ (gauge)           | ✓              |
| Histogram `.time()` decorator / ctx manager   | ✓                   | ✓              |
| Custom collectors                             | ✓                   | ✓              |
| Multiprocess shared-memory mode               | ✓                   | ✗              |
| Async exposition                              | ✓ (optional)        | ✗              |
| **Runtime dependencies**                       | **0**              | **0**          |
| **Lines of code**                              | **~3 000**         | **~700**       |

For 90% of services the `✗` rows aren't needed. When they are, drop down to
`prometheus_client`.

## Install

```bash
# Copy tiny_metrics.py into your project — that's the whole install.
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-metrics/main/tiny_metrics.py
```

Or as a package:

```bash
pip install tiny-metrics  # coming soon
```

## Quickstart

```python
from tiny_metrics import (
    Counter, Gauge, Histogram, Summary,
    start_http_server, enable_default_collectors,
)

REQUESTS = Counter("http_requests_total", "Total HTTP requests",
                   labelnames=("method", "code"))
INFLIGHT = Gauge("http_inflight_requests", "In-flight requests")
LATENCY  = Histogram("http_request_seconds", "Request latency",
                     buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0))

@LATENCY.time()
def handle(req):
    INFLIGHT.inc()
    try:
        # ... do work ...
        REQUESTS.labels(method=req.method, code=200).inc()
    finally:
        INFLIGHT.dec()

if __name__ == "__main__":
    enable_default_collectors()  # process_cpu_seconds_total, process_resident_memory_bytes, python_info
    start_http_server(port=8000)  # /metrics is now scrape-ready
```

Then point Prometheus at it:

```yaml
scrape_configs:
  - job_name: my-service
    static_configs:
      - targets: ['localhost:8000']
```

## What's emitted

Scrape `/metrics` and you get standard Prometheus format:

```
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{code="200",method="GET"} 142
http_requests_total{code="500",method="POST"} 3
# HELP http_request_seconds Request latency
# TYPE http_request_seconds histogram
http_request_seconds_bucket{le="0.005"} 0
http_request_seconds_bucket{le="0.01"} 1
http_request_seconds_bucket{le="0.05"} 12
http_request_seconds_bucket{le="0.1"} 38
http_request_seconds_bucket{le="0.5"} 130
http_request_seconds_bucket{le="1"} 140
http_request_seconds_bucket{le="5"} 142
http_request_seconds_bucket{le="+Inf"} 142
http_request_seconds_count 142
http_request_seconds_sum 38.21
# HELP process_resident_memory_bytes Resident memory size in bytes
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 25165824
# HELP python_info Python platform information
# TYPE python_info gauge
python_info{implementation="CPython",machine="x86_64",system="Linux",version="3.12.4"} 1.0
# EOF
```

## API

### Counter

```python
c = Counter("requests_total", "Total requests", labelnames=("method", "code"))
c.inc()                              # default labels (none)
c.inc(5, method="GET", code="200")   # +5 to GET/200
c.dec(2, method="GET", code="200")   # counter decrement (allowed)
```

`Counter.inc` rejects negative amounts (raises `ValueError`). `Counter.dec`
exists for the rare "I really mean subtract" case.

### Gauge

```python
g = Gauge("inflight", "In-flight requests")
g.inc(); g.dec(); g.set(42); g.set_to_current_time()   # time.time()

with g.track_inprogress():                # context manager (ctx_mngr) — see below
    do_work()
```

`Gauge` accepts negative increments. Useful for "delta since last scrape"
counters stored as a gauge.

### Histogram

```python
h = Histogram("latency_seconds", "Latency",
              buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1.0))

h.observe(0.123)                # manual
with h.time():                  # context manager
    do_work()
@h.time()                       # decorator
def handler(): ...
```

Buckets are cumulative — `le="1.0"` includes everything that fell into
smaller buckets. `+Inf` always equals `_count`. Default buckets:
`(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)`.

### Summary

```python
s = Summary("rpc_duration_seconds", "RPC latency",
            max_age_seconds=600, age_buckets=5)
s.observe(0.42)
```

`Summary` uses a bounded ring buffer per child and computes quantiles on
scrape (P50, P90, P95, P99). For very-high-cardinality streams prefer
`Histogram`.

### HTTP exposition

```python
start_http_server(port=8000)                       # all interfaces
start_http_server(port=8000, addr="127.0.0.1")     # localhost only
start_http_server(port=0, registry=my_registry)    # ephemeral port
```

Endpoints:
- `GET /metrics` → OpenMetrics text format
- `GET /` → tiny HTML link page

### Collectors

```python
from tiny_metrics import enable_default_collectors
enable_default_collectors()         # process + python_info
```

`ProcessCollector` emits:
- `process_cpu_seconds_total` (counter)
- `process_resident_memory_bytes` (gauge)
- `process_virtual_memory_bytes` (gauge)
- `process_start_time_seconds` (gauge)
- `process_open_fds` (gauge, Linux)

`PlatformCollector` emits:
- `python_info{version, implementation, system, machine}` (gauge=1)

### Custom collectors

```python
from tiny_metrics import _Collector, _Sample

class MyCollector(_Collector):
    def collect(self):
        return [("my_metric", "gauge", "My custom metric",
                 [_Sample("my_value", {"shard": "1"}, 42.0)])]

REGISTRY.register_collector(MyCollector())
```

## Why one file?

The "tiny-" rule is **one file, zero dependencies, MIT-licensed, fully
tested**. This library fits:

- A single `tiny_metrics.py` file (~700 LOC)
- 0 third-party imports
- 23 unit tests, 100% in-process

If you'd rather have async / multiprocess support, use `prometheus_client`.

## Tests

```bash
python3 test_tiny_metrics.py
```

23 tests, covers counter / gauge / histogram / summary / labels / exposition
format / HTTP handler / process collector / thread safety / validation.

## Migration from `prometheus_client`

| `prometheus_client`              | `tiny_metrics` |
|----------------------------------|----------------|
| `Counter("x", "x").inc()`        | same           |
| `Histogram(...).time()`          | same           |
| `start_http_server(port)`        | same           |
| `CollectorRegistry()`            | `MetricRegistry()` |
| `generate_latest(registry)`      | `generate_latest(registry)` |
| `multiprocess.MultiProcessCollector` | not yet supported |
| `prometheus_client.exposition` twisted/aiohttp | stdlib only |

## License

MIT.

## Sibling libraries

- [`tiny-router`](https://github.com/hussain-alsaibai/tiny-router) — HTTP routing
- [`tiny-log`](https://github.com/hussain-alsaibai/tiny-log) — structured logging
- [`tiny-config`](https://github.com/hussain-alsaibai/tiny-config) — config loader
- [`tiny-validator`](https://github.com/hussain-alsaibai/tiny-validator) — input validation
- [`tiny-cli`](https://github.com/hussain-alsaibai/tiny-cli) — CLI builder
- [`tiny-cron`](https://github.com/hussain-alsaibai/tiny-cron) — scheduler
- [`tiny-flags`](https://github.com/hussain-alsaibai/tiny-flags) — feature flags
- [`tiny-queue`](https://github.com/hussain-alsaibai/tiny-queue) — persistent queue
- [`tiny-rate`](https://github.com/hussain-alsaibai/tiny-rate) — rate limiter
- [`tiny-retry`](https://github.com/hussain-alsaibai/tiny-retry) — retry + backoff
- [`tiny-pool`](https://github.com/hussain-alsaibai/tiny-pool) — worker pool
- [`tiny-cache`](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL cache
- [`tiny-trace`](https://github.com/hussain-alsaibai/tiny-trace) — distributed tracing
- [`tiny-secret`](https://github.com/hussain-alsaibai/tiny-secret) — secret loader
- [`tiny-compose`](https://github.com/hussain-alsaibai/tiny-compose) — DI container
- [`tiny-agent`](https://github.com/hussain-alsaibai/tiny-agent) — agent framework
- [`tiny-mcp`](https://github.com/hussain-alsaibai/tiny-mcp) — MCP server
- [`tiny-embed`](https://github.com/hussain-alsaibai/tiny-embed) — embeddings

Built by [OpenClaw](https://github.com/hussain-alsaibai) — autonomous developer agent.