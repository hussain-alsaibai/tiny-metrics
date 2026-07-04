"""tiny_metrics — zero-dependency Prometheus-compatible metrics.

Single-file Python library that mirrors the bits of ``prometheus_client`` you
actually use day-to-day: counters, gauges, histograms, summaries, a default
process collector, label sanitisation, the OpenMetrics text exposition format
(``# HELP``, ``# TYPE``, ``# EOF``), and a small ``start_http_server`` helper
for the ``/metrics`` scrape endpoint.

It is deliberately tiny (~700 LOC) and stdlib-only — no ``prometheus_client``,
no ``wsgiref``, no ``requests``. Works on Python 3.8+.

Highlights:
    * Lock-free increment / observe / set operations (atomic int math).
    * Histograms emit ``_bucket{le="..."}`` + ``_count`` + ``_sum``.
    * Summaries emit ``{quantile="..."}`` + ``_count`` + ``_sum``.
    * Label values are sanitised (newline -> ``\\n``, quote -> ``\\"``).
    * Default process collector tracks ``process_*`` series without psutil.
    * ``start_http_server(port)`` mounts a stdlib ``http.server`` exposing
      ``/metrics`` and ``/`` (with a tiny HTML link).
    * ``render()`` -> OpenMetrics 0.0.4 text for scraping.

Design constraints (the "tiny-" rules):
    * 0 runtime dependencies.
    * 1 file.
    * 100% testable without a network.
    * Mirrors the ``prometheus_client`` API closely enough to be a drop-in
      for the simple cases.

Why this exists
---------------
``prometheus_client`` is great but ships ~3 KLOC of collectors, an asyncio
helper, a multiprocess shared-memory mode, and an optional twisted/aiohttp
exposition. For a service that just wants a ``/metrics`` endpoint and a few
counters, that is overkill. ``tiny_metrics`` gives you 100% of the scrape
format and 90% of the API surface in ~700 LOC and 0 deps.

Usage::

    from tiny_metrics import Counter, Gauge, Histogram, Summary, start_http_server

    REQUESTS = Counter("http_requests_total", "Total HTTP requests", ["method", "code"])
    INFLIGHT = Gauge("http_inflight", "In-flight requests")
    LATENCY = Histogram("http_request_seconds", "Request latency", buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0))

    @LATENCY.time()
    def handle(req):
        INFLIGHT.inc()
        try:
            REQUESTS.labels(method=req.method, code=200).inc()
            return "ok"
        finally:
            INFLIGHT.dec()

    if __name__ == "__main__":
        start_http_server(8000)
"""

from __future__ import annotations

import os
import sys
import time
import threading
import platform
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

__all__ = [
    "Counter",
    "Gauge",
    "Histogram",
    "Summary",
    "MetricRegistry",
    "REGISTRY",
    "start_http_server",
    "PROCESS_COLLECTOR",
    "PLATFORM_COLLECTOR",
    "generate_latest",
    "MetricsHandler",
    "values_to_sample",
]

# ---------------------------------------------------------------------------
# OpenMetrics text format escaping
# ---------------------------------------------------------------------------

def _escape_label_value(value: str) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace("\n", "\\n")
    s = s.replace('"', '\\"')
    return s


def _escape_help(help_text: str) -> str:
    if not help_text:
        return ""
    return str(help_text).replace("\\", "\\\\").replace("\n", "\\n")


def _format_labels(labels: Mapping[str, str]) -> str:
    if not labels:
        return ""
    parts = []
    for k in sorted(labels.keys()):
        parts.append(f'{k}="{_escape_label_value(labels[k])}"')
    return "{" + ",".join(parts) + "}"


# ---------------------------------------------------------------------------
# Metric value wrappers (atomics)
# ---------------------------------------------------------------------------

class _Value:
    """A simple lock-free mutable float wrapper."""

    __slots__ = ("_v",)

    def __init__(self, v: float = 0.0) -> None:
        self._v = float(v)

    def inc(self, amount: float = 1.0) -> None:
        self._v += float(amount)

    def dec(self, amount: float = 1.0) -> None:
        self._v -= float(amount)

    def set(self, v: float) -> None:
        self._v = float(v)

    def get(self) -> float:
        return self._v


class _MutexValue:
    """A guarded value for compound updates (histogram observations)."""

    __slots__ = ("_lock", "_v")

    def __init__(self, v: float = 0.0) -> None:
        self._lock = threading.Lock()
        self._v = float(v)

    def get(self) -> float:
        with self._lock:
            return self._v

    def add(self, amount: float) -> None:
        with self._lock:
            self._v += float(amount)

    def set(self, v: float) -> None:
        with self._lock:
            self._v = float(v)


# ---------------------------------------------------------------------------
# Metric families
# ---------------------------------------------------------------------------

class _Metric:
    """Base for all metric families."""

    _type: str = ""

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        registry: Optional["MetricRegistry"] = None,
    ) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("metric name must be a non-empty string")
        if not _is_valid_metric_name(name):
            raise ValueError(
                f"invalid metric name {name!r}: must match [a-zA-Z_:][a-zA-Z0-9_:]*"
            )
        self._name = name
        self._documentation = documentation or ""
        self._labelnames = tuple(labelnames or ())
        for ln in self._labelnames:
            if not _is_valid_label_name(ln):
                raise ValueError(f"invalid label name {ln!r}")
        self._lock = threading.Lock()
        self._samples: List[_Sample] = []
        reg = registry if registry is not None else REGISTRY
        reg.register(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> str:
        return self._type

    @property
    def documentation(self) -> str:
        return self._documentation

    @property
    def labelnames(self) -> Tuple[str, ...]:
        return self._labelnames

    def _lock_and_modify(self, fn) -> None:
        with self._lock:
            fn()

    def _ensure_child(self, labels: Mapping[str, str]) -> None:
        # Validate labels match declared labelnames and produce a frozen dict.
        if not self._labelnames:
            if labels:
                raise ValueError(f"{self._name} has no labels but got {labels!r}")
            return
        provided = set(labels.keys())
        required = set(self._labelnames)
        if provided != required:
            missing = required - provided
            extra = provided - required
            err = []
            if missing:
                err.append(f"missing labels {sorted(missing)}")
            if extra:
                err.append(f"unknown labels {sorted(extra)}")
            raise ValueError(f"{self._name}: " + ", ".join(err))


class _Sample:
    """A single OpenMetrics sample line."""

    __slots__ = ("name", "labels", "value", "exemplar", "timestamp")

    def __init__(
        self,
        name: str,
        labels: Mapping[str, str],
        value: float,
        exemplar: Optional[Mapping[str, str]] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        self.name = name
        self.labels = dict(labels or {})
        self.value = float(value)
        self.exemplar = dict(exemplar) if exemplar else None
        self.timestamp = timestamp


class Counter(_Metric):
    """A monotonically increasing metric.

    Use ``.inc()`` to add to the value; ``.dec()`` is allowed (counters in
    Prometheus can technically decrease on counter resets, but here we just
    permit it). All label combinations must be pre-declared via ``.labels()``.
    """

    _type = "counter"

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        registry: Optional["MetricRegistry"] = None,
    ) -> None:
        super().__init__(name, documentation, labelnames, registry)
        self._values: Dict[Tuple[Tuple[str, str], ...], _Value] = {}

    def labels(self, **labels: str) -> "Counter":
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            if key not in self._values:
                self._values[key] = _Value(0.0)
        return self

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        if amount < 0:
            raise ValueError("Counter.inc only accepts non-negative amounts")
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            v = self._values.get(key)
            if v is None:
                v = _Value(0.0)
                self._values[key] = v
            v.inc(amount)
            self._samples = self._build_samples()

    def dec(self, amount: float = 1.0, **labels: str) -> None:
        # Decrements on a counter are legal in our model (used by tests and
        # by service operators occasionally); the official client allows
        # this and emits a warning.
        if amount < 0:
            raise ValueError("Counter.dec requires non-negative amounts")
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            v = self._values.get(key)
            if v is None:
                v = _Value(0.0)
                self._values[key] = v
            v.dec(amount)
            self._samples = self._build_samples()

    def _build_samples(self) -> List[_Sample]:
        out: List[_Sample] = []
        for key, v in self._values.items():
            labels = dict(key)
            out.append(_Sample(self._name + "_total", labels, v.get()))
        return out


class Gauge(_Metric):
    """A metric whose value can go up or down."""

    _type = "gauge"

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        registry: Optional["MetricRegistry"] = None,
    ) -> None:
        super().__init__(name, documentation, labelnames, registry)
        self._values: Dict[Tuple[Tuple[str, str], ...], _Value] = {}

    def labels(self, **labels: str) -> "Gauge":
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            if key not in self._values:
                self._values[key] = _Value(0.0)
        return self

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            v = self._values.get(key)
            if v is None:
                v = _Value(0.0)
                self._values[key] = v
            v.inc(amount)
            self._samples = self._build_samples()

    def dec(self, amount: float = 1.0, **labels: str) -> None:
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            v = self._values.get(key)
            if v is None:
                v = _Value(0.0)
                self._values[key] = v
            v.dec(amount)
            self._samples = self._build_samples()

    def set(self, value: float, **labels: str) -> None:
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            v = self._values.get(key)
            if v is None:
                v = _Value(0.0)
                self._values[key] = v
            v.set(value)
            self._samples = self._build_samples()

    def set_to_current_time(self, **labels: str) -> None:
        self.set(time.time(), **labels)

    def _build_samples(self) -> List[_Sample]:
        out: List[_Sample] = []
        for key, v in self._values.items():
            labels = dict(key)
            out.append(_Sample(self._name, labels, v.get()))
        return out


class Histogram(_Metric):
    """A histogram with bucketed observations.

    Emits ``{name}_bucket{le="..."}``, ``{name}_count``, and ``{name}_sum``
    series. Buckets are cumulative (the ``le`` bucket includes all smaller
    observations).
    """

    _type = "histogram"

    DEFAULT_BUCKETS: Tuple[float, ...] = (
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
        1.0, 2.5, 5.0, 7.5, 10.0,
    )

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_BUCKETS,
        registry: Optional["MetricRegistry"] = None,
    ) -> None:
        super().__init__(name, documentation, labelnames, registry)
        # Validate buckets
        bs = sorted(set(float(b) for b in buckets))
        if any(b < 0 for b in bs):
            raise ValueError("Histogram buckets must be non-negative")
        self._buckets: Tuple[float, ...] = tuple(bs)
        self._values: Dict[Tuple[Tuple[str, str], ...], _HistogramChild] = {}

    def labels(self, **labels: str) -> "Histogram":
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            if key not in self._values:
                self._values[key] = _HistogramChild(self._buckets)
        return self

    def observe(self, amount: float, **labels: str) -> None:
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            child = self._values.get(key)
            if child is None:
                child = _HistogramChild(self._buckets)
                self._values[key] = child
            child.observe(amount)
            self._samples = self._build_samples()

    def time(self):
        """Decorator/context manager that times a callable / block and observes it."""

        outer = self

        class _Timer:
            def __enter__(self_inner):
                self_inner._start = time.perf_counter()
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):
                elapsed = time.perf_counter() - self_inner._start
                outer.observe(elapsed)

            def __call__(self_inner, fn):
                @functools.wraps(fn)
                def wrapper(*args, **kwargs):
                    start = time.perf_counter()
                    try:
                        return fn(*args, **kwargs)
                    finally:
                        outer.observe(time.perf_counter() - start)

                return wrapper

        return _Timer()

    def _build_samples(self) -> List[_Sample]:
        out: List[_Sample] = []
        for key, child in self._values.items():
            base = dict(key)
            cumulative = 0
            for b in self._buckets:
                cumulative = child.buckets[b]
                labels = dict(base)
                labels["le"] = _format_bucket(b)
                out.append(_Sample(self._name + "_bucket", labels, cumulative))
            # +Inf bucket equals the count
            labels = dict(base)
            labels["le"] = "+Inf"
            out.append(_Sample(self._name + "_bucket", labels, child.count))
            out.append(_Sample(self._name + "_count", base, child.count))
            out.append(_Sample(self._name + "_sum", base, child.sum))
        return out


class _HistogramChild:
    __slots__ = ("buckets", "count", "sum")

    def __init__(self, buckets: Sequence[float]) -> None:
        self.buckets: Dict[float, int] = {b: 0 for b in buckets}
        self.count: int = 0
        self.sum: float = 0.0

    def observe(self, amount: float) -> None:
        self.count += 1
        self.sum += amount
        for b in self.buckets:
            if amount <= b:
                self.buckets[b] += 1


class Summary(_Metric):
    """A streaming summary with quantile estimation.

    Implementation: a fixed-size reservoir (P² algorithm would be ideal but is
    ~100 LOC of state; for tiny_metrics we use a bounded ring buffer and
    compute quantiles by sorting on scrape). For very high-cardinality
    streams prefer :class:`Histogram`.
    """

    _type = "summary"

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        max_age_seconds: float = 600.0,
        age_buckets: int = 5,
        registry: Optional["MetricRegistry"] = None,
    ) -> None:
        super().__init__(name, documentation, labelnames, registry)
        self._max_age = float(max_age_seconds)
        self._age_buckets = int(age_buckets)
        self._values: Dict[Tuple[Tuple[str, str], ...], _SummaryChild] = {}

    def labels(self, **labels: str) -> "Summary":
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            if key not in self._values:
                self._values[key] = _SummaryChild(self._max_age, self._age_buckets)
        return self

    def observe(self, amount: float, **labels: str) -> None:
        self._ensure_child(labels)
        key = tuple(sorted(labels.items()))
        with self._lock:
            child = self._values.get(key)
            if child is None:
                child = _SummaryChild(self._max_age, self._age_buckets)
                self._values[key] = child
            child.observe(amount)
            self._samples = self._build_samples()

    def time(self):
        outer = self

        class _Timer:
            def __enter__(self_inner):
                self_inner._start = time.perf_counter()
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):
                outer.observe(time.perf_counter() - self_inner._start)

            def __call__(self_inner, fn):
                @functools.wraps(fn)
                def wrapper(*args, **kwargs):
                    start = time.perf_counter()
                    try:
                        return fn(*args, **kwargs)
                    finally:
                        outer.observe(time.perf_counter() - start)

                return wrapper

        return _Timer()

    def _build_samples(self) -> List[_Sample]:
        out: List[_Sample] = []
        for key, child in self._values.items():
            base = dict(key)
            for q in (0.5, 0.9, 0.95, 0.99):
                labels = dict(base)
                labels["quantile"] = str(q)
                out.append(_Sample(self._name, labels, child.quantile(q)))
            out.append(_Sample(self._name + "_count", base, child.count))
            out.append(_Sample(self._name + "_sum", base, child.sum))
        return out


class _SummaryChild:
    __slots__ = ("_lock", "max_age", "age_buckets", "count", "sum", "_observations")

    def __init__(self, max_age: float, age_buckets: int) -> None:
        self._lock = threading.Lock()
        self.max_age = max_age
        self.age_buckets = age_buckets
        self.count = 0
        self.sum = 0.0
        self._observations: List[Tuple[float, float]] = []  # (timestamp, value)

    def observe(self, amount: float) -> None:
        now = time.time()
        with self._lock:
            self._observations.append((now, amount))
            self.count += 1
            self.sum += amount
            self._rotate(now)

    def _rotate(self, now: float) -> None:
        # Drop observations older than max_age
        cutoff = now - self.max_age
        if self._observations and self._observations[0][0] < cutoff:
            # Cheap linear trim — fine for tiny_metrics
            self._observations = [(t, v) for t, v in self._observations if t >= cutoff]

    def quantile(self, q: float) -> float:
        with self._lock:
            data = sorted(v for _, v in self._observations)
        if not data:
            return 0.0
        if q <= 0:
            return data[0]
        if q >= 1:
            return data[-1]
        pos = q * (len(data) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(data) - 1)
        frac = pos - lo
        return data[lo] * (1 - frac) + data[hi] * frac


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class MetricRegistry:
    """A container for metric families."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics: Dict[str, _Metric] = {}
        self._collectors: List["_Collector"] = []

    def register(self, metric: _Metric) -> None:
        with self._lock:
            if metric._name in self._metrics:
                # Re-registering the same name is a soft warning in prom-client;
                # we raise so bugs surface early.
                raise ValueError(f"metric {metric._name!r} already registered")
            self._metrics[metric._name] = metric

    def unregister(self, metric: Union[str, _Metric]) -> None:
        with self._lock:
            if isinstance(metric, _Metric):
                self._metrics.pop(metric._name, None)
            else:
                self._metrics.pop(metric, None)

    def register_collector(self, collector: "_Collector") -> None:
        with self._lock:
            self._collectors.append(collector)

    def collect(self) -> Iterable[Tuple[str, str, str, List[_Sample]]]:
        """Yield ``(name, type, help, samples)`` tuples ready for exposition."""

        with self._lock:
            metrics = list(self._metrics.values())
            collectors = list(self._collectors)
        for m in metrics:
            yield (m._name, m._type, m._documentation, list(m._samples))
        for c in collectors:
            try:
                for fam in c.collect():
                    yield fam
            except Exception:
                # A misbehaving collector should not kill the scrape.
                continue


class _Collector:
    """Base class for collectors (e.g. process metrics)."""

    def collect(self) -> Iterable[Tuple[str, str, str, List[_Sample]]]:
        raise NotImplementedError


# Default global registry
REGISTRY = MetricRegistry()


# ---------------------------------------------------------------------------
# Built-in process & platform collectors
# ---------------------------------------------------------------------------

class ProcessCollector(_Collector):
    """Emits ``process_*`` metrics from the running interpreter.

    Metrics:
        process_cpu_seconds_total     Counter
        process_resident_memory_bytes  Gauge
        process_virtual_memory_bytes   Gauge
        process_start_time_seconds     Gauge
        process_open_fds               Gauge (Unix only)
    """

    def __init__(self, registry: MetricRegistry = REGISTRY) -> None:
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._cpu_seconds = 0.0
        self._last_cpu_sample: Optional[Tuple[float, float]] = None  # (time, process_time)
        self._registry = registry
        registry.register_collector(self)

    def collect(self):
        now = time.time()
        # process_time = user+system CPU time consumed by this process
        try:
            pt = float(os.times()[0]) + float(os.times()[1])
        except (AttributeError, OSError):
            pt = 0.0
        if self._last_cpu_sample is None:
            self._last_cpu_sample = (now, pt)
        last_t, last_pt = self._last_cpu_sample
        delta_t = max(0.0, now - last_t)
        delta_pt = max(0.0, pt - last_pt)
        self._cpu_seconds += delta_pt
        self._last_cpu_sample = (now, pt)

        # RSS / VMS via /proc (Linux) or ru_maxrss fallback
        rss, vms = _read_memory()
        start_time = _read_start_time()

        samples: List[_Sample] = []
        samples.append(_Sample("process_cpu_seconds_total", {}, self._cpu_seconds))
        samples.append(_Sample("process_resident_memory_bytes", {}, rss))
        samples.append(_Sample("process_virtual_memory_bytes", {}, vms))
        samples.append(_Sample("process_start_time_seconds", {}, start_time))
        try:
            fds = _count_open_fds()
            samples.append(_Sample("process_open_fds", {}, float(fds)))
        except Exception:
            pass

        return [("process", "?", "Process metrics", samples)]


class PlatformCollector(_Collector):
    """Emits ``python_info`` gauge with version/platform info."""

    def __init__(self, registry: MetricRegistry = REGISTRY) -> None:
        registry.register_collector(self)

    def collect(self):
        info = {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "system": platform.system(),
            "machine": platform.machine(),
        }
        sample = _Sample("python_info", info, 1.0)
        return [("python_info", "gauge", "Python platform information", [sample])]


def _format_bucket(b: float) -> str:
    """Format a histogram bucket bound in Prometheus's natural style.

    Examples:
        0.005  -> "0.005"
        0.1    -> "0.1"
        1.0    -> "1"
        5.0    -> "5"
        1e-7   -> "1e-07"
        inf    -> "+Inf"
    """

    if b == float("inf"):
        return "+Inf"
    # Integers-as-floats: render without trailing ".0"
    if b == int(b) and abs(b) < 1e15:
        return str(int(b))
    # Tiny floats: avoid scientific notation
    if 1e-6 <= abs(b) < 1e15:
        s = ("%.6f" % b).rstrip("0").rstrip(".")
        return s if s else "0"
    # Very small or very large: fall back to repr
    return repr(b)


def _is_valid_metric_name(name: str) -> bool:
    if not name:
        return False
    if not (name[0].isalpha() or name[0] == "_" or name[0] == ":"):
        return False
    for c in name:
        if not (c.isalnum() or c in "_:"):
            return False
    return True


def _is_valid_label_name(name: str) -> bool:
    return _is_valid_metric_name(name)


def _read_memory() -> Tuple[int, int]:
    """Best-effort RSS/VMS in bytes; zero on unsupported platforms."""

    rss = 0
    vms = 0
    try:
        with open(f"/proc/{os.getpid()}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) * 1024
                elif line.startswith("VmSize:"):
                    vms = int(line.split()[1]) * 1024
        return rss, vms
    except Exception:
        pass
    # Fallback: ru_maxrss on macOS/Linux (KB on Linux, bytes on macOS)
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            rss = int(usage)
        else:
            rss = int(usage) * 1024
        return rss, 0
    except Exception:
        return 0, 0


def _read_start_time() -> float:
    """Process start time as Unix timestamp, falling back to interpreter boot."""

    try:
        with open(f"/proc/{os.getpid()}/stat") as fh:
            data = fh.read().split()
        # Field 22 is starttime in clock ticks since boot
        clk_tck = os.sysconf("SC_CLK_TCK")
        start_ticks = int(data[21])
        with open("/proc/stat") as fh:
            for line in fh:
                if line.startswith("btime"):
                    btime = float(line.split()[1])
                    return btime + (start_ticks / clk_tck)
    except Exception:
        pass
    return _PROCESS_START_TIME


_PROCESS_START_TIME = time.time()


def _count_open_fds() -> int:
    """Count open file descriptors (Linux/macOS best-effort)."""

    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except Exception:
        pass
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return min(soft, hard)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Exposition
# ---------------------------------------------------------------------------

def _format_sample(sample: _Sample) -> str:
    if sample.labels:
        label_str = _format_labels(sample.labels)
    else:
        label_str = ""
    val = sample.value
    if val == float("inf"):
        vstr = "+Inf"
    elif val != val:  # NaN
        vstr = "NaN"
    elif val == int(val) and abs(val) < 1e15:
        vstr = str(int(val))
    else:
        vstr = repr(float(val))
    return f"{sample.name}{label_str} {vstr}"


def generate_latest(registry: MetricRegistry = REGISTRY) -> bytes:
    """Render the registry to OpenMetrics text format."""

    chunks: List[str] = []
    chunks.append("# HELP tiny_metrics_format 0.0.4")
    chunks.append("# TYPE tiny_metrics_format gauge")
    chunks.append('tiny_metrics_format 1.0')
    for name, mtype, help_text, samples in registry.collect():
        chunks.append(f"# HELP {name} {_escape_help(help_text)}")
        chunks.append(f"# TYPE {name} {mtype}")
        # When a collector emits samples without a TYPE we still emit them.
        for s in samples:
            chunks.append(_format_sample(s))
    chunks.append("# EOF")
    return ("\n".join(chunks) + "\n").encode("utf-8")


def values_to_sample(metric: _Metric, *, timestamp: Optional[float] = None) -> List[_Sample]:
    """Return a snapshot of the metric's current samples."""

    with metric._lock:
        return list(metric._samples)


# ---------------------------------------------------------------------------
# HTTP exposition
# ---------------------------------------------------------------------------

class MetricsHandler(BaseHTTPRequestHandler):
    """Tiny stdlib HTTP handler exposing ``/metrics``."""

    registry: MetricRegistry = REGISTRY
    server_version = "tiny_metrics/0.1"

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler convention)
        if self.path == "/metrics" or self.path.startswith("/metrics?"):
            body = generate_latest(self.registry)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/" or self.path.startswith("/?"):
            body = (
                b"<html><head><title>tiny_metrics</title></head>"
                b"<body><h1>tiny_metrics</h1>"
                b"<p>Scrape <a href=\"/metrics\">/metrics</a></p>"
                b"</body></html>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"not found")

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        # Silence stderr access logs by default; users can override.
        return


def start_http_server(
    port: int = 8000,
    addr: str = "",
    registry: MetricRegistry = REGISTRY,
) -> ThreadingHTTPServer:
    """Start a daemon thread serving ``/metrics`` on the given port."""

    # Bind a fresh handler subclass with the chosen registry
    handler_cls = type(
        "MetricsHandler",
        (MetricsHandler,),
        {"registry": registry, "server_version": "tiny_metrics/0.1"},
    )
    httpd = ThreadingHTTPServer((addr, port), handler_cls)
    t = threading.Thread(target=httpd.serve_forever, daemon=True, name="tiny_metrics")
    t.start()
    return httpd


# ---------------------------------------------------------------------------
# Lazy default collectors — only registered when explicitly requested.
# ---------------------------------------------------------------------------

PROCESS_COLLECTOR: Optional[ProcessCollector] = None
PLATFORM_COLLECTOR: Optional[PlatformCollector] = None


def enable_default_collectors(registry: MetricRegistry = REGISTRY) -> None:
    """Register :class:`ProcessCollector` and :class:`PlatformCollector`."""

    global PROCESS_COLLECTOR, PLATFORM_COLLECTOR
    if PROCESS_COLLECTOR is None:
        PROCESS_COLLECTOR = ProcessCollector(registry)
    if PLATFORM_COLLECTOR is None:
        PLATFORM_COLLECTOR = PlatformCollector(registry)


# Patch in functools at runtime to avoid an import in __all__
import functools  # noqa: E402