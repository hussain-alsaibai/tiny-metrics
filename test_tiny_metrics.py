"""Tests for tiny_metrics.

Run with: ``python3 -m unittest test_tiny_metrics``.
"""

import os
import sys
import time
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tiny_metrics as tm
from tiny_metrics import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    MetricRegistry,
    REGISTRY,
    generate_latest,
    start_http_server,
    ProcessCollector,
    PlatformCollector,
    _Collector,
    _Sample,
)


def fresh_registry() -> MetricRegistry:
    """Return a clean MetricRegistry (unconnected to the global one)."""

    return MetricRegistry()


class TestCounter(unittest.TestCase):
    def test_basic_inc(self):
        reg = fresh_registry()
        c = Counter("req_total", "Requests", registry=reg)
        c.inc()
        c.inc(2)
        self.assertEqual(len(c._samples), 1)
        self.assertEqual(c._samples[0].value, 3.0)

    def test_labels(self):
        reg = fresh_registry()
        c = Counter("req_total", "Requests", labelnames=("method", "code"), registry=reg)
        c.inc(method="GET", code="200")
        c.inc(method="GET", code="200")
        c.inc(method="POST", code="500")
        names = sorted(s.labels["method"] for s in c._samples)
        self.assertEqual(names, ["GET", "POST"])

    def test_negative_inc_rejected(self):
        reg = fresh_registry()
        c = Counter("neg_inc_total", "neg", registry=reg)
        with self.assertRaises(ValueError):
            c.inc(-1)

    def test_unknown_label_rejected(self):
        reg = fresh_registry()
        c = Counter("unknown_lbl", "x", labelnames=("a",), registry=reg)
        with self.assertRaises(ValueError):
            c.inc(b="1")

    def test_missing_label_rejected(self):
        reg = fresh_registry()
        c = Counter("missing_lbl", "x", labelnames=("a",), registry=reg)
        with self.assertRaises(ValueError):
            c.inc()


class TestGauge(unittest.TestCase):
    def test_inc_dec_set(self):
        reg = fresh_registry()
        g = Gauge("inflight_q1", "In-flight", registry=reg)
        g.inc()
        g.inc(5)
        g.dec(2)
        self.assertEqual(g._samples[0].value, 4.0)
        g.set(10)
        self.assertEqual(g._samples[0].value, 10.0)

    def test_negative_inc_allowed(self):
        reg = fresh_registry()
        g = Gauge("neg_inc_q1", "g", registry=reg)
        g.inc(-5)
        self.assertEqual(g._samples[0].value, -5.0)

    def test_labels(self):
        reg = fresh_registry()
        g = Gauge("temp_q1", "Temperature", labelnames=("room",), registry=reg)
        g.set(20, room="kitchen")
        g.set(18, room="office")
        rooms = sorted(s.labels["room"] for s in g._samples)
        self.assertEqual(rooms, ["kitchen", "office"])


class TestHistogram(unittest.TestCase):
    def test_basic_observe(self):
        reg = fresh_registry()
        h = Histogram("lat_q1", "Latency", registry=reg)
        for v in (0.01, 0.05, 0.5, 5.0):
            h.observe(v)
        bucket_samples = [s for s in h._samples if s.name.endswith("_bucket")]
        count = next(s for s in h._samples if s.name.endswith("_count"))
        sum_ = next(s for s in h._samples if s.name.endswith("_sum"))
        self.assertTrue(any(s.labels.get("le") == "+Inf" for s in bucket_samples))
        self.assertEqual(count.value, 4)
        self.assertAlmostEqual(sum_.value, 0.01 + 0.05 + 0.5 + 5.0, places=6)

    def test_cumulative_buckets(self):
        reg = fresh_registry()
        h = Histogram("lat_q1c", "Latency", buckets=(1.0, 5.0), registry=reg)
        h.observe(0.5)
        h.observe(3.0)
        h.observe(7.0)
        out = {s.labels["le"]: s.value for s in h._samples if s.name.endswith("_bucket")}
        # Prometheus convention: integer-valued buckets render without trailing ".0"
        self.assertEqual(out["1"], 1)
        self.assertEqual(out["5"], 2)
        self.assertEqual(out["+Inf"], 3)

    def test_time_decorator(self):
        reg = fresh_registry()
        h = Histogram("d_q1", "d", registry=reg)

        @h.time()
        def slow():
            time.sleep(0.01)
            return 42

        self.assertEqual(slow(), 42)
        count = next(s for s in h._samples if s.name.endswith("_count"))
        self.assertEqual(count.value, 1)

    def test_time_context_manager(self):
        reg = fresh_registry()
        h = Histogram("d_q1c", "d", registry=reg)
        with h.time():
            time.sleep(0.001)
        count = next(s for s in h._samples if s.name.endswith("_count"))
        self.assertEqual(count.value, 1)

    def test_labels(self):
        reg = fresh_registry()
        h = Histogram("lat_q1l", "lat", labelnames=("path",), registry=reg)
        h.observe(0.1, path="/a")
        h.observe(1.0, path="/a")
        h.observe(0.05, path="/b")
        bucket_a = [s for s in h._samples if s.name.endswith("_bucket") and s.labels.get("path") == "/a"]
        bucket_b = [s for s in h._samples if s.name.endswith("_bucket") and s.labels.get("path") == "/b"]
        self.assertEqual(len(bucket_a), len(h._buckets) + 1)
        self.assertEqual(len(bucket_b), len(h._buckets) + 1)

    def test_invalid_bucket_rejected(self):
        with self.assertRaises(ValueError):
            Histogram("h_q1bad", "h", buckets=(-1.0, 1.0))


class TestSummary(unittest.TestCase):
    def test_basic_observe(self):
        reg = fresh_registry()
        s = Summary("rt_q1", "rt", registry=reg)
        for v in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0):
            s.observe(v)
        count = next(x for x in s._samples if x.name.endswith("_count"))
        sum_ = next(x for x in s._samples if x.name.endswith("_sum"))
        self.assertEqual(count.value, 10)
        self.assertAlmostEqual(sum_.value, 5.5, places=6)
        q50 = next(x for x in s._samples if x.labels.get("quantile") == "0.5")
        self.assertGreaterEqual(q50.value, 0.4)
        self.assertLessEqual(q50.value, 0.6)


class TestRegistry(unittest.TestCase):
    def test_register_unregister(self):
        reg = fresh_registry()
        c = Counter("foo_r", "foo", registry=reg)
        self.assertIn("foo_r", reg._metrics)
        reg.unregister(c)
        self.assertNotIn("foo_r", reg._metrics)

    def test_duplicate_register_rejected(self):
        reg = fresh_registry()
        Counter("foo_dup", "foo", registry=reg)
        with self.assertRaises(ValueError):
            Counter("foo_dup", "foo", registry=reg)


class TestExposition(unittest.TestCase):
    def test_format(self):
        reg = fresh_registry()
        c = Counter("hits_fmt", "Hit count", registry=reg)
        c.inc(3)
        body = generate_latest(reg).decode()
        self.assertIn("# HELP hits_fmt Hit count", body)
        self.assertIn("# TYPE hits_fmt counter", body)
        self.assertIn("hits_fmt_total 3", body)
        self.assertIn("# EOF", body)

    def test_label_value_escaping(self):
        reg = fresh_registry()
        c = Counter("weird_fmt", "weird", labelnames=("k",), registry=reg)
        c.inc(k='val"with\nstuff')
        body = generate_latest(reg).decode()
        # The label value should be escaped: " -> \", \n -> \\n in the output
        self.assertIn(r'val\"with\nstuff', body)

    def test_histogram_with_labels_exposition(self):
        reg = fresh_registry()
        h = Histogram("lat_fmt", "lat", labelnames=("p",), buckets=(0.1, 1.0), registry=reg)
        h.observe(0.05, p="/")
        body = generate_latest(reg).decode()
        # Sorted labels means p comes before le? Actually format labels key-sorted:
        # we render keys sorted alphabetically, so le comes before p.
        self.assertIn('lat_fmt_bucket{le="0.1",p="/"} 1', body)
        self.assertIn('lat_fmt_bucket{le="+Inf",p="/"} 1', body)
        self.assertIn('lat_fmt_count{p="/"} 1', body)

    def test_gauge_set(self):
        reg = fresh_registry()
        g = Gauge("temp_fmt", "Temperature", registry=reg)
        g.set(20.5)
        body = generate_latest(reg).decode()
        self.assertIn("# TYPE temp_fmt gauge", body)
        self.assertIn("temp_fmt 20.5", body)

    def test_nan_and_inf(self):
        reg = fresh_registry()
        g = Gauge("nan_fmt", "NaN test", registry=reg)
        g.set(float("nan"))
        g.set(float("inf"))
        body = generate_latest(reg).decode()
        self.assertIn("NaN", body)
        self.assertIn("+Inf", body)


class TestHTTP(unittest.TestCase):
    def test_serve_metrics(self):
        reg = fresh_registry()
        c = Counter("hits_http", "hits", registry=reg)
        c.inc(7)
        server = start_http_server(port=0, registry=reg)
        try:
            host, port = server.server_address
            from http.client import HTTPConnection

            conn = HTTPConnection(host, port, timeout=2)
            conn.request("GET", "/metrics")
            resp = conn.getresponse()
            body = resp.read().decode()
            self.assertEqual(resp.status, 200)
            self.assertIn("hits_http_total 7", body)
            self.assertIn("text/plain", resp.getheader("Content-Type"))
            conn.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_serve_root(self):
        reg = fresh_registry()
        server = start_http_server(port=0, registry=reg)
        try:
            host, port = server.server_address
            from http.client import HTTPConnection

            conn = HTTPConnection(host, port, timeout=2)
            conn.request("GET", "/")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            self.assertIn(b"tiny_metrics", resp.read())
            conn.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_serve_404(self):
        reg = fresh_registry()
        server = start_http_server(port=0, registry=reg)
        try:
            host, port = server.server_address
            from http.client import HTTPConnection

            conn = HTTPConnection(host, port, timeout=2)
            conn.request("GET", "/nope")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 404)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()


class TestCollectors(unittest.TestCase):
    def test_process_collector_runs(self):
        reg = fresh_registry()
        ProcessCollector(registry=reg)
        body = generate_latest(reg).decode()
        self.assertIn("process_cpu_seconds_total", body)
        self.assertIn("process_resident_memory_bytes", body)

    def test_platform_collector(self):
        reg = fresh_registry()
        PlatformCollector(registry=reg)
        body = generate_latest(reg).decode()
        self.assertIn("python_info", body)
        self.assertIn("version=", body)

    def test_custom_collector(self):
        reg = fresh_registry()

        class StubCollector(_Collector):
            def collect(self):
                return [("stub", "gauge", "stub", [_Sample("stub_value", {}, 99.0)])]

        reg.register_collector(StubCollector())
        body = generate_latest(reg).decode()
        self.assertIn("stub_value 99", body)


class TestValidation(unittest.TestCase):
    def test_invalid_metric_name(self):
        reg = fresh_registry()
        with self.assertRaises(ValueError):
            Counter("1bad_v", "x", registry=reg)
        with self.assertRaises(ValueError):
            Counter("has space_v", "x", registry=reg)

    def test_invalid_label_name(self):
        reg = fresh_registry()
        with self.assertRaises(ValueError):
            Counter("x_v", "x", labelnames=("1bad",), registry=reg)


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_inc(self):
        reg = fresh_registry()
        c = Counter("hits_thr", "hits", registry=reg)
        n = 1000

        def worker():
            for _ in range(n):
                c.inc()

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(c._samples[0].value, 8 * n)


class TestSummaryStreaming(unittest.TestCase):
    def test_quantile(self):
        reg = fresh_registry()
        s = Summary("rt_strm", "rt", registry=reg)
        for v in range(1, 101):
            s.observe(v)
        q99 = next(x for x in s._samples if x.labels.get("quantile") == "0.99")
        self.assertGreaterEqual(q99.value, 95.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)