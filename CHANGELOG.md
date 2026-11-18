# Changelog

All notable changes to tiny-metrics are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.1] - 2026-11-18

### Fixed

- **`HEAD /metrics` answered `501 Unsupported method`.** `MetricsHandler`
  defined only `do_GET`, so everything else fell through to
  `BaseHTTPRequestHandler`'s 501. Prometheus itself scrapes with GET, so the
  test suite — which also only ever sent GET — never saw it. But HEAD is what
  uptime checkers, service meshes sidecars and load-balancer health probes
  send, and to all of them a 501 on a healthy `/metrics` reads as "this
  service is broken". Measured before the fix:

  ```
  HEAD /metrics -> HTTP/1.0 501 Unsupported method ('HEAD')
  ```

  `do_GET` and `do_HEAD` now share one `_route(write_body=...)` path, so the
  two can no longer drift apart: HEAD returns exactly the headers GET would
  and writes zero body bytes, including the `Content-Length` GET would have
  declared.
- **The 404 response had no `Content-Length`.** It wrote `b"not found"` with
  only a `Content-Type`. Under HTTP/1.0 the connection closes on finish, so
  the body was framed by EOF and nothing broke locally. A proxy or client
  that speaks HTTP/1.1 to this server keeps the connection alive and then has
  two ways to misread it: wait forever for a body boundary, or take the next
  response's status line as body bytes. The 404 now declares its length.

### Added

- Four tests in `TestHTTP`: `HEAD /metrics` returns 200 with a positive
  `Content-Length` and an empty body; `HEAD /` likewise; `GET /nope` declares
  `Content-Length: 9`; `HEAD /nope` declares it and sends nothing. Verified
  against the pre-fix module: 4 of 36 fail (`AssertionError: 501 != 200`).

### Notes

- No API change. `start_http_server()` and every public name are untouched.
- Still zero dependencies: the handler uses `BaseHTTPRequestHandler.do_HEAD`
  and nothing outside the stdlib.
