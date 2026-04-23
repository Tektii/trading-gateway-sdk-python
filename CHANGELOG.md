# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-04-09

First public release.

### Added

- `AsyncTektiiGateway` and `TektiiGateway` clients covering the full Tektii
  Trading Gateway REST API: account, orders (submit, modify, cancel, list,
  history), positions (list, get, close), market data (quotes, bars),
  trades, capabilities, status, health, and circuit breakers.
- `AsyncEventStream` and `SyncEventStream` WebSocket clients with automatic
  reconnection (exponential backoff + jitter, cap applied after jitter,
  bounded attempts, short-circuit on 401/403 handshake failures),
  ping/pong heartbeats, resource caps on frame size and idle timeout,
  tolerant parsing of malformed JSON and unknown event types, and
  `auto_ack=True` support for the Tektii backtest engine.
- Typed exception hierarchy:
  - `TektiiError` — base.
  - `TektiiConnectionError` — wraps `httpx` transport errors (timeout,
    connect refused, pool exhaustion) so users never need to `import httpx`.
  - `TektiiProtocolError` — non-JSON responses, malformed JSON, and
    oversized bodies (Content-Length above the SDK safety cap).
  - `TektiiAPIError` + subclasses for 400, 401, 404, 409, 422, 429, 500, 503.
- Retry policy for idempotent requests (`GET`, `DELETE`, `HEAD`) on
  transport failures and `502/503/504`. `429` honours `Retry-After`. `POST`
  is never retried to avoid duplicate order submission. Configurable via
  `max_retries` (default `2`, `0` disables).
- Connection pooling in the sync client: `TektiiGateway` runs a single
  background event loop + shared `AsyncTektiiGateway`, so polling
  strategies no longer pay a TLS handshake per call. The sync client
  refuses to run from inside an existing event loop with a clear error.
- `TEKTII_API_KEY` and `TEKTII_GATEWAY_URL` environment variable fallbacks.
- Default `User-Agent: tektii-gateway-python/<version> httpx/<version>`
  with a `headers=` kwarg for adding/overriding request headers.
- `timeout` accepts `float` or `httpx.Timeout` for granular connect /
  read / write / pool control.
- Plaintext HTTP with an API key to a non-local host is now a hard
  `ValueError` with an `allow_insecure=True` escape hatch — previously a
  silent warning that could be filtered.
- Filter datetimes (`since`, `until`, `start`, `end`) must be
  timezone-aware; naive datetimes raise `ValueError` rather than silently
  being sent without an offset.
- Exception messages strip query strings from request URLs to avoid
  leaking sensitive query parameters (e.g. `client_order_id` carrying a
  customer identifier) into user logs and observability stacks.
- Pydantic v2 models generated from the gateway's OpenAPI spec. WebSocket
  event models are hand-written with `ConfigDict(extra="ignore")` pinned
  for forward-compatibility.
- PEP 561 `py.typed` marker — downstream type checkers pick up SDK types.
- API key redaction in `repr()`.
- Full test suite (153 tests) covering REST, WebSocket reconnect, auto-ACK
  timing, URL escaping, sync/async parity, retries, connection error
  wrapping, oversized responses, protocol errors, header merging,
  environment-variable fallback, sync-in-event-loop guard, and
  reconnect-backoff cap.
- GitHub Actions CI (pytest + ruff + mypy + coverage floor of 85% on
  Python 3.11/3.12/3.13), OpenAPI drift check, sdist contents audit,
  weekly `pip-audit`, and release workflow using PyPI Trusted Publishers
  with PEP 740 attestations. All third-party actions pinned to commit
  SHAs.
- Dependabot for `pip` and `github-actions` ecosystems.

[Unreleased]: https://github.com/Tektii/tektii-gateway-sdk-python/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Tektii/tektii-gateway-sdk-python/releases/tag/v0.1.0
