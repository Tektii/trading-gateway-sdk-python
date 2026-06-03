# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.6.0] — 2026-06-03

### Added

- `quantity_for_notional()` on both `TradingGateway` and
  `AsyncTradingGateway` — sizes an order by a target notional or a fraction
  of account equity instead of a hand-picked instrument amount. An order
  `quantity` is a fixed instrument amount, not a share of capital, so a
  default like `0.01` BTC is a near-zero position on a six-figure account and
  produces a flat, meaningless backtest. Pass `notional="5000"` for $5,000 of
  exposure or `equity_fraction=0.10` for 10% of equity; the helper resolves
  the quote midpoint `(bid + ask) / 2` (override with `price=` to skip the
  quote fetch) and returns a `Decimal` ready for `submit_order()`.

## [1.5.1] — 2026-04-29

### Fixed

- WebSocket auto-ACK now fires for **every** yielded event, not only those
  carrying an `event_id`. The Tektii backtest gateway strips `event_id`
  from its wire-format `WsMessage` and relies on auto-correlation (any
  strategy ACK drains every engine event already broadcast), so the
  previous "ACK only when `event_id` is present" rule produced zero ACKs
  through the gateway sidecar and stalled backtests after the engine's
  consecutive-ACK-timeout threshold. Frames sent to live and mock backends
  are still no-ops because those backends do not register an ACK bridge.
  ([TEK-309](https://linear.app/tektii/issue/TEK-309))

### Changed

- Auto-ACK is now always on and no longer part of the public API. Event
  acknowledgements fire automatically after the user's iterator body runs
  for every yielded event. Same strategy code runs unchanged against any
  backend.

### Removed

- `auto_ack=` constructor keyword on `TradingGateway`,
  `AsyncTradingGateway`, `AsyncEventStream`, and `SyncEventStream`.
- Public `AsyncEventStream.ack()` / `SyncEventStream.ack()` methods. ACK
  coordination is fully internal now.

## [1.5.0] — 2026-04-23

First public release. Published to PyPI as `tektii`. Version starts at 1.5.0
because the name was previously held by an unrelated package on PyPI; versioning
above its highest release avoids distribution-filename collisions.

### Added

- `AsyncTradingGateway` and `TradingGateway` clients covering the full Trading
  Gateway REST API: account, orders (submit, modify, cancel, list,
  history), positions (list, get, close), market data (quotes, bars),
  trades, capabilities, status, health, and circuit breakers.
- `AsyncEventStream` and `SyncEventStream` WebSocket clients with automatic
  reconnection (exponential backoff + jitter, cap applied after jitter,
  bounded attempts, short-circuit on 401/403 handshake failures),
  ping/pong heartbeats, resource caps on frame size and idle timeout,
  tolerant parsing of malformed JSON and unknown event types, and
  auto-ACK support for the Tektii backtest engine.
- Typed exception hierarchy:
  - `TektiiError` — base.
  - `APIConnectionError` — wraps `httpx` transport errors (timeout,
    connect refused, pool exhaustion) so users never need to `import httpx`.
  - `APIProtocolError` — non-JSON responses, malformed JSON, and
    oversized bodies (Content-Length above the SDK safety cap).
  - `APIStatusError` + subclasses for 400, 401, 404, 409, 422, 429, 500, 503.
- Retry policy for idempotent requests (`GET`, `DELETE`, `HEAD`) on
  transport failures and `502/503/504`. `429` honours `Retry-After`. `POST`
  is never retried to avoid duplicate order submission. Configurable via
  `max_retries` (default `2`, `0` disables).
- Connection pooling in the sync client: `TradingGateway` runs a single
  background event loop + shared `AsyncTradingGateway`, so polling
  strategies no longer pay a TLS handshake per call. The sync client
  refuses to run from inside an existing event loop with a clear error.
- `TRADING_GATEWAY_API_KEY` and `TRADING_GATEWAY_URL` environment variable fallbacks.
- Default `User-Agent: tektii-python/<version> httpx/<version>`
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

[Unreleased]: https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.5.1...HEAD
[1.5.1]: https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/Tektii/trading-gateway-sdk-python/releases/tag/v1.5.0
