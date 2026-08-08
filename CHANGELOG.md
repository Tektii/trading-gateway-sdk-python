# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0](https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.9.1...v2.0.0) (2026-08-08)


### ⚠ BREAKING CHANGES

* close_position() no longer accepts cancel_associated_orders. Remove the argument from your calls; there is no replacement, as the gateway always cancels the associated orders.

### Features

* drop cancel_associated_orders from close_position ([#37](https://github.com/Tektii/trading-gateway-sdk-python/issues/37)) ([9ee61cd](https://github.com/Tektii/trading-gateway-sdk-python/commit/9ee61cd891e2ef0947fd119a13f9a0cd05e379e4))
* **orders:** reject order types the provider doesn't support ([#35](https://github.com/Tektii/trading-gateway-sdk-python/issues/35)) ([e55b8b3](https://github.com/Tektii/trading-gateway-sdk-python/commit/e55b8b3a41529a36f6e7acd054e5e2f6578dd81d))
* **positions:** add modify_position_exits for moving a resting SL/TP ([#33](https://github.com/Tektii/trading-gateway-sdk-python/issues/33)) ([246932a](https://github.com/Tektii/trading-gateway-sdk-python/commit/246932a37873e2a9c56eba67c0c1840d133c8f92))
* **stream:** surface unparseable frames as UnknownEvent ([#36](https://github.com/Tektii/trading-gateway-sdk-python/issues/36)) ([79fc6f9](https://github.com/Tektii/trading-gateway-sdk-python/commit/79fc6f9ecc5daac540c126314fe5afe1c4ce90c9))

## [1.9.1](https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.9.0...v1.9.1) (2026-07-13)


### Bug Fixes

* **stream:** echo event_id in terminal and dropped-frame acks ([#25](https://github.com/Tektii/trading-gateway-sdk-python/issues/25)) ([442a822](https://github.com/Tektii/trading-gateway-sdk-python/commit/442a8224509f094ab12341a2ba10dd42893274da))

## [1.9.0](https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.8.0...v1.9.0) (2026-06-11)


### Features

* **client:** read TEKTII_-prefixed gateway env vars with legacy fallback ([#18](https://github.com/Tektii/trading-gateway-sdk-python/issues/18)) ([1dd7f6f](https://github.com/Tektii/trading-gateway-sdk-python/commit/1dd7f6f278ce33cbf02b4206f12f1f6f99301d2c))

## [1.8.0](https://github.com/Tektii/trading-gateway-sdk-python/compare/v1.7.0...v1.8.0) (2026-06-05)


### Features

* surface financing WebSocket events ([#14](https://github.com/Tektii/trading-gateway-sdk-python/issues/14)) ([b5306d2](https://github.com/Tektii/trading-gateway-sdk-python/commit/b5306d22d87d97c4972105655393d057bb0c4761))

## [Unreleased]

### Fixed

- The end-of-backtest flush-ack now echoes the terminal event's `event_id`
  when present, instead of always sending an empty ack. The gateway's ACK
  contract releases exactly the ids an ack names, so an empty ack for an
  id-bearing terminal previously left the engine waiting out its teardown
  ack-timeout on every run.
- A WebSocket frame dropped for malformed JSON or an unrecognised event type
  is now acked when a top-level `event_id` can be recovered from it. In
  backtest mode, every engine-paced frame must be acked or the engine stalls
  toward its consecutive-timeout halt; this turns a schema mismatch into a
  logged skip instead of a run-killing stall.

### Changed

- Docstrings describing the ACK contract no longer advertise "empty ack ⇒
  gateway auto-correlation drains everything". The gateway's contract is
  strict: an `event_ack` releases exactly the `event_id`s named in
  `events_processed`, and an empty list is a no-op, not a wildcard. A custom
  client relying on the old behaviour to pace engine-driven events will
  stall its backtests under the new contract.

## [1.7.0] — 2026-06-03

### Added

- The event stream now recognises a clean **end-of-backtest** terminal
  (`backtest_complete`) and ends the run loop gracefully — no error, no
  reconnect attempt, no spurious disconnect log. Previously the close that
  follows a finished backtest surfaced as a disconnect, so the stream waited
  out a timeout or tried to reconnect and logged an error on every clean run.
- Optional `on_backtest_complete=` hook on `gw.stream(...)` (both
  `TradingGateway` and `AsyncTradingGateway`), fired exactly once with the
  `BacktestCompleteEvent` just before the loop returns; absence is a no-op.
  The async client accepts a plain or a coroutine function; the sync client
  runs the hook on the iterating thread. The terminal carries `broker` and
  `timestamp` — call `get_account()` from inside the hook for final equity.
  Useful for strategy teardown.
- On terminal receipt the SDK now sends a flush-ACK immediately, letting the
  backtest engine release its teardown without waiting out its ack-timeout
  (a latency optimisation; the engine is correct either way).

### Fixed

- `TradingGateway.stream()` (sync) no longer stalls for the full
  `close_timeout` and logs a spurious shutdown error on close. This affected
  both a stream that ended on its own (a clean backtest end, or any server
  close with `reconnect=False`) and breaking out of the loop early: shutdown
  used to block on a close future that the background loop could abandon as it
  stopped. Close now unwinds the loop without blocking on that future.

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
