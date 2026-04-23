# Tektii Gateway Python SDK

[![PyPI](https://img.shields.io/pypi/v/tektii-gateway.svg)](https://pypi.org/project/tektii-gateway/)
[![Python versions](https://img.shields.io/pypi/pyversions/tektii-gateway.svg)](https://pypi.org/project/tektii-gateway/)
[![CI](https://github.com/Tektii/tektii-gateway-sdk-python/actions/workflows/ci.yml/badge.svg)](https://github.com/Tektii/tektii-gateway-sdk-python/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Python SDK for the [Tektii Trading Gateway](https://github.com/Tektii/trading-gateway) — a unified REST + WebSocket trading proxy that connects your strategies to any broker.

Write your strategy once, run it against Alpaca, Binance, Oanda, Saxo, or the Tektii backtesting engine — zero code changes.

> **Status**: v0.1.0 — Beta

## Risk disclaimer

This SDK connects real strategies to real brokers. **Bugs in your strategy, in
this SDK, or in the upstream gateway can cause financial loss.** Validate
against the mock provider or the Tektii backtest engine before going live, and
do not commit credentials to version control. Distributed under the MIT
licence — **no warranty, see [LICENSE](LICENSE)**.

## Install

```bash
pip install tektii-gateway
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add tektii-gateway
```

## Prerequisites

A running [Tektii Trading Gateway](https://github.com/Tektii/trading-gateway). The fastest way to start:

```bash
docker run -e GATEWAY_PROVIDER=mock -p 8080:8080 ghcr.io/tektii/gateway:latest
```

This starts the gateway with a mock provider that simulates AAPL price movements — perfect for testing.

## Quickstart

```python
from tektii_gateway import TektiiGateway

gw = TektiiGateway()  # connects to localhost:8080
account = gw.get_account()
print(f"Balance: {account.balance} {account.currency}")

order = gw.submit_order("AAPL", "buy", "1")
print(f"Order {order.id}: {order.status}")
```

### Async

```python
import asyncio
from tektii_gateway import AsyncTektiiGateway

async def main():
    async with AsyncTektiiGateway() as gw:
        account = await gw.get_account()
        print(f"Balance: {account.balance} {account.currency}")

        order = await gw.submit_order("AAPL", "buy", "1", "limit", limit_price="185.00")
        print(f"Order {order.id}: {order.status}")

asyncio.run(main())
```

## Example strategies

End-to-end reference strategies that use this SDK live in the gateway repository under
[`examples/python/`](https://github.com/Tektii/trading-gateway/tree/main/examples/python):

- [`ma_crossover/`](https://github.com/Tektii/trading-gateway/tree/main/examples/python/ma_crossover) — moving-average crossover with bracket orders and a small state machine.
- [`rsi_momentum/`](https://github.com/Tektii/trading-gateway/tree/main/examples/python/rsi_momentum) — RSI-based momentum strategy.

Each example is a self-contained strategy with its own tests and README, runnable against the mock provider, a live broker, or the Tektii backtest engine.

## Credentials

The SDK never hardcodes credentials. Pass the API key via constructor, or via
environment variables:

| Variable | Purpose | Default |
|---|---|---|
| `TEKTII_API_KEY` | Bearer token sent in the `Authorization` header | _unset_ |
| `TEKTII_GATEWAY_URL` | Gateway base URL | `http://localhost:8080` |

```python
# Constructor (highest precedence)
gw = TektiiGateway(api_key="tk_live_…", base_url="https://gw.example.com")

# Environment (fallback)
import os
os.environ["TEKTII_API_KEY"] = "tk_live_…"
gw = TektiiGateway()  # picks up TEKTII_API_KEY automatically
```

**Never hardcode a production API key in committed source.** Use your
secrets manager, environment variables, or a local `.env` that is
git-ignored.

The SDK **refuses to send an API key over plain `http://`** to a non-local
host, raising `ValueError` at construction. Use `https://` for remote
gateways, or pass `allow_insecure=True` after auditing the network path
(e.g. a private VPN).

## Streaming Events

Real-time market data and trading events via WebSocket:

```python
import asyncio
from tektii_gateway import AsyncTektiiGateway, CandleEvent, OrderEvent

async def main():
    async with AsyncTektiiGateway() as gw:
        async with gw.stream() as events:
            async for event in events:
                match event:
                    case CandleEvent(bar=bar):
                        print(f"[{bar.symbol}] Close: {bar.close}")
                    case OrderEvent(order=order):
                        print(f"Order {order.id}: {order.status}")

asyncio.run(main())
```

The stream auto-reconnects with exponential backoff + jitter (capped at
`max_reconnect_delay`), short-circuits on 401/403 handshake failures, and
handles ping/pong heartbeats internally. Malformed frames and unknown
event types are logged and skipped — they never kill the iterator.

## Backtesting with Auto-ACK

When running against the Tektii backtesting engine, set `auto_ack=True` to automatically acknowledge events. This controls simulation time progression — the engine waits for your strategy to process each event before advancing.

```python
from tektii_gateway import AsyncTektiiGateway, CandleEvent
from decimal import Decimal
import asyncio

async def main():
    async with AsyncTektiiGateway(
        base_url="http://localhost:8080",  # Engine URL
        auto_ack=True,                      # Auto-ACK for backtest
    ) as gw:
        async with gw.stream() as events:
            async for event in events:
                match event:
                    case CandleEvent(bar=bar) if bar.close < Decimal("150"):
                        await gw.submit_order(bar.symbol, "buy", "1")

asyncio.run(main())
```

The same strategy code works against the live gateway (where auto-ACK is a harmless no-op) and the backtest engine — zero code changes.

## Bracket Orders (Stop-Loss / Take-Profit)

Submit orders with built-in exit management:

```python
order = gw.submit_order(
    "AAPL", "buy", "100", "limit",
    limit_price="185.00",
    stop_loss="180.00",      # Exit if price drops to $180
    take_profit="195.00",    # Exit if price rises to $195
)
```

The gateway manages SL/TP orders across all brokers with a unified interface — even those that don't natively support bracket orders.

## API Reference

### Client Construction

```python
from tektii_gateway import TektiiGateway, AsyncTektiiGateway

# Sync (notebooks, simple scripts, production too)
gw = TektiiGateway(
    base_url="http://localhost:8080",  # or $TEKTII_GATEWAY_URL
    api_key=None,                       # or $TEKTII_API_KEY
    timeout=30.0,                       # float or httpx.Timeout
    auto_ack=False,                     # auto-ACK for backtesting
    headers={"X-Tenant": "acme"},       # extra headers (optional)
    max_retries=2,                      # retry idempotent requests on transient failure
    allow_insecure=False,               # opt-in to plain http:// with API key
)

# Async (high-throughput strategies, async frameworks)
gw = AsyncTektiiGateway(...)

# Both support context managers; using `with` / `async with` ensures the
# HTTP connection pool is cleaned up on exit.
async with AsyncTektiiGateway() as gw:
    ...

with TektiiGateway() as gw:
    ...
```

The sync client shares a single background event loop + HTTP connection
pool across all calls, so a polling strategy does not pay a TLS handshake
per request. It **cannot be called from inside an existing event loop**
(e.g., inside a Jupyter cell with `%autoawait` or a FastAPI startup hook) —
use `AsyncTektiiGateway` there instead.

### Orders

| Method | Description |
|--------|-------------|
| `submit_order(symbol, side, quantity, ...)` | Submit a new order |
| `get_order(order_id)` | Get order details |
| `list_orders(*, symbol=, status=)` | List open orders |
| `get_order_history(*, symbol=, status=)` | Get filled/cancelled orders |
| `modify_order(order_id, *, quantity=, ...)` | Modify an existing order |
| `cancel_order(order_id)` | Cancel a specific order |
| `cancel_all_orders(*, symbol=)` | Cancel all orders |

### Positions

| Method | Description |
|--------|-------------|
| `list_positions(*, symbol=)` | List open positions |
| `get_position(position_id)` | Get position details |
| `close_position(position_id, ...)` | Close a position |
| `close_all_positions(*, symbol=)` | Close all positions |

### Market Data

| Method | Description |
|--------|-------------|
| `get_quote(symbol)` | Current bid/ask/last |
| `get_bars(symbol, timeframe, ...)` | Historical OHLCV bars |

### Account & System

| Method | Description |
|--------|-------------|
| `get_account()` | Balance, equity, margin |
| `list_trades(*, symbol=, order_id=)` | Trade history |
| `get_capabilities()` | Supported order types, assets |
| `get_status()` | Broker connection status |
| `get_health()` | Per-provider health details |
| `get_circuit_breakers()` | Exit management circuit breaker status |

### WebSocket Streaming

| Method | Description |
|--------|-------------|
| `stream()` | Build an event stream (async or sync context manager + iterator) |
| `stream.ack(event_ids)` | Manually ACK events (when `auto_ack=False`) |

**Event types**: `CandleEvent`, `QuoteEvent`, `OrderEvent`, `PositionEvent`, `AccountEvent`, `TradeEvent`, `ConnectionEvent`, `DataStalenessEvent`, `RateLimitEvent`, `ErrorEvent`

All events can be pattern-matched with Python's `match`/`case` syntax.

## Error Handling

```python
from tektii_gateway import (
    TektiiGateway,
    TektiiAPIError,
    TektiiConnectionError,
    OrderRejectedError,
    NotFoundError,
)

gw = TektiiGateway()
try:
    gw.submit_order("AAPL", "buy", "999999")
except OrderRejectedError as e:
    # Structured reject reason from the gateway, e.g. "INSUFFICIENT_MARGIN".
    print(f"Rejected: {e.message} ({e.code})")
except NotFoundError as e:
    print(f"Not found: {e.message}")
except TektiiConnectionError as e:
    # Network / timeout / DNS failure — the original httpx exception
    # is preserved on e.__cause__ for debugging.
    print(f"Transport failure: {e}")
except TektiiAPIError as e:
    print(f"API error [{e.status_code}]: {e.code} - {e.message}")
```

**Exception hierarchy**:

- `TektiiError` — base for all SDK errors
  - `TektiiConnectionError` — network/timeout/DNS failure. Wraps the
    underlying `httpx` exception (available on `__cause__`) so user code
    never needs to `import httpx`.
  - `TektiiProtocolError` — gateway returned something the SDK cannot parse
    (unexpected content type, malformed JSON, oversized body). Carries
    `status_code`, `method`, `path`.
  - `TektiiAPIError` — gateway returned a structured error response (has
    `status_code`, `code`, `message`, `details`)
    - `BadRequestError` (400)
    - `AuthenticationError` (401)
    - `NotFoundError` (404)
    - `ConflictError` (409)
    - `OrderRejectedError` (422) — includes `details.reject_reason`
    - `RateLimitedError` (429)
    - `ServerError` (500)
    - `ProviderUnavailableError` (503)

`TektiiAPIError.details` carries the gateway's structured error envelope.
When you ship provider adapters that echo upstream broker payloads, treat
the field as potentially sensitive — do not blindly forward it into
third-party log sinks.

### Retries

Idempotent requests (`GET`, `DELETE`, `HEAD`) are retried automatically on:

- `httpx.TimeoutException`, `httpx.TransportError`, `httpx.NetworkError`
- `502 Bad Gateway`, `503 Service Unavailable`, `504 Gateway Timeout`
- `429 Too Many Requests` — honours the `Retry-After` header if present

`POST` and `PATCH` are **never** retried automatically — retrying
`submit_order` on a network blip could duplicate fills. Pass
`max_retries=0` to the client constructor to disable all retries.

## Quantities and Prices

All financial values are strings to avoid floating-point precision issues. You can pass `str` or `Decimal`:

```python
from decimal import Decimal

# Both work — strings are recommended
gw.submit_order("BTC/USD", "buy", "0.001", "limit", limit_price="50000.00")
gw.submit_order("BTC/USD", "buy", Decimal("0.001"), "limit", limit_price=Decimal("50000.00"))
```

Response model fields (`order.quantity`, `position.unrealized_pnl`, etc.) are strings, matching the gateway's wire format.

Filter datetimes (`since`, `until`, `start`, `end`) **must be
timezone-aware** — the SDK rejects naive datetimes with a `ValueError`
rather than silently misinterpreting their offset:

```python
from datetime import UTC, datetime

orders = gw.list_orders(since=datetime(2025, 1, 1, tzinfo=UTC))
```

## Requirements

- Python 3.11+
- A running [Tektii Trading Gateway](https://github.com/Tektii/trading-gateway)

## Development

```bash
git clone https://github.com/Tektii/tektii-gateway-sdk-python
cd tektii-gateway-sdk-python
uv sync          # Install deps + create venv
uv run pytest    # Run tests
uv run ruff check src/ tests/  # Lint
```

### Regenerate models from OpenAPI spec

Response models are generated from the gateway's OpenAPI spec using `datamodel-code-generator`. To regenerate after the spec changes:

```bash
./scripts/generate.sh          # Regenerate models
./scripts/generate.sh --check  # Check for drift (useful in CI)
```

### Project structure

```
src/tektii_gateway/
├── __init__.py          # Public exports
├── client.py            # TektiiGateway (sync, persistent background loop)
├── async_client.py      # AsyncTektiiGateway (async, retries, env vars)
├── _http.py             # Shared HTTP utilities + response safety caps
├── models.py            # Re-exports + WebSocket event models
├── _generated/models.py # Generated from OpenAPI spec (DO NOT EDIT)
├── stream.py            # WebSocket streaming (async + sync)
└── errors.py            # Exception hierarchy
```

## License

MIT
