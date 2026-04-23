# CLAUDE.md — tektii-gateway-sdk-python

## What This Repo Is

Python SDK for the [Tektii Trading Gateway](https://github.com/Tektii/trading-gateway). Wraps the gateway's REST + WebSocket API with typed Pydantic models, sync/async clients, and auto-ACK for backtesting.

This is an **open-source client library** — it does not contain any backend logic. The gateway itself lives at `../tektii-gateway/`.

## Task Tracking

Linear is the source of truth for tasks/bugs/features. See [docs/LINEAR.md](./docs/LINEAR.md) for agent conventions (required labels, lifecycle rules, MCP tool cheat sheet). Tickets for this repo use the `area/gateway` label with a `[py-sdk]` prefix in the title.

## Architecture

```
src/tektii_gateway/
├── client.py            # TektiiGateway (sync) — thin wrapper, calls asyncio.run per method
├── async_client.py      # AsyncTektiiGateway (async) — real implementation, uses httpx
├── _http.py             # URL building, auth headers, response→error dispatch
├── models.py            # Re-exports generated models + hand-written WebSocket events
├── _generated/models.py # Pydantic v2 models generated from OpenAPI spec (DO NOT HAND-EDIT)
├── stream.py            # AsyncEventStream + SyncEventStream (WebSocket with auto-ACK)
└── errors.py            # TektiiError → TektiiAPIError → subclasses (NotFound, Rejected, etc.)
```

**Key design decisions:**
- Flat methods (`gw.submit_order()`) not sub-clients (`gw.orders.submit()`)
- Method kwargs for request parameters, Pydantic models for responses only
- Async is the real implementation; sync wraps via `asyncio.run()`
- WebSocket auto-ACK sends after user processes each event (yield semantics)

## Commands

```bash
uv sync                          # Install all deps (runtime + dev)
uv run pytest                    # Run tests (69 tests)
uv run pytest -v                 # Verbose test output
uv run ruff check src/ tests/    # Lint
uv run mypy src/                 # Type check (note: not --strict, generated code has issues)
./scripts/generate.sh            # Regenerate models from OpenAPI spec
./scripts/generate.sh --check    # Check if models are out of date (CI drift check)
```

## Model Generation

REST API models are generated from `../tektii-gateway/openapi.json` using `datamodel-code-generator`. Config is in `pyproject.toml` under `[tool.datamodel-codegen]`.

**Generated file**: `src/tektii_gateway/_generated/models.py` — DO NOT HAND-EDIT. Regenerate with `./scripts/generate.sh`.

**Hand-written models**: WebSocket event types live in `src/tektii_gateway/models.py` because the WebSocket protocol isn't in the OpenAPI spec. These reference the generated models (e.g., `CandleEvent.bar: Bar`).

## WebSocket / Auto-ACK

The auto-ACK mechanism in `stream.py` is the most nuanced piece:

1. Gateway sends events with optional `event_id` (present only from backtest engine)
2. With `auto_ack=True`, the SDK sends `event_ack` **after** the user's `async for` body runs
3. This works because `yield` in an async generator suspends until the consumer calls `__anext__` again
4. Against live/mock gateway, `event_id` is absent and no ACK is sent — same code, zero changes
5. Ping/pong heartbeats are handled internally and never yielded to the user

## Dependencies

**Runtime**: `httpx`, `websockets`, `pydantic` (v2)
**Dev**: `pytest`, `pytest-asyncio`, `respx`, `ruff`, `mypy`, `datamodel-code-generator`

## Testing Patterns

- **REST client tests** (`test_async_client.py`, `test_client.py`): Use `respx` to mock httpx transport
- **Model tests** (`test_models.py`): Round-trip JSON parsing with sample gateway responses
- **Streaming tests** (`test_stream.py`): Spin up a real `websockets` server on localhost, send scripted events
- **Error tests** (`test_errors.py`): Verify status code → exception class mapping

All streaming tests use `reconnect=False` to avoid hanging on server close.

## Conventions

- Line length: 100 (ruff + black)
- Python target: 3.11+
- All financial values are `str` (not `Decimal` or `float`) on the wire — matching the gateway's Rust `rust_decimal` serialization
- Enums are `StrEnum` for JSON compat
- No `unsafe` code, no `# type: ignore` outside generated files

## Relationship to Other Repos

| Repo | Relationship |
|------|-------------|
| `tektii-gateway/` | The API this SDK wraps. `openapi.json` is the source of truth for models. |
| `tektii-be/` | The backtest engine implements the same protocol as the gateway. Auto-ACK is for this. |
| `tektii-ui/` | No direct relationship. |
| `tektii-infra/` | Python conventions (ruff, mypy config) were initially drawn from here. |
