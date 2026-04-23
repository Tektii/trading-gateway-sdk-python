# CLAUDE.md — tektii-gateway-sdk-python

## What This Repo Is

Python SDK for the [Trading Gateway](https://github.com/Tektii/trading-gateway). Wraps the gateway's REST + WebSocket API with typed Pydantic models and sync/async clients. Internal auto-ACK lets the same strategy code run against live brokers and the Tektii backtest engine without changes.

This is an **open-source client library** — it does not contain any backend logic. The gateway itself lives at `../tektii-gateway/`.

## Task Tracking

Linear is the source of truth for tasks/bugs/features. See [docs/LINEAR.md](./docs/LINEAR.md) for agent conventions (required labels, lifecycle rules, MCP tool cheat sheet). Tickets for this repo use the `area/gateway` label with a `[py-sdk]` prefix in the title.

## Architecture

```
src/tektii/
├── client.py            # TradingGateway (sync) — thin wrapper, calls asyncio.run per method
├── async_client.py      # AsyncTradingGateway (async) — real implementation, uses httpx
├── _http.py             # URL building, auth headers, response→error dispatch
├── models.py            # Re-exports generated models + hand-written WebSocket events
├── _generated/models.py # Pydantic v2 models generated from OpenAPI spec (DO NOT HAND-EDIT)
├── stream.py            # AsyncEventStream + SyncEventStream (WebSocket with auto-ACK)
└── errors.py            # TektiiError → APIStatusError → subclasses (NotFound, Rejected, etc.)
```

**Key design decisions:**
- Flat methods (`gw.submit_order()`) not sub-clients (`gw.orders.submit()`)
- Method kwargs for request parameters, Pydantic models for responses only
- Async is the real implementation; sync wraps via `asyncio.run()`
- WebSocket auto-ACK sends after user processes each event (yield semantics)

## Commands

```bash
uv sync                          # Install all deps (runtime + dev)
uv run pytest                    # Run tests (156 tests)
uv run pytest -v                 # Verbose test output
uv run ruff check src/ tests/    # Lint
uv run mypy src/                 # Type check (note: not --strict, generated code has issues)
./scripts/generate.sh            # Regenerate models from OpenAPI spec
./scripts/generate.sh --check    # Check if models are out of date (CI drift check)
```

## Model Generation

REST API models are generated from the Trading Gateway's OpenAPI spec using `datamodel-code-generator`. Config is in `pyproject.toml` under `[tool.datamodel-codegen]`.

`./scripts/generate.sh` fetches the spec from the public [trading-gateway](https://github.com/Tektii/trading-gateway) repo (`main` branch by default). No vendored `openapi.json` lives in this repo — overrides:

- `GATEWAY_REF=<branch|tag|sha>` — pin the fetch to a specific ref.
- `OPENAPI_SPEC=<path-or-url>` — use a local file (e.g. `../tektii-gateway/openapi.json`) or arbitrary URL.

**Generated file**: `src/tektii/_generated/models.py` — DO NOT HAND-EDIT. Regenerate with `./scripts/generate.sh`.

**Hand-written models**: WebSocket event types live in `src/tektii/models.py` because the WebSocket protocol isn't in the OpenAPI spec. These reference the generated models (e.g., `CandleEvent.bar: Bar`).

## WebSocket / Auto-ACK

Auto-ACK is always on and **hidden from users** — no public kwarg, no manual
`ack()` method. The value prop is "same code runs live and backtest"; users
shouldn't have to know the mechanism exists. If a PR re-introduces an
`auto_ack=` parameter or a public `ack()` method, reject it.

The mechanism in `stream.py`:

1. Gateway sends events with optional `event_id` (present only from backtest engine)
2. SDK sends `event_ack` **after** the user's `async for` body runs when `event_id` is present
3. This works because `yield` in an async generator suspends until the consumer calls `__anext__` again
4. Against live/mock gateway, `event_id` is absent and no ACK is sent
5. `SyncEventStream` flips the inner `AsyncEventStream`'s private `_ack_on_yield=False` and re-ACKs from the sync iterator thread — preserves the "ACK after user processes" guarantee across the thread boundary
6. Ping/pong heartbeats are handled internally and never yielded to the user

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
| `tektii-gateway/` | The API this SDK wraps. Its `openapi.json` (fetched from GitHub at generate time) is the source of truth for models. |
| `tektii-be/` | The backtest engine implements the same protocol as the gateway. Auto-ACK is for this. |
| `tektii-ui/` | No direct relationship. |
| `tektii-infra/` | Python conventions (ruff, mypy config) were initially drawn from here. |
