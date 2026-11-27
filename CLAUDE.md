# CLAUDE.md — tektii-gateway-sdk-python

## What This Repo Is

Python SDK for the [Trading Gateway](https://github.com/Tektii/trading-gateway). Wraps the gateway's REST + WebSocket API with typed Pydantic models and sync/async clients. Internal auto-ACK lets the same strategy code run against live brokers and the Tektii backtest engine without changes.

This is an **open-source client library** — it does not contain any backend logic. The gateway is a separate open-source project: https://github.com/Tektii/trading-gateway

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

## Specs and Change Management (OpenSpec)

Work is specced with [OpenSpec](https://github.com/Fission-AI/OpenSpec) in `openspec/`, committed in full. One change = one feature or gap.

- `openspec/specs/` — living specs, the source of truth for current behaviour
- `openspec/changes/` — in-flight proposals (proposal, design, delta specs, tasks); archived to `openspec/changes/archive/`
- `openspec/config.yaml` — project context and the rules for each artifact, apply and archive

Run `openspec init --tools claude` once to generate the local `/opsx:*` commands and skills (git-ignored).

1. **Explore** (optional) — `/opsx:explore` to investigate before committing to a change. The outcome is a proposal, or nothing.
2. **Propose** — `/opsx:propose "<idea>"` writes `proposal.md`, delta specs, `design.md` (when warranted) and `tasks.md`. Reviewed before any code.
3. **Apply** — `/opsx:apply` works the tasks **test-first** on a branch (never `main`): failing test, then minimum code to pass. One commit per task, files staged by name.
4. **Quality gates** — what CI runs: `uv run ruff check src/ tests/`, `uv run ruff format --check src/ tests/`, `uv run mypy src/`, `uv run pytest --cov --cov-fail-under=85`; plus `./scripts/generate.sh --check` when models are involved.
5. **Ship** — push the branch, `gh pr create`, then work Copilot's review through the `pr-review` skill (`.claude/skills/pr-review/`) and stop. The change's OpenSpec artifacts ship in the same pull request as its code. The PR title must be a lowercase [Conventional Commit](https://www.conventionalcommits.org/) — pull requests are squash-merged and release-please derives the version bump and `CHANGELOG.md` from it, so never hand-edit `CHANGELOG.md` or `src/tektii/_version.py`. Never merge a pull request and never enable auto-merge; the maintainer merges.
6. **Verify** — `/opsx:verify` checks the merged code against the change's specs and tasks. Always run `/opsx:verify` before archiving; never fold it into apply.
7. **Archive** — `/opsx:archive`, only after the pull request is merged, merges the delta specs into `openspec/specs/` and moves the change to `changes/archive/`.

`.worktreeinclude` lists the git-ignored files (`.claude/settings.local.json`, the generated `/opsx:*` commands and skills) copied into a new `claude --worktree` checkout; add to it when a session needs another.

`openspec list` shows in-flight changes; `openspec validate --all` checks them.

**This repo is public.** Everything under `openspec/` must stand on its own — no private ticket ids, internal links, or commercial context. The public Python API is the contract: a change that alters it says so.

## Model Generation

REST API models are generated from the Trading Gateway's OpenAPI spec using `datamodel-code-generator`. Config is in `pyproject.toml` under `[tool.datamodel-codegen]`.

`./scripts/generate.sh` fetches the spec from the public [trading-gateway](https://github.com/Tektii/trading-gateway) repo (`main` branch by default). No vendored `openapi.json` lives in this repo — overrides:

- `GATEWAY_REF=<branch|tag|sha>` — pin the fetch to a specific ref.
- `OPENAPI_SPEC=<path-or-url>` — use a local file (e.g. a checked-out copy of the gateway's `openapi.json`) or arbitrary URL.

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

## Unparseable frames

A frame that fails JSON parsing or validation against the `GatewayEvent`
union is **not** dropped. It is logged at `error` and yielded as an
`UnknownEvent` (`models.py`) carrying `raw_payload`, `error`, and any
`event_id` recovered from the raw text — so it flows through the normal
yield/ACK path like any other event. A silent skip was indistinguishable
from "the event never fired" for a strategy author with no logging
configured. Tolerant iteration is preserved: one bad frame never kills the
stream. Note this means a user's bare `case _` now sees these.

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

## Relationship to the Gateway

The [Trading Gateway](https://github.com/Tektii/trading-gateway) is the API this SDK wraps; its `openapi.json` (fetched from GitHub at generate time) is the source of truth for the REST models. The Tektii backtest engine implements the same wire protocol as the gateway — the hidden auto-ACK exists so the same strategy code runs unchanged against both a live gateway and the backtest engine.
