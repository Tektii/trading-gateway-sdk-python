# Contributing to trading-gateway-sdk-python

Thanks for your interest in contributing! This SDK is open source under the
MIT licence and we welcome bug reports, feature requests, and pull requests.

## Development setup

You'll need [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/Tektii/trading-gateway-sdk-python.git
cd trading-gateway-sdk-python
uv sync
```

This installs all runtime and development dependencies into a local `.venv`.

## Running the quality bar

Before opening a pull request, make sure all checks pass locally:

```bash
uv run ruff check src/ tests/       # lint
uv run ruff format --check src/ tests/  # formatting
uv run mypy src/                    # type check
uv run pytest -q                    # tests
uv run pytest --cov                 # tests with coverage
```

CI runs the same checks on Python 3.11, 3.12, and 3.13.

## Regenerating models from the OpenAPI spec

REST models live in `src/tektii/_generated/models.py` and are
generated from the Trading Gateway's OpenAPI spec. **Do not hand-edit them.**

`./scripts/generate.sh` fetches the spec directly from the public
[trading-gateway](https://github.com/Tektii/trading-gateway) repo (`main`
branch by default). Nothing is vendored in this repo.

To pick up gateway changes:

1. Run `./scripts/generate.sh` to regenerate the models.
2. Run `./scripts/generate.sh --check` to verify no drift remains.
3. Commit the regenerated `src/tektii/_generated/models.py`.

Overrides:

- `GATEWAY_REF=<branch|tag|sha> ./scripts/generate.sh` — pin to a specific ref.
- `OPENAPI_SPEC=<path-or-url> ./scripts/generate.sh` — use a local file or
  alternative URL (useful when developing against `../tektii-gateway/openapi.json`).

CI runs `./scripts/generate.sh --check` on every PR against `main` of the
gateway to catch drift.

## Code style

- **Line length**: 100
- **Formatting**: `ruff format`
- **Linting**: `ruff check` with rules `E, W, F, I, UP, B, C4, PL, SIM`
- **Typing**: `mypy` with `disallow_untyped_defs=true`
- **Python**: 3.11+, using `from __future__ import annotations`
- **No `unsafe` code or `# type: ignore`** outside the generated models
- **All financial values are `str` on the wire** — we never use `float` for
  prices, quantities, or P&L to preserve Rust `rust_decimal` precision.

## Testing

- REST client tests: use `respx` to mock `httpx` transport. See
  `tests/test_async_client.py` for patterns.
- Stream tests: spin up a real `websockets` server on localhost. See
  `tests/test_stream.py`.
- All new public API surface must come with tests before merge.
- Regression tests for bug fixes should fail against the old code and pass
  against the fix.

## Pull request checklist

- [ ] Branch is up to date with `main`.
- [ ] `ruff check`, `ruff format --check`, `mypy`, and `pytest` all pass.
- [ ] New behaviour has tests.
- [ ] `CHANGELOG.md` has an entry under `## [Unreleased]` describing the
      change (user-facing wording, not internal detail).
- [ ] If you regenerated models, the regenerated
      `src/tektii/_generated/models.py` is committed.
- [ ] Public API additions are exported from `src/tektii/__init__.py`
      and added to `__all__`.

## Reporting bugs

Open an issue at
<https://github.com/Tektii/trading-gateway-sdk-python/issues> with:

- SDK version (`python -c "import tektii; print(tektii.__version__)"`).
- Python version.
- Gateway version.
- A minimal reproduction.
- Full traceback if applicable.

For security vulnerabilities, see [SECURITY.md](SECURITY.md) instead — do
not file public issues for security bugs.

## Code of conduct

Be kind. Assume good intent. Focus feedback on the code, not the person.

## Licence

By contributing, you agree that your contributions will be licensed under
the [MIT Licence](LICENSE).
