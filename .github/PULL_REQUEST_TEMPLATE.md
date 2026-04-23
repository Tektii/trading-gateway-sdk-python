<!--
Thanks for contributing to the Trading Gateway Python SDK.

Please do not use pull requests for security vulnerabilities —
see SECURITY.md for the private reporting process.
-->

## Summary

<!-- What does this PR do, and why? Keep it to a few bullet points. -->

-
-

## Test plan

<!-- How did you verify this works? Include commands reviewers can run locally. -->

- [ ] `uv run pytest`
- [ ] `uv run ruff check src/ tests/`
- [ ] `uv run ruff format --check src/ tests/`
- [ ] `uv run mypy src/`

## Checklist

- [ ] `CHANGELOG.md` updated under `## [Unreleased]` if the change is user-visible.
- [ ] New public API additions are exported from `src/tektii/__init__.py`.
- [ ] Tests cover any new behaviour (regression tests for bug fixes).
- [ ] If you regenerated models, the regenerated
      `src/tektii/_generated/models.py` is committed.
- [ ] No credentials / secrets in code, tests, or example output.
