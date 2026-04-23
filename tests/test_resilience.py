"""Tests for error wrapping, retries, protocol errors, and misc guards.

These exercise behaviours that are easy to get wrong and whose breakage
would be hard to notice in CI without explicit coverage: httpx error
wrapping, the retry policy for idempotent calls, naive-datetime rejection,
plaintext-HTTP hard-fail, the sync-in-event-loop guard, and the protocol
error path for responses the SDK refuses to parse.
"""

from __future__ import annotations

import json
import random
from datetime import datetime

import httpx
import pytest
import respx
import websockets.asyncio.server
import websockets.exceptions

from tektii import stream as stream_mod
from tektii.async_client import _STATUS_TOO_MANY_REQUESTS, AsyncTradingGateway
from tektii.client import TradingGateway
from tektii.errors import (
    TektiiAPIError,
    TektiiConnectionError,
    TektiiError,
    TektiiProtocolError,
)
from tektii.stream import AsyncEventStream, SyncEventStream

from .conftest import SAMPLE_ACCOUNT

# ---------------------------------------------------------------------------
# Transport error wrapping — httpx errors become TektiiConnectionError
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_timeout_wrapped_to_connection_error(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(side_effect=httpx.ReadTimeout("simulated"))
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(TektiiConnectionError) as exc_info:
            await gw.get_account()
    assert isinstance(exc_info.value.__cause__, httpx.ReadTimeout)


@respx.mock(base_url="http://localhost:8080")
async def test_connect_error_wrapped_to_connection_error(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/v1/account").mock(side_effect=httpx.ConnectError("refused"))
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(TektiiConnectionError):
            await gw.get_account()


# ---------------------------------------------------------------------------
# Retry policy — idempotent only, never POST, honours 429 Retry-After
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_get_retries_on_transient_502(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account")
    route.side_effect = [
        httpx.Response(502, json={"code": "BAD_GATEWAY", "message": "upstream bad"}),
        httpx.Response(200, json=SAMPLE_ACCOUNT),
    ]
    async with AsyncTradingGateway(max_retries=2) as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"
    assert route.call_count == 2


@respx.mock(base_url="http://localhost:8080")
async def test_get_retries_on_timeout_then_succeeds(
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get("/v1/account")
    route.side_effect = [
        httpx.ReadTimeout("first"),
        httpx.Response(200, json=SAMPLE_ACCOUNT),
    ]
    async with AsyncTradingGateway(max_retries=2) as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"
    assert route.call_count == 2


@respx.mock(base_url="http://localhost:8080")
async def test_post_order_never_retries(respx_mock: respx.MockRouter) -> None:
    """Retrying POST /v1/orders could duplicate a fill. Ban it unconditionally."""
    route = respx_mock.post("/v1/orders")
    route.side_effect = [httpx.Response(503, json={"code": "UNAVAILABLE", "message": "x"})]
    async with AsyncTradingGateway(max_retries=5) as gw:
        with pytest.raises(TektiiAPIError):
            await gw.submit_order("AAPL", "buy", "1")
    # POST must have been attempted exactly once.
    assert route.call_count == 1


@respx.mock(base_url="http://localhost:8080")
async def test_retry_honours_retry_after_seconds(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account")
    route.side_effect = [
        httpx.Response(
            _STATUS_TOO_MANY_REQUESTS,
            headers={"Retry-After": "0"},
            json={"code": "RATE_LIMITED", "message": "slow down"},
        ),
        httpx.Response(200, json=SAMPLE_ACCOUNT),
    ]
    async with AsyncTradingGateway(max_retries=2) as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"
    assert route.call_count == 2


@respx.mock(base_url="http://localhost:8080")
async def test_retries_disabled_raises_on_first_failure(
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get("/v1/account")
    route.side_effect = [httpx.Response(503, json={"code": "x", "message": "y"})]
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(TektiiAPIError):
            await gw.get_account()
    assert route.call_count == 1


# ---------------------------------------------------------------------------
# Protocol errors — non-JSON, malformed JSON, oversized body
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_non_json_success_raises_protocol_error(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, text="<html/>", headers={"content-type": "text/html"})
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(TektiiProtocolError) as exc_info:
            await gw.get_account()
    # Protocol errors must carry method + path, and must NOT leak query string.
    assert exc_info.value.method == "GET"
    assert exc_info.value.path == "/v1/account"


@respx.mock(base_url="http://localhost:8080")
async def test_malformed_json_raises_protocol_error(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(
            200, content=b"{bad", headers={"content-type": "application/json"}
        )
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(TektiiProtocolError):
            await gw.get_account()


@respx.mock(base_url="http://localhost:8080")
async def test_oversized_content_length_raises_protocol_error(
    respx_mock: respx.MockRouter,
) -> None:
    """Content-Length > SDK cap should fail fast before parsing."""
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(
            200,
            json=SAMPLE_ACCOUNT,
            headers={"content-length": str(100 * 1024 * 1024)},  # 100 MiB advertised
        )
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(TektiiProtocolError, match="exceeds cap"):
            await gw.get_account()


@respx.mock(base_url="http://localhost:8080")
async def test_content_type_is_case_insensitive(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(
            200, json=SAMPLE_ACCOUNT, headers={"content-type": "Application/JSON; charset=utf-8"}
        )
    )
    async with AsyncTradingGateway() as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"


# ---------------------------------------------------------------------------
# Query string is NOT leaked into exception messages (HIGH-1 in the review)
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_query_string_not_in_protocol_error_message(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/v1/orders").mock(
        return_value=httpx.Response(200, text="<html/>", headers={"content-type": "text/html"})
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(TektiiProtocolError) as exc_info:
            await gw.list_orders(client_order_id="very-sensitive-customer-uuid-42")
    assert "very-sensitive-customer-uuid-42" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# User-Agent default
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_default_user_agent_is_set(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTradingGateway() as gw:
        await gw.get_account()
    ua = route.calls[0].request.headers.get("user-agent", "")
    assert ua.startswith("tektii-python/")


@respx.mock(base_url="http://localhost:8080")
async def test_headers_kwarg_merges_with_defaults(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTradingGateway(headers={"X-Tenant": "acme"}) as gw:
        await gw.get_account()
    headers = route.calls[0].request.headers
    assert headers.get("x-tenant") == "acme"
    assert "tektii-python/" in headers.get("user-agent", "")


# ---------------------------------------------------------------------------
# Env-var fallbacks
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_api_key_env_var_fallback(
    respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRADING_GATEWAY_API_KEY", "tk_env_value")
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTradingGateway() as gw:
        await gw.get_account()
    assert route.calls[0].request.headers.get("authorization") == "Bearer tk_env_value"


async def test_base_url_env_var_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADING_GATEWAY_URL", "https://gw.env.example.com")
    gw = AsyncTradingGateway()
    try:
        assert gw.base_url == "https://gw.env.example.com"
    finally:
        await gw.close()


# ---------------------------------------------------------------------------
# Naive datetime rejection
# ---------------------------------------------------------------------------


async def test_naive_datetime_rejected() -> None:
    gw = AsyncTradingGateway()
    try:
        with pytest.raises(ValueError, match="timezone-aware"):
            await gw.list_orders(since=datetime(2025, 1, 1))  # noqa: DTZ001
    finally:
        await gw.close()


# ---------------------------------------------------------------------------
# list_orders(status=[]) empty list — pin current behaviour
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders_empty_status_list_sends_no_filter(
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[]))
    async with AsyncTradingGateway() as gw:
        await gw.list_orders(status=[])
    params = dict(route.calls[0].request.url.params)
    # Empty list → status param should be omitted entirely, not sent as "".
    assert "status" not in params


# ---------------------------------------------------------------------------
# Sync-in-event-loop detection (the #1 Jupyter foot-gun)
# ---------------------------------------------------------------------------


async def test_sync_client_inside_running_loop_raises() -> None:
    """TradingGateway().get_account() from inside a running event loop must
    raise a TektiiError with an actionable message — *not* a raw
    ``RuntimeError: asyncio.run() ...``.
    """
    gw = TradingGateway()
    with pytest.raises(TektiiError, match="running event loop"):
        gw.get_account()


# ---------------------------------------------------------------------------
# SyncEventStream.ack outside of `with` block raises
# ---------------------------------------------------------------------------


def test_sync_stream_ack_outside_with_raises() -> None:
    stream = SyncEventStream(ws_url="ws://localhost:8080/v1/ws")
    with pytest.raises(TektiiError, match="outside of a 'with stream:' block"):
        stream.ack(["evt_1"])


# ---------------------------------------------------------------------------
# Malformed JSON and unknown event type over WebSocket — tolerated, not fatal
# ---------------------------------------------------------------------------


async def test_malformed_ws_frame_is_skipped() -> None:
    """A single malformed JSON frame must not kill the iterator.

    The gateway protocol is versioned; a bad frame from a mis-wired upstream
    should be logged and skipped rather than bubbling up as a JSONDecodeError.
    """

    async def handler(ws):
        await ws.send(b"{not-valid-json")
        # Then send a valid one so the iterator definitely receives something.
        await ws.send(
            json.dumps(
                {
                    "type": "error",
                    "code": "INTERNAL_ERROR",
                    "message": "real event",
                    "timestamp": "2025-01-15T10:30:00Z",
                }
            )
        )
        await ws.close()

    server = await websockets.asyncio.server.serve(handler, "127.0.0.1", 0)
    host, port = next(iter(server.sockets)).getsockname()[:2]
    url = f"ws://{host}:{port}"
    try:
        events = []
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1
    assert events[0].code == "INTERNAL_ERROR"


async def test_unknown_ws_event_type_is_skipped() -> None:
    """Unknown event ``type`` should be dropped with a warning, not escape
    the iterator as a Pydantic ``ValidationError``.
    """

    async def handler(ws):
        await ws.send(
            json.dumps({"type": "brand_new_event_type", "timestamp": "2025-01-15T10:30:00Z"})
        )
        await ws.send(
            json.dumps(
                {
                    "type": "error",
                    "code": "INTERNAL_ERROR",
                    "message": "known event",
                    "timestamp": "2025-01-15T10:30:00Z",
                }
            )
        )
        await ws.close()

    server = await websockets.asyncio.server.serve(handler, "127.0.0.1", 0)
    host, port = next(iter(server.sockets)).getsockname()[:2]
    url = f"ws://{host}:{port}"
    try:
        events = []
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1


# ---------------------------------------------------------------------------
# Reconnect backoff cap — jitter must never push delay over the documented max
# ---------------------------------------------------------------------------


async def test_reconnect_delay_respects_max(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: jitter was previously applied after ``min(...)`` so a
    ``max_reconnect_delay=30`` could sleep up to 45s. Now the cap is
    applied AFTER jitter.

    We drive the reconnect loop without a real WebSocket by making
    ``_connect`` immediately raise ``ConnectionClosed``, and we capture
    the recorded delays by patching ``asyncio.sleep`` on the stream module.
    """
    # Pin jitter to the upper bound so the computation is deterministic.
    monkeypatch.setattr(random, "random", lambda: 1.0)

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(stream_mod.asyncio, "sleep", fake_sleep)

    stream = AsyncEventStream(
        ws_url="ws://localhost:1/v1/ws",
        reconnect=True,
        max_reconnect_delay=1.0,
        max_reconnect_attempts=3,
    )

    async def failing_connect() -> None:
        raise websockets.exceptions.ConnectionClosedError(None, None)

    stream._connect = failing_connect  # type: ignore[method-assign]

    with pytest.raises(websockets.exceptions.ConnectionClosedError):
        async for _event in stream:
            pass

    # Every recorded sleep must respect the cap — no jitter spillover.
    assert sleeps, "expected at least one reconnect sleep"
    assert all(s <= 1.0 for s in sleeps), f"delay exceeded cap: {sleeps}"
    # 3 attempts + initial = exactly 3 retries before exhausting.
    assert len(sleeps) == 3
