"""Tests for AsyncTektiiGateway — all HTTP calls mocked with respx."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx

from tektii_gateway._http import http_to_ws_url
from tektii_gateway.async_client import AsyncTektiiGateway
from tektii_gateway.errors import (
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    OrderRejectedError,
    ProviderUnavailableError,
    RateLimitedError,
    ServerError,
    TektiiAPIError,
    TektiiError,
)
from tektii_gateway.models import (
    Account,
    Bar,
    CancelAllResult,
    CancelOrderResult,
    Capabilities,
    CircuitBreakerStatusResponse,
    ConnectionStatus,
    DetailedHealthStatus,
    ModifyOrderResult,
    Order,
    OrderHandle,
    Position,
    Quote,
    Trade,
)

from .conftest import (
    SAMPLE_ACCOUNT,
    SAMPLE_BAR,
    SAMPLE_CAPABILITIES,
    SAMPLE_CIRCUIT_BREAKERS,
    SAMPLE_CONNECTION_STATUS,
    SAMPLE_HEALTH,
    SAMPLE_ORDER,
    SAMPLE_ORDER_HANDLE,
    SAMPLE_POSITION,
    SAMPLE_QUOTE,
    SAMPLE_TRADE,
)


@pytest.fixture
def mock_router():
    with respx.mock(base_url="http://localhost:8080") as router:
        yield router


@pytest.fixture
def gw():
    return AsyncTektiiGateway(base_url="http://localhost:8080")


# -----------------------------------------------------------------------
# Account
# -----------------------------------------------------------------------


def test_stream_url_builder_does_not_corrupt_hostname() -> None:
    """Regression for Tier 2.5: naive ``str.replace('http://', 'ws://')``
    would corrupt hostnames containing the literal ``http``. The helper
    must only rewrite the URL scheme.
    """
    assert http_to_ws_url("http://myhttpserver.local:8080") == "ws://myhttpserver.local:8080"
    assert http_to_ws_url("https://gw.example.com") == "wss://gw.example.com"
    assert http_to_ws_url("http://localhost:8080") == "ws://localhost:8080"
    assert http_to_ws_url("ws://already-ws") == "ws://already-ws"
    assert http_to_ws_url("wss://already-wss") == "wss://already-wss"


def test_stream_url_builder_rejects_unsupported_scheme() -> None:
    with pytest.raises(ValueError, match="Unsupported base URL scheme"):
        http_to_ws_url("ftp://example.com")


def test_repr_redacts_api_key() -> None:
    # localhost default avoids plaintext warning
    gw = AsyncTektiiGateway(api_key="tk_secret_value")
    representation = repr(gw)
    assert "tk_secret_value" not in representation
    assert "'***'" in representation


def test_repr_without_api_key() -> None:
    gw = AsyncTektiiGateway()
    representation = repr(gw)
    assert "api_key=None" in representation
    assert "'***'" not in representation


def test_plaintext_http_with_api_key_hard_fails() -> None:
    """Sending an API key over plain HTTP to a non-local host must be a
    hard error by default — a silent warning can be filtered away and a
    financial credential leak is not a "silent" consequence.
    """
    with pytest.raises(ValueError, match="plaintext HTTP"):
        AsyncTektiiGateway(base_url="http://remote.example.com", api_key="tk_xyz")


def test_plaintext_http_with_api_key_allowed_with_opt_in() -> None:
    """``allow_insecure=True`` is the documented escape hatch for test
    doubles and audited private networks.
    """
    # Must not raise.
    AsyncTektiiGateway(base_url="http://remote.example.com", api_key="tk_xyz", allow_insecure=True)


def test_localhost_http_with_api_key_does_not_raise() -> None:
    AsyncTektiiGateway(base_url="http://localhost:8080", api_key="tk_xyz")
    AsyncTektiiGateway(base_url="http://127.0.0.1:8080", api_key="tk_xyz")


def test_https_with_api_key_does_not_raise() -> None:
    AsyncTektiiGateway(base_url="https://gw.example.com", api_key="tk_xyz")


@respx.mock(base_url="http://localhost:8080")
async def test_get_account(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    async with AsyncTektiiGateway() as gw:
        account = await gw.get_account()
    assert isinstance(account, Account)
    assert account.balance == "10000.00"


# -----------------------------------------------------------------------
# Orders
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))
    async with AsyncTektiiGateway() as gw:
        handle = await gw.submit_order("AAPL", "buy", "10")
    assert isinstance(handle, OrderHandle)
    assert handle.id == "ord_123"


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_with_decimal_quantity(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.submit_order("BTC/USD", "buy", Decimal("0.001"), "limit", limit_price="50000")
    # Verify the body sent the quantity as string
    body = route.calls[0].request.content
    parsed = json.loads(body)
    assert parsed["quantity"] == "0.001"
    assert parsed["limit_price"] == "50000"


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_with_bracket(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.submit_order(
            "AAPL",
            "buy",
            "1",
            "limit",
            limit_price="185.00",
            stop_loss="180.00",
            take_profit="195.00",
        )
    body = json.loads(route.calls[0].request.content)
    assert body["stop_loss"] == "180.00"
    assert body["take_profit"] == "195.00"


@respx.mock(base_url="http://localhost:8080")
async def test_get_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/ord_123").mock(return_value=httpx.Response(200, json=SAMPLE_ORDER))
    async with AsyncTektiiGateway() as gw:
        order = await gw.get_order("ord_123")
    assert isinstance(order, Order)
    assert order.symbol == "AAPL"


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    async with AsyncTektiiGateway() as gw:
        orders = await gw.list_orders()
    assert len(orders) == 1
    assert orders[0].id == "ord_123"


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders_with_filters(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[]))
    async with AsyncTektiiGateway() as gw:
        await gw.list_orders(symbol="AAPL", status=["OPEN", "PENDING"])
    params = dict(route.calls[0].request.url.params)
    assert params["symbol"] == "AAPL"
    assert params["status"] == "OPEN,PENDING"


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders_serialises_datetimes(respx_mock: respx.MockRouter) -> None:
    """Regression: ``since``/``until`` datetimes must round-trip as ISO 8601
    in the query string. A Rust backend will reject malformed timestamps.
    """
    route = respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[]))
    since = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
    until = datetime(2025, 1, 15, 11, 0, 0, tzinfo=UTC)
    async with AsyncTektiiGateway() as gw:
        await gw.list_orders(since=since, until=until)
    params = dict(route.calls[0].request.url.params)
    assert params["since"].startswith("2025-01-15T10:30:00")
    assert params["until"].startswith("2025-01-15T11:00:00")


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_full_parameter_matrix(
    respx_mock: respx.MockRouter,
) -> None:
    """Verify that every optional kwarg lands in the JSON body with the
    correct serialisation (Decimals → strings, booleans as-is, None omitted).
    """
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.submit_order(
            "AAPL",
            "buy",
            Decimal("1.5"),
            "limit",
            time_in_force="GTC",
            limit_price=Decimal("180.00"),
            stop_loss="175.00",
            take_profit="190.00",
            trailing_distance="1.5",
            trailing_type="ABSOLUTE",
            client_order_id="my-client-id",
            oco_group_id="oco-1",
            position_id="pos-1",
            reduce_only=True,
            post_only=False,
            hidden=False,
            display_quantity="0.5",
            leverage="2",
            margin_mode="ISOLATED",
        )
    body = json.loads(route.calls[0].request.content)
    assert body == {
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "1.5",
        "order_type": "limit",
        "time_in_force": "GTC",
        "limit_price": "180.00",
        "stop_loss": "175.00",
        "take_profit": "190.00",
        "trailing_distance": "1.5",
        "trailing_type": "ABSOLUTE",
        "client_order_id": "my-client-id",
        "oco_group_id": "oco-1",
        "position_id": "pos-1",
        "reduce_only": True,
        "post_only": False,
        "hidden": False,
        "display_quantity": "0.5",
        "leverage": "2",
        "margin_mode": "ISOLATED",
    }


@respx.mock(base_url="http://localhost:8080")
async def test_get_order_history(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/history").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    async with AsyncTektiiGateway() as gw:
        orders = await gw.get_order_history()
    assert len(orders) == 1


@respx.mock(base_url="http://localhost:8080")
async def test_modify_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.patch("/v1/orders/ord_123").mock(
        return_value=httpx.Response(200, json={"order": SAMPLE_ORDER})
    )
    async with AsyncTektiiGateway() as gw:
        result = await gw.modify_order("ord_123", quantity="20")
    assert isinstance(result, ModifyOrderResult)


@respx.mock(base_url="http://localhost:8080")
async def test_cancel_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders/ord_123").mock(
        return_value=httpx.Response(200, json={"success": True, "order": SAMPLE_ORDER})
    )
    async with AsyncTektiiGateway() as gw:
        result = await gw.cancel_order("ord_123")
    assert isinstance(result, CancelOrderResult)
    assert result.success is True


@respx.mock(base_url="http://localhost:8080")
async def test_cancel_all_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders").mock(
        return_value=httpx.Response(200, json={"cancelled_count": 3, "failed_count": 0})
    )
    async with AsyncTektiiGateway() as gw:
        result = await gw.cancel_all_orders()
    assert isinstance(result, CancelAllResult)
    assert result.cancelled_count == 3


# -----------------------------------------------------------------------
# Positions
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_list_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions").mock(return_value=httpx.Response(200, json=[SAMPLE_POSITION]))
    async with AsyncTektiiGateway() as gw:
        positions = await gw.list_positions()
    assert len(positions) == 1
    assert isinstance(positions[0], Position)


@respx.mock(base_url="http://localhost:8080")
async def test_get_position(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_POSITION)
    )
    async with AsyncTektiiGateway() as gw:
        position = await gw.get_position("pos_001")
    assert position.symbol == "AAPL"


@respx.mock(base_url="http://localhost:8080")
async def test_close_position(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTektiiGateway() as gw:
        handle = await gw.close_position("pos_001")
    assert isinstance(handle, OrderHandle)
    # Full close with no options should send an empty body (or no body at all).
    body = route.calls.last.request.content
    assert body in (b"", b"null", b"{}")


@respx.mock(base_url="http://localhost:8080")
async def test_close_position_partial_sends_body(respx_mock: respx.MockRouter) -> None:
    """Regression: partial close kwargs must reach the gateway.

    Previously `close_position()` built a body dict but never passed it to
    `_delete()`, silently turning partial closes into full closes.
    """
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.close_position(
            "pos_001",
            quantity=Decimal("5"),
            order_type="limit",
            limit_price="185.50",
            cancel_associated_orders=True,
        )
    sent = json.loads(route.calls.last.request.content)
    assert sent == {
        "quantity": "5",
        "order_type": "limit",
        "limit_price": "185.50",
        "cancel_associated_orders": True,
    }


@respx.mock(base_url="http://localhost:8080")
async def test_close_all_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/positions").mock(
        return_value=httpx.Response(200, json=[SAMPLE_ORDER_HANDLE])
    )
    async with AsyncTektiiGateway() as gw:
        handles = await gw.close_all_positions()
    assert len(handles) == 1


# -----------------------------------------------------------------------
# Market Data
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_get_quote(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/quotes/AAPL").mock(return_value=httpx.Response(200, json=SAMPLE_QUOTE))
    async with AsyncTektiiGateway() as gw:
        quote = await gw.get_quote("AAPL")
    assert isinstance(quote, Quote)
    assert quote.bid == "185.10"


@respx.mock(base_url="http://localhost:8080")
async def test_get_quote_escapes_symbol_with_slash(
    respx_mock: respx.MockRouter,
) -> None:
    """Regression: symbols like BTC/USD must be percent-encoded in the path."""
    route = respx_mock.get("/v1/quotes/BTC%2FUSD").mock(
        return_value=httpx.Response(200, json=SAMPLE_QUOTE)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.get_quote("BTC/USD")
    assert route.called
    # The raw request path should contain the encoded slash, not a real one.
    assert "BTC%2FUSD" in str(route.calls.last.request.url)


@respx.mock(base_url="http://localhost:8080")
async def test_get_order_escapes_id(respx_mock: respx.MockRouter) -> None:
    """Regression: order IDs with URL-unsafe chars must be encoded."""
    route = respx_mock.get("/v1/orders/ord%2Fweird%23id").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.get_order("ord/weird#id")
    assert route.called


@respx.mock(base_url="http://localhost:8080")
async def test_get_bars(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/bars/AAPL").mock(return_value=httpx.Response(200, json=[SAMPLE_BAR]))
    async with AsyncTektiiGateway() as gw:
        bars = await gw.get_bars("AAPL", "1m")
    assert len(bars) == 1
    assert isinstance(bars[0], Bar)


# -----------------------------------------------------------------------
# Trades
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_list_trades(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/trades").mock(return_value=httpx.Response(200, json=[SAMPLE_TRADE]))
    async with AsyncTektiiGateway() as gw:
        trades = await gw.list_trades()
    assert len(trades) == 1
    assert isinstance(trades[0], Trade)


# -----------------------------------------------------------------------
# System
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_get_capabilities(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/capabilities").mock(
        return_value=httpx.Response(200, json=SAMPLE_CAPABILITIES)
    )
    async with AsyncTektiiGateway() as gw:
        caps = await gw.get_capabilities()
    assert isinstance(caps, Capabilities)


@respx.mock(base_url="http://localhost:8080")
async def test_get_status(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/status").mock(
        return_value=httpx.Response(200, json=SAMPLE_CONNECTION_STATUS)
    )
    async with AsyncTektiiGateway() as gw:
        status = await gw.get_status()
    assert isinstance(status, ConnectionStatus)
    assert status.connected is True


@respx.mock(base_url="http://localhost:8080")
async def test_get_health(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/health").mock(return_value=httpx.Response(200, json=SAMPLE_HEALTH))
    async with AsyncTektiiGateway() as gw:
        health = await gw.get_health()
    assert isinstance(health, DetailedHealthStatus)


@respx.mock(base_url="http://localhost:8080")
async def test_get_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/circuit-breakers").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    async with AsyncTektiiGateway() as gw:
        cb = await gw.get_circuit_breakers()
    assert isinstance(cb, CircuitBreakerStatusResponse)


@respx.mock(base_url="http://localhost:8080")
async def test_reset_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.post("/v1/circuit-breakers/reset").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    async with AsyncTektiiGateway() as gw:
        cb = await gw.reset_circuit_breakers()
    assert isinstance(cb, CircuitBreakerStatusResponse)


@respx.mock(base_url="http://localhost:8080")
async def test_reset_circuit_breakers_cooldown_raises(
    respx_mock: respx.MockRouter,
) -> None:
    """Regression: a 409 from the reset cooldown should surface as
    ``ConflictError``, not a generic API error.
    """
    respx_mock.post("/v1/circuit-breakers/reset").mock(
        return_value=httpx.Response(
            409,
            json={
                "code": "RESET_COOLDOWN",
                "message": "Must wait 5 minutes between resets",
            },
        )
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(ConflictError):
            await gw.reset_circuit_breakers()


# -----------------------------------------------------------------------
# Error handling
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_404_raises_not_found(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/nonexistent").mock(
        return_value=httpx.Response(
            404, json={"code": "ORDER_NOT_FOUND", "message": "Order not found"}
        )
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(NotFoundError) as exc_info:
            await gw.get_order("nonexistent")
    assert exc_info.value.status_code == 404
    assert exc_info.value.code == "ORDER_NOT_FOUND"


@respx.mock(base_url="http://localhost:8080")
async def test_422_raises_order_rejected(respx_mock: respx.MockRouter) -> None:
    respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(
            422,
            json={
                "code": "ORDER_REJECTED",
                "message": "Insufficient margin",
                "details": {"reject_reason": "INSUFFICIENT_MARGIN"},
            },
        )
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(OrderRejectedError) as exc_info:
            await gw.submit_order("AAPL", "buy", "10000")
    assert exc_info.value.details == {"reject_reason": "INSUFFICIENT_MARGIN"}


@pytest.mark.parametrize(
    ("status", "error_cls", "code"),
    [
        (400, BadRequestError, "INVALID_REQUEST"),
        (401, AuthenticationError, "UNAUTHORIZED"),
        (409, ConflictError, "ORDER_NOT_MODIFIABLE"),
        (429, RateLimitedError, "RATE_LIMITED"),
        (500, ServerError, "INTERNAL_ERROR"),
        (503, ProviderUnavailableError, "PROVIDER_UNAVAILABLE"),
    ],
)
@respx.mock(base_url="http://localhost:8080")
async def test_error_status_codes_map_to_subclasses(
    respx_mock: respx.MockRouter,
    status: int,
    error_cls: type[TektiiAPIError],
    code: str,
) -> None:
    """Each HTTP error status should dispatch to its own exception subclass."""
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(status, json={"code": code, "message": "err"})
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(error_cls) as exc_info:
            await gw.get_account()
    assert exc_info.value.status_code == status
    assert exc_info.value.code == code


@respx.mock(base_url="http://localhost:8080")
async def test_non_json_error_body_still_raises(respx_mock: respx.MockRouter) -> None:
    """A non-JSON error body should still raise an error with a reasonable
    fallback message rather than crashing the parser.
    """
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(
            503,
            text="<html>service unavailable</html>",
            headers={"content-type": "text/html"},
        )
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(ProviderUnavailableError) as exc_info:
            await gw.get_account()
    assert exc_info.value.status_code == 503
    # Fallback code when body can't be parsed
    assert exc_info.value.code == "UNKNOWN"


@respx.mock(base_url="http://localhost:8080")
async def test_success_with_non_json_content_type_raises(
    respx_mock: respx.MockRouter,
) -> None:
    """A 200 with a non-JSON content-type would previously return None and
    crash downstream model validation; now it raises a clear TektiiError.
    """
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, text="not json", headers={"content-type": "text/plain"})
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(TektiiError, match="Expected JSON response"):
            await gw.get_account()


@respx.mock(base_url="http://localhost:8080")
async def test_success_with_malformed_json_raises(
    respx_mock: respx.MockRouter,
) -> None:
    """A 200 with application/json but a broken body should raise a clear
    SDK error rather than crashing downstream Pydantic validation.
    """
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(
            200,
            content=b"{not valid json",
            headers={"content-type": "application/json"},
        )
    )
    async with AsyncTektiiGateway() as gw:
        with pytest.raises(TektiiError, match="malformed JSON"):
            await gw.get_account()


# -----------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_api_key_sent_as_bearer(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTektiiGateway(api_key="test-key-123") as gw:
        await gw.get_account()
    auth = route.calls[0].request.headers.get("authorization")
    assert auth == "Bearer test-key-123"


@respx.mock(base_url="http://localhost:8080")
async def test_no_auth_header_when_no_key(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTektiiGateway() as gw:
        await gw.get_account()
    assert "authorization" not in route.calls[0].request.headers


# -----------------------------------------------------------------------
# Context manager
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_context_manager(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    async with AsyncTektiiGateway() as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"
