"""Tests for AsyncTradingGateway — all HTTP calls mocked with respx."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx

from tektii._http import http_to_ws_url
from tektii.async_client import AsyncTradingGateway
from tektii.errors import (
    APIStatusError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    OrderRejectedError,
    PositionUnprotectedError,
    ProviderUnavailableError,
    RateLimitedError,
    ServerError,
    TektiiError,
    UnsupportedOrderTypeError,
)
from tektii.models import (
    Account,
    Bar,
    CancelAllResult,
    CancelOrderResult,
    Capabilities,
    CircuitBreakerStatusResponse,
    ConnectionStatus,
    DetailedHealthStatus,
    ModifyOrderResult,
    ModifyPositionExitsResult,
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
    SAMPLE_EXIT_MOVE,
    SAMPLE_HEALTH,
    SAMPLE_ORDER,
    SAMPLE_ORDER_HANDLE,
    SAMPLE_POSITION,
    SAMPLE_QUOTE,
    SAMPLE_TRADE,
    mock_capabilities,
)


@pytest.fixture
def mock_router():
    with respx.mock(base_url="http://localhost:8080") as router:
        yield router


@pytest.fixture
def gw():
    return AsyncTradingGateway(base_url="http://localhost:8080")


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
    gw = AsyncTradingGateway(api_key="tk_secret_value")
    representation = repr(gw)
    assert "tk_secret_value" not in representation
    assert "'***'" in representation


def test_repr_without_api_key() -> None:
    gw = AsyncTradingGateway()
    representation = repr(gw)
    assert "api_key=None" in representation
    assert "'***'" not in representation


def test_plaintext_http_with_api_key_hard_fails() -> None:
    """Sending an API key over plain HTTP to a non-local host must be a
    hard error by default — a silent warning can be filtered away and a
    financial credential leak is not a "silent" consequence.
    """
    with pytest.raises(ValueError, match="plaintext HTTP"):
        AsyncTradingGateway(base_url="http://remote.example.com", api_key="tk_xyz")


def test_plaintext_http_with_api_key_allowed_with_opt_in() -> None:
    """``allow_insecure=True`` is the documented escape hatch for test
    doubles and audited private networks.
    """
    # Must not raise.
    AsyncTradingGateway(base_url="http://remote.example.com", api_key="tk_xyz", allow_insecure=True)


def test_localhost_http_with_api_key_does_not_raise() -> None:
    AsyncTradingGateway(base_url="http://localhost:8080", api_key="tk_xyz")
    AsyncTradingGateway(base_url="http://127.0.0.1:8080", api_key="tk_xyz")


def test_https_with_api_key_does_not_raise() -> None:
    AsyncTradingGateway(base_url="https://gw.example.com", api_key="tk_xyz")


@respx.mock(base_url="http://localhost:8080")
async def test_get_account(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    async with AsyncTradingGateway() as gw:
        account = await gw.get_account()
    assert isinstance(account, Account)
    assert account.balance == "10000.00"


# -----------------------------------------------------------------------
# Orders
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order(respx_mock: respx.MockRouter) -> None:
    mock_capabilities(respx_mock)
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))
    async with AsyncTradingGateway() as gw:
        handle = await gw.submit_order("AAPL", "buy", "10")
    assert isinstance(handle, OrderHandle)
    assert handle.id == "ord_123"


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_with_decimal_quantity(respx_mock: respx.MockRouter) -> None:
    mock_capabilities(respx_mock)
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.submit_order("BTC/USD", "buy", Decimal("0.001"), "limit", limit_price="50000")
    # Verify the body sent the quantity as string
    body = route.calls[0].request.content
    parsed = json.loads(body)
    assert parsed["quantity"] == "0.001"
    assert parsed["limit_price"] == "50000"


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_with_bracket(respx_mock: respx.MockRouter) -> None:
    mock_capabilities(respx_mock)
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
        order = await gw.get_order("ord_123")
    assert isinstance(order, Order)
    assert order.symbol == "AAPL"


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    async with AsyncTradingGateway() as gw:
        orders = await gw.list_orders()
    assert len(orders) == 1
    assert orders[0].id == "ord_123"


@respx.mock(base_url="http://localhost:8080")
async def test_list_orders_with_filters(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[]))
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    mock_capabilities(respx_mock)
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
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


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_submit_order_rejects_type_the_provider_does_not_support(
    respx_mock: respx.MockRouter,
) -> None:
    """A type the enum allows but the provider rejects must fail client-side.

    ``TRAILING_STOP`` is a valid ``OrderType``, so a strategy type-checks
    against it and then fails deep inside the engine. Catch it before the
    order leaves the SDK.
    """
    mock_capabilities(respx_mock, order_types=["MARKET", "LIMIT", "STOP"])
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(UnsupportedOrderTypeError) as exc_info:
            await gw.submit_order("AAPL", "buy", "10", "trailing_stop")

    message = str(exc_info.value)
    assert "trailing_stop" in message.lower()
    assert "MARKET" in message and "LIMIT" in message and "STOP" in message
    assert not route.called, "order must not be sent when the type is unsupported"


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_allows_type_the_provider_does_support(
    respx_mock: respx.MockRouter,
) -> None:
    """Validation follows the provider, not a hardcoded list."""
    mock_capabilities(respx_mock, order_types=["MARKET", "TRAILING_STOP"])
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.submit_order("AAPL", "buy", "10", "trailing_stop", trailing_distance="1.5")
    assert route.called


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_submit_order_rejects_unknown_order_type(respx_mock: respx.MockRouter) -> None:
    """A typo'd type is caught here too, rather than as an opaque 400."""
    mock_capabilities(respx_mock)
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(UnsupportedOrderTypeError):
            await gw.submit_order("AAPL", "buy", "10", "markte")
    assert not route.called


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_matches_order_type_case_insensitively(
    respx_mock: respx.MockRouter,
) -> None:
    """Callers pass lowercase; the wire enum is uppercase. Both must work."""
    spellings = ["market", "MARKET", "Market"]
    mock_capabilities(respx_mock)
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        for spelling in spellings:
            await gw.submit_order("AAPL", "buy", "10", spelling)

    # The caller's spelling is forwarded untouched — normalisation is only
    # for the comparison, not the wire format.
    sent = [json.loads(call.request.content)["order_type"] for call in route.calls]
    assert sent == spellings


@respx.mock(base_url="http://localhost:8080")
async def test_submit_order_fetches_capabilities_once(respx_mock: respx.MockRouter) -> None:
    """Capabilities are static per connection — don't pay a round-trip per order."""
    caps_route = mock_capabilities(respx_mock)
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))
    async with AsyncTradingGateway() as gw:
        await gw.submit_order("AAPL", "buy", "10")
        await gw.submit_order("AAPL", "sell", "10")
        await gw.submit_order("MSFT", "buy", "5", "limit", limit_price="400")
    assert caps_route.call_count == 1


@respx.mock(base_url="http://localhost:8080")
async def test_concurrent_first_submits_fetch_capabilities_once(
    respx_mock: respx.MockRouter,
) -> None:
    """A cold-start burst must not fan out into one fetch per order.

    Firing several entries at once right after connecting is normal, and
    ``respx`` resolves without a real suspension point — so a sequential
    test cannot catch the check-then-await-then-set race. This one delays
    the response to force the overlap.
    """

    async def slow_capabilities(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.01)
        return httpx.Response(200, json=SAMPLE_CAPABILITIES)

    caps_route = respx_mock.get("/v1/capabilities").mock(side_effect=slow_capabilities)
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))

    async with AsyncTradingGateway() as gw:
        await asyncio.gather(*(gw.submit_order("AAPL", "buy", "1") for _ in range(10)))

    assert caps_route.call_count == 1


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_submit_order_blocked_when_capabilities_unavailable(
    respx_mock: respx.MockRouter,
) -> None:
    """Fail closed: no order goes out unvalidated."""
    respx_mock.get("/v1/capabilities").mock(
        return_value=httpx.Response(503, json={"code": "PROVIDER_UNAVAILABLE", "message": "down"})
    )
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(ProviderUnavailableError):
            await gw.submit_order("AAPL", "buy", "10")
    assert not route.called


@respx.mock(base_url="http://localhost:8080")
async def test_failed_capabilities_fetch_is_not_cached(
    respx_mock: respx.MockRouter,
) -> None:
    """A failed fetch must not be cached, or one blip poisons the session."""
    caps_route = respx_mock.get("/v1/capabilities")
    caps_route.side_effect = [
        httpx.Response(503, json={"code": "PROVIDER_UNAVAILABLE", "message": "down"}),
        httpx.Response(200, json=SAMPLE_CAPABILITIES),
    ]
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(ProviderUnavailableError):
            await gw.submit_order("AAPL", "buy", "10")
        await gw.submit_order("AAPL", "buy", "10")
    assert route.called


@respx.mock(base_url="http://localhost:8080")
async def test_get_order_history(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/history").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    async with AsyncTradingGateway() as gw:
        orders = await gw.get_order_history()
    assert len(orders) == 1


@respx.mock(base_url="http://localhost:8080")
async def test_modify_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.patch("/v1/orders/ord_123").mock(
        return_value=httpx.Response(200, json={"order": SAMPLE_ORDER})
    )
    async with AsyncTradingGateway() as gw:
        result = await gw.modify_order("ord_123", quantity="20")
    assert isinstance(result, ModifyOrderResult)


@respx.mock(base_url="http://localhost:8080")
async def test_cancel_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders/ord_123").mock(
        return_value=httpx.Response(200, json={"success": True, "order": SAMPLE_ORDER})
    )
    async with AsyncTradingGateway() as gw:
        result = await gw.cancel_order("ord_123")
    assert isinstance(result, CancelOrderResult)
    assert result.success is True


@respx.mock(base_url="http://localhost:8080")
async def test_cancel_all_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders").mock(
        return_value=httpx.Response(200, json={"cancelled_count": 3, "failed_count": 0})
    )
    async with AsyncTradingGateway() as gw:
        result = await gw.cancel_all_orders()
    assert isinstance(result, CancelAllResult)
    assert result.cancelled_count == 3


# -----------------------------------------------------------------------
# Positions
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_list_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions").mock(return_value=httpx.Response(200, json=[SAMPLE_POSITION]))
    async with AsyncTradingGateway() as gw:
        positions = await gw.list_positions()
    assert len(positions) == 1
    assert isinstance(positions[0], Position)


@respx.mock(base_url="http://localhost:8080")
async def test_get_position(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_POSITION)
    )
    async with AsyncTradingGateway() as gw:
        position = await gw.get_position("pos_001")
    assert position.symbol == "AAPL"


@respx.mock(base_url="http://localhost:8080")
async def test_close_position(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
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
    mock_capabilities(respx_mock)
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.close_position(
            "pos_001",
            quantity=Decimal("5"),
            order_type="limit",
            limit_price="185.50",
        )
    sent = json.loads(route.calls.last.request.content)
    assert sent == {
        "quantity": "5",
        "order_type": "limit",
        "limit_price": "185.50",
    }


async def test_close_position_rejects_cancel_associated_orders() -> None:
    """The gateway dropped this field, so the SDK must not advertise it.

    It never worked — passing ``False`` never kept the exit legs resting — so
    the caller is better served by an error at the call site than by a flag
    the gateway silently ignores.
    """
    async with AsyncTradingGateway() as gw:
        with pytest.raises(TypeError, match="cancel_associated_orders"):
            gw.close_position("pos_001", cancel_associated_orders=False)


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_close_position_rejects_unsupported_order_type(
    respx_mock: respx.MockRouter,
) -> None:
    """Closing is the other path that creates an order — guard it too."""
    mock_capabilities(respx_mock, order_types=["MARKET", "LIMIT", "STOP"])
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(UnsupportedOrderTypeError):
            await gw.close_position("pos_001", order_type="trailing_stop")
    assert not route.called


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_close_position_without_order_type_skips_validation(
    respx_mock: respx.MockRouter,
) -> None:
    """No type named means the provider picks — nothing to validate.

    Closing a position is risk-reducing, so it must not become dependent on
    a capabilities fetch that could fail.
    """
    caps_route = respx_mock.get("/v1/capabilities").mock(
        return_value=httpx.Response(503, json={"code": "PROVIDER_UNAVAILABLE", "message": "down"})
    )
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    async with AsyncTradingGateway(max_retries=0) as gw:
        await gw.close_position("pos_001", quantity="5")
    assert route.called
    assert not caps_route.called, "must not fetch capabilities when no type is named"


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_EXIT_MOVE)
    )
    async with AsyncTradingGateway() as gw:
        result = await gw.modify_position_exits("pos_001", stop_loss=Decimal("180.00"))
    assert isinstance(result, ModifyPositionExitsResult)
    assert result.position_id == "pos_001"
    assert result.stop_loss is not None
    assert result.stop_loss.trigger_price == "180.00"
    assert result.stop_loss.order_ids == ["ord_sl_1", "ord_sl_2"]
    assert result.take_profit is None
    # Decimal is stringified; the untouched leg is omitted, not sent as null —
    # sending `take_profit: null` would read as "move it" to the gateway.
    assert json.loads(route.calls.last.request.content) == {"stop_loss": "180.00"}


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_omits_untouched_stop_loss(
    respx_mock: respx.MockRouter,
) -> None:
    """Mirror of the above: moving only the take-profit must not touch the stop."""
    route = respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_EXIT_MOVE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.modify_position_exits("pos_001", take_profit="195.00")
    assert json.loads(route.calls.last.request.content) == {"take_profit": "195.00"}


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_both_legs(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_EXIT_MOVE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.modify_position_exits("pos_001", stop_loss="180.00", take_profit="195.00")
    assert json.loads(route.calls.last.request.content) == {
        "stop_loss": "180.00",
        "take_profit": "195.00",
    }


async def test_modify_position_exits_requires_a_leg() -> None:
    """Neither leg given is a caller bug — fail locally, don't spend a round-trip."""
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="stop_loss.*take_profit"):
            await gw.modify_position_exits("pos_001")


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_encodes_position_id(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.patch("/v1/positions/pos%2F001").mock(
        return_value=httpx.Response(200, json=SAMPLE_EXIT_MOVE)
    )
    async with AsyncTradingGateway() as gw:
        await gw.modify_position_exits("pos/001", stop_loss="180.00")
    assert route.called


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_conflict_is_still_protected(
    respx_mock: respx.MockRouter,
) -> None:
    """409 — the move was rejected and the original exit restored."""
    respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(
            409,
            json={"code": "ORDER_NOT_MODIFIABLE", "message": "leg not tracked"},
        )
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ConflictError) as exc_info:
            await gw.modify_position_exits("pos_001", stop_loss="180.00")
    assert exc_info.value.status_code == 409
    # A caller must be able to tell "still protected" from "unprotected".
    assert not isinstance(exc_info.value, PositionUnprotectedError)


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_bad_gateway_is_unprotected(
    respx_mock: respx.MockRouter,
) -> None:
    """502 — the exit was cancelled and could not be re-established."""
    respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(
            502,
            json={"code": "PROVIDER_ERROR", "message": "exit could not be restored"},
        )
    )
    async with AsyncTradingGateway() as gw:
        with pytest.raises(PositionUnprotectedError) as exc_info:
            await gw.modify_position_exits("pos_001", stop_loss="180.00")
    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "PROVIDER_ERROR"


@respx.mock(base_url="http://localhost:8080")
async def test_modify_position_exits_does_not_retry_on_502(respx_mock: respx.MockRouter) -> None:
    """A 502 here means the position is uncovered — re-firing the move is unsafe.

    Suppression comes from PATCH being outside ``_RETRYABLE_METHODS`` rather
    than from anything endpoint-specific; this pins that safety property for
    the one call where a silent retry would move an exit twice.
    """
    route = respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(502, json={"code": "PROVIDER_ERROR", "message": "gone"})
    )
    async with AsyncTradingGateway(max_retries=3) as gw:
        with pytest.raises(PositionUnprotectedError):
            await gw.modify_position_exits("pos_001", stop_loss="180.00")
    assert route.call_count == 1


@respx.mock(base_url="http://localhost:8080")
async def test_502_elsewhere_is_not_position_unprotected(respx_mock: respx.MockRouter) -> None:
    """Only the exit-move endpoint promotes a 502.

    A proxy in front of the gateway returning Bad Gateway on an unrelated call
    must not tell a caller their position is uncovered — that invites a flatten
    over infra noise.
    """
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(502, text="<html>bad gateway</html>")
    )
    async with AsyncTradingGateway(max_retries=0) as gw:
        with pytest.raises(APIStatusError) as exc_info:
            await gw.get_account()
    assert not isinstance(exc_info.value, PositionUnprotectedError)


@respx.mock(base_url="http://localhost:8080")
async def test_close_all_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/positions").mock(
        return_value=httpx.Response(200, json=[SAMPLE_ORDER_HANDLE])
    )
    async with AsyncTradingGateway() as gw:
        handles = await gw.close_all_positions()
    assert len(handles) == 1


# -----------------------------------------------------------------------
# Market Data
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_get_quote(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/quotes/AAPL").mock(return_value=httpx.Response(200, json=SAMPLE_QUOTE))
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
        await gw.get_order("ord/weird#id")
    assert route.called


@respx.mock(base_url="http://localhost:8080")
async def test_get_bars(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/bars/AAPL").mock(return_value=httpx.Response(200, json=[SAMPLE_BAR]))
    async with AsyncTradingGateway() as gw:
        bars = await gw.get_bars("AAPL", "1m")
    assert len(bars) == 1
    assert isinstance(bars[0], Bar)


# -----------------------------------------------------------------------
# Trades
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_list_trades(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/trades").mock(return_value=httpx.Response(200, json=[SAMPLE_TRADE]))
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
        caps = await gw.get_capabilities()
    assert isinstance(caps, Capabilities)


@respx.mock(base_url="http://localhost:8080")
async def test_get_status(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/status").mock(
        return_value=httpx.Response(200, json=SAMPLE_CONNECTION_STATUS)
    )
    async with AsyncTradingGateway() as gw:
        status = await gw.get_status()
    assert isinstance(status, ConnectionStatus)
    assert status.connected is True


@respx.mock(base_url="http://localhost:8080")
async def test_get_health(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/health").mock(return_value=httpx.Response(200, json=SAMPLE_HEALTH))
    async with AsyncTradingGateway() as gw:
        health = await gw.get_health()
    assert isinstance(health, DetailedHealthStatus)


@respx.mock(base_url="http://localhost:8080")
async def test_get_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/circuit-breakers").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    async with AsyncTradingGateway() as gw:
        cb = await gw.get_circuit_breakers()
    assert isinstance(cb, CircuitBreakerStatusResponse)


@respx.mock(base_url="http://localhost:8080")
async def test_reset_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.post("/v1/circuit-breakers/reset").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
        with pytest.raises(NotFoundError) as exc_info:
            await gw.get_order("nonexistent")
    assert exc_info.value.status_code == 404
    assert exc_info.value.code == "ORDER_NOT_FOUND"


@respx.mock(base_url="http://localhost:8080")
async def test_422_raises_order_rejected(respx_mock: respx.MockRouter) -> None:
    mock_capabilities(respx_mock)
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
    async with AsyncTradingGateway() as gw:
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
    error_cls: type[APIStatusError],
    code: str,
) -> None:
    """Each HTTP error status should dispatch to its own exception subclass."""
    respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(status, json={"code": code, "message": "err"})
    )
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway() as gw:
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
    async with AsyncTradingGateway(api_key="test-key-123") as gw:
        await gw.get_account()
    auth = route.calls[0].request.headers.get("authorization")
    assert auth == "Bearer test-key-123"


@respx.mock(base_url="http://localhost:8080")
async def test_no_auth_header_when_no_key(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    async with AsyncTradingGateway() as gw:
        await gw.get_account()
    assert "authorization" not in route.calls[0].request.headers


# -----------------------------------------------------------------------
# Context manager
# -----------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
async def test_context_manager(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    async with AsyncTradingGateway() as gw:
        account = await gw.get_account()
    assert account.balance == "10000.00"


# -----------------------------------------------------------------------
# Sizing helper: quantity_for_notional
# -----------------------------------------------------------------------

# SAMPLE_QUOTE has bid 185.10 / ask 185.15 -> midpoint 185.125.
_MID = (Decimal("185.10") + Decimal("185.15")) / 2


@respx.mock(base_url="http://localhost:8080")
async def test_quantity_for_notional_sizes_at_quote_midpoint(
    respx_mock: respx.MockRouter,
) -> None:
    quote_route = respx_mock.get("/v1/quotes/AAPL").mock(
        return_value=httpx.Response(200, json=SAMPLE_QUOTE)
    )
    async with AsyncTradingGateway() as gw:
        qty = await gw.quantity_for_notional("AAPL", notional="5000")
    assert qty == Decimal("5000") / _MID
    assert quote_route.called


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_quantity_for_notional_price_override_skips_quote(
    respx_mock: respx.MockRouter,
) -> None:
    """Passing price= must not hit the quote endpoint."""
    quote_route = respx_mock.get("/v1/quotes/AAPL").mock(
        return_value=httpx.Response(200, json=SAMPLE_QUOTE)
    )
    async with AsyncTradingGateway() as gw:
        qty = await gw.quantity_for_notional("AAPL", notional="5000", price="200")
    assert qty == Decimal("5000") / Decimal("200")
    assert not quote_route.called


@respx.mock(base_url="http://localhost:8080")
async def test_quantity_for_equity_fraction_sizes_off_account_equity(
    respx_mock: respx.MockRouter,
) -> None:
    # SAMPLE_ACCOUNT equity is 10500.00; 10% -> 1050 notional.
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    quote_route = respx_mock.get("/v1/quotes/AAPL").mock(
        return_value=httpx.Response(200, json=SAMPLE_QUOTE)
    )
    async with AsyncTradingGateway() as gw:
        qty = await gw.quantity_for_notional("AAPL", equity_fraction=0.10)
    expected = (Decimal("10500.00") * Decimal("0.10")) / _MID
    assert qty == expected
    assert quote_route.called


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
async def test_quantity_for_equity_fraction_price_override_skips_quote_only(
    respx_mock: respx.MockRouter,
) -> None:
    """equity_fraction still needs the account; price= only skips the quote."""
    account_route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    quote_route = respx_mock.get("/v1/quotes/AAPL").mock(
        return_value=httpx.Response(200, json=SAMPLE_QUOTE)
    )
    async with AsyncTradingGateway() as gw:
        qty = await gw.quantity_for_notional("AAPL", equity_fraction="0.10", price="200")
    assert qty == (Decimal("10500.00") * Decimal("0.10")) / Decimal("200")
    assert account_route.called
    assert not quote_route.called


async def test_quantity_for_notional_requires_exactly_one_target() -> None:
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="exactly one"):
            await gw.quantity_for_notional("AAPL", price="200")
        with pytest.raises(ValueError, match="exactly one"):
            await gw.quantity_for_notional(
                "AAPL", notional="5000", equity_fraction=0.1, price="200"
            )


async def test_quantity_for_notional_rejects_nonpositive_notional() -> None:
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="positive"):
            await gw.quantity_for_notional("AAPL", notional="0", price="200")
        with pytest.raises(ValueError, match="positive"):
            await gw.quantity_for_notional("AAPL", notional="-100", price="200")


async def test_quantity_for_notional_rejects_nonpositive_fraction() -> None:
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="positive"):
            await gw.quantity_for_notional("AAPL", equity_fraction=0, price="200")


async def test_quantity_for_notional_rejects_nonpositive_price() -> None:
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="positive"):
            await gw.quantity_for_notional("AAPL", notional="5000", price="0")


async def test_quantity_for_notional_rejects_non_numeric_inputs() -> None:
    """Garbage user input fails with a field-labelled ValueError, not a bare
    InvalidOperation."""
    async with AsyncTradingGateway() as gw:
        with pytest.raises(ValueError, match="notional is not a valid number"):
            await gw.quantity_for_notional("AAPL", notional="abc", price="200")
        with pytest.raises(ValueError, match="price is not a valid number"):
            await gw.quantity_for_notional("AAPL", notional="5000", price="abc")
        with pytest.raises(ValueError, match="equity_fraction is not a valid number"):
            await gw.quantity_for_notional("AAPL", equity_fraction="abc", price="200")
