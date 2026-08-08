"""Tests for TradingGateway (sync client) — verifies it wraps async correctly."""

from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest
import respx

from tektii.client import TradingGateway
from tektii.errors import NotFoundError, PositionUnprotectedError, UnsupportedOrderTypeError
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
from tektii.stream import SyncEventStream

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


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_account(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    gw = TradingGateway()
    account = gw.get_account()
    assert isinstance(account, Account)
    assert account.balance == "10000.00"


@respx.mock(base_url="http://localhost:8080")
def test_sync_submit_order(respx_mock: respx.MockRouter) -> None:
    mock_capabilities(respx_mock)
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))
    gw = TradingGateway()
    handle = gw.submit_order("AAPL", "buy", "1")
    assert isinstance(handle, OrderHandle)
    assert handle.id == "ord_123"


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
def test_sync_submit_order_rejects_unsupported_order_type(respx_mock: respx.MockRouter) -> None:
    """The sync client inherits validation from the async client it wraps."""
    mock_capabilities(respx_mock, order_types=["MARKET", "LIMIT", "STOP"])
    route = respx_mock.post("/v1/orders").mock(
        return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE)
    )
    gw = TradingGateway()
    with pytest.raises(UnsupportedOrderTypeError):
        gw.submit_order("AAPL", "buy", "1", "trailing_stop")
    assert not route.called


@respx.mock(base_url="http://localhost:8080")
def test_sync_submit_order_shares_the_capabilities_cache(respx_mock: respx.MockRouter) -> None:
    """One shared async gateway per sync client means one capabilities fetch."""
    caps_route = mock_capabilities(respx_mock)
    respx_mock.post("/v1/orders").mock(return_value=httpx.Response(201, json=SAMPLE_ORDER_HANDLE))
    gw = TradingGateway()
    gw.submit_order("AAPL", "buy", "1")
    gw.submit_order("AAPL", "sell", "1")
    assert caps_route.call_count == 1


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/ord_123").mock(return_value=httpx.Response(200, json=SAMPLE_ORDER))
    gw = TradingGateway()
    order = gw.get_order("ord_123")
    assert isinstance(order, Order)


@respx.mock(base_url="http://localhost:8080")
def test_sync_list_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions").mock(return_value=httpx.Response(200, json=[SAMPLE_POSITION]))
    gw = TradingGateway()
    positions = gw.list_positions()
    assert len(positions) == 1
    assert isinstance(positions[0], Position)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_quote(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/quotes/AAPL").mock(return_value=httpx.Response(200, json=SAMPLE_QUOTE))
    gw = TradingGateway()
    quote = gw.get_quote("AAPL")
    assert isinstance(quote, Quote)


@respx.mock(base_url="http://localhost:8080")
def test_sync_quantity_for_notional(respx_mock: respx.MockRouter) -> None:
    """Sync wrapper mirrors the async sizing helper end-to-end."""
    respx_mock.get("/v1/quotes/AAPL").mock(return_value=httpx.Response(200, json=SAMPLE_QUOTE))
    gw = TradingGateway()
    qty = gw.quantity_for_notional("AAPL", notional="5000")
    mid = (Decimal("185.10") + Decimal("185.15")) / 2
    assert qty == Decimal("5000") / mid


@respx.mock(base_url="http://localhost:8080")
def test_sync_quantity_for_equity_fraction(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    respx_mock.get("/v1/quotes/AAPL").mock(return_value=httpx.Response(200, json=SAMPLE_QUOTE))
    gw = TradingGateway()
    qty = gw.quantity_for_notional("AAPL", equity_fraction=0.10)
    mid = (Decimal("185.10") + Decimal("185.15")) / 2
    assert qty == (Decimal("10500.00") * Decimal("0.10")) / mid


@respx.mock(base_url="http://localhost:8080")
def test_sync_context_manager(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/account").mock(return_value=httpx.Response(200, json=SAMPLE_ACCOUNT))
    with TradingGateway() as gw:
        account = gw.get_account()
    assert account.currency == "USD"


@respx.mock(base_url="http://localhost:8080")
def test_sync_api_key(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get("/v1/account").mock(
        return_value=httpx.Response(200, json=SAMPLE_ACCOUNT)
    )
    gw = TradingGateway(api_key="my-key")
    gw.get_account()
    auth = route.calls[0].request.headers.get("authorization")
    assert auth == "Bearer my-key"


# ---------------------------------------------------------------------------
# Coverage: every remaining sync method must forward to its async counterpart.
# ---------------------------------------------------------------------------


@respx.mock(base_url="http://localhost:8080")
def test_sync_list_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    orders = TradingGateway().list_orders()
    assert len(orders) == 1
    assert isinstance(orders[0], Order)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_order_history(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/orders/history").mock(return_value=httpx.Response(200, json=[SAMPLE_ORDER]))
    orders = TradingGateway().get_order_history()
    assert len(orders) == 1


@respx.mock(base_url="http://localhost:8080")
def test_sync_modify_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.patch("/v1/orders/ord_123").mock(
        return_value=httpx.Response(
            200,
            json={
                "order": SAMPLE_ORDER,
                "previous_order_id": "ord_122",
            },
        )
    )
    result = TradingGateway().modify_order("ord_123", quantity="5")
    assert isinstance(result, ModifyOrderResult)


@respx.mock(base_url="http://localhost:8080")
def test_sync_cancel_order(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders/ord_123").mock(
        return_value=httpx.Response(
            200,
            json={"order": SAMPLE_ORDER, "success": True},
        )
    )
    result = TradingGateway().cancel_order("ord_123")
    assert isinstance(result, CancelOrderResult)


@respx.mock(base_url="http://localhost:8080")
def test_sync_cancel_all_orders(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/orders").mock(
        return_value=httpx.Response(
            200,
            json={
                "cancelled_count": 3,
                "failed_count": 0,
                "cancelled_ids": ["a", "b", "c"],
                "failed": [],
            },
        )
    )
    result = TradingGateway().cancel_all_orders()
    assert isinstance(result, CancelAllResult)
    assert result.cancelled_count == 3


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_position(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_POSITION)
    )
    pos = TradingGateway().get_position("pos_001")
    assert isinstance(pos, Position)


@respx.mock(base_url="http://localhost:8080")
def test_sync_close_position(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    handle = TradingGateway().close_position("pos_001", quantity="5")
    assert isinstance(handle, OrderHandle)
    sent = json.loads(route.calls.last.request.content)
    assert sent == {"quantity": "5"}


def test_sync_close_position_rejects_cancel_associated_orders() -> None:
    """The sync signature must drop the dead knob in step with the async one."""
    with pytest.raises(TypeError, match="cancel_associated_orders"):
        TradingGateway().close_position("pos_001", cancel_associated_orders=False)


@respx.mock(base_url="http://localhost:8080", assert_all_called=False)
def test_sync_close_position_rejects_unsupported_order_type(
    respx_mock: respx.MockRouter,
) -> None:
    mock_capabilities(respx_mock, order_types=["MARKET", "LIMIT", "STOP"])
    route = respx_mock.delete("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_ORDER_HANDLE)
    )
    with pytest.raises(UnsupportedOrderTypeError):
        TradingGateway().close_position("pos_001", order_type="trailing_stop")
    assert not route.called


@respx.mock(base_url="http://localhost:8080")
def test_sync_modify_position_exits(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(200, json=SAMPLE_EXIT_MOVE)
    )
    result = TradingGateway().modify_position_exits("pos_001", stop_loss=Decimal("180.00"))
    assert isinstance(result, ModifyPositionExitsResult)
    assert result.stop_loss is not None
    assert result.stop_loss.trigger_price == "180.00"
    assert json.loads(route.calls.last.request.content) == {"stop_loss": "180.00"}


@respx.mock(base_url="http://localhost:8080")
def test_sync_modify_position_exits_unprotected(respx_mock: respx.MockRouter) -> None:
    """The 502 → unprotected mapping must survive the asyncio.run boundary."""
    respx_mock.patch("/v1/positions/pos_001").mock(
        return_value=httpx.Response(
            502,
            json={"code": "PROVIDER_ERROR", "message": "exit could not be restored"},
        )
    )
    with pytest.raises(PositionUnprotectedError) as exc_info:
        TradingGateway().modify_position_exits("pos_001", take_profit="195.00")
    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "PROVIDER_ERROR"


@respx.mock(base_url="http://localhost:8080")
def test_sync_close_all_positions(respx_mock: respx.MockRouter) -> None:
    respx_mock.delete("/v1/positions").mock(
        return_value=httpx.Response(200, json=[SAMPLE_ORDER_HANDLE])
    )
    handles = TradingGateway().close_all_positions()
    assert len(handles) == 1


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_bars(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/bars/AAPL").mock(return_value=httpx.Response(200, json=[SAMPLE_BAR]))
    bars = TradingGateway().get_bars("AAPL", "1m")
    assert len(bars) == 1
    assert isinstance(bars[0], Bar)


@respx.mock(base_url="http://localhost:8080")
def test_sync_list_trades(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/trades").mock(return_value=httpx.Response(200, json=[SAMPLE_TRADE]))
    trades = TradingGateway().list_trades()
    assert len(trades) == 1
    assert isinstance(trades[0], Trade)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_capabilities(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/capabilities").mock(
        return_value=httpx.Response(200, json=SAMPLE_CAPABILITIES)
    )
    assert isinstance(TradingGateway().get_capabilities(), Capabilities)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_status(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/status").mock(
        return_value=httpx.Response(200, json=SAMPLE_CONNECTION_STATUS)
    )
    assert isinstance(TradingGateway().get_status(), ConnectionStatus)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_health(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/health").mock(return_value=httpx.Response(200, json=SAMPLE_HEALTH))
    assert isinstance(TradingGateway().get_health(), DetailedHealthStatus)


@respx.mock(base_url="http://localhost:8080")
def test_sync_get_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/v1/circuit-breakers").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    assert isinstance(TradingGateway().get_circuit_breakers(), CircuitBreakerStatusResponse)


@respx.mock(base_url="http://localhost:8080")
def test_sync_reset_circuit_breakers(respx_mock: respx.MockRouter) -> None:
    respx_mock.post("/v1/circuit-breakers/reset").mock(
        return_value=httpx.Response(200, json=SAMPLE_CIRCUIT_BREAKERS)
    )
    assert isinstance(TradingGateway().reset_circuit_breakers(), CircuitBreakerStatusResponse)


def test_sync_repr_redacts_api_key() -> None:
    gw = TradingGateway(api_key="tk_sync_secret")
    rep = repr(gw)
    assert "tk_sync_secret" not in rep
    assert "'***'" in rep

    rep_no_key = repr(TradingGateway())
    assert "api_key=None" in rep_no_key


def test_sync_stream_factory_builds_ws_url() -> None:
    """The sync ``stream()`` factory returns a SyncEventStream with the
    correct ``ws://`` URL. We don't actually connect — just verify wiring.
    """
    gw = TradingGateway(base_url="http://localhost:8080")
    stream = gw.stream()
    assert isinstance(stream, SyncEventStream)
    assert stream._ws_url == "ws://localhost:8080/v1/ws"


@respx.mock(base_url="http://localhost:8080")
def test_sync_error_propagates(respx_mock: respx.MockRouter) -> None:
    """Exceptions raised in the async layer must reach the sync caller."""
    respx_mock.get("/v1/orders/ord_missing").mock(
        return_value=httpx.Response(
            404,
            json={"code": "ORDER_NOT_FOUND", "message": "no such order"},
        )
    )
    with pytest.raises(NotFoundError):
        TradingGateway().get_order("ord_missing")
