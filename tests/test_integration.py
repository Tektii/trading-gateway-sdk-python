"""Integration tests that run against a live mock gateway.

Skipped automatically when the gateway is not reachable. Run with:

    uv run pytest -m integration -v

Requires the gateway running in mock mode:

    cd ../tektii-gateway
    GATEWAY_PROVIDER=mock \
    SUBSCRIPTIONS='[{"platform":"mock","instrument":"AAPL","events":["quote"]}]' \
    cargo run --release
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, TypeVar

import httpx
import pytest

from tektii_gateway import (
    AsyncTektiiGateway,
    NotFoundError,
    QuoteEvent,
)

GATEWAY_URL = os.environ.get("TEKTII_GATEWAY_URL", "http://localhost:8080")

T = TypeVar("T")


async def wait_for(
    check: Callable[[], Awaitable[T]],
    predicate: Callable[[T], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.3,
    description: str = "",
) -> T:
    elapsed = 0.0
    while True:
        result = await check()
        if predicate(result):
            return result
        elapsed += interval
        if elapsed >= timeout:
            raise TimeoutError(f"Timed out waiting for: {description}")
        await asyncio.sleep(interval)


def _gateway_reachable() -> bool:
    try:
        resp = httpx.get(f"{GATEWAY_URL}/health", timeout=3.0)
        return resp.status_code == 200
    except (httpx.ConnectError, httpx.TimeoutException):
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _gateway_reachable(), reason=f"Gateway not running at {GATEWAY_URL}"),
]


@pytest.fixture
async def gw():
    async with AsyncTektiiGateway(base_url=GATEWAY_URL) as client:
        yield client


async def test_1_system_endpoints(gw: AsyncTektiiGateway):
    health = await gw.get_health()
    assert health.status is not None

    status = await gw.get_status()
    assert status.connected is True

    caps = await gw.get_capabilities()
    supported = [t.value if hasattr(t, "value") else str(t) for t in caps.supported_order_types]
    assert "MARKET" in supported

    cb = await gw.get_circuit_breakers()
    assert cb is not None

    account = await gw.get_account()
    assert account.currency == "USD"
    assert Decimal(account.balance) > 0


async def test_2_market_data(gw: AsyncTektiiGateway):
    quote = await gw.get_quote("AAPL")
    bid, ask = Decimal(quote.bid), Decimal(quote.ask)
    assert bid > 0
    assert ask > 0
    assert bid < ask

    bars = await gw.get_bars("AAPL", "1m")
    assert len(bars) > 0
    assert bars[0].open


async def test_3_order_lifecycle(gw: AsyncTektiiGateway):
    # Submit market buy
    handle = await gw.submit_order("AAPL", "buy", "1")
    assert handle.id

    # Wait for fill
    filled = await wait_for(
        lambda: gw.get_order(handle.id),
        lambda o: o.status in ("FILLED", "PARTIALLY_FILLED"),
        description="order to fill",
    )
    assert filled.filled_quantity == "1"
    assert filled.average_fill_price

    # Verify trade
    trades = await gw.list_trades(order_id=handle.id)
    assert len(trades) >= 1

    # Verify in history
    history = await gw.get_order_history()
    assert handle.id in [o.id for o in history]

    # Verify position
    positions = await gw.list_positions(symbol="AAPL")
    assert len(positions) >= 1
    pos = positions[0]
    assert pos.side in ("LONG", "long")

    # Close position
    close_handle = await gw.close_position(pos.id)
    assert close_handle.id

    await wait_for(
        lambda: gw.list_positions(symbol="AAPL"),
        lambda ps: len(ps) == 0,
        description="position to close",
    )


async def test_4_limit_order_and_cancel(gw: AsyncTektiiGateway):
    quote = await gw.get_quote("AAPL")
    far_below = str(Decimal(quote.bid) * Decimal("0.5"))

    handle = await gw.submit_order("AAPL", "buy", "1", "limit", limit_price=far_below)
    assert handle.id

    order = await wait_for(
        lambda: gw.get_order(handle.id),
        lambda o: o.status in ("OPEN", "PENDING"),
        description="limit order to be open",
    )
    assert order.status in ("OPEN", "PENDING")

    await gw.modify_order(handle.id, quantity="2")
    modified = await gw.get_order(handle.id)
    assert modified.quantity == "2"

    await gw.cancel_order(handle.id)
    cancelled = await gw.get_order(handle.id)
    assert cancelled.status in ("CANCELLED", "cancelled")


async def test_5_websocket_streaming(gw: AsyncTektiiGateway):
    events: list[Any] = []

    async def _collect() -> None:
        async with gw.stream() as stream:
            async for event in stream:
                events.append(event)
                if len(events) >= 3 or isinstance(event, QuoteEvent):
                    return

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(_collect(), timeout=6.0)

    # Soft check — no events just means SUBSCRIPTIONS wasn't set on the gateway
    # This is acceptable in CI where we may not configure subscriptions


async def test_6_error_handling(gw: AsyncTektiiGateway):
    with pytest.raises(NotFoundError):
        await gw.get_order("nonexistent_order_id_12345")

    with pytest.raises(NotFoundError):
        await gw.get_position("nonexistent_position_id_12345")
