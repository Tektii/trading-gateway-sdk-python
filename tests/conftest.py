"""Shared test fixtures."""

from __future__ import annotations

import httpx
import pytest
import respx

_GATEWAY_ENV_VARS = (
    "TEKTII_TRADING_GATEWAY_URL",
    "TEKTII_TRADING_GATEWAY_API_KEY",
    "TRADING_GATEWAY_URL",
    "TRADING_GATEWAY_API_KEY",
)


@pytest.fixture(autouse=True)
def _clean_gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit tests must not inherit gateway config from the host environment."""
    for name in _GATEWAY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# Sample JSON responses matching the gateway's API
SAMPLE_ACCOUNT = {
    "balance": "10000.00",
    "equity": "10500.00",
    "margin_used": "2000.00",
    "margin_available": "8000.00",
    "unrealized_pnl": "500.00",
    "currency": "USD",
}

SAMPLE_ORDER_HANDLE = {
    "id": "ord_123",
    "status": "PENDING",
    "client_order_id": None,
    "correlation_id": "corr_abc",
}

SAMPLE_ORDER = {
    "id": "ord_123",
    "symbol": "AAPL",
    "side": "BUY",
    "order_type": "MARKET",
    "quantity": "10",
    "filled_quantity": "10",
    "remaining_quantity": "0",
    "status": "FILLED",
    "time_in_force": "GTC",
    "average_fill_price": "185.50",
    "created_at": "2025-01-15T10:30:00Z",
    "updated_at": "2025-01-15T10:30:01Z",
}

SAMPLE_POSITION = {
    "id": "pos_001",
    "symbol": "AAPL",
    "side": "LONG",
    "quantity": "10",
    "average_entry_price": "185.50",
    "current_price": "187.25",
    "unrealized_pnl": "17.50",
    "realized_pnl": "0.00",
    "opened_at": "2025-01-15T10:30:00Z",
    "updated_at": "2025-01-15T10:30:00Z",
}

SAMPLE_EXIT_MOVE = {
    "position_id": "pos_001",
    "stop_loss": {
        "order_ids": ["ord_sl_1", "ord_sl_2"],
        "trigger_price": "180.00",
    },
    "take_profit": None,
}

SAMPLE_QUOTE = {
    "symbol": "AAPL",
    "provider": "alpaca",
    "bid": "185.10",
    "ask": "185.15",
    "last": "185.12",
    "timestamp": "2025-01-15T10:30:00Z",
}

SAMPLE_BAR = {
    "symbol": "AAPL",
    "provider": "alpaca",
    "timeframe": "1m",
    "timestamp": "2025-01-15T10:30:00Z",
    "open": "185.00",
    "high": "185.50",
    "low": "184.90",
    "close": "185.25",
    "volume": "50000",
}

SAMPLE_TRADE = {
    "id": "trade_001",
    "order_id": "ord_123",
    "symbol": "AAPL",
    "side": "BUY",
    "quantity": "10",
    "price": "185.50",
    "commission": "0.50",
    "commission_currency": "USD",
    "timestamp": "2025-01-15T10:30:00Z",
}

SAMPLE_CAPABILITIES = {
    "supported_order_types": ["MARKET", "LIMIT", "STOP"],
    "supported_asset_classes": ["STOCK"],
    "position_mode": "NETTING",
    "features": ["bracket_orders"],
}

SAMPLE_CONNECTION_STATUS = {
    "connected": True,
    "last_heartbeat": "2025-01-15T10:30:00Z",
    "latency_ms": 42,
}

SAMPLE_HEALTH = {
    "status": "connected",
    "git_sha": "abc1234",
    "version": "0.1.0",
    "providers": [
        {
            "platform": "alpaca-paper",
            "connected": True,
        }
    ],
}

SAMPLE_CIRCUIT_BREAKERS = {
    "exit_order": {"state": "closed", "failure_count": 0},
    "adapter": {"state": "closed", "failure_count": 0},
}


def mock_capabilities(
    respx_mock: respx.MockRouter, *, order_types: list[str] | None = None
) -> respx.Route:
    """Route ``GET /v1/capabilities`` for a test that submits an order.

    Order submission checks the requested type against what the provider
    supports, so every test reaching ``submit_order`` must serve this
    endpoint. Pass ``order_types`` to override the supported set.
    """
    body = dict(SAMPLE_CAPABILITIES)
    if order_types is not None:
        body["supported_order_types"] = order_types
    return respx_mock.get("/v1/capabilities").mock(return_value=httpx.Response(200, json=body))
