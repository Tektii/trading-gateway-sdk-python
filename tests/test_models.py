"""Tests for model parsing — round-trip with sample JSON from the gateway."""

from pydantic import TypeAdapter

from tektii.models import (
    Account,
    AccountEvent,
    Bar,
    CancelAllResult,
    CandleEvent,
    Capabilities,
    CircuitBreakerStatusResponse,
    ConnectionEvent,
    ConnectionStatus,
    DataStalenessEvent,
    DetailedHealthStatus,
    ErrorEvent,
    FinancingEvent,
    GatewayEvent,
    ModifyOrderResult,
    Order,
    OrderEvent,
    OrderHandle,
    PingEvent,
    Position,
    PositionEvent,
    Quote,
    QuoteEvent,
    RateLimitEvent,
    Trade,
    TradeEvent,
)

from .conftest import (
    SAMPLE_CAPABILITIES,
    SAMPLE_CIRCUIT_BREAKERS,
    SAMPLE_CONNECTION_STATUS,
    SAMPLE_HEALTH,
)

# ---------------------------------------------------------------------------
# REST model parsing
# ---------------------------------------------------------------------------


def test_account_parse() -> None:
    data = {
        "balance": "10000.00",
        "equity": "10500.00",
        "margin_used": "2000.00",
        "margin_available": "8000.00",
        "unrealized_pnl": "500.00",
        "currency": "USD",
    }
    account = Account.model_validate(data)
    assert account.balance == "10000.00"
    assert account.currency == "USD"


def test_order_parse() -> None:
    data = {
        "id": "ord_123",
        "symbol": "AAPL",
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": "10",
        "filled_quantity": "0",
        "remaining_quantity": "10",
        "status": "PENDING",
        "time_in_force": "GTC",
        "created_at": "2025-01-15T10:30:00Z",
        "updated_at": "2025-01-15T10:30:00Z",
    }
    order = Order.model_validate(data)
    assert order.id == "ord_123"
    assert order.symbol == "AAPL"
    assert order.side == "BUY"
    assert order.status == "PENDING"
    assert order.limit_price is None


def test_order_with_optional_fields() -> None:
    data = {
        "id": "ord_456",
        "symbol": "EUR/USD",
        "side": "SELL",
        "order_type": "LIMIT",
        "quantity": "1000",
        "filled_quantity": "500",
        "remaining_quantity": "500",
        "status": "PARTIALLY_FILLED",
        "time_in_force": "DAY",
        "limit_price": "1.0850",
        "stop_loss": "1.0900",
        "take_profit": "1.0750",
        "client_order_id": "my-order-1",
        "average_fill_price": "1.0855",
        "created_at": "2025-01-15T10:30:00Z",
        "updated_at": "2025-01-15T10:31:00Z",
    }
    order = Order.model_validate(data)
    assert order.limit_price == "1.0850"
    assert order.stop_loss == "1.0900"
    assert order.client_order_id == "my-order-1"
    assert order.average_fill_price == "1.0855"


def test_order_handle_parse() -> None:
    data = {"id": "ord_789", "status": "PENDING"}
    handle = OrderHandle.model_validate(data)
    assert handle.id == "ord_789"
    assert handle.status == "PENDING"
    assert handle.client_order_id is None


def test_position_parse() -> None:
    data = {
        "id": "pos_001",
        "symbol": "AAPL",
        "side": "LONG",
        "quantity": "100",
        "average_entry_price": "185.50",
        "current_price": "187.25",
        "unrealized_pnl": "175.00",
        "realized_pnl": "0.00",
        "opened_at": "2025-01-15T09:30:00Z",
        "updated_at": "2025-01-15T10:30:00Z",
    }
    position = Position.model_validate(data)
    assert position.id == "pos_001"
    assert position.side == "LONG"
    assert position.unrealized_pnl == "175.00"


def test_quote_parse() -> None:
    data = {
        "symbol": "AAPL",
        "provider": "alpaca",
        "bid": "185.10",
        "ask": "185.15",
        "last": "185.12",
        "timestamp": "2025-01-15T10:30:00Z",
    }
    quote = Quote.model_validate(data)
    assert quote.symbol == "AAPL"
    assert quote.bid == "185.10"
    assert quote.bid_size is None


def test_bar_parse() -> None:
    data = {
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
    bar = Bar.model_validate(data)
    assert bar.close == "185.25"
    assert bar.timeframe == "1m"


def test_trade_parse() -> None:
    data = {
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
    trade = Trade.model_validate(data)
    assert trade.id == "trade_001"
    assert trade.price == "185.50"


# ---------------------------------------------------------------------------
# WebSocket event parsing (discriminated union)
# ---------------------------------------------------------------------------

event_adapter = TypeAdapter(GatewayEvent)


def test_ping_event() -> None:
    data = {"type": "ping", "timestamp": "2025-01-15T10:30:00Z"}
    event = event_adapter.validate_python(data)
    assert isinstance(event, PingEvent)


def test_candle_event() -> None:
    data = {
        "type": "candle",
        "timestamp": "2025-01-15T10:30:00Z",
        "bar": {
            "symbol": "AAPL",
            "provider": "mock",
            "timeframe": "1m",
            "timestamp": "2025-01-15T10:30:00Z",
            "open": "150.00",
            "high": "150.50",
            "low": "149.90",
            "close": "150.25",
            "volume": "1000",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, CandleEvent)
    assert event.bar.symbol == "AAPL"
    assert event.bar.close == "150.25"


def test_order_event() -> None:
    data = {
        "type": "order",
        "event": "ORDER_FILLED",
        "timestamp": "2025-01-15T10:30:00Z",
        "order": {
            "id": "ord_123",
            "symbol": "AAPL",
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": "1",
            "filled_quantity": "1",
            "remaining_quantity": "0",
            "status": "FILLED",
            "time_in_force": "GTC",
            "average_fill_price": "150.25",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:01Z",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, OrderEvent)
    assert event.event == "ORDER_FILLED"
    assert event.order.average_fill_price == "150.25"


def test_position_event() -> None:
    data = {
        "type": "position",
        "event": "POSITION_OPENED",
        "timestamp": "2025-01-15T10:30:00Z",
        "position": {
            "id": "pos_001",
            "symbol": "AAPL",
            "side": "LONG",
            "quantity": "1",
            "average_entry_price": "150.25",
            "current_price": "150.25",
            "unrealized_pnl": "0.00",
            "realized_pnl": "0.00",
            "opened_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:00Z",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, PositionEvent)
    assert event.position.symbol == "AAPL"


def test_trade_event() -> None:
    data = {
        "type": "trade",
        "event": "TRADE_FILLED",
        "timestamp": "2025-01-15T10:30:00Z",
        "trade": {
            "id": "t_001",
            "order_id": "ord_123",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": "1",
            "price": "150.25",
            "commission": "0.01",
            "commission_currency": "USD",
            "timestamp": "2025-01-15T10:30:00Z",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, TradeEvent)
    assert event.trade.price == "150.25"


def test_connection_event() -> None:
    data = {
        "type": "connection",
        "event": "CONNECTED",
        "timestamp": "2025-01-15T10:30:00Z",
        "broker": "alpaca-paper",
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, ConnectionEvent)
    assert event.broker == "alpaca-paper"


def test_rate_limit_event() -> None:
    data = {
        "type": "rate_limit",
        "event": "RATE_LIMIT_WARNING",
        "timestamp": "2025-01-15T10:30:00Z",
        "requests_remaining": 5,
        "reset_at": "2025-01-15T10:31:00Z",
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, RateLimitEvent)
    assert event.requests_remaining == 5


def test_error_event() -> None:
    data = {
        "type": "error",
        "code": "INTERNAL_ERROR",
        "message": "Something went wrong",
        "timestamp": "2025-01-15T10:30:00Z",
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, ErrorEvent)
    assert event.code == "INTERNAL_ERROR"
    assert event.message == "Something went wrong"


def test_financing_event() -> None:
    data = {
        "type": "financing",
        "timestamp": "2025-01-15T10:30:00Z",
        "amount": "-12.34",
        "symbol": "EURUSD",
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, FinancingEvent)
    assert event.amount == "-12.34"
    assert event.symbol == "EURUSD"


def test_event_with_event_id() -> None:
    """Events from the Tektii backtest engine include event_id."""
    data = {
        "type": "candle",
        "timestamp": "2025-01-15T10:30:00Z",
        "event_id": "evt_abc123",
        "bar": {
            "symbol": "AAPL",
            "provider": "tektii",
            "timeframe": "1m",
            "timestamp": "2025-01-15T10:30:00Z",
            "open": "150.00",
            "high": "150.50",
            "low": "149.90",
            "close": "150.25",
            "volume": "1000",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, CandleEvent)
    assert event.event_id == "evt_abc123"


def test_event_without_event_id() -> None:
    """Events from live/mock gateway have no event_id."""
    data = {"type": "ping", "timestamp": "2025-01-15T10:30:00Z"}
    event = event_adapter.validate_python(data)
    assert isinstance(event, PingEvent)
    assert event.event_id is None


# ---------------------------------------------------------------------------
# Additional WebSocket events
# ---------------------------------------------------------------------------


def test_quote_event() -> None:
    data = {
        "type": "quote",
        "timestamp": "2025-01-15T10:30:00Z",
        "quote": {
            "symbol": "AAPL",
            "provider": "mock",
            "bid": "185.10",
            "ask": "185.20",
            "last": "185.15",
            "bid_size": "100",
            "ask_size": "100",
            "timestamp": "2025-01-15T10:30:00Z",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, QuoteEvent)
    assert event.quote.bid == "185.10"


def test_account_event() -> None:
    data = {
        "type": "account",
        "event": "BALANCE_UPDATED",
        "timestamp": "2025-01-15T10:30:00Z",
        "account": {
            "balance": "10000.00",
            "equity": "10000.00",
            "margin_used": "0",
            "margin_available": "10000.00",
            "unrealized_pnl": "0",
            "currency": "USD",
        },
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, AccountEvent)
    assert event.event == "BALANCE_UPDATED"
    assert event.account.currency == "USD"


def test_data_staleness_event() -> None:
    data = {
        "type": "data_staleness",
        "event": "STALE",
        "timestamp": "2025-01-15T10:30:00Z",
        "symbols": ["AAPL", "MSFT"],
        "broker": "alpaca-paper",
        "stale_since": "2025-01-15T10:29:50Z",
    }
    event = event_adapter.validate_python(data)
    assert isinstance(event, DataStalenessEvent)
    assert event.symbols == ["AAPL", "MSFT"]
    assert event.broker == "alpaca-paper"


# ---------------------------------------------------------------------------
# REST response models (direct parse)
# ---------------------------------------------------------------------------


def test_capabilities_parse() -> None:
    caps = Capabilities.model_validate(SAMPLE_CAPABILITIES)
    assert caps.position_mode == "NETTING"
    assert "bracket_orders" in caps.features


def test_connection_status_parse() -> None:
    status = ConnectionStatus.model_validate(SAMPLE_CONNECTION_STATUS)
    assert status.connected is True
    assert status.latency_ms == 42


def test_detailed_health_parse() -> None:
    health = DetailedHealthStatus.model_validate(SAMPLE_HEALTH)
    assert health.status == "connected"
    assert len(health.providers) == 1
    assert health.git_sha == "abc1234"
    assert health.version == "0.1.0"


def test_circuit_breaker_status_parse() -> None:
    status = CircuitBreakerStatusResponse.model_validate(SAMPLE_CIRCUIT_BREAKERS)
    assert status.exit_order.state == "closed"


def test_modify_order_result_parse() -> None:
    data = {
        "order": {
            "id": "ord_123",
            "symbol": "AAPL",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": "10",
            "filled_quantity": "0",
            "remaining_quantity": "10",
            "status": "OPEN",
            "time_in_force": "GTC",
            "limit_price": "180.00",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:01Z",
        },
        "previous_order_id": "ord_122",
    }
    result = ModifyOrderResult.model_validate(data)
    assert result.previous_order_id == "ord_122"
    assert result.order.id == "ord_123"


def test_cancel_all_result_parse() -> None:
    data = {
        "cancelled_count": 2,
        "failed_count": 1,
        "cancelled_ids": ["ord_a", "ord_b"],
        "failed": [{"order_id": "ord_c", "reason": "ORDER_NOT_MODIFIABLE"}],
    }
    result = CancelAllResult.model_validate(data)
    assert result.cancelled_count == 2
    assert result.failed_count == 1
