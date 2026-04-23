"""Public models for the Tektii Gateway SDK.

REST response models are generated from the OpenAPI spec (see _generated/models.py).
WebSocket event models are hand-written here since they aren't in the OpenAPI spec.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag

# ---------------------------------------------------------------------------
# Re-exports from generated models (REST API types)
# ---------------------------------------------------------------------------
# Also re-export event type enums for users who want to match on them
from tektii_gateway._generated.models import (
    Account,
    AccountEventType,
    ApiError,
    AssetClass,
    Bar,
    CancelAllResult,
    CancelOrderResult,
    Capabilities,
    CircuitBreakerSnapshot,
    CircuitBreakerStatusResponse,
    ConnectionEventType,
    ConnectionStatus,
    DataStalenessEventType,
    DetailedHealthStatus,
    ErrorCode,
    HealthStatus,
    MarginMode,
    ModifyOrderResult,
    Order,
    OrderEventType,
    OrderHandle,
    OrderStatus,
    OrderType,
    OverallStatus,
    Position,
    PositionEventType,
    PositionMode,
    PositionSide,
    ProviderHealth,
    Quote,
    RateLimitEventType,
    RateLimits,
    ReadyStatus,
    RejectReason,
    Side,
    Timeframe,
    TimeInForce,
    Trade,
    TradeEventType,
    TradingPlatform,
    TrailingType,
    WsErrorCode,
)

# ---------------------------------------------------------------------------
# WebSocket event models (hand-written — not in OpenAPI spec)
# ---------------------------------------------------------------------------


class BaseEvent(BaseModel):
    """Common fields for all WebSocket events."""

    # ``extra="ignore"`` is Pydantic v2's default but we pin it explicitly:
    # the gateway is free to add new fields to event payloads without
    # breaking existing SDK users, even if a future Pydantic release
    # flipped the default.
    model_config = ConfigDict(extra="ignore")

    timestamp: datetime
    event_id: str | None = None  # Present only from Tektii backtest engine


class PingEvent(BaseEvent):
    type: Literal["ping"] = "ping"


class CandleEvent(BaseEvent):
    type: Literal["candle"] = "candle"
    bar: Bar


class QuoteEvent(BaseEvent):
    type: Literal["quote"] = "quote"
    quote: Quote


class OrderEvent(BaseEvent):
    type: Literal["order"] = "order"
    event: OrderEventType
    order: Order
    parent_order_id: str | None = None


class PositionEvent(BaseEvent):
    type: Literal["position"] = "position"
    event: PositionEventType
    position: Position


class AccountEvent(BaseEvent):
    type: Literal["account"] = "account"
    event: AccountEventType
    account: Account


class TradeEvent(BaseEvent):
    type: Literal["trade"] = "trade"
    event: TradeEventType
    trade: Trade


class ConnectionEvent(BaseEvent):
    type: Literal["connection"] = "connection"
    event: ConnectionEventType
    broker: str | None = None
    error: str | None = None
    gap_duration_ms: int | None = None


class DataStalenessEvent(BaseEvent):
    type: Literal["data_staleness"] = "data_staleness"
    event: DataStalenessEventType
    symbols: list[str] = Field(default_factory=list)
    broker: str | None = None
    stale_since: datetime | None = None


class RateLimitEvent(BaseEvent):
    type: Literal["rate_limit"] = "rate_limit"
    event: RateLimitEventType
    requests_remaining: int
    reset_at: datetime


class ErrorEvent(BaseEvent):
    type: Literal["error"] = "error"
    code: WsErrorCode
    message: str
    details: Any | None = None


def _get_event_type(data: Any) -> str:
    value = data.get("type", "") if isinstance(data, dict) else getattr(data, "type", "")
    return str(value) if value is not None else ""


GatewayEvent = Annotated[
    Annotated[PingEvent, Tag("ping")]
    | Annotated[CandleEvent, Tag("candle")]
    | Annotated[QuoteEvent, Tag("quote")]
    | Annotated[OrderEvent, Tag("order")]
    | Annotated[PositionEvent, Tag("position")]
    | Annotated[AccountEvent, Tag("account")]
    | Annotated[TradeEvent, Tag("trade")]
    | Annotated[ConnectionEvent, Tag("connection")]
    | Annotated[DataStalenessEvent, Tag("data_staleness")]
    | Annotated[RateLimitEvent, Tag("rate_limit")]
    | Annotated[ErrorEvent, Tag("error")],
    Discriminator(_get_event_type),
]

__all__ = [
    # Generated REST models
    "Account",
    "ApiError",
    "AssetClass",
    "Bar",
    "CancelAllResult",
    "CancelOrderResult",
    "Capabilities",
    "CircuitBreakerSnapshot",
    "CircuitBreakerStatusResponse",
    "ConnectionStatus",
    "DetailedHealthStatus",
    "ErrorCode",
    "HealthStatus",
    "MarginMode",
    "ModifyOrderResult",
    "Order",
    "OrderHandle",
    "OrderStatus",
    "OrderType",
    "OverallStatus",
    "Position",
    "PositionMode",
    "PositionSide",
    "ProviderHealth",
    "Quote",
    "RateLimits",
    "ReadyStatus",
    "RejectReason",
    "Side",
    "Timeframe",
    "TimeInForce",
    "Trade",
    "TradingPlatform",
    "TrailingType",
    "WsErrorCode",
    # Event type enums
    "AccountEventType",
    "ConnectionEventType",
    "DataStalenessEventType",
    "OrderEventType",
    "PositionEventType",
    "RateLimitEventType",
    "TradeEventType",
    # WebSocket event models
    # (``PingEvent`` is intentionally omitted — pings are intercepted by the
    # SDK and never yielded to users.)
    "BaseEvent",
    "CandleEvent",
    "QuoteEvent",
    "OrderEvent",
    "PositionEvent",
    "AccountEvent",
    "TradeEvent",
    "ConnectionEvent",
    "DataStalenessEvent",
    "RateLimitEvent",
    "ErrorEvent",
    "GatewayEvent",
]
