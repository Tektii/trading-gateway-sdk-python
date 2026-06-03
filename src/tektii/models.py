"""Public models for the Trading Gateway SDK.

REST response models are generated from the OpenAPI spec (see _generated/models.py).
WebSocket event models are hand-written here since they aren't in the OpenAPI spec.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag, model_validator

# ---------------------------------------------------------------------------
# Re-exports from generated models (REST API types)
# ---------------------------------------------------------------------------
# Also re-export event type enums for users who want to match on them
from tektii._generated.models import (
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
    event_id: str | None = None  # Present only from the backtest engine


class PingEvent(BaseEvent):
    type: Literal["ping"] = "ping"


class BacktestCompleteEvent(BaseEvent):
    """Terminal signalling a clean end-of-backtest.

    Sent by the Tektii backtest engine (via the gateway) when a backtest
    finishes replaying, immediately before the WebSocket Close frame. It marks
    a *clean* end — distinct from a ``ConnectionEvent`` carrying
    ``BROKER_DISCONNECTED``, which is a feed loss. The SDK intercepts this
    terminal, exits the run loop cleanly, and invokes the optional
    ``on_backtest_complete`` hook; it is never yielded to the user.

    The wire payload carries only ``broker`` and ``timestamp`` — a strategy
    that needs final equity should call ``get_account()`` from the hook.
    """

    type: Literal["backtest_complete"] = "backtest_complete"
    broker: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _tolerate_absent_timestamp(cls, data: Any) -> Any:
        """Default a missing ``timestamp`` to the Unix epoch.

        This terminal is load-bearing: it must always parse and drive a clean
        exit, never be dropped as "unrecognised" and fall through to the
        spurious-disconnect path. ``timestamp`` is required on every other
        event, so rather than weaken the shared :class:`BaseEvent` type we fill
        an absent value here (epoch = "unknown"). A present value is parsed
        normally.
        """
        if isinstance(data, dict) and data.get("timestamp") is None:
            return {**data, "timestamp": datetime(1970, 1, 1, tzinfo=UTC)}
        return data


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
    | Annotated[BacktestCompleteEvent, Tag("backtest_complete")]
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
    # SDK and never yielded to users. ``BacktestCompleteEvent`` is also
    # intercepted, but is exported because users reference it in their
    # ``on_backtest_complete`` hook signature.)
    "BaseEvent",
    "BacktestCompleteEvent",
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
