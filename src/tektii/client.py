"""Sync client for the Trading Gateway.

Wraps :class:`AsyncTradingGateway` by owning a persistent background event
loop in a dedicated thread. All sync methods dispatch to the async client
via ``asyncio.run_coroutine_threadsafe`` and block until the result is
ready. Connections are reused across calls (httpx keeps the pool alive on
the background loop), so a sync strategy that polls the gateway does not
pay a TLS handshake per call.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import os
import threading
from collections.abc import Callable, Coroutine
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from tektii._http import http_to_ws_url
from tektii.async_client import (
    ENV_API_KEY,
    ENV_BASE_URL,
    AsyncTradingGateway,
    _check_credentials_over_plaintext,
)
from tektii.errors import TektiiError
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
    Order,
    OrderHandle,
    Position,
    Quote,
    Timeframe,
    Trade,
)

if TYPE_CHECKING:
    from tektii.stream import SyncEventStream

_T = TypeVar("_T")


class TradingGateway:
    """Sync client for the Trading Gateway REST + WebSocket API.

    Internally this holds a lazily-started background thread running its own
    ``asyncio`` event loop, through which a single :class:`AsyncTradingGateway`
    is shared across all method calls. Connection pooling is preserved
    end-to-end, so a polling strategy does not reopen a TLS connection per
    request.

    The sync client is **safe to call from plain scripts and notebooks** but
    not from inside an existing event loop (Jupyter ``%autoawait``, FastAPI
    startup hooks, async REPLs). If it detects a running loop on first call
    it raises :class:`TektiiError` telling you to use
    :class:`AsyncTradingGateway` instead.

    Args:
        base_url: Gateway base URL. Falls back to ``$TRADING_GATEWAY_URL``
            then ``http://localhost:8080``.
        api_key: Bearer API key. Falls back to ``$TRADING_GATEWAY_API_KEY``.
        timeout: HTTP timeout as float or ``httpx.Timeout``.
        headers: Extra headers to merge with SDK defaults.
        max_retries: Retry idempotent requests on transient failures.
        allow_insecure: Explicit opt-in to sending API key over plain HTTP.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float | httpx.Timeout = 30.0,
        *,
        headers: dict[str, str] | None = None,
        max_retries: int = 2,
        allow_insecure: bool = False,
    ) -> None:
        # Resolve config up front so the user sees env-var and plaintext-HTTP
        # errors at TradingGateway() construction time, not on first API call.
        self._base_url = (
            base_url or os.environ.get(ENV_BASE_URL) or "http://localhost:8080"
        ).rstrip("/")
        self._api_key = api_key if api_key is not None else os.environ.get(ENV_API_KEY)
        _check_credentials_over_plaintext(
            self._base_url, self._api_key, allow_insecure=allow_insecure
        )

        # Everything else is pass-through to AsyncTradingGateway on first call.
        # Store the already-resolved values so the background thread doesn't
        # re-read env vars at a different time (which would be confusing
        # behaviour if os.environ changed between __init__ and first call).
        self._config: dict[str, Any] = {
            "base_url": self._base_url,
            "api_key": self._api_key,
            "timeout": timeout,
            "headers": headers,
            "max_retries": max_retries,
            "allow_insecure": True,  # already checked above
        }

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._async_gw: AsyncTradingGateway | None = None
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        redacted = "None" if self._api_key is None else "'***'"
        return f"{type(self).__name__}(base_url={self._base_url!r}, api_key={redacted})"

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def __enter__(self) -> TradingGateway:
        self._ensure_started()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _ensure_started(self) -> None:
        """Lazily start the background loop + async client on first use."""
        with self._lock:
            if self._loop is not None:
                return

            # Refuse to run inside an existing event loop — ``asyncio.run``-
            # based sync wrappers are the #1 Jupyter / FastAPI foot-gun.
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise TektiiError(
                    "TradingGateway (sync) cannot be used from inside a running "
                    "event loop. Use AsyncTradingGateway and 'await' its methods "
                    "instead."
                )

            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _run() -> None:
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()

            thread = threading.Thread(target=_run, name="tektii-sync-loop", daemon=True)
            thread.start()
            ready.wait()

            async def _build() -> AsyncTradingGateway:
                return AsyncTradingGateway(**self._config)

            self._async_gw = asyncio.run_coroutine_threadsafe(_build(), loop).result(timeout=5.0)
            self._loop = loop
            self._thread = thread

    def close(self) -> None:
        """Shut down the background loop and close the shared async client."""
        with self._lock:
            loop = self._loop
            thread = self._thread
            async_gw = self._async_gw
            self._loop = None
            self._thread = None
            self._async_gw = None

        if loop is None or async_gw is None:
            return
        with contextlib.suppress(Exception):
            asyncio.run_coroutine_threadsafe(async_gw.close(), loop).result(timeout=5.0)
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5.0)

    def _run(self, fn: Callable[[AsyncTradingGateway], Coroutine[Any, Any, _T]]) -> _T:
        """Run an async gateway call synchronously on the background loop."""
        self._ensure_started()
        assert self._loop is not None
        assert self._async_gw is not None
        future: concurrent.futures.Future[_T] = asyncio.run_coroutine_threadsafe(
            fn(self._async_gw), self._loop
        )
        return future.result()

    # -----------------------------------------------------------------------
    # Account
    # -----------------------------------------------------------------------

    def get_account(self) -> Account:
        """Get account information (balance, equity, margin)."""
        return self._run(lambda gw: gw.get_account())

    # -----------------------------------------------------------------------
    # Orders
    # -----------------------------------------------------------------------

    def submit_order(
        self,
        symbol: str,
        side: str,
        quantity: str | Decimal,
        order_type: str = "market",
        *,
        time_in_force: str | None = None,
        limit_price: str | Decimal | None = None,
        stop_price: str | Decimal | None = None,
        stop_loss: str | Decimal | None = None,
        take_profit: str | Decimal | None = None,
        trailing_distance: str | Decimal | None = None,
        trailing_type: str | None = None,
        client_order_id: str | None = None,
        oco_group_id: str | None = None,
        position_id: str | None = None,
        reduce_only: bool | None = None,
        post_only: bool | None = None,
        hidden: bool | None = None,
        display_quantity: str | Decimal | None = None,
        leverage: str | Decimal | None = None,
        margin_mode: str | None = None,
    ) -> OrderHandle:
        """Submit a new order."""
        return self._run(
            lambda gw: gw.submit_order(
                symbol,
                side,
                quantity,
                order_type,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=stop_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                trailing_distance=trailing_distance,
                trailing_type=trailing_type,
                client_order_id=client_order_id,
                oco_group_id=oco_group_id,
                position_id=position_id,
                reduce_only=reduce_only,
                post_only=post_only,
                hidden=hidden,
                display_quantity=display_quantity,
                leverage=leverage,
                margin_mode=margin_mode,
            )
        )

    def get_order(self, order_id: str) -> Order:
        """Get details of a specific order."""
        return self._run(lambda gw: gw.get_order(order_id))

    def list_orders(
        self,
        *,
        symbol: str | None = None,
        status: list[str] | None = None,
        side: str | None = None,
        client_order_id: str | None = None,
        oco_group_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[Order]:
        """List open orders, with optional filters."""
        return self._run(
            lambda gw: gw.list_orders(
                symbol=symbol,
                status=status,
                side=side,
                client_order_id=client_order_id,
                oco_group_id=oco_group_id,
                since=since,
                until=until,
                limit=limit,
            )
        )

    def get_order_history(
        self,
        *,
        symbol: str | None = None,
        status: list[str] | None = None,
        side: str | None = None,
        client_order_id: str | None = None,
        oco_group_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[Order]:
        """Get historical orders (filled, cancelled, rejected)."""
        return self._run(
            lambda gw: gw.get_order_history(
                symbol=symbol,
                status=status,
                side=side,
                client_order_id=client_order_id,
                oco_group_id=oco_group_id,
                since=since,
                until=until,
                limit=limit,
            )
        )

    def modify_order(
        self,
        order_id: str,
        *,
        quantity: str | Decimal | None = None,
        limit_price: str | Decimal | None = None,
        stop_price: str | Decimal | None = None,
        stop_loss: str | Decimal | None = None,
        take_profit: str | Decimal | None = None,
        trailing_distance: str | Decimal | None = None,
    ) -> ModifyOrderResult:
        """Modify an existing order."""
        return self._run(
            lambda gw: gw.modify_order(
                order_id,
                quantity=quantity,
                limit_price=limit_price,
                stop_price=stop_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                trailing_distance=trailing_distance,
            )
        )

    def cancel_order(self, order_id: str) -> CancelOrderResult:
        """Cancel a specific order."""
        return self._run(lambda gw: gw.cancel_order(order_id))

    def cancel_all_orders(self, *, symbol: str | None = None) -> CancelAllResult:
        """Cancel all open orders, optionally filtered by symbol."""
        return self._run(lambda gw: gw.cancel_all_orders(symbol=symbol))

    # -----------------------------------------------------------------------
    # Positions
    # -----------------------------------------------------------------------

    def list_positions(self, *, symbol: str | None = None) -> list[Position]:
        """List open positions."""
        return self._run(lambda gw: gw.list_positions(symbol=symbol))

    def get_position(self, position_id: str) -> Position:
        """Get a specific position."""
        return self._run(lambda gw: gw.get_position(position_id))

    def close_position(
        self,
        position_id: str,
        *,
        quantity: str | Decimal | None = None,
        order_type: str | None = None,
        limit_price: str | Decimal | None = None,
        cancel_associated_orders: bool | None = None,
    ) -> OrderHandle:
        """Close a position (partially or fully)."""
        return self._run(
            lambda gw: gw.close_position(
                position_id,
                quantity=quantity,
                order_type=order_type,
                limit_price=limit_price,
                cancel_associated_orders=cancel_associated_orders,
            )
        )

    def close_all_positions(self, *, symbol: str | None = None) -> list[OrderHandle]:
        """Close all positions, optionally filtered by symbol."""
        return self._run(lambda gw: gw.close_all_positions(symbol=symbol))

    # -----------------------------------------------------------------------
    # Market Data
    # -----------------------------------------------------------------------

    def get_quote(self, symbol: str) -> Quote:
        """Get the current quote for a symbol."""
        return self._run(lambda gw: gw.get_quote(symbol))

    def get_bars(
        self,
        symbol: str,
        timeframe: str | Timeframe,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Bar]:
        """Get historical OHLCV bars."""
        return self._run(
            lambda gw: gw.get_bars(symbol, timeframe, start=start, end=end, limit=limit)
        )

    # -----------------------------------------------------------------------
    # Trades
    # -----------------------------------------------------------------------

    def list_trades(
        self,
        *,
        symbol: str | None = None,
        order_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[Trade]:
        """Get trade history."""
        return self._run(
            lambda gw: gw.list_trades(
                symbol=symbol,
                order_id=order_id,
                since=since,
                until=until,
                limit=limit,
            )
        )

    # -----------------------------------------------------------------------
    # System
    # -----------------------------------------------------------------------

    def get_capabilities(self) -> Capabilities:
        """Get provider capabilities."""
        return self._run(lambda gw: gw.get_capabilities())

    def get_status(self) -> ConnectionStatus:
        """Get connection status to the broker."""
        return self._run(lambda gw: gw.get_status())

    def get_health(self) -> DetailedHealthStatus:
        """Get detailed health status."""
        return self._run(lambda gw: gw.get_health())

    def get_circuit_breakers(self) -> CircuitBreakerStatusResponse:
        """Get circuit breaker status."""
        return self._run(lambda gw: gw.get_circuit_breakers())

    def reset_circuit_breakers(self) -> CircuitBreakerStatusResponse:
        """Reset circuit breakers."""
        return self._run(lambda gw: gw.reset_circuit_breakers())

    # -----------------------------------------------------------------------
    # WebSocket Streaming
    # -----------------------------------------------------------------------

    def stream(self) -> SyncEventStream:
        """Connect to the gateway WebSocket and stream events.

        Returns a ``SyncEventStream`` (context manager + iterator)::

            with gw.stream() as events:
                for event in events:
                    ...
        """
        from tektii.stream import SyncEventStream

        ws_url = http_to_ws_url(self._base_url) + "/v1/ws"
        return SyncEventStream(
            ws_url=ws_url,
            api_key=self._api_key,
        )
