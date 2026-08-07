"""Async client for the Trading Gateway."""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

import httpx

from tektii._http import auth_headers, build_params, handle_response, http_to_ws_url
from tektii._version import __version__
from tektii.errors import APIConnectionError, APIStatusError, PositionUnprotectedError
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
    Timeframe,
    Trade,
)

if TYPE_CHECKING:
    from tektii.stream import AsyncBacktestCompleteHook, AsyncEventStream


_LOCALHOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})

# Env var names — documented in README and SECURITY.md. Keeping them as
# module-level constants so users grep easily. The TEKTII_-prefixed names are
# the platform's reserved namespace and win over the legacy names; both are
# read so existing deployments keep working through the rename.
ENV_API_KEY_PREFIXED = "TEKTII_TRADING_GATEWAY_API_KEY"
ENV_BASE_URL_PREFIXED = "TEKTII_TRADING_GATEWAY_URL"
ENV_API_KEY = "TRADING_GATEWAY_API_KEY"
ENV_BASE_URL = "TRADING_GATEWAY_URL"


def _env_base_url() -> str | None:
    return os.environ.get(ENV_BASE_URL_PREFIXED) or os.environ.get(ENV_BASE_URL)


def _env_api_key() -> str | None:
    return os.environ.get(ENV_API_KEY_PREFIXED) or os.environ.get(ENV_API_KEY)


# Default User-Agent identifies SDK traffic in gateway access logs and helps
# OSS adoption tracking. Overridable via the ``headers`` kwarg.
_DEFAULT_USER_AGENT = f"tektii-python/{__version__} httpx/{httpx.__version__}"

# Retries only run for idempotent methods. POST is deliberately excluded to
# avoid duplicate orders on transient network blips.
_RETRYABLE_METHODS = frozenset({"GET", "DELETE", "HEAD"})
_RETRYABLE_STATUSES = frozenset({502, 503, 504})
_STATUS_TOO_MANY_REQUESTS = 429
_STATUS_BAD_GATEWAY = 502


def _check_credentials_over_plaintext(
    base_url: str, api_key: str | None, *, allow_insecure: bool
) -> None:
    """Reject (or allow with opt-in) sending an API key over plaintext HTTP.

    Local development against ``http://localhost`` is intentional and silent.
    Any non-local plaintext HTTP target with an API key is almost certainly a
    misconfiguration that would leak the credential on the wire — this used
    to be a ``UserWarning`` but warnings can be filtered; for a financial
    credential the default is now hard-fail with an explicit ``allow_insecure``
    escape hatch for test doubles and private networks.
    """
    if api_key is None:
        return
    parts = urlsplit(base_url)
    if parts.scheme != "http":
        return
    host = (parts.hostname or "").lower()
    if host in _LOCALHOSTS:
        return
    if allow_insecure:
        return
    raise ValueError(
        f"Refusing to send API key to {base_url!r} over plaintext HTTP. "
        "Use https:// for remote hosts to avoid leaking credentials, or pass "
        "allow_insecure=True if you have audited the network path."
    )


def _require_aware_datetime(dt: datetime, *, field: str) -> datetime:
    """Reject naive datetimes — the gateway interprets wire timestamps as UTC
    but cannot tell whether a naive input was meant as local or UTC. Force
    the user to be explicit rather than silently drift by time-zone offset.
    """
    if dt.tzinfo is None:
        raise ValueError(
            f"{field} requires a timezone-aware datetime (got naive {dt!r}). "
            "Use datetime.now(tz=UTC) or dt.replace(tzinfo=UTC)."
        )
    return dt


class AsyncTradingGateway:
    """Async client for the Trading Gateway REST + WebSocket API.

    Args:
        base_url: Gateway base URL. Falls back to ``$TEKTII_TRADING_GATEWAY_URL``,
            then ``$TRADING_GATEWAY_URL``, then ``http://localhost:8080``.
        api_key: Bearer API key for authentication. Falls back to
            ``$TEKTII_TRADING_GATEWAY_API_KEY``, then ``$TRADING_GATEWAY_API_KEY``.
        timeout: HTTP request timeout as a float (applied to all phases) or
            an ``httpx.Timeout`` for granular connect/read/write/pool control.
        headers: Extra headers to merge with SDK defaults (User-Agent, auth).
            User-supplied headers win on collision.
        max_retries: Retry idempotent requests (GET/DELETE) on transient
            failures (httpx connect/read errors, 502/503/504). ``0`` disables.
        allow_insecure: Explicit opt-in to sending the API key over plain
            ``http://`` to a non-local host. Defaults to ``False`` (hard-fail).
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
        resolved_url = (base_url or _env_base_url() or "http://localhost:8080").rstrip("/")
        resolved_key = api_key if api_key is not None else _env_api_key()

        self.base_url = resolved_url
        self._api_key = resolved_key
        self._max_retries = max(0, max_retries)

        _check_credentials_over_plaintext(
            self.base_url, resolved_key, allow_insecure=allow_insecure
        )

        merged_headers: dict[str, str] = {
            "User-Agent": _DEFAULT_USER_AGENT,
            **auth_headers(resolved_key),
        }
        if headers:
            merged_headers.update(headers)

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=merged_headers,
            timeout=timeout,
        )

    def __repr__(self) -> str:
        redacted = "None" if self._api_key is None else "'***'"
        return f"{type(self).__name__}(base_url={self.base_url!r}, api_key={redacted})"

    async def __aenter__(self) -> AsyncTradingGateway:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    # -- Internal helpers --

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> httpx.Response:
        """Dispatch a request with SDK-level retry + transport-error wrapping.

        Retries only on idempotent methods (GET/DELETE/HEAD) and only for
        transport failures or 502/503/504. POST is never retried — retrying
        order submission could duplicate fills. 429 Retry-After is honoured.
        """
        retries = self._max_retries if method.upper() in _RETRYABLE_METHODS else 0
        attempt = 0
        while True:
            try:
                response = await self._client.request(method, path, params=params, json=json)
            except (httpx.TimeoutException, httpx.TransportError, httpx.NetworkError) as err:
                if attempt >= retries:
                    raise APIConnectionError(
                        f"{method} {path} failed after {attempt + 1} attempt(s): "
                        f"{type(err).__name__}: {err}"
                    ) from err
                attempt += 1
                await asyncio.sleep(_backoff(attempt))
                continue

            if response.status_code in _RETRYABLE_STATUSES and attempt < retries:
                attempt += 1
                await asyncio.sleep(_backoff(attempt))
                continue
            if response.status_code == _STATUS_TOO_MANY_REQUESTS and attempt < retries:
                # Honour Retry-After: seconds or HTTP-date. Fall back to backoff.
                wait_s = _parse_retry_after(response.headers.get("retry-after"))
                await asyncio.sleep(wait_s if wait_s is not None else _backoff(attempt + 1))
                attempt += 1
                continue
            return response

    async def _get(self, path: str, **params: Any) -> Any:
        resp = await self._request("GET", path, params=build_params(**params))
        return handle_response(resp)

    async def _post(self, path: str, json: Any = None) -> Any:
        resp = await self._request("POST", path, json=json)
        return handle_response(resp)

    async def _patch(self, path: str, json: Any = None) -> Any:
        resp = await self._request("PATCH", path, json=json)
        return handle_response(resp)

    async def _delete(self, path: str, *, json: Any = None, **params: Any) -> Any:
        resp = await self._request("DELETE", path, params=build_params(**params) or None, json=json)
        return handle_response(resp)

    @staticmethod
    def _str(value: str | Decimal | None) -> str | None:
        """Convert Decimal/str to string for JSON. None passthrough."""
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _seg(value: str) -> str:
        """Percent-encode a URL path segment.

        Symbols like ``BTC/USD`` and IDs with special characters must be
        encoded so they don't split the path or break the route.
        """
        return quote(value, safe="")

    @staticmethod
    def _isoformat(dt: datetime | None, *, field: str) -> str | None:
        if dt is None:
            return None
        return _require_aware_datetime(dt, field=field).isoformat()

    # -----------------------------------------------------------------------
    # Account
    # -----------------------------------------------------------------------

    async def get_account(self) -> Account:
        """Get account information (balance, equity, margin)."""
        data = await self._get("/v1/account")
        return Account.model_validate(data)

    # -----------------------------------------------------------------------
    # Orders
    # -----------------------------------------------------------------------

    async def submit_order(
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
        """Submit a new order.

        Args:
            symbol: Trading symbol (e.g., "AAPL", "EUR/USD").
            side: "buy" or "sell" (case-insensitive).
            quantity: Order quantity as string or Decimal.
            order_type: "market", "limit", "stop", "stop_limit", "trailing_stop".
        """
        body: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "order_type": order_type,
        }
        # Add optional fields only if set
        optionals: dict[str, Any] = {
            "time_in_force": time_in_force,
            "limit_price": self._str(limit_price),
            "stop_price": self._str(stop_price),
            "stop_loss": self._str(stop_loss),
            "take_profit": self._str(take_profit),
            "trailing_distance": self._str(trailing_distance),
            "trailing_type": trailing_type,
            "client_order_id": client_order_id,
            "oco_group_id": oco_group_id,
            "position_id": position_id,
            "reduce_only": reduce_only,
            "post_only": post_only,
            "hidden": hidden,
            "display_quantity": self._str(display_quantity),
            "leverage": self._str(leverage),
            "margin_mode": margin_mode,
        }
        body.update({k: v for k, v in optionals.items() if v is not None})

        data = await self._post("/v1/orders", json=body)
        return OrderHandle.model_validate(data)

    async def get_order(self, order_id: str) -> Order:
        """Get details of a specific order."""
        data = await self._get(f"/v1/orders/{self._seg(order_id)}")
        return Order.model_validate(data)

    async def list_orders(
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
        data = await self._get(
            "/v1/orders",
            symbol=symbol,
            status=",".join(status) if status else None,
            side=side,
            client_order_id=client_order_id,
            oco_group_id=oco_group_id,
            since=self._isoformat(since, field="since"),
            until=self._isoformat(until, field="until"),
            limit=limit,
        )
        return [Order.model_validate(item) for item in data]

    async def get_order_history(
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
        data = await self._get(
            "/v1/orders/history",
            symbol=symbol,
            status=",".join(status) if status else None,
            side=side,
            client_order_id=client_order_id,
            oco_group_id=oco_group_id,
            since=self._isoformat(since, field="since"),
            until=self._isoformat(until, field="until"),
            limit=limit,
        )
        return [Order.model_validate(item) for item in data]

    async def modify_order(
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
        body: dict[str, Any] = {}
        fields = {
            "quantity": quantity,
            "limit_price": limit_price,
            "stop_price": stop_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_distance": trailing_distance,
        }
        body.update({k: str(v) for k, v in fields.items() if v is not None})

        data = await self._patch(f"/v1/orders/{self._seg(order_id)}", json=body)
        return ModifyOrderResult.model_validate(data)

    async def cancel_order(self, order_id: str) -> CancelOrderResult:
        """Cancel a specific order."""
        data = await self._delete(f"/v1/orders/{self._seg(order_id)}")
        return CancelOrderResult.model_validate(data)

    async def cancel_all_orders(self, *, symbol: str | None = None) -> CancelAllResult:
        """Cancel all open orders, optionally filtered by symbol."""
        data = await self._delete("/v1/orders", symbol=symbol)
        return CancelAllResult.model_validate(data)

    # -----------------------------------------------------------------------
    # Positions
    # -----------------------------------------------------------------------

    async def list_positions(self, *, symbol: str | None = None) -> list[Position]:
        """List open positions."""
        data = await self._get("/v1/positions", symbol=symbol)
        return [Position.model_validate(item) for item in data]

    async def get_position(self, position_id: str) -> Position:
        """Get a specific position."""
        data = await self._get(f"/v1/positions/{self._seg(position_id)}")
        return Position.model_validate(data)

    async def close_position(
        self,
        position_id: str,
        *,
        quantity: str | Decimal | None = None,
        order_type: str | None = None,
        limit_price: str | Decimal | None = None,
        cancel_associated_orders: bool | None = None,
    ) -> OrderHandle:
        """Close a position (partially or fully)."""
        body: dict[str, Any] = {}
        if quantity is not None:
            body["quantity"] = str(quantity)
        if order_type is not None:
            body["order_type"] = order_type
        if limit_price is not None:
            body["limit_price"] = str(limit_price)
        if cancel_associated_orders is not None:
            body["cancel_associated_orders"] = cancel_associated_orders

        data = await self._delete(
            f"/v1/positions/{self._seg(position_id)}",
            json=body,
        )
        return OrderHandle.model_validate(data)

    async def modify_position_exits(
        self,
        position_id: str,
        *,
        stop_loss: str | Decimal | None = None,
        take_profit: str | Decimal | None = None,
    ) -> ModifyPositionExitsResult:
        """Move a position's resting stop-loss or take-profit.

        Use this — not ``modify_order`` — for the exit legs the gateway placed
        when the position's entry filled. ``modify_order`` addresses the
        provider directly and leaves the gateway's exit handler pointing at an
        order that may no longer exist.

        Omit a leg to leave it where it is. Only legs the gateway already
        tracks can be moved; this does not attach an exit to a position that
        has none.

        The returned ``order_ids`` for a leg can differ from the previous ones:
        where the provider cannot modify in place the gateway cancels and
        re-places the orders. **Never cache leg order ids** — read them from
        the result of each move. A leg whose entry filled in parts rests as
        several orders, and all of them move together.

        Legs are moved one at a time and are not rolled back as a set: if both
        are named and the second fails, the first stays at its new price and
        the call still raises. Re-read the position to see where each leg
        ended up.

        Args:
            position_id: The position whose exits should move.
            stop_loss: New stop-loss trigger price.
            take_profit: New take-profit trigger price.

        Raises:
            ValueError: If neither leg is given.
            ConflictError: 409 — the leg is not tracked, or the move was
                rejected and the original exit was restored. **The position is
                still protected.**
            PositionUnprotectedError: 502 — the exit was cancelled and could
                not be re-established. **The position is uncovered for that
                leg**, and the same condition arrives on the WebSocket as
                ``POSITION_UNPROTECTED``.
        """
        if stop_loss is None and take_profit is None:
            raise ValueError("modify_position_exits requires stop_loss, take_profit, or both")

        fields = {"stop_loss": stop_loss, "take_profit": take_profit}
        body = {k: str(v) for k, v in fields.items() if v is not None}

        try:
            data = await self._patch(f"/v1/positions/{self._seg(position_id)}", json=body)
        except APIStatusError as err:
            # 502 carries the "position is now uncovered" meaning only on this
            # endpoint. The shared status→exception map is global, so promoting
            # it here keeps a proxy's generic Bad Gateway on some other call
            # from masquerading as an unprotected position.
            if err.status_code == _STATUS_BAD_GATEWAY:
                raise PositionUnprotectedError(
                    err.status_code, err.code, err.message, err.details
                ) from err
            raise
        return ModifyPositionExitsResult.model_validate(data)

    async def close_all_positions(self, *, symbol: str | None = None) -> list[OrderHandle]:
        """Close all positions, optionally filtered by symbol."""
        data = await self._delete("/v1/positions", symbol=symbol)
        return [OrderHandle.model_validate(item) for item in data]

    # -----------------------------------------------------------------------
    # Market Data
    # -----------------------------------------------------------------------

    async def get_quote(self, symbol: str) -> Quote:
        """Get the current quote for a symbol."""
        data = await self._get(f"/v1/quotes/{self._seg(symbol)}")
        return Quote.model_validate(data)

    async def get_bars(
        self,
        symbol: str,
        timeframe: str | Timeframe,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Bar]:
        """Get historical OHLCV bars."""
        data = await self._get(
            f"/v1/bars/{self._seg(symbol)}",
            timeframe=str(timeframe),
            start=self._isoformat(start, field="start"),
            end=self._isoformat(end, field="end"),
            limit=limit,
        )
        return [Bar.model_validate(item) for item in data]

    # -----------------------------------------------------------------------
    # Sizing
    # -----------------------------------------------------------------------

    async def quantity_for_notional(
        self,
        symbol: str,
        *,
        notional: str | Decimal | None = None,
        equity_fraction: float | str | Decimal | None = None,
        price: str | Decimal | None = None,
    ) -> Decimal:
        """Size an order by target notional or fraction of account equity.

        An order ``quantity`` is a *fixed instrument amount*, not a share of
        capital — so a default like ``0.01`` BTC is a near-zero position on a
        six-figure account and produces a flat, meaningless backtest until you
        hand-size it to the instrument's price. This helper closes that gap:
        give it a notional or a fraction of equity and it returns the quantity
        to trade at the current price.

        Provide **exactly one** of:

        - ``notional``: target position value in the account currency
          (e.g. ``"5000"`` for $5,000 of exposure).
        - ``equity_fraction``: share of current account equity, where ``0.10``
          means 10%. This fetches :meth:`get_account` to read equity.

        The reference price is the quote **midpoint** ``(bid + ask) / 2``, read
        via :meth:`get_quote`. Pass ``price`` to supply your own reference
        (e.g. the latest bar close while handling a stream event) and skip the
        quote request entirely.

        Returns a :class:`~decimal.Decimal` ready to pass to
        :meth:`submit_order`; round it to your venue's lot size if required.
        Raises :class:`ValueError` if the target is under- or over-specified,
        or if the resolved price, notional, or fraction is not positive.

        Example::

            qty = await gw.quantity_for_notional("BTC/USD", equity_fraction=0.10)
            await gw.submit_order("BTC/USD", "buy", qty)
        """
        if (notional is None) == (equity_fraction is None):
            raise ValueError(
                "quantity_for_notional requires exactly one of `notional` or `equity_fraction`."
            )

        ref_price = (
            _to_decimal(price, field="price")
            if price is not None
            else await self._midpoint_price(symbol)
        )
        if ref_price <= 0:
            raise ValueError(f"Reference price must be positive (got {ref_price}).")

        if notional is not None:
            target = _to_decimal(notional, field="notional")
            if target <= 0:
                raise ValueError(f"notional must be positive (got {target}).")
        else:
            # Non-None by the exactly-one check above; assert narrows the type.
            assert equity_fraction is not None
            fraction = _to_decimal(equity_fraction, field="equity_fraction")
            if fraction <= 0:
                raise ValueError(f"equity_fraction must be positive (got {fraction}).")
            account = await self.get_account()
            # equity is a gateway-guaranteed decimal string; let a malformed
            # value surface as ArithmeticError rather than a "user input" error.
            target = Decimal(account.equity) * fraction

        return target / ref_price

    async def _midpoint_price(self, symbol: str) -> Decimal:
        """Current reference price as the quote midpoint ``(bid + ask) / 2``.

        ``bid``/``ask`` are gateway-guaranteed decimal strings, so they go
        straight to ``Decimal`` — a malformed value is a gateway contract
        violation that should raise loudly, not be reframed as user error.
        """
        quote = await self.get_quote(symbol)
        return (Decimal(quote.bid) + Decimal(quote.ask)) / 2

    # -----------------------------------------------------------------------
    # Trades
    # -----------------------------------------------------------------------

    async def list_trades(
        self,
        *,
        symbol: str | None = None,
        order_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[Trade]:
        """Get trade history."""
        data = await self._get(
            "/v1/trades",
            symbol=symbol,
            order_id=order_id,
            since=self._isoformat(since, field="since"),
            until=self._isoformat(until, field="until"),
            limit=limit,
        )
        return [Trade.model_validate(item) for item in data]

    # -----------------------------------------------------------------------
    # System
    # -----------------------------------------------------------------------

    async def get_capabilities(self) -> Capabilities:
        """Get provider capabilities (supported order types, asset classes, etc.)."""
        data = await self._get("/v1/capabilities")
        return Capabilities.model_validate(data)

    async def get_status(self) -> ConnectionStatus:
        """Get connection status to the broker."""
        data = await self._get("/v1/status")
        return ConnectionStatus.model_validate(data)

    async def get_health(self) -> DetailedHealthStatus:
        """Get detailed health status with per-provider information."""
        data = await self._get("/health")
        return DetailedHealthStatus.model_validate(data)

    async def get_circuit_breakers(self) -> CircuitBreakerStatusResponse:
        """Get circuit breaker status."""
        data = await self._get("/v1/circuit-breakers")
        return CircuitBreakerStatusResponse.model_validate(data)

    async def reset_circuit_breakers(self) -> CircuitBreakerStatusResponse:
        """Reset circuit breakers (5-minute cooldown between resets)."""
        data = await self._post("/v1/circuit-breakers/reset")
        return CircuitBreakerStatusResponse.model_validate(data)

    # -----------------------------------------------------------------------
    # WebSocket Streaming
    # -----------------------------------------------------------------------

    def stream(
        self, *, on_backtest_complete: AsyncBacktestCompleteHook | None = None
    ) -> AsyncEventStream:
        """Build an ``AsyncEventStream`` bound to this gateway.

        This is a synchronous factory — no I/O happens until the returned
        stream is entered::

            async with gw.stream() as events:
                async for event in events:
                    ...

        Args:
            on_backtest_complete: Optional hook fired exactly once when a
                backtest reaches a clean end (the engine's end-of-backtest
                terminal). Receives the
                :class:`~tektii.models.BacktestCompleteEvent` and may be a
                plain function or a coroutine function (which is awaited). The
                run loop then exits cleanly — no error, no reconnect. Useful
                for teardown; call :meth:`get_account` from inside it for the
                final equity. Against live/mock backends the terminal never
                arrives, so the hook simply never fires.
        """
        from tektii.stream import AsyncEventStream

        ws_url = http_to_ws_url(self.base_url) + "/v1/ws"
        return AsyncEventStream(
            ws_url=ws_url,
            api_key=self._api_key,
            on_backtest_complete=on_backtest_complete,
        )


def _to_decimal(value: str | float | Decimal, *, field: str) -> Decimal:
    """Coerce a user-supplied numeric value to ``Decimal``.

    Goes via ``str()`` so a float input doesn't carry binary-float noise —
    ``Decimal(0.1)`` is ``0.1000000000000000055...`` but ``Decimal("0.1")`` is
    exact. Raises ``ValueError`` (not the bare ``InvalidOperation``) so callers
    see a consistent, field-labelled error.
    """
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as err:
        raise ValueError(f"{field} is not a valid number: {value!r}") from err


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped at 8s."""
    base: float = float(min(2 ** max(0, attempt - 1), 8))
    return base * (0.5 + random.random())


def _parse_retry_after(value: str | None) -> float | None:
    """Parse Retry-After header (seconds-only form). Returns None on unknown."""
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
