"""WebSocket event streaming with auto-ACK and reconnection."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import queue
import random
import threading
import time
import warnings
from collections.abc import AsyncIterator, Iterator
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import pydantic
import websockets
import websockets.asyncio.client
import websockets.exceptions
from pydantic import TypeAdapter

from tektii.errors import TektiiError
from tektii.models import GatewayEvent, PingEvent

logger = logging.getLogger("tektii.stream")

_event_adapter: TypeAdapter[GatewayEvent] = TypeAdapter(GatewayEvent)

# Safety caps on the raw WebSocket connection. A trading event is at most a
# few KB; 256 KiB is plenty of headroom. Ping/pong keeps dead connections from
# hanging for minutes.
_WS_MAX_SIZE = 256 * 1024
_WS_PING_INTERVAL = 20.0
_WS_PING_TIMEOUT = 20.0
_WS_CLOSE_TIMEOUT = 5.0

# Bound exponential backoff so `2 ** attempt` never explodes on long outages.
_MAX_BACKOFF_EXPONENT = 8  # 2**8 = 256s pre-cap

_LOCALHOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})


def _warn_if_ws_credentials_over_plaintext(ws_url: str, api_key: str | None) -> None:
    """Warn if an API key is being sent to a remote host over plain ``ws://``.

    Mirrors the REST-side check in ``async_client._warn_if_credentials_over_plaintext``
    so a user that configures ``base_url="http://prod.example.com"`` sees the
    warning on both transports, not just the first REST call.
    """
    if api_key is None:
        return
    parts = urlsplit(ws_url)
    if parts.scheme != "ws":
        return
    host = (parts.hostname or "").lower()
    if host in _LOCALHOSTS:
        return
    warnings.warn(
        f"Sending API key to {ws_url!r} over plaintext ws://. "
        "Use wss:// for remote hosts to avoid leaking credentials.",
        UserWarning,
        stacklevel=3,
    )


def _is_retryable_handshake_error(err: BaseException) -> bool:
    """Return False for terminal WS handshake errors that must not be retried.

    Auth failures (401/403) during the WebSocket upgrade will never succeed
    on retry — short-circuiting prevents a CLI user with a bad key from
    spinning forever in exponential-backoff land.
    """
    status = getattr(err, "status_code", None)
    # websockets ≥14 raises InvalidStatus with a `.response` attribute.
    if status is None:
        response = getattr(err, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)
    if status is None:
        return True
    return status not in (401, 403)


class AsyncEventStream:
    """Async WebSocket stream with auto-reconnect and optional auto-ACK.

    Usage::

        stream = AsyncEventStream(ws_url="ws://localhost:8080/v1/ws", auto_ack=True)
        async with stream:
            async for event in stream:
                match event:
                    case CandleEvent(bar=bar):
                        ...

    When ``auto_ack=True``, event acknowledgements are sent *after* the user
    processes each event (i.e., after the ``async for`` body runs). This is
    critical for the Tektii backtest engine, which uses ACKs to advance
    simulation time.
    """

    def __init__(
        self,
        ws_url: str,
        api_key: str | None = None,
        auto_ack: bool = False,
        reconnect: bool = True,
        max_reconnect_delay: float = 30.0,
        max_reconnect_attempts: int | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._auto_ack = auto_ack
        self._reconnect = reconnect
        self._max_reconnect_delay = max_reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._closed = False
        _warn_if_ws_credentials_over_plaintext(ws_url, api_key)

    async def __aenter__(self) -> AsyncEventStream:
        await self._connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _connect(self) -> None:
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        self._ws = await websockets.asyncio.client.connect(
            self._ws_url,
            additional_headers=headers,
            max_size=_WS_MAX_SIZE,
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
            close_timeout=_WS_CLOSE_TIMEOUT,
        )

    async def close(self) -> None:
        """Close the WebSocket connection."""
        self._closed = True
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def ack(self, event_ids: list[str]) -> None:
        """Manually acknowledge events (for auto_ack=False mode)."""
        if not event_ids or self._ws is None:
            return
        msg = json.dumps(
            {
                "type": "event_ack",
                "correlation_id": str(uuid4()),
                "events_processed": event_ids,
                "timestamp": int(time.time() * 1000),
            }
        )
        await self._ws.send(msg)

    async def _try_flush_ack(self, event_id: str) -> None:
        """Best-effort ACK flush used on disconnect/close paths.

        The websocket may already be dead — we log and swallow rather than
        letting a dropped trailing ACK bubble into the reconnect loop.
        """
        try:
            await self.ack([event_id])
        except Exception as err:  # noqa: BLE001 — best-effort path
            logger.warning(
                "Failed to flush trailing ACK %s on disconnect: %s",
                event_id,
                err,
            )

    async def __aiter__(self) -> AsyncIterator[GatewayEvent]:
        attempt = 0

        while not self._closed:
            try:
                if self._ws is None:
                    await self._connect()

                attempt = 0  # Reset on successful connection
                assert self._ws is not None

                async for raw in self._ws:
                    # Tolerant parsing: a single malformed frame or unknown
                    # event type must not kill the iterator. Log and skip.
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError as err:
                        logger.warning(
                            "Dropping WebSocket frame with malformed JSON: %s",
                            err,
                        )
                        continue
                    try:
                        event = _event_adapter.validate_python(data)
                    except pydantic.ValidationError as err:
                        logger.warning(
                            "Dropping unrecognised WebSocket event (type=%r): %s",
                            data.get("type") if isinstance(data, dict) else None,
                            err,
                        )
                        continue

                    # Handle ping internally — never yield to user.
                    if isinstance(event, PingEvent):
                        with contextlib.suppress(Exception):
                            await self._ws.send(json.dumps({"type": "pong"}))
                        continue

                    # Yield to the user, then ACK immediately after their
                    # body runs. ``finally`` guarantees the ACK fires on
                    # normal continuation, ``break``, or an exception in the
                    # user's loop body — so the backtest engine can safely
                    # block on ACK before sending the next event.
                    try:
                        yield event
                    finally:
                        if self._auto_ack and event.event_id is not None:
                            await self._try_flush_ack(event.event_id)

                # Clean close — stop unless reconnect is enabled
                self._ws = None
                if not self._reconnect:
                    return

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError,
            ) as err:
                self._ws = None
                if self._closed or not self._reconnect:
                    return

                # Don't burn cycles on terminal handshake failures.
                if not _is_retryable_handshake_error(err):
                    logger.error(
                        "WebSocket handshake failed with non-retryable error: %s",
                        err,
                    )
                    raise

                attempt += 1
                if (
                    self._max_reconnect_attempts is not None
                    and attempt > self._max_reconnect_attempts
                ):
                    logger.error(
                        "WebSocket reconnect attempts exhausted (%d) — giving up",
                        self._max_reconnect_attempts,
                    )
                    raise

                # Exponential backoff with jitter, then cap. Bounding the
                # exponent keeps 2**attempt from exploding on long outages;
                # applying the cap *after* jitter prevents the documented
                # max from being silently exceeded by up to 50%.
                bounded_exp = min(attempt, _MAX_BACKOFF_EXPONENT)
                raw_delay = (2**bounded_exp) * (0.5 + random.random())
                delay = min(raw_delay, self._max_reconnect_delay)
                logger.warning(
                    "WebSocket disconnected (%s), reconnecting in %.1fs (attempt %d)",
                    err,
                    delay,
                    attempt,
                )
                await asyncio.sleep(delay)


class SyncEventStream:
    """Sync wrapper over AsyncEventStream using a background thread.

    Usage::

        stream = SyncEventStream(ws_url="ws://localhost:8080/v1/ws", auto_ack=True)
        with stream:
            for event in stream:
                match event:
                    case CandleEvent(bar=bar):
                        ...
    """

    def __init__(
        self,
        ws_url: str,
        api_key: str | None = None,
        auto_ack: bool = False,
        reconnect: bool = True,
        max_reconnect_delay: float = 30.0,
        max_reconnect_attempts: int | None = None,
        close_timeout: float = 5.0,
    ) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._auto_ack = auto_ack
        self._reconnect = reconnect
        self._max_reconnect_delay = max_reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._close_timeout = close_timeout

        self._queue: queue.Queue[GatewayEvent | _Sentinel] = queue.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._async_stream: AsyncEventStream | None = None

    def __enter__(self) -> SyncEventStream:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Stop the background event loop and close the WebSocket."""
        if self._loop is not None and self._async_stream is not None:
            future = asyncio.run_coroutine_threadsafe(self._async_stream.close(), self._loop)
            try:
                future.result(timeout=self._close_timeout)
            except Exception as err:  # noqa: BLE001 — best-effort close path
                logger.warning("Error closing async stream during shutdown: %s", err)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=self._close_timeout)
            if self._thread.is_alive():
                logger.warning(
                    "Background WebSocket thread did not exit within %.1fs",
                    self._close_timeout,
                )
            self._thread = None
        self._loop = None

    def ack(self, event_ids: list[str]) -> None:
        """Manually acknowledge events (thread-safe).

        Raises ``TektiiError`` if called outside of a ``with stream:`` block —
        a silent no-op here would deadlock the backtest engine, which won't
        advance time until it sees the ACK.
        """
        if self._loop is None or self._async_stream is None:
            raise TektiiError("SyncEventStream.ack() called outside of a 'with stream:' block")
        future = asyncio.run_coroutine_threadsafe(self._async_stream.ack(event_ids), self._loop)
        future.result(timeout=self._close_timeout)

    def _run_loop(self) -> None:
        """Run the async event loop in a background thread."""
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._consume())
        # Signal iterator to stop
        self._queue.put(_Sentinel())

    async def _consume(self) -> None:
        """Consume async events and push to the sync queue.

        The underlying ``AsyncEventStream`` runs with ``auto_ack=False`` so we
        can preserve the "ACK fires after user processes the event" guarantee
        across the thread boundary. The sync iterator handles ACKing from the
        main thread after each ``for`` body completes.
        """
        self._async_stream = AsyncEventStream(
            ws_url=self._ws_url,
            api_key=self._api_key,
            auto_ack=False,  # sync iter owns ACK timing
            reconnect=self._reconnect,
            max_reconnect_delay=self._max_reconnect_delay,
            max_reconnect_attempts=self._max_reconnect_attempts,
        )
        try:
            async with self._async_stream:
                async for event in self._async_stream:
                    self._queue.put(event)
        except Exception as err:
            self._queue.put(_Sentinel(error=err))

    def __iter__(self) -> Iterator[GatewayEvent]:
        pending_ack_id: str | None = None
        try:
            while True:
                # ACK the previous event now that the user's for-body has run.
                # Must happen *before* we block on the queue: in backtest mode
                # the server will not send the next event until it sees the ACK.
                if pending_ack_id is not None:
                    try:
                        self.ack([pending_ack_id])
                    except Exception as err:  # noqa: BLE001 — best-effort ACK
                        logger.warning(
                            "Failed to flush pending ACK %s: %s",
                            pending_ack_id,
                            err,
                        )
                    pending_ack_id = None
                item = self._queue.get()
                if isinstance(item, _Sentinel):
                    if item.error:
                        raise item.error
                    return
                if self._auto_ack and item.event_id is not None:
                    pending_ack_id = item.event_id
                yield item
        finally:
            # Flush pending ACK on break, exception, or clean exit.
            if pending_ack_id is not None:
                try:
                    self.ack([pending_ack_id])
                except Exception as err:  # noqa: BLE001 — best-effort trailing ACK
                    logger.warning(
                        "Failed to flush trailing ACK %s on exit: %s",
                        pending_ack_id,
                        err,
                    )


class _Sentinel:
    """Signals the sync iterator to stop."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def __repr__(self) -> str:
        return f"_Sentinel(error={self.error!r})"
