"""WebSocket event streaming with automatic reconnection.

The SDK sends an ``event_ack`` frame after the user's iterator body runs for
*every* yielded event. The Tektii backtest gateway uses auto-correlation —
any strategy ACK drains every engine event already broadcast — so the
strategy never has to track engine ``event_id`` values. ``event_id`` is
forwarded when the gateway includes it (engine path) and omitted otherwise
(live and mock backends, which do not register an ACK bridge and ignore the
frame). Same strategy code runs unchanged against any backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import queue
import random
import threading
import time
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import pydantic
import websockets
import websockets.asyncio.client
import websockets.exceptions
from pydantic import TypeAdapter

from tektii.models import BacktestCompleteEvent, GatewayEvent, PingEvent

# Hook fired once on a clean end-of-backtest terminal. The async stream
# accepts a coroutine function (awaited) or a plain callable; the sync stream
# accepts a plain callable, run on the iterating thread.
AsyncBacktestCompleteHook = Callable[[BacktestCompleteEvent], Awaitable[None] | None]
BacktestCompleteHook = Callable[[BacktestCompleteEvent], None]

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
    """Async WebSocket stream with auto-reconnect.

    Usage::

        stream = AsyncEventStream(ws_url="ws://localhost:8080/v1/ws")
        async with stream:
            async for event in stream:
                match event:
                    case CandleEvent(bar=bar):
                        ...

    Every yielded event is acknowledged *after* the user's iterator body
    runs. The Tektii backtest gateway uses auto-correlation (any strategy
    ACK drains every engine event already broadcast), so the strategy never
    has to track engine ``event_id`` values. ``event_id`` is forwarded when
    present and omitted otherwise; live and mock backends ignore the frame.
    Same strategy code, any backend.
    """

    def __init__(
        self,
        ws_url: str,
        api_key: str | None = None,
        reconnect: bool = True,
        max_reconnect_delay: float = 30.0,
        max_reconnect_attempts: int | None = None,
        on_backtest_complete: AsyncBacktestCompleteHook | None = None,
        *,
        _ack_on_yield: bool = True,
    ) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        # Internal: SyncEventStream disables inline ACK so the sync iterator
        # can own ACK timing across the thread boundary (the async-side ACK
        # would fire before the sync user's for-body runs). It owns the hook
        # too, for the same reason — running it on the iterating thread.
        self._ack_on_yield = _ack_on_yield
        self._on_backtest_complete = on_backtest_complete
        self._reconnect = reconnect
        self._max_reconnect_delay = max_reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._closed = False
        # Set when a clean end-of-backtest terminal is received, so the sync
        # wrapper can fire its hook on the iterating thread after the loop ends.
        self._terminal_event: BacktestCompleteEvent | None = None
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

    async def _ack(self, event_ids: list[str]) -> None:
        """Send an event_ack frame (internal).

        An empty ``event_ids`` list is valid: the Tektii backtest gateway
        drains every delivered engine event on any strategy ACK
        (auto-correlation), so the strategy does not need to track engine
        ids. Live and mock gateways have no ACK bridge and ignore the frame.
        """
        if self._ws is None:
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

    async def _try_flush_ack(self, event_ids: list[str]) -> None:
        """Best-effort ACK flush used on the per-event yield path.

        The websocket may already be dead — we log and swallow rather than
        letting a dropped trailing ACK bubble into the reconnect loop.
        """
        try:
            await self._ack(event_ids)
        except Exception as err:  # noqa: BLE001 — best-effort path
            logger.warning(
                "Failed to flush trailing ACK %s on disconnect: %s",
                event_ids,
                err,
            )

    async def _handle_terminal(self, event: BacktestCompleteEvent) -> None:
        """Process a clean end-of-backtest terminal.

        Records it (so the sync wrapper can find it after the loop ends),
        flushes an ACK immediately — a latency optimisation that lets the
        engine release its teardown without waiting out its ack-timeout;
        best-effort, so send failures are swallowed — and fires the optional
        hook once. The sync wrapper disables inline ACK (``_ack_on_yield=
        False``) and owns its hook timing, so the hook is skipped here for it
        and fired on the iterating thread instead.
        """
        self._terminal_event = event
        try:
            await self._ack([])
        except Exception as err:  # noqa: BLE001 — best-effort optimisation
            # Logged at debug, not warning: the flush-ACK is a latency
            # optimisation, not correctness — the engine tears down anyway
            # once its bounded ack-timeout elapses.
            logger.debug("Failed to flush end-of-backtest ACK: %s", err)
        if self._ack_on_yield and self._on_backtest_complete is not None:
            result = self._on_backtest_complete(event)
            if inspect.isawaitable(result):
                await result

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

                    # Clean end-of-backtest terminal — never yield to user.
                    # Handle it (ACK + hook), then exit the loop cleanly: a
                    # clean end is not a disconnect, so no error, no reconnect.
                    if isinstance(event, BacktestCompleteEvent):
                        await self._handle_terminal(event)
                        return

                    # Yield to the user, then ACK immediately after their
                    # body runs. ``finally`` guarantees the ACK fires on
                    # normal continuation, ``break``, or an exception in the
                    # user's loop body — so the backtest engine can safely
                    # block on ACK before sending the next event.
                    #
                    # The ACK fires regardless of ``event_id`` presence:
                    # the Tektii backtest gateway strips ``event_id`` from
                    # the wire format and uses auto-correlation, while
                    # live/mock backends ignore the frame entirely.
                    try:
                        yield event
                    finally:
                        if self._ack_on_yield:
                            ids = [event.event_id] if event.event_id is not None else []
                            await self._try_flush_ack(ids)

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

        stream = SyncEventStream(ws_url="ws://localhost:8080/v1/ws")
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
        reconnect: bool = True,
        max_reconnect_delay: float = 30.0,
        max_reconnect_attempts: int | None = None,
        on_backtest_complete: BacktestCompleteHook | None = None,
        close_timeout: float = 5.0,
    ) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._reconnect = reconnect
        self._max_reconnect_delay = max_reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._on_backtest_complete = on_backtest_complete
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
        loop = self._loop
        # ``is_running()`` is only True when the user stops iterating early
        # (the loop is still driving the stream). On a natural end — server
        # close or a clean end-of-backtest terminal — the background loop has
        # already exited and the async stream closed itself, so there is
        # nothing to schedule here; we just join the finished thread.
        #
        # When it *is* running, fire-and-forget the close: closing the socket
        # makes the pending recv raise, which ends ``_consume`` and returns
        # control from ``run_until_complete``, unwinding the thread. We must
        # NOT block on this coroutine's future — ``run_until_complete`` stops
        # the loop the instant ``_consume`` ends, which can abandon the
        # separately scheduled future before its done-callback resolves it,
        # surfacing as a spurious ``close_timeout`` wait and shutdown warning.
        # The thread join below is the real synchronisation point.
        if loop is not None and loop.is_running() and self._async_stream is not None:
            asyncio.run_coroutine_threadsafe(self._async_stream.close(), loop)
        if self._thread is not None:
            self._thread.join(timeout=self._close_timeout)
            if self._thread.is_alive():
                # The loop didn't unwind on its own — force it down, then
                # give the thread one more bounded chance to exit. This path
                # skips the graceful ``async with`` close, so a wedged loop may
                # leave the socket fd to be reclaimed by GC; acceptable for a
                # best-effort shutdown that has already waited a full timeout.
                if loop is not None:
                    loop.call_soon_threadsafe(loop.stop)
                self._thread.join(timeout=self._close_timeout)
                if self._thread.is_alive():
                    logger.warning(
                        "Background WebSocket thread did not exit within %.1fs",
                        self._close_timeout,
                    )
            self._thread = None
        self._loop = None

    def _ack(self, event_ids: list[str]) -> None:
        """Thread-safe wrapper around AsyncEventStream._ack (internal)."""
        if self._loop is None or self._async_stream is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._async_stream._ack(event_ids), self._loop)
        future.result(timeout=self._close_timeout)

    def _run_loop(self) -> None:
        """Run the async event loop in a background thread."""
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._consume())
        # Signal iterator to stop, carrying any clean end-of-backtest terminal
        # so the iterator can fire the hook on the iterating thread.
        terminal = self._async_stream._terminal_event if self._async_stream is not None else None
        self._queue.put(_Sentinel(terminal=terminal))

    async def _consume(self) -> None:
        """Consume async events and push to the sync queue.

        The inner ``AsyncEventStream`` has inline ACK disabled so the sync
        iterator can preserve the "ACK fires after user processes the event"
        guarantee across the thread boundary. The sync iterator ACKs from
        the main thread after each ``for`` body completes.
        """
        self._async_stream = AsyncEventStream(
            ws_url=self._ws_url,
            api_key=self._api_key,
            reconnect=self._reconnect,
            max_reconnect_delay=self._max_reconnect_delay,
            max_reconnect_attempts=self._max_reconnect_attempts,
            _ack_on_yield=False,  # sync iter owns ACK timing
        )
        try:
            async with self._async_stream:
                async for event in self._async_stream:
                    self._queue.put(event)
        except Exception as err:
            self._queue.put(_Sentinel(error=err))

    def __iter__(self) -> Iterator[GatewayEvent]:
        # ``None`` = no event yielded yet; ``list`` = the user's for-body has
        # run for an event and we owe an ACK. The list carries the engine's
        # ``event_id`` if present, or is empty for events without one (the
        # gateway's auto-correlation drains all delivered events on any ACK).
        pending_ack: list[str] | None = None
        try:
            while True:
                # ACK the previous event now that the user's for-body has run.
                # Must happen *before* we block on the queue: in backtest mode
                # the server will not send the next event until it sees the ACK.
                if pending_ack is not None:
                    try:
                        self._ack(pending_ack)
                    except Exception as err:  # noqa: BLE001 — best-effort ACK
                        logger.warning(
                            "Failed to flush pending ACK %s: %s",
                            pending_ack,
                            err,
                        )
                    pending_ack = None
                item = self._queue.get()
                if isinstance(item, _Sentinel):
                    if item.error:
                        raise item.error
                    # Fire the end-of-backtest hook once, on this (iterating)
                    # thread, before the loop returns. Absence is a no-op.
                    if item.terminal is not None and self._on_backtest_complete is not None:
                        self._on_backtest_complete(item.terminal)
                    return
                pending_ack = [item.event_id] if item.event_id is not None else []
                yield item
        finally:
            # Flush pending ACK on break, exception, or clean exit.
            if pending_ack is not None:
                try:
                    self._ack(pending_ack)
                except Exception as err:  # noqa: BLE001 — best-effort trailing ACK
                    logger.warning(
                        "Failed to flush trailing ACK %s on exit: %s",
                        pending_ack,
                        err,
                    )


class _Sentinel:
    """Signals the sync iterator to stop.

    ``terminal`` carries the clean end-of-backtest event (when the stream
    ended on a ``backtest_complete`` terminal) so the sync iterator can fire
    its hook on the iterating thread.
    """

    def __init__(
        self,
        error: Exception | None = None,
        terminal: BacktestCompleteEvent | None = None,
    ) -> None:
        self.error = error
        self.terminal = terminal

    def __repr__(self) -> str:
        return f"_Sentinel(error={self.error!r}, terminal={self.terminal!r})"
