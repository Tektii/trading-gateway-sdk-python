"""Tests for WebSocket streaming — event parsing, auto-ACK, ping/pong."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import threading
import time
from datetime import UTC, datetime

import pytest
import websockets
import websockets.asyncio.server

from tektii.models import BacktestCompleteEvent, CandleEvent, ErrorEvent, OrderEvent
from tektii.stream import AsyncEventStream, SyncEventStream


async def _run_ws_server(handler, port: int = 0):
    """Start a WebSocket server on a random port, return (server, url)."""
    server = await websockets.asyncio.server.serve(handler, "127.0.0.1", port)
    host, actual_port = list(server.sockets)[0].getsockname()
    url = f"ws://{host}:{actual_port}"
    return server, url


# ---------------------------------------------------------------------------
# Event parsing
# ---------------------------------------------------------------------------


async def test_candle_event_parsed() -> None:
    candle_msg = json.dumps(
        {
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
    )

    async def handler(ws):
        await ws.send(candle_msg)
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1
    assert isinstance(events[0], CandleEvent)
    assert events[0].bar.symbol == "AAPL"


async def test_order_event_parsed() -> None:
    order_msg = json.dumps(
        {
            "type": "order",
            "event": "ORDER_FILLED",
            "timestamp": "2025-01-15T10:30:00Z",
            "order": {
                "id": "ord_1",
                "symbol": "AAPL",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": "1",
                "filled_quantity": "1",
                "remaining_quantity": "0",
                "status": "FILLED",
                "time_in_force": "GTC",
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-01-15T10:30:01Z",
            },
        }
    )

    async def handler(ws):
        await ws.send(order_msg)
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1
    assert isinstance(events[0], OrderEvent)
    assert events[0].event == "ORDER_FILLED"


# ---------------------------------------------------------------------------
# Ping/pong — pings should be handled internally, not yielded
# ---------------------------------------------------------------------------


async def test_ping_handled_internally() -> None:
    """Ping messages should not be yielded; pong should be sent back."""
    pong_received = asyncio.Event()

    async def handler(ws):
        # Send a ping
        await ws.send(json.dumps({"type": "ping", "timestamp": "2025-01-15T10:30:00Z"}))
        # Wait for pong
        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("type") == "pong":
            pong_received.set()
        # Send a real event then close
        await ws.send(
            json.dumps(
                {
                    "type": "error",
                    "code": "INTERNAL_ERROR",
                    "message": "test",
                    "timestamp": "2025-01-15T10:30:00Z",
                }
            )
        )
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    # Ping should NOT appear in events
    assert len(events) == 1
    assert isinstance(events[0], ErrorEvent)
    # But pong should have been sent
    assert pong_received.is_set()


# ---------------------------------------------------------------------------
# Auto-ACK
# ---------------------------------------------------------------------------


async def test_auto_ack_sends_after_processing() -> None:
    """Events carrying an event_id should be ACKed after the user processes them."""
    received_acks: list[dict] = []

    async def handler(ws):
        # Send event with event_id
        await ws.send(
            json.dumps(
                {
                    "type": "candle",
                    "timestamp": "2025-01-15T10:30:00Z",
                    "event_id": "evt_001",
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
            )
        )
        # Send a second event to trigger the ACK for the first
        await ws.send(
            json.dumps(
                {
                    "type": "candle",
                    "timestamp": "2025-01-15T10:30:01Z",
                    "event_id": "evt_002",
                    "bar": {
                        "symbol": "AAPL",
                        "provider": "tektii",
                        "timeframe": "1m",
                        "timestamp": "2025-01-15T10:30:01Z",
                        "open": "150.25",
                        "high": "150.75",
                        "low": "150.10",
                        "close": "150.50",
                        "volume": "800",
                    },
                }
            )
        )

        # Collect ACK messages
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                msg = json.loads(raw)
                if msg.get("type") == "event_ack":
                    received_acks.append(msg)
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
                if len(events) >= 2:
                    await stream.close()
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 2
    # At least the first event should have been ACK'd
    assert len(received_acks) >= 1
    assert received_acks[0]["events_processed"] == ["evt_001"]


async def test_auto_ack_unblocks_backtest_engine() -> None:
    """Regression: the auto-ACK must fire *immediately* after the user's
    ``async for`` body runs, not when the next raw message arrives.

    The Tektii backtest engine waits for ACK_N before sending event N+1.
    If the SDK defers the ACK until event N+1 arrives, both sides deadlock.
    """
    order = []

    async def handler(ws):
        # Send event 1 with event_id
        await ws.send(
            json.dumps(
                {
                    "type": "candle",
                    "timestamp": "2025-01-15T10:30:00Z",
                    "event_id": "evt_bt_001",
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
            )
        )
        # Block until we see the ACK for event 1. This is the backtest
        # engine's "don't advance time until strategy acknowledges" contract.
        raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
        msg = json.loads(raw)
        assert msg.get("type") == "event_ack"
        assert msg.get("events_processed") == ["evt_bt_001"]
        order.append("ack_received")
        # Now send event 2
        await ws.send(
            json.dumps(
                {
                    "type": "candle",
                    "timestamp": "2025-01-15T10:30:01Z",
                    "event_id": "evt_bt_002",
                    "bar": {
                        "symbol": "AAPL",
                        "provider": "tektii",
                        "timeframe": "1m",
                        "timestamp": "2025-01-15T10:30:01Z",
                        "open": "150.25",
                        "high": "150.75",
                        "low": "150.10",
                        "close": "150.50",
                        "volume": "800",
                    },
                }
            )
        )
        # Wait for ACK 2 then close
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ws.recv(), timeout=3.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
                order.append(f"processed_{event.event_id}")
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 2
    # Critical ordering: event 1 processed → ack received → event 2 processed.
    assert order == ["processed_evt_bt_001", "ack_received", "processed_evt_bt_002"]


async def test_ack_without_event_id_one_per_yielded_event() -> None:
    """Each yielded event without ``event_id`` must produce its own ACK frame."""
    received_acks: list[dict] = []

    async def handler(ws):
        for i in range(3):
            await ws.send(
                json.dumps(
                    {
                        "type": "candle",
                        "timestamp": f"2025-01-15T10:30:0{i}Z",
                        "bar": {
                            "symbol": "AAPL",
                            "provider": "mock",
                            "timeframe": "1m",
                            "timestamp": f"2025-01-15T10:30:0{i}Z",
                            "open": "150.00",
                            "high": "150.50",
                            "low": "149.90",
                            "close": str(150 + i),
                            "volume": "1000",
                        },
                    }
                )
            )
        try:
            while len(received_acks) < 3:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                msg = json.loads(raw)
                if msg.get("type") == "event_ack":
                    received_acks.append(msg)
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 3
    assert len(received_acks) == 3
    for ack in received_acks:
        assert ack["events_processed"] == []


async def test_ack_without_event_id_sends_empty_list() -> None:
    """Events without event_id (gateway sidecar / live / mock) must still
    trigger an ACK frame with ``events_processed: []``.

    The Tektii backtest gateway strips ``event_id`` from the wire-format
    ``WsMessage`` and relies on auto-correlation: any strategy ACK drains
    every engine event already broadcast. Live/mock gateways ignore the
    frame because they do not register an ACK bridge. Either way, the SDK
    sends one ACK per yielded event.
    """
    received_acks: list[dict] = []

    async def handler(ws):
        # Send event WITHOUT event_id
        await ws.send(
            json.dumps(
                {
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
            )
        )
        # Wait for the ACK, then close server-side so the trailing ACK
        # path runs on the client (rather than the user breaking out, which
        # would short-circuit close before the iterator resumes).
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            msg = json.loads(raw)
            if msg.get("type") == "event_ack":
                received_acks.append(msg)
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1
    assert len(received_acks) == 1
    assert received_acks[0]["events_processed"] == []


# ---------------------------------------------------------------------------
# Multiple events in sequence
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# SyncEventStream — background-thread wrapper, ACK timing, manual ACK
# ---------------------------------------------------------------------------


def _sample_candle(event_id: str, close: str = "150.25") -> dict:
    return {
        "type": "candle",
        "timestamp": "2025-01-15T10:30:00Z",
        "event_id": event_id,
        "bar": {
            "symbol": "AAPL",
            "provider": "tektii",
            "timeframe": "1m",
            "timestamp": "2025-01-15T10:30:00Z",
            "open": "150.00",
            "high": "150.50",
            "low": "149.90",
            "close": close,
            "volume": "1000",
        },
    }


async def test_sync_stream_auto_ack_after_processing() -> None:
    """Regression: SyncEventStream must ACK *after* the user's for-body runs,
    not before.

    Previously the background consumer auto-ACKed on the async side, firing
    the ACK before the sync user had processed the event — breaking the
    backtest engine's "advance time on ACK" contract.
    """
    received_acks: list[dict] = []

    async def handler(ws):
        await ws.send(json.dumps(_sample_candle("evt_sync_001")))
        # Wait for the ACK, then send a second event so the ACK isn't lost
        # to connection close.
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                msg = json.loads(raw)
                if msg.get("type") == "event_ack":
                    received_acks.append(msg)
                    break
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
        await ws.send(json.dumps(_sample_candle("evt_sync_002", close="150.50")))
        # Let client close
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ws.wait_closed(), timeout=2.0)

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> list[CandleEvent]:
        stream = SyncEventStream(ws_url=url, reconnect=False)
        collected: list[CandleEvent] = []
        with stream:
            for event in stream:
                collected.append(event)  # type: ignore[arg-type]
                if len(collected) >= 2:
                    break
        return collected

    try:
        events = await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 2
    assert len(received_acks) >= 1
    assert received_acks[0]["events_processed"] == ["evt_sync_001"]


async def test_sync_stream_ack_without_event_id_sends_empty_list() -> None:
    """SyncEventStream must ACK every yielded event, including those without
    ``event_id`` — same contract as the async path. The frame carries an
    empty ``events_processed`` list when ``event_id`` is absent.
    """
    received_messages: list[dict] = []

    async def handler(ws):
        await ws.send(
            json.dumps(
                {
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
            )
        )
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            received_messages.append(json.loads(raw))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ws.wait_closed(), timeout=1.0)

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> int:
        stream = SyncEventStream(ws_url=url, reconnect=False)
        count = 0
        with stream:
            for _event in stream:
                count += 1
                break
        return count

    try:
        count = await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()

    assert count == 1
    acks = [m for m in received_messages if m.get("type") == "event_ack"]
    assert len(acks) == 1
    assert acks[0]["events_processed"] == []


async def test_trailing_ack_flushed_on_clean_close() -> None:
    """Regression: when the server closes cleanly with a pending ACK, the
    client should attempt to flush it instead of letting the failed send
    bubble into the reconnect loop.
    """
    received_acks: list[dict] = []

    async def handler(ws):
        await ws.send(json.dumps(_sample_candle("evt_trail_001")))
        # Give the client a chance to send the trailing ACK before we close.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            msg = json.loads(raw)
            if msg.get("type") == "event_ack":
                received_acks.append(msg)
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
                # Don't break — let server drive the close so the trailing
                # ACK branch runs.
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 1
    # The trailing ACK should have been sent before the server closed.
    assert len(received_acks) == 1
    assert received_acks[0]["events_processed"] == ["evt_trail_001"]


async def test_trailing_ack_flush_survives_dead_socket() -> None:
    """Regression: if the socket is already dead when we try to flush the
    trailing ACK, the failure must be swallowed, not raised.
    """

    async def handler(ws):
        await ws.send(json.dumps(_sample_candle("evt_dead_001")))
        # Slam the connection shut without waiting for an ACK.
        await ws.close(code=1011, reason="server went away")

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    # The single event should have been delivered; no exception should have
    # escaped the iterator even though the trailing ACK could not be sent.
    assert len(events) == 1


# ---------------------------------------------------------------------------
# Reconnect logic — exponential backoff, recovery, attempt cap
# ---------------------------------------------------------------------------


async def test_reconnect_after_initial_disconnect(monkeypatch) -> None:
    """The stream should reconnect after a dropped connection and resume
    yielding events.
    """
    # Minimise jitter so the test doesn't linger.
    monkeypatch.setattr(random, "random", lambda: 0.0)

    connection_count = {"n": 0}

    async def handler(ws):
        connection_count["n"] += 1
        i = connection_count["n"]
        await ws.send(
            json.dumps(
                {
                    "type": "candle",
                    "timestamp": f"2025-01-15T10:3{i}:00Z",
                    "bar": {
                        "symbol": "AAPL",
                        "provider": "mock",
                        "timeframe": "1m",
                        "timestamp": f"2025-01-15T10:3{i}:00Z",
                        "open": "150.00",
                        "high": "150.50",
                        "low": "149.90",
                        "close": str(150 + i),
                        "volume": "1000",
                    },
                }
            )
        )
        if i == 1:
            # Abruptly drop the first connection; client should reconnect.
            await ws.close(code=1011, reason="server bounce")

    server, url = await _run_ws_server(handler)
    events: list[CandleEvent] = []

    async def collect() -> None:
        stream = AsyncEventStream(ws_url=url, reconnect=True, max_reconnect_delay=0.2)
        async with stream:
            async for event in stream:
                events.append(event)  # type: ignore[arg-type]
                if len(events) >= 2:
                    await stream.close()

    try:
        await asyncio.wait_for(collect(), timeout=10.0)
    finally:
        server.close()
        await server.wait_closed()

    assert connection_count["n"] >= 2
    assert len(events) >= 2
    # Confirm we got events from two distinct connections
    closes = {e.bar.close for e in events}
    assert len(closes) >= 2


async def test_reconnect_disabled_returns_on_disconnect() -> None:
    """With ``reconnect=False``, a dropped connection must exit the iterator
    rather than reconnect.
    """

    async def handler(ws):
        await ws.send(
            json.dumps(
                {
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
            )
        )
        await ws.close(code=1011, reason="no reconnect")

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    # Single connection, single event, no reconnect
    assert len(events) == 1


async def test_multiple_events() -> None:
    """Stream should yield multiple events in order."""

    async def handler(ws):
        for i in range(3):
            await ws.send(
                json.dumps(
                    {
                        "type": "candle",
                        "timestamp": f"2025-01-15T10:30:0{i}Z",
                        "bar": {
                            "symbol": "AAPL",
                            "provider": "mock",
                            "timeframe": "1m",
                            "timestamp": f"2025-01-15T10:30:0{i}Z",
                            "open": "150.00",
                            "high": "150.50",
                            "low": "149.90",
                            "close": str(150 + i),
                            "volume": "1000",
                        },
                    }
                )
            )
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)
    finally:
        server.close()
        await server.wait_closed()

    assert len(events) == 3
    assert all(isinstance(e, CandleEvent) for e in events)


# ---------------------------------------------------------------------------
# End-of-backtest terminal — clean exit, flush-ACK, optional hook
# ---------------------------------------------------------------------------


def _backtest_complete(broker: str = "tektii") -> dict:
    return {
        "type": "backtest_complete",
        "broker": broker,
        "timestamp": "2025-01-15T10:31:00Z",
    }


async def test_backtest_complete_exits_cleanly_without_yielding() -> None:
    """A clean end-of-backtest terminal ends the loop without yielding the
    terminal as an event and without raising.
    """

    async def handler(ws):
        await ws.send(json.dumps(_sample_candle("evt_eob_1")))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)  # candle ACK
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)  # terminal ACK
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []

    async def collect() -> None:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for event in stream:
                events.append(event)

    try:
        await asyncio.wait_for(collect(), timeout=5.0)
    finally:
        server.close()
        await server.wait_closed()

    # Only the candle is yielded — the terminal is intercepted, not yielded.
    assert len(events) == 1
    assert isinstance(events[0], CandleEvent)


async def test_backtest_complete_does_not_reconnect_even_when_enabled() -> None:
    """The terminal must short-circuit the reconnect loop: a clean end is not
    a disconnect, so the SDK must not re-dial even with ``reconnect=True``.
    """
    connection_count = {"n": 0}

    async def handler(ws):
        connection_count["n"] += 1
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)  # terminal ACK
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []

    async def collect() -> None:
        stream = AsyncEventStream(ws_url=url, reconnect=True, max_reconnect_delay=0.1)
        async with stream:
            async for event in stream:
                events.append(event)

    try:
        await asyncio.wait_for(collect(), timeout=5.0)
        # Give any erroneous reconnect attempt time to re-dial.
        await asyncio.sleep(0.3)
    finally:
        server.close()
        await server.wait_closed()

    assert events == []
    assert connection_count["n"] == 1


async def test_backtest_complete_sends_flush_ack() -> None:
    """The SDK sends a flush-ACK (``event_ack`` with an empty list) immediately
    on terminal receipt — the latency optimisation that releases engine teardown.
    """
    received: list[dict] = []

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            received.append(json.loads(raw))
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()

    acks = [m for m in received if m.get("type") == "event_ack"]
    assert len(acks) == 1
    assert acks[0]["events_processed"] == []


async def test_backtest_complete_fires_hook_once() -> None:
    """The optional hook fires exactly once with the parsed terminal event."""
    calls: list[BacktestCompleteEvent] = []

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete(broker="tektii")))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=calls.append)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()

    assert len(calls) == 1
    assert isinstance(calls[0], BacktestCompleteEvent)
    assert calls[0].broker == "tektii"


async def test_backtest_complete_awaits_async_hook() -> None:
    """An async hook is awaited exactly once."""
    calls: list[BacktestCompleteEvent] = []

    async def hook(event: BacktestCompleteEvent) -> None:
        calls.append(event)

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=hook)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()

    assert len(calls) == 1
    assert calls[0].broker == "tektii"


async def test_backtest_complete_absent_hook_is_noop() -> None:
    """No hook supplied → still a clean exit, no error."""

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()
    # Reaching here without raising is the assertion.


async def test_sync_backtest_complete_fires_hook_on_iterating_thread() -> None:
    """SyncEventStream fires the hook exactly once, on the iterating (main)
    thread — not the background WebSocket loop thread.
    """
    calls: list[tuple[BacktestCompleteEvent, int]] = []

    def hook(event: BacktestCompleteEvent) -> None:
        calls.append((event, threading.get_ident()))

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> int:
        stream = SyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=hook)
        with stream:
            for _event in stream:
                pass
        return threading.get_ident()

    try:
        iter_thread = await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()

    assert len(calls) == 1
    assert isinstance(calls[0][0], BacktestCompleteEvent)
    assert calls[0][1] == iter_thread


async def test_backtest_complete_fires_hook_exactly_once_and_short_circuits() -> None:
    """A second terminal (or any frame) after the first must never be
    processed — the loop returns on the first terminal, so the hook fires
    exactly once and nothing further is yielded.
    """
    calls: list[BacktestCompleteEvent] = []

    async def handler(ws):
        with contextlib.suppress(websockets.exceptions.ConnectionClosed):
            await ws.send(json.dumps(_backtest_complete()))
            # A trailing candle and a second terminal — both must be ignored.
            await ws.send(json.dumps(_sample_candle("evt_after_terminal")))
            await ws.send(json.dumps(_backtest_complete()))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    events = []

    async def collect() -> None:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=calls.append)
        async with stream:
            async for event in stream:
                events.append(event)

    try:
        await asyncio.wait_for(collect(), timeout=5.0)
    finally:
        server.close()
        await server.wait_closed()

    assert events == []  # nothing after the terminal is yielded
    assert len(calls) == 1  # hook fired exactly once


async def test_backtest_complete_absent_broker_is_none() -> None:
    """The terminal's ``broker`` is optional — a payload without it parses and
    the hook receives ``broker=None``.
    """
    calls: list[BacktestCompleteEvent] = []

    async def handler(ws):
        await ws.send(
            json.dumps({"type": "backtest_complete", "timestamp": "2025-01-15T10:31:00Z"})
        )
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=calls.append)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()

    assert len(calls) == 1
    assert calls[0].broker is None


async def test_backtest_complete_absent_timestamp_still_parses() -> None:
    """The terminal is load-bearing: a payload missing ``timestamp`` must still
    parse and fire the hook (timestamp defaults to the epoch), not be dropped
    as unrecognised and fall through to the disconnect path.
    """
    calls: list[BacktestCompleteEvent] = []

    async def handler(ws):
        await ws.send(json.dumps({"type": "backtest_complete", "broker": "tektii"}))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=calls.append)
        async with stream:
            async for _event in stream:
                pass
    finally:
        server.close()
        await server.wait_closed()

    assert len(calls) == 1
    assert calls[0].timestamp == datetime(1970, 1, 1, tzinfo=UTC)


async def test_sync_early_break_closes_promptly_and_quietly(caplog) -> None:
    """Regression: breaking out of a sync stream mid-flight must not stall on
    ``close_timeout`` or log a spurious shutdown warning. The background loop
    is still driving the stream, so ``close()`` must unwind it without blocking
    on a future the loop-stop would abandon.
    """

    async def handler(ws):
        # Stream continuously so the client is mid-recv when it breaks.
        with contextlib.suppress(websockets.exceptions.ConnectionClosed):
            i = 0
            while True:
                await ws.send(json.dumps(_sample_candle(f"evt_break_{i}")))
                await asyncio.sleep(0.02)
                i += 1

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> float:
        stream = SyncEventStream(ws_url=url, reconnect=False)
        n = 0
        with stream:
            for _event in stream:
                n += 1
                if n >= 2:
                    break
            t0 = time.perf_counter()
        return time.perf_counter() - t0  # time spent in close()

    try:
        with caplog.at_level(logging.WARNING, logger="tektii.stream"):
            elapsed = await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()

    # Fix makes this ~0.01s; assert well under a single close_timeout (5s).
    assert elapsed < 3.0
    noisy = [
        r.getMessage()
        for r in caplog.records
        if "shutdown" in r.getMessage() or "did not exit" in r.getMessage()
    ]
    assert noisy == []


async def test_backtest_complete_sync_hook_exception_propagates() -> None:
    """A raising hook surfaces to the caller (we do not swallow user teardown
    bugs) — async path, plain callable.
    """

    def hook(event: BacktestCompleteEvent) -> None:
        raise RuntimeError("teardown boom")

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=hook)
        with pytest.raises(RuntimeError, match="teardown boom"):
            async with stream:
                async for _event in stream:
                    pass
    finally:
        server.close()
        await server.wait_closed()


async def test_backtest_complete_async_hook_exception_propagates() -> None:
    """A raising awaitable hook surfaces to the caller — async path, coroutine."""

    async def hook(event: BacktestCompleteEvent) -> None:
        raise RuntimeError("async teardown boom")

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)
    try:
        stream = AsyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=hook)
        with pytest.raises(RuntimeError, match="async teardown boom"):
            async with stream:
                async for _event in stream:
                    pass
    finally:
        server.close()
        await server.wait_closed()


async def test_sync_backtest_complete_hook_exception_propagates() -> None:
    """A raising hook surfaces to the caller on the sync path too."""

    def hook(event: BacktestCompleteEvent) -> None:
        raise RuntimeError("sync teardown boom")

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
        await ws.close()

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> None:
        stream = SyncEventStream(ws_url=url, reconnect=False, on_backtest_complete=hook)
        with stream:
            for _event in stream:
                pass

    try:
        with pytest.raises(RuntimeError, match="sync teardown boom"):
            await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()


async def test_sync_backtest_complete_sends_flush_ack() -> None:
    """SyncEventStream also flushes the terminal ACK on terminal receipt."""
    received: list[dict] = []

    async def handler(ws):
        await ws.send(json.dumps(_backtest_complete()))
        with contextlib.suppress(TimeoutError, websockets.exceptions.ConnectionClosed):
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            received.append(json.loads(raw))
        await ws.close()

    server, url = await _run_ws_server(handler)

    def run_sync_iter() -> None:
        stream = SyncEventStream(ws_url=url, reconnect=False)
        with stream:
            for _event in stream:
                pass

    try:
        await asyncio.to_thread(run_sync_iter)
    finally:
        server.close()
        await server.wait_closed()

    acks = [m for m in received if m.get("type") == "event_ack"]
    assert len(acks) == 1
    assert acks[0]["events_processed"] == []
