"""Tests for the EventBus Monitor — WebSocket handshake, event streaming, filtering."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest

from eventbus import Event, EventBus
from eventbus.monitor import MonitorMixin


PORT = 19755


class MonitorEventBus(MonitorMixin, EventBus):
    """Bus with monitor mixin."""


@pytest.fixture
async def monitor_bus():
    bus = MonitorEventBus()
    task = asyncio.create_task(bus.start_monitor("127.0.0.1", PORT))
    # Wait until port is accepting connections (up to 3s)
    ready = False
    for _ in range(30):
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", PORT),
                timeout=0.1,
            )
            w.close()
            await w.wait_closed()
            ready = True
            break
        except OSError:
            await asyncio.sleep(0.1)
    if not ready:
        pytest.fail("Monitor server failed to start within 3 s")
    yield bus
    await bus.stop_monitor()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Helpers — all async, so they don't starve the event loop
# ---------------------------------------------------------------------------


async def _read_http_headers(reader: asyncio.StreamReader) -> bytes:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = await reader.read(4096)
        if not chunk:
            break
        data += chunk
    return data


async def _read_ws_frame(reader: asyncio.StreamReader) -> bytes:
    first, second = await reader.readexactly(2)
    length = second & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    return await reader.readexactly(length) if length > 0 else b""


async def _send_ws_upgrade(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    path: str,
    key: str,
) -> bytes:
    request = (
        f"GET {path} HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    writer.write(request.encode())
    await writer.drain()
    return await _read_http_headers(reader)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_dashboard_serves_html(monitor_bus):
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
    try:
        writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        await writer.drain()
        data = await _read_http_headers(reader)
        assert b"200 OK" in data, f"Expected 200 OK, got: {data[:200]}"
        assert b"text/html" in data
        assert b"Event Bus Monitor" in data
    finally:
        writer.close()
        await writer.wait_closed()


async def test_websocket_handshake_succeeds(monitor_bus):
    key = base64.b64encode(b"testkey123456789").decode()
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
    try:
        response = await _send_ws_upgrade(reader, writer, "/ws", key)
        assert b"101 Switching Protocols" in response
    finally:
        writer.close()
        await writer.wait_closed()


async def test_websocket_receives_events(monitor_bus):
    # Emit before connect (will be queued)
    monitor_bus.emit(Event("test.echo", {"hello": "world"}))
    await asyncio.sleep(0.05)

    key = base64.b64encode(b"echoclient12345678").decode()
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
    try:
        response = await _send_ws_upgrade(reader, writer, "/ws", key)
        assert b"101 Switching Protocols" in response

        # Emit another event while connected
        monitor_bus.emit(Event("test.ping", {"from": "client"}))
        await asyncio.sleep(0.05)

        payload = await _read_ws_frame(reader)
        assert payload
        data = json.loads(payload)
        assert data["subject"] in ("test.echo", "test.ping")
    finally:
        writer.close()
        await writer.wait_closed()


async def test_pattern_filter_works(monitor_bus):
    key = base64.b64encode(b"filterclient123456").decode()
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
    try:
        response = await _send_ws_upgrade(
            reader, writer, "/ws?pattern=user.*", key
        )
        assert b"101 Switching Protocols" in response

        # Emit events AFTER connecting so client can receive them
        monitor_bus.emit(Event("user.login", {"user": "monke"}))
        monitor_bus.emit(Event("system.restart", {"why": "because"}))
        await asyncio.sleep(0.05)

        payload = await _read_ws_frame(reader)
        assert payload
        data = json.loads(payload)
        assert "user" in data["subject"]
        assert data["subject"] != "system.restart"
    finally:
        writer.close()
        await writer.wait_closed()


async def test_once_handler_with_monitor_active(monitor_bus):
    call_count = 0

    def once_handler(event):
        nonlocal call_count
        call_count += 1

    monitor_bus.once("once.test", once_handler)
    monitor_bus.emit(Event("once.test"))
    monitor_bus.emit(Event("once.test"))
    assert call_count == 1


async def test_subscribe_callback_invoked(monitor_bus):
    received: list[Event] = []

    def sub(event):
        received.append(event)

    monitor_bus.subscribe(sub)
    monitor_bus.emit(Event("sub.test", payload={"x": 1}))
    monitor_bus.emit(Event("sub.test", payload={"x": 2}))
    assert len(received) == 2
    assert received[0].payload == {"x": 1}
    assert received[1].payload == {"x": 2}


async def test_unsubscribe_removes_callback(monitor_bus):
    received: list[Event] = []

    def sub(event):
        received.append(event)

    monitor_bus.subscribe(sub)
    monitor_bus.emit(Event("go"))
    monitor_bus.unsubscribe(sub)
    monitor_bus.emit(Event("go"))
    assert len(received) == 1


async def test_subscribers_introspection(monitor_bus):
    def my_sub(event):
        pass

    monitor_bus.subscribe(my_sub)
    assert "my_sub" in monitor_bus.subscribers()


async def test_health_endpoint(monitor_bus):
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
    try:
        writer.write(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        await writer.drain()
        data = await _read_http_headers(reader)
        assert b"200 OK" in data
        assert b"application/json" in data
    finally:
        writer.close()
        await writer.wait_closed()
