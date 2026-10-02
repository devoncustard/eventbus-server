from __future__ import annotations

import asyncio
import fnmatch
import json
import sys
import os
from pathlib import Path
from typing import Any

from .bus import EventBus
from .event import Event
from .stores import EventStoreInterface, create_store


class MonitorMixin:
    """Mixin that adds a WebSocket monitor server to EventBus.

    Clients connect with optional query params:
      ?pattern=...  filter by event name glob
      ?max=...      limit displayed events (default 50)
    The server broadcasts all events (or filtered subset) in real-time.

    Usage::

        class MyBus(MonitorMixin, EventBus):
            pass

        bus = MyBus()
        bus.on("user.*", my_handler)
        bus.start_monitor_sync(host="0.0.0.0", port=8765)
    """

    _monitor_server: Any = None
    _monitor_task: Any = None
    _monitor_started: bool = False

    def __init__(
        self,
        *,
        db_path: str = "events.db",
        max_events: int = 5000,
        store: EventStoreInterface | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._monitor_clients: set = set()
        self._monitor_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._store: EventStoreInterface | None = None
        self._store_started = False

        # Create the store for persistence (or use a pre-built one)
        if store is not None:
            self._store = store
        else:
            self._store = create_store(db_path=db_path, max_events=max_events)

    def _format_event(self, event: Event, event_id: int) -> dict[str, Any]:
        """Serialize an event for WebSocket transmission."""
        return {
            "id": event_id,
            "subject": event.subject,
            "source": event.source,
            "labels": event.labels,
            "payload": event.payload,
            "ts": event.timestamp,
        }

    def _on_event(self, event: Event) -> int | None:
        """Subscriber callback — persist event, then queue for broadcast.

        Returns the assigned event ID if the store is active, None otherwise.
        """
        if self._store is None:
            return None
        event_id = self._store.append(event)
        try:
            self._monitor_queue.put_nowait((event, event_id))
        except asyncio.QueueFull:
            pass  # Drop events if queue is full
        return event_id

    def _broadcast_frame(self, data: bytes) -> bytes:
        """Encode raw data as a WebSocket text frame."""
        length = len(data)
        if length < 126:
            header = bytes([0x81, length])  # FIN+text opcode, unmasked
        elif length < 65536:
            header = bytes([0x81, 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([0x81, 127]) + length.to_bytes(8, "big")
        return header + data

    async def _broadcast_loop(self) -> None:
        """Read from queue and broadcast to all connected WebSocket clients."""
        while True:
            try:
                event, event_id = await self._monitor_queue.get()
            except asyncio.CancelledError:
                return

            frame = self._broadcast_frame(
                json.dumps(self._format_event(event, event_id)).encode() + b"\n"
            )

            disconnected: list[Any] = []
            for writer in self._monitor_clients:
                # Check per-client pattern filter
                pattern = getattr(writer, "_pattern_filter", None)
                if pattern and not fnmatch.fnmatch(event.subject, pattern):
                    continue
                try:
                    writer.write(frame)
                    await writer.drain()
                except (ConnectionResetError, BrokenPipeError, OSError):
                    disconnected.append(writer)
                except Exception:
                    disconnected.append(writer)

            for w in disconnected:
                self._monitor_clients.discard(w)

    async def start_monitor(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        """Start the WebSocket monitor server (async)."""
        if self._monitor_started:
            return
        self._monitor_started = True

        # Start the event store and its writer
        if self._store:
            try:
                await self._store.start()
                print(f"Event store started ({type(self._store).__name__})")
                await self._store.start_writer()
                self._store_started = True
            except Exception as e:
                print(f"[ERROR] Failed to start event store: {e}", flush=True)
                raise

        # Register as subscriber
        self.subscribe(self._on_event)

        # Start broadcast loop task
        self._monitor_task = asyncio.create_task(self._broadcast_loop())

        # Start TCP server
        self._monitor_server = await asyncio.start_server(
            self._ws_handler, host, port
        )
        addr = self._monitor_server.sockets[0].getsockname()
        print(f"Monitor server listening on ws://{addr[0]}:{addr[1]}")

        await self._monitor_server.serve_forever()

    async def stop_monitor(self) -> None:
        """Stop the monitor server."""
        if not self._monitor_started:
            return
        self._monitor_started = False

        # Close TCP server
        if self._monitor_server:
            self._monitor_server.close()
            await self._monitor_server.wait_closed()

        # Cancel broadcast loop
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        # Unsubscribe from bus
        self.unsubscribe(self._on_event)
        self._monitor_clients.clear()

        # Close the event store
        if self._store:
            await self._store.close()
            self._store_started = False

    def start_monitor_sync(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        """Start the monitor server (sync wrapper).

        This starts the server on a background thread so the caller's
        event loop is not blocked.  It returns immediately.
        """
        if self._monitor_started:
            return

        loop = asyncio.new_event_loop()

        def _run() -> None:
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.start_monitor(host, port))
            except KeyboardInterrupt:
                pass
            finally:
                loop.close()

        import threading

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        # Give the server a moment to bind before returning
        for _ in range(30):
            try:
                import socket

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, port))
                s.close()
                return
            except OSError:
                import time

                time.sleep(0.1)
        # If we get here the port may not be ready yet; caller can retry

    async def _ws_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single WebSocket or HTTP client."""
        try:
            await self._handle_ws(reader, writer)
        except Exception as exc:
            try:
                writer.close()
            except Exception:
                pass

    async def _handle_ws(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single WebSocket or HTTP client."""
        # Read HTTP upgrade request
        header_lines: list[str] = []
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                decoded = line.decode("utf-8", errors="replace").strip()
                if decoded == "" or decoded == "\r":
                    break
                header_lines.append(decoded)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return

        if not header_lines:
            return

        first_line = header_lines[0].split()
        if len(first_line) < 2:
            return

        full_path = first_line[1]  # Preserve full path with query string
        path = full_path.split("?")[0]

        headers_dict: dict[str, str] = {}
        for h in header_lines[1:]:
            if ": " in h:
                k, v = h.split(": ", 1)
                headers_dict[k.lower()] = v

        # Check if WebSocket upgrade
        upgrade = headers_dict.get("upgrade", "")
        connection = headers_dict.get("connection", "")
        sec_ws_key = headers_dict.get("sec-websocket-key", "")

        if (
            upgrade.lower() != "websocket"
            or "upgrade" not in connection.lower()
            or not sec_ws_key
        ):
            # Regular HTTP — dispatch to /events, /health, or dashboard
            http_path = path.split("?")[0]
            method = first_line[0]
            if http_path == "/events":
                if method == "POST":
                    cl = int(headers_dict.get("content-length", "0"))
                    await self._serve_post_event(writer, full_path, reader, cl)
                elif method == "DELETE":
                    await self._serve_purge_events(writer)
                else:
                    await self._serve_events(writer, full_path)
                return
            if http_path == "/health":
                await self._serve_health(writer)
                return
            await self._serve_dashboard(writer)
            return

        # Complete WebSocket handshake
        import hashlib
        import base64

        magic = "258EAFA5-E914-47DA-95CA-5AB9F132C6CC"
        accept = base64.b64encode(
            hashlib.sha1(sec_ws_key.encode() + magic.encode()).digest()
        ).decode()

        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        )
        try:
            writer.write(response.encode())
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

        # Parse per-client pattern filter from query params
        pattern_filter = None
        qs_part = full_path.split("?", 1)[1] if "?" in full_path else ""
        for param in qs_part.split("&"):
            if "=" in param:
                key, val = param.split("=", 1)
                if key == "pattern":
                    pattern_filter = val

        # Set filter on writer and add to clients
        writer._pattern_filter = pattern_filter  # type: ignore[attr-defined]
        self._monitor_clients.add(writer)

        # Read client frames (close/ping detection)
        try:
            while True:
                header_bytes = await reader.readexactly(2)
                if len(header_bytes) < 2:
                    break
                first, second = header_bytes
                opcode = first & 0x0F
                length = second & 0x7F
                if length == 126:
                    length = int.from_bytes(await reader.readexactly(2), "big")
                elif length == 127:
                    length = int.from_bytes(await reader.readexactly(8), "big")
                data = await reader.readexactly(length) if length > 0 else b""

                if opcode == 0x8:  # close
                    close_frame = b"\x88\x00"
                    writer.write(close_frame)
                    await writer.drain()
                    break
                elif opcode == 0x9:  # ping
                    pong = b"\x8a" + bytes([len(data)]) + data
                    writer.write(pong)
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            self._monitor_clients.discard(writer)
            try:
                writer.close()
            except Exception:
                pass

    async def _serve_dashboard(self, writer: asyncio.StreamWriter) -> None:
        """Serve the HTML dashboard as a regular HTTP response."""
        from .dashboard_html import dashboard_html

        body = dashboard_html.encode()
        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "Content-Length: " + str(len(body)) + "\r\n"
            "Cache-Control: no-store, no-cache, must-revalidate, proxy-revalidate\r\n"
            "Pragma: no-cache\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        writer.write(response.encode() + body)
        await writer.drain()
        try:
            writer.close()
        except Exception:
            pass

    async def _serve_post_event(
        self,
        writer: asyncio.StreamWriter,
        path: str,
        reader: asyncio.StreamReader,
        content_length: int,
    ) -> None:
        """Handle POST /events — dispatch an event from an HTTP client."""
        # Read body
        body = b""
        if content_length > 0:
            try:
                body = await asyncio.wait_for(
                    reader.readexactly(content_length), timeout=2.0
                )
            except Exception:
                pass

        if not body:
            response = json.dumps({"error": "no payload"}).encode()
            resp = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + str(len(response)) + "\r\nConnection: close\r\n\r\n"
            writer.write(resp.encode() + response)
            await writer.drain()
            return

        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            response = json.dumps({"error": "invalid JSON"}).encode()
            resp = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + str(len(response)) + "\r\nConnection: close\r\n\r\n"
            writer.write(resp.encode() + response)
            await writer.drain()
            return

        # Validate required fields
        if not isinstance(data, dict) or "subject" not in data:
            response = json.dumps({"error": "need 'subject' field"}).encode()
            resp = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + str(len(response)) + "\r\nConnection: close\r\n\r\n"
            writer.write(resp.encode() + response)
            await writer.drain()
            return

        # Build Event
        event_subject = str(data["subject"])
        labels = tuple(data.get("labels", []))
        payload = data.get("payload", {})
        source = data.get("source")
        event = Event(subject=event_subject, labels=labels, payload=payload, source=source)

        # Emit through the bus
        event_id = self._on_event(event)
        if event_id is None:
            response = json.dumps({"error": "bus not running"}).encode()
            resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: " + str(len(response)) + "\r\nConnection: close\r\n\r\n"
            writer.write(resp.encode() + response)
            await writer.drain()
            return

        os.write(2, f"[POST] {event.subject} from {source} (id={event_id})\n".encode())

        # Acknowledge
        response = json.dumps({"id": event_id, "subject": event_subject, "source": source}).encode()
        resp = "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: " + str(len(response)) + "\r\nConnection: close\r\n\r\n"
        writer.write(resp.encode() + response)
        await writer.drain()
        try:
            writer.close()
        except Exception:
            pass

    async def _serve_events(self, writer: asyncio.StreamWriter, full_path: str) -> None:
        """Serve the /events pull endpoint as JSON."""
        # Parse query params
        params: dict[str, str] = {}
        if "?" in full_path:
            qs = full_path.split("?", 1)[1]
            for param in qs.split("&"):
                if "=" in param:
                    key, val = param.split("=", 1)
                    params[key] = val

        try:
            cursor = int(params.get("cursor", "0"))
        except ValueError:
            cursor = 0

        limit = int(params.get("limit", "50"))
        if limit < 1:
            limit = 50
        if limit > 500:
            limit = 500

        labels: tuple[str, ...] = ()
        raw_labels = params.get("labels", "")
        if raw_labels:
            labels = tuple(raw_labels.split(","))

        # Parse source filter - only use None when param not provided at all
        raw_source = params.get("source")
        source = None if raw_source is None else (raw_source or None)

        type_pattern = params.get("type", "").strip() or None

        if self._store is None:
            response = json.dumps({"error": "event store not available"}).encode()
        else:
            events, next_cursor, done = self._store.pull(
                cursor=cursor, labels=labels, source=source, type_pattern=type_pattern, limit=limit
            )
            response = json.dumps({
                "events": events,
                "next_cursor": next_cursor,
                "done": done,
            }).encode()

        resp = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: " + str(len(response)) + "\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        writer.write(resp.encode() + response)
        await writer.drain()
        try:
            writer.close()
        except Exception:
            pass

    async def _serve_health(self, writer: asyncio.StreamWriter) -> None:
        """Serve a simple health-check HTTP response."""
        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: 2\r\n"
            "Connection: close\r\n"
            "\r\n"
            "{}"
        )
        writer.write(response.encode())
        await writer.drain()
        try:
            writer.close()
        except Exception:
            pass

    async def _serve_purge_events(self, writer: asyncio.StreamWriter) -> None:
        """Serve DELETE /events — purge all events from the store."""
        if self._store is None:
            response = json.dumps({"error": "event store not available"}).encode()
            status = "400 Bad Request"
        else:
            try:
                await self._store.purge()
                response = json.dumps({"success": True, "message": "All events purged"}).encode()
                status = "200 OK"
            except Exception as e:
                response = json.dumps({"error": str(e)}).encode()
                status = "500 Internal Server Error"

        resp = (
            f"HTTP/1.1 {status}\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: " + str(len(response)) + "\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        writer.write(resp.encode() + response)
        await writer.drain()
        try:
            writer.close()
        except Exception:
            pass
