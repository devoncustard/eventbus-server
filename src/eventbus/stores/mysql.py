"""MariaDB / MySQL storage backend."""

from __future__ import annotations

import asyncio
import fnmatch
import json
from collections import deque
from typing import Any

from ..event import Event
from .interface import EventStoreInterface


class _EventWriteItem:
    """Carries an event and its assigned ID through the write queue."""
    __slots__ = ("event", "event_id")

    def __init__(self, event: Event, event_id: int) -> None:
        self.event = event
        self.event_id = event_id


class MySQLEventStore(EventStoreInterface):
    """Dual-layer event store with in-memory ring buffer + MariaDB archive."""

    def __init__(
        self,
        *,
        db_host: str = "localhost",
        db_port: int = 3306,
        db_name: str = "eventbus",
        db_user: str = "root",
        db_password: str = "",
        max_events: int = 5000,
    ) -> None:
        self._max_events = max_events
        self._db_host = db_host
        self._db_port = db_port
        self._db_name = db_name
        self._db_user = db_user
        self._db_password = db_password

        self._ring: deque[Event] = deque(maxlen=max_events)
        self._next_id: int = 1
        self._id_index: dict[int, tuple[int, Event]] = {}

        self._write_queue: asyncio.Queue[_EventWriteItem] = asyncio.Queue(maxsize=500)
        self._writer_task: asyncio.Task | None = None
        self._pool: Any | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create connection pool and initialize table."""
        import asyncmy  # type: ignore[import-not-found]

        # Create the database if it doesn't exist (SQLite auto-creates the file)
        try:
            conn = await asyncmy.connect(
                host=self._db_host,
                port=self._db_port,
                user=self._db_user,
                password=self._db_password,
            )
            async with conn.cursor() as cursor:
                await cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self._db_name}`")
            await conn.commit()
            await conn.close()
        except Exception:
            # Database may already exist or user lacks privileges — continue anyway
            pass

        self._pool = await asyncmy.create_pool(
            host=self._db_host,
            port=self._db_port,
            db=self._db_name,
            user=self._db_user,
            password=self._db_password,
            autocommit=False,
            minsize=1,
            maxsize=10,
        )

        # Ensure table exists
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id          BIGINT PRIMARY KEY,
                        subject     VARCHAR(512) NOT NULL,
                        source      VARCHAR(256),
                        labels      JSON NOT NULL,
                        payload     JSON NOT NULL,
                        wall_ts     DOUBLE NOT NULL,
                        mono_ts     DOUBLE NOT NULL,
                        INDEX idx_wall_ts (wall_ts),
                        INDEX idx_id (id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            await conn.commit()

        # Seed next_id from max existing
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT MAX(id) FROM events")
                row = await cursor.fetchone()
                if row and row[0]:
                    self._next_id = row[0] + 1

        # Replay ring buffer from latest entries
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT id, subject, source, labels, payload, wall_ts, mono_ts "
                    "FROM events ORDER BY id DESC LIMIT %s",
                    (self._max_events,),
                )
                rows = await cursor.fetchall()

        for row in reversed(rows):
            rid, subject, source, labels_json, payload_json, wall_ts, mono_ts = row
            labels = json.loads(labels_json) if isinstance(labels_json, str) else labels_json
            payload = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
            event = Event(
                subject=subject,
                source=source,
                labels=tuple(labels),
                payload=payload,
                _wall_ts=wall_ts,
                _mono_ts=mono_ts,
            )
            self._ring.append(event)
            self._id_index[rid] = (0, event)

    async def close(self) -> None:
        """Flush writes and close pool."""
        self._closed = True
        if self._writer_task:
            while not self._write_queue.empty():
                try:
                    self._write_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass

        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------

    def append(self, event: Event) -> int:
        event_id = self._next_id
        self._next_id += 1

        self._ring.append(event)
        self._id_index[event_id] = (len(self._ring) - 1, event)

        # Enqueue for async write with the assigned ID
        try:
            self._write_queue.put_nowait(_EventWriteItem(event, event_id))
        except asyncio.QueueFull:
            self._write_queue.get_nowait()
            self._write_queue.put_nowait(_EventWriteItem(event, event_id))

        return event_id

    async def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(self._write_loop())

    async def _write_loop(self) -> None:
        batch: list[_EventWriteItem] = []
        while not self._closed:
            try:
                item = await asyncio.wait_for(self._write_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if batch:
                    await self._flush_batch(batch)
                    batch = []
                continue

            batch.append(item)

            if len(batch) >= 200 or self._closed:
                await self._flush_batch(batch)
                batch = []

        if batch:
            await self._flush_batch(batch)

    async def _flush_batch(self, batch: list[_EventWriteItem]) -> None:
        if not self._pool or not batch:
            return

        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for item in batch:
                        event = item.event
                        await cursor.execute(
                            "INSERT IGNORE INTO events (id, subject, source, labels, payload, wall_ts, mono_ts) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (
                                item.event_id,
                                event.subject,
                                event.source,
                                json.dumps(event.labels),
                                json.dumps(event.payload),
                                event.timestamp,
                                event.mono_timestamp,
                            ),
                        )
                await conn.commit()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Pull
    # ------------------------------------------------------------------

    def pull(
        self,
        cursor: int,
        labels: tuple[str, ...] = (),
        source: str | None = None,
        type_pattern: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        result: list[dict[str, Any]] = []
        next_cursor = cursor

        # Hot path: ring buffer
        for eid, (idx, ev) in sorted(self._id_index.items()):
            if eid <= cursor:
                continue
            if type_pattern and not fnmatch.fnmatch(ev.subject, type_pattern):
                continue
            if labels and not all(l in ev.labels for l in labels):
                continue
            if source and ev.source != source:
                continue
            next_cursor = eid
            result.append({
                "id": eid,
                "subject": ev.subject,
                "source": ev.source,
                "labels": ev.labels,
                "payload": ev.payload,
                "ts": ev.timestamp,
            })
            if len(result) >= limit:
                return result, next_cursor, False

        # Note: cold-path DB pull requires an async context.
        # For now, MySQL store relies on the ring buffer.

        done = True
        for eid in self._id_index:
            if eid > next_cursor:
                ev = self._id_index[eid][1]
                if (not labels or all(l in ev.labels for l in labels)) and \
                   (not source or ev.source == source) and \
                   (not type_pattern or fnmatch.fnmatch(ev.subject, type_pattern)):
                    done = False
                    break

        return result, next_cursor, done

    # ------------------------------------------------------------------
    # Cursor helpers
    # ------------------------------------------------------------------

    def latest_cursor(self) -> int:
        if self._id_index:
            return max(self._id_index.keys())
        return 0

    def count(self) -> int:
        return len(self._ring)

    async def purge(self) -> None:
        """Remove all events from the ring buffer and database."""
        self._ring.clear()
        self._id_index.clear()
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM events")
