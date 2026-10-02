"""SQLite storage backend."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import sqlite3
from collections import deque
from pathlib import Path
from typing import Any

from ..event import Event
from .interface import EventStoreInterface


def _labels_match_sql(labels: tuple[str, ...]) -> tuple[str, list[Any]]:
    """Build SQL WHERE clause for label intersection match."""
    if not labels:
        return "", []
    parts = []
    params: list[Any] = []
    for label in labels:
        parts.append(
            "EXISTS (SELECT 1 FROM json_each(events.labels) WHERE json_each.value = ?)"
        )
        params.append(label)
    return " AND ".join(parts), params


class SQLiteEventStore(EventStoreInterface):
    """Dual-layer event store with in-memory ring buffer + SQLite archive."""

    def __init__(
        self,
        db_path: str = "events.db",
        max_events: int = 5000,
    ) -> None:
        self._max_events = max_events
        self._db_path = db_path
        self._ring: deque[Event] = deque(maxlen=max_events)
        self._next_id: int = 1
        self._id_index: dict[int, tuple[int, Event]] = {}
        self._write_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=500)
        self._writer_task: asyncio.Task | None = None
        self._conn: sqlite3.Connection | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open SQLite and seed sequence counter from existing data."""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id       INTEGER PRIMARY KEY,
                subject  TEXT NOT NULL,
                source   TEXT,
                labels   TEXT NOT NULL DEFAULT '[]',
                payload  TEXT NOT NULL,
                wall_ts  REAL NOT NULL,
                mono_ts  REAL NOT NULL
            )
        """)
        self._conn.commit()

        row = self._conn.execute("SELECT MAX(id) FROM events").fetchone()
        if row and row[0]:
            self._next_id = row[0] + 1

        rows = list(self._conn.execute(
            "SELECT id, subject, source, labels, payload, wall_ts, mono_ts FROM events "
            "ORDER BY id DESC LIMIT ?", (self._max_events,)
        ))
        for rid, subject, source, labels_json, payload_json, wall_ts, mono_ts in reversed(rows):
            event = Event(
                subject=subject,
                source=source,
                labels=tuple(json.loads(labels_json)),
                payload=json.loads(payload_json),
                _wall_ts=wall_ts,
                _mono_ts=mono_ts,
            )
            self._ring.append(event)
            self._id_index[rid] = (0, event)

    async def close(self) -> None:
        """Flush remaining writes and close connection."""
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

        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------

    def append(self, event: Event) -> int:
        event_id = self._next_id
        self._next_id += 1

        self._ring.append(event)
        self._id_index[event_id] = (len(self._ring) - 1, event)

        if self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO events (id, subject, source, labels, payload, wall_ts, mono_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    event.subject,
                    event.source,
                    json.dumps(event.labels),
                    json.dumps(event.payload),
                    event.timestamp,
                    event.mono_timestamp,
                ),
            )
            self._conn.commit()

        try:
            self._write_queue.put_nowait(event)
        except asyncio.QueueFull:
            self._write_queue.get_nowait()
            self._write_queue.put_nowait(event)

        return event_id

    async def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(self._write_loop())

    async def _write_loop(self) -> None:
        batch: list[tuple] = []
        while not self._closed:
            try:
                event = await asyncio.wait_for(self._write_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if batch:
                    self._flush_batch(batch)
                    batch = []
                continue

            batch.append((
                event.subject,
                event.source,
                json.dumps(event.labels),
                json.dumps(event.payload),
                event.timestamp,
                event.mono_timestamp,
            ))

            if len(batch) >= 200 or self._closed:
                self._flush_batch(batch)
                batch = []

        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch: list[tuple]) -> None:
        if not self._conn or not batch:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO events (id, subject, source, labels, payload, wall_ts, mono_ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        self._conn.commit()

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

        if len(result) < limit and self._conn:
            latest_ring_id = max(self._id_index.keys()) if self._id_index else cursor
            missing_start = latest_ring_id + 1 if latest_ring_id > cursor else cursor

            where_parts = ["id > ?", "id <= ?"]
            params: list[Any] = [max(cursor, missing_start), latest_ring_id]

            if labels:
                label_sql, label_params = _labels_match_sql(labels)
                where_parts.append(label_sql)
                params.extend(label_params)

            if source:
                where_parts.append("source = ?")
                params.append(source)

            if type_pattern:
                where_parts.append("subject GLOB ?")
                params.append(type_pattern)

            sql = """
                SELECT id, subject, source, labels, payload, wall_ts, mono_ts FROM events
                WHERE {where_clause}
                ORDER BY id ASC
                LIMIT ?
            """.format(where_clause=" AND ".join(where_parts))
            params.append(limit - len(result))

            rows = self._conn.execute(sql, params).fetchall()

            for rid, subject, source, labels_json, payload_json, wall_ts, mono_ts in rows:
                result.append({
                    "id": rid,
                    "subject": subject,
                    "source": source,
                    "labels": tuple(json.loads(labels_json)),
                    "payload": json.loads(payload_json),
                    "ts": wall_ts,
                })
                next_cursor = rid

        done = True
        if self._conn:
            where_parts = ["id > ?"]
            params: list[Any] = [next_cursor]

            if labels:
                label_sql, label_params = _labels_match_sql(labels)
                where_parts.append(label_sql)
                params.extend(label_params)

            if source:
                where_parts.append("source = ?")
                params.append(source)

            if type_pattern:
                where_parts.append("subject GLOB ?")
                params.append(type_pattern)

            sql = """
                SELECT 1 FROM events
                WHERE {where_clause}
                LIMIT 1
            """.format(where_clause=" AND ".join(where_parts))
            row = self._conn.execute(sql, params).fetchone()
            if row:
                done = False

        if self._conn is None or (self._conn and not self._conn.execute(
            "SELECT 1 FROM events WHERE id > ?", (next_cursor,)
        ).fetchone()):
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
        if self._conn:
            row = self._conn.execute("SELECT MAX(id) FROM events").fetchone()
            if row and row[0]:
                return row[0]
        return 0

    def count(self) -> int:
        return len(self._ring)

    async def purge(self) -> None:
        """Remove all events from the ring buffer and database."""
        self._ring.clear()
        self._id_index.clear()
        if self._conn:
            self._conn.execute("DELETE FROM events")
            self._conn.commit()
