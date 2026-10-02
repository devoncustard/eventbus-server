"""Persistent event store — ring buffer + SQLite archive."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import sqlite3
from collections import deque
from pathlib import Path
from typing import Any

from .event import Event


def _labels_match_sql(labels: tuple[str, ...]) -> tuple[str, list[Any]]:
    """Build SQL WHERE clause for label intersection match.

    Labels are stored as JSON arrays in SQLite. We use
    json_extract to check each element index. Returns
    (sql_fragment, params).
    """
    if not labels:
        return "", []
    parts = []
    params: list[Any] = []
    for label in labels:
        # json_each iterates over the array; we match any element equal to label
        parts.append(
            "EXISTS (SELECT 1 FROM json_each(events.labels) WHERE json_each.value = ?)"
        )
        params.append(label)
    return " AND ".join(parts), params


class EventStore:
    """Dual-layer event store.

    Hot layer: in-memory deque (ring buffer, bounded by ``max_events``).
    Cold layer: SQLite on-disk archive.

    Usage::

        store = EventStore("data/events.db", max_events=5000)
        store.append(event)
        await store.close()
    """

    def __init__(
        self,
        db_path: str = "events.db",
        max_events: int = 5000,
    ) -> None:
        self._max_events = max_events
        self._db_path = db_path
        self._ring: deque[Event] = deque(maxlen=max_events)

        # Latest global sequence counter (monotonically increasing)
        self._next_id: int = 1

        # Ring buffer index: maps event_id -> (index_in_ring, Event)
        self._id_index: dict[int, tuple[int, Event]] = {}

        # SQLite: background writer queue
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

        # Seed next_id from max existing
        row = self._conn.execute("SELECT MAX(id) FROM events").fetchone()
        if row and row[0]:
            self._next_id = row[0] + 1

        # Replay ring buffer from latest entries (so clients can catch up fast)
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
            self._id_index[rid] = (0, event)  # index position doesn't matter for replay

    async def close(self) -> None:
        """Flush remaining writes and close connection."""
        self._closed = True
        if self._writer_task:
            # Drain write queue
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
        """Append an event. Returns the assigned event ID.

        Writes synchronously to SQLite for durability, then enqueues
        for the batch writer (which handles any remaining buffering).
        """
        event_id = self._next_id
        self._next_id += 1

        # Hot layer
        self._ring.append(event)
        self._id_index[event_id] = (len(self._ring) - 1, event)

        # Cold layer: synchronous write (always) + enqueue for batching
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
        """Start the background SQLite writer task."""
        self._writer_task = asyncio.create_task(self._write_loop())

    async def _write_loop(self) -> None:
        """Drain the write queue and batch-insert into SQLite."""
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
    # Pull by cursor (offset)
    # ------------------------------------------------------------------

    def pull(
        self,
        cursor: int,
        labels: tuple[str, ...] = (),
        source: str | None = None,
        type_pattern: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        """Pull events after *cursor*.

        Args:
            cursor: Global event ID to start after.
            labels: If provided, return only events containing **all**
                specified labels (intersection match).
            source: If provided, return only events from this source.
            type_pattern: If provided, return only events whose subject
                matches this glob pattern (e.g. ``"user.*"``).
            limit: Maximum events to return.

        Returns:
            ``(events, next_cursor, done)`` where:
            - ``events`` is a list of event dicts
            - ``next_cursor`` is the cursor to use for the next page
            - ``done`` is True when no more events remain
        """
        result: list[dict[str, Any]] = []
        next_cursor = cursor

        # 1) Try ring buffer first (hot path)
        for eid, (idx, ev) in sorted(self._id_index.items()):
            if eid <= cursor:
                continue
            # Type (subject) filter: glob match
            if type_pattern and not fnmatch.fnmatch(ev.subject, type_pattern):
                continue
            # Label filter: intersection match (all specified labels present)
            if labels and not all(l in ev.labels for l in labels):
                continue
            # Source filter: exact match
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

        # 2) If we haven't filled the limit and the buffer doesn't have more,
        #    fall back to SQLite for the gap + remaining
        if len(result) < limit and self._conn:
            # Get the latest event ID in the ring buffer to know where we are
            latest_ring_id = max(self._id_index.keys()) if self._id_index else cursor
            missing_start = latest_ring_id + 1 if latest_ring_id > cursor else cursor

            # Build SQL with label filters
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

        # Check if there's a next event after what we returned
        done = True
        if self._conn:
            # Reuse the same label filtering to check if more exist
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

        # If we got everything from the ring and it's all there is, check ring
        if self._conn is None or not row:
            # Also check ring buffer for events not yet written to SQLite
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
    # Latest cursor (what's the latest event ID?)
    # ------------------------------------------------------------------

    def latest_cursor(self) -> int:
        """Return the highest event ID known to the store."""
        if self._id_index:
            return max(self._id_index.keys())
        if self._conn:
            row = self._conn.execute("SELECT MAX(id) FROM events").fetchone()
            if row and row[0]:
                return row[0]
        return 0

    def count(self) -> int:
        """Total events in ring buffer."""
        return len(self._ring)
