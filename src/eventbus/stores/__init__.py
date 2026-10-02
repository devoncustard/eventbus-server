"""Storage backend interface and implementations."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any

from .interface import EventStoreInterface
from .sqlite import SQLiteEventStore
from .mysql import MySQLEventStore


def create_store(
    backend: str | None = None,
    *,
    db_path: str = "events.db",
    max_events: int = 5000,
    db_host: str = "localhost",
    db_port: int = 3306,
    db_name: str = "eventbus",
    db_user: str = "root",
    db_password: str = "",
) -> EventStoreInterface:
    """Factory to create an event store backed by the chosen database.

    Args:
        backend: "sqlite" (default) or "mysql".
            Falls back to ``STORE_BACKEND`` env var if not set.
        db_path: Path for SQLite database file.
        max_events: Size of the in-memory ring buffer.
        db_host / db_port / db_name / db_user / db_password: MariaDB/MySQL
            connection parameters (read from env vars when not provided).

    Returns:
        An ``EventStoreInterface`` instance.
    """
    if backend is None:
        backend = os.environ.get("STORE_BACKEND", "sqlite").lower()

    if backend == "mysql":
        return MySQLEventStore(
            db_host=os.environ.get("DB_HOST", db_host),
            db_port=int(os.environ.get("DB_PORT", str(db_port))),
            db_name=os.environ.get("DB_NAME", db_name),
            db_user=os.environ.get("DB_USER", db_user),
            db_password=os.environ.get("DB_PASSWORD", db_password),
            max_events=max_events,
        )

    return SQLiteEventStore(
        db_path=os.environ.get("DB_PATH", db_path),
        max_events=max_events,
    )


__all__ = [
    "EventStoreInterface",
    "SQLiteEventStore",
    "MySQLEventStore",
    "create_store",
]
