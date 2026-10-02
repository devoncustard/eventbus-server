"""Storage backend interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..event import Event


class EventStoreInterface(ABC):
    """Common interface for event storage backends."""

    @abstractmethod
    async def start(self) -> None:
        """Open connection and initialize tables."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Flush writes and close connection."""
        ...

    @abstractmethod
    def append(self, event: Event) -> int:
        """Append an event. Returns the assigned event ID."""
        ...

    @abstractmethod
    async def start_writer(self) -> None:
        """Start background writer task."""
        ...

    @abstractmethod
    def pull(
        self,
        cursor: int,
        labels: tuple[str, ...] = (),
        source: str | None = None,
        type_pattern: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        """Pull events after *cursor*.

        Returns ``(events, next_cursor, done)`` where ``done`` is True
        when no more events remain.
        """
        ...

    @abstractmethod
    def latest_cursor(self) -> int:
        """Return the highest event ID known to the store."""
        ...

    @abstractmethod
    def count(self) -> int:
        """Total events in ring buffer."""
        ...

    @abstractmethod
    async def purge(self) -> None:
        """Remove all events from the store."""
        ...
