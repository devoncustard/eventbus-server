from __future__ import annotations

import asyncio
import fnmatch
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

from .event import Event


@dataclass(frozen=True, eq=True)
class _Registration:
    handler: Callable
    priority: int
    is_once: bool = False


SyncHandler = Callable[[Event], None]
AsyncHandler = Callable[[Event], Coroutine[Any, Any, None]]


class EventBus:
    """A simple pub/sub event bus with glob pattern matching and priorities.

    - Handlers are registered by pattern (supports * wildcards via fnmatch).
    - Lower priority number = higher precedence.
    - ``once`` handlers are auto-removed after their first invocation.
    - Async handlers are run as coroutines via ``emit_async()``.
    - By default a failing handler does NOT prevent other handlers from
      running (fail-silent).  Pass ``fail_fast=True`` to stop on the first
      exception.
    - Subscribers (via ``subscribe()``) receive every event regardless of
      pattern and can be sync or async.
    - Optionally run a live-monitor server with ``MonitorMixin.start_monitor()``
      for real-time WebSocket access.
    """

    def __init__(self, *, fail_fast: bool = False) -> None:
        self._handlers: dict[str, list[_Registration]] = defaultdict(list)
        self._fail_fast = fail_fast
        self._listeners: list[Callable] = []  # subscribers
        self._background_tasks: set[asyncio.Task] = set()
        """Track fire-and-forget async tasks so they survive until complete."""

    # -- subscription (handlers) ---------------------------------------------

    def on(
        self,
        pattern: str,
        handler: SyncHandler | AsyncHandler,
        *,
        priority: int = 0,
    ) -> None:
        """Register a handler for *pattern* with the given priority (lower = first)."""
        self._handlers[pattern].append(_Registration(handler, priority))

    def once(
        self,
        pattern: str,
        handler: SyncHandler | AsyncHandler,
        *,
        priority: int = 0,
    ) -> None:
        """Register a one-shot handler (auto-removed after first emission)."""
        self._handlers[pattern].append(
            _Registration(handler, priority, is_once=True)
        )

    def off(self, pattern: str, handler: SyncHandler | AsyncHandler) -> None:
        """Remove a handler previously registered for *pattern*."""
        bucket = self._handlers.get(pattern)
        if bucket is None:
            return
        self._handlers[pattern] = [
            r for r in bucket if r.handler is not handler
        ]
        if not self._handlers[pattern]:
            del self._handlers[pattern]

    # -- subscription (subscribers) ------------------------------------------

    def subscribe(self, callback: Callable[[Event], Any]) -> None:
        """Subscribe a callback to be called on every ``emit``."""
        self._listeners.append(callback)

    def unsubscribe(self, callback: Callable[[Event], Any]) -> None:
        """Unsubscribe a previously registered callback."""
        self._listeners.remove(callback)

    def subscribers(self) -> list[str]:
        """Return subscriber callback names."""
        return [cb.__name__ for cb in self._listeners]

    # -- internal ------------------------------------------------------------

    def _resolve_targets(self, event: Event) -> list[tuple[_Registration, bool]]:
        """Resolve matching patterns and sort by priority."""
        targets: list[tuple[_Registration, bool]] = []
        for pattern, bucket in self._handlers.items():
            if fnmatch.fnmatch(event.subject, pattern):
                for reg in sorted(bucket, key=lambda r: r.priority):
                    targets.append(
                        (reg, asyncio.iscoroutinefunction(reg.handler))
                    )
        targets.sort(key=lambda t: t[0].priority)
        return targets

    def _cleanup_once(self) -> None:
        """Remove once-handlers whose handler was called this emission cycle."""
        for pattern in list(self._handlers):
            self._handlers[pattern] = [
                r for r in self._handlers[pattern]
                if not (r.is_once and r.handler in self._once_seen)
            ]
            if not self._handlers[pattern]:
                del self._handlers[pattern]

    # -- emission ------------------------------------------------------------

    def _invoke(self, reg: _Registration, event: Event) -> None:
        """Invoke a single handler, handling once-cleanup and error policy."""
        try:
            if asyncio.iscoroutinefunction(reg.handler):
                task = asyncio.create_task(reg.handler(event))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                if reg.is_once:
                    self._once_seen.add(reg.handler)
            else:
                reg.handler(event)
                if reg.is_once:
                    self._once_seen.add(reg.handler)
        except Exception:
            if self._fail_fast:
                raise

    def emit(self, event: Event) -> None:
        """Synchronously emit an event, invoking all matching handlers and subscribers."""
        self._once_seen: set[Callable] = set()
        targets = self._resolve_targets(event)
        for reg, _is_async in targets:
            self._invoke(reg, event)
        self._cleanup_once()

        # Run subscribers (fire-and-forget async ones)
        for listener in self._listeners:
            try:
                result = listener(event)
                if asyncio.iscoroutine(result):
                    task = asyncio.create_task(result)
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
            except Exception:
                if self._fail_fast:
                    raise

    def cleanup_tasks(self) -> None:
        """Remove completed background tasks from the tracking set."""
        self._background_tasks.discard(None)

    async def emit_async(self, event: Event) -> None:
        """Emit and run any async handlers and subscribers as coroutines."""
        self._once_seen: set[Callable] = set()
        targets = self._resolve_targets(event)
        for reg, is_async in targets:
            try:
                if is_async:
                    await reg.handler(event)
                else:
                    reg.handler(event)
            except Exception:
                if self._fail_fast:
                    raise
            # Track sync-once handlers that ran during this async emit
            if reg.is_once and not asyncio.iscoroutinefunction(reg.handler):
                self._once_seen.add(reg.handler)
        self._cleanup_once()

        # Run subscribers (await async ones)
        for listener in self._listeners:
            try:
                result = listener(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                if self._fail_fast:
                    raise

    # -- introspection -------------------------------------------------------

    def listeners(self, pattern: str) -> list[str]:
        """Return handler function names registered for *pattern*."""
        return [r.handler.__name__ for r in self._handlers.get(pattern, [])]
