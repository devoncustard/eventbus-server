"""Tests for the EventBus — sync, async, pattern matching, once, priority, off, fail-fast."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

from eventbus import Event, EventBus


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


def test_emit_sync_invokes_handler():
    bus = EventBus()
    handler = Mock()
    bus.on("user.login", handler)
    bus.emit(Event("user.login", payload={"user": "monke"}))
    handler.assert_called_once()
    assert handler.call_args.args[0].payload == {"user": "monke"}


def test_emit_sync_with_wildcard():
    bus = EventBus()
    handler = Mock()
    bus.on("user.*", handler)
    bus.emit(Event("user.login", payload={"user": "monke"}))
    bus.emit(Event("user.logout", payload={"user": "monke"}))
    assert handler.call_count == 2


def test_emit_sync_priority_order():
    bus = EventBus()
    order: list[str] = []
    bus.on("tick", lambda e: order.append("first"), priority=1)
    bus.on("tick", lambda e: order.append("second"), priority=0)
    bus.emit(Event("tick"))
    assert order == ["second", "first"]


def test_emit_once_removes_after_call():
    bus = EventBus()
    handler = Mock()
    bus.once("ping", handler)
    bus.emit(Event("ping"))
    bus.emit(Event("ping"))
    assert handler.call_count == 1


def test_emit_off_removes_handler():
    bus = EventBus()
    handler = Mock()
    bus.on("go", handler)
    bus.off("go", handler)
    bus.emit(Event("go"))
    handler.assert_not_called()


def test_emit_fail_fast_raises():
    bus = EventBus(fail_fast=True)
    second = Mock()
    bus.on("boom", lambda e: (_ for _ in ()).throw(RuntimeError("nope")))
    bus.on("boom", second)
    try:
        bus.emit(Event("boom"))
    except RuntimeError as exc:
        assert str(exc) == "nope"
    else:
        assert False, "expected RuntimeError"
    second.assert_not_called()


def test_emit_fail_silent_continues():
    bus = EventBus(fail_fast=False)
    second = Mock()
    bus.on("boom", lambda e: (_ for _ in ()).throw(RuntimeError("nope")))
    bus.on("boom", second)
    bus.emit(Event("boom"))
    second.assert_called_once()


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


def test_emit_async_runs_coroutines():
    bus = EventBus()
    result: list[int] = []

    async def ah(e: Event) -> None:
        result.append(1)

    bus.on("async_test", ah)
    asyncio.run(bus.emit_async(Event("async_test")))
    assert result == [1]


def test_emit_async_mixed_sync_and_async():
    bus = EventBus()
    sync_result: list[str] = []
    async_result: list[str] = []

    def sh(e: Event) -> None:
        sync_result.append("sync")

    async def ah(e: Event) -> None:
        async_result.append("async")

    bus.on("mixed", sh, priority=0)
    bus.on("mixed", ah, priority=1)
    asyncio.run(bus.emit_async(Event("mixed")))
    assert sync_result == ["sync"]
    assert async_result == ["async"]


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------


def test_glob_star_matches_anything():
    bus = EventBus()
    handler = Mock()
    bus.on("a.*.c", handler)
    bus.emit(Event("a.x.c"))
    bus.emit(Event("a.y.c"))
    bus.emit(Event("a.b.c"))
    assert handler.call_count == 3


def test_glob_no_match():
    bus = EventBus()
    handler = Mock()
    bus.on("a.x.c", handler)
    bus.emit(Event("a.x.d"))  # different token → no match
    assert handler.call_count == 0


# ---------------------------------------------------------------------------
# Subject field rename
# ---------------------------------------------------------------------------


def test_event_has_subject_field():
    e = Event("test.subject", labels=("a", "b"), payload={"x": 1})
    assert e.subject == "test.subject"
    assert e.labels == ("a", "b")
    assert e.payload == {"x": 1}


def test_pattern_matching_uses_subject():
    bus = EventBus()
    handler = Mock()
    bus.on("deploy.*", handler)
    bus.emit(Event("deploy.complete"))
    bus.emit(Event("deploy.failed"))
    bus.emit(Event("user.login"))
    assert handler.call_count == 2


# ---------------------------------------------------------------------------
# Subscribers
# ---------------------------------------------------------------------------


def test_subscribers_receive_all_events():
    bus = EventBus()
    received: list[Event] = []

    def sub(e: Event) -> None:
        received.append(e)

    bus.subscribe(sub)
    bus.emit(Event("foo"))
    bus.emit(Event("bar", payload={"x": 1}))
    assert len(received) == 2
    assert received[0].subject == "foo"
    assert received[1].payload == {"x": 1}


def test_subscribers_are_async():
    bus = EventBus()
    received: list[int] = []

    async def sub(e: Event) -> None:
        received.append(1)

    bus.subscribe(sub)
    asyncio.run(bus.emit_async(Event("go")))
    assert received == [1]


def test_subscribers_introspection():
    bus = EventBus()

    def my_sub(e: Event) -> None:
        pass

    bus.subscribe(my_sub)
    assert "my_sub" in bus.subscribers()
