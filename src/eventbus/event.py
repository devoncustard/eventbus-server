from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Event:
    """An immutable event carrying a subject, labels, payload, and origin.

    Args:
        subject: Event identifier (e.g. "user.login").
        labels: Ordered list of label strings for filtering/pulling.
        payload: Arbitrary event data.
        source: Origin of the event (e.g. service name, host, module).
    """

    subject: str
    labels: tuple[str, ...] = field(default_factory=tuple)
    payload: dict[str, Any] = field(default_factory=dict)
    source: str | None = field(default=None)
    _wall_ts: float = field(default_factory=time.time)
    _mono_ts: float = field(default_factory=time.monotonic)

    @property
    def timestamp(self) -> float:
        """Wall-clock timestamp (seconds since epoch) for display."""
        return self._wall_ts

    @property
    def mono_timestamp(self) -> float:
        """Monotonic timestamp (seconds) for ordering and deltas."""
        return self._mono_ts
