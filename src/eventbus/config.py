"""Configuration discovery for EventBus."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict


@dataclass
class EventBusConfig:
    """Serializable configuration describing how to connect to the event bus."""

    host: str = "0.0.0.0"
    port: int = 8765
    dashboard_url: str = ""
    events_url: str = ""
    health_url: str = ""
    ws_url: str = ""
    max_events: int = 5000
    store_backend: str = "sqlite"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def save(self, path: str = "config.json") -> None:
        import os
        from pathlib import Path

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())

    @classmethod
    def load(cls, path: str = "config.json") -> "EventBusConfig":
        with open(path) as f:
            return cls(**json.load(f))
