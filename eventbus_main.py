#!/usr/bin/env python3
"""Start the EventBus Monitor server."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running as `python demo.py` from the project root (src is on path)
src_dir = Path(__file__).resolve().parent / "src"
if src_dir.exists() and str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

import asyncio
import os
import signal

from eventbus import EventBus
from eventbus.monitor import MonitorMixin
from eventbus.stores import create_store


class DemoBus(MonitorMixin, EventBus):
    pass


async def main() -> None:
    max_events = int(os.environ.get("MAX_EVENTS", "5000"))
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8765"))

    store = create_store(
        db_path=os.environ.get("DB_PATH", "events.db"),
        max_events=max_events,
        db_host=os.environ.get("DB_HOST", "localhost"),
        db_port=int(os.environ.get("DB_PORT", "3306")),
        db_name=os.environ.get("DB_NAME", "eventbus"),
        db_user=os.environ.get("DB_USER", "root"),
        db_password=os.environ.get("DB_PASSWORD", ""),
    )
    bus = DemoBus(store=store)

    try:
        await bus.start_monitor(host, port)
    except Exception as e:
        print(f"[FATAL] Server failed to start: {e}", flush=True)
        raise
    task = None  # Already done
    print()
    backend = os.environ.get("STORE_BACKEND", "sqlite")
    print(f"  Store backend:      {backend}")
    print(f"  Dashboard ready at: http://localhost:{port}")
    print(f"  Events API:         http://localhost:{port}/events?cursor=0&limit=50")
    print(f"  Health check:       http://localhost:{port}/health")
    print(f"  Press Ctrl+C to stop")
    print()

    # Wait for shutdown signal
    loop = asyncio.get_event_loop()
    stop = loop.create_future()

    def _stop() -> None:
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    await stop

    print("\n[SHUTDOWN] Interrupted")
    await bus.stop_monitor()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
