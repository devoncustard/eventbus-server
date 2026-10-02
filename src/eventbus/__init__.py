from .bus import EventBus
from .event import Event
from .monitor import MonitorMixin
from .stores import EventStoreInterface, SQLiteEventStore, MySQLEventStore, create_store

# Backwards compat alias
EventStore = SQLiteEventStore

__version__ = "0.1.0"

__all__ = [
    "Event",
    "EventBus",
    "MonitorMixin",
    "EventStore",
    "EventStoreInterface",
    "SQLiteEventStore",
    "MySQLEventStore",
    "create_store",
    "__version__",
]
