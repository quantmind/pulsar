try:
    from ..utils.lib import EventHandler, AbortEvent
except ImportError:
    from ..utils.events_py import EventHandler, AbortEvent


__all__ = ['EventHandler', 'AbortEvent']
