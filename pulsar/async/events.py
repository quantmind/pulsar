import logging

from .access import create_future


__all__ = ['EventHandler', 'Event', 'OneTime', 'AbortEvent']


LOGGER = logging.getLogger('pulsar.events')


class AbortEvent(Exception):
    """Use this exception to abort events"""
    pass


class Event:
    """Abstract event handler
    """
    __slots__ = ('name', '_handlers', '_waiter', '_self')
    onetime = False

    def __init__(self, name, o):
        self.name = name
        self._handlers = None
        self._self = o
        self._waiter = None

    def __repr__(self):
        return '%s: %s' % (self.name, self._handlers)
    __str__ = __repr__

    @property
    def handlers(self):
        if self._handlers is None:
            self._handlers = []
        return self._handlers

    def bind(self, callback, *callbacks):
        """Bind a ``callback`` to this event.
        """
        handlers = self.handlers
        if callback not in handlers:
            handlers.append(callback)
        if callbacks:
            for callback in callbacks:
                if callback not in handlers:
                    handlers.append(callback)
        return self

    def clear(self):
        self._handlers = None

    def remove_callback(self, callback):
        """Remove a callback from the list
        """
        handlers = self._handlers
        if handlers:
            filtered_callbacks = [f for f in handlers if f != callback]
            removed_count = len(handlers) - len(filtered_callbacks)
            if removed_count:
                self.clear()
                self._handlers.extend(filtered_callbacks)
            return removed_count

    def fire(self, **kw):
        if self._handlers:
            o = self._self
            for hnd in self._handlers:
                try:
                    hnd(o, **kw)
                except Exception:
                    self.logger.exception('Exception while firing %s', self)


class OneTime(Event):
    '''An :class:`AbstractEvent` which can be fired once only.

    This event handler is a subclass of :class:`.Future`.
    Implemented mainly for the one time events of the :class:`EventHandler`.
    '''
    onetime = True

    def fire(self, **kw):
        if self._handlers:
            o = self._self
            self._handlers, handlers = None, self._handlers
            for hnd in handlers:
                hnd(o, **kw)
            self._self = None

    def waiter(self):
        if not self._waiter:
            self._waiter = create_future()
        return self._waiter


class EventHandler:
    '''A Mixin for handling events on :ref:`async objects <async-object>`.

    It handles :class:`OneTime` events and :class:`Event` that occur
    several times.
    '''
    ONE_TIME_EVENTS = None
    _events = None

    def event(self, name):
        '''Returns the :class:`Event` at ``name``.

        If no event is registered for ``name`` returns nothing.
        '''
        events = self._events
        if events is None:
            self._events = events = dict(((n, OneTime(n, self))
                                          for n in self.ONE_TIME_EVENTS))
        if name not in events:
            events[name] = Event(name, self)
        return events[name]

    def bind_events(self, **events):
        '''Register all known events found in ``events`` key-valued parameters.
        '''
        evs = self._events
        if evs:
            for event in evs.values():
                if event.name in events:
                    event.bind(events[event.name])

    def copy_many_times_events(self, other):
        '''Copy :ref:`many times events <many-times-event>` from  ``other``.

        All many times events of ``other`` are copied to this handler
        provided the events handlers already exist.
        '''
        events = self._events
        if events and other._events:
            for name, event in other._events.items():
                if not event.onetime and event._handlers:
                    ev = events.get(name)
                    # If the event is available add it
                    if ev:
                        for callback in event._handlers:
                            ev.bind(callback)
