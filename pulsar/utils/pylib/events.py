import logging
from asyncio import get_event_loop


LOGGER = logging.getLogger('pulsar.events')


class AbortEvent(Exception):
    """Use this exception to abort events"""
    pass


class Event:
    __slots__ = ('name', '_onetime', '_handlers', '_waiter', '_self')

    def __init__(self, name, o, onetime):
        self.name = name
        self._onetime = onetime
        self._self = o
        self._handlers = None
        self._waiter = None

    def __repr__(self):
        return '%s: %s' % (self.name, self._handlers)

    __str__ = __repr__

    def handlers(self):
        return self._handlers

    def onetime(self):
        return bool(self._onetime)

    def fired(self):
        """Returns true or false depending if this event was fired

        One-time events only can be fired
        """
        return self._self is None

    def bind(self, callback):
        """Bind a ``callback`` to this event.
        """
        handlers = self._handlers
        if self._self is None:
            raise RuntimeError('%s already fired, cannot add callbacks' % self)
        if handlers is None:
            handlers = []
            self._handlers = handlers
        handlers.append(callback)

    def clear(self):
        self._handlers = None
        return self

    def unbind(self, callback):
        """Remove a callback from the list
        """
        handlers = self._handlers
        if handlers:
            filtered_callbacks = [f for f in handlers if f != callback]
            removed_count = len(handlers) - len(filtered_callbacks)
            if removed_count:
                self._handlers = filtered_callbacks
            return removed_count
        return 0

    def fire(self, exc=None, data=None):
        """Fire the event

        :param exc: fire the event with an exception
        :param data: fire an event with data
        """
        o = self._self

        if o is not None:
            handlers = self._handlers
            if self._onetime:
                self._handlers = None
                self._self = None

            if handlers:
                if exc is not None:
                    for hnd in handlers:
                        hnd(o, exc=exc)
                elif data is not None:
                    for hnd in handlers:
                        hnd(o, data=data)
                else:
                    for hnd in handlers:
                        hnd(o)

            if self._waiter:
                if exc:
                    self._waiter.set_exception(exc)
                else:
                    self._waiter.set_result(data if data is not None else o)
                self._waiter = None

    def waiter(self):
        """Return a :class:`~asyncio.Future` called back once the event
        has been fired.
        If the event has been fired already return a resolved future.

        This method is available only for one-time events
        """
        assert self._onetime, 'One time events only can invoke waiter'
        if not self._waiter:
            self._waiter = get_event_loop().create_future()
            if self.fired():
                self._waiter.set_result(None)
        return self._waiter


class EventHandler:
    '''A Mixin for handling events on :ref:`async objects <async-object>`.

    It handles :class:`OneTime` events and :class:`Event` that occur
    several times.
    '''
    ONE_TIME_EVENTS = None
    _events = None

    def events(self):
        if self._events is None:
            ot = self.ONE_TIME_EVENTS or ()
            self._events = dict(((n, Event(n, self, 1)) for n in ot))
        return self._events

    def event(self, name):
        """Returns the :class:`Event` at ``name``.

        If no event is registered for ``name`` creates a new :class:`Event`
        object and returns it.
        """
        events = self.events()
        if name not in events:
            events[name] = Event(name, self, 0)
        return events[name]

    def fire_event(self, name, exc=None, data=None):
        """Fire event at ``name`` if it is registered
        """
        if self._events and name in self._events:
            self._events[name].fire(exc=exc, data=data)

    def bind_events(self, events):
        '''Register all known events found in ``events`` key-valued parameters.
        '''
        evs = self._events
        if evs and events:
            for event in evs.values():
                if event.name in events:
                    event.bind(events[event.name])

    def copy_many_times_events(self, other):
        '''Copy :ref:`many times events <many-times-event>` from  ``other``.

        All many times events of ``other`` are copied to this handler
        provided the events handlers already exist.
        '''
        events = self.events()
        other_events = other.events()
        if events and other_events:
            for name, event in other_events.items():
                handlers = event.handlers()
                if not event.onetime() and handlers:
                    ev = events.get(name)
                    # If the event is available add it
                    if ev:
                        for callback in handlers:
                            ev.bind(callback)
