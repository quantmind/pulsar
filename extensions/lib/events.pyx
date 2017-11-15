import logging
from asyncio import get_event_loop


LOGGER = logging.getLogger('pulsar.events')


class AbortEvent(Exception):
    """Use this exception to abort events"""
    pass


cdef class EventHandler:
    ONE_TIME_EVENTS = None

    cpdef dict events(self):
        if self._events is None:
            self._events = {}
            for n in self.ONE_TIME_EVENTS or ():
                self._events[n] = Event(n, self, 1)
        return self._events

    cpdef Event event(self, str name):
        """Returns the :event at ``name``.
        """
        cdef dict events = self.events()
        if name is None:
            raise ValueError('event name must be a string')
        if name not in events:
            event = Event(name, self, 0)
            events[name] = event
        return events[name]

    cpdef fire_event(self, str name, exc=None, data=None):
        cdef dict events = self.events()
        if name in events:
            events[name].fire(exc=exc, data=data)

    cpdef bind_events(self, dict events):
        '''Register all known events found in ``events`` key-valued parameters.
        '''
        cdef dict evs = self.events()
        if evs and events:
            for event in evs.values():
                if event.name in events:
                    event.bind(events[event.name])

    cpdef reset_event(self, str name):
        cdef dict events = self.events()
        cdef Event event;

        if name in events:
            event = events[name]
            events[name] = Event(name, self, 1 if event.onetime() else 0)

    cpdef copy_many_times_events(self, EventHandler other):
        '''Copy :ref:`many times events <many-times-event>` from  ``other``.

        All many times events of ``other`` are copied to this handler
        provided the events handlers already exist.
        '''
        cdef dict events = self.events()
        cdef dict other_events = other.events() if other else None
        cdef str name
        cdef Event event
        cdef list handlers
        cdef object callback

        if events and other_events:
            for name, event in other_events.items():
                handlers = event.handlers()
                if not event.onetime() and handlers:
                    ev = events.get(name)
                    # If the event is available add it
                    if ev:
                        for callback in handlers:
                            ev.bind(callback)


cdef class Event:

    def __cinit__(self, str name, object o, int onetime):
        self.name = name
        self._onetime = onetime
        self._self = o

    def __repr__(self):
        return '%s: %s' % (self.name, self._handlers)
    __str__ = __repr__

    cpdef object onetime(self):
        return bool(self._onetime)

    cpdef object fired(self):
        return self._self is None

    cpdef list handlers(self):
        return self._handlers

    cpdef bind(self, object callback):
        """Bind a ``callback`` to this event.
        """
        cdef list handlers = self._handlers
        if self._self is None:
            raise RuntimeError('%s already fired, cannot add callbacks' % self)
        if handlers is None:
            handlers = []
            self._handlers = handlers
        handlers.append(callback)

    cpdef clear(self):
        self._handlers = None
        return self

    cpdef int unbind(self, object callback):
        """Remove a callback from the list
        """
        cdef list handlers = self._handlers
        cdef list filtered_callbacks
        cdef int removed_count
        if handlers:
            filtered_callbacks = [f for f in handlers if f != callback]
            removed_count = len(handlers) - len(filtered_callbacks)
            if removed_count:
                self._handlers = filtered_callbacks
            return removed_count
        return 0

    cpdef fire(self, exc=None, data=None):
        cdef object o = self._self
        cdef list handlers

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

    cpdef object waiter(self):
        if not self._waiter:
            self._waiter = get_event_loop().create_future()
            if self.fired():
                self._waiter.set_result(None)
        return self._waiter
