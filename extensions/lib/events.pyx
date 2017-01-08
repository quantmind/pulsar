import logging
from asyncio import get_event_loop


LOGGER = logging.getLogger('pulsar.events')


class AbortEvent(Exception):
    """Use this exception to abort events"""
    pass


class EventHandler:
    '''A Mixin for handling events on :ref:`async objects <async-object>`.

    It handles :class:`OneTime` events and :class:`Event` that occur
    several times.
    '''
    ONE_TIME_EVENTS = None
    _events = None

    def event(self, str name):
        '''Returns the :class:`Event` at ``name``.

        If no event is registered for ``name`` returns nothing.
        '''
        if self._events is None:
            self._events = {}
            populate_events(self, self._events)
        return get_event(self, self._events, name)

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
            copy_many_times_events(events, other._events)


cdef class Event:
    """Abstract event handler
    """
    cdef int _onetime
    cdef str name
    cdef list _handlers
    cdef object _self
    cdef object _waiter

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
        return self._self == None

    cpdef list handlers(self):
        return self._handlers

    cpdef void bind(self, object callback):
        """Bind a ``callback`` to this event.
        """
        cdef list handlers = self._handlers
        if handlers is None:
            handlers = []
            self._handlers = handlers
        if callback not in handlers:
            handlers.append(callback)

    cpdef void clear(self):
        self._handlers = None

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

    cpdef void fire(self, object exc=None, object data=None):
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
                    self._waiter.set_result(o)
                self._waiter = None

    def waiter(self):
        if not self._waiter:
            self._waiter = get_event_loop().create_future()
        return self._waiter


cdef void populate_events(object self, dict events):
    cdef tuple ot = self.ONE_TIME_EVENTS or ()
    for n in ot:
        events[n] = Event(n, self, 1)


cdef object get_event(object self, dict events, str name):
    if name not in events:
        event = Event(name, self, 0)
        events[name] = event
    return events[name]


cdef void copy_many_times_events(dict events, dict other_events):
    cdef str name
    cdef Event event
    cdef list handlers
    cdef object callback

    for name, event in other_events.items():
        handlers = event.handlers()
        if not event.onetime() and handlers:
            ev = events.get(name)
            # If the event is available add it
            if ev:
                for callback in handlers:
                    ev.bind(callback)
