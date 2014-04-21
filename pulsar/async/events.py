from collections import deque
from functools import partial

from pulsar.utils.pep import iteritems

from .futures import (Future, maybe_async, InvalidStateError,
                      future_result_exc, AsyncObject)


__all__ = ['EventHandler', 'Event', 'OneTime']


class AbstractEvent(AsyncObject):
    '''Abstract event handler.'''
    _silenced = False
    _handlers = None
    _fired = 0

    @property
    def silenced(self):
        '''Boolean indicating if this event is silenced.

        To silence an event one uses the :meth:`silence` method.
        '''
        return self._silenced

    @property
    def handlers(self):
        if self._handlers is None:
            self._handlers = []
        return self._handlers

    def bind(self, callback):
        '''Bind a ``callback`` to this event.
        '''
        self.handlers.append(callback)

    def fired(self):
        '''The number of times this event has fired'''
        return self._fired

    def fire(self, arg, **kwargs):
        '''Fire this event.'''
        raise NotImplementedError

    def clear(self):
        if self._handlers:
            self._handlers[:] = []

    def silence(self):
        '''Silence this event.

        A silenced event won't fire when the :meth:`fire` method is called.
        '''
        self._silenced = True


class Event(AbstractEvent):
    '''The default implementation of :class:`AbstractEvent`.
    '''
    def __init__(self, loop=None):
        self._loop = loop

    def __repr__(self):
        return repr(self._handlers)
    __str__ = __repr__

    def fire(self, arg, **kwargs):
        if not self._silenced:
            self._fired += self._fired + 1
            if self._handlers:
                for hnd in self._handlers:
                    try:
                        maybe_async(hnd(arg, **kwargs), self._loop)
                    except Exception:
                        self.logger.exception('Exception while firing event')
        return self


class OneTime(Future, AbstractEvent):
    '''An :class:`AbstractEvent` which can be fired once only.

    This event handler is a subclass of :class:`.Future`.
    Implemented mainly for the one time events of the :class:`EventHandler`.
    '''
    @property
    def handlers(self):
        if self._handlers is None:
            self._handlers = deque()
        return self._handlers

    def bind(self, callback):
        '''Bind a ``callback`` to this event.
        '''
        if not self.done():
            self.handlers.append(callback)
        else:
            result, exc = future_result_exc(self)
            self._loop.call_soon(
                lambda: maybe_async(callback(result, exc=exc), self._loop))

    def fire(self, arg, exc=None, **kwargs):
        '''The callback handlers registered via the :meth:~AbstractEvent.bind`
        method are executed first.

        :param arg: the argument
        :param exc: optional exception
        '''
        if not self._silenced:
            if self._fired:
                raise InvalidStateError('already fired')
            self._fired = 1
            self._process(arg, exc, kwargs)
        return self

    def clear(self):
        if self._handlers:
            self._handlers.clear()

    def _process(self, arg, exc, kwargs, future=None):
        while self._handlers:
            hnd = self._handlers.popleft()
            try:
                result = maybe_async(hnd(arg, exc=exc, **kwargs), self._loop)
            except Exception:
                self.logger.exception('Exception while firing event')
            else:
                if isinstance(result, Future):
                    result.add_done_callback(
                        partial(self._process, arg, exc, kwargs))
                    return
        if exc:
            self.set_exception(exc)
        else:
            self.set_result(arg)


class EventHandler(AsyncObject):
    '''A Mixin for handling events.

    It handles :class:`OneTime` events and :class:`Event` that occur
    several times.
    '''
    ONE_TIME_EVENTS = ()
    '''Event names which occur once only.'''
    MANY_TIMES_EVENTS = ()
    '''Event names which occur several times.'''
    def __init__(self, loop=None, one_time_events=None,
                 many_times_events=None):
        one = self.ONE_TIME_EVENTS
        if one_time_events:
            one = set(one)
            one.update(one_time_events)
        events = dict(((name, OneTime(loop=loop)) for name in one))
        many = self.MANY_TIMES_EVENTS
        if many_times_events:
            many = set(many)
            many.update(many_times_events)
        events.update(((name, Event(loop=loop)) for name in many))
        self._events = events

    @property
    def events(self):
        '''The dictionary of all events.
        '''
        return self._events

    def event(self, name):
        '''Returns the :class:`Event` at ``name``.

        If no event is registered for ``name`` returns nothing.
        '''
        event = self._events.get(name)
        if event:
            assert self._loop, "No event loop for %s" % self
            event._loop = self._loop
        return event

    def fired_event(self, name):
        event = self._events.get(name)
        return event._fired if event else 0

    def bind_event(self, name, callback):
        '''Register a ``callback`` with ``event``.

        The callback must be a callable accepting one positional parameter
        and at least the ``exc`` optional parameter::

            def callback(arg, ext=None):
                ...

            o.bind_event('start', callback)

        the instance firing the event or the first positional argument
        passed to the :meth:`fire_event` method.

        :param name: the event name. If the event is not available a warning
            message is logged.
        :param callback: a callable receiving one positional parameter. It
            can also be a list/tuple of callables.
        :return: nothing.
        '''
        if name not in self._events:
            self._events[name] = Event()
        event = self._events[name]
        event.bind(callback)

    def bind_events(self, **events):
        '''Register all known events found in ``events`` key-valued parameters.

        The events callbacks can be specified as a single callable or as
        list/tuple of callabacks or (callback, erroback) tuples.
        '''
        for name in self._events:
            if name in events:
                callbacks = events[name]
                self.bind_event(name, events[name])

    def fire_event(self, name, *args, **kwargs):
        """Dispatches ``arg`` or ``self`` to event ``name`` listeners.

        * If event at ``name`` is a one-time event, it makes sure that it was
          not fired before.

        :param args: optional argument passed as positional parameter to the
            event handler.
        :param kwargs: optional key-valued parameters to pass to the event
            handler. Can only be used for
            :ref:`many times events <many-times-event>`.
        :return: the :class:`Event` fired
        """
        if not args:
            arg = self
        elif len(args) == 1:
            arg = args[0]
        else:
            raise TypeError('fire_event expected at most 1 argument got %s' %
                            len(args))
        event = self.event(name)
        if event:
            try:
                event.fire(arg, **kwargs)
            except InvalidStateError:
                self.logger.error('Event %s already fired' % name)
            return event
        else:
            self.logger.warning('Unknown event "%s" for %s', name, self)

    def silence_event(self, name):
        '''Silence event ``name``.

        This causes the event not to fire at the :meth:`fire_event` method
        is invoked with the event ``name``.
        '''
        event = self._events.get(name)
        if event:
            event.silence()

    def copy_many_times_events(self, other):
        '''Copy :ref:`many times events <many-times-event>` from  ``other``.

        All many times events of ``other`` are copied to this handler
        provided the events handlers already exist.
        '''
        if isinstance(other, EventHandler):
            events = self._events
            for name, event in iteritems(other._events):
                if isinstance(event, Event) and event._handlers:
                    ev = events.get(name)
                    # If the event is available add it
                    if ev:
                        for callback in event._handlers:
                            ev.bind(callback)
