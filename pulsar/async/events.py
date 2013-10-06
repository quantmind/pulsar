from inspect import isgenerator

from pulsar.utils.pep import itervalues

from .defer import Deferred, maybe_async
from .access import logger


__all__ = ['EventHandler', 'Event']


class Event(object):
    '''An event managed by an :class:`EventHandler` class.'''
    _silenced = False

    @property
    def silenced(self):
        '''Boolean indicating if this event is silenced.

        To silence an event one uses the :meth:`EventHandler.silence_event`
        method.
        '''
        return self._silenced

    def bind(self, callback, errback=None):
        '''Bind a ``callback`` for ``caller`` to this :class:`Event`.'''
        pass

    def has_fired(self):
        '''Check if this event has fired.

        This only make sense for one time events.
        '''
        return True

    def fire(self, arg, **kwargs):
        '''Fire this event.

        This method is called by the :meth:`EventHandler.fire_event` method.
        '''
        raise NotImplementedError

    def silence(self):
        '''Silence this event.

        A silenced event won't fire when the :meth:`fire` method is called.
        '''
        self._silenced = True

    def chain(self, event):
        raise NotImplementedError


class ManyEvent(Event):

    def __init__(self, name):
        self.name = name
        self._handlers = []

    def __repr__(self):
        return repr(self._handlers)
    __str__ = __repr__

    def bind(self, callback, errback=None):
        assert errback is None, 'errback not supported in many-times events'
        self._handlers.append(callback)

    def fire(self, arg, **kwargs):
        if not self._silenced:
            for hnd in self._handlers:
                try:
                    g = hnd(arg, **kwargs)
                except Exception:
                    logger().exception('Exception while firing "%s" '
                                       'event for %s', self.name, arg)
                else:
                    if isgenerator(g):
                        # Add it to the event loop
                        maybe_async(g)


class OneTime(Deferred, Event):

    def __init__(self, name):
        super(OneTime, self).__init__()
        self.name = name
        self._events = Deferred()

    def bind(self, callback, errback=None):
        self._events.add_callback(callback, errback)

    def has_fired(self):
        return self._events.done()

    def fire(self, arg, **kwargs):
        if not self._silenced:
            if self._events.done():
                logger().warning('Event "%s" already fired for %s',
                                 self.name, arg)
            else:
                assert not kwargs, ("One time events can don't support "
                                    "key-value parameters")
                result = self._events.callback(arg)
                if isinstance(result, Deferred):
                    # a deferred, add a check at the end of the callback pile
                    return self._events.add_callback(self._check, self._check)
                elif not self._chained_to:
                    return self.callback(result)

    def _check(self, result):
        if self._events._callbacks:
            # other callbacks have been added,
            # put another check at the end of the pile
            return self._events.add_callback(self._check, self._check)
        elif not self._chained_to:
            return self.callback(result)


class EventHandler(object):
    '''A Mixin for handling events.

    It handles one time events and events that occur several
    times. This mixin is used in :class:`Protocol` and :class:`Producer`
    for scheduling connections and requests.
    '''
    ONE_TIME_EVENTS = ()
    '''Event names which occur once only.'''
    MANY_TIMES_EVENTS = ()
    '''Event names which occur several times.'''
    def __init__(self, one_time_events=None, many_times_events=None):
        one = self.ONE_TIME_EVENTS
        if one_time_events:
            one = set(one)
            one.update(one_time_events)
        events = dict(((e, OneTime(e)) for e in one))
        many = self.MANY_TIMES_EVENTS
        if many_times_events:
            many = set(many)
            many.update(many_times_events)
        events.update(((e, ManyEvent(e)) for e in many))
        self._events = events

    @property
    def events(self):
        '''The dictionary of all events.
        '''
        return self._events

    def event(self, name):
        '''Return the :class:`Event` for ``name``.

        If no event is registered returns nothing.
        '''
        return self._events.get(name)

    def bind_event(self, name, callback, errback=None):
        '''Register a ``callback`` with ``event``.

        **The callback must be a callable accepting one parameter only**,
        the instance firing the event or the first positional argument
        passed to the :meth:`fire_event` method.

        :param name: the event name. If the event is not available a warning
            message is logged.
        :param callback: a callable receiving one positional parameter. It
            can also be a list/tuple of callables.
        :return: nothing.
        '''
        if name not in self._events:
            self._events[name] = ManyEvent(name)
        event = self._events[name]
        if isinstance(callback, (list, tuple)):
            assert errback is None, "list of callbacks with errback"
            for cbk in callback:
                event.bind(cbk)
        else:
            event.bind(callback, errback)

    def bind_events(self, **events):
        '''Register all known events found in ``events`` key-valued parameters.
        '''
        for name in self._events:
            if name in events:
                self.bind_event(name, events[name])

    def fire_event(self, name, arg=None, **kwargs):
        """Dispatches ``arg`` or ``self`` to event ``name`` listeners.

        * If event at ``name`` is a one-time event, it makes sure that it was
          not fired before.

        :param arg: optional argument passed as positional parameter to the
            event handler.
        :param kwargs: optional key-valued parameters to pass to the event
            handler. Can only be used for
            :ref:`many times events <many-times-event>`.
        :return: for one-time events, it returns whatever is returned by the
            event handler. For many times events it returns nothing.
        """
        if arg is None:
            arg = self
        if name in self._events:
            return self._events[name].fire(arg, **kwargs)
        else:
            logger().warning('Unknown event "%s" for %s', name, self)

    def silence_event(self, name):
        '''Silence event ``name``.

        This causes the event not to fire at the :meth:`fire_event` method
        is invoked with the event ``name``.
        '''
        event = self._events.get(name)
        if event:
            event.silence()

    def chain_event(self, other, name):
        '''Chain the event ``name`` from ``other``.

        :param other: an :class:`EventHandler` to chain to.
        :param name: event name to chain.
        '''
        event = self._events.get(name)
        if event and isinstance(other, EventHandler):
            event2 = other._events.get(name)
            if event2:
                event.chain(event2)

    def copy_many_times_events(self, other):
        '''Copy :ref:`many times events <many-times-event>` from  ``other``.

        All many times events of ``other`` are copied to this handler
        provided the events handlers already exist.
        '''
        if isinstance(other, EventHandler):
            events = self._events
            for event in itervalues(other._events):
                if isinstance(event, ManyEvent):
                    ev = events.get(event.name)
                    # If the event is available add it
                    if ev:
                        for callback in event._handlers:
                            ev.bind(callback)
