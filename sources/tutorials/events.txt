.. _event-handling:

=======================
Events
=======================

Event handling is implemented via the :class:`.Event` and :class:`.OneTime`
classes. In addition the :class:`.EventHandler` provide a mixin which can
be used to attach events to a class. Event are implemented in the
:mod:`pulsar.async.events` module.

.. _many-times-event:

Many times event
====================

An event handler is created simply::

    event = Event()

You can bind callbacks to an event::

    def say_hello(arg, **kw):
        print('Hello %s!')

    event.bind(say_hello)

and you can fire it::

    >>> event.fire(None)
    Hello None!
    >>> event.fire('luca')
    Hello luca!

An :class:`.Event` can be fired as many times as you like and therefore we
referred to this type of event as a **may times event**.


.. _one-time-event:

One time event
=====================
As the name says, the :class:`.OneTime` is a special event which
can be fired once only. Firing a :class:`.OneTime` event multiple
times will cause an :class:`~asyncio.InavlidStateError` to be raised.

A :class:`.OneTime` event is a specialised :class:`~asyncio.Future` and
therefore you can yield it in a coroutine to wait until it get fired.


Event handler
=================
The :class:`.EventHandler` mixin adds event handling methods to python classes::

    import pulsar

    class Events(pulsar.EventHandler):
        ONE_TIME_EVENTS = ('start', 'finish')
        MANY_TIMES_EVENTS = ('data')


To fire an event, one uses the :meth:`~.EventHandler.fire_event` method with
first positional argument given by the event name::

    >> o = Events()
    >> o.fire_event('start')

Optionally, it is possible to pass one additional parameter::

    >> o.fire_event('start', 'hello')

Adding event callbacks is done via the :meth:`~.EventHandler.bind_event`
method. The method accept two parameters, the event name and a callable
accepting one parameters, the caller which fires the event or the
optional positional parameter passed to the :meth:`~.EventHandler.fire_event`
method mentioned above::

    def start_handler(result):
        ...

    o.bind_event('start', start_handler)
