.. module:: pulsar

.. _event-handling:

=======================
Events
=======================

Event handling is implemented via the :class:`EventHandler` mixin which
implements two types of events: events which occurs one time only during the
life of the :class:`EventHandler` and events which can occurs several times::

    import pulsar

    class Events(pulsar.EventHandler):
        ONE_TIME_EVENTS = ('start', 'finish')
        MANY_TIMES_EVENTS = ('data')
        
        def __init__(self):
          super(Events, self).__init__()


To fire an event, one uses the :meth:`EventHandler.fire_event` method with
first positional argument given by the event name::

	>> o = Events()
	>> o.fire_event('start')

Optionally, it is possible to pass one additional parameter::

	>> o.fire_event('start', 'hello')
	
Adding event handlers is done via the :meth:`EventHandler.bind_event`
method. The method accept two parameters, the event name and a callable
accepting one parameters, the caller which fires the event or the
optional positional parameter passed to the :meth:`EventHandler.fire_event`
method mentioned above::

    def start_handler(result):
        ...
        
    o.bind_event('start', start_handler)
    
   
.. _one-time-event:

One time event
=====================
As the name says, they can be fired once only. Firing these events multiple
times won't have any effect other than a warning message from the logger.

For one time events it is possible to pass an optional
function called if the result passed to the :meth:`EventHandler.fire_event`
is a :class:`pulsar.Failure`::
 
    def error_handler(failure):
        ...
        
    o.bind_event('start', start_handler, error_handler)

.. _many-times-event:

Many times event
=====================
These events can be fired several times::

    >> o.fire_event('data', 5)
    >> o.fire_event('data', 12)


