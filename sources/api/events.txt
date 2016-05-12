.. module:: pulsar.async.events

============
Event API
============

Events are classes implementing the :class:`AbstractEvent` interface

The :class:`EventHandler` class is for creating objects with events.
These events can occur once only during the life of an :class:`EventHandler`
or can occur several times. Check the
:ref:`event dispatching tutorial <event-handling>` for an overview.


Abstract Event
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AbstractEvent
   :members:
   :member-order: bysource


Event
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Event
   :members:
   :member-order: bysource


One time
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: OneTime
   :members:
   :member-order: bysource


Events Handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EventHandler
   :members:
   :member-order: bysource
