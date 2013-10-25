.. module:: pulsar.async

.. _async-api:

==================
Asynchonous API
==================

Event loop
=================

Poller
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.pollers.Poller
   :members:
   :member-order: bysource


EventLoop
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.eventloop.EventLoop
   :members:
   :member-order: bysource


.. _async-discovery:

.. module:: pulsar.async.defer

Async Discovery Functions
=================================

This section describes the asynchronous discover functions which are used
throughout the library to access if objects are asynchronous or not.
There are two important functions: :func:`.maybe_async` and
:func:`.maybe_failure` for asynchronous exceptions.

Maybe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_async

Maybe Failure
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_failure


Set Async
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: set_async


Is failure
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: is_failure


Async Utilities
====================

A collection of asynchronous utilities which facilitates manipulation and
interaction with :ref:`asynchronous components <tutorials-coroutine>`.

Async
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: async


Multi Async
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: multi_async


Safe Async
~~~~~~~~~~~~~~~~~~~~~~~~~
.. function:: safe_async(callable, *args, **kwargs)

    Safely execute a ``callable`` and always return a :class:`Deferred`,
    even if the ``callable`` is not asynchronous. Never throws.


Async Sleep
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: async_sleep


Async While
~~~~~~~~~~~~~~~~~~
.. autofunction:: async_while


Run in loop thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: run_in_loop_thread


Asynchronous Classes
==========================

While :class:`Actor` represents the concurrent side of pulsar,
the :class:`Deferred` adds the asynchronous flavour to it by using callbacks
functions similar to twisted_.

Deferred
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Deferred
   :members:
   :member-order: bysource

Multi Deferred
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: MultiDeferred
   :members:
   :member-order: bysource

DeferredTask
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DeferredTask
   :members:
   :member-order: bysource

Failure
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Failure
   :members:
   :member-order: bysource


.. module:: pulsar.async.events

Events
============

The :class:`EventHandler` class is for creating objects with events.
These events can occur once only during the life of an :class:`EventHandler`
or can occur several times. Check the
:ref:`event dispatching tutorial <event-handling>` for an overview.

Event
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Event
   :members:
   :member-order: bysource

Events Handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EventHandler
   :members:
   :member-order: bysource


.. module:: pulsar.async.queues

Queues
=============

Queue
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Queue
   :members:
   :member-order: bysource


.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
.. _twisted: http://twistedmatrix.com/trac/
