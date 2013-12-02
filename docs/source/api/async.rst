.. module:: pulsar.async

.. _async-api:

==================
Asynchonous API
==================

Event loop
=================

get event loop
~~~~~~~~~~~~~~~~~~~~~~~~

.. function:: get_event_loop()

    Returns the event loop in the current context. If a loop is not available
    it returns nothing.


new event loop
~~~~~~~~~~~~~~~~~~~~~~~

.. function:: new_event_loop(**kw)

    Build a new event loop.


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

Async Utilities
=================================

A collection of asynchronous utilities which facilitates manipulation and
interaction with :ref:`asynchronous components <tutorials-coroutine>`.

Async
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: async


Maybe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_async


Safe Async
~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: safe_async


Maybe Failure
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_failure


Add async binding
~~~~~~~~~~~~~~~~~~~~~~
.. function:: add_async_binding(binding)

    Add a third-party asynchronous ``binding`` to pulsar asynchronous engine.

    ``binding`` is a function which accept one parameter only and must return
    ``None`` or a :class:`.Deferred`.


Is failure
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: is_failure


Multi Async
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: multi_async


Async Sleep
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: async_sleep


Async While
~~~~~~~~~~~~~~~~~~
.. autofunction:: async_while

Run in loop thread
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: run_in_loop_thread


Async Decorators
=====================

Both the :func:`in_loop` and :func:`in_loop_thread` can be applied to
member functions of classes for wich instances expose the ``_loop``
attribute (an instance of an event loop).

In loop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: in_loop


In loop thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: in_loop_thread


.. module:: pulsar.async.fallbacks.defer

Deferred
==========================

While :class:`.Actor` represents the concurrent side of pulsar,
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
