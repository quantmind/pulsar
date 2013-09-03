.. module:: pulsar

.. _async-api:

==================
Asynchonous API
==================

Event loop
=================

Poller
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Poller
   :members:
   :member-order: bysource
   
   
Timed Call
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TimedCall
   :members:
   :member-order: bysource
   
   
EventLoop
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EventLoop
   :members:
   :member-order: bysource
      

.. _async-discovery:

Async Discovery Functions
=================================

This section describes the asynchronous discover functions which are used
throughout the library to access if objects are asynchronous or not.
There are two important functions: :func:`maybe_async` and :func:`maybe_failure`
for asynchronous exceptions.

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
   
Task
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Task
   :members:
   :member-order: bysource
   
Failure
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Failure
   :members:
   :member-order: bysource

Events Handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EventHandler
   :members:
   :member-order: bysource

Queues
=============

Queue
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Queue
   :members:
   :member-order: bysource


.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
.. _twisted: http://twistedmatrix.com/trac/