.. _async-api:

.. module:: pulsar.async.futures

==================
Asynchonous API
==================

Pulsar asynchronous api is built on top of the new python :mod:`asyncio`
module, therefore ``pulsar.Future`` class is an alias for
:class:`.asyncio.Future`.

However, pulsar can run on python 2.7 and 3.3+ using the same code base.
To achieve this, pulsar uses a specialised :class:`asyncio.Task` class
with the following features:

* works with both ``yield`` and ``yield from``
* tolerant of synchronous values


Async object interface
=================================

This small class is the default interface for
:ref:`asynchronous objects <async-object>`. It is provided mainly for
documentation purposes.

.. autoclass:: AsyncObject
   :members:
   :member-order: bysource

.. autoclass:: Bench
   :members:
   :member-order: bysource


.. _async-discovery:


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


Chain Future
~~~~~~~~~~~~~~~~~~
.. autofunction:: chain_future


Coroutine return
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: coroutine_return


Add async binding
~~~~~~~~~~~~~~~~~~~~~~
.. function:: add_async_binding(binding)

    Add a third-party asynchronous ``binding`` to pulsar asynchronous engine.

    ``binding`` is a function which accept one parameter only and must return
    ``None`` or a :class:`~asyncio.Future`.


Multi Async
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: multi_async


Async While
~~~~~~~~~~~~~~~~~~
.. autofunction:: async_while


Run in loop
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: run_in_loop


Async Decorators
=====================

Both the :func:`task` and :func:`in_loop` decorator can be applied to
member functions of :ref:`async objects <async-object>`.


task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: task


In loop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: in_loop


.. module:: pulsar.async.threads

Executor
================

Classes used by pulsar to create event loop executors.

Thread pool
~~~~~~~~~~~~~~~~

.. autoclass:: ThreadPool
   :members:
   :member-order: bysource
