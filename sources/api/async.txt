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


task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: task


Maybe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_async


Chain Future
~~~~~~~~~~~~~~~~~~
.. autofunction:: chain_future



Multi Async
~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: multi_async


Async While
~~~~~~~~~~~~~~~~~~
.. autofunction:: async_while


Run in loop
~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: run_in_loop
