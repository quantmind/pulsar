.. _async-api:

.. module:: pulsar.async.futures

==================
Asynchonous API
==================

The ``pulsar.Future`` class is an alias for :class:`.asyncio.Future`


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

Both the :func:`in_loop` and :func:`in_loop_thread` can be applied to
member functions of classes for wich instances expose the ``_loop``
attribute (an instance of an event loop).


task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: task


In loop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: in_loop
