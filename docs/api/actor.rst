.. module:: pulsar.api

.. _actor-api:

=======================
Actors API
=======================

For an overview of pulsar actors
check out the :ref:`design documentation <design-actor>`.


.. _spawn-function:

spawn
============

.. autofunction:: spawn

.. _send-function:

send
============

.. autofunction:: send


get_actor
============

.. function:: get_actor

    Returns the :class:`Actor` controlling the current thread.
    Returns ``None`` if no actor is available.

arbiter
============

.. autofunction:: arbiter


command
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: command

