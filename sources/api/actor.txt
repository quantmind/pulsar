.. module:: pulsar.async.actor

.. _actor-api:

=======================
Actors API
=======================

For an overview of pulsar :class:`.Actor`
check out the :ref:`design documentation <design-actor>`.


High Level Functions
=======================

.. _spawn-function:

spawn
~~~~~~~~~~~~~~

.. autofunction:: pulsar.async.actor.spawn

.. _send-function:

send
~~~~~~~~~~~~~~

.. autofunction:: send


get_actor
~~~~~~~~~~~~~~

.. function:: get_actor

    Returns the :class:`Actor` controlling the current thread.
    Returns ``None`` if no actor is available.

arbiter
~~~~~~~~~~~~~~

.. autofunction:: pulsar.async.concurrency.arbiter


.. _api-actors:

Actor
=======================

At the core of the library we have the :class:`Actor` class which defines
the primitive of pulsar concurrent framework. Actor's instances communicate
with each other via messages in a *share-nothing architecture*.


.. autoclass:: Actor
   :members:
   :member-order: bysource


Actor Internals
=======================


ActorProxy
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.proxy.ActorProxy
   :members:
   :member-order: bysource


ActorProxyMonitor
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.proxy.ActorProxyMonitor
   :members:
   :member-order: bysource


.. _api-remote_commands:

Messages
====================

:class:`Actor` communicate with each other via the :func:`send` function
which uses the via :attr:`Actor.mailbox` attribute of the actor in the
current context. When an actor communicate with
another remote actor it does so by *sending* an **action** to it
with positional and/or key-valued arguments. For example::

    send(target, 'ping')

will :ref:`send <send-function>` the *ping* action to *target* from the actor
in the current context of execution. The above is equivalent to::

    get_actor().send(target, 'ping')


Each action is implemented via the :func:`command` decorator implemented
in the :mod:`pulsar.async.commands` module.
A :ref:`list of standard commands <actor_commands>`
is available in the design documentation.

pulsar command
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.commands.command


.. module:: pulsar.async.concurrency

Concurrency
==================

The :class:`Concurrency` class implements the behaviour of an :class:`.Actor`
and therefore allows for decoupling between the :class:`.Actor` abstraction
and its implementation (`bridge pattern`).

Base Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Concurrency
   :members:
   :member-order: bysource


Monitor Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: MonitorConcurrency
   :members:
   :member-order: bysource


Arbiter Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ArbiterConcurrency
   :members:
   :member-order: bysource


Constants
=========================

.. automodule:: pulsar.async.consts
   :members:
   :member-order: bysource
