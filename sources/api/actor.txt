.. module:: pulsar

.. _actor-api:

=======================
Actors API
=======================


High Level Functions
=======================

.. _spawn-function:

spawn
~~~~~~~~~~~~~~

.. autofunction:: spawn

.. _send-function:

send
~~~~~~~~~~~~~~

.. autofunction:: send

get_actor
~~~~~~~~~~~~~~

.. function:: get_actor

    Return the :class:`Actor` controlling the current thread.

arbiter
~~~~~~~~~~~~~~

.. autofunction:: arbiter


.. _api-actors:

Actors
=======================

At the core of the library we have the :class:`Actor` class which defines
the primitive of pulsar concurrent framework. Actor's instances communicate
with each other via messages in a *share-nothing architecture*.
   
Actor
~~~~~~~~~~~~~~

.. autoclass:: Actor
   :members:
   :member-order: bysource

Monitor
~~~~~~~~~~~~~~~~~~

.. autoclass:: Monitor
   :members:
   :member-order: bysource


Arbiter
~~~~~~~~~~~~~~~~~~

.. autoclass:: Arbiter
   :members:
   :member-order: bysource
      

Actor Internals
=======================

PoolMixin
~~~~~~~~~~~~~~~~~~

.. autoclass:: PoolMixin
   :members:
   :member-order: bysource


ActorProxy
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorProxy
   :members:
   :member-order: bysource
   
   
ActorProxyDeferred
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorProxyDeferred
   :members:
   :member-order: bysource
   
   
ActorProxyMonitor
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorProxyMonitor
   :members:
   :member-order: bysource


ThreadPool
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ThreadPool
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
in the :mod:`pulsar.async.commands` module. A :ref:`list of standard commands <actor_commands>`
is available in the design documentation. 

pulsar command
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: command


Mailbox Consumer
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.mailbox.MailboxConsumer
   :members:
   :member-order: bysource


Concurrency
==================

The :class:`Concurrency` class implements the behaviour of an :class:`Actor`
and therefore allows for decoupling between the :class:`Actor` abstraction
and its implementation (`bridge pattern`).

Base Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Concurrency
   :members:
   :member-order: bysource

   
Monitor Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.concurrency.MonitorConcurrency
   :members:
   :member-order: bysource
   
   
Arbiter Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.concurrency.ArbiterConcurrency
   :members:
   :member-order: bysource
   
   
Constants
=========================

.. automodule:: pulsar.async.consts
   :members:
   :member-order: bysource