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


PoolMixin
~~~~~~~~~~~~~~~~~~

.. autoclass:: PoolMixin
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
   
   
ActorMessage
~~~~~~~~~~~~~~

.. autoclass:: ActorMessage
   :members:
   :member-order: bysource
   

Mailbox
~~~~~~~~~~~~~~

.. autoclass:: Mailbox
   :members:
   :member-order: bysource
   
   
Concurrency
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Concurrency
   :members:
   :member-order: bysource


.. _api-remote_commands:

Remote Commands
====================

:class:`Actor` communicate with each other via :class:`Mailbox` which
each actor has in its process domain. When an actor communicate with
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
