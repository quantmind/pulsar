.. module:: pulsar

.. _api:

==================
API
==================

.. _pulsar_primitives:

Pulsar is built on top of a set of **primitive** classes which handle the
different aspects of the asynchronous concurrent framework.
These primitive classes are:

* :class:`Deferred` the primitive for handling asynchronous execution.
* :class:`Actor` the primitive for handling parallel execution.
* :class:`IOLoop` the primitive for handling asynchronous events.
* :class:`Socket` the primitive for sockets.

.. _pulsar_framework:

A second layer of classes, forming the concurrent framework, is built
directly on top of :ref:`pulsar primitives <pulsar_primitives>`. These
**framework** classes are:

* :class:`Arbiter` manages the execution of pulsar-powered applications.
* :class:`AsyncIOStream` the primitive for handling asynchronous IO on a :class:`Socket`.
* :class:`AsyncSocketServer` base class for all asynchronous servers performing IO on
  a :class:`Socket`.



High level functions
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

    Return the :class:`Actor` in the current thread/process

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


.. _api-async-tools:

Asyncronous Tools
=====================

This section describes the asynchronous utilities used throughout the library
and which form the building block of the event driven concurrent framework.
While :class:`Actor` represents the concurrent side of pulsar,
the :class:`Deferred` adds the asynchronous flavour to it by using callbacks
functions similar to twisted_.

Make Async
~~~~~~~~~~~~~~~~~

.. autofunction:: make_async


Safe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: safe_async


Maybe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_async


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
   
Deferred Generator
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DeferredGenerator
   :members:
   :member-order: bysource
   
Failure
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Failure
   :members:
   :member-order: bysource


Decorators
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: async

.. autofunction:: multi_async

.. autofunction:: raise_failure


Sockets
================

handling asynchronous sockets is an important task in pulsar. The core component
for asynchronous I/O on sockets is the :class:`AsyncIOStream` class which has been
adapted from tornado_ web server.

Base Socket
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: BaseSocket
   :members:
   :member-order: bysource
   
   
Socket
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Socket
   :members:
   :member-order: bysource
   
   
AsyncIOStream
~~~~~~~~~~~~~~~~~

.. autoclass:: AsyncIOStream
   :members:
   :member-order: bysource


Socket with Protocol
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtocolSocket
   :members:
   :member-order: bysource
   
AsyncSocketServer
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AsyncSocketServer
   :members:
   :member-order: bysource
   
AsyncConnection
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AsyncConnection
   :members:
   :member-order: bysource
   
AsyncResponse
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AsyncResponse
   :members:
   :member-order: bysource
   
   
Utilities
~~~~~~~~~~~~~~~

.. autofunction:: create_socket

.. autofunction:: socket_pair
   
Eventloop
=================

IOLoop
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: IOLoop
   :members:
   :member-order: bysource
     
IOQueue
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: IOQueue
   :members:
   :member-order: bysource
 
PeriodicCallback
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PeriodicCallback
   :members:
   :member-order: bysource
   
   
Application
========================

.. autoclass:: Application
   :members:
   :member-order: bysource


Application Worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: Worker
   :members:
   :member-order: bysource


Application Monitor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: ApplicationMonitor
   :members:
   :member-order: bysource
   
   
.. _api-config:

Utilities
===============

Config
~~~~~~~~~~

.. autoclass:: pulsar.Config
   :members:
   :member-order: bysource
   
Setting
~~~~~~~~~~

.. autoclass:: pulsar.Setting
   :members:
   :member-order: bysource

Exceptions
~~~~~~~~~~~~~~~

.. autoclass:: PulsarException
   :members:
   :member-order: bysource
   

.. autoclass:: AlreadyCalledError
   :members:
   :member-order: bysource
     
      
.. _internals:


Internals
=======================


.. module:: pulsar.utils.system

System info
~~~~~~~~~~~~~~~~~

.. autofunction:: system_info



.. module:: pulsar.utils.tools

checkarity
~~~~~~~~~~~~~~~~~

.. autofunction:: checkarity




.. tornado: http://www.tornadoweb.org/