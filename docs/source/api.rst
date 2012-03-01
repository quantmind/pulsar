.. module:: pulsar

.. _api:

==================
API
==================

High level functions
=======================

spawn
~~~~~~~~~~~~~~

.. autofunction:: spawn

send
~~~~~~~~~~~~~~

.. autofunction:: send

get_actor
~~~~~~~~~~~~~~

.. autofunction:: get_actor

.. _api-actors:

Actors
=======================

At the core of the library we have the :class:`Actor` class which defines
the primitive of pulsar concurrent framework. Actor's instances communicate
with each other via messages in a *share-nothing architecture*. 

Actor Metaclass
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorMetaClass
   :members:
   :member-order: bysource
   
   
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
   

ActorImpl
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorImpl
   :members:
   :member-order: bysource


ActorLink
~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorLink
   :members:
   :member-order: bysource

ActorLinkCallback
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorLinkCallback
   :members:
   :member-order: bysource
   
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


Asynchronous Pair
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: async_pair


Deferred
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Deferred
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


Mailbox
~~~~~~~~~~~~~~

.. autoclass:: Mailbox
   :members:
   :member-order: bysource


Sockets
================

Create Socket
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: create_socket


Socket Pair
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: socket_pair


Socket
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Socket
   :members:
   :member-order: bysource
   
   
IOStream
~~~~~~~~~~~~~~~~~

.. autoclass:: IOStream
   :members:
   :member-order: bysource
   
   
AsyncIOStream
~~~~~~~~~~~~~~~~~

.. autoclass:: AsyncIOStream
   :members:
   :member-order: bysource


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
   

Exceptions
===================

.. autoclass:: PulsarException
   :members:
   :member-order: bysource
   

.. autoclass:: AlreadyCalledError
   :members:
   :member-order: bysource
   

.. _net:

.. module:: pulsar.net

Net
========================
   

HttpRequest
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpRequest
   :members:
   :member-order: bysource
   
  
HttpResponse
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpResponse
   :members:
   :member-order: bysource
   

HttpClient
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: HttpClient


HttpClientHandler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpClientHandler
   :members:
   :member-order: bysource
   
   
HttpClientResponse
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpClientResponse
   :members:
   :member-order: bysource

   