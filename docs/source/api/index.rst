.. module:: pulsar

.. _api:

==================
API
==================

.. _pulsar_primitives:

Pulsar is built on top of a set of **primitive** classes which handle the
different aspects of the concurrent framework.
These primitive classes are:

* :class:`IOLoop` the primitive for handling asynchronous events.
* :class:`Deferred` the primitive for handling asynchronous execution.
* :class:`Protocol` the primitive for handling parallel execution.
* :class:`Actor` the primitive for handling parallel execution.

.. _pulsar_framework:

A second layer of classes, forming the concurrent framework, is built
directly on top of :ref:`pulsar primitives <pulsar_primitives>`. These
**framework** classes are:

* :class:`Arbiter` manages the execution of pulsar-powered applications.
* :class:`AsyncIOStream` the primitive for handling asynchronous IO on a :class:`Socket`.
* :class:`AsyncSocketServer` base class for all asynchronous servers performing IO on
  a :class:`Socket`.


.. toctree::
   :maxdepth: 1
   
   async
   actor
   protocols
   application
   utility
   