.. module:: pulsar

.. _api:

==================
API
==================

.. _pulsar_primitives:

Pulsar is built on top of a set of **primitive** classes which handle the
different aspects of the concurrent framework.
These primitive classes are:

* :class:`EventLoop` to handle scheduling of asynchronous events.
* :class:`Deferred` to handle asynchronous call backs.
* :class:`Actor` manage parallel execution in threads or processes.

.. _pulsar_framework:

A second layer of classes, forming the concurrent framework, is built
directly on top of :ref:`pulsar primitives <pulsar_primitives>`. These
**framework** classes are:

* :class:`Arbiter` manages the execution of pulsar-powered applications.
* :class:`ProtocolConsumer` consume stream of data provided by the :class:`Protocol`.


.. toctree::
   :maxdepth: 1
   
   async
   actor
   protocols
   application
   utilities
   
