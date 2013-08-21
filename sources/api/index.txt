.. module:: pulsar

.. _api:

==================
API
==================

.. _pulsar_primitives:

Primitives
===============

Pulsar is built on top of a set of **primitive** classes which handle the
different aspects of the concurrent framework.
These primitive classes are:

* :class:`EventLoop`: handles scheduling of asynchronous and recurrent events.
  Designed to conform with pep-3156_ eventloop interface.
* :class:`Deferred`: handles asynchronous call backs. Designed along the
  lines of futures_ or promises, with a similar implementation
  of twisted_ deferred.
* :class:`Actor`: manage parallel execution in threads or processes.
  Each live actor has its own :class:`EventLoop`.
* :class:`Transport`: abstract class handling end-to-end communication
  services for applications. Designed to conform with pep-3156_ transport interface.
* :class:`ProtocolConsumer`: base class for consuming stream of data provided
  by the :class:`Transport` via a :class:`Server` or :class:`Client`
  :class:`Connection`.

.. _pulsar_framework:

Framework
===============

A second layer of classes, forming the concurrent framework, is built
directly on top of :ref:`pulsar primitives <pulsar_primitives>`. These
**framework** classes are:

* :class:`Arbiter` manages the execution of pulsar-powered applications.
* :class:`apps.Application` is the base class for all higher level pulsar usage.


Contents
===============

.. toctree::
   :maxdepth: 1
   
   async
   actor
   protocols
   stream
   application
   utilities
   exceptions
   

.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
.. _futures: http://en.wikipedia.org/wiki/Futures_and_promises
.. _twisted: http://twistedmatrix.com/