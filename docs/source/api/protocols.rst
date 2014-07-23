
.. _protocol-api:

================================
Protocols/Transports API
================================

This part of the :ref:`pulsar API <api>` is about classes responsible for
implementing the Protocol/Transport paradigm. They are based on
:class:`asyncio.Protocol` and :class:`asyncio.DatagramProtocol` classes.


.. module:: pulsar.async.protocols

Protocols
=================


PulsarProtocol
~~~~~~~~~~~~~~~~~~
.. autoclass:: PulsarProtocol
   :members:
   :member-order: bysource

Protocol
~~~~~~~~~~~~~~~~~~
.. autoclass:: Protocol
   :members:
   :member-order: bysource

Connection
~~~~~~~~~~~~~~
.. autoclass:: Connection
   :members:
   :member-order: bysource

Protocol Consumer
~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: ProtocolConsumer
   :members:
   :member-order: bysource


Producers
=================

Producers are factory of :class:`.Protocol` with end-points.
They are used by both servers and clients classes.

Producer
~~~~~~~~~~~~~~~~~
.. autoclass:: Producer
   :members:
   :member-order: bysource


TCP Server
~~~~~~~~~~~~~~~~~

.. autoclass:: TcpServer
   :members:
   :member-order: bysource


UDP
=====

Classes for the (user) datagram protocol. UDP uses a simple transmission
model with a minimum of protocol mechanism.


Datagram Protocol
~~~~~~~~~~~~~~~~~~
.. autoclass:: DatagramProtocol
   :members:
   :member-order: bysource

Datagram Server
~~~~~~~~~~~~~~~~~~
.. autoclass:: DatagramServer
   :members:
   :member-order: bysource


.. module:: pulsar.async.mixins

.. _protocol-mixins-api:

Protocol Mixins
=====================

FlowControl
~~~~~~~~~~~~~~~~~~
.. autoclass:: FlowControl
   :members:
   :member-order: bysource

Timeout
~~~~~~~~~~~~~~
.. autoclass:: Timeout
   :members:
   :member-order: bysource
   

.. module:: pulsar.async.clients

.. _clients-api:

Clients
=================


This section introduces classes implementing the transport/protocol paradigm
for clients with several connections to a remote :class:`.TcpServer`.


Abstract Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AbstractClient
   :members:
   :member-order: bysource


Abstract UDP Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AbstractUdpClient
   :members:
   :member-order: bysource

Pool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Pool
   :members:
   :member-order: bysource


Pool Connection
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PoolConnection
   :members:
   :member-order: bysource


.. _pep-3153: http://www.python.org/dev/peps/pep-3153/
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
