
.. _protocol-api:

================================
Protocols/Transports API
================================

This part of the :ref:`pulsar API <api>` is about classes responsible for
implementing the Protocol/Transport paradigm. :class:`.SocketTransport`
and :class:`.Protocol` are designed to comply with pep-3156_ specification
and derived from ``asyncio.Transport`` and ``asyncio.Protocol``.


Transports
=================

The :class:`.SocketTransport` is used as base class for all socket transports
and it is the only class in this section which is also used outside
TCP sockets.

SocketTransport
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.async.internet.SocketTransport
   :members:
   :member-order: bysource


.. module:: pulsar.async.stream

SocketStreamTransport
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SocketStreamTransport
   :members:
   :member-order: bysource


SocketStreamSslTransport
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SocketStreamSslTransport
   :members:
   :member-order: bysource


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


.. module:: pulsar.async.udp

UDP
=====

Classes for the (user) datagram protocol. UDP uses a simple transmission
model with a minimum of protocol mechanism.

Datagram Transport
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DatagramTransport
   :members:
   :member-order: bysource


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
