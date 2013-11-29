
.. _protocol-api:

================================
Protocols/Transports API
================================

This part of the :ref:`pulsar API <api>` is about classes responsible for
implementing the Protocol/Transport paradigm. :class:`.SocketTransport`
and :class:`.Protocol` are designed to comply with pep-3156_ specification
and derived from ``asyncio.Transport`` and ``asyncio.Protocol``.

.. _eventloop-class:

.. note:: **Event Loop classes**

    An event-loop class create objects with the ``_loop``
    attribute which is the ``asincio.eventloop`` controlling the event-loop
    object.


Transports
==========================

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
==========================

Protocol
~~~~~~~~~~~~~~
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
==========================

Producers are factory of connections with end-points. They are used by
both servers and clients classes.

Producer
~~~~~~~~~~~~~~~~~
.. autoclass:: Producer
   :members:
   :member-order: bysource


TcpServer
~~~~~~~~~~~~~~~~~

.. autoclass:: TcpServer
   :members:
   :member-order: bysource

.. module:: pulsar.async.clients

.. _clients-api:

Clients
=================

This section introduces classes implementing the transport/protocol paradigm
for clients with several connections to a remote :class:`.TcpServer`.
:class:`BaseClient` is the main class here, and :class:`BaseClient.request`
is the single most important method a subclass must implement.

Abstract Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AbstractClient
   :members:
   :member-order: bysource


Pool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Pool
   :members:
   :member-order: bysource

.. _pep-3153: http://www.python.org/dev/peps/pep-3153/
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
