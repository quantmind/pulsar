.. module:: pulsar

.. _protocol-api:

================================
Protocols/Transports API
================================

This part of the :ref:`pulsar API <api>` is about classes responsible for
implementing the Protocol/Transport paradigm as well as :class:`Server` and
:class:`Client` base classes. :class:`Transport`
and :class:`Protocol` are designed to
comply with pep-3156_ specification

Transports
==========================

Transport
~~~~~~~~~~~~~~~~~~

.. autoclass:: Transport
   :members:
   :member-order: bysource
   
   
SocketTransport
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SocketTransport
   :members:
   :member-order: bysource
   
  
Protocols
==========================

BaseProtocol
~~~~~~~~~~~~~~~~~~~

.. autoclass:: BaseProtocol
   :members:
   :member-order: bysource


Protocol
~~~~~~~~~~~~~~~~~~~

.. autoclass:: Protocol
   :members:
   :member-order: bysource
   
   
DatagramProtocol
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DatagramProtocol
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
   
Producer
==========================
.. autoclass:: Producer
   :members:
   :member-order: bysource


.. _clients-api:

Clients
=================

This section introduces classes implementing the transport/protocol paradigm
for clients with several connections to a remote :class:`Server`.
:class:`Client` is the main class here, and :class:`Client.request`
is the single most important method a subclass must implement.

Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: Client
   :members:
   :member-order: bysource


Client Connection Pool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: ConnectionPool
   :members:
   :member-order: bysource

Request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: Request
   :members:
   :member-order: bysource
   
Server
==================

.. autoclass:: Server
   :members:
   :member-order: bysource


.. _pep-3153: http://www.python.org/dev/peps/pep-3153/
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/