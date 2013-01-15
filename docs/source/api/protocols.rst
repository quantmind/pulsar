.. module:: pulsar

.. _protocol-api:

=======================
Protocols API
=======================

This part of the :ref:`pulsar API <api>` is about classes responsible for
implementing the Protocol/Transport paradigm as well as the :class:`Server`,
the base class for all socket servers.

Transport
==========================

.. autoclass:: Transport
   :members:
   :member-order: bysource
   
   
Protocol
==========================
.. autoclass:: Protocol
   :members:
   :member-order: bysource

   
Protocol Response
=====================================
.. autoclass:: ProtocolResponse
   :members:
   :member-order: bysource
   
   
Server
==========================
.. autoclass:: Server
   :members:
   :member-order: bysource
   
   
Client
==========================
.. autoclass:: Client
   :members:
   :member-order: bysource


Client Connection Pool
==========================
.. autoclass:: ConnectionPool
   :members:
   :member-order: bysource
   
   
Producer
==========================
.. autoclass:: Producer
   :members:
   :member-order: bysource



.. _pep-3153: http://www.python.org/dev/peps/pep-3153/
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/