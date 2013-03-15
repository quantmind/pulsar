.. _tutorials-synchronous:

=======================
Synchronous Clients
=======================

Both :class:`pulsar.Client` and :class:`pulsar.Server` are asynchronous by design.
However, sometimes, mainly for testing purposing, can be useful to have
:class:`pulsar.Client` which behave in a synchronous fashion.

Pulsar achieves this by using the ``force_sync`` keyword when building a client.

  