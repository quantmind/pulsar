.. _tutorials-synchronous:

=======================
Synchronous Clients
=======================

Both :class:`pulsar.Client` and :class:`pulsar.Server` are asynchronous by design.
However, sometimes, mainly for testing purposing, can be useful to have
:class:`pulsar.Client` which behaves in a synchronous fashion.

Pulsar achieves this by using the ``force_sync`` keyword when building a client.
For example, this statement creates a synchronous :class:`pulsar.apps.http.HttpClient`::

    >>> from pulsar.apps import http
    >>> client = http.HttpClient(force_sync=True)

Under the hood, pulsar treats a synchronous clients exactly in the same way as
an asynchronous one with the only difference in the :class:`pulsar.EventLoop`
used.

Synchronous clients create their own event loop and invoke the
:meth:`pulsar.EventLoop.run_until_complete` to wait for the response to
be available.
    