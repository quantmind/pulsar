.. _tutorials-synchronous:

=========================
Synchronous Components
=========================

:class:`.AsyncObject` are used everywhere in pulsar.
These objects expose the ``_loop`` attribute as discussed in the
:ref:`design documentation <async-object>`.

Normally, the ``_loop`` is a running event loop controlled by an
:class:`.Actor`.
In this case, all operations which requires the loop, are carried out
asynchronously as one would expect.

However, sometimes it can be useful to have
:class:`.AsyncObject` which behaves in a synchronous fashion.
Pulsar achieves this by using a new event loop for that object.
For example, this statement creates a synchronous :class:`.HttpClient`::

    >>> from pulsar import new_event_loop
    >>> from pulsar.apps import http
    >>> client = http.HttpClient(loop=new_event_loop())
    >>> client._loop.is_running()
    False

You can now execute synchronous requests::

    >>> response = client.get('https://pypi.python.org/pypi/pulsar/json')

Under the hood, pulsar treats a synchronous request exactly in the same way as
an asynchronous one with the only difference that the
:ref:`event loop <asyncio-event-loop>`
is always used via the :meth:`~.asyncio.BaseEventLoop.run_until_complete`
method to wait for the response to be available.
