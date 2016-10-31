
.. _wsgi-async:

.. module:: pulsar.apps.wsgi.handlers

=======================================
Asynchronous WSGI
=======================================

This section describes the asynchronous WSGI specification used by pulsar.
It is a superset of the `WSGI 1.0.1`_ specification for synchronous
server/middleware.
If an application handler is synchronous, this specification is exactly
equivalent to `WSGI 1.0.1`_. Changes with respect `WSGI 1.0.1`_
concern asynchronous responses and nothing else.

Introduction
========================

The WSGI interface has two sides: the ``server`` or ``gateway`` side, and the
``application`` or ``framework`` side. The server side invokes a callable
object, here referred as **application handler**, that is provided by the
application side.


.. note::

    A standard WSGI application handler is always a callable, either a function
    or a callable object, which accepts two positional arguments:
    ``environ`` and ``start_response``. When called by the server,
    the application object must return an iterable yielding zero or more bytes.


.. _wsgi-handlers:

Application handlers
=============================

An asynchronous :ref:`application handler <wsgi-handlers>` must conform
with the standard `WSGI 1.0.1`_ specification with the following two
exceptions:

* It can return an awaitable object
* If it returns an awaitable object, this object must result in an
  :ref:`asynchronous iterable <wsgi-async-iter>`.

An awaitable object is an object that can be used in an await expression.
Can be a coroutine or an object with an :meth:`~object.__await__` method
(a :class:`~asyncio.Future` for example). See also `PEP 492`_.
Pulsar is shipped with two WSGI application handlers documented below.

.. _wsgi-async-iter:

Asynchronous Iterable
========================

An asynchronous iterable is an iterable over a combination of ``bytes`` and/or
awaitable objects which result in ``bytes``.
For example this could be an asynchronous iterable::

    def simple_async():
        yield b'hello'
        c = Future()
        c.set_result(b' ')
        yield c
        yield b'World!'


.. _wsgi-lazy-handler:

WsgiHandler
======================

The first application handler is the :class:`WsgiHandler`
which is a step above the :ref:`hello callable <tutorials-hello-world>`
in the tutorial. It accepts two iterables, a list of
:ref:`wsgi middleware <wsgi-middleware>` and an optional list of
:ref:`response middleware <wsgi-response-middleware>`.

.. autoclass:: WsgiHandler
   :members:
   :member-order: bysource


.. _wsgi-handler:

Lazy Wsgi Handler
======================

.. autoclass:: LazyWsgi
   :members:
   :member-order: bysource


.. _wsgi-pulsar-variables:

Pulsar Variables
======================
Pulsar injects two server-defined variables into the WSGI environ:

* ``pulsar.connection``, the :class:`.Connection` serving the request
* ``pulsar.cfg``, the :class:`.Config` dictionary of the server

The event loop serving the application can be retrieved from the connection
via the ``_loop`` attribute::

    loop = environ['pulsar.connection']._loop

.. _WSGI: http://www.wsgi.org
.. _`WSGI 1.0.1`: http://www.python.org/dev/peps/pep-3333/
.. _`PEP 492`: https://www.python.org/dev/peps/pep-0492/
