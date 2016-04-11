"""
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

* It can return a :class:`~asyncio.Future`.
* If it returns a :class:`~asyncio.Future`, it must results in an
  :ref:`asynchronous iterable <wsgi-async-iter>`.

Pulsar is shipped with two WSGI application handlers documented below.

.. _wsgi-async-iter:

Asynchronous Iterable
========================

An asynchronous iterable is an iterable over a combination of ``bytes`` or
:class:`~asyncio.Future` which result in ``bytes``.
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
"""
from pulsar import Http404, isawaitable
from pulsar.utils.log import LocalMixin, local_method

from .utils import handle_wsgi_error
from .wrappers import WsgiResponse


__all__ = ['WsgiHandler', 'LazyWsgi']


class WsgiHandler:
    '''An handler for applications conforming to python WSGI_.

    .. attribute:: middleware

        List of :ref:`asynchronous WSGI middleware <wsgi-middleware>` callables
        which accept ``environ`` and ``start_response`` as arguments.
        The order matter, since the response returned by the callable
        is the non ``None`` value returned by a middleware.

    .. attribute:: response_middleware

        List of functions of the form::

            def ..(environ, response):
                ...

        where ``response`` is a :ref:`WsgiResponse <wsgi-response>`.
        Pulsar contains some
        :ref:`response middlewares <wsgi-response-middleware>`.

    '''
    def __init__(self, middleware=None, response_middleware=None, async=True):
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = response_middleware or []
        self._async = async

    def __call__(self, environ, start_response):
        c = self._async_call if self._async else self._sync_call
        return c(environ, start_response)

    async def _async_call(self, environ, start_response):
        response = None
        try:
            for middleware in self.middleware:
                response = middleware(environ, start_response)
                if isawaitable(response):
                    response = await response
                if response is not None:
                    break
            if response is None:
                raise Http404

        except Exception as exc:
            response = handle_wsgi_error(environ, exc)

        if isinstance(response, WsgiResponse) and not response.started:
            for middleware in self.response_middleware:
                response = middleware(environ, response) or response
                if isawaitable(response):
                    response = await response
            response.start(start_response)
        return response

    def _sync_call(self, environ, start_response):
        response = None
        try:
            for middleware in self.middleware:
                response = middleware(environ, start_response)
                if response is not None:
                    break
            if response is None:
                raise Http404

        except Exception as exc:
            response = handle_wsgi_error(environ, exc)

        if isinstance(response, WsgiResponse) and not response.started:
            for middleware in self.response_middleware:
                response = middleware(environ, response) or response
            response.start(start_response)
        return response


class LazyWsgi(LocalMixin):
    '''A :ref:`wsgi handler <wsgi-handlers>` which loads the actual
    handler the first time it is called.

    Subclasses must implement the :meth:`setup` method.
    Useful when working in multiprocessing mode when the application
    handler must be a ``picklable`` instance. This handler can rebuild
    its wsgi :attr:`handler` every time is pickled and un-pickled without
    causing serialisation issues.
    '''
    def __call__(self, environ, start_response):
        return self.handler(environ)(environ, start_response)

    @local_method
    def handler(self, environ=None):
        '''The :ref:`wsgi application handler <wsgi-handlers>` which
        is loaded via the :meth:`setup` method, once only,
        when first accessed.
        '''
        return self.setup(environ)

    def setup(self, environ=None):
        '''The setup function for this :class:`LazyWsgi`.

        Called once only the first time this application handler is invoked.
        This **must** be implemented by subclasses and **must** return a
        :ref:`wsgi application handler <wsgi-handlers>`.
        '''
        raise NotImplementedError
