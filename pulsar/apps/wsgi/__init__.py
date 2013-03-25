"""This is the most important :ref:`pulsar application <application-api>`.
The server is a specialized :class:`pulsar.apps.socket.SocketServer` class
for serving web applications conforming with the python web server
gateway interface (WSGI_).
The server can be used in conjunction with several web frameworks
as well as :ref:`pulsar wsgi application handlers <apps-wsgi-handlers>`,
the :ref:`pulsar RPC middleware <apps-rpc>` and
the :ref:`websocket middleware <apps-ws>`.

An example of a web server written with :mod:`pulsar.apps.wsgi` which responds
with "Hello World!" for every request::

    from pulsar.apps import wsgi

    def hello(environ, start_response):
        data = b"Hello World!"
        response_headers = [('Content-type','text/plain'),
                            ('Content-Length', str(len(data)))]
        start_response("200 OK", response_headers)
        return [data]

    if __name__ == '__main__':
        wsgi.WSGIServer(callable=hello).start()


For more information regarding WSGI check the pep3333_ specification.
To run the application::

    python script.py
    
For available run options::

    python script.py --help


.. _wsgi-async:

WSGI asynchronous implementation
=======================================
When dealing with asynchronous :ref:`application handlers <apps-wsgi-handlers>`,
the WSGI_ specification has one main issue: it requires the application
to invoke the ``start_response`` callable before the iterable it returns
yields its first body bytestring.
This is the case even if the bytestring it yields is empty.

If an application handler returns an asynchronous object
(a :class:`pulsar.Deferred` instance), the response headers are not yet known,
therefore calling ``start_response`` is not an option (``start_response`` can be
called once only, unless is communicating an exception).


Lets consider the following example::

    from pulsar import is_async
    
    def async_middleware(middleware):
        # A decorator for asynchronous middlewares
        def _(environ, start_response):
            response = middleware(environ, start_response)
            if is_async(response):
                response = response.result if response.called else response
                while is_async(response):
                    # the response is not yet ready!
                    # yield and empty bytestring
                    yield b''
                    response = response.result if response.called else response
                if is_failure(response):
                    response.raise_all()
            # the response is ready!
            start_response(environ, response.headers)
            for data in response:
                yield data
        return _
        
    @async_middleware
    def create_response(environ, start_response):
        #Return an iterable over bytestrings
        ...
        
If the response is asynchronous, the above middleware does not, fully,
conform with WSGI. If, on the other hand, the response is synchronous than
the ``start_response`` method is called before any bytestring is yielded
and WSGI is fully satisfied.

Therefore the ``async_middleware`` decorator fully conforms with WSGI when using
standard synchronous handlers, and switches to a non-conforming version when
the application handler returns asynchronous responses.
This is the WSGI specification pulsar uses and it
implements in the :class:`server.HttpServerResponse` protocol.

**Yielding empty bytes**

When the application middleware yields an empty byte, pulsar wsgi server pauses
to consume the generator and add a callback to the :class:`pulsar.EventLoop`
running the current :class:`pulsar.Actor` to resume the iteration at the
next :class:`pulsar.EventLoop` loop.
This is implemented in the :meth:`pulsar.Transport.writelines` method when
called with a generator as parameter::

    def _write_lines_async(self, lines):
        try:
            result = next(lines)
            if result == b'':
                # stop writing and resume at next loop
                self._event_loop.call_soon(self._write_lines_async, lines)
            else:
                self.write(result)
                self._write_lines_async(lines)
        except StopIteration:
            pass



WSGI Server
===================

.. autoclass:: WSGIServer
   :members:
   :member-order: bysource
   
   
.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _WSGI: http://www.wsgi.org
"""
from functools import partial

import pulsar
from pulsar.apps.socket import SocketServer

from .content import *
from .utils import *
from .middleware import *
from .wrappers import *
from .server import *
from .route import *
from .handlers import *
from .plugins import *


class HttpParser(pulsar.Setting):
    app = 'wsgi'
    section = "WSGI Servers"
    name = "http_parser"
    flags = ["--http-parser"]
    desc = """\
        The HTTP Parser to use. By default it uses the fastest possible.

        Specify `python` if you wich to use the pure python implementation
        """

class WsgiFactory(object):
    
    def __call__(self):
        raise NotImplementedError


class WSGIServer(SocketServer):
    cfg_apps = ('socket',)
    _app_name = 'wsgi'

    def protocol_consumer(self):
        '''Build the :class:`pulsar.ProtocolConsumer` factory for this
WSGI server. It uses the :class:`pulsar.apps.wsgi.server.HttpServerResponse`
protocol consumer and the wsgi callable provided as parameter during
initialisation.'''
        callable = self.callable
        if isinstance(callable, WsgiFactory):
            callable = callable()
        return partial(HttpServerResponse, callable)
