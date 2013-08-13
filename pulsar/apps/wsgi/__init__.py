"""This is the most important :ref:`pulsar application <application-api>`.
The server is a specialized :ref:`socket server <apps-socket>`
for web applications conforming with the python web server
gateway interface (`WSGI 1.0.1`_).
The server can be used in conjunction with several web frameworks
as well as :ref:`pulsar wsgi application handlers <wsgi-handlers>`,
:ref:`pulsar router <wsgi-middleware>`,
the :ref:`pulsar RPC middleware <apps-rpc>` and
the :ref:`websocket middleware <apps-ws>`.

An example of a web server written with :mod:`pulsar.apps.wsgi` which responds
with ``Hello World!`` for every request::

    from pulsar.apps import wsgi

    def hello(environ, start_response):
        data = b"Hello World!"
        response_headers = [('Content-type','text/plain'),
                            ('Content-Length', str(len(data)))]
        start_response("200 OK", response_headers)
        return [data]

    if __name__ == '__main__':
        wsgi.WSGIServer(hello).start()


For more information regarding WSGI check the pep3333_ specification.
To run the application::

    python script.py
    
For available run options::

    python script.py --help


WSGI Server
===================

.. autoclass:: WSGIServer
   :members:
   :member-order: bysource
   
   
.. _`WSGI 1.0.1`: http://www.python.org/dev/peps/pep-3333/
"""
from functools import partial

import pulsar
from pulsar.apps.socket import SocketServer

from .html import *
from .content import *
from .utils import *
from .middleware import *
from .wrappers import *
from .server import *
from .route import *
from .handlers import *
from .plugins import *


class WsgiFactory(object):
    
    def __call__(self):
        raise NotImplementedError


class WSGIServer(SocketServer):
    '''A specialised :class:`pulsar.apps.socket.SocketServer` for the HTTP
protocol and WSGI compatible applications.'''
    name = 'wsgi'
    cfg = pulsar.Config(apps=['socket', 'wsgi'])

    def protocol_consumer(self):
        '''Build the :class:`pulsar.ProtocolConsumer` factory for this
WSGI server. It uses the :class:`pulsar.apps.wsgi.server.HttpServerResponse`
protocol consumer and the wsgi callable provided as parameter during
initialisation.'''
        callable = self.callable
        if isinstance(callable, WsgiFactory):
            callable = callable()
        return partial(HttpServerResponse, callable, self.cfg)
