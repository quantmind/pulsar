"""This is the most important :ref:`pulsar application <application-api>`.
The server is a specialized :ref:`socket server <apps-socket>`
for web applications conforming with the python web server
gateway interface (`WSGI 1.0.1`_).
The server can be used in conjunction with several web frameworks
as well as :ref:`pulsar wsgi application handlers <wsgi-handlers>`,
:ref:`pulsar router <wsgi-middleware>`,
the :ref:`pulsar RPC middleware <apps-rpc>` and
the :ref:`websocket middleware <apps-ws>`.

.. note::

    Pulsar wsgi server is production ready designed to easily
    handle fast, scalable http applications. As all pulsar applications,
    it uses an event-driven, non-blocking I/O model that makes it
    lightweight and efficient. In addition, its multiprocessing
    capabilities allow to handle the `c10k problem`_ with ease.


An example of a web server written with the :mod:`~pulsar.apps.wsgi`
module which responds with ``Hello World!`` for every request::

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
.. _`c10k problem`: http://en.wikipedia.org/wiki/C10k_problem
"""
from functools import partial

import pulsar
from pulsar.apps.socket import SocketServer, Connection

from .html import HtmlVisitor
from .content import (String, Html, Json, HtmlDocument, Links, Scripts, Media,
                      html_factory)
from .middleware import (clean_path_middleware, authorization_middleware,
                         wait_for_body_middleware, middleware_in_executor)
from .response import AccessControl, GZipMiddleware
from .wrappers import EnvironMixin, WsgiResponse, WsgiRequest, cached_property
from .server import HttpServerResponse, test_wsgi_environ, AbortWsgi
from .route import route, Route
from .handlers import WsgiHandler, LazyWsgi
from .routers import (Router, MediaRouter, MediaMixin, RouterParam,
                      file_response)
from .auth import HttpAuthenticate, parse_authorization_header
from .formdata import parse_form_data
from .utils import (handle_wsgi_error, render_error_debug, wsgi_request,
                    set_wsgi_request_class, dump_environ, HOP_HEADERS)

__all__ = [
    # Server
    'WSGIServer',
    'HttpServerResponse',
    'test_wsgi_environ',
    'AbortWsgi',
    #
    # Content strings
    'String',
    'Html',
    'Json',
    'HtmlDocument',
    'Links',
    'Scripts',
    'Media',
    'html_factory',
    'HtmlVisitor',
    #
    # Request middleware
    'clean_path_middleware',
    'authorization_middleware',
    'wait_for_body_middleware',
    'middleware_in_executor',
    #
    # Response middleware
    'AccessControl',
    'GZipMiddleware',
    #
    # WSGI Wrappers
    'EnvironMixin',
    'WsgiResponse',
    'WsgiRequest',
    'cached_property',
    #
    # WSGI Handlers
    'WsgiHandler',
    'LazyWsgi',
    #
    # Routes and Routers
    'route',
    'Route',
    'Router',
    'MediaRouter',
    'MediaMixin',
    'RouterParam',
    'file_response',
    #
    # Utilities
    'parse_form_data',
    'HttpAuthenticate',
    'parse_authorization_header',
    'handle_wsgi_error',
    'render_error_debug',
    'wsgi_request',
    'set_wsgi_request_class',
    'dump_environ',
    'HOP_HEADERS'
]


class WSGIServer(SocketServer):
    '''A WSGI :class:`.SocketServer`.
    '''
    name = 'wsgi'
    cfg = pulsar.Config(apps=['socket'],
                        server_software=pulsar.SERVER_SOFTWARE)

    def protocol_factory(self):
        cfg = self.cfg
        consumer_factory = partial(HttpServerResponse, cfg.callable, cfg,
                                   cfg.server_software)
        return partial(Connection, consumer_factory)
