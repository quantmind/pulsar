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

from .html import *         # noqa
from .content import *      # noqa
from .utils import *        # noqa
from .middleware import *   # noqa
from .response import *     # noqa
from .wrappers import *     # noqa
from .server import *       # noqa
from .route import *        # noqa
from .handlers import *     # noqa
from .routers import *      # noqa
from .auth import *         # noqa
from .formdata import *     # noqa


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
