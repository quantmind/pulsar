'''Asynchronous WSGI_ Remote Procedure Calls middleware. It implements a
JSON-RPC_ server and client. Check out the
:ref:`json-rpc tutorial <tutorials-calculator>` if you want to get started
quickly with a working example.

To quickly setup a server::

    class MyRpc(rpc.JSONRPC):

        def rpc_ping(self, request):
            return 'pong'


    class Wsgi(wsgi.LazyWsgi):

        def handler(self, environ=None):
            app = wsgi.Router('/',
                              post=MyRpc(),
                              response_content_types=['application/json'])
            return wsgi.WsgiHandler([app])

    if __name__ == '__main__':
        wsgi.WSGIServer(Wsgi()).start()


* The ``MyRpc`` handles the requests
* Routing is delegated to the :class:`.Router` which handle only ``post``
  requests with content type ``application/json``.


API
===========

.. module:: pulsar.apps.rpc.handlers

RpcHandler
~~~~~~~~~~~~~~

.. autoclass:: RpcHandler
   :members:
   :member-order: bysource

rpc method decorator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: rpc_method


.. module:: pulsar.apps.rpc.jsonrpc

JSON RPC
~~~~~~~~~~~~~~~~

.. autoclass:: JSONRPC
   :members:
   :member-order: bysource


JsonProxy
~~~~~~~~~~~~~~~~

.. autoclass:: JsonProxy
   :members:
   :member-order: bysource


.. module:: pulsar.apps.rpc.mixins

Server Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PulsarServerCommands
   :members:
   :member-order: bysource

.. _JSON-RPC: http://www.jsonrpc.org/specification
.. _WSGI: http://www.python.org/dev/peps/pep-3333/
'''
from .handlers import *     # noqa
from .jsonrpc import *      # noqa
from .mixins import *       # noqa
