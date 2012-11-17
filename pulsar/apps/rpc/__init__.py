'''Asynchronous WSGI Remote Procedure Calls middleware. It implements a
JSON-RPC_ server and client.

RPC server
=====================
To create a server first you create your rpc handler and
(optional) subhandlers::

    from pulsar.apps import rpc, wsgi
    
    class Root(rpc.PulsarServerCommands):
        pass
        
    class Calculator(rpc.JSONRPC):
        
        def rpc_add(self, request, a, b):
            return float(a) + float(b)
        
        def rpc_subtract(self, request, a, b):
            return float(a) - float(b)
        
        def rpc_multiply(self, request, a, b):
            return float(a) * float(b)
        
        def rpc_divide(self, request, a, b):
            return float(a) / float(b)
    

Then you create the WSGI_ middleware::

    def server():
        root = Root().putSubHandler('calc',Calculator())
        return wsgi.WSGIServer(callable=rpc.RpcMiddleware(root))
    
    if __name__ == '__main__':
        server().start()
    
.. _JSON-RPC: http://www.jsonrpc.org/specification
.. _WSGI: http://www.python.org/dev/peps/pep-3333/
'''
from .exceptions import *
from .handlers import *
from .jsonrpc import *
from .decorators import *
from .mixins import *