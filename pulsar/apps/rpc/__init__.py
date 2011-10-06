'''\
Asynchronous WSGI Remote Procedure Calls middleware. It implements a
JSON-RPC_ server and client. To create a server first you create your
rpc handler and (optional) subhandlers::

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
            
    
    root = Root().putSubHandler('calc',Calculator())
    wsgi.createServer(callable = root).start()
    

.. _JSON-RPC: http://en.wikipedia.org/wiki/JSON-RPC
'''
from .exceptions import *
from .handlers import *
from .jsonrpc import *
from .decorators import *
from .mixins import *