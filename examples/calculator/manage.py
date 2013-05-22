'''\
This is a a :ref:`JSON-RPC <apps-rpc>` server with some simple functions.
To run the server type::

    python manage.py

Open a new shell and launch python and type::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://localhost:8060', force_sync=True)
    >>> p.ping()
    'pong'
    >>> p.functions_list()
    [[...
    >>> p.calc.add(3,4)
    7.0

The ``force_sync`` keyword is used here to force the Json RPC client to
wait for a full response rather than returning a :class:`pulsar.Deferred`.
Check the :ref:`creating synchronous clients <tutorials-synchronous>` tutorial. 

Implementation
-----------------

The calculator rpc functions are implemented by the :class:`Calculator`
handler, while the :class:`Root` handler exposes utility methods from
the :class:`pulsar.apps.rpc.PulsarServerCommands` handler.

.. autoclass:: Calculator
   :members:
   :member-order: bysource
   
.. autoclass:: Site
   :members:
   :member-order: bysource

'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
from random import normalvariate

from pulsar.apps import rpc, wsgi
from pulsar.utils.httpurl import range, JSON_CONTENT_TYPES


def divide(request, a, b):
    '''Divide two numbers. This method illustrate how to use the
:func:`pulsar.apps.rpc.rpc_method` decorator.'''
    return float(a)/float(b)

def request_handler(request, format, kwargs):
    '''Dummy request handler'''
    return kwargs

def randompaths(request, num_paths=1, size=250, mu=0, sigma=1):
    '''Lists of random walks.'''
    r = []
    for p in range(num_paths):
        v = 0
        path = [v]
        r.append(path)
        for t in range(size):
            v += normalvariate(mu, sigma)
            path.append(v)
    return r


class RequestCheck:
    
    def __call__(self, request, name):
        assert(request.json_data['method'] == name)
        return True


class Root(rpc.PulsarServerCommands):
    
    def rpc_dodgy_method(self, request):
        '''This method will fails because the return object is not
json serializable.'''
        return Calculator
    
    rpc_check_request = RequestCheck()


class Calculator(rpc.JSONRPC):
    '''A :class:`pulsar.apps.rpc.JSONRPC` handler which implements few simple
remote functions.'''
    def rpc_add(self, request, a, b):
        '''Add two numbers'''
        return float(a) + float(b)

    def rpc_subtract(self, request, a, b):
        '''Subtract two numbers'''
        return float(a) - float(b)

    def rpc_multiply(self, request, a, b):
        '''Multiply two numbers'''
        return float(a) * float(b)

    rpc_divide = rpc.rpc_method(divide, request_handler=request_handler)
    rpc_randompaths = rpc.rpc_method(randompaths)


class Site(wsgi.LazyWsgi):
    '''WSGI handler for the RPC server'''
    def setup(self):
        '''Called once to setup the list of wsgi middleware.'''
        json_handler = Root().putSubHandler('calc', Calculator())
        middleware = wsgi.Router('/', post=json_handler,
                                 accept_content_types=JSON_CONTENT_TYPES)
        response = [wsgi.GZipMiddleware(200)]
        return wsgi.WsgiHandler(middleware=[middleware],
                                response_middleware=response)
    

def server(callable=None, **params):
    return wsgi.WSGIServer(Site(), **params)


if __name__ == '__main__':  #pragma nocover
    server().start()

