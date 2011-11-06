'''\
A a JSON-RPC Server with some simple functions.
To run the server type::

    python manage.py
    
Open a new shell and launch python and type::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://localhost:8060')
    >>> p.ping()
    'pong'
    >>> p.functions_list()
    >>> ...
    >>> p.calc.add(3,4)
    7.0
    >>>
    
'''
from pulsar.apps import rpc, wsgi
from pulsar.utils.py2py3 import range
from random import normalvariate


def divide(a,b):
    '''Divide two numbers'''
    return float(a) / float(b)


def randompaths(num_paths = 1, size = 250, mu = 0, sigma = 1):
    r = []
    for p in range(num_paths):
        v = 0
        path = [v]
        r.append(path)
        for t in range(size):
            v += normalvariate(mu,sigma)
            path.append(v)
    return r
    

class Root(rpc.PulsarServerCommands):
    pass
        
    
class Calculator(rpc.JSONRPC):
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)
    
    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)
    
    rpc_divide = rpc.FromApi(divide)
    
    rpc_randompaths = rpc.FromApi(randompaths)


def server(**params):
    root = rpc.RpcMiddleware(Root().putSubHandler('calc',Calculator()))
    return wsgi.createServer(callable = root, **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

