'''\
A a JSON-RPC Server with some simple functions.
To run the server type::

    python manage.py
    
Open a new shell and launch python and type::

    >>> from pulsar.http import rpc
    >>> p = rpc.JsonProxy('http://localhost:8060')
    >>> p.ping()
    'pong'
    >>> p.calc.add(3,4)
    7.0
    >>>
    
'''
try:
    from penv import pulsar
except ImportError:
    import pulsar
    
from pulsar.http import rpc


class Root(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_server_info(self, request, full = False):
        worker = request.environ['pulsar.worker']
        return worker.proxy.info(worker.arbiter, full = full)
        
    
class Calculator(rpc.JSONRPC):
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)
    
    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)
    
    def rpc_divide(self, request, a, b):
        return float(a) / float(b)


def server(**params):
    root = Root().putSubHandler('calc',Calculator())
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = root, **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

