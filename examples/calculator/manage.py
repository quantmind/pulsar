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
from random import normalvariate

from pulsar.apps import rpc, wsgi
from pulsar.utils.httpurl import range


def divide(a, b):
    '''Divide two numbers'''
    return float(a)/float(b)

def randompaths(num_paths=1, size=250, mu=0, sigma=1):
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
        assert(str(request)==name)
        assert(request.user==None)
        return True


class Root(rpc.PulsarServerCommands):
    
    def rpc_dodgy_method(self, request):
        '''This method will fails because the return object is not
json serializable.'''
        return Calculator
    
    rpc_check_request = RequestCheck()


class Calculator(rpc.JSONRPC):

    def rpc_add(self, request, a, b):
        return float(a) + float(b)

    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)

    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)

    rpc_divide = rpc.FromApi(divide)

    rpc_randompaths = rpc.FromApi(randompaths)


def wsgi_handler():
    return rpc.RpcMiddleware(Root().putSubHandler('calc',Calculator()))

def server(**params):
    return wsgi.WSGIServer(callable=wsgi_handler(), **params)


def start_server(**params):
    return server(**params).start()


if __name__ == '__main__':
    start_server()

