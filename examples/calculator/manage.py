'''\
A very simple JSON-RPC Calculator
'''
from .penv import pulsar
from pulsar.http import rpc

class Calculator(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_server_info(self, request):
        return self.server_proxy.server_info()
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)
    
    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)
    
    def rpc_divide(self, request, a, b):
        return float(a) / float(b)


def server(**params):
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = Calculator(), **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start()

