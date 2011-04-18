'''\
A very simple JSON-RPC Calculator
'''
import pulsar
from pulsar.http import rpc

class Calculator(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_server_info(self, request):
        arbiter = self._pulsar_arbiter
        return arbiter.server_info()
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)
    
    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)
    
    def rpc_divide(self, request, a, b):
        return float(a) / float(b)


def server():
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = Calculator())
    
    
if __name__ == '__main__':
    server().start()

