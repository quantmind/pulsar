'''\
A very simple JSON-RPC Calculator
'''
import pulsar
from pulsar.http import rpc

class Calculator(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    def rpc_subtract(self, request, a, b):
        return float(a) - float(b)
    
    def rpc_multiply(self, request, a, b):
        return float(a) * float(b)
    
    def rpc_divide(self, request, a, b):
        return float(a) / float(b)


def run():
    wsgi = pulsar.require('wsgi')
    wsgi.createServer(callable = Calculator()).run()
    
    
if __name__ == '__main__':
    run()

