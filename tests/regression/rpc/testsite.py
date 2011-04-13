import time
from datetime import datetime, date

from pulsar.utils.rpc import Handler, JSONRPC


class Site(Handler):
    '''Root Site'''
    def rpc_ping(self, request, **kwargs):
        return 'pong'
    

class Calculator(JSONRPC):
    
    def rpc_today(self, request):
        return date.today()
    
    def rpc_add(self, request, a, b):
        try:
            return float(a) + float(b)
        except:
            return a + b
    
    def rpc_subtract(self, request, a, b):
        return a - b
    
    def rpc_longcalculation(self, request, sleep = 1):
        time.sleep(sleep)
        return sleep


class Whatever(JSONRPC):
    
    def rpc_today(self, request):
        return date.today()
    
    def rpc_longcalculation(self, request, sleep = 1):
        time.sleep(1)
        return sleep


def handler():
    s = Site()
    s.putSubHandler('calc',Calculator())
    s.putSubHandler('what',Whatever())
    return s
