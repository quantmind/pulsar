from pulsar.apps.tasks import Task
from pulsar.utils import rpc

class Add(Task):
    
    def run(self):
        pass
    
    
class Handler(rpc.Handle):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_add(self, request, a, b):
        return float(a) + float(b)
    
    
