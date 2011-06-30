from pulsar.http import rpc
from pulsar.utils.py2py3 import iteritems
from .models import Task


class PulsarServerCommands(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        '''Ping the server'''
        return 'pong'
    
    def rpc_server_info(self, request, full = False):
        '''Dictionary of information about the server'''
        worker = request.environ['pulsar.worker']
        info = worker.proxy.info(worker.arbiter, full = full)
        return info.add_callback(lambda res : self.extra_server_info(request, res))
    
    def rpc_functions_list(self, request):
        return list(self.listFunctions())
    
    def rpc_tasks(self, request, **kwargs):
        '''\
Retrive a list of tasks according to a set of filters.

:parameter id: task id
:parameter name: task name
:parameter status: task status, one of 'PENDING','SUCCESS','FAILED'.
:parameter user: user who subbmitted the task
'''
        if Task:
            fws = {}
            for k,v in iteritems(kwargs):
                if v is not None and k in Task.filtering:
                    fws[k] = v
            tasks = Task.objects.filter(**fws)
            return [task.todict() for task in tasks]

    # INTERNAL FUNCTIONS
    
    def extra_server_info(self, request, info):
        return info
    
