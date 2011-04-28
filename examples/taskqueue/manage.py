'''\
A a JSON-RPC Server with a Task Queue.
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


class RpcRoot(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        return 'pong'
    
    def rpc_server_info(self, request):
        return self.send(request,'info')
    
    def rpc_evalcode(self, request, *args, **kwargs):
        return self.send(request, 'codetask', (args, kwargs), server = 'task_server')
        

def createTaskQueue(tasks_path = None, **params):
    # Create the taskqueue application using the tasks in the sampletasks directory
    tasks = pulsar.require('tasks')
    return tasks.TaskQueue(tasks_path = ['taskqueue.sampletasks.*'],
                           **params)
    
    
def server(task_workers = 1, **params):
    # Create the taskqueue application with an rpc server
    taskqueue = createTaskQueue(workers = task_workers)
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(RpcRoot(),
                             links = {'taskqueue':taskqueue},
                             **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

