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
        return self.send(request, 'codetask', (args, kwargs),
                         server = 'task_server')
        

def server(**params):
    root = RpcRoot()
    wsgi = pulsar.require('wsgi')
    tasks = pulsar.require('tasks')
    task_server = tasks.createServer(tasks_path = ['taskqueue.sampletasks'])
    rpc_server = wsgi.createServer(callable = root,
                                   links = {'task_server':task_server},
                                   **params)
    return rpc_server


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

