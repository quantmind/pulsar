'''A a JSON-RPC Server with a Task Queue for processing tasks.
To run the server type::

    python manage.py
    
Open a new shell and launch python and type::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://localhost:8060')
    >>> p.ping()
    'pong'
    >>> p.calc.add(3,4)
    7.0
    >>>
    
'''
try:
    import pulsar
except ImportError:
    import sys
    sys.path.append('../../')
from pulsar.apps import rpc, tasks, wsgi

TASK_PATHS = ['sampletasks.*']


class RpcRoot(rpc.PulsarServerCommands,
              tasks.TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''
    
    def rpc_runpycode(self, request, code = None, **params):
        return self.task_run(request, 'runpycode', code = code, **params)
        
        
def createTaskQueue(**params):
    return tasks.TaskQueue(tasks_path = TASK_PATHS,
                           script = __file__,
                           **params)

    
def server(name='taskqueue', **params):
    # Create the taskqueue application with an rpc server
    tq = createTaskQueue(name=name, **params)
    return wsgi.WSGIServer(rpc.RpcMiddleware(RpcRoot(tq)),
                           name = '{0}_rpc'.format(tq.name),
                           **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

