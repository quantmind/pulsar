'''\
A a JSON-RPC Server with a Task Queue for processing tasks.
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
import pulsar
from pulsar.apps import rpc, tasks, wsgi

TASK_PATHS = ['sampletasks.*']


class RpcRoot(rpc.PulsarServerCommands,
              tasks.TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''
    
    def rpc_runpycode(self, request, code = None):
        return self.task_queue_manager(request.actor,
                                       'addtask',
                                       job = 'runpycode',
                                       code = code)
        
        
def createTaskQueue(name = 'taskqueue', **params):
    return tasks.TaskQueue(tasks_path = TASK_PATHS,
                           name = name,
                           script = __file__,
                           **params)
    
def server(**params):
    # Create the taskqueue application with an rpc server
    createTaskQueue(**params)
    return wsgi.createServer(RpcRoot(),
                             name = 'rpc',
                             **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

