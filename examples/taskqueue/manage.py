'''\
A a JSON-RPC Server with a Task Queue.
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


def test_middleware(request, margs):
    '''Add a simple flag to the dictionary. Just for testing middleware.'''
    margs['test'] = True

task_manager = tasks.HttpTaskManager('taskqueue')
task_manager.add_request_middleware(test_middleware)


class RpcRoot(rpc.PulsarServerCommands,
              tasks.TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''
    task_queue_manager = task_manager
    
    rpc_evalcode = tasks.queueTask('codetask',
                                   'Evaluate python code on the task queue.')
        
    
def server(**params):
    # Create the taskqueue application with an rpc server
    tasks.TaskQueue(tasks_path = TASK_PATHS,
                    name = 'taskqueue',
                    **params)
    return wsgi.createServer(RpcRoot(),
                             name = 'rpc',
                             **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

