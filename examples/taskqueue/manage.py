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
from pulsar.apps import rpc, tasks

TASK_QUEUE_NAME = 'taskqueue'
TASK_PATHS = ['taskqueue.sampletasks.*']


def test_middleware(request, margs):
    '''Add a simple flag to the dictionary. Just for testing middleware.'''
    margs['test'] = True

task_manager = tasks.HttpTaskManager(TASK_QUEUE_NAME)
task_manager.add_request_middleware(test_middleware)


class RpcRoot(rpc.PulsarServerCommands,
              tasks.TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''
    task_queue_manager = task_manager
    
    rpc_evalcode = tasks.queueTask('codetask',
                                   'Evaluate python code on the task queue.')
        

def createTaskQueue(tasks_path = None, **params):
    # Create the taskqueue application using the tasks in
    # the sampletasks directory
    tasks_path = tasks_path or TASK_PATHS
    return tasks.TaskQueue(tasks_path = tasks_path,
                           name = TASK_QUEUE_NAME,
                           **params)
    
    
def server(task_workers = 1, concurrency = 'process', **params):
    # Create the taskqueue application with an rpc server
    createTaskQueue(workers = task_workers,
                    concurrency = concurrency)
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(RpcRoot(),concurrency=concurrency,**params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()

