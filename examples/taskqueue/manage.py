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
    
from pulsar.apps.tasks import TaskQueueRpcMixin, queueTask, TaskQueue
from pulsar.http import rpc, actorCall


class RpcRoot(rpc.JsonServer,TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''    
    rpc_get_task = actorCall('get_task', server = 'taskqueue')
    rpc_evalcode = queueTask('codetask', server = 'taskqueue')
        

def createTaskQueue(tasks_path = None, **params):
    # Create the taskqueue application using the tasks in
    # the sampletasks directory
    return TaskQueue(tasks_path = ['taskqueue.sampletasks.*'],
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

