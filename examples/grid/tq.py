'''\
A a Task Queue with a redis powered distributed queue.
To run a server type::

    python td.py
    
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
from pulsar.apps.pulsardjp import Queue, Task

from stdnet import orm

TASK_PATHS = []


def gridqueue():
    orm.register_applications('pulsar.apps.pulsardjp')
    queue = Queue(name = 'grid.tq.gridqueue').save()
    return queue

    
def server(**params):
    # Create the taskqueue application with an rpc server
    return tasks.TaskQueue(tasks_path = TASK_PATHS,
                           name = 'taskqueue',
                           task_class = Task,
                           task_queue_factory = 'grid.tq.gridqueue',
                           **params)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()
