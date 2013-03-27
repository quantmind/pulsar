'''This example creates two :ref:`pulsar applications <apps-framework>`
performing different duties. The first application is a distributed
a :ref:`task queue <apps-tasks>` for processing tasks implemented
in the :mod:`examples.taskqueue.simpletasks` module.
The second application is a :ref:`WSGI server <apps-wsgi>` which
exposes the task queue functionalities via a :ref:`JSON-RPC api <apps-rpc>`.

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

Implementation
====================

.. autoclass:: RpcRoot
   :members:
   :member-order: bysource
   
.. autoclass:: server
   :members:
   :member-order: bysource    
'''
try:
    import pulsar
except ImportError:
    import sys
    sys.path.append('../../')
    import pulsar
from pulsar.apps import rpc, tasks, wsgi

TASK_PATHS = ['sampletasks.*']


class RpcRoot(rpc.PulsarServerCommands, tasks.TaskQueueRpcMixin):
    '''The :class:`pulsar.apps.rpc.JSONRPC` handler which communicates
with the task queue.'''
    
    def rpc_runpycode(self, request, code=None, **params):
        return self.task_run(request, 'runpycode', code=code, **params)
        

class Rpc(wsgi.LazyWsgi):
    
    def __init__(self, tqname):
        self.tqname = tqname
        
    def setup(self):
        return wsgi.Router('/', post=RpcRoot(self.tqname))
    
    
class server(pulsar.MultiApp):
    '''Build a taskqueue and an rpc server. This class shows how to
use :class:`pulsar.apps.MultiApp` utility for starting several
:ref:`pulsar applications <apps-framework>` at once.'''
    cfg_apps = frozenset(('cpubound', 'task', 'socket', 'wsgi'))
    cfg = {'tasks_path': TASK_PATHS}
    def build(self):
        yield self.new_app(tasks.TaskQueue)
        yield self.new_app(wsgi.WSGIServer, name='rpc',
                           callable=Rpc(self.name))
    

if __name__ == '__main__':
    server('taskqueue').start()

