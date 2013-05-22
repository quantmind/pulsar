'''This example creates two :ref:`pulsar applications <apps-framework>`
performing different duties. The first application is a distributed
a :ref:`task queue <apps-taskqueue>` for processing tasks implemented
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
from pulsar.utils.path import Path
from pulsar.apps import rpc, tasks, wsgi

Path(pulsar.__file__).add2python('stdnet', up=2, down=['python-stdnet'],
                                 must_exist=False)

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
        # only post allowed by the JSON RPC handler
        request = [wsgi.Router('/', post=RpcRoot(self.tqname))]
        response = [wsgi.GZipMiddleware(200)]
        return wsgi.WsgiHandler(middleware=request,
                                response_middleware=response)
    
    
class server(pulsar.MultiApp):
    '''Build a multi-app consisting on a taskqueue and a JSON-RPC server.
This class shows how to
use :class:`pulsar.apps.MultiApp` utility for starting several
:ref:`pulsar applications <apps-framework>` at once.'''
    cfg = pulsar.Config('Taskqueue with JSON-RPC API example')
    def build(self):
        yield self.new_app(tasks.TaskQueue, task_paths=TASK_PATHS)
        yield self.new_app(wsgi.WSGIServer, prefix='rpc',
                           callable=Rpc(self.name))
    

if __name__ == '__main__':
    server('taskqueue').start()

