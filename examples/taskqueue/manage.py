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
    import pulsar
from pulsar.apps import rpc, tasks, wsgi

TASK_PATHS = ['sampletasks.*']


class RpcRoot(rpc.PulsarServerCommands,
              tasks.TaskQueueRpcMixin):
    '''The rpc handler which communicates with the task queue'''
    
    def rpc_runpycode(self, request, code=None, **params):
        return self.task_run(request, 'runpycode', code=code, **params)
        
def dummy():
    pass

class server(pulsar.MultiApp):
    
    def __call__(self, actor=None):
        name = self.name
        params = self.params
        tq = tasks.TaskQueue(name=name, callable=dummy,
                             tasks_path=TASK_PATHS,
                             script=__file__, **params)
        self.apps.append(tq)
        rpcs = wsgi.WSGIServer(rpc.RpcMiddleware(RpcRoot(tq)),
                               name = '{0}_rpc'.format(tq.name),
                               **params)
        self.apps.append(rpcs)
        return rpcs(actor)
    

if __name__ == '__main__':
    server = server()
    server()
    server.start()

