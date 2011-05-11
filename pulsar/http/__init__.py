from pulsar.utils.importer import import_module

from .client import HttpClient, urlencode

EMPTY_TUPLE = ()
EMPTY_DICT = {}

def get_httplib(cfg = None):
    name = None if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        return import_module('pulsar.http.http_gunicorn')
    else:
        return import_module('pulsar.http.base')
    
    

def actorCall(function, doc = '', ack = True, server = "taskqueue"):
    
    def _(self, request, **kwargs):
        worker = request.environ['pulsar.worker']
        tk = worker.ACTOR_LINKS[server]
        if ack:
            return tk.send(worker.aid, (EMPTY_TUPLE,kwargs), name=function, ack=True)
        else:
            tk.send(worker.aid, (EMPTY_TUPLE,kwargs), name='addtask_noack', ack=False)
        
    _.__doc__ = doc
    _.__name__ = function
    return _
    
        
def queueTask(taskname, doc = '', ack = True, server = "taskqueue"):
    # A decorator for running a taskname in the taskqueue
    
    def _(self, request, **kwargs):
        worker = request.environ['pulsar.worker']
        tk = worker.ACTOR_LINKS[server]
        args = (taskname,(),kwargs)
        if ack:
            return tk.send(worker.aid, (args,EMPTY_DICT), name='addtask', ack=True)
        else:
            tk.send(worker.aid, (args,EMPTY_DICT), name='addtask_noack', ack=False)
        
    _.__doc__ = doc
    _.__name__ = taskname
    return _



class SendToQueue(object):
    '''Same as ``queueTask`` decorator, but it returns an object.'''
    def __init__(self, taskname, request, *args, **kwargs):
        self.taskname = taskname
        self.request = request
        self.args = args
        self.kwargs = kwargs
        
    def __call__(self):
        server = self.kwargs.pop('server','taskqueue')
        worker = self.request.environ['pulsar.worker']
        tk = worker.ACTOR_LINKS[server]
        args = (self.taskname,self.args,self.kwargs)
        return tk.send(worker.aid, (args,EMPTY_DICT), name='addtask')
        

