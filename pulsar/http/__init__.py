from pulsar.utils.importer import import_module

from .client import HttpClient

def get_httplib(cfg = None):
    name = None if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        return import_module('pulsar.http.http_gunicorn')
    else:
        return import_module('pulsar.http.base')
    
    
def queueTask(taskname, doc = '', ack = True, taskqueue = "taskqueue"):
    # A decorator for running a taskname in the taskqueue
    
    def _(self, request, **kwargs):
        worker = request.environ['pulsar.worker']
        tk = worker.ACTOR_LINKS[taskqueue]
        r = tk.send(worker.aid, ((taskname,),kwargs), 'addtask')
        if ack:
            return r
        
    _.__doc__ = doc
    _.__name__ = taskname
    return _

