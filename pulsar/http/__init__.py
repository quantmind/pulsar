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
    

def actor_call(request, actorname, actorfunction, ack = True, **kwargs):
    '''\
Send a remote call to an actor *actorname*.

:parameter request: a http request object. It containes the environment.
:parameter actorname: The name of the actor which will receive the call
:parameter actorfunction: The function to invoke. The remote actor must have a
                          "actor_<actorfunction>" function defined.
:parameter ack: if ``True`` it returns a deferred object.
'''
    worker = request.environ['pulsar.worker']
    if actorname in worker.ACTOR_LINKS:
        tk = worker.ACTOR_LINKS[actorname]
        r = tk.send(worker.aid, (EMPTY_TUPLE,kwargs),
                    name=actorfunction,
                    ack=ack)
        if ack:
            return r
        

# A decorator
def actorCall(function, doc = '', ack = True, server = "taskqueue"):
    
    def _(self, request, **kwargs):
        return actor_call(request, server, function, ack=ack, **kwargs)
        
    _.__doc__ = doc
    _.__name__ = function
    return _

