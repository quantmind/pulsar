from pulsar.utils.importer import import_module

from .client import HttpClient, urlencode
from .link import *

EMPTY_TUPLE = ()
EMPTY_DICT = {}

def get_httplib(cfg = None):
    name = None if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        return import_module('pulsar.http.http_gunicorn')
    else:
        return import_module('pulsar.http.base')
    


# A decorator
#def actorCall(function, doc = '', ack = True, server = "taskqueue"):
#    
#    def _(self, request, **kwargs):
#        return actor_call(request, server, function, ack=ack, **kwargs)
#        
#    _.__doc__ = doc
#    _.__name__ = function
#    return _

