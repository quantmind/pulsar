from pulsar.utils.importer import import_module

from .client import HttpClient, urlencode
from .link import *
from .wsgi import *
from .websocket import *

EMPTY_TUPLE = ()
EMPTY_DICT = {}

def get_httplib(cfg = None):
    name = None if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        return import_module('pulsar.http.http_gunicorn')
    else:
        return import_module('pulsar.http.base')
    

