from pulsar.utils.importer import import_module

def get_httplib(cfg = None):
    name = None if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        return import_module('pulsar.http.http_gunicorn')
    else:
        return import_module('pulsar.http.base')
    

