#
#   HTTP TOOLS FROM THIRD PARTIES APPLICATION
from pulsar.utils.importer import import_module

def get_library(cfg):
    name = 'gunicorn' if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        import pulsar.http._gunicorn as httplib
    elif name == 'werkzeug':
        import pulsar.http._werkzeug as httplib
    else:
        import pulsar.http._standard as httplib
    return httplib