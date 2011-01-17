#
#   HTTP TOOLS FROM THIRD PARTIES APPLICATION
from pulsar.utils.importer import import_module

def get_library(cfg):
    name = cfg.settings.httplib
    if name == 'gunicorn':
        import pulsar.http._gunicorn as httplib
    elif name == 'werkzeug':
        import pulsar.http._werkzeug as httplib
    else:
        raise ValueError('Unknown {0} http library'.format(name))
    return httplib