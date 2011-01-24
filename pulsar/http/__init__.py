#
#   HTTP TOOLS FROM THIRD PARTIES APPLICATION
from pulsar.utils.importer import import_module

def get_httplib(cfg = None):
    name = 'gunicorn' if not cfg else cfg.settings['httplib'].value
    if name == 'gunicorn':
        import pulsar.http.http_gunicorn as httplib
    elif name == 'werkzeug':
        import pulsar.http._werkzeug as httplib
    else:
        import pulsar.http.http_gunicorn as httplib
    return httplib