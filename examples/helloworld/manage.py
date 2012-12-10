'''Pulsar "Hello World!" application. It runs a HTTP server which
display the two famous words::

    python manage.py
    
To see options type::

    python manage.py -h
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
from pulsar.apps import wsgi


def hello(environ, start_response):
    '''Pulsar HTTP "Hello World!" application'''
    data = b'Hello World!\n'
    status = '200 OK'
    response_headers = [
        ('Content-type','text/plain'),
        ('Content-Length', str(len(data)))
    ]
    start_response(status, response_headers)
    return iter([data])


def server(description=None, **kwargs):
    description = description or 'Pulsar Hello World Application'
    return wsgi.WSGIServer(callable=hello,
                           description=description,
                           **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()
