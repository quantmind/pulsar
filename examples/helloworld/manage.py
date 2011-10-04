'''Pulsar "Hello World!" application. It runs a HTTP server which
display the two famous words::

    python manage.py
'''
import pulsar
from pulsar.apps import wsgi


def hello(environ, start_response):
    '''Pulsar HTTP "Hello World!" application'''
    data = b'Hello World!\n'
    status = '200 OK'
    response_headers = (
        ('Content-type','text/plain'),
        ('Content-Length', str(len(data)))
    )
    start_response(status, response_headers)
    return iter([data])


def server(**kwargs):
    return wsgi.createServer(callable = hello, **kwargs)
    
def start_server(**params):
    return server(**params).start()

if __name__ == '__main__':
    start_server()
