'''This example is a simple WSGI_ script which displays
the "Hello World!" message. To run the script type::

    python manage.py
    
To see all options available type::

    python manage.py -h

.. autofunction:: hello

.. _WSGI: http://www.python.org/dev/peps/pep-3333/ 
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
from pulsar.apps import wsgi


def hello(environ, start_response):
    '''The WSGI_ application handler which returns an iterable
over the "Hello World!" message.'''
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
