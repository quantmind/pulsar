'''Pulsar "Hello World!" application. It runs a HTTP server which
display the two famous words::

    python manage.py
'''
import pulsar


def hello(environ, start_response):
    '''Pulsar HTTP "Hello World!" application'''
    data = 'Hello World!\n'
    status = '200 OK'
    response_headers = (
        ('Content-type','text/plain'),
        ('Content-Length', str(len(data)))
    )
    start_response(status, response_headers)
    return iter([data])


def server(**kwargs):
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = hello,
                             **kwargs)
    
    
if __name__ == '__main__':
    server().start()
