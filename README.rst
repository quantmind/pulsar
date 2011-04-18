
Event driven network library for python. Tested in Windows and Linux,
it requires python 2.6 and up, including python 3.

An example of a web server written with pulsar which responds 
with "Hello World!" for every request::

    
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
    
    
    if __name__ == '__main__':
        wsgi = pulsar.require('wsgi')
        return wsgi.createServer(callable = hello, **kwargs).run()
    
    
Pulsar's goal is to provide an easy way to build scalable network programs.
In the "Hello world!" web server example above, many client connections can be handled
concurrently.
Pulsar tells the operating system (through epoll or select) that it should be
notified when a new connection is made, and then it goes to sleep.

Pulsar uses the multiprocessing_ module from the standard python library and it can
be configured to run in multi-processing or multi-threading mode.

Kudos
============
This project started as fork of gunicorn_.

.. _gunicorn: http://gunicorn.org/
.. _multiprocessing: http://docs.python.org/library/multiprocessing.html
