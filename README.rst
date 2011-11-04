Event driven concurrent framework for python. Tested in Windows and Linux,
it requires python 2.6 and up, including python 3.
With pulsar you can write asynchronous servers performing one or several
activities in different threads and or processes.

An example of a web server written with ``pulsar`` which responds 
with "Hello World!" for every request::

    
    import pulsar
    
    def hello(environ, start_response):
        '''Pulsar HTTP "Hello World!" application'''
        data = b'Hello World!\n'
        status = '200 OK'
        response_headers = (
            ('Content-type','text/plain'),
            ('Content-Length', str(len(data)))
        )
        start_response(status, response_headers)
        return [data]
    
    
    if __name__ == '__main__':
        wsgi = pulsar.require('wsgi')
        return wsgi.createServer(callable = hello, **kwargs).run()
    
    
Pulsar's goal is to provide an easy way to build scalable network programs.
In the "Hello world!" web server example above, many client
connections can be handled concurrently.
Pulsar tells the operating system (through epoll or select) that it should be
notified when a new connection is made, and then it goes to sleep.

Pulsar uses the multiprocessing_ module from the standard python library and
it can be configured to run in multi-processing or multi-threading mode.


Applications
==============
Pulsar design allows for a host of different applications to be implemented
in an elegant and efficient way. Out of the box it is shipped with the
the following

* WSGI server (with a RPC handler too)
* A distributed task queue.
* Pulsar shell for asynchronous scripting (posix only).
* Asynchronous testing suite.

Design
=============
Pulsar internals are based on `actors primitive`_. Actors are the atoms of 
pulsar's concurrent computation,they do not share state between them,
communication is achieved via asynchronous inter-process message passing, implemented using
the standard library ``multiprocessing.Queue`` class.
Two special classes of actors are the ``Arbiter``, used as a singletone,
and the ``Monitor``, a manager of several actors performing similar functions.

More information about design and philosophy in the documentations.  

Kudos
============
Pulsar project started as a fork of gunicorn_ (from where the arbiter idea) and has been developed using
ideas from nodejs_ (api design), twisted_ (the deferred implementation), tornado_ web server
(the event-loop implementation), celery_ (the task queue application) and
many other open-source efforts.

.. _gunicorn: http://gunicorn.org/
.. _nodejs: http://nodejs.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _tornado: http://www.tornadoweb.org/
.. _celery: http://celeryproject.org/
.. _multiprocessing: http://docs.python.org/library/multiprocessing.html
.. _`actors primitive`: http://en.wikipedia.org/wiki/Actor_model