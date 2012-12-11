Event driven concurrent framework for python. Tested in Windows and Linux,
it requires python 2.6, 2.7, 3.2 or pypy_.
With pulsar you can write asynchronous servers performing one or several
activities in different threads and/or processes.

|pulsar-build|_

.. |pulsar-build| image:: https://secure.travis-ci.org/quantmind/pulsar.png?branch=master
.. _pulsar-build: http://travis-ci.org/quantmind/pulsar

An example of a web server written with ``pulsar`` application
framework which responds with "Hello World!" for every request::

    
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
        return [data]
    
    
    if __name__ == '__main__':
        return wsgi.WSGIServer(callable=hello).start()
    
    
Pulsar's goal is to provide an easy way to build scalable network programs.
In the "Hello world!" web server example above, many client
connections can be handled concurrently.
Pulsar tells the operating system (through epoll or select) that it should be
notified when a new connection is made, and then it goes to sleep.

Pulsar uses the multiprocessing_ module from the standard python library and
it can be configured to run in multi-processing or multi-threading mode.

Installing
============

Pulsar is a stand alone python library which works for python 2.6 up to
python 3.3.
Installing pulsar can be done via `pip`::

    pip install pulsar
    
`easy_install` or downloading the tarball from pypi_.


Applications
==============
Pulsar design allows for a host of different applications to be implemented
in an elegant and efficient way. Out of the box it is shipped with the
the following

* Socket servers.
* WSGI server (with a JSON-RPC handler).
* Distributed task queue.
* Shell for asynchronous scripting.
* Asynchronous test suite.

Examples
=============
Check out the ``examples`` directory for various working applications created using
pulsar alone. It includes:

* Hello world! wsgi example.
* An Httpbin wsgi application.
* A HTTP Proxy server.
* A simple JSON-RPC Calculator server.
* A taskqueue application with a JSON-RPC.
* Websocket graph.
* The dining philosophers problem.
* Asynchronous shell.


Design
=============
Pulsar internals are based on `actors primitive`_. ``Actors`` are the *atoms* of 
pulsar's concurrent computation, they do not share state between them,
communication is achieved via asynchronous inter-process message passing,
implemented using the standard python socket library.

Two special classes of actors are the ``Arbiter``, used as a singletone,
and the ``Monitor``, a manager of several actors performing similar functions.
The Arbiter runs the main eventloop and it controls the life of all actors.
Monitors manage group of actors performing similar functions, You can think
of them as a pool of actors.

More information about design and philosophy in the documentations.  


Add-ons
=========
Pulsar check if some additional python libraries are available, either
during installation or at runtime, and uses them to add new functionalities.

* setproctitle_. If installed, pulsar will used to change the processes names.
  To install::

    pip install setproctitle
    
* psutil_. If installed, a ``system`` key is available in the dictionary returned by
  Actor info method.

Running Tests
==================
Pulsar test suite uses the pulsar test applications. If you are using python 2.6
you need to install unittest2_. To run the tests::

    python runtests.py

For options and help type::

    python runtests.py -h
    
For full coverage run tests with the following flags::

    python runtests.py --concurrency thread --profile --benchmark --http-py-parser --verbosity 2

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
.. _unittest2: http://pypi.python.org/pypi/unittest2
.. _setproctitle: http://code.google.com/p/py-setproctitle/
.. _psutil: http://code.google.com/p/psutil/
.. _pypi: http://pypi.python.org/pypi/pulsar
.. _pypy: http://pypy.org/
