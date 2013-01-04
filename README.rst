Event driven concurrent framework for python. Tested in Windows and Linux,
it requires python 2.6, 2.7, 3.2, 3.3 or pypy_.
With pulsar you can write asynchronous servers performing one or several
activities in different threads and/or processes.

:CI: |pulsar-build|_ 
:Documentation: http://packages.python.org/pulsar/
:Dowloads: http://pypi.python.org/pypi/pulsar
:Source: https://github.com/quantmind/pulsar
:Keywords: server, asynchronous, concurrency, actor, thread, process, socket, task queue


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
it can be configured to run in multi-processing mode, multi-threading mode or
a combination of the two.

Installing
============

Pulsar is a stand alone python library and it can be installed via `pip`::

    pip install pulsar
    
`easy_install` or downloading the tarball from pypi_.


Applications
==============
Pulsar design allows for a host of different applications to be implemented
in an elegant and efficient way. Out of the box it is shipped with the
the following

* Socket servers.
* WSGI server.
* JSON-RPC WSGI middleware.
* Web Sockets WSGI middleware.
* Distributed task queue.
* Shell for asynchronous scripting.
* Asynchronous test suite.

.. _examples:

Examples
=============
Check out the ``examples`` directory for various working applications created using
pulsar alone. It includes:

* Hello world! wsgi example.
* An Httpbin wsgi application (similar to http://httpbin.org/).
* An HTTP Proxy server with headers middleware.
* A simple JSON-RPC Calculator server.
* A taskqueue application with a JSON-RPC interface.
* Websocket graph.
* The `dining philosophers problem <http://en.wikipedia.org/wiki/Dining_philosophers_problem>`_.
* Asynchronous shell.


Design
=============
Pulsar internals are based on `actors primitive`_. ``Actors`` are the *atoms* of 
pulsar's concurrent computation, they do not share state between them,
communication is achieved via asynchronous inter-process message passing,
implemented using the standard python socket library.

Two special classes of actors are the ``Arbiter``, used as a singleton_,
and the ``Monitor``, a manager of several actors performing similar functions.
The Arbiter runs the main eventloop and it controls the life of all actors.
Monitors manage group of actors performing similar functions, You can think
of them as a pool of actors.

More information about design and philosophy in the documentation.  


Add-ons
=========
Pulsar checks if some additional libraries are available, either
during installation or at runtime, and uses them to add new functionalities.

* setproctitle_: if installed, pulsar can use it to change the processes names of
  the running application.  
* psutil_: if installed, a ``system`` key is available in the dictionary returned by
  Actor info method.

Running Tests
==================
Pulsar test suite uses the pulsar test application. If you are using python 2.6
you need to install unittest2_, and if not running on python 3.3, the mock_
library is also needed. To run tests::

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

.. _contributing:

Contributing
=================
Development of pulsar_ happens at Github. We very much welcome your contribution
of course. To do so, simply follow these guidelines:

1. Fork pulsar_ on github
2. Create a topic branch ``git checkout -b my_branch``
3. Push to your branch ``git push origin my_branch``
4. Create an issue at https://github.com/quantmind/pulsar/issues with a link to your patch.


.. _license:

License
=============
This software is licensed under the New BSD_ License. See the LICENSE
file in the top distribution directory for the full license text.

.. _gunicorn: http://gunicorn.org/
.. _nodejs: http://nodejs.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _tornado: http://www.tornadoweb.org/
.. _celery: http://celeryproject.org/
.. _multiprocessing: http://docs.python.org/library/multiprocessing.html
.. _`actors primitive`: http://en.wikipedia.org/wiki/Actor_model
.. _unittest2: http://pypi.python.org/pypi/unittest2
.. _mock: http://pypi.python.org/pypi/mock
.. _setproctitle: http://code.google.com/p/py-setproctitle/
.. _psutil: http://code.google.com/p/psutil/
.. _pypi: http://pypi.python.org/pypi/pulsar
.. _pypy: http://pypy.org/
.. _BSD: http://www.opensource.org/licenses/bsd-license.php
.. _pulsar: https://github.com/quantmind/pulsar
.. _singleton: http://en.wikipedia.org/wiki/Singleton_pattern