.. module:: pulsar

.. _tutorials-wsgi:

========
WSGI
========

We assume you are familiar with python Web Standard Gateway Interface (WSGI),
if not you should read pep-3333_ first.


Setting Up
================

All pulsar needs to set-up a WSGI server is the callable of your
application. We start by creating a script, called ``managed.py``
which will be used to run your server::

    from pulsar.apps import wsgi

    def callable(environ, start_response):
        ...

    if __name__ == '__main__':
        wsgi.WSGIServer(callable).start()


.. _multi-wsgi:

Multiple Process
======================

When serving lots of requests, an asynchronous single-process server may not be
enough. To add power, you can run the server using 2 or more process (workers)::

    python manage.py --workers 4

Multi-process servers are only available for:

* Posix systems.
* Windows running python 3.3 or above.

Check :ref:`multi-process socket servers <socket-server-concurrency>`
documentation for more information.


.. _blocking-wsgi:

Blocking middleware
============================

Pulsar is an asynchronous server, however, the callable which
consume your application may not be.


.. note::

    Most web-framework in the public domain are blocking by
    construction. This is OK as long as the WSGI callable consumes the
    request and return control to pulsar as fast as possible.

In order to facilitate the integration of blocking frameworks, pulsar ships with
several WSGI utilities, including:

* :class:`.WsgiHandler` for grouping several WSGI middleware together
* :func:`.wait_for_body_middleware` should be placed before any synchronous middleware
  which needs to process the body of the HTTP request (such as POST methods)
* :func:`.middleware_in_executor`, a wrapper to execute a blocking middleware in the
  executor (threading pool) of the actor serving the request.

A pulsar friendly WSGI handler for a ``callable`` can take the following form::

    from pulsar.apps import wsgi

    def callable(environ, start_response):
        ...

    wsgi_handler = wsgi.WsgiHandler([wsgi.wait_for_body_middleware,
                                     wsgi.middleware_in_executor(callable)])

    if __name__ == '__main__':
        wsgi.WSGIServer(wsgi_handler()).start()

To control the number of thread workers in the event loop executor, one uses the
:ref:`thread-workers <setting-thread_workers>` option. For example, the
following command::

    python manage.py -w 4 --thread-workers 10

will run four :ref:`process based actors <concurrency>`, each with
an executor with up to 10 threads.


Serving More than one application
=======================================

You can serve as many applications, on different addresses, as you like.
For example, our ``manage.py`` script could be::

    from pulsar import arbiter
    from pulsar.apps import wsgi

    def callable1(environ, start_response):
        ...

    def callable2(environ, start_response):
        ...


    if __name__ == '__main__':
        wsgi.WSGIServer(callable1, name='wsgi1')
        wsgi.WSGIServer(callable2, name='wsgi2', bind=8070)
        arbiter().start()



.. _pep-3333: http://www.python.org/dev/peps/pep-3333/