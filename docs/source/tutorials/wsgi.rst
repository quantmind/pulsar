.. module:: pulsar

.. _tutorials-wsgi:

========
WSGI  
========

We assume you are familiar with python Web Standard Gateway Interface (WSGI),
if not you should read pep-3333_ first.

Server
=============

Setting Up
~~~~~~~~~~~~~~

All pulsar needs to setup a WSGI server is the wsgi callable of your
application. We start by creating a script, called ``managed.py``
which will be used to run your server::

    from pulsar.apps import wsgi
    
    def callable(environ, start_response):
        ...
        
    if __name__ == '__main__':
        wsgi.WSGIServer(callable).start()


Pulsar is an asynchronous server, however, the callable which
consume your application may not be.


.. note::

    Most web-framework in the public domain are not asynchronous by
    construction. This is ok as long as the WSGI callable consumes the
    request and return control to pulsar as fast as possible.


.. _multi-wsgi:

Multiple Process
~~~~~~~~~~~~~~~~~~~~~~~~

When serving lots of requests, an asynchronous single-process server may not be
enough. To add power, you can run the server using 2 or more process (workers)::

    python manage.py --workers 4
 
Multi-process servers are only available for:

* Posix systems.
* Windows running python 3.2 or above.

Check :ref:`multi-process socket servers <socket-server-concurrency>` 
documentation for more information.

Serving More than one application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        
        
Application Handlers
==========================

.. _tutorial-router:

Router
~~~~~~~~~~~~~~


.. _pep-3333: http://www.python.org/dev/peps/pep-3333/ 