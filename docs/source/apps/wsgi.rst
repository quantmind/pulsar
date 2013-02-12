.. _apps-wsgi:

=================
WSGI Servers
=================

.. automodule:: pulsar.apps.wsgi

.. _apps-wsgi-handlers:

HTTP Parsers
================

Pulsar ships with its own :ref:`HTTP parser <tools-http-parser>` used by
both server and the :ref:`HTTP client <apps-http>`. Headers are collected using
the :ref:`Headers data structure <tools-http-headers>` which exposes a
list/dictionary-type interface.

At runtime, pulsar checks if the http-parser_ package and cython_
are available. If this is the case, it switches the default HTTP parser
to be the one provided by the :mod:`http_parser.paser` module.
To check if the C parser is the default parser::

    from pulsar.lib import hasextensions


WSGI application handlers
===============================

Pulsar is shipped with several WSGI application handlers which facilitate the
development of server-side python web applications. A WSGI application handler
is always a callable, either a function or a callable instance, which
accept two positional arguments: *environ* and *start_response*.

WsgiHandler
~~~~~~~~~~~~~~~~~~~~

The first and most basic handler is the :class:`WsgiHandler` which is
a step above the hello callable above. It accepts two iterables,
a list of wsgi middleware and an optional list of response middleware.

Response middleware is a callable of the form::

    def my_response_middleware(environ, response):
        ...
        
where *environ* is the WSGI environ dictionary and *response* is an instance
of :class:`WsgiResponse`. 

Router
~~~~~~~~~~~~~~~~~~~~

Next up is routing. Routing is the process of match and
parse the URL to something we can use. Pulsar provides a flexible integrated
routing system you can use for that. It works by creating a
:class:`Router` instance with its own ``rule`` and, optionally, additional
sub-routers for handling additional urls::

    class Page(Router):
        
        def get(self, request):
            '''This method handle request with get-method''' 
            ...
            
        def post(self, request):
            '''This method handle request with post-method''' 
            ...
            
    middleware = Page('/bla')
    
The ``middleware`` constructed can be used to serve ``get`` and ``post`` methods
at ``/bla``.

The :class:`Router` introduces a new element into pulsar WSGI handlers, the
:class:`WsgiRequest` instance ``request``, which is a light-weight
wrapper of the WSGI environ.


API
=========

WSGI Server
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WSGIServer
   :members:
   :member-order: bysource
   

WsgiResponse
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WsgiResponse
   :members:
   :member-order: bysource
   
   
WsgiHandler
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WsgiHandler
   :members:
   :member-order: bysource
   
   
Router
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Router
   :members:
   :member-order: bysource
   

MediaRouter
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: MediaRouter
   :members:
   :member-order: bysource
   

WsgiRequest
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WsgiRequest
   :members:
   :member-order: bysource
   
   
Url Route
~~~~~~~~~~~~~~~~~~~~~

.. automodule:: pulsar.apps.wsgi.route
   
   
.. _http-parser: https://github.com/benoitc/http-parser
.. _cython: http://cython.org/