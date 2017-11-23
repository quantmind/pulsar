.. _wsgi-routing:

================
Routing
================

.. module:: pulsar.apps.wsgi


.. contents::
    :local:

Routing is the process of matching and parsing a URL to something we can use.
Pulsar provides a flexible integrated
routing system you can use for that. It works by creating a
:class:`Router` instance with its own ``rule`` and, optionally, additional
sub-routers for handling additional urls::

    class Page(Router):
        response_content_types = RouterParam(('text/html',
                                              'text/plain',
                                              'application/json'))

        def get(self, request):
            "This method handle requests with get-method"
            ...

        def post(self, request):
            "This method handle requests with post-method"
            ...

        def delete(self, request):
            "This method handle requests with delete-method"
            ...

        ...


    middleware = Page('/bla')


.. _wsgi-router:

Router
=====================

The :ref:`middleware <wsgi-middleware>` constructed in the snippet above
handles ``get`` and ``post`` methods at the ``/bla`` url.
The :class:`Router` introduces a new element into pulsar WSGI handlers, the
:ref:`wsgi request <app-wsgi-request>`, a light-weight wrapper of the
WSGI environ.

For an exhaustive example on how to use the :class:`Router` middleware make
sure you check out the :ref:`HttpBin example <tutorials-httpbin>`.

.. autoclass:: Router
   :members:
   :member-order: bysource


.. _wsgi-media-router:

Media Router
=====================

The :class:`MediaRouter` is a specialised :class:`Router` for serving static
files such ass ``css``, ``javascript``, images and so forth.

.. autoclass:: MediaRouter
   :members:
   :member-order: bysource


File Response
=====================

High level, battery included function for serving small and large files
concurrently. Caveat, you app does not need to be asynchronous to use this
method.

.. autofunction:: file_response


RouterParam
=================

.. autoclass:: RouterParam
   :members:
   :member-order: bysource

Route rules
=============

Routing classes for matching and parsing urls.

.. note::

    The :mod:`~.route` module was originally from the routing
    module in werkzeug_. Original License:

    copyright (c) 2011 by the Werkzeug Team. License BSD

.. _werkzeug: https://github.com/mitsuhiko/werkzeug

A :class:`Route` is a class for relative url paths::

    r1 = Route('bla')
    r2 = Route('bla/foo')

Integers::

    # accept any integer
    Route('<int:size>')
    # accept an integer between 1 and 200 only
    Route('<int(min=1,max=200):size>')


Paths::

    # accept any path (including slashes)
    Route('<path:pages>')
    Route('<path:pages>/edit')



.. _wsgi-route-decorator:

Route decorator
==================

.. autoclass:: route
   :members:
   :member-order: bysource

.. _apps-wsgi-route:

Route
================

.. autoclass:: Route
   :members:
   :member-order: bysource




.. _WSGI: http://www.wsgi.org
