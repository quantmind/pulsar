'''The :mod:`pulsar.apps.pulse` module is a :django:`django application <>`
for running a django web site with pulsar.
Add it to the list of your ``INSTALLED_APPS``::

    INSTALLED_APPS = (
        ...,
        'pulsar.apps.pulse',
        ...
    )

and run the site via the ``pulse`` command::

    python manage.py pulse

Check the :ref:`django chat example <tutorials-django>` for a django chat
application served by a multiprocessing pulsar server.

Greenlets
===============

By default, the ``pulse`` command creates a :class:`Wsgi` middleware which
runs the django application in a separate thread of execution from the
main event loop.
This is a standard programming pattern when using :ref:`asyncio with blocking
functions <asyncio-multithreading>`.

Wsgi middleware
===================

.. autoclass:: Wsgi
   :members:
   :member-order: bysource
'''
from pulsar.apps.wsgi import (LazyWsgi, WsgiHandler,
                              wait_for_body_middleware,
                              middleware_in_executor)
try:
    from pulsar.apps import greenio
    from pulsar.apps.greenio import pg, local
except ImportError:
    greenio = None
    pg = None


class Wsgi(LazyWsgi):
    '''The Wsgi middleware used by the django ``pulse`` command
    '''
    cfg = None

    def setup(self, environ=None):
        '''Set up the :class:`.WsgiHandler` the first time this
        middleware is accessed.
        '''
        from django.conf import settings
        from django.core.wsgi import get_wsgi_application
        #
        app = get_wsgi_application()
        green_workers = self.cfg.greenlet if self.cfg else 0
        if greenio and green_workers:
            if pg:
                pg.make_asynchronous()
            app = greenio.RunInPool(app, green_workers)
            self.green_safe_connections()
        else:
            app = middleware_in_executor(app)
        return WsgiHandler((wait_for_body_middleware, app))

    def green_safe_connections(self):
        from django.db import connections
        connections._connections = local()
