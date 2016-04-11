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

By default, the ``pulse`` command creates a :class:`Wsgi` middleware which
runs the django application in a separate thread of execution from the
main event loop.
This is a standard programming pattern when using :ref:`asyncio with blocking
functions <asyncio-multithreading>`.
To control the number of thread workers in the event loop executor (which
is a pool of threads) one uses the
:ref:`thread-workers <setting-thread_workers>` option. For example, the
following command::

    python manage.py pulse -w 4 --thread-workers 20

will run four :ref:`process based actors <concurrency>`, each with
an executor with up to 20 threads.

Greenlets
===============

It is possible to run django in fully asynchronous mode, i.e. without
running the middleware in the event loop executor.
Currently, this is available when using :postgresql:`PostgreSql backend <>`
only, and it requires the :greenlet:`greenlet library <>`.

To run django using greenlet support::

    python manage.py pulse -w 4 --greenlet

By default it will run the django middleware on a pool of 100 greenlets (and
therefore approximately 100 separate database connections per actor). To
adjust this number::

    python manage.py pulse -w 4 --greenlet 200


Wsgi middleware
===================

.. autoclass:: Wsgi
   :members:
   :member-order: bysource
'''
from pulsar.apps.wsgi import (LazyWsgi, WsgiHandler,
                              wait_for_body_middleware,
                              middleware_in_executor)
from pulsar.utils.importer import module_attribute


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
        try:
            dotted = settings.WSGI_APPLICATION
        except AttributeError:  # pragma nocover
            dotted = None
        if dotted:
            app = module_attribute(dotted)
        else:
            app = get_wsgi_application()
        app = middleware_in_executor(app)
        return WsgiHandler((wait_for_body_middleware, app))
