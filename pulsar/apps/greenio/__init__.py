"""
Pulsar :mod:`~greenio` facilitates the integration of synchronous
third-party libraries into pulsar asynchronous framework.
It requires the :greenlet:`greenlet <>` library.

If you want to understand how integration works but you are unfamiliar with
greenlets, check out the :greenlet:`greenlet documentation <>` first.

.. note::

    pulsar-odm_ is a separate library, maintained by the same authors of
    pulsar, which provides asynchronous object data mapping with
    asyncio_ and sqlalchemy_ and uses pulsar greenio extensions.

This application **does not use monkey patching** and therefore it
works quite differently from implicit asynchronous libraries such as
gevent_. This module provides the user with a set
of utilities for **explicitly** transferring execution from one greenlet
to another which executes the blocking call in a greenlet-friendly way.

The caller has the responsibility that the blocking call is greenlet-friendly,
i.e. it transfers the control of execution back to the parent greenlet when
needed.

Usage
=======

Lets assume you are building an application which uses pulsar asynchronous
engine and would like to

* either use an external library written in blocking style,
  i.e. without yielding control to the event loop when IO calls are performed.
* or write your client code without dealing with :class:`~asyncio.Future` or
  coroutines, in other words in an implicit asynchronous style. In this way
  your client code can be used on other frameworks just as well.

In both cases, the :class:`~pulsar.apps.greenio` application is what you need.

.. _green-wsgi:

Green WSGI
---------------

Assume you are using pulsar web server and would like to write your application
in an implicit asynchronous mode, i.e. without dealing with futures nor
coroutines, then you can wrap your WSGI ``app`` with the :class:`.GreenWSGI`
utility::

    from pulsar.apps.wsgi import WSGIServer
    from pulsar.apps.greenio import GreenPool, GreenWSGI

    green_pool = greenio.GreenPool()
    callable = GreenWSGI(app, green_pool)

    WSGIServer(callable=callable).start()

The :class:`.GreenPool` manages a pool of greenlets which execute your
application. In this way, within your ``app`` you can invoke the
:func:`.wait` function when needing to wait for asynchronous results to be
ready.

.. _green-http:

GreenHttp
-----------------

The :class:`.HttpClient` can be used with greenlets::

    >>> from pulsar.apps.greenio import GreenHttp
    >>> http = GreenHttp()

And now you can write synchronous looking code and run it in a separate
greenlet via the :func:`.run_in_greenlet` decorator::

    @greenio.run_in_greenlet
    def example():
        response = http.get('http://bbc.co.uk')
        ...
        return response.text()


and somewhere, in your asynchronous code::

        result = await example()
        result == ...


the :func:`.run_in_greenlet` decorator, execute the function on a child
greenlet without blocking the asynchronous engine. Once the ``example``
function returns, the asynchronous code continue from the ``yield``
statement as usual.


API
======

Wait
----------

.. autofunction:: pulsar.apps.greenio.utils.wait


Run in greenlet
-------------------

.. autofunction:: pulsar.apps.greenio.utils.run_in_greenlet


Green Pool
----------------

.. autoclass:: pulsar.apps.greenio.pool.GreenPool
   :members:
   :member-order: bysource

Green Lock
----------------

.. autoclass:: pulsar.apps.greenio.lock.GreenLock
   :members:
   :member-order: bysource


Green WSGI
----------------

.. autoclass:: pulsar.apps.greenio.wsgi.GreenWSGI
   :members:
   :member-order: bysource


.. _gevent: http://www.gevent.org/
.. _pulsar-odm: https://github.com/quantmind/pulsar-odm
.. _sqlalchemy: http://www.sqlalchemy.org/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
"""
from greenlet import greenlet, getcurrent

from .pool import GreenPool
from .lock import GreenLock
from .http import GreenHttp
from .wsgi import GreenWSGI
from .utils import MustBeInChildGreenlet, wait, run_in_greenlet

__all__ = ['GreenPool',
           'GreenLock',
           'GreenHttp',
           'GreenWSGI',
           'MustBeInChildGreenlet',
           'wait',
           'run_in_greenlet',
           'greenlet',
           'getcurrent']
