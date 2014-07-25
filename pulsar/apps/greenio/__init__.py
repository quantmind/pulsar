'''
Greenlet support facilitates the integration of synchronous
third-party libraries into pulsar asynchronous framework.
It requires the :greenlet:`greenlet <>` library.

If you want to understand how integration works but you are unfamiliar with
greenlets, check out the :greenlet:`greenlet documentation <>` first.
On the other hand,
if you need to use it in the context of :ref:`asynchronous psycopg2 <psycopg2>`
connections for example, you can skip the implementation details.

This application **does not use monkey patching** and therefore it
works quite differently from implicit asynchronous libraries such as
gevent_. All it does, it provides the user with a limited set
of utilities for **explicitly** transferring execution from one greenlet
to a another which execute the blocking call in a greenlet-friendly way.

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
coroutines, then you can wrap your wsgi ``app`` with the :class:`.RunInPool`
utility::

    from pulsar.apps import wsgi, greenio

    callable = wsgi.WsgiHandler([wsgi.wait_for_body_middleware,
                                 greenio.RunInPool(app, 20)])

    wsgi.WsgiServer(callable=callable).start()

The :class:`.RunInPool` manages a pool of greenlets which execute your
application. In this way, within your ``app`` you can invoke the
:func:`.wait` function when needing to wait for asynchronous results to be
ready.

.. _green-http:

Green Http
-----------------

The :class:`.HttpClient` can be used with greenlets::

    >>> from pulsar.apps.http import HttpClient
    >>> http = HttpClient(green=True)
    >>> http.green
    True

And now you can write synchronous looking code and run it in a separate
greenlet via the :func:`.run_in_greenlet` decorator::

    @greenio.run_in_greenlet
    def example():
        response = http.get('http://bbc.co.uk')
        ...
        return 'done'


and somewhere, in your asynchronous code::

        result = yield example()
        result == 'done'


the :func:`.run_in_greenlet` decorator, execute the function on a child
greenlet without blocking the asynchronous engine. Once the ``example``
function returns, the asynchronous code continue from the ``yield``
statement as usual.


API
======

Wait
----------

.. autofunction:: wait

Wait file descriptor
-----------------------

.. autofunction:: wait_fd


Run in greenlet
-------------------

.. autofunction:: run_in_greenlet


Green task
-------------

.. autofunction:: green_task


.. module:: pulsar.apps.greenio.pool

Green Pool
----------------

.. autoclass:: GreenPool
   :members:
   :member-order: bysource

Run in Pool
----------------

.. autoclass:: RunInPool
   :members:
   :member-order: bysource


.. _psycopg2:

Psycopg2
===========

.. automodule:: pulsar.apps.greenio.pg


.. _gevent: http://www.gevent.org/
'''
from functools import wraps, partial

import greenlet

from pulsar import isfuture, async, coroutine_return, From

from .pool import GreenPool, RunInPool
from .local import local


class PulsarGreenlet(greenlet.greenlet):
    pass


def run_in_greenlet(callable):
    '''Decorator to run a ``callable`` on a new greenlet.

    A ``callable`` decorated with this decorator returns a coroutine
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        gr = PulsarGreenlet(callable)
        # switch to the new greenlet
        result = gr.switch(*args, **kwargs)
        # back to the parent
        while isfuture(result):
            # keep on switching back to the greenlet if we get a Future
            result = gr.switch((yield From(result)))
        # For some reason this line does not show in coverage reports
        # but it is covered!
        coroutine_return(result)    # prgma nocover

    return _


def green_task(method):
    '''Decorator to run a ``method`` an a new greenlet in the event loop
    of the instance of the bound ``method``.

    This method is the greenlet equivalent of the :func:`.task` decorator.
    The instance must be an :ref:`async object <async-object>`.

    :return: a :class:`~asyncio.Future`
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        future = Future(loop=self._loop)
        self._loop.call_soon_threadsafe(
            _green, self, method, future, args, kwargs)
        return future

    return _


def wait(coro_or_future, loop=None):
    '''Wait for a coroutine or a :class:`~asyncio.Future` to complete.

    **This function must be called from a greenlet other than the main one**.
    It can be used in conjunction with the :func:`run_in_greenlet`
    decorator or the :class:`.GreenPool`.
    '''
    current = greenlet.getcurrent()
    parent = current.parent
    assert parent, 'Waiter cannot be initialised in main greenlet'
    future = async(coro_or_future, loop=loop)
    parent.switch(future)
    return future.result()


def wait_fd(fd, read=True):
    '''Wait for an event on file descriptor ``fd``.

    :param fd: file descriptor
    :param read=True: wait for a read event if ``True``, otherwise a wait
        for write event.

    This function must be invoked from a coroutine with parent, therefore
    invoking it from the main greenlet will raise an exception.
    Check how this function is used in the :func:`.psycopg2_wait_callback`
    function.
    '''
    current = greenlet.getcurrent()
    parent = current.parent
    assert parent, '"wait_fd" must be called by greenlet with a parent'
    try:
        fileno = fd.fileno()
    except AttributeError:
        fileno = fd
    future = Future()
    # When the event on fd occurs switch back to the current greenlet
    if read:
        future._loop.add_reader(fileno, _done_wait_fd, fileno, future, read)
    else:
        future._loop.add_writer(fileno, _done_wait_fd, fileno, future, read)
    # switch back to parent greenlet
    parent.switch(future)
    return future.result()


# INTERNALS
def _done_wait_fd(fd, future, read):
    if read:
        future._loop.remove_reader(fd)
    else:
        future._loop.remove_writer(fd)
    future.set_result(None)


def _green(self, method, future, args, kwargs):
    # Called in the main greenlet
    try:
        gr = PulsarGreenlet(method)
        result = gr.switch(self, *args, **kwargs)
        if isinstance(result, Future):
            result.add_done_callback(partial(_green_check, gr, future))
        else:
            future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def _green_check(gr, future, fut):
    # Called in the main greenlet
    try:
        result = gr.switch(fut.result())
        if isinstance(result, Future):
            result.add_done_callback(partial(_green_check, gr, future))
        else:
            future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)
