'''
Greenlet support facilitates the integration of synchronous
third-party libraries into pulsar asynchronous framework.
It requires the :greenlet:`greenlet <>` library.

If you want to understand how integration works and you are unfamiliar with
greenlets, check out the :greenlet:`greenlet documentation <>` first.
On the other hand,
if you need to use it in the context of :ref:`asynchronous psycopg2 <psycopg2>`
connections for example, you can skip the implementation details.

This application **does not use monkey patching** and therefore it
works quite differently from implicit asynchronous libraries such as
gevent_. All it does, it provides the user with a set
of utilities for **explicitly** transferring execution from one greenlet
to a another which execute the blocking call in a greenlet-friendly way.

The caller has the responsibility that the blocking call is greenlet-friendly,
i.e. it transfers the control of execution back to the parent greenlet when
needed.

API
======

Wait
------

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
from inspect import isgeneratorfunction
from functools import wraps, partial

import greenlet

from pulsar import Future, get_event_loop, async, task, coroutine_return
from pulsar.async.futures import Task, chain_future
from pulsar.async.threads import run_in_executor
from pulsar.utils.config import Global

from .pool import GreenPool, RunInPool
from .local import local


class PulsarGreenlet(greenlet.greenlet):
    pass


def run_in_greenlet(callable):
    '''Decorator to run a ``callable`` on a new greenlet.

    A callable decorated with this decorator returns a coroutine.
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        gr = PulsarGreenlet(callable)
        result = gr.switch(*args, **kwargs)
        while isinstance(result, Future):
            result = gr.switch((yield result))
        coroutine_return(result)

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
    loop = get_event_loop()
    future = Future(loop=loop)
    # When the event on fd occurs switch back to the current greenlet
    if read:
        loop.add_reader(fileno, _done_wait_fd, fileno, future, read)
    else:
        loop.add_writer(fileno, _done_wait_fd, fileno, future, read)
    # switch back to parent greenlet
    parent.switch(future)
    return future.result()


def wait(coro_or_future, loop=None):
    '''Wait for a coroutine or a future to complete.

    This method must be called from a greenlet other than the main one
    '''
    current = greenlet.getcurrent()
    parent = current.parent
    assert parent, 'Waiter cannot be initialised in main greenlet'
    future = async(coro_or_future, loop)
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


class GreenTask(Task):
    '''An :class:`asyncio.Task` for running synchronous code in greenlets.
    '''
    _greenlet = None

    def _step(self, value=None, exc=None):
        if self._greenlet is None:
            # Means that the task is not currently in a suspended greenlet
            # waiting for results
            assert greenlet.getcurrent().parent is None
            gl = PulsarGreenlet(super(GreenTask, self)._step)
            self._greenlet = gl
            gl.switch(value, exc)
        else:
            gl = self._greenlet.parent
            assert gl.parent is None
            self._greenlet = None
            gl.switch()
            self._step(value, exc)

        # There no waiting future
        if not self._fut_waiter:
            self._greenlet = None
