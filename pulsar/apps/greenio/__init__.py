'''
Greenlet support facilitates the integration of synchronous
third-party libraries into pulsar asynchronous framework.
It requires the greenlet_ library.


Usage
=======
The key component is the :class:`GreenEventLoop`, a specialised
:class:`event-loop <asyncio-event-loop>` which uses the
:class:`GreenTask` to switch between greenlets.

.. _greenlet: http://greenlet.readthedocs.org/
'''
import asyncio
from asyncio import Future
from inspect import isgeneratorfunction
from functools import wraps

import greenlet

from pulsar import async, task, coroutine_return
from pulsar.async.futures import Task, chain_future
from pulsar.async.threads import run_in_executor
from pulsar.utils.config import Global

from .pool import GreenPool, RunInPool
from .local import local


class PulsarGreenlet(greenlet.greenlet):
    pass


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


def run_in_greenlet(method):

    @wraps(method)
    def _(*args, **kwargs):
        gr = PulsarGreenlet(method)
        result = gr.switch(*args, **kwargs)
        while isinstance(result, Future):
            result = gr.switch((yield result))
        coroutine_return(result)


    return _


class middleware_in_greenlet:
    '''Run a function on a separate greenlet
    '''
    def __init__(self, middleware, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._middleware = middleware

    @task
    def __call__(self, *args, **kwargs):
        gr = PulsarGreenlet(self._middleware)
        result = gr.switch(*args, **kwargs)
        while isinstance(result, Future):
            result = gr.switch((yield result))
        coroutine_return(result)


def wait_fd(fd, read=True):
    '''Wait for an event on file descriptor ``fd``.

    :param fd: file descriptor
    :param read=True: wait for a read event if ``True``, otherwise a wait
        for write event
    '''
    current = greenlet.getcurrent()
    parent = current.parent
    assert parent, '"wait_fd" must be called by greenlet with a parent'
    try:
        fileno = fd.fileno()
    except AttributeError:
        fileno = fd
    loop = asyncio.get_event_loop()
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


def _done_wait_fd(fd, future, read):
    if read:
        future._loop.remove_reader(fd)
    else:
        future._loop.remove_writer(fd)
    future.set_result(None)
