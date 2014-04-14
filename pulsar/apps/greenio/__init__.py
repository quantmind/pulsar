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

import greenlet

from pulsar.async.futures import Task, chain_future
from pulsar.async.threads import run_in_executor


class _TaskGreenlet(greenlet.greenlet):
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
            gl = _TaskGreenlet(super(GreenTask, self)._step)
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

    def _wakeup(self, future, inthread=False):
        super(GreenTask, self)._wakeup(future, inthread=inthread)


def _proxy(attr, target):

    def proxy(self, *args, **kwargs):
        meth = getattr(self._loop, attr)
        return meth(*args, **kwargs)

    proxy.__name__ = attr
    proxy.__qualname__ = attr
    proxy.__doc__ = getattr(getattr(target, attr), '__doc__', None)
    return proxy


class GreenEventLoop(asyncio.SelectorEventLoop):
    task_factory = GreenTask

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

    def run_in_executor(self, executor, callback, *args):
        future = run_in_executor(self._loop, executor, callback, *args)
        return chain_future(future, next=Future(loop=self))


for method in ('call_at', 'call_soon', 'add_reader', 'add_writer',
               'remove_reader', 'remove_writer', 'run_forever'):
    setattr(GreenEventLoop, method, _proxy(method, asyncio.SelectorEventLoop))
