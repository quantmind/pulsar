'''
Pulsar :mod:`~greenio` facilitates the integration of synchronous
third-party libraries into pulsar asynchronous framework.
It requires the :greenlet:`greenlet <>` library.

If you want to understand how integration works but you are unfamiliar with
greenlets, check out the :greenlet:`greenlet documentation <>` first.

This application **does not use monkey patching** and therefore it
works quite differently from implicit asynchronous libraries such as
gevent_. All it does, it provides the user with a set
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
coroutines, then you can wrap your WSGI ``app`` with the :class:`.GreenWSGI`
utility::

    from pulsar.apps import wsgi, greenio

    green_pool = greenio.GreenPool()
    callable = wsgi.WsgiHandler([wsgi.wait_for_body_middleware,
                                 greenio.GreenWSGI(app, green_pool)],
                                async=True)

    wsgi.WsgiServer(callable=callable).start()

The :class:`.GreenPool` manages a pool of greenlets which execute your
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


Run in greenlet
-------------------

.. autofunction:: run_in_greenlet


Green Pool
----------------

.. autoclass:: GreenPool
   :members:
   :member-order: bysource

Green Lock
----------------

.. autoclass:: GreenLock
   :members:
   :member-order: bysource


Green WSGI
----------------

.. autoclass:: GreenWSGI
   :members:
   :member-order: bysource


.. _gevent: http://www.gevent.org/
'''
import threading
import logging
from collections import deque
from functools import wraps

from greenlet import greenlet, getcurrent

from pulsar import async
from pulsar import Future, get_event_loop, AsyncObject, is_async


_DEFAULT_WORKERS = 100
_MAX_WORKERS = 1000


class _DONE:
    pass


class GreenletWorker(greenlet):
    pass


def wait(value):
    '''Wait for a possible asynchronous value to complete.
    '''
    current = getcurrent()
    parent = current.parent
    return parent.switch(value) if parent else value


def run_in_greenlet(callable):
    '''Decorator to run a ``callable`` on a new greenlet.

    A ``callable`` decorated with this decorator returns a coroutine
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        green = GreenletWorker(callable)
        # switch to the new greenlet
        result = green.switch(*args, **kwargs)
        # back to the parent
        while is_async(result):
            # keep on switching back to the greenlet if we get a Future
            try:
                result = green.switch((yield from result))
            except Exception as exc:
                result = green.throw(exc)

        return green.switch(result)

    return _


class GreenPool(AsyncObject):
    '''A pool of running greenlets.

    This pool maintains a group of greenlets to perform asynchronous
    tasks via the :meth:`submit` method.
    '''
    worker_name = 'exec'

    def __init__(self, max_workers=None, loop=None):
        self._loop = loop or get_event_loop()
        self._max_workers = min(max_workers or _DEFAULT_WORKERS, _MAX_WORKERS)
        self._greenlets = set()
        self._available = set()
        self._queue = deque()
        self._shutdown = False
        self._waiter = None
        self._logger = logging.getLogger('pulsar.greenpool')
        self._shutdown_lock = threading.Lock()

    @property
    def max_workers(self):
        return self._max_workers

    @max_workers.setter
    def max_workers(self, value):
        value = int(value)
        assert value > 0
        self._max_workers = value

    def submit(self, func, *args, **kwargs):
        '''Equivalent to ``func(*args, **kwargs)``.

        This method create a new task for function ``func`` and adds it to
        the queue.
        Return a :class:`~asyncio.Future` called back once the task
        has finished.
        '''
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'cannot schedule new futures after shutdown')
            future = Future(loop=self._loop)
            self._put((future, func, args, kwargs))
            return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._put(None)
            if wait:
                self._waiter = Future(loop=self._loop)
                return self._waiter

    # INTERNALS
    def _adjust_greenlet_count(self):
        if (not self._shutdown and not self._available and
                len(self._greenlets) < self._max_workers):
            green = GreenletWorker(self._green_run)
            self._greenlets.add(green)
            self.logger.debug('Num greenlets: %d', len(self._greenlets))
            green.switch()
        return self._available

    def _put(self, task):
        # Run in the main greenlet of the evnet-loop thread
        self._queue.appendleft(task)
        self._check_queue()

    def _check_queue(self):
        # Run in the main greenlet of the event-loop thread
        if not self._adjust_greenlet_count():
            self.logger.debug('No greenlet available')
            return self._loop.call_soon(self._check_queue)
        try:
            task = self._queue.pop()
        except IndexError:
            return
        async(self._green_task(self._available.pop(), task), loop=self._loop)

    def _green_task(self, green, task):
        # Coroutine executing the in main greenlet
        # This coroutine is executed for every task put into the queue

        while task is not _DONE:
            # switch to the greenlet to start the task
            task = green.switch(task)

            # if an asynchronous result is returned, yield from
            while is_async(task):
                try:
                    task = yield from task
                except Exception as exc:
                    # This call can return an asynchronous component
                    task = green.throw(exc)

    def _green_run(self):
        # The run method of a worker greenlet
        task = True
        while task:
            green = getcurrent()
            parent = green.parent
            assert parent
            # add greenlet in the available greenlets
            self._available.add(green)
            task = parent.switch(_DONE)  # switch back to the main execution
            if task:
                future, func, args, kwargs = task
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            else:  # Greenlet cleanup
                self._greenlets.remove(green)
                if self._greenlets:
                    self._put(None)
                elif self._waiter:
                    self._waiter.set_result(None)
                    self._waiter = None
                parent.switch(_DONE)


class GreenLock:
    '''A Locking primitive that is owned by a particular greenlet
    when locked.The main greenlet cannot acquire the lock.

    A primitive lock is in one of two states, 'locked' or 'unlocked'.

    It is created in the unlocked state. It has two basic methods,
    :meth:`.acquire` and :meth:`.release. When the state is unlocked,
    :meth:`.acquire` changes the state to locked and returns immediately.

    When the state is locked, :meth:`.acquire` blocks the current greenlet
    until a call to :meth:`.release` changes it to unlocked,
    then the :meth:`.acquire` call resets it to locked and returns.
    '''
    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()
        self._local = threading.local()
        self._local.locked = None
        self._queue = deque()

    def locked(self):
        ''''Return the greenlet that acquire the lock or None.
        '''
        return self._local.locked

    def acquire(self, timeout=None):
        '''Acquires the lock if in the unlocked state otherwise switch
        back to the parent coroutine.
        '''
        green = getcurrent()
        parent = green.parent
        if parent is None:
            raise RuntimeError('acquire in main greenlet')

        if self._local.locked:
            future = Future(loop=self._loop)
            self._queue.append(future)
            parent.switch(future)

        self._local.locked = green
        return self.locked()

    def release(self):
        '''Release the lock.

        This method should only be called in the locked state;
        it changes the state to unlocked and returns immediately.
        If an attempt is made to release an unlocked lock,
        a RuntimeError will be raised.
        '''
        if self._local.locked:
            while self._queue:
                future = self._queue.popleft()
                if not future.done():
                    return future.set_result(None)
            self._local.locked = None
        else:
            raise RuntimeError('release unlocked lock')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.release()


class GreenWSGI:
    '''Wraps a WSGI application to be executed on a :class:`.GreenPool`
    '''
    def __init__(self, wsgi, pool):
        self.wsgi = wsgi
        self.pool = pool

    def __call__(self, environ, start_response):
        return self.pool.submit(self._green_handler, environ, start_response)

    def _green_handler(self, environ, start_response):
        return wait(self.wsgi(environ, start_response))
