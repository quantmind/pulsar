from collections import Mapping
from inspect import isgeneratorfunction, isgenerator
from functools import wraps, partial

from pulsar.utils.pep import iteritems, range

from .consts import MAX_ASYNC_WHILE
from .access import (trollius, get_event_loop, async, Return, Future, sleep,
                     From, isfuture, LOGGER, _PENDING, _CANCELLED, _FINISHED,
                     _EVENT_LOOP_CLASSES)

coroutine = trollius.coroutine
iscoroutine = trollius.iscoroutine
CancelledError = trollius.CancelledError
TimeoutError = trollius.TimeoutError
InvalidStateError = trollius.InvalidStateError


__all__ = ['CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'coroutine_return',
           'maybe_async',
           'run_in_loop',
           'async',
           'add_errback',
           'add_callback',
           'future_timeout',
           'task_callback',
           'multi_async',
           'async_while',
           'task',
           'chain_future',
           'future_result_exc',
           'AsyncObject',
           'yield_from']


def coroutine_return(*value):
    raise Return(*value)


def yield_from(coro, timeout=None, loop=None):
    '''Wraps a ``coroutine`` by yielding values wrapped with
    the ``From`` function.
    '''
    value = None
    exc = None
    while True:
        if exc is not None:
            result = coro.throw(exc)
        elif value is not None:
            result = coro.send(value)
        else:
            result = next(coro)
        exc = None
        try:
            if result is not None:
                result = async(result, loop=loop)
                future_timeout(result, timeout)
        except TypeError:
            value = result
        else:
            try:
                value = yield From(result)
            except Exception as e:
                exc = e


def future_timeout(future, timeout=None, exc_class=None):
    '''Add a ``timeout`` to ``future`` in a thread-safe way.

    :return: the ``future``
    '''
    exc_class = exc_class or TimeoutError

    def _check_timeout():
        if not future.done():
            future.set_exception(exc_class())

    if timeout and timeout > 0 and not future.done():
        if future._loop == get_event_loop():
            future._loop.call_later(timeout, _check_timeout)
        else:
            future._loop.call_soon_threadsafe(future._loop.call_later,
                                              timeout, _check_timeout)

    return future


def chain_future(future, callback=None, errback=None, next=None, timeout=None):
    '''Chain a :class:`~asyncio.Future` to an existing ``future``.

    This function `chain` the ``next`` future to an existing ``future``.
    When the input ``future`` receive a result the optional
    ``callback`` is executed and its result set as the results of ``next``.
    If an exception occurs the optional ``errback`` is executed.

    :param future: the original :class:`~asyncio.Future` (can be a coroutine)
    :param callback: optional callback to execute on the result of ``future``
    :param errback: optional callback to execute on the exception of ``future``
    :param next: optional :class:`~asyncio.Future` to chain.
        If not provided a new future is created
    :param timeout: optional timeout to set on ``next``
    :return: the future ``next``
    '''
    future = async(future)
    if next is None:
        next = Future(loop=future._loop)
        if timeout and timeout > 0:
            future_timeout(next, timeout)
    else:
        assert timeout is None, 'cannot set timeout'

    def _callback(fut):
        try:
            try:
                result = future.result()
            except Exception as exc:
                if errback:
                    result = errback(exc)
                    exc = None
                else:
                    raise
            else:
                if callback:
                    result = callback(result)
        except Exception as exc:
            next.set_exception(exc)
        else:
            if isfuture(result):
                chain_future(result, next=next)
            else:
                next.set_result(result)

    future.add_done_callback(_callback)
    return next


def as_exception(fut):
    if fut._exception:
        return fut.exception()
    elif getattr(fut, '_state', None) == _CANCELLED:
        return CancelledError()


def add_errback(future, callback, loop=None):
    '''Add a ``callback`` to a ``future`` executed only if an exception
    or cancellation has occurred.'''
    def _error_back(fut):
        if fut._exception:
            callback(fut.exception())
        elif fut.cancelled():
            callback(CancelledError())

    future = async(future, loop=None)
    future.add_done_callback(_error_back)
    return future


def add_callback(future, callback, loop=None):
    '''Add a ``callback`` to ``future`` executed only if an exception
    has not occurred.'''
    def _call_back(fut):
        if not (fut._exception or fut.cancelled()):
            callback(fut.result())

    future = async(future, loop=None)
    future.add_done_callback(_call_back)
    return future


def future_result_exc(future):
    '''Return a two elements tuple containing the future result and exception.

    The :class:`.Future` must be ``done``
    '''
    if future._state == _CANCELLED:
        return None, CancelledError()
    elif future._exception:
        return None, future.exception()
    else:
        return future.result(), None


def task_callback(callback):

    @wraps(callback)
    def _task_callback(fut):
        return async(callback(fut.result()), fut._loop)

    return _task_callback


def maybe_async(value, loop=None):
    '''Handle a possible asynchronous ``value``.

    Return an :ref:`asynchronous instance <tutorials-coroutine>`
    only if ``value`` is a generator, a :class:`.Future`.

    :parameter value: the value to convert to an asynchronous instance
        if it needs to.
    :parameter loop: optional :class:`.EventLoop`.
    :return: a :class:`.Future` or a synchronous ``value``.
    '''
    try:
        return async(value, loop=loop)
    except TypeError:
        return value


def task(function):
    '''Thread-safe decorator to run a ``function`` in an event loop.

    :param function: a callable which can return coroutines,
        :class:`.asyncio.Future` or synchronous data. Can be a method of
        an :ref:`async object <async-object>`, in which case the loop
        is given by the object ``_loop`` attribute.
    :return: a :class:`~asyncio.Future`

    The coroutine is wrapped with the :func:`yield_from` function.
    '''
    if isgeneratorfunction(function):
        wrapper = function
    else:
        def wrapper(*args, **kw):
            res = yield function(*args, **kw)
            raise Return(res)

    @wraps(function)
    def _(*args, **kwargs):
        event_loop = get_event_loop()
        coro = wrapper(*args, **kwargs)
        if args:
            loop = getattr(args[0], '_loop', event_loop)
            # Not in the current event loop
            if loop != event_loop and isinstance(loop, _EVENT_LOOP_CLASSES):
                future = run_in_loop(loop, yield_from, coro, loop=loop)
                if (not getattr(loop, '_iothreadloop', True)
                        and not loop.is_running()):
                    return loop.run_until_complete(future)
                else:
                    return future
        return async(yield_from(coro))

    return _


def run_in_loop(_loop, callable, *args, **kwargs):
    '''Run ``callable`` in the event ``loop`` thread, thread safe.

    :param _loop: The event loop where ``callable`` is run
    :return: a :class:`~asyncio.Future`
    '''
    waiter = Future(loop=_loop)

    def _():
        try:
            result = callable(*args, **kwargs)
        except Exception as exc:
            waiter.set_exception(exc)
        else:
            try:
                future = async(result, loop=_loop)
            except TypeError:
                waiter.set_result(result)
            else:
                chain_future(future, next=waiter)

    _loop.call_soon_threadsafe(_)
    return waiter


def async_while(timeout, while_clause, *args):
    '''The asynchronous equivalent of ``while while_clause(*args):``

    Use this function within a :ref:`coroutine <coroutine>` when you need
    to wait ``while_clause`` to be satisfied.

    :parameter timeout: a timeout in seconds after which this function stop.
    :parameter while_clause: while clause callable.
    :parameter args: optional arguments to pass to the ``while_clause``
        callable.
    :return: A :class:`.Future`.
    '''
    loop = get_event_loop()

    def _():
        start = loop.time()
        di = 0.1
        interval = 0
        result = while_clause(*args)
        while result:
            interval = min(interval+di, MAX_ASYNC_WHILE)
            try:
                yield From(sleep(interval, loop=loop))
            except TimeoutError:
                pass
            if timeout and loop.time() - start >= timeout:
                break
            result = while_clause(*args)
        coroutine_return(result)

    return async(_(), loop)


# ############################################################## Bench
class Bench:
    '''Execute a given number of asynchronous requests and wait for results.
    '''
    start = None
    '''The :meth:`~asyncio.BaseEventLoop.time` when the execution starts'''
    finish = None
    '''The :meth:`~asyncio.BaseEventLoop.time` when the execution finishes'''
    result = ()
    '''Tuple of results'''

    def __init__(self, times, loop=None):
        self._loop = loop or get_event_loop()
        self.times = times

    @property
    def taken(self):
        '''The total time taken for execution
        '''
        if self.finish:
            return self.finish - self.start

    def __call__(self, func, *args, **kwargs):
        self.start = self._loop.time()
        data = (func(*args, **kwargs) for t in range(self.times))
        self.result = multi_async(data, loop=self._loop)
        return chain_future(self.result, callback=self._done)

    def _done(self, result):
        self.finish = self._loop.time()
        self.result = tuple(result)
        return self


# ############################################################## AsyncObject
class AsyncObject(object):
    '''Interface for :ref:`async objects <async-object>`

    .. attribute:: _loop

        The :ref:`event loop <asyncio-event-loop>` associated with this object

    .. attribute:: _logger

        Optional logger instance, used by the :attr:`logger` attribute
    '''
    _logger = None
    _loop = None

    @property
    def logger(self):
        '''The logger for this object.

        It is either the :attr:`_logger` or the logger of the :attr:`_loop`
        '''
        return self._logger or getattr(self._loop, 'logger', LOGGER)

    def timeit(self, method, times, *args, **kwargs):
        '''Useful utility for benchmarking an asynchronous ``method``.

        :param method: the name of the ``method`` to execute
        :param times: number of times to execute the ``method``
        :param args: positional arguments to pass to the ``method``
        :param kwargs: key-valued arguments to pass to the ``method``
        :return: a :class:`~asyncio.Future` which results in a :class:`Bench`
            object if successful

        The usage is simple::

            >>> b = self.timeit('asyncmethod', 100)
        '''
        bench = Bench(times, loop=self._loop)
        return bench(getattr(self, method), *args, **kwargs)


# ############################################################## Task
class Task(trollius.Task):

    def _wakeup(self, future, inthread=False):
        if inthread or future._loop is self._loop:
            try:
                value = future.result()
            except Exception as exc:
                # This may also be a cancellation.
                self._step(None, exc)
            else:
                self._step(value, None)
        else:
            self._loop.call_soon_threadsafe(self._wakeup, future, True)
        self = None


# ############################################################## MultiFuture
class MultiFuture(Future):
    '''Handle several futures at once. Thread safe.
    '''
    def __init__(self, data=None, loop=None, type=None, raise_on_error=True):
        super(MultiFuture, self).__init__(loop=loop)
        self._futures = {}
        self._failures = []
        self._raise_on_error = raise_on_error
        if data is not None:
            type = type or data.__class__
            if issubclass(type, Mapping):
                data = iteritems(data)
            else:
                type = list
                data = enumerate(data)
        else:
            type = list
            data = ()
        self._stream = type()
        for key, value in data:
            value = self._get_set_item(key, maybe_async(value, loop))
            if isfuture(value):
                self._futures[key] = value
                value.add_done_callback(partial(self._future_done, key))
            elif self.done():
                break
        self._check()

    @property
    def failures(self):
        return self._failures

    #    INTERNALS
    def _check(self):
        if not self._futures and not self.done():
            self.set_result(self._stream)

    def _future_done(self, key, future, inthread=False):
        # called by future when future is done
        # thread safe
        if inthread or future._loop is self._loop:
            self._futures.pop(key, None)
            if self._state == _PENDING:
                self._get_set_item(key, future)
                self._check()
        else:
            self._loop.call_soon_threadsafe(
                self._future_done, key, future, True)

    def _get_set_item(self, key, value):
        if isfuture(value):
            if value.done():
                exc = as_exception(value)
                if exc:
                    if self._raise_on_error:
                        self._futures.clear()
                        self.set_exception(exc)
                        return
                    else:
                        self._failures.append(exc)
                        value = exc
                else:
                    value = value._result
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value
        return value


# Backward compatibility
multi_async = MultiFuture
