import sys
import traceback
import types
from collections import deque, namedtuple, Mapping
from inspect import isgeneratorfunction
from functools import wraps, partial

from pulsar.utils.pep import iteritems, default_timer, range

from .consts import MAX_ASYNC_WHILE
from .access import (asyncio, get_request_loop, get_event_loop, logger, LOGGER,
                     _PENDING, _CANCELLED, _FINISHED)


CancelledError = asyncio.CancelledError
TimeoutError = asyncio.TimeoutError
InvalidStateError = asyncio.InvalidStateError
Future = asyncio.Future
GeneratorType = types.GeneratorType

__all__ = ['Future',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'FutureTypeError',
           'Return',
           'coroutine_return',
           'add_async_binding',
           'maybe_async',
           'run_in_loop',
           'async',
           'add_errback',
           'add_callback',
           'future_timeout',
           'task_callback',
           'multi_async',
           'async_while',
           'in_loop',
           'task',
           'wait_complete',
           'chain_future',
           'future_result_exc',
           'AsyncObject']


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


if hasattr(asyncio, 'Return'):
    Return = asyncio.Return

else:

    class Return(StopIteration):

        def __init__(self, *value):
            StopIteration.__init__(self)
            if not value:
                self.value = None
            elif len(value) == 1:
                self.value = value[0]
            else:
                self.value = value
            self.raised = False

        def __del__(self):
            if not self.raised:
                asyncio.log.logger.error(
                    'Return(%r) used without raise', self.value)


def coroutine_return(*value):
    error = Return(*value)
    error.raised = True
    raise error


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


def future_timeout(future, timeout, exc_class=None):
    '''Add a ``timeout`` to ``future``.

    :return: the future
    '''
    exc_class = exc_class or TimeoutError

    def _check_timeout():
        if not future.done():
            future.set_exception(exc_class())

    future._loop.call_later(timeout, _check_timeout)

    return future


def chain_future(future, callback=None, errback=None, next=None, timeout=None):
    '''Chain a :class:`asyncio.Future` to an existing ``future``.

    This function `chain` the ``next`` future to an existing ``future``.
    When the input ``future`` receive a result the optional
    ``callback`` is executed and its result set as the results of ``next``.
    If an exception occurs the optional ``errback`` is executed.

    :param future: the original :class:`asyncio.Future` (can be a coroutine)
    :param callback: optional callback to execute on the result of ``future``
    :param errback: optional callback to execute on the exception of ``future``
    :param next: optional :class:`asyncio.Future` to chain.
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
                exc = future.exception()
            except CancelledError as e:
                exc = e
            if exc:
                if errback:
                    exc = None
                    result = errback(result)
            else:
                result = future.result()
                if callback:
                    result = callback(result)
        except Exception as exc:
            next.set_exception(exc)
        else:
            if exc:
                next.set_exception(exc)
            elif isinstance(result, Future):
                chain(result, next=next)
            else:
                next.set_result(result)

    future.add_done_callback(_callback)
    return next


def as_exception(fut):
    if fut._exception:
        return fut.exception()
    elif getattr(fut, '_state', None) == _CANCELLED:
        return CancelledError()


def future_result_exc(future):
    '''Return a two elements tuple containing the future result and exception.

    The :class:`.Future` must be ``done``
    '''
    if future._state == _CANCELLED:
        return None, CancelledError()
    elif future._exception:
        return None, fut.exception()
    else:
        return future.result(), None


def task_callback(callback):

    @wraps(callback)
    def _task_callback(fut):
        return async(callback(fut.result()), fut._loop)

    return _task_callback


def async(coro_or_future, loop=None):
    '''Handle an asynchronous ``coro_or_future``.

    Equivalent to the ``asyncio.async`` function but returns a
    :class:`.Future`. Raises :class:`.FutureTypeError` if ``value``
    is not a generator nor a :class:`.Future`.

    :parameter coro_or_future: the value to convert to a :class:`.Future`.
    :parameter loop: optional :class:`.EventLoop`.
    :return: a :class:`.Future`.
    '''
    if _bindings:
        for binding in _bindings:
            d = binding(coro_or_future, loop)
            if d is not None:
                return d
    if isinstance(coro_or_future, Future):
        return coro_or_future
    elif isinstance(coro_or_future, GeneratorType):
        loop = loop or get_request_loop()
        task_factory = getattr(loop, 'task_factory', Task)
        return task_factory(coro_or_future, loop=loop)
    else:
        raise FutureTypeError


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
        return async(value, loop)
    except FutureTypeError:
        return value


def task(method):
    '''Decorator to run a ``method`` returning a coroutine in the event loop
    of the instance of the bound ``method``.

    The instance must be an :ref:`async object <async-object>`.
    '''
    assert isgeneratorfunction(method)

    @wraps(method)
    def _(self, *args, **kwargs):
        loop = self._loop
        task_factory = getattr(loop, 'task_factory', Task)
        coro = method(self, *args, **kwargs)
        future = task_factory(coro, loop=loop)
        if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
            return loop.run_until_complete(future)
        else:
            return future

    return _


def wait_complete(method):
    '''Decorator to wait for a ``method`` to complete.

    It only affects asynchronous object with a local event loop.
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        loop = self._loop
        result = method(self, *args, **kwargs)
        if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
            return loop.run_until_complete(async(result, loop=loop))
        else:
            return result

    return _


def run_in_loop(loop, callback, *args, **kwargs):
    '''Run ``callable`` in the event ``loop`` thread.

    Return a :class:`.Future`
    '''
    def _():
        result = callback(*args, **kwargs)
        try:
            future = async(result, loop=loop)
        except FutureTypeError:
            coroutine_return(result)
        else:
            result = yield future
            coroutine_return(result)

    return getattr(loop, 'task_factory', Task)(_(), loop=loop)


def in_loop(method):
    '''Decorator to run a method in the event loop of the instance of
    the bound ``method``.

    The instance must be an :ref:`async object <async-object>`.
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        return run_in_loop(self._loop, method, self, *args, **kwargs)

    return _


def multi_async(iterable=None, loop=None, **kwargs):
    '''Utility to convert an ``iterable`` over possible asynchronous
    components into a :class:`~asyncio.Future` which results in an iterable
    of results.

    The ``iterable`` can be:

    * a ``list``, ``tuple`` or a ``generator``: in this case
      the returned future will result in a ``list``
    * a :class:`~collections.abc.Mapping` instance: in this case
      the returned future will result in a ``dict``
    '''
    return MultiFuture(loop, iterable, **kwargs)


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
                yield asyncio.sleep(interval, loop)
            except TimeoutError:
                pass
            if timeout and loop.time() - start >= timeout:
                break
            result = while_clause(*args)
        coroutine_return(result)

    return async(_(), loop)


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
        self.result = MultiFuture(
            self._loop, (func(*args, **kwargs) for t in range(self.times)))
        return chain_future(self.result, callback=self._done)

    def _done(self, result):
        self.finish = self._loop.time()
        self.result = tuple(result)
        return self


class AsyncObject(object):
    '''Interface for :ref:`async objects <async-object>`

    .. attribute:: _loop

        The event loop associated with this object
    '''
    _logger = None
    _loop = None

    @property
    def logger(self):
        '''The logger for this object
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


class Task(asyncio.Task):
    '''A modified ``asyncio`` :class:`.Task`.

    It has the following features:

    * handles both ``yield`` and ``yield from``
    * tolerant of synchronous values
    '''
    _current_tasks = {}

    def _step(self, value=None, exc=None):
        assert not self.done(), \
            '_step(): already done: {!r}, {!r}, {!r}'.format(self, value, exc)
        if self._must_cancel:
            if not isinstance(exc, CancelledError):
                exc = CancelledError()
            self._must_cancel = False
        coro = self._coro
        self._fut_waiter = None
        sync = True
        self.__class__._current_tasks[self._loop] = self
        #
        try:
            while sync:
                sync = False
                try:
                    if exc:
                        result = coro.throw(exc)
                    elif value is not None:
                        result = coro.send(value)
                    else:
                        result = next(coro)
                    # handle possibly asynchronous results
                    try:
                        result = async(result, self._loop)
                    except FutureTypeError:
                        pass
                except Return as exc:
                    exc.raised = True
                    self.set_result(exc.value)
                except StopIteration as e:
                    self.set_result(getattr(e, 'value', None))
                except Exception as exc:
                    self.set_exception(exc)
                except BaseException as exc:
                    self.set_exception(exc)
                    raise
                else:
                    if isinstance(result, Future):
                        result._blocking = False
                        result.add_done_callback(self._wakeup)
                        self._fut_waiter = result
                        if self._must_cancel:
                            if self._fut_waiter.cancel():
                                self._must_cancel = False
                    elif result is None:
                        # transfer control to the event loop
                        self._loop.call_soon(self._step)
                    else:
                        # Go again
                        value, exc, sync = result, None, True
        finally:
            self.__class__._current_tasks.pop(self._loop)
        self = None

    def _wakeup(self, future, inthread=False):
        if inthread or future._loop is self._loop:
            try:
                exc = future.exception()
            except CancelledError as e:
                exc = e
            if exc:
                self._step(exc=exc)
            else:
                self._step(future.result())
        else:
            self._loop.call_soon_threadsafe(self._wakeup, future, True)
        self = None


# ############################################################## MultiFuture
class MultiFuture(Future):

    def __init__(self, loop, data, type=None, raise_on_error=True):
        super(MultiFuture, self).__init__(loop=loop)
        self._futures = {}
        self._failures = []
        self._raise_on_error = raise_on_error
        if not type:
            type = data.__class__ if data is not None else list
        if issubclass(type, Mapping):
            data = iteritems(data)
        else:
            type = list
            data = enumerate(data)
        self._stream = type()
        for key, value in data:
            value = self._get_set_item(key, maybe_async(value, self._loop))
            if isinstance(value, Future):
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
        if inthread or future._loop is self._loop:
            self._futures.pop(key, None)
            if self._state == _PENDING:
                self._get_set_item(key, future)
                self._check()
        else:
            self._loop.call_soon(self._future_done, key, future, True)

    def _get_set_item(self, key, value):
        if isinstance(value, Future):
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


def add_async_binding(callable):
    global _bindings
    _bindings.append(callable)


_bindings = []
