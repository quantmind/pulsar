import sys
import traceback
import types
import asyncio
from collections import deque, namedtuple, Mapping
from inspect import isgeneratorfunction
from functools import wraps, partial

from pulsar.utils.pep import iteritems, default_timer

from .consts import MAX_ASYNC_WHILE
from .access import (get_request_loop, get_event_loop, logger,
                     _PENDING, _CANCELLED, _FINISHED)


CancelledError = asyncio.CancelledError
TimeoutError = asyncio.TimeoutError
InvalidStateError = asyncio.InvalidStateError
Future = asyncio.Future
GeneratorType = types.GeneratorType
ASYNC_OBJECTS = (Future, GeneratorType)

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
           'chain_future',
           'ASYNC_OBJECTS',
           'future_result_exc']


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
                logger().error('Return(%r) used without raise', self.value)


def coroutine_return(*value):
    raise Return(*value)


def add_errback(future, callback):
    '''Add a ``callback`` to a ``future`` executed only if an exception
    or cancellation has occurred.'''
    def _error_back(fut):
        if fut._exception:
            callback(fut.exception())
        elif fut.cancelled():
            callback(CancelledError())

    future.add_done_callback(_error_back)


def add_callback(future, callback):
    '''Add a ``callback`` to ``future`` executed only if an exception
    has not occurred.'''
    def _call_back(fut):
        if not (fut._exception or fut.cancelled()):
            callback(fut.result())

    future.add_done_callback(_call_back)


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


def chain_future(future, callback=None, next=None, timeout=None):
    if next is None:
        next = Future(loop=future._loop)
        if timeout and timeout > 0:
            future_timeout(next, timeout)
    else:
        assert timeout is None, 'cannot set timeout'

    def _callback(fut):
        try:
            result = fut.result()
            if callback:
                result = callback(result)
        except Exception as exc:
            next.set_exception(exc)
        else:
            if isinstance(result, Future):
                chain(result, next=next)
            else:
                next.set_result(result)

    future.add_done_callback(_callback)
    return next


def as_exception(fut):
    if fut._state == _CANCELLED:
        return CancelledError()
    elif fut._exception:
        return fut.exception()


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


def _coroutine(coro):
    '''wrap a coroutine so that not asynchronous value are
    not yield
    '''
    value = None
    while True:
        if value is None:
            result = next(coro)
        else:
            result = coro.send(value)
        try:
            value = async(result, task=False)
        except FutureTypeError:
            value = result
        else:
            value = yield result


def async(coro_or_future, loop=None, task=True):
    '''Handle an asynchronous ``coro_or_future``.

    Equivalent to the ``asyncio.async`` function but returns a
    :class:`.Future`. Raises :class:`.FutureTypeError` if ``value``
    is not a generator nor a :class:`.Future`.

    This function can be overwritten by the :func:`set_async` function.

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
        if task:
            loop = loop or get_request_loop()
            task_factory = getattr(loop, 'task_factory', Task)
            return task_factory(_coroutine(coro_or_future), loop=loop)
        else:
            return coro_or_future
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
        coro = _coroutine(method(self, *args, **kwargs))
        future = task_factory(coro, loop=loop)
        if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
            return loop.run_until_complete(future)
        else:
            return future

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


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


class Task(asyncio.Task):
    '''A :class:`.Future` which consumes a :ref:`coroutine <coroutine>`.

    The callback will occur once the coroutine has finished
    (when it raises StopIteration), or an unhandled exception occurs.
    '''
    _current_tasks = {}

    def _step(self, value=None, exc=None):
        __skip_traceback__ = True
        assert not self.done(), \
            '_step(): already done: {!r}, {!r}, {!r}'.format(self, value, exc)
        if self._must_cancel:
            if not isinstance(exc, CancelledError):
                exc = CancelledError()
            self._must_cancel = False
        coro = self._coro
        self._fut_waiter = None
        self.__class__._current_tasks[self._loop] = self
        #
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
            elif result == None:
                # transfer control to the event loop
                self._loop.call_soon(self._step)
            else:
                # Yielding something else is an error.
                self._loop.call_soon(
                    self._step, None,
                    RuntimeError(
                        'Task got bad yield: {!r}'.format(result)))
        finally:
            self.__class__._current_tasks.pop(self._loop)
        self = None

    def _wakeup(self, fut, inthread=False):
        if inthread or fut._loop is self._loop:
            super(Task, self)._wakeup(fut)
        else:
            self._loop.call_soon(self._wakeup, fut, True)


def multi_async(iterable=None, loop=None, lock=True, **kwargs):
    '''This is an utility function to convert an ``iterable`` into a
    :class:`MultiFuture` element.'''
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


############################################################### MultiFuture
class MultiFuture(Future):
    '''A :class:`.Future` for a ``collection`` of asynchronous objects.

    The ``collection`` can be either a ``list`` or a ``dict``.
    '''
    def __init__(self, loop, data, type=None, raise_on_error=True, **kwargs):
        super(MultiFuture, self).__init__(loop=loop)
        self._deferred = {}
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
            if self._state == _PENDING:
                value = self._get_set_item(key, maybe_async(value, self._loop))
                if isinstance(value, Future):
                    self._deferred[key] = value
                    value.add_done_callback(partial(self._future_done, key))
        self._check()

    @property
    def failures(self):
        return self._failures

    ###    INTERNALS
    def _check(self):
        if not self._deferred and self._state == _PENDING:
            self.set_result(self._stream)

    def _future_done(self, key, future):
        self._deferred.pop(key, None)
        self._get_set_item(key, future)
        self._check()

    def _get_set_item(self, key, value):
        if isinstance(value, Future):
            if value._state != _PENDING:
                exc = as_exception(value)
                if exc:
                    if self._raise_on_error:
                        self._deferred.clear()
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
