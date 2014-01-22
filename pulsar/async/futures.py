import sys
import traceback
from collections import deque, namedtuple, Mapping
from inspect import isgenerator, istraceback
from functools import wraps

from pulsar.utils.pep import iteritems, default_timer

from .consts import MAX_ASYNC_WHILE
from .access import (get_request_loop, get_event_loop, logger, asyncio,
                     _PENDING, _CANCELLED, _FINISHED)
from .fallbacks.coro import CoroutineReturn, coroutine_return


NOT_DONE = object()
CancelledError = asyncio.CancelledError
TimeoutError = asyncio.TimeoutError
InvalidStateError = asyncio.InvalidStateError
Future = asyncio.Future


__all__ = ['Future',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'FutureTypeError',
           'coroutine_return',
           'add_async_binding',
           'maybe_async',
           'async',
           'add_errback',
           'task_callback',
           'multi_async',
           'async_while',
           'run_in_loop_thread',
           'in_loop',
           'in_loop_thread',
           'raise_error_and_log',
           'NOT_DONE']


def add_errback(future, errback):

    def _error_back(fut):
        if fut._exception:
            errback(fut.exception())
        elif fut.cancelled():
            errback(CancelledError())

    future.add_done_callback(_error_back)


def task_callback(callback):

    @wraps(callback)
    def _task_callback(fut):
        return async(callback(fut.result()), fut._loop)

    return _task_callback


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


class CoroTask(Future):
    '''A :class:`Deferred` which consumes a :ref:`coroutine <coroutine>`.

    The callback will occur once the coroutine has finished
    (when it raises StopIteration), or an unhandled exception occurs.
    Instances of :class:`CoroTask` are never
    initialised directly, they are created by the :func:`.async` or
    :func:`.maybe_async` functions when a generator is passed as argument.
    '''
    _waiting = None

    def __init__(self, gen, loop):
        self._gen = gen
        self._loop = loop
        self._callbacks = []
        self._step(None, None)

    def _step(self, result, error):
        __skip_traceback__ = True
        gen = self._gen
        self._waiting = None
        try:
            if error:
                result = gen.throw(error)
            else:
                result = gen.send(result)
            # handle possibly asynchronous results
            result = maybe_async(result, self._loop)
        except StopIteration as e:
            self.set_result(getattr(e, 'value', None))
        except Exception as exc:
            self.set_exception(exc)
        except BaseException as exc:
            self.set_exception(exc)
            raise
        else:
            if isinstance(result, Future):
                # async result add callback/errorback and transfer control
                # to the event loop
                result.add_done_callback(self._restart)
                self._waiting = result
                return
            elif result == NOT_DONE:
                # transfer control to the event loop
                self._loop.call_soon(self._consume, None)
                return
            self._step(result, None)

    def _restart(self, future):
        try:
            value = future.result()
        except Exception as exc:
            self._step(None, exc)
        else:
            self._step(value, None)
        self = None

    def cancel(self, msg='', mute=False, exception_class=None):
        if self._waiting:
            self._waiting.cancel(msg, mute, exception_class)
        else:
            super(DeferredTask, self).cancel(msg, mute, exception_class)


######################################################### Async Bindings
class AsyncBindings:

    def __init__(self):
        self._bindings = []

    def add(self, callable):
        self._bindings.append(callable)

    def __call__(self, coro_or_future, loop=None):
        '''Handle an asynchronous ``coro_or_future``.

        Equivalent to the ``asyncio.async`` function but returns a
        :class:`.Deferred`. Raises :class:`.FutureTypeError` if ``value``
        is not a generator nor a :class:`.Future`.

        This function can be overwritten by the :func:`set_async` function.

        :parameter value: the value to convert to a :class:`.Deferred`.
        :parameter loop: optional :class:`.EventLoop`.
        :return: a :class:`Deferred`.
        '''
        if self._bindings:
            for binding in self._bindings:
                d = binding(coro_or_future, loop)
                if d is not None:
                    return d
        if isinstance(coro_or_future, Future):
            return coro_or_future
        elif isgenerator(coro_or_future):
            loop = loop or get_request_loop()
            task_factory = getattr(loop, 'task_factory', CoroTask)
            return task_factory(coro_or_future, loop)
        else:
            raise FutureTypeError

    def maybe(self, value, loop=None, get_result=True):
        '''Handle a possible asynchronous ``value``.

        Return an :ref:`asynchronous instance <tutorials-coroutine>`
        only if ``value`` is a generator, a :class:`.Deferred` or
        ``get_result`` is set to ``False``.

        :parameter value: the value to convert to an asynchronous instance
            if it needs to.
        :parameter loop: optional :class:`.EventLoop`.
        :parameter get_result: optional flag indicating if to get the result
            in case the return value is a :class:`.Deferred` already done.
            Default: ``True``.
        :return: a :class:`.Deferred` or  a :class:`.Failure` or a synchronous
            value.
        '''
        try:
            return self(value, loop)
        except FutureTypeError:
            return value


async = AsyncBindings()
add_async_binding = async.add
maybe_async = async.maybe


def iterdata(stream, start=0):
    '''Iterate over a stream which is either a dictionary or a list.

    This iterator is over key-value pairs for a dictionary, and
    index-value pairs for a list.'''
    if isinstance(stream, Mapping):
        return iteritems(stream)
    else:
        return enumerate(stream, start)


def multi_async(iterable=None, loop=None, lock=True, **kwargs):
    '''This is an utility function to convert an ``iterable`` into a
    :class:`MultiFuture` element.'''
    m = MultiFuture.make(loop, iterable, **kwargs)
    return m.lock() if lock else m


def raise_error_and_log(error, level=None):
    try:
        raise error
    except Exception as e:
        Failure(sys.exc_info()).log(msg=str(e), level=level)
        raise


def async_while(timeout, while_clause, *args):
    '''The asynchronous equivalent of ``while while_clause(*args):``

    Use this function within a :ref:`coroutine <coroutine>` when you need
    to wait ``while_clause`` to be satisfied.

    :parameter timeout: a timeout in seconds after which this function stop.
    :parameter while_clause: while clause callable.
    :parameter args: optional arguments to pass to the ``while_clause``
        callable.
    :return: A :class:`.Deferred`.
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
                yield Deferred(loop).set_timeout(interval)
            except TimeoutError:
                pass
            if timeout and loop.time() - start >= timeout:
                break
            result = while_clause(*args)
        yield result

    return async(_(), loop)


def run_in_loop_thread(loop, callback, *args, **kwargs):
    '''Run ``callable`` in the event ``loop`` thread.

    Return a :class:`.Deferred`
    '''
    d = Future(loop)

    def _():
        try:
            result = yield callback(*args, **kwargs)
        except Exception as exc:
            d.set_exception(exc)
        else:
            d.set_result(result)

    loop.call_soon_threadsafe(_)
    if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
        return loop.run_until_complete(d)
    return d


def in_loop(method):
    '''Decorator to run a method in the event loop of the instance of
    the bound ``method``.

    The instance must be an :ref:`async object <async-object>`.
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        result = method(self, *args, **kwargs)
        return maybe_async(result, self._loop)

    return _


def in_loop_thread(method):
    '''Decorator to run a method in the thread of the event loop
    of the instance of the bound ``method``.

    This decorator turns ``method`` into a thread-safe asynchronous
    executor.

    The instance must be an :ref:`async object <async-object>`.

    It uses the :func:`run_in_loop_thread` function.
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        return run_in_loop_thread(self._loop, method, self, *args, **kwargs)

    return _


############################################################### MultiFuture
class MultiFuture(Future):
    '''A :class:1Deferred` for a ``collection`` of asynchronous objects.

    The ``collection`` can be either a ``list`` or a ``dict``.
    '''
    _locked = False
    _time_locked = None
    _time_finished = None

    @classmethod
    def make(cls, loop, data, type=None, raise_on_error=True,
             mute_failures=False, **kwargs):
        self = cls(loop)
        self._deferred = {}
        self._failures = []
        self._mute_failures = mute_failures
        self._raise_on_error = raise_on_error
        if not type:
            type = data.__class__ if data is not None else list
        if not issubclass(type, (list, Mapping)):
            type = list
        self._stream = type()
        self._time_start = default_timer()
        if data:
            self.update(data)
        return self

    @property
    def raise_on_error(self):
        '''When ``True`` and at least one value of the result collections is a
        :class:`.Failure`, the callback will receive the failure rather than
        the collection of results.

        Default ``True``.
        '''
        return self._raise_on_error

    @property
    def locked(self):
        '''When ``True``, the :meth:`update` or :meth:`append` methods can no
        longer be used.'''
        return self._locked

    @property
    def type(self):
        '''The type of multi-deferred. Either a ``list`` or a ``Mapping``.'''
        return self._stream.__class__.__name__

    @property
    def total_time(self):
        '''Total number of seconds taken to obtain the result.'''
        if self._time_finished:
            return self._time_finished - self._time_start

    @property
    def locked_time(self):
        '''Number of econds taken to obtain the result once :class:`locked`.'''
        if self._time_finished:
            return self._time_finished - self._time_locked

    @property
    def num_failures(self):
        return len(self._failures)

    @property
    def failures(self):
        return self._failures

    def lock(self):
        '''Lock the :class:`.MultiFuture` so that no new items can be added.

        If it was alread :attr:`locked` a runtime exception occurs.'''
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +
                               ' cannot be locked twice.')
        self._time_locked = default_timer()
        self._locked = True
        if not self._deferred:
            self._finish()
        return self

    def update(self, stream):
        '''Update the :class:`MultiFuture` with new data.

        It works for both ``list`` and ``dict`` :attr:`type`.
        '''
        add = self._add
        for key, value in iterdata(stream, len(self._stream)):
            add(key, value)
        return self

    def append(self, value):
        '''Append a new ``value`` to this asynchronous container.

        Works for a list :attr:`type` only.'''
        if self.type == 'list':
            self._add(len(self._stream), value)
        else:
            raise RuntimeError('Cannot append a value to a "dict" type '
                               'multideferred')

    ###    INTERNALS

    def _add(self, key, value):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +
                               ' cannot add a dependent once locked.')
        value = self._get_set_item(key, maybe_async(value, self._loop))
        # add callback if an asynchronous value
        if isinstance(value, Future):
            self._deferred[key] = value
            value.add_done_callback(lambda f: self._future_done(key, f))

    def _future_done(self, key, future):
        self._deferred.pop(key, None)
        self._get_set_item(key, future)
        if self._locked and not self._deferred and not self.done():
            self._finish()
        return result

    def _finish(self):
        if not self._locked:
            raise RuntimeError(self.__class__.__name__ +
                               ' cannot finish until completed.')
        if self._deferred:
            raise RuntimeError(self.__class__.__name__ +
                               ' cannot finish whilst waiting for '
                               'dependents %r' % self._deferred)
        self._time_finished = default_timer()
        if self.raise_on_error and self._failures:
            self.set_exception(self._failures[0])
        else:
            self.set_result(self._stream)

    def _get_set_item(self, key, value):
        if isinstance(value, Future):
            if value._state != _PENDING:
                if value._exception:
                    self._failures.append(value.exception())
                    value = value._exception
                else:
                    value = value._result
            else:
                return value
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value
        return value
