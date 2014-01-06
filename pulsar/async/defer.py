import sys
from collections import Mapping

from pulsar.utils.pep import iteritems, default_timer

from .access import get_event_loop, get_request_loop
from .consts import MAX_ASYNC_WHILE

if False:   # pragma nocover
    from pulsar.utils.lib import (Deferred, DeferredTask, Failure, async,
                                  maybe_async, maybe_failure, NOT_DONE,
                                  add_async_binding, set_access,
                                  CoroutineReturn, Error,
                                  CancelledError, TimeoutError,
                                  InvalidStateError, FutureTypeError)

    set_access(get_event_loop, get_request_loop)
else:
    from .fallbacks.defer import (Deferred, DeferredTask, Failure, async,
                                  maybe_async, maybe_failure, NOT_DONE,
                                  add_async_binding,
                                  CoroutineReturn, Error,
                                  CancelledError, TimeoutError,
                                  InvalidStateError, FutureTypeError)

__all__ = ['Deferred',
           'Error',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'FutureTypeError',
           'log_failure',
           'Failure',
           'maybe_failure',
           'coroutine_return',
           'is_failure',
           'add_async_binding',
           'maybe_async',
           'async',
           'multi_async',
           'async_sleep',
           'async_while',
           'safe_async',
           'run_in_loop_thread',
           'in_loop',
           'in_loop_thread',
           'raise_error_and_log',
           'NOT_DONE']


def coroutine_return(value=None):
    '''Use this function to return ``value`` from a
    :ref:`coroutine <coroutine>`.

    For example::

        def mycoroutine():
            a = yield ...
            yield ...
            ...
            coroutine_return('OK')

    If a coroutine does not invoke this function, its result is ``None``.
    '''
    raise CoroutineReturn(value)


def iterdata(stream, start=0):
    '''Iterate over a stream which is either a dictionary or a list.

    This iterator is over key-value pairs for a dictionary, and
    index-value pairs for a list.'''
    if isinstance(stream, Mapping):
        return iteritems(stream)
    else:
        return enumerate(stream, start)


def multi_async(iterable=None, loop=None, lock=True, **kwargs):
    '''This is an utility function to convert an *iterable* into a
:class:`MultiDeferred` element.'''
    m = MultiDeferred.make(loop, iterable, **kwargs)
    return m.lock() if lock else m


def is_failure(obj, *classes):
    '''Check if ``obj`` is a :class:`.Failure`.

    If optional ``classes`` are given, it checks if the error is an instance
    of those classes.
    '''
    if isinstance(obj, Failure):
        return obj.isinstance(classes) if classes else True
    return False


def log_failure(value):
    '''Lag a :class:`.Failure` if ``value`` is one.

    Return ``value``.
    '''
    value = maybe_failure(value)
    if isinstance(value, Failure):
        value.log()
    return value


def raise_error_and_log(error, level=None):
    try:
        raise error
    except Exception as e:
        Failure(sys.exc_info()).log(msg=str(e), level=level)
        raise


def async_sleep(timeout, loop=None):
    '''The asynchronous equivalent of ``time.sleep(timeout)``. Use this
function within a :ref:`coroutine <coroutine>` when you need to resume
the coroutine after ``timeout`` seconds. For example::

    ...
    yield async_sleep(2)
    ...

This function returns a :class:`.Deferred` called back in ``timeout`` seconds
with the ``timeout`` value.
'''
    def _cancel(failure):
        if failure.isinstance(TimeoutError):
            failure.mute()
            return timeout
        else:
            return failure
    loop = loop or get_request_loop()
    return Deferred(loop).set_timeout(timeout, mute=True).add_errback(_cancel)


def safe_async(callable, *args, **kwargs):
    '''Safely execute a ``callable`` and always return a :class:`.Deferred`.

    Never throws.
    '''
    loop = kwargs.pop('loop', None)
    try:
        result = callable(*args, **kwargs)
    except Exception:
        result = sys.exc_info()
    return maybe_async(result, loop, False)


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
    d = Deferred(loop)

    def _():
        try:
            result = yield callback(*args, **kwargs)
        except Exception:
            d.callback(sys.exc_info())
            raise
        else:
            d.callback(result)

    loop.call_soon_threadsafe(_)
    if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
        return loop.run_until_complete(d)
    return d


def in_loop(method):
    '''Decorator to run a method in the event loop of the instance of
    the bound ``method``.

    The instance must be an :ref:`async object <async-object>`.
    '''
    def _(self, *args, **kwargs):
        try:
            result = method(self, *args, **kwargs)
        except Exception:
            result = sys.exc_info()
        return maybe_async(result, self._loop, False)

    _.__name__ = method.__name__
    _.__doc__ = method.__doc__
    return _


def in_loop_thread(method):
    '''Decorator to run a method in the thread of the event loop
    of the instance of the bound ``method``.

    This decorator turns ``method`` into a thread-safe asynchronous
    executor.

    The instance must be an :ref:`async object <async-object>`.

    It uses the :func:`run_in_loop_thread` function.
    '''
    def _(self, *args, **kwargs):
        return run_in_loop_thread(self._loop, method, self, *args, **kwargs)

    _.__name__ = method.__name__
    _.__doc__ = method.__doc__
    return _


############################################################### MultiDeferred
class MultiDeferred(Deferred):
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
        '''Lock the :class:`MultiDeferred` so that no new items can be added.

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
        '''Update the :class:`MultiDeferred` with new data. It works for
both ``list`` and ``dict`` :attr:`type`.'''
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
        value = maybe_async(value)
        self._setitem(key, value)
        # add callback if an asynchronous value
        if isinstance(value, Deferred):
            self._deferred[key] = value
            value.add_both(lambda result: self._deferred_done(key, result))

    def _deferred_done(self, key, result):
        self._deferred.pop(key, None)
        self._setitem(key, result)
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
            self.callback(self._failures[0])
        else:
            self.callback(self._stream)

    def _setitem(self, key, value):
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value
        if isinstance(value, Failure):
            if self._mute_failures:
                value.mute()
            self._failures.append(value)
