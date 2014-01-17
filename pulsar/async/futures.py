import sys
import traceback
from collections import deque, namedtuple, Mapping
from inspect import isgenerator, istraceback
from functools import wraps

from pulsar.utils.pep import iteritems, default_timer

from .consts import MAX_ASYNC_WHILE
from .access import (get_request_loop, get_event_loop, logger, asyncio,
                     _PENDING, _CANCELLED, _FINISHED)


NOT_DONE = object()
CancelledError = asyncio.CancelledError
TimeoutError = asyncio.TimeoutError
InvalidStateError = asyncio.InvalidStateError


__all__ = ['Future',
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


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


class CoroutineReturn(StopIteration):

    def __init__(self, value):
        self.value = value


async_exec_info = namedtuple('async_exec_info', 'error_class error trace')
log_exc_info = ('error', 'critical')


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


def is_relevant_tb(tb):
    return not ('__skip_traceback__' in tb.tb_frame.f_locals or
                '__unittest' in tb.tb_frame.f_globals)


def tb_length(tb):
    length = 0
    while tb and is_relevant_tb(tb):
        length += 1
        tb = tb.tb_next
    return length


def format_exception(exctype, value, tb):
    trace = getattr(value, '__async_traceback__', None)
    while tb and not is_relevant_tb(tb):
        tb = tb.tb_next
    length = tb_length(tb)
    if length or not trace:
        tb = traceback.format_exception(exctype, value, tb, length)
    if trace:
        if tb:
            tb = tb[:-1]
            tb.extend(trace[1:])
        else:
            tb = trace
    value.__async_traceback__ = tb
    value.__traceback__ = None
    return tb


def as_async_exec_info(exc_info):
    if not isinstance(exc_info, async_exec_info):
        exctype, value, tb = exc_info
        trace = format_exception(exctype, value, tb)
        exc_info = async_exec_info(exctype, value, trace)
    return exc_info


def is_exc_info(exc_info):
    if isinstance(exc_info, async_exec_info):
        return True
    elif isinstance(exc_info, tuple) and len(exc_info) == 3:
        return istraceback(exc_info[2])
    return False


def maybe_failure(value, noisy=False):
    '''Convert ``value`` into a :class:`Failure` if it needs to.

    The conversion happen only if ``value`` is a stack trace or an
    exception, otherwise returns ``value``.

    :param value: the value to convert to a :class:`Failure` instance
        if it needs to.
    :return: a :class:`Failure` or the original ``value``.
    '''
    __skip_traceback__ = True
    if isinstance(value, BaseException):
        exc_info = sys.exc_info()
        if value is not exc_info[1]:
            try:
                raise value
            except:
                exc_info = sys.exc_info()
        return Failure(exc_info)
    elif is_exc_info(value):
        value = Failure(value)
    if isinstance(value, Failure) and noisy:
        value.log()
    return value


############################################################### FAILURE
class Failure(object):
    '''The asynchronous equivalent of python Exception.

    It has several useful methods and features which facilitates logging,
    and throwing exceptions.

    .. attribute:: exc_info

        The exception as a three elements tuple
        (``errorType``, ``errvalue``, ``traceback``) occured during
        the execution of a :class:`Deferred`.

    .. attribute:: logged

        Check if the :attr:`error` was logged.

    '''
    __slots__ = ('_mute', 'exc_info')
    _msg = 'Pulsar Asynchronous Failure'

    def __init__(self, exc_info, noisy=False):
        self._mute = False
        self.exc_info = as_async_exec_info(exc_info)
        if noisy:
            self.log()

    def __del__(self):
        if not self._mute:
            self.log()

    def __repr__(self):
        return ''.join(self.exc_info[2])
    __str__ = __repr__

    @property
    def logged(self):
        return getattr(self.error, '_failure_logged', False)

    @property
    def error(self):
        '''The python :class:`Exception` instance.'''
        return self.exc_info[1]

    def isinstance(self, classes):
        '''Check if :attr:`error` is an instance of exception ``classes``.'''
        return isinstance(self.error, classes)

    def throw(self, gen=None):
        '''Raises the exception from the :attr:`exc_info`.

        :parameter gen: Optional generator. If provided the exception is throw
            into the generator via the ``gen.throw`` method.

        Without ``gen``, this method is used when interacting with libraries
        supporting both synchronous and asynchronous flow controls.
        '''
        __skip_traceback__ = True
        if gen:
            return gen.throw(self.exc_info[0], self.exc_info[1])
        else:
            # mute only this Failure, not the error
            self._mute = True
            raise self.exc_info[1]

    def log(self, log=None, msg=None, level=None):
        '''Log the :class:`Failure` and set :attr:`logged` to ``True``.

Logging for a given failure occurs once only. To suppress logging for
a given failure set :attr:`logged` to ``True`` of invoke the :meth:`mute`
method.
Returns ``self`` so that this method can easily be added as an error
back to perform logging and propagate the failure. For example::

    .add_errback(lambda failure: failure.log())
'''
        if not self.logged:
            self.mute()
            msg = msg or self._msg
            log = log or logger()
            level = level or 'error'
            handler = getattr(log, level)
            if level in log_exc_info:
                exc_info = self.exc_info
                if exc_info[2]:
                    c = '\n'
                    emsg = ''.join(exc_info[2])
                else:
                    c = ': '
                    emsg = str(exc_info[1])
                msg = '%s%s%s' % (msg, c, emsg) if msg else emsg
            handler(msg)
        return self

    def critical(self, msg):
        return self.log(msg=msg, level='critical')

    def mute(self):
        '''Mute logging and return ``self``.'''
        setattr(self.exc_info[1], '_failure_logged', True)
        return self


############################################################### Deferred
class Future(asyncio.Future):
    _callbacks = None

    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()

    def _schedule_callbacks(self):
        if not self._callbacks:
            return

        callbacks = self._callbacks[:]
        self._callbacks[:] = []
        for callback in callbacks:
            self._loop.call_soon(callback, self)

    def has_callbacks(self):
        '''The number of callbacks.
        '''
        return len(self._callbacks) if self._callbacks else 0

    def set_exception(self, exception):
        if self._state == _PENDING:
            raise InvalidStateError('{}: {!r}'.format(self._state, self))
        self._exception = exception
        self._state = _FINISHED
        self._schedule_callbacks()
        self._tb_logger = maybe_failure(exception)
        # Arrange for the logger to be activated after all callbacks
        # have had a chance to call result() or exception().
        self._loop.call_soon(self._tb_logger.activate)

    def __iter__(self):
        if not self.done():
            yield self  # This tells Task to wait for completion.
        coroutine_return(self.result())


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
        self._step(None)

    def _step(self, result):
        __skip_traceback__ = True
        gen = self._gen
        try:
            if isinstance(result, Failure):
                failure, result = result, None
                result = failure.throw(gen)
                failure.mute()
            else:
                result = gen.send(result)
            # handle possibly asynchronous results
            result = maybe_async(result, self._loop)
        except StopIteration as e:
            self.set_result(getattr(e, 'value', None))
        except Exception:
            self.set_exception(sys.exc_info())
        except BaseException:
            self.set_exception(sys.exc_info())
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
            self._step(result, gen)

    def _restart(self, future):
        self._waiting = None
        try:
            value = future.result()
        except Exception as exc:
            value = exc
        self._loop.call_soon_threadsafe(self._step, result)

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
        if isinstance(coro_or_future, Deferred):
            return coro_or_future
        elif isgenerator(coro_or_future):
            loop = loop or get_request_loop()
            task_factory = getattr(loop, 'task_factory', CoroTask)
            return task_factory(coro_or_future, loop)
        else:
            raise FutureTypeError('A Future or coroutine is required')

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
            value = self(value, loop)
            if get_result and value._state != _PENDING:
                value = value._result
            return value
        except FutureTypeError:
            if get_result:
                return maybe_failure(value)
            else:
                d = Deferred()
                d.callback(value)
                return d


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
    @wraps(method)
    def _(self, *args, **kwargs):
        try:
            result = method(self, *args, **kwargs)
        except Exception:
            result = sys.exc_info()
        return maybe_async(result, self._loop, False)

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
