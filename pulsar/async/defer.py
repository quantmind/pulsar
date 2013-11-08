import sys
import traceback
from collections import deque, namedtuple, Mapping
from inspect import isgenerator, istraceback

from pulsar.utils.pep import iteritems, default_timer

from .access import asyncio, get_request_loop, get_event_loop, logger
from .consts import *


__all__ = ['Deferred',
           'MultiDeferred',
           'DeferredTask',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'log_failure',
           'Failure',
           'maybe_failure',
           'coroutine_return',
           'is_failure',
           'set_async',
           'maybe_async',
           'async',
           'multi_async',
           'async_sleep',
           'async_while',
           'safe_async',
           'run_in_loop_thread',
           'in_loop',
           'in_loop_thread']


if not getattr(asyncio, 'fallback', False):
    from asyncio.futures import _PENDING, _CANCELLED, _FINISHED

else:   # pragma    nocover
    from ._asyncio import _PENDING, _CANCELLED, _FINISHED

from . import _asyncio

# States of Deferred
coroutine_return = _asyncio.coroutine_return
CoroutineReturn = _asyncio.CoroutineReturn
InvalidStateError = asyncio.InvalidStateError
CancelledError = asyncio.CancelledError
TimeoutError = asyncio.TimeoutError
Future = asyncio.Future
EMPTY_EXC_INFO = (None, None, None)
async_exec_info = namedtuple('async_exec_info', 'error_class error trace')
log_exc_info = ('error', 'critical')


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


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


def iterdata(stream, start=0):
    '''Iterate over a stream which is either a dictionary or a list. This
iterator is over key-value pairs for a dictionary, and index-value pairs
for a list.'''
    if isinstance(stream, Mapping):
        return iteritems(stream)
    else:
        return enumerate(stream, start)


def is_exc_info(exc_info):
    if isinstance(exc_info, async_exec_info):
        return True
    elif isinstance(exc_info, tuple) and len(exc_info) == 3:
        return istraceback(exc_info[2])
    return False


def multi_async(iterable, **kwargs):
    '''This is an utility function to convert an *iterable* into a
:class:`MultiDeferred` element.'''
    return MultiDeferred(iterable, **kwargs).lock()


def is_failure(obj, *classes):
    '''Check if ``obj`` is a :class:`Failure`.

    If optional ``classes`` are given, it checks if the error is an instance
    of those classes.
    '''
    if isinstance(obj, Failure):
        return obj.isinstance(classes) if classes else True
    return False


def maybe_failure(value):
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
        return Failure(value)
    else:
        return value


def default_async(coro_or_future, loop=None):
    if isinstance(coro_or_future, Future):
        if not isinstance(coro_or_future, Deferred):
            d = Deferred(loop)
            d._wrapped_future = coro_or_future
            coro_or_future.add_done_callback(FutureDeferredCallback(d))
            coro_or_future = d
        return coro_or_future
    elif isgenerator(coro_or_future):
        loop = loop or get_request_loop()
        task_factory = getattr(loop, 'task_factory', DeferredTask)
        return task_factory(coro_or_future, loop)
    else:
        raise FutureTypeError('A Future or coroutine is required')


def maybe_async(value, loop=None, get_result=True):
    '''Handle a possible asynchronous ``value``.

    Return an :ref:`asynchronous instance <tutorials-coroutine>`
    only if ``value`` is a generator, a :class:`Deferred` or ``get_result``
    is set to ``False``.

    :parameter value: the value to convert to an asynchronous instance
        if it needs to.
    :parameter loop: optional :class:`.EventLoop`.
    :parameter get_result: optional flag indicating if to get the result in
        case the return value is a :class:`Deferred` already done.
        Default: ``True``.
    :return: a :class:`Deferred` or  a :class:`Failure` or a synchronous
        value.
    '''
    try:
        value = async(value, loop)
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


def async(value, loop=None):
    '''Handle an asynchronous ``value``.

    Equivalent to the ``asyncio.async`` function but returns a
    :class:`.Deferred`. Raises :class:`FutureTypeError` if ``value``
    is not a generator nor a :class:`.Future`.

    This function can be overwritten by the :func:`set_async` function.

    :parameter value: the value to convert to a :class:`.Deferred`.
    :parameter loop: optional :class:`.EventLoop`.
    :return: a :class:`Deferred`.
    '''
    global _async
    return _async(value, loop)


def log_failure(value):
    '''Lag a :class:`Failure` if ``value`` is one.

    Return ``value``.
    '''
    value = maybe_failure(value)
    if isinstance(value, Failure):
        value.log()
    return value


_async = default_async


def set_async(async_callable):    # pragma    nocover
    '''Set the asynchronous and failure discovery functions. This can be
used when third-party asynchronous objects are used in conjunction
with pulsar :class:`Deferred` and :class:`Failure`.'''
    global _async
    _async = async_callable

def async_sleep(timeout):
    '''The asynchronous equivalent of ``time.sleep(timeout)``. Use this
function within a :ref:`coroutine <coroutine>` when you need to resume
the coroutine after ``timeout`` seconds. For example::

    ...
    yield async_sleep(2)
    ...

This function returns a :class:`Deferred` called back in ``timeout`` seconds
with the ``timeout`` value.
'''
    def _cancel(failure):
        if failure.isinstance(TimeoutError):
            failure.mute()
            return timeout
        else:
            return failure
    return Deferred().set_timeout(timeout, mute=True).add_errback(_cancel)


def safe_async(callable, *args, **kwargs):
    '''Safely execute a ``callable`` and always return a :class:`Deferred`.

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
    :return: A :class:`Deferred`.
    '''
    loop = get_event_loop()
    def _():
        start = default_timer()
        di = 0.1
        interval = 0
        result = while_clause(*args)
        while result:
            interval = min(interval+di, MAX_ASYNC_WHILE)
            try:
                yield Deferred(loop).set_timeout(interval)
            except TimeoutError:
                pass
            if timeout and default_timer() - start >= timeout:
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
            result = sys.exc_info()
            d.callback(result)
            raise
        else:
            d.callback(result)
    loop.call_soon_threadsafe(_)
    return d


def in_loop(method):
    '''Decorator to run a method on the event loop of the bound instance.
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
    of the bound instance.

    It uses the :func:`run_in_loop_thread` function.
    '''
    def _(self, *args, **kwargs):
        return run_in_loop_thread(self._loop, method, self, *args, **kwargs)

    _.__name__ = method.__name__
    _.__doc__ = method.__doc__
    return _


class DoneCallback:

    def __init__(self, fut, fn):
        self.fut = fut
        self.fn = fn

    def __call__(self, result):
        fut = self.fut
        self.fn(fut)
        return fut._exception if fut._exception is not None else fut._result

    def __eq__(self, fn):
        return self.fn == fn

    def __ne__(self, fn):
        return self.fn != fn


class FutureDeferredCallback:

    def __init__(self, deferred):
        self.deferred = deferred

    def __call__(self, fut):
        try:
            self.deferred.set_result(fut.result())
        except Exception:
            self.deferred.set_exception(sys.exc_info())


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

        Check if the :attr:`error` was logged. It can be used for switching off
        logging for certain errors by setting::

            failure.logged = True

    '''
    _msg = 'Pulsar Asynchronous Failure'

    def __init__(self, exc_info):
        self.exc_info = as_async_exec_info(exc_info)

    def __del__(self):
        self.log()

    def __repr__(self):
        return ''.join(self.exc_info[2])
    __str__ = __repr__

    def _get_logged(self):
        return getattr(self.error, '_failure_logged', False)

    def _set_logged(self, value):
        err = self.error
        if err:
            setattr(err, '_failure_logged', value)
    logged = property(_get_logged, _set_logged)

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
        if gen:
            __skip_traceback__ = True
            return gen.throw(self.exc_info[0], self.exc_info[1])
        else:
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
            self.logged = True
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

    def mute(self):
        '''Mute logging and return self.'''
        self.logged = True
        return self


############################################################### Deferred
class Deferred(asyncio.Future):
    """The main class of pulsar asynchronous engine.

    It is a ``asyncio.Future`` with an implementation similar to the
    ``twisted.defer.Deferred`` class.

    :param loop: If supplied, it is the :class:`.EventLoop` associated
        with this :class:`Deferred`. If not supplied, the default event loop
        is used. The event loop associated with this :class:`Deferred` is
        accessed via the :attr:`_loop` attribute.

    .. attribute:: _loop

        The event loop associated with this :class:`Deferred`.

    .. attribute:: _paused

        Integer indicating the number of times this :class:`Deferred` has been
        paused because the result of a callback was another :class:`Deferred`.

    .. attribute:: _result

        This is available once the :class:`Deferred` is done. Note,
        this can be anything, including another :class:`Deferred`.

    .. attribute:: _timeout

        The ``asyncio.TimerHandle`` which handles the timeout of this
        :class:`Deferred`.

        Available only when a :meth:`set_timeout` has been called with a valid
        timeout.
    """
    _paused = 0
    _runningCallbacks = False
    _suppressAlreadyCalled = False
    _timeout = None
    _callbacks = None
    _chained_to = None

    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()

    def __repr__(self):
        v = self.__class__.__name__
        if self.cancelled():
            v += ' (cancelled)'
        elif self.done():
            v += ' (done)'
        else:
            v += ' (pending)'
        return v

    def __str__(self):
        return self. __repr__()

    @property
    def timeout(self):
        '''The ``asyncio.TimerHandle`` which handles the timeout of this
        :class:`Deferred`.

        Available only when a timeout is set.
        '''
        return self._timeout

    # Future methods, PEP 3156

    def cancel(self, msg='', mute=False, exception_class=None):
        '''Cancel the deferred and schedule callbacks.

        If the deferred is waiting for another :class:`Deferred`,
        forward the cancellation to that one.

        If the :class:`Deferred` is already :meth:`done`, it does nothing.

        :param msg: Optional message to pass to the when the
            :class:`CancelledError` is initialised.
        :param mute: if ``True`` mute the logging of the exception class
        :param exception_class: optional exception class to raise on
            cancellation. By default it is the :class:`.CancelledError`
            exception.
        :return: ``True`` if the cancellation was done otherwise ``False``.
        '''
        if self._state == _PENDING:
            self._suppressAlreadyCalled = True
            exception_class = exception_class or CancelledError
            self.callback(exception_class(msg))
            if mute and is_failure(self._result, exception_class):
                self._result.mute()
            return True
        elif isinstance(self._result, Deferred):
            return self._result.cancel(msg, mute, exception_class)
        else:
            return False

    def result(self):
        if self._state == _PENDING:
            raise InvalidStateError('Result is not ready.')
        if isinstance(self._result, Failure):
            self._result.throw()
        else:
            return self._result

    def exeption(self):
        if self._state == _PENDING:
            raise InvalidStateError('Result is not ready.')
        if self._state == _CANCELLED:
            self._result.throw()
        if isinstance(self._result, Failure):
            self._result.mute()
            return self._result

    def add_done_callback(self, fn):
        '''Add a callback to be run when the :class:`Deferred` becomes done.

        The callback is called with a single argument,
        the :class:`Deferred` object.
        '''
        callback = DoneCallback(self, fn)
        self.add_callback(callback, callback)

    def remove_done_callback(self, fn):
        """Remove all instances of a callback from the "call when done" list.

        Returns the number of callbacks removed.
        """
        callbacks = self._callbacks
        removed_count = 0
        if callbacks:
            for i, callback_errback in enumerate(tuple(callbacks)):
                if callback_errback[0] == fn:
                    del callbacks[i]
                    removed_count += 1
        return removed_count

    def add_callback(self, callback, errback=None):
        '''Add a ``callback``, and an optional ``errback``.

        Add the two functions to the list of callbaks. Both of them take at
        most one argument, the result passed to the :meth:`callback` method.

        If the ``errback`` callable is provided it will be called when an
        exception occurs.

        :return: ``self``
        '''
        if not (callback or errback):
            return
        if ((not callback or hasattr(callback, '__call__')) and
                (not errback or hasattr(errback, '__call__'))):
            if self._callbacks is None:
                self._callbacks = deque()
            self._callbacks.append((callback, errback))
            self._run_callbacks()
        else:
            raise TypeError('callbacks must be callable or None')
        return self

    def add_errback(self, errback):
        '''Same as :meth:`add_callback` but only for errors.'''
        return self.add_callback(None, errback)

    def add_both(self, callback):
        '''Equivalent to `self.add_callback(callback, callback)`.'''
        return self.add_callback(callback, callback)

    def callback(self, result, state=None):
        '''Run registered callbacks with the given *result*.

        This can only be run once. Later calls to this will raise
        :class:`InvalidStateError`. If further callbacks are added after
        this point, :meth:`add_callback` will run the *callbacks* immediately.

        :return: the :attr:`_result` after all callbacks have been run.
        '''
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        elif self._state != _PENDING:
            if self._suppressAlreadyCalled:
                self._suppressAlreadyCalled = False
                return self._result
            raise InvalidStateError('Already called')
        self._result = maybe_failure(result)
        if not state:
            state = (_CANCELLED if is_failure(self._result, CancelledError)
                     else _FINISHED)
        self._state = state
        if self._callbacks:
            self._run_callbacks()
        return self._result
    set_result = callback
    set_exception = callback

    def set_timeout(self, timeout, mute=False, exception_class=None):
        '''Set a the :attr:`_timeout` for this :class:`Deferred`.

        If this method is called more than once, the previous :attr:`_timeout`
        is cancelled.

        :parameter timeout: a timeout in seconds.
        :return: ``self`` so that other methods can be concatenated.
        '''
        if timeout and timeout > 0:
            if self._timeout:
                self._timeout.cancel()
            # create the timeout. We don't cancel the timeout after
            # a callback is received since the result may be still asynchronous
            exception_class = exception_class or TimeoutError
            self._timeout = self._loop.call_later(
                timeout, self.cancel, 'timeout (%s seconds)' % timeout,
                mute, exception_class)
        return self

    def chain(self, deferred):
        '''Chain another ``deferred`` to this :class:`Deferred` callbacks.

        This method adds callbacks to this :class:`Deferred` to call
        ``deferred``'s callback or errback, as appropriate.

        :param deferred: a :class:`Deferred` which will partecipate in the
            callback chain of this :class:`Deferred`.
        :return: this :class:`Deferred`
        '''
        deferred._chained_to = self
        return self.add_callback(deferred.callback, deferred.callback)

    def then(self, deferred=None):
        '''Add another ``deferred`` to this :class:`Deferred` callbacks.

        :parameter deferred: Optional :class:`Deferred` to call back when this
            :class:`Deferred` receives the result or the exception.
            If not supplied a new :class:`Deferred` is created.
        :return: The ``deferred`` passed as parameter or the new deferred
            created.

        This method adds callbacks to this :class:`Deferred` to call
        ``deferred``'s callback or errback, as appropriate.
        It is a shorthand way of performing the following::

            def cbk(result):
                deferred.callback(result)
                return result

            self.add_both(cbk)

        When you use ``then`` on deferred ``d1``::

            d2 = d1.then()

        you obtains a new deferred ``d2`` which receives the callback when
        ``d1`` has received the callback, with the following properties:

        * Any event that fires ``d1`` will also fire ``d2``.
        * The converse is not true; if ``d2`` is fired ``d1`` will not be
          affected.
        * The callbacks of ``d2`` won't affect ``d1`` result.

        This method can be used instead of :meth:`add_callback` if a bright new
        deferred is required::

            d2 = d1.then().add_callback(...)
        '''
        if deferred is None:
            deferred = Deferred(loop=self._loop)

        def cbk(result):
            deferred.callback(result)
            return result

        self.add_callback(cbk, cbk)
        return deferred

    ##################################################    INTERNAL METHODS
    def _run_callbacks(self):
        if self._state == _PENDING or self._runningCallbacks or self._paused:
            return
        loop = self._loop
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            cbk = callbacks[isinstance(self._result, Failure)]
            if cbk:
                try:
                    self._runningCallbacks = True
                    try:
                        self._result = maybe_async(cbk(self._result), loop)
                    finally:
                        self._runningCallbacks = False
                except Exception:
                    self._result = Failure(sys.exc_info())
                else:
                    # received an asynchronous instance, add a continuation
                    if isinstance(self._result, Deferred):
                        # Add a pause
                        self._paused += 1
                        # Add a callback to the result to resume callbacks
                        self._result.add_callback(self._continue,
                                                  self._continue)
                        break

    def _continue(self, result):
        self._result = result
        self._paused -= 1
        self._run_callbacks()
        return self._result


class DeferredTask(Deferred):
    '''A :class:`Deferred` which consumes a :ref:`coroutine <coroutine>`.

    The callback will occur once the coroutine has finished
    (when it raises StopIteration), or an unhandled exception occurs.
    Instances of :class:`DeferredTask` are never
    initialised directly, they are created by the :func:`async` or
    :func:`maybe_async` functions when a generator is passed as argument.
    '''
    _waiting = None

    def __init__(self, gen, loop):
        self._gen = gen
        self._loop = loop
        self._consume(None)

    def _consume(self, result):
        step = self._step
        switch = False
        while self._state == _PENDING and not switch:
            result, switch = step(result, self._gen)

    def _step(self, result, gen):
        __skip_traceback__ = True
        conclude = False
        try:
            failure = None
            if isinstance(result, Failure):
                failure, result = result, None
                result = failure.throw(gen)
                failure.mute()
            else:
                result = gen.send(result)
        except CoroutineReturn as e:
            result = e.value
            conclude = True
        except StopIteration:
            conclude = True
        except Exception:
            result = sys.exc_info()
            conclude = True
        else:
            result = maybe_async(result, self._loop)
            if isinstance(result, Deferred):
                # async result add callback/errorback and transfer control
                # to the event loop
                self._waiting = result.add_callback(self._restart,
                                                    self._restart)
                return None, True
            elif result == NOT_DONE:
                # transfer control to the event loop
                self._loop.call_soon(self._consume, None)
                return None, True
        if conclude:
            if failure:
                result = maybe_failure(result)
                if isinstance(result, Failure):
                    if result.exc_info[1] is not failure.exc_info[1]:
                        failure.mute()
                else:
                    failure.mute()
            self._gen.close()
            del self._gen
            self.callback(result)
        return result, False

    def _restart(self, result):
        self._waiting = None
        # restart the coroutine in the same event loop it was started
        self._loop.call_soon_threadsafe(self._consume, result)
        # Important, this is a callback of a deferred, therefore we return
        # the passed result (which is synchronous).
        return result

    def cancel(self, msg='', mute=False, exception_class=None):
        if self._waiting:
            self._waiting.cancel(msg, mute, exception_class)
        else:
            super(DeferredTask, self).cancel(msg, mute, exception_class)


############################################################### MultiDeferred
class MultiDeferred(Deferred):
    '''A :class:1Deferred` for a ``collection`` of asynchronous objects.

    The ``collection`` can be either a ``list`` or a ``dict``.
    '''
    _locked = False
    _time_locked = None
    _time_finished = None

    def __init__(self, data=None, type=None, raise_on_error=True,
                 mute_failures=False, **kwargs):
        self._deferred = {}
        self._failures = []
        self._mute_failures = mute_failures
        self._raise_on_error = raise_on_error
        if not type:
            type = data.__class__ if data is not None else list
        if not issubclass(type, (list, Mapping)):
            type = list
        self._stream = type()
        super(MultiDeferred, self).__init__(**kwargs)
        self._time_start = default_timer()
        if data:
            self.update(data)

    @property
    def raise_on_error(self):
        '''When ``True`` and at least one value of the result collections is a
        :class:`Failure`, the callback will receive the failure rather than
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
