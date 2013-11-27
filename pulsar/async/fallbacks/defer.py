import sys
import traceback
from collections import deque, namedtuple
from inspect import isgenerator, istraceback

from pulsar.utils.pep import iteritems, default_timer

from ..access import asyncio, get_request_loop, get_event_loop, logger


NOT_DONE = object()

class Error(Exception):
    '''Raised when no other information is available on a Failure'''


class CancelledError(Error):
    pass


class TimeoutError(Error):
    pass


class InvalidStateError(Error):
    """The operation is not allowed in this state."""


class FutureTypeError(TypeError):
    '''raised when invoking ``async`` on a wrong type.'''


class CoroutineReturn(BaseException):

    def __init__(self, value):
        self.value = value


_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


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
    _msg = 'Pulsar Asynchronous Failure'

    def __init__(self, exc_info, noisy=False):
        self.exc_info = as_async_exec_info(exc_info)
        self._mute = False
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
        if gen:
            __skip_traceback__ = True
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
        '''Mute logging and return self.'''
        setattr(self.exc_info[1], '_failure_logged', True)


class DoneCallback:

    def __init__(self, fut, fn):
        self.fut = fut
        self.fn = fn

    def __call__(self, result):
        fut = self.fut
        self.fn(fut)
        return fut._result

    def __eq__(self, fn):
        return self.fn == fn

    def __ne__(self, fn):
        return self.fn != fn


############################################################### Deferred
class Deferred(object):
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
    _state = _PENDING

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

    @property
    def has_callbacks(self):
        '''``True`` if this :class:`Deferred` has callbacks.
        '''
        return bool(self._callbacks)

    # Future methods, PEP 3156

    def done(self):
        '''Returns ``True`` if the :class:`Deferred` is done.

        This is the case when it was called or cancelled.
        '''
        return self._state != _PENDING

    def cancelled(self):
        '''pep-3156_ API method, it returns ``True`` if the :class:`Deferred`
        was cancelled.'''
        return self._state == _CANCELLED

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
            if mute and isinstance(self._result, Failure):
                if self._result.isinstance(exception_class):
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

    def exception(self):
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
        self._result = maybe_failure(result,
                                     getattr(self._loop, 'noisy', False))
        if not state:
            if isinstance(self._result, Failure):
                state = (_CANCELLED if self._result.isinstance(CancelledError)
                         else _FINISHED)
            else:
                state = _FINISHED
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
        noisy = getattr(loop, 'noisy', False)
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
                    self._result = Failure(sys.exc_info(), noisy)
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

    def __init__(self, gen, loop):
        self._gen = gen
        self._loop = loop
        self._waiting = None
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
                result = maybe_failure(result, self._loop.noisy)
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


######################################################### Async Bindings
class AsyncBindings:

    def __init__(self):
        self._bindings = []

    def add(self, callable):
        self._bindings.append(callable)

    def __call__(self, coro_or_future, loop=None):
        '''Handle an asynchronous ``coro_or_future``.

        Equivalent to the ``asyncio.async`` function but returns a
        :class:`.Deferred`. Raises :class:`FutureTypeError` if ``value``
        is not a generator nor a :class:`.Future`.

        This function can be overwritten by the :func:`set_async` function.

        :parameter value: the value to convert to a :class:`.Deferred`.
        :parameter loop: optional :class:`.EventLoop`.
        :return: a :class:`Deferred`.
        '''
        if self._bindings:
            for binding in bindings:
                d = binding(coro_or_future, loop)
                if d is not None:
                    return d
        if isinstance(coro_or_future, Deferred):
            return coro_or_future
        elif isgenerator(coro_or_future):
            loop = loop or get_request_loop()
            task_factory = getattr(loop, 'task_factory', DeferredTask)
            return task_factory(coro_or_future, loop)
        else:
            raise FutureTypeError('A Future or coroutine is required')

    def maybe(self, value, loop=None, get_result=True):
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
