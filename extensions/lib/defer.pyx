import sys
import logging
from traceback import format_exception as tb_format_exception
from types import TracebackType, GeneratorType
from collections import deque

cdef list async_bindings = []
cdef int _PENDING = 0
cdef int _FINISHED = 1
cdef int _CANCELLED = 2
cdef dict _states = {_PENDING: 'PENDING',
                     _FINISHED: 'FINISHED',
                     _CANCELLED: 'CANCELLED'}
cdef tuple log_exc_info = ('error', 'critical')


NOT_DONE = object()


class Error(Exception):
    pass


class InvalidStateError(Error):
    pass


class CancelledError(Error):
    pass


class TimeoutError(Error):
    pass


class FutureTypeError(Error):
    pass


class CoroutineReturn(BaseException):

    def __init__(self, value):
        self.value = value


class Traceback(tuple):
    pass


cdef class Failure:
    cdef tuple _exc_info
    cdef bint _mute

    def __cinit__(self, tuple exc_info):
        if not isinstance(exc_info[2], Traceback):
            exctype, value, tb = exc_info
            trace = format_exception(exctype, value, tb)
            exc_info = (exctype, value, trace)
        self._exc_info = exc_info
        self._mute = False

    def __dealloc__(self):
        if not self._mute:
            self.log()

    def __repr__(self):
        return ''.join(self.exc_info[2])
    __str__ = __repr__

    @property
    def exc_info(self):
        return self._exc_info[1]

    @property
    def error(self):
        return self._exc_info[1]

    @property
    def logged(self):
        return getattr(self._exc_info[1], '_failure_logged', False)

    def throw(self, gen=None):
        if gen:
            return gen.throw(self._exc_info[0], self._exc_info[1])
        else:
            # mute only this Failure, not the error
            self._mute = True
            raise self._exc_info[1]

    cpdef bint isinstance(self, classes):
        return isinstance(self._exc_info[1], classes)

    cpdef log(self, log=None, msg=None, level=None):
        if not self.logged:
            self.mute()
            msg = msg or 'Pulsar Asynchronous Failure'
            log = log or _async.logger()
            level = level or 'error'
            handler = getattr(log, level)
            if level in log_exc_info:
                if self._exc_info[2]:
                    c = '\n'
                    emsg = ''.join(self._exc_info[2])
                else:
                    c = ': '
                    emsg = str(self._exc_info[1])
                msg = '%s%s%s' % (msg, c, emsg) if msg else emsg
            handler(msg)
        return self

    def critical(self, msg):
        return self.log(msg=msg, level='critical')

    cpdef mute(self):
        setattr(self._exc_info[1], '_failure_logged', True)


cdef inline bint is_relevant_tb(object tb):
    return not ('__skip_traceback__' in tb.tb_frame.f_locals or
                '__unittest' in tb.tb_frame.f_globals)


cdef inline int tb_length(object tb):
    cdef int length = 0
    while tb and is_relevant_tb(tb):
        length += 1
        tb = tb.tb_next
    return length


cdef inline object format_exception(exctype, value, tb):
    trace = getattr(value, '__async_traceback__', None)
    while tb and not is_relevant_tb(tb):
        tb = tb.tb_next
    length = tb_length(tb)
    if length or not trace:
        tb = tb_format_exception(exctype, value, tb, length)
    if trace:
        if tb:
            tb = tb[:-1]
            tb.extend(trace[1:])
        else:
            tb = trace
    value.__async_traceback__ = tb
    value.__traceback__ = None
    return Traceback(tb)


cdef inline bint is_exc_info(exc_info):
    if isinstance(exc_info, tuple) and len(exc_info) == 3:
        return isinstance(exc_info[2], (Traceback, Traceback))
    return False


cdef inline object _maybe_failure(object value):
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
    return value


def maybe_failure(value):
    return _maybe_failure(value)


cdef class Deferred:
    cdef int _paused
    cdef bint _runningCallbacks
    cdef bint _suppressAlreadyCalled
    cdef object _timeout
    cdef object _callbacks
    cdef object _chained_to
    cdef object _event_loop
    cdef object _result
    cdef Deferred __chained_to
    cdef int _state_code

    def __cinit__(self, object loop=None):
        self._event_loop = loop or _async.get_event_loop()
        self._state_code = _PENDING

    @property
    def _state(self):
        return _states[self._state_code]

    @property
    def _loop(self):
        return self._event_loop

    @property
    def _chained_to(self):
        return self.__chained_to

    def done(self):
        return self._state_code != _PENDING

    def cancelled(self):
        return self._state_code == _CANCELLED

    def cancel(self, msg='', mute=False, exception_class=None):
        if self._state_code == _PENDING:
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
        if self._state_code == _PENDING:
            raise InvalidStateError('Result is not ready.')
        if isinstance(self._result, Failure):
            self._result.throw()
        else:
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
        cdef list callbacks = self._callbacks
        removed_count = 0
        if callbacks:
            for i, callback_errback in enumerate(tuple(callbacks)):
                if callback_errback[0] == fn:
                    del callbacks[i]
                    removed_count += 1
        return removed_count

    cpdef Deferred add_callback(self, callback, errback=None):
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
        return self.add_callback(None, errback)

    def add_both(self, callback):
        return self.add_callback(callback, callback)

    cpdef Deferred chain(self, Deferred deferred):
        deferred.__chained_to = self
        return self.add_callback(deferred.callback, deferred.callback)

    def then(self, deferred=None):
        if deferred is None:
            deferred = Deferred(loop=self._loop)

        def cbk(result):
            deferred.callback(result)
            return result

        self.add_callback(cbk, cbk)
        return deferred

    cpdef object callback(self, result, state=None):
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        elif self._state_code != _PENDING:
            if self._suppressAlreadyCalled:
                self._suppressAlreadyCalled = False
                return self._result
            raise InvalidStateError('Already called')
        self._result = _maybe_failure(result)
        if not state:
            if isinstance(self._result, Failure):
                state = (_CANCELLED if self._result.isinstance(CancelledError)
                         else _FINISHED)
            else:
                state = _FINISHED
        self._state_code = state
        if self._callbacks:
            self._run_callbacks()
        return self._result
    set_result = callback
    set_exception = callback

    cdef _run_callbacks(self):
        if (self._state_code == _PENDING or self._runningCallbacks or
                self._paused):
            return
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            cbk = callbacks[isinstance(self._result, Failure)]
            if cbk:
                try:
                    self._runningCallbacks = True
                    try:
                        self._result = maybe_async(cbk(self._result),
                                                   self._event_loop)
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


cdef class DeferredTask(Deferred):
    cdef object _gen
    cdef Deferred _waiting

    cdef start(self, gen):
        self._gen = gen
        self._waiting = None
        self._consume(None)

    def _consume(self, result):
        cdef bint switch = False
        while self._state_code == _PENDING and not switch:
            result, switch = self._step(result)

    cdef tuple _step(self, object result):
        cdef Failure failure = None
        cdef bint conclude = False
        try:
            if isinstance(result, Failure):
                failure, result = result, None
                result = failure.throw(self._gen)
                failure.mute()
            else:
                result = self._gen.send(result)
        except CoroutineReturn as e:
            result = e.value
            conclude = True
        except StopIteration:
            conclude = True
        except Exception:
            result = sys.exc_info()
            conclude = True
        else:
            result = maybe_async(result, self._event_loop)
            if isinstance(result, Deferred):
                # async result add callback/errorback and transfer control
                # to the event loop
                self._waiting = result.add_callback(self._restart,
                                                    self._restart)
                return None, True
            elif result == NOT_DONE:
                # transfer control to the event loop
                self._event_loop.call_soon(self._consume, None)
                return None, True
        if conclude:
            if failure:
                result = _maybe_failure(result)
                if isinstance(result, Failure):
                    if result.exc_info[1] is not failure.exc_info[1]:
                        failure.mute()
                else:
                    failure.mute()
            self._gen.close()
            self.callback(result)
        return result, False

    def _restart(self, result):
        self._waiting = None
        # restart the coroutine in the same event loop it was started
        self._event_loop.call_soon_threadsafe(self._consume, result)
        # Important, this is a callback of a deferred, therefore we return
        # the passed result (which is synchronous).
        return result

    def cancel(self, msg='', mute=False, exception_class=None):
        if self._waiting:
            self._waiting.cancel(msg, mute, exception_class)
        else:
            super(DeferredTask, self).cancel(msg, mute, exception_class)


cdef class AsyncBindings:
    cdef object Future
    cdef list _bindings
    cdef object get_event_loop
    cdef object get_request_loop

    def __init__(self):
        self._bindings = []

    def add(self, callable):
        self._bindings.append(callable)

    def __call__(self, coro_or_future, loop=None):
        cdef DeferredTask task
        if self._bindings:
            for binding in self._bindings:
                d = binding(coro_or_future, loop)
                if d is not None:
                    return d
        if isinstance(coro_or_future, Deferred):
            return coro_or_future
        elif isinstance(coro_or_future, GeneratorType):
            loop = loop or self.get_request_loop()
            task_factory = getattr(loop, 'task_factory', DeferredTask)
            task = task_factory(loop)
            task.start(coro_or_future)
        else:
            raise FutureTypeError('A Future or coroutine is required')

    def maybe(self, value, loop=None, get_result=True):
        cdef Deferred d
        try:
            d = self(value, loop)
            if get_result and d._state_code != _PENDING:
                return value._result
            return d
        except FutureTypeError:
            if get_result:
                return _maybe_failure(value)
            else:
                d = Deferred(loop)
                d.callback(value)
                return d

    def logger(self):
        logger = getattr(self.get_request_loop(), 'logger', None)
        if not logger:
            logger = logging.getLogger('pulsar')
        return logger


cdef AsyncBindings _async = AsyncBindings()

async = _async
maybe_async = _async.maybe
add_async_binding = _async.add


def set_access(object get_event_loop, object get_request_loop):
    _async.get_event_loop = get_event_loop
    _async.get_request_loop = get_request_loop
