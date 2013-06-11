import sys
import logging
import traceback
from collections import deque, namedtuple, Mapping
from itertools import chain
from inspect import isgenerator, istraceback, isclass

from pulsar.utils import events
from pulsar.utils.pep import raise_error_trace, iteritems, default_timer,\
                             get_event_loop, ispy3k

from .access import get_request_loop, NOTHING
from .consts import *


__all__ = ['Deferred',
           'EventHandler',
           'MultiDeferred',
           'Task',
           'Error',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'Failure',
           'maybe_failure',
           'is_failure',
           'log_failure',
           'is_async',
           'set_async',
           'maybe_async',
           'async',
           'multi_async',
           'async_sleep',
           'safe_async']

if ispy3k:
    from concurrent.futures._base import Error, CancelledError, TimeoutError

else:
    class Error(Exception):
        '''Raised when no other information is available on a Failure'''
    
    class CancelledError(Error):
        pass
    
    class TimeoutError(Error):
        pass

class InvalidStateError(Error):
    """The operation is not allowed in this state."""
    
# States of Deferred
_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'
    
LOGGER = logging.getLogger('pulsar.defer')

remote_stacktrace = namedtuple('remote_stacktrace', 'error_class error trace')
call_back = namedtuple('call_back', 'call error continuation')

pass_through = lambda result: result

def iterdata(stream, start=0):
    '''Iterate over a stream which is either a dictionary or a list. This
iterator is over key-value pairs for a dictionary, and index-value pairs
for a list.'''
    if isinstance(stream, Mapping):
        return iteritems(stream)
    else:
        return enumerate(stream, start)

def is_generalised_generator(value):
    '''Check if *value* is a generator. This is more general than the
inspect.isgenerator function.'''
    return hasattr(value, '__iter__') and not hasattr(value, '__len__')

def is_exc_info_error(exc_info):
    if isinstance(exc_info, remote_stacktrace):
        return True
    elif isinstance(exc_info, tuple) and len(exc_info) == 3:
        trace = exc_info[2]
        if not istraceback(trace):
            err_class = exc_info[0]
            if trace is None and isclass(err_class) and\
                    issubclass(err_class, BaseException):
                return isinstance(exc_info[1], err_class)
            return False
        else:
            return True
    return False
    
def multi_async(iterable, **kwargs):
    '''This is an utility function to convert an *iterable* into a
:class:`MultiDeferred` element.'''
    return MultiDeferred(iterable, **kwargs).lock()

def log_failure(failure, msg=None, level=None):
    '''Log the *failure* if *failure* is a :class:`Failure` or a
:class:`Deferred` with a called failure.'''
    failure = maybe_async(failure)
    if is_failure(failure):
        failure.log(msg=msg, level=level)
    return failure

def is_async(obj):
    '''Return ``True`` if *obj* is an asynchronous object'''
    return isinstance(obj, Deferred)

def is_failure(obj, classes=None):
    '''Check if ``obj`` is a :class:`Failure`. If optional ``classes``
are given, it checks if the error is an instance of those classes.'''
    if isinstance(obj, Failure):
        if classes:
            return obj.isinstance(classes)
        return True
    return False

def default_maybe_failure(value, msg=None):
    if isinstance(value, BaseException):
        exc_info = sys.exc_info()
        if value == exc_info[1]:
            return Failure(exc_info, msg)
        else:
            return Failure((value.__class__, value, None), msg)
    elif is_exc_info_error(value):
        return Failure(value, msg)
    else:
        return value

def default_maybe_async(val, get_result=True, **kwargs):
    if isgenerator(val):
        val = Task(val, **kwargs)
    if isinstance(val, Deferred):
        if get_result and val.done():
            return val.result
        else:
            return val
    elif get_result:
        return maybe_failure(val)
    else:
        d = Deferred()
        d.callback(val)
        return d
    
def maybe_async(value, description=None, event_loop=None, max_errors=0,
                timeout=None, get_result=True):
    '''Return an asynchronous instance only if *value* is
a generator or already an asynchronous instance. If *value* is asynchronous
it checks if it done. In this case it returns the *result*.

:parameter value: the value to convert to an asynchronous instance
    if it needs to.
:parameter description: optional description, rarely used.
:parameter event_loop: optional :class:`EventLoop`.
:parameter max_errors: The maximum number of exceptions allowed before
    stopping the generator and raise exceptions. By default it is 0.
    Pass ``None`` to continue the processing of the coroutine regardless of
    any exception.
:parameter timeout: optional timeout after which any asynchronous element get
    a cancellation.
:parameter get_result: optional flag indicating if to get the result in case
    the return values is a :class:`Deferred` already done. Default: ``True``.
:return: a :class:`Deferred` or  a :class:`Failure` or a **synchronous value**.
'''
    global _maybe_async
    return _maybe_async(value, description=description,
                        event_loop=event_loop, max_errors=max_errors,
                        timeout=timeout, get_result=get_result)
    
def maybe_failure(value, msg=None):
    '''Convert *value* into a :class:`Failure` if it is a stack trace or an
exception, otherwise returns *value*.

:parameter value: the value to convert to a :class:`Failure` instance
    if it needs to.
:parameter msg: Optional message to display in the log if *value* is a failure.
:return: a :class:`Failure` or the original *value*.
'''
    global _maybe_failure
    return _maybe_failure(value, msg=msg)

_maybe_async = default_maybe_async
_maybe_failure = default_maybe_failure

def set_async(maybe_async_callable, maybe_failure_callable):
    '''Set the asynchronous and failure discovery functions. This can be
used when third-party asynchronous objects are used in conjunction
with pulsar :class:`Deferred` and :class:`Failure`.'''
    global _maybe_async, _maybe_failure
    _maybe_async = maybe_async_callable
    _maybe_failure = maybe_failure_callable
    
def async_sleep(timeout):
    '''The asynchronous equivalent of ``time.sleep(timeout)``. Use this
function within a :ref:`coroutine <coroutine>` when you need to resume
the coroutine after *timeout* seconds. For example::

    ...
    yield async_sleep(2)
    ...
'''
    def _(err):
        if isinstance(err.trace[1], CancelledError):
            return timeout
        else:
            return err
    return Deferred(timeout=timeout).add_errback(_)
    
############################################################### DECORATORS
class async:
    '''A decorator class which invokes :func:`maybe_async` on the return
value of the function it is decorating. The input parameters and the outcome
are the same as :func:`maybe_async`.

Typical usage::

    @async()
    def myfunction(...):
        ...
        
It can also be used to safely call functions::

    async()(myfunction, ...)
    
This syntax will always return a :class:`Deferred`::

    async(get_result=False)(myfunction, ...)
'''
    def __init__(self, **params):
         self.params = params

    def __call__(self, func):
        def _(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        _.__name__ = func.__name__
        _.__doc__ = func.__doc__
        _.async = True
        return _
    
    def call(self, callable, *args, **kwargs):
        '''Safely execute a ``callable`` and always return a :class:`Deferred`,
even if the ``callable`` is not asynchronous. Never throws.'''
        try:
            result = callable(*args, **kwargs)
        except Exception:
            result = sys.exc_info()
        return maybe_async(result, **self.params)


safe_async = async(get_result=False).call

        
############################################################### FAILURE
class Failure(object):
    '''The asynchronous equivalent of python Exception. It accumulate errors
during :class:`Deferred` callbacks.

.. attribute:: traces

    List of (``errorType``, ``errvalue``, ``traceback``) occured during
    the execution of a :class:`Deferred`. Usually, this list contains
    one element only.

.. attribute:: logged

    Check if the :attr:`error` was logged. It can be a way of switching off
    logging for certain errors.
    
'''
    def __init__(self, err=None, msg=None):
        self.msg = msg or ''
        self.traces = []
        self.append(err)

    def __repr__(self):
        return '\n\n'.join(self.format_all())
    __str__ = __repr__
    
    #def __del__(self):
    #    self.log('Deferred Failure never retrieved')

    def _get_logged(self):
        return getattr(self.error, '_failure_logged', False)
    def _set_logged(self, value):
        err = self.error
        if err:
            setattr(err, '_failure_logged', value)
    logged = property(_get_logged, _set_logged)
    
    @property
    def trace(self):
        if self.traces:
            return self.traces[-1]
        else:
            return (None, None, None)
    
    @property
    def exc_info(self):
        if self.traces:
            return self.traces[-1]
        else:
            return (None, None, None)
    
    @property
    def error(self):
        '''Last python :class:`Exception` instance if available.'''
        return self.trace[1]
    
    def append(self, failure):
        '''Append new ``failure`` to self. The input ``failure`` can be another
:class:`Failure`, an :class:`Exception` or a system exception info tuple
obtained from ``sys.exc_info()``.'''
        if failure:
            if is_failure(failure):
                self.traces.extend(failure.traces)
            elif isinstance(failure, Exception):
                self.traces.append(sys.exc_info())
            elif is_exc_info_error(failure):
                self.traces.append(failure)
        return self
    
    def clear(self):
        '''Clear this :class:`Failure`'''
        self.traces = []

    def format_all(self):
        for exctype, value, tb in self:
            if istraceback(tb):
                tb = traceback.format_exception(exctype, value, tb)
            if tb:
                yield '\n'.join(tb)
            else:
                yield str(value)
    
    def isinstance(self, classes):
        '''Check if :attr:`error` is an instance of exception ``classes``.'''
        return isinstance(self.error, classes)
            
    def __getstate__(self):
        self.log()
        traces = []
        for exctype, value, tb in self:
            if istraceback(tb):
                tb = traceback.format_exception(exctype, value, tb)
            traces.append(remote_stacktrace(exctype, value, tb))
        state = self.__dict__.copy()
        state['traces'] = traces
        return state

    def __getitem__(self, index):
        return self.traces[index]

    def __len__(self):
        return len(self.traces)

    def __iter__(self):
        return iter(self.traces)

    def raise_all(self, first=True):
        pos = 0 if first else -1
        if self.traces and isinstance(self.traces[pos][1], Exception):
            eclass, error, trace = self.traces.pop()
            self.log()
            raise_error_trace(error, trace)
        else:
            self.log()
            N = len(self.traces)
            if N == 1:
                raise DeferredFailure(
                    'There was one failure during callbacks.')
            elif N > 1:
                raise DeferredFailure(
                    'There were {0} failures during callbacks.'.format(N))

    def log(self, log=None, msg=None, level=None):
        if self.traces and not self.logged:
            log = log or LOGGER
            msg = msg or self.msg
            if level:
                getattr(log, level)(msg)
            else:
                log.error(msg, exc_info=self.trace)
        self.logged = True


############################################################### Deferred
class Deferred(object):
    """The main class of pulsar asynchronous engine. It is a callback
which will be put off until later. It conforms with the
``tulip.Future`` interface with an implementation similar to the
``twisted.defer.Deferred`` class.

:param description: optional description which set the :attr:`description`
    attribute.
:param timeout: optional timeout. If greater than zero the deferred will be
    cancelled after *timeout* seconds if no result is available.
:param event_loop: If supplied, it is the :class:`EventLoop` associated
    with this :class:`Deferred`. If not supplied, the default event loop
    is used. The event loop associated with this :class:`Deferred` is accessed
    via the :attr:`event_loop` attribute.

.. attribute:: description

    An optional description of the :class:`Deferred` (for debugging and
    representation purposes).

.. attribute:: paused

    Integer indicating the number of times this :class:`Deferred` has been
    paused because the result of a callback was another :class:`Deferred`.

.. attribute:: result

    This is available once the :class:`Deferred` is done. Note,
    this can be anything, including another :class:`Deferred`. Trying to access
    this attribute when :meth:`done` is ``False`` will result in an
    ``AttributeError`` exception.
"""
    paused = 0
    _state = _PENDING
    _runningCallbacks = False
    _timeout = None

    def __init__(self, description=None, timeout=None, event_loop=None):
        self._description = description
        self._callbacks = deque()
        self._event_loop = event_loop
        self.set_timeout(timeout)

    def __repr__(self):
        v = self._description or self.__class__.__name__
        if self.cancelled():
            v += ' (cancelled)'
        elif self.done():
            v += ' (done)'
        return v

    def __str__(self):
        return self. __repr__()

    @property
    def event_loop(self):
        '''The :class:`EventLoop` associated with this :class:`Deferred`. It
can be set during initialisation.'''
        return self._event_loop or get_event_loop()
    
    @property
    def timeout(self):
        '''The :class:`TimedCall` which handles the timeout of this
:class:`Deferred`. Available only when a timeout is set.'''
        return self._timeout
    
    def set_timeout(self, timeout):
        '''Set a the :attr:`timeout` for this :class:`Deferred`. It returns
``self`` so that other methods can be concatenated.'''
        if timeout and timeout > 0:
            if self._timeout:
                self._timeout.cancel()
            # create the timeout. We don't cancel the timeout after
            # a callback is received since the result may be still asynchronous
            self._timeout = self.event_loop.call_later(timeout, self.cancel,
                                            'timeout (%s seconds)' % timeout)
        return self
        
    def cancelled(self):
        '''pep-3156_ API method, it returns ``True`` if the :class:`Deferred`
was cancelled.'''
        return self._state == _CANCELLED
    
    def done(self):
        '''pep-3156_ API method, it returns ``True`` if the :class:`Deferred` is
done, that is it was called or cancelled.'''
        return self._state != _PENDING
    
    def cancel(self, msg=''):
        '''pep-3156_ API method, it cancel the deferred and schedule callbacks.
If the deferred is waiting for another :class:`Deferred`, forward the
cancellation to that one. If the :class:`Deferred` is already :meth:`done`,
it does nothing.

:param msg: Optional message to pass to the when the :class:`CancelledError`
    is initialised.'''
        if not self.done():
            return self._cancel(msg)
        elif is_async(self.result):
            return self.result.cancel(msg)
    
    def running(self):
        '''pep-3156_ API method, always returns ``False``.'''
        return False
        
    def add_done_callback(self, fn):
        '''pep-3156_ API method, Add a callback to be run when the
:class:`Deferred` becomes done. The callback is called with a single argument,
the :class:`Deferred` object.'''
        callback = lambda r: fn(self)
        return self.add_callback(callback, callback)
    
    def set_result(self, result):
        '''pep-3156_ API method, same as :meth:`callback`'''
        return self.callback(result)
        
    def set_exception(self, exc):
        '''pep-3156_ API method, same as :meth:`callback`'''
        return self.callback(exc)
        
    def add_callback(self, callback, errback=None, continuation=None):
        """Add a callback as a callable function.
The function takes at most one argument, the result passed to the
:meth:`callback` method. If the *errback* callable is provided it will
be called when an exception occurs."""
        errback = errback if errback is not None else pass_through
        if hasattr(callback, '__call__') and hasattr(errback, '__call__'):
            self._callbacks.append(call_back(callback, errback, continuation))
            self._run_callbacks()
        else:
            raise TypeError('callbacks must be callable')
        return self

    def add_errback(self, errback, continuation=None):
        '''Same as :meth:`add_callback` but only for errors.'''
        return self.add_callback(pass_through, errback, continuation)

    def add_both(self, callback, continuation=None):
        '''Equivalent to `self.add_callback(callback, callback)`.'''
        return self.add_callback(callback, callback, continuation)

    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result, *args, **kwargs))

    def callback(self, result=None, state=None):
        '''Run registered callbacks with the given *result*.
This can only be run once. Later calls to this will raise
:class:`InvalidStateError`. If further callbacks are added after
this point, :meth:`add_callback` will run the *callbacks* immediately.

:return: the *result* input parameter
'''
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        elif self.done():
            raise InvalidStateError('Deferred %s already done' % self)
        self.result = maybe_failure(result)
        self._state = state or _FINISHED
        self._run_callbacks()
        return self.result
        
    def raise_all(self):
        '''raise an exception only if :meth:`done` is ``True`` and
the result is a :class:`Failure`'''
        if self.done() and is_failure(self.result):
            self.result.raise_all()

    ##################################################    INTERNAL METHODS
    def _run_callbacks(self):
        if not self.done() or self._runningCallbacks or self.paused:
            return
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            callback = callbacks[is_failure(self.result)]
            try:
                self._runningCallbacks = True
                try:
                    self.result = maybe_async(callback(self.result))
                finally:
                    self._runningCallbacks = False
            except Exception as e:
                self._add_exception(e)
            else:
                # if we received an asynchronous instance we add a continuation
                if is_async(self.result):
                    # Add a pause
                    self._pause()
                    # Add a callback to the result to resume callbacks
                    self.result.add_both(self._continue, self)
                    break

    def _pause(self):
        """Stop processing until :meth:`unpause` is called."""
        self.paused += 1

    def _unpause(self):
        """Process all callbacks made since :meth:`pause` was called."""
        self.paused -= 1
        self._run_callbacks()

    def _continue(self, result):
        self.result = result
        self._unpause()
        return self.result

    def _add_exception(self, e):
        if not isinstance(self.result, Failure):
            self.result = Failure(e)
        else:
            self.result.append(e)
    
    def _cancel(self, msg):
        return self.callback(CancelledError(msg))
    
    
class EventHandler(object):
    '''A Mixin for handling one time events and events that occur several
times. This mixin is used in :class:`Protocol` and :class:`Producer`
for scheduling connections and requests.'''
    ONE_TIME_EVENTS = ()
    '''Event names which occur once only. Implemented as :class:`Deferred`.'''
    MANY_TIMES_EVENTS = ()
    '''Event names which occur several times. Implemented as list
of callables.'''
    
    def __init__(self):
        o = dict(((e, Deferred()) for e in self.ONE_TIME_EVENTS))
        m = dict(((e, []) for e in self.MANY_TIMES_EVENTS))
        self.ONE_TIME_EVENTS = o
        self.MANY_TIMES_EVENTS = m
        
    def __copy__(self):
        d = self.__dict__.copy()
        cls = self.__class__
        obj = cls.__new__(cls)
        o = dict(((e, Deferred()) for e in self.ONE_TIME_EVENTS))
        d['ONE_TIME_EVENTS'] = o
        obj.__dict__ = d
        return obj
        
    def event(self, name):
        '''Return the handler for event *name*.'''
        if name in self.ONE_TIME_EVENTS:
            return self.ONE_TIME_EVENTS[name]
        else:
            return self.MANY_TIMES_EVENTS[name]
    
    def bind_event(self, event, callback):
        '''Register a *callback* with *event*. The callback must be
a callable which accept one argument only.'''
        callback = self.safe_callback(event, callback)
        if event in self.ONE_TIME_EVENTS:
            self.ONE_TIME_EVENTS[event].add_both(callback)
        elif event in self.MANY_TIMES_EVENTS:
            self.MANY_TIMES_EVENTS[event].append(callback)
        else:
            LOGGER.warning('unknown event "%s" for %s', event, self)
        
    def fire_event(self, event, event_data=NOTHING, sender=None):
        """Dispatches *event_data* to the *event* listeners.
* If *event* is a one-time event, it makes sure that it was not fired before.
        
:param event_data: if not provided, ``self`` will be dispatched instead, if an
    error it will be converted to a :class:`Failure`.
:param sender: optional sender of this event. It is only used for
    dispatching :ref:`global events <global-events>`.
:return: boolean indicating if the event was fired or not.
"""
        event_data = self if event_data is NOTHING else maybe_failure(event_data)
        fired = True
        if event in self.ONE_TIME_EVENTS:
            fired = not self.ONE_TIME_EVENTS[event].done()
            if fired:
                self.ONE_TIME_EVENTS[event].callback(event_data)
            else:
                LOGGER.debug('Event "%s" already fired for %s', event, self)
        elif event in self.MANY_TIMES_EVENTS:
            for callback in self.MANY_TIMES_EVENTS[event]:
                callback(event_data)
        else:
            fired = False
            LOGGER.warning('unknown event "%s" for %s', event, self)
        if fired:
            if sender is None:
                sender = getattr(event_data, '__class__', event_data) 
            events.fire(event, sender, data=event_data)
            log_failure(event_data)
        return fired
        
    def all_events(self):
        return chain(self.ONE_TIME_EVENTS, self.MANY_TIMES_EVENTS)
    
    def copy_many_times_events(self, other, *names):
        many = other.MANY_TIMES_EVENTS
        names = names or many
        for name in names:
            if name in many:
                if name in self.MANY_TIMES_EVENTS:
                    self.MANY_TIMES_EVENTS[name].extend(many[name])
                elif name in self.ONE_TIME_EVENTS:
                    d = self.ONE_TIME_EVENTS[name]
                    for callback in many[name]:
                        d.add_callback(callback)
    
    def safe_callback(self, event, callback):
        def _(result):
            try:
                callback(result)
            except Exception:
                LOGGER.exception('Unhandled exception in "%s" event', event)
            # always return result
            return result
        return _
            
                    
class Task(Deferred):
    '''A :class:`Deferred` coroutine is a consumer of, possibly,
asynchronous objects.
The callback will occur once the coroutine has stopped
(when it raises StopIteration), or a preset maximum number of errors (default 1)
has occurred. Instances of :class:`Task` are never
initialised directly, they are created by the :func:`maybe_async`
function when a generator is passed as argument.'''
    def __init__(self, gen, max_errors=0, event_loop=None, **kwargs):
        self.gen = gen
        self.max_errors = max(0, max_errors) if max_errors is not None else None
        self.errors = None
        event_loop = event_loop or get_request_loop()
        super(Task, self).__init__(event_loop=event_loop, **kwargs)
        self._consume(None)
    
    def _consume(self, result):
        while not self.done():
            if is_failure(result):
                if not self.errors:
                    self.errors = result
                elif result is not self.errors:
                    self.errors.append(result)
                if self.max_errors is not None and\
                        len(self.errors) > self.max_errors:
                    return self._conclude(result)
            try:
                result = self.gen.send(result)
            except StopIteration:
                break
            except Exception:
                result = sys.exc_info()
            result = maybe_async(result, event_loop=self.event_loop)
            if is_async(result):
                return result.add_both(self._restart)
            elif result == NOT_DONE:
                return self.event_loop.call_soon(self._consume, None)
        if not self.done():
            return self._conclude(result)

    def _restart(self, result):
        #restart the coroutine in the same event loop it was started
        self.event_loop.call_now_threadsafe(self._consume, result)
        # Important, this is a callback of a deferred, therefore we return
        # the passed result (which is not asynchronous).
        return result
    
    def _conclude(self, result):
        # Conclude the generator and callback the listeners
        self.gen.close()
        del self.gen
        del self.errors
        return self.callback(result)
    
    def _cancel(self, msg):
        return self._conclude(CancelledError(msg))
        
############################################################### MultiDeferred
class MultiDeferred(Deferred):
    '''A :class:`Deferred` for managing a ``collection`` of independent
asynchronous objects. The ``collection`` can be either a ``list`` or
a ``dict``.
The :class:`MultiDeferred` is recursive on values which are ``generators``,
``dic``, ``lists`` and ``sets``. It is not recursive on immutable
containers such as tuple, and frozenset.

.. attribute:: locked

    When ``True``, the :meth:`update` or :meth:`append` methods can no longer
    be used.
    
.. attribute:: type

    The type of multi-deferred. Either a ``list`` or a ``dict``.
    
.. attribute:: raise_on_error

    When ``True`` and at least one value of the result collections is a
    :class:`Failure`, the callback will receive the failure rather than
    the collection of results.
    
    Default ``True``.

'''
    _locked = False
    _time_locked = None
    _time_finished = None

    def __init__(self, data=None, type=None, raise_on_error=True,
                  handle_value=None, log_failure=False, **kwargs):
        self._deferred = {}
        self._failures = Failure()
        self.log_failure = log_failure
        self.raise_on_error = raise_on_error
        self.handle_value = handle_value
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
    def locked(self):
        return self._locked
        
    @property
    def type(self):
        return self._stream.__class__.__name__
    
    @property
    def total_time(self):
        if self._time_finished:
            return self._time_finished - self._time_start
        
    @property
    def locked_time(self):
        if self._time_finished:
            return self._time_finished - self._time_locked
        
    @property
    def num_failures(self):
        return len(self._failures)

    def lock(self):
        '''Lock the :class:`MultiDeferred` so that no new items can be added.
If it was alread :attr:`locked` a runtime exception occurs.'''
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
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
        '''Append a new ``value`` to this asynchronous container. Works for a
list :attr:`type` only.'''
        if self.type == 'list':
            self._add(len(self._stream), value)
        else:
            raise RuntimeError('Cannot append a value to a "dict" type '
                               'multideferred')

    ###    INTERNALS
    
    def _add(self, key, value):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot add a dependent once locked.')
        value = maybe_async(value)
        if is_generalised_generator(value):
            value = list(value)
        if isinstance(value, (Mapping, list, set)):
            value = self._make(value)    
        if not is_async(value) and self.handle_value:
            try:
                val = self.handle_value(value)
            except Exception as e:
                value = maybe_failure(e)
            else:
                if val is not value:
                    return self._add(key, val)
        self._setitem(key, value)
        # add callback if an asynchronous value
        if is_async(value):
            self._add_deferred(key, value)

    def _make(self, value):
        md = self.__class__(value, raise_on_error=self.raise_on_error,
                            handle_value=self.handle_value)
        return maybe_async(md.lock())

    def _add_deferred(self, key, value):
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
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot finish until completed.')
        if self._deferred:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot finish whilst waiting for '
                               'dependents %r' % self._deferred)
        self._time_finished = default_timer()
        if self.raise_on_error and self._failures:
            self.callback(self._failures)
        else:
            self.callback(self._stream)

    def _setitem(self, key, value):
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value
        if is_failure(value):
            if self.log_failure:
                log_failure(value)
            self._failures.append(value)
