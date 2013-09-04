import sys
import traceback
from weakref import ref
from collections import deque, namedtuple, Mapping
from itertools import chain
from inspect import isgenerator, istraceback

from pulsar.utils import events
from pulsar.utils.pep import raise_error_trace, iteritems, default_timer,\
                             get_event_loop, ispy3k

from .access import get_request_loop, NOTHING, logger
from .consts import *


__all__ = ['Deferred',
           'EventHandler',
           'MultiDeferred',
           'Task',
           'Error',
           'CancelledError',
           'TimeoutError',
           'InvalidStateError',
           'log_failure',
           'Failure',
           'FailureRefs',
           'maybe_failure',
           'coroutine_return',
           'is_failure',
           'set_async',
           'maybe_async',
           'async',
           'multi_async',
           'async_sleep',
           'async_while',
           'safe_async']

if ispy3k:
    from concurrent.futures._base import Error, CancelledError, TimeoutError
else:   #pragma    nocover
    class Error(Exception):
        '''Raised when no other information is available on a Failure'''
    
    class CancelledError(Error):
        pass
    
    class TimeoutError(Error):
        pass

class InvalidStateError(Error):
    """The operation is not allowed in this state."""
    
class CoroutineReturn(BaseException):
    def __init__(self, value):
        self.value = value
    
# States of Deferred
_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'
EMPTY_EXC_INFO = (None, None, None)

remote_stacktrace = namedtuple('remote_stacktrace', 'error_class error trace')
call_back = namedtuple('call_back', 'call error continuation')

pass_through = lambda result: result

def remote_exc_info(exc_info):
    if not isinstance(exc_info, remote_stacktrace):
        exctype, value, tb = exc_info
        tb = traceback.format_exception(exctype, value, tb)
        exc_info = remote_stacktrace(exctype, value, tb)
    return exc_info

def coroutine_return(value=None):
    raise CoroutineReturn(value)

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

def is_exc_info(exc_info):
    if isinstance(exc_info, remote_stacktrace):
        return True
    elif isinstance(exc_info, tuple) and len(exc_info) == 3:
        return istraceback(exc_info[2])
    return False
    
def multi_async(iterable, **kwargs):
    '''This is an utility function to convert an *iterable* into a
:class:`MultiDeferred` element.'''
    return MultiDeferred(iterable, **kwargs).lock()

def is_failure(obj, *classes):
    '''Check if ``obj`` is a :class:`Failure`. If optional ``classes``
are given, it checks if the error is an instance of those classes.'''
    if isinstance(obj, Failure):
        return obj.isinstance(classes) if classes else True
    return False

def default_maybe_failure(value):
    if isinstance(value, Exception):
        return Failure.make(value)
    elif is_exc_info(value):
        return Failure(value)
    else:
        return value

def default_maybe_async(val, get_result=True, event_loop=None, **kwargs):
    if isgenerator(val):
        event_loop = event_loop or get_request_loop()
        task_factory = getattr(event_loop, 'task_factory', Task)
        val = task_factory(val, event_loop, **kwargs)
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
    
def maybe_async(value, canceller=None, event_loop=None, timeout=None,
                get_result=True):
    '''Return an :ref:`asynchronous instance <tutorials-coroutine>`
only if ``value`` is a generator, a :class:`Deferred` or ``get_result`` is
set to ``False``.

:parameter value: the value to convert to an asynchronous instance
    if it needs to.
:parameter canceller: optional canceller (see :attr:`Deferred.canceller`).
:parameter event_loop: optional :class:`EventLoop`.
:parameter timeout: optional timeout after which any asynchronous element get
    a cancellation.
:parameter get_result: optional flag indicating if to get the result in case
    the return value is a :class:`Deferred` already done. Default: ``True``.
:return: a :class:`Deferred` or  a :class:`Failure` or a synchronous value.
'''
    global _maybe_async
    return _maybe_async(value, canceller=canceller, event_loop=event_loop,
                        timeout=timeout, get_result=get_result)
    
def log_failure(value):
    value = maybe_failure(value)
    if isinstance(value, Failure):
        value.log()
    else:
        value = maybe_failure(value)
        if isinstance(value, Failure):
            value.log()
    return value

def maybe_failure(value):
    '''Convert *value* into a :class:`Failure` if it is a stack trace or an
exception, otherwise returns *value*.

:parameter value: the value to convert to a :class:`Failure` instance
    if it needs to.
:parameter msg: Optional message to display in the log if *value* is a failure.
:return: a :class:`Failure` or the original *value*.
'''
    global _maybe_failure
    return _maybe_failure(value)

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
the coroutine after ``timeout`` seconds. For example::

    ...
    yield async_sleep(2)
    ...
    
This function returns a :class:`Deferred` called back in ``timeout`` seconds
with the ``timeout`` value.
'''
    def _cancel(failure):
        if failure.isinstance(CancelledError):
            failure.mute()
            return timeout
        else:
            return failure        
    return Deferred(timeout=timeout).add_errback(_cancel)
    
############################################################### DECORATORS
class async:
    '''A decorator for :ref:`asynchronous components <tutorials-coroutine>`.
    
It convert the return value of the callable it decorates into a
:class:`Deferred` via the :func:`maybe_async` function. The return value is
**always** a :class:`Deferred` (unless :attr:`get_result` is ``True``),
and the function never throws.

:parameter timeout: Optional timeout in seconds.
:parameter get_result: if ``True`` return the deferred value if the deferred
    is done. Default ``False``.

Check the :ref:`Asynchronous utilities tutorial <tutorial-async-utilities>`
for detailed discussion and examples.

Typical usage::

    @async()
    def myfunction(...):
        ...
        
When invoked, ``myfunction`` it returns a :class:`Deferred`. For example::

    @async()
    def simple():
        return 1
        
    @async()
    def simple_error():
        raise ValueError('Kaput!')
        
    @async()
    def simple_coro():
        result = yield ...
        ...

when invoked::
        
    >>> d = simple()
    >>> d
    Deferred (done)
    >>> d.result
    1
    >>> d = simple_error()
    >>> d
    Deferred (done)
    >>> d.result
    Failure: Traceback (most recent call last):
      File "...pulsar\async\defer.py", line 260, in call
        result = callable(*args, **kwargs)
      File ...
        raise ValueError('Kaput!')
    ValueError: Kaput!
'''
    def __init__(self, timeout=None, get_result=False):
        self.timeout = timeout
        self.get_result = get_result

    def __call__(self, func):
        def _(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        _.__name__ = func.__name__
        _.__doc__ = func.__doc__
        _.async = True
        return _
    
    def call(self, callable, *args, **kwargs):
        try:
            result = callable(*args, **kwargs)
        except Exception:
            result = Failure(sys.exc_info())
        return maybe_async(result, get_result=self.get_result,
                           timeout=self.timeout)


safe_async = async().call

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
    def _():
        start = default_timer()
        di = 0.1
        interval = 0
        result = while_clause(*args)
        while result:
            interval = min(interval+di, MAX_ASYNC_WHILE) 
            try:
                yield Deferred(timeout=interval)
            except CancelledError:
                pass
            if timeout and default_timer() - start >= timeout:
                break
            result = while_clause(*args)
        yield result
    return maybe_async(_(), get_result=False)


class FailureRefs(object):
    '''Avoid the use of ``__del__`` method in the failure class.'''
    def __init__(self):
        self._refs = {}
        
    def add(self, failure):
        self._refs[ref(failure, self._log_failure)] = failure.exc_info
    
    def _log_failure(self, ref):
        exc_info = self._refs.pop(ref, None)
        if exc_info:
            log_exc('Deferred Failure never retrieved', exc_info)

    
def log_exc(msg, exc_info, log=None, level=None):
    error = exc_info[1]
    if not getattr(error, '_failure_logged', False):
        setattr(error, '_failure_logged', True)
        log = log or logger()
        if isinstance(exc_info, remote_stacktrace):
            msg = msg or ''.join(exc_info[2])
            exc_info = None
        if level:
            getattr(log, level)(msg)
        else:
            log.error(msg, exc_info=exc_info)
        
        
failure_refs = FailureRefs()

############################################################### FAILURE
class Failure(object):
    '''The asynchronous equivalent of python Exception. It has several useful
methods and features which facilitates logging, pickling and throwing
errors.

:parameter error: an ``exc_info`` tuple or a :class:`BaseException` instance.

.. attribute:: exc_info

    The exception as a three elements tuple
    (``errorType``, ``errvalue``, ``traceback``) occured during
    the execution of a :class:`Deferred`.

.. attribute:: logged

    Check if the :attr:`error` was logged. It can be used for switching off
    logging for certain errors by setting::
    
        failure.logged = True
    
'''
    _msg = 'Pulsar asynchronous failure'
    
    @classmethod
    def make(cls, error, frefs=None):
        if isinstance(error, cls):
            return error
        elif isinstance(error, BaseException):
            exc_info = sys.exc_info()
            if error is not exc_info[1]: 
                try:
                    raise error
                except:
                    exc_info = sys.exc_info()
        else:
            exc_info = error
        return cls(exc_info, frefs)
    
    def __init__(self, exc_info, frefs=None):
        self.exc_info = exc_info
        (frefs or failure_refs).add(self)
         
    def __repr__(self):
        if self.is_remote:
            tb = self.exc_info[2]
        else:
            tb = traceback.format_exception(*self.exc_info)
        return ''.join(tb)
    __str__ = __repr__
    
    #def __del__ (self):
    #    self.log(msg='Deferred Failure never retrieved')

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
    
    @property
    def is_remote(self):
        return isinstance(self.exc_info, remote_stacktrace)

    def isinstance(self, classes):
        '''Check if :attr:`error` is an instance of exception ``classes``.'''
        return isinstance(self.error, classes)

    def throw(self, gen=None):
        '''Raises the exception from the :attr:`exc_info`.
        
:parameter gen: Optional generator. If provided the exception is throw into
    the generator via the ``gen.throw`` method.
    
Without ``gen``, this method is used when interacting with libraries
supporting both synchronous and asynchronous flow controls.'''
        if gen:
            if self.is_remote:
                return gen.throw(self.exc_info[0], self.exc_info[1])
            else:
                return gen.throw(*self.exc_info)
        else:
            if self.is_remote:
                raise self.exc_info[1]
            else:
                _, error, trace = self.exc_info
                self.log()
                raise_error_trace(error, trace)

    def log(self, log=None, msg=None, level=None):
        '''Log the :class:`Failure` and set :attr:`logged` to ``True``.
        
Logging for a given failure occurs once only. To suppress logging for
a given failure set :attr:`logged` to ``True`` of invoke the :meth:`mute`
method.
Returns ``self`` so that this method can easily be added as an error
back to perform logging and propagate the failure. For example::

    .add_errback(lambda failure: failure.log())
'''
        if not getattr(self.error, '_failure_logged', False):
            if not self.is_remote:
                msg = msg or self._msg
            log_exc(msg, self.exc_info, log, level)
        return self
    
    def mute(self):
        '''Mute logging and return self.'''
        self.logged = True
        return self
       
    def __getstate__(self):
        self.log()
        return {'exc_info': remote_exc_info(self.exc_info)}

############################################################### Deferred
class Deferred(object):
    """The main class of pulsar asynchronous engine. It is a callback
which will be put off until later. It conforms with the
``tulip.Future`` interface with an implementation similar to the
``twisted.defer.Deferred`` class.

:param canceller: optional callable which set the :attr:`canceller`
    attribute.
:param timeout: optional timeout. If greater than zero the deferred will be
    cancelled after ``timeout`` seconds if no result is available.
:param event_loop: If supplied, it is the :class:`EventLoop` associated
    with this :class:`Deferred`. If not supplied, the default event loop
    is used. The event loop associated with this :class:`Deferred` is accessed
    via the :attr:`event_loop` attribute.

.. attribute:: canceller

    A callable used to stop the pending operation scheduled by this
    :class:`Deferred` when :meth:`cancel` is invoked. The canceller must
    accept a deferred as the only argument.

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
    _suppressAlreadyCalled = False
    _timeout = None
    _callbacks = None

    def __init__(self, canceller=None, timeout=None, event_loop=None):
        self._canceller = canceller
        self._event_loop = event_loop
        if timeout:
            self.set_timeout(timeout)

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
    def event_loop(self):
        '''The :class:`EventLoop` associated with this :class:`Deferred`. It
can be set during initialisation.'''
        return self._event_loop or get_event_loop()
    
    @property
    def timeout(self):
        '''The :class:`TimedCall` which handles the timeout of this
:class:`Deferred`. Available only when a timeout is set.'''
        return self._timeout
    
    def set_timeout(self, timeout, event_loop=None):
        '''Set a the :attr:`timeout` for this :class:`Deferred`.

:parameter timeout: a timeout in seconds.
:parameter event_loop: optional event loop where to run the callback. If not
    supplied the :attr:`event_loop` attribute is used.
:return: returns ``self`` so that other methods can be concatenated.'''
        if timeout and timeout > 0:
            if self._timeout:
                self._timeout.cancel()
            if not event_loop:
                event_loop = self.event_loop
            elif self._event_loop:
                assert event_loop == self._event_loop, "Incompatible event loop"
            # create the timeout. We don't cancel the timeout after
            # a callback is received since the result may be still asynchronous
            self._timeout = event_loop.call_later(
                timeout, self.cancel, 'timeout (%s seconds)' % timeout)
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
            if self._canceller:
                self._canceller(self)
            else:
                self._suppressAlreadyCalled = True
            if not self.done():
                self.callback(CancelledError(msg))
        elif isinstance(self.result, Deferred):
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
        """Add a ``callback``, and an optional ``errback``, to the list
of callbaks. Both of them are functions which take at most one argument,
the result passed to the :meth:`callback` method. If the ``errback`` callable
is provided it will be called when an exception occurs."""
        errback = errback if errback is not None else pass_through
        if hasattr(callback, '__call__') and hasattr(errback, '__call__'):
            if self._callbacks is None:
                self._callbacks = deque()
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
            if self._suppressAlreadyCalled:
                self._suppressAlreadyCalled = False
                return self.result
            raise InvalidStateError
        self.result = maybe_failure(result)
        if not state:
            state = _CANCELLED if is_failure(self.result, CancelledError) else\
                        _FINISHED
        self._state = state
        if self._callbacks:
            self._run_callbacks()
        return self.result
        
    def throw(self):
        '''raise an exception only if :meth:`done` is ``True`` and
the result is a :class:`Failure`'''
        if self.done() and isinstance(self.result, Failure):
            self.result.throw()
            
    def log(self, **kwargs):
        '''Log a failure via the :meth:`Failure.log` method if the
:attr:`result` is a :class:`Failure`.'''
        if self.done() and isinstance(self.result, Failure):
            self.result.log(**kwargs)
            
    def then(self, deferred=None):
        '''Add another ``deferred`` to this :class:`Deferred` callbacks.

:parameter deferred: Optional :class:`Deferred` to call back when this
    :class:`Deferred` receives the result or the exception. If not supplied
    a new :class:`Deferred` is created.
:return: The ``deferred`` passed as parameter or the new deferred created.

This method adds callbacks to this :class:`Deferred` to call ``deferred``'s
callback or errback, as appropriate. It is a shorthand way
of performing the following::

    def cbk(result):
        deferred.callback(result)
        return result
        
    self.add_both(cbk)
   
When you use ``then`` on deferred ``d1``::

    d2 = d1.then()
    
you obtains a new deferred ``d2`` which receives the callback when ``d1``
has received the callback, with the following properties:

* Any event that fires ``d1`` will also fire ``d2``.
* The converse is not true; if ``d2`` is fired ``d1`` will not be affected.
* The callbacks of ``d2`` won't affect ``d1`` result.

This method can be used instead of :meth:`add_callback` if a bright new
deferred is required::

    d2 = d1.then().add_callback(...)
'''
        if deferred is None:
            deferred = Deferred(event_loop=self._event_loop)
        def cbk(result):
            deferred.callback(result)
            return result
        self.add_both(cbk)
        return deferred

    ##################################################    INTERNAL METHODS
    def _run_callbacks(self):
        if not self.done() or self._runningCallbacks or self.paused:
            return
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            callback = callbacks[isinstance(self.result, Failure)]
            try:
                self._runningCallbacks = True
                try:
                    self.result = maybe_async(callback(self.result))
                finally:
                    self._runningCallbacks = False
            except Exception:
                self.result = Failure(sys.exc_info())
            else:
                # if we received an asynchronous instance we add a continuation
                if isinstance(self.result, Deferred):
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
            logger().warning('unknown event "%s" for %s', event, self)
        
    def fire_event(self, event, event_data=NOTHING, sender=None):
        """Dispatches ``event_data`` to the ``event`` listeners.
* If ``event`` is a one-time event, it makes sure that it was not fired before.
        
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
                logger().warning('Event "%s" already fired for %s', event, self)
        elif event in self.MANY_TIMES_EVENTS:
            for callback in self.MANY_TIMES_EVENTS[event]:
                callback(event_data)
        else:
            fired = False
            logger().warning('unknown event "%s" for %s', event, self)
        if fired:
            if sender is None:
                sender = getattr(event_data, '__class__', event_data) 
            events.fire(event, sender, data=event_data)
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
            safe_async(callback, result).add_errback(lambda f: f.log(
                            msg='Unhandled exception in "%s" event' % event))
            return result
        return _
            
                    
class Task(Deferred):
    '''A :class:`Task` is a :class:`Deferred` which consumes a
:ref:`coroutine <coroutine>`.
The callback will occur once the coroutine has finished
(when it raises StopIteration), or an unhandled exception occurs.
Instances of :class:`Task` are never
initialised directly, they are created by the :func:`maybe_async`
function when a generator is passed as argument.'''
    _waiting = None
    
    def __init__(self, gen, event_loop, canceller=None, timeout=None):
        self._gen = gen
        self._event_loop = event_loop
        self._canceller = canceller
        if timeout:
            self.set_timeout(timeout)
        self._consume(None)
    
    def _consume(self, result):
        step = self._step
        switch = False
        while self._state == _PENDING and not switch:
            result, switch = step(result, self._gen)
            
    def _step(self, result, gen):
        try:
            if isinstance(result, Failure):
                failure, result = result, None
                try:
                    result = failure.throw(gen)
                except StopIteration:
                    failure.logged = True
                    raise
                else:
                    failure.logged = True
            else:
                result = gen.send(result)
        except CoroutineReturn as e:
            result = e.value
            self._conclude(result)
        except StopIteration:
            self._conclude(result)
        except Exception as e:
            result = Failure(sys.exc_info())
            self._conclude(result)
        else:
            result = maybe_async(result, event_loop=self.event_loop)
            if isinstance(result, Deferred):
                # async result add callback/errorback and transfer control
                # to the event loop
                self._waiting = result.add_both(self._restart)
                return None, True 
            elif result == NOT_DONE:
                # transfer control to the event loop
                self.event_loop.call_soon(self._consume, None)
                return None, True
        return result, False
    
    def _restart(self, result):
        self._waiting = None
        #restart the coroutine in the same event loop it was started
        self.event_loop.call_now_threadsafe(self._consume, result)
        # Important, this is a callback of a deferred, therefore we return
        # the passed result (which is not asynchronous).
        return result
    
    def _conclude(self, result):
        # Conclude the generator and callback the listeners
        self._gen.close()
        del self._gen
        self.callback(result)
    
    def cancel(self, msg=''):
        if self._waiting:
            self._waiting.cancel(msg)
        else:
            super(Task, self).cancel(msg)
        
############################################################### MultiDeferred
class MultiDeferred(Deferred):
    '''A :class:`Deferred` for managing a ``collection`` of independent
asynchronous objects. The ``collection`` can be either a ``list`` or
a ``dict``.
The :class:`MultiDeferred` is recursive on values which are ``generators``,
``Mapping``, ``lists`` and ``sets``. It is not recursive on immutable
containers such as tuple and frozenset.

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
        self._failures = []
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
        if not isinstance(value, Deferred) and self.handle_value:
            try:
                val = self.handle_value(value)
            except Exception:
                value = Failure(sys.exc_info())
            else:
                if val is not value:
                    return self._add(key, val)
        self._setitem(key, value)
        # add callback if an asynchronous value
        if isinstance(value, Deferred):
            self._deferred[key] = value
            value.add_both(lambda result: self._deferred_done(key, result))

    def _make(self, value):
        md = self.__class__(value, raise_on_error=self.raise_on_error,
                            handle_value=self.handle_value)
        return maybe_async(md.lock())

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
            if self.log_failure:
                value.log()
            self._failures.append(value)
