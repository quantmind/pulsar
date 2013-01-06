'''A deferred module with almost the same API as twisted.'''
import sys
from copy import copy
import logging
import traceback
from threading import current_thread, local
from collections import deque, namedtuple
from inspect import isgenerator, isfunction, ismethod, istraceback
from time import sleep

from pulsar import AlreadyCalledError, DeferredFailure, HaltServer

from .access import thread_loop

EXIT_EXCEPTIONS = (KeyboardInterrupt, SystemExit, HaltServer)

__all__ = ['Deferred',
           'MultiDeferred',
           'DeferredGenerator',
           'Failure',
           'as_failure',
           'is_failure',
           'raise_failure',
           'log_failure',
           'is_async',
           'maybe_async',
           'make_async',
           'safe_async',
           'async',
           'multi_async',
           'ispy3k',
           'NOT_DONE',
           'STOP_ON_FAILURE',
           'EXIT_EXCEPTIONS',
           'CLEAR_ERRORS']

ispy3k = sys.version_info >= (3, 0)
if ispy3k:
    import pickle
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
    def raise_error_trace(err, traceback):
        if istraceback(traceback):
            raise err.with_traceback(traceback)
        else:
            raise err
    range = range
    can_generate = lambda gen: hasattr(gen, '__next__')
else:   # pragma : nocover
    import cPickle as pickle
    from pulsar.utils._py2 import *
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
    range = xrange
    can_generate = lambda gen: hasattr(gen, 'next')

# Special objects
class NOT_DONE(object):
    pass

class STOP_ON_FAILURE(object):
    pass

class CLEAR_ERRORS(object):
    pass

EMPTY_DICT = {}

logger = logging.getLogger('pulsar.async.defer')

remote_stacktrace = namedtuple('remote_stacktrace', 'error_class error trace')

pass_through = lambda result: result

def iterdata(stream, start=0):
    '''Iterate over a stream which is either a dictionary or a list. This
iterator is over key-value pairs for a dictionary, and index-value pairs
for a list.'''
    if isinstance(stream, dict):
        return iteritems(stream)
    else:
        return enumerate(stream, start)

def is_generalised_generator(value):
    '''Check if *value* is a generator. This is more general than the
inspect.isgenerator function.'''
    return hasattr(value, '__iter__') and not hasattr(value, '__len__')

def is_stack_trace(trace):
    if isinstance(trace, remote_stacktrace):
        return True
    elif isinstance(trace,tuple) and len(trace) == 3:
        return istraceback(trace[2]) or\
                 (trace[2] is None and isinstance(trace[1],trace[0]))
    return False

def is_failure(value):
    return isinstance(value, Failure)

def as_failure(value, msg=None):
    '''Convert *value* into a :class:`Failure` if it is a stack trace or an
exception, otherwise returns *value*.'''
    if isinstance(value, Exception):
        exc_info = sys.exc_info()
        if value == exc_info[1]:
            return Failure(exc_info, msg)
        else:
            return Failure((value.__class__, value, None), msg)
    elif is_stack_trace(value):
        return Failure(value, msg)
    else:
        return value

def is_async(obj):
    '''Check if *obj* is an asynchronous instance'''
    return isinstance(obj, Deferred)

def maybe_async(val, description=None, max_errors=None):
    '''Convert *val* into an asynchronous instance only if *val* is a generator
or a function. If *val* is a :class:`Deferred` it checks if it has been
called and all callbacks have been consumed.
In this case it returns the :attr:`Deferred.result` attribute.'''
    if isgenerator(val):
        val = DeferredGenerator(val, max_errors=max_errors,
                                description=description)
    if isfunction(val) or ismethod(val):
        try:
            val = maybe_async(val())
        except:
            val = sys.exc_info()
    if is_async(val):
        return val.result_or_self()
    else:
        return as_failure(val)

def make_async(val=None, description=None, max_errors=None):
    '''Convert *val* into an :class:`Deferred` asynchronous instance
so that callbacks can be attached to it.

:parameter val: can be a generator or any other value. If a generator, a
    :class:`DeferredGenerator` instance will be returned.
:parameter max_errors: the maximum number of errors tolerated if *val* is
    a generator. Default `None`.
:return: a :class:`Deferred` instance.

This function is useful when someone needs to treat a value as a deferred::

    v = ...
    make_async(v).add_callback(...)

'''
    val = maybe_async(val, description, max_errors)
    if not is_async(val):
        d = Deferred(description=description)
        d.callback(val)
        return d
    else:
        return val
    
def safe_async(f, args=None, kwargs=None, description=None, max_errors=None):
    '''Execute function *f* safely and **always** returns an asynchronous
result.

:parameter f: function to execute
:parameter args: tuple of positional arguments for *f*.
:parameter kwargs: dictionary of key-word parameters for *f*.
:parameter description: Optional description for the :class:`Deferred` returned.
:parameter max_errors: the maximum number of errors tolerated if a :class:`DeferredGenerator`
    is returned.
:return: a :class:`Deferred` instance.
'''
    try:
        kwargs = kwargs if kwargs is not None else EMPTY_DICT
        args = args or ()
        result = f(*args, **kwargs)
    except:
        result = sys.exc_info()
    return make_async(result, max_errors=max_errors, description=description)

def log_failure(failure):
    '''Log the *failure* if *failure* is a :class:`Failure`.'''
    if is_failure(failure):
        failure.log()
    return failure

############################################################### DECORATORS
class async:
    '''A decorator class which transforms a function into
an asynchronous callable.
    
:parameter max_errors: The maximum number of errors permitted if the
    asynchronous value is a :class:`DeferredGenerator`.
:parameter description: optional description.

Typical usage::

    @async()
    def myfunction(...):
        ...
'''
    def __init__(self, max_errors=None, description=None):
         self.max_errors = max_errors
         self.description = description or 'async decorator for '

    def __call__(self, func):
        description = '%s%s' % (self.description, func.__name__)
        def _(*args, **kwargs):
            return safe_async(func, args=args, kwargs=kwargs,
                              max_errors=self.max_errors,
                              description=description)
        _.__name__ = func.__name__
        _.__doc__ = func.__doc__
        return _

def multi_async(func):
    '''Decorator for a function *func* which returns an iterable over, possibly
asynchronous, values. This decorator create an instance of a
:class:`MultiDeferred` called once all asynchronous values have been caled.'''
    def _(*args, **kwargs):
        try:
            return MultiDeferred(func(*args, **kwargs), type=list).lock()
        except Exception as e:
            return make_async(e)
    _.__name__ = func.__name__
    _.__doc__ = func.__doc__
    return _

def raise_failure(f):
    '''Decorator for raising failures'''
    def _(*args, **kwargs):
        r = f(*args, **kwargs)
        if is_failure(r):
            r.raise_all()
        return r
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _

############################################################### FAILURE
class Failure(object):
    '''Aggregate failures during :class:`Deferred` callbacks.

.. attribute:: traces

    List of (``errorType``, ``errvalue``, ``traceback``) occured during
    the execution of a :class:`Deferred`.

'''
    logged = False
    def __init__(self, err=None, msg=None):
        self.should_stop = False
        self.msg = msg or ''
        self.traces = []
        self.append(err)

    def __repr__(self):
        return '\n\n'.join(self.format_all())
    __str__ = __repr__

    def append(self, trace):
        '''Add new failure to self.'''
        if trace:
            if isinstance(trace, Failure):
                self.traces.extend(trace.traces)
            elif isinstance(trace, Exception):
                self.traces.append(sys.exc_info())
            elif is_stack_trace(trace):
                self.traces.append(trace)
        return self

    def clear(self):
        self.traces = []

    def format_all(self):
        for exctype, value, tb in self:
            if istraceback(tb):
                tb = traceback.format_exception(exctype, value, tb)
            if tb:
                yield '\n'.join(tb)
            else:
                yield str(value)

    def __getstate__(self):
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
        if self.traces and isinstance(self.traces[pos][1],Exception):
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

    @property
    def trace(self):
        if self.traces:
            return self.traces[-1]
        else:
            return (None,None,None)

    def log(self, log=None):
        if not self.logged:
            self.logged = True
            log = log or logger
            for e in self:
                log.critical(self.msg, exc_info=e)

############################################################### Deferred
class Deferred(object):
    """The main class of the pulsar asynchronous tools.
It is a callback which will be put off until later.
The implementation is very similar to the ``twisted.defer.Deferred`` object.

.. attribute:: called

    ``True`` if the deferred was called. In this case the asynchronous result
    is ready and available in the :attr:`result`.

.. attribute:: running

    ``True`` if the deferred is running callbacks.
    
.. attribute:: paused

    Integer indicating the number of times this :class:`Deferred` has been
    paused because the result of a callback was another :class::`Deferred`.

.. attribute:: result

    This is available once the :class:`Deferred` has been called back. Note,
    this can be anything, including another :class:`Deferred`. Trying to access
    this attribute when :attr:`called` is ``False`` will result in an
    ``AttributeError`` exception.
"""
    paused = 0
    _called = False
    _runningCallbacks = False

    def __init__(self, description=None):
        self._description = description
        self._callbacks = deque()

    def __repr__(self):
        v = self._description or self.__class__.__name__
        if self.called:
            v += ' (called)'
        return v

    def __str__(self):
        return self. __repr__()

    @property
    def called(self):
        return self._called

    @property
    def running(self):
        return self._runningCallbacks

    def add_callback(self, callback, errback=None):
        """Add a callback as a callable function.
The function takes at most one argument, the result passed to the
:meth:`callback` method. If the *errback* callable is provided it will
be called when an exception occurs."""
        errback = errback if errback is not None else pass_through
        if hasattr(callback, '__call__') and hasattr(errback, '__call__'):
            self._callbacks.append((callback, errback))
            self._run_callbacks()
        else:
            raise TypeError('callback must be callable')
        return self

    def add_errback(self, errback):
        '''Same as :meth:`add_callback` but only for errors.'''
        return self.add_callback(pass_through, errback)

    def addBoth(self, callback):
        '''Equivalent to `self.add_callback(callback, callback)`.'''
        return self.add_callback(callback, callback)

    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result, *args, **kwargs))

    def callback(self, result=None):
        '''Run registered callbacks with the given *result*.
This can only be run once. Later calls to this will raise
:class:`AlreadyCalledError`. If further callbacks are added after
this point, :meth:`add_callback` will run the *callbacks* immediately.

:return: the *result* input parameter
'''
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        elif self._called:
            raise AlreadyCalledError('Deferred %s already called' % self)
        self.result = as_failure(result)
        self._called = True
        self._run_callbacks()
        return self.result

    def result_or_self(self):
        '''It returns the :attr:`result` only if available and all
callbacks have been consumed, otherwise it returns this :class:`Deferred`.
Users should use this method to obtain the result, rather than accessing
directly the :attr:`result` attribute.'''
        return self.result if self._called and not self._callbacks else self

    ##################################################    INTERNAL METHODS
    def _run_callbacks(self):
        if not self._called or self._runningCallbacks or self.paused:
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
                if isinstance(self.result, Deferred):
                    # Add a pause
                    self._pause()
                    # Add a callback to the result to resume callbacks
                    self.result.addBoth(self._continue)
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


class DeferredGenerator(Deferred):
    '''A :class:`Deferred` for a generator over, possibly, deferred objects.
The callback will occur once the generator has stopped
(when it raises StopIteration), or a preset maximum number of errors has
occurred.

:parameter gen: a generator.
:parameter max_errors: The maximum number of exceptions allowed before
    stopping the generator and raise exceptions. By default the
    generator will continue regardless of errors, accumulating them into
    the final result.
'''
    def __init__(self, gen, max_errors=None, description=None):
        self.gen = gen
        if not can_generate(gen):
            raise TypeError('DeferredGenerator requires an iterable')
        self.max_errors = max(1, max_errors) if max_errors else 0
        self._consumed = 0
        self.errors = Failure()
        super(DeferredGenerator,self).__init__(description=description)
        self.loop = thread_loop()
        self._consume()

    def _resume_in_thread(self, result=None):
        # When the generator finds an asynchronous object still waiting
        # for results, it adds this callback to resume the generator at the
        # next iteration in the eventloop.
        if self.loop.tid != current_thread().ident:
            # Generators are not thread-safe. If the callback is on a different
            # thread we restart the generator on the original thread
            # by adding a callback in the generator eventloop.
            self.loop.add_callback(lambda: self._consume(result))
        else:
            self._consume(result)
        # IMPORTANT! We return the original result.
        # Otherwise we just keep adding deferred objects to the callbacks.
        return result

    def _consume(self, last_result=None):
        '''override the deferred consume private method for handling the
generator. Important! Callbacks are always added to the event loop on the
current thread.'''
        if isinstance(last_result, Failure):
            if self.should_stop(last_result):
                return self.conclude()
        try:
            result = next(self.gen)
            self._consumed += 1
        except EXIT_EXCEPTIONS:
            raise
        except StopIteration as e:
            # The generator has finished producing data
            return self.conclude(last_result)
        except Exception as e:
            if self.should_stop(e):
                return self.conclude()
            return self._consume()
        else:
            if result == NOT_DONE:
                # The NOT_DONE object indicates that the generator needs to
                # abort so that the event loop can continue. This generator
                # will resume at the next event loop.
                self.loop.add_callback(self._consume, wake=False)
                return self
            result = maybe_async(result)
            if is_async(result):
                # The result is asynchronous and it is not ready yet.
                # We pause the generator and attach a callback to continue
                # on the same thread.
                return result.addBoth(self._resume_in_thread)
            if result == CLEAR_ERRORS:
                self.errors.clear()
                result = None
            # continue with the loop
            return self._consume(result)

    def should_stop(self, failure):
        self.errors.append(failure)
        return self.max_errors and len(self.errors) >= self.max_errors

    def conclude(self, last_result=None):
        # Conclude the generator and callback the listeners
        result = last_result if not self.errors else self.errors
        self.gen = None
        self.errors = None
        #log_failure(result)
        return self.callback(result)


############################################################### MultiDeferred
class MultiDeferred(Deferred):
    '''A :class:`Deferred` for managing a stream if independent objects
which may be :class:`Deferred`.

.. attribute:: lock

    If ``True`` items can no longer be added to this :class:`MultiDeferred`.
    
.. attribute:: type

    The type of multideferred. Either a ``list`` or a ``dict``.
'''
    _locked = False

    def __init__(self, data=None, type=None, fireOnOneErrback=False,
                 handle_value=None):
        self._deferred = {}
        self._failures = Failure()
        self.fireOnOneErrback = fireOnOneErrback
        self.handle_value = handle_value
        if not type:
            type = data.__class__ if data is not None else list
        if not issubclass(type, (list, dict)):
            type = list
        self._stream = type()
        super(MultiDeferred, self).__init__()
        if data:
            self.update(data)

    @property
    def locked(self):
        return self._locked
        
    @property
    def type(self):
        return self._stream.__class__.__name__

    def lock(self):
        '''Lock the :class:`MultiDeferred` so that no new items can be added.
If it was alread :attr:`locked` a runtime exception is raised.'''
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                        ' cannot be locked twice.')
        self._locked = True
        if not self._deferred:
            self._finish()
        return self

    def update(self, stream):
        '''Update the :class:`MultiDeferred` with new data. It works for
both ``list`` and ``dict`` types.'''
        add = self._add
        try:
            for key, value in iterdata(stream, len(self._stream)):
                add(key, value)
        except:
            raise
        return self

    def append(self, value):
        '''Append only works for a list type multideferred'''
        if self.type == 'list':
            self._add(len(self._stream), value)
        else:
            raise RuntimeError('Cannot append a value to a "dict" type '
                               'multideferred')

    def _add(self, key, value):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot add a dependent once locked.')
        if is_generalised_generator(value):
            value = list(value)
        value = maybe_async(value)
        if isinstance(value, (dict, list, tuple, set, frozenset)):
            value = self._make(value)    
        if not is_async(value) and self.handle_value:
            try:
                val = self.handle_value(value)
            except Exception as e:
                value = as_failure(e)
            else:
                if val is not value:
                    return self._add(key, val)
        self._setitem(key, value)
        # add callback if an asynchronous value
        if is_async(value):
            self._add_deferred(key, value)

    def _make(self, value):
        md = self.__class__(value, fireOnOneErrback=self.fireOnOneErrback,
                            handle_value=self.handle_value)
        return maybe_async(md.lock())

    def _add_deferred(self, key, value):
        self._deferred[key] = value
        value.addBoth(lambda result: self._deferred_done(key, result))

    def _deferred_done(self, key, result):
        self._deferred.pop(key, None)
        self._setitem(key, result)
        if self._locked and not self._deferred and not self.called:
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
        if self.fireOnOneErrback and self._failures:
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
            self._failures.append(value)
