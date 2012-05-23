'''A deferred module with almost the same API as twisted.'''
import sys
from copy import copy
import logging
import traceback
from threading import current_thread, local
from collections import deque, namedtuple
from inspect import isgenerator, isfunction, ismethod, istraceback
from time import sleep

from pulsar import AlreadyCalledError, DeferredFailure


__all__ = ['Deferred',
           'MultiDeferred',
           'Failure',
           'as_failure',
           'is_failure',
           'is_async',
           'maybe_async',
           'make_async',
           'safe_async',
           'ispy3k',
           'thread_loop',
           'thread_local_data',
           'NOT_DONE']

# Special objects
class NOT_DONE(object):
    pass

def thread_local_data(name, value=None):
    ct = current_thread()
    if not hasattr(ct,'_pulsar_local'):
        ct._pulsar_local = local()
    loc = ct._pulsar_local
    if value is not None:
        if hasattr(loc, name):
            raise RuntimeError('%s is already available on this thread'%name)
        setattr(loc, name, value)
    return getattr(loc, name)

def thread_loop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('eventloop', ioloop)

logger = logging.getLogger('pulsar.async.defer')

remote_stacktrace = namedtuple('remote_stacktrace', 'error_class error trace')

pass_through = lambda result: result

ispy3k = sys.version_info >= (3, 0)
if ispy3k:
    import pickle
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
    def raise_error_trace(err,traceback):
        raise err.with_traceback(traceback)
    range = range
else:   # pragma : nocover
    import cPickle as pickle
    from pulsar.utils._py2 import *
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
    range = xrange
    
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
    if isinstance(trace,remote_stacktrace):
        return True
    elif isinstance(trace,tuple) and len(trace) == 3:
        return istraceback(trace[2]) or\
                 (trace[2] is None and isinstance(trace[1],trace[0]))
    return False    
    
def update_failure(f):
    '''If *f* is an instance of :class:`Failure` add the current
 ``sys.exc_info`` otherwuise return a new :class:`Failure` with current
 ``sys.exc_info``.'''
    if not isinstance(f,Failure):
        f = Failure()
    return f.append(sys.exc_info())
    
def is_failure(value):
    return isinstance(value, Failure)
    
def as_failure(value):
    '''Convert *value* into a :class:`Failure` if it is a stack trace or an
exception, otherwise returns *value*.'''
    if isinstance(value, Exception):
        exc_info = sys.exc_info()
        if value == exc_info[1]:
            return Failure(exc_info)
        else:
            return Failure((value.__class__, value, None))
    elif is_stack_trace(value):
        return Failure(value)
    else:
        return value
    
def is_async(obj):
    '''Check if *obj* is an asynchronous instance'''
    return isinstance(obj, Deferred)

def async_func_call(func, result, *args, **kwargs):
    callback = lambda : func(*args,**kwargs)
    if is_async(result):
        return result.add_callback(callback)
    else:
        return callback()

def safe_async(f, args=(), description=None, max_errors=None):
    try:
        result = f(*args)
    except Exception as e:
        result = e
    return make_async(result, max_errors=max_errors, description=description)

def maybe_async(val, description=None, max_errors=None):
    '''Convert *val* into an asynchronous instance only if *val* is a generator
or a function.'''
    if isgenerator(val):
        return DeferredGenerator(val, max_errors=max_errors,
                                 description=description)
    elif isfunction(val) or ismethod(val):
        return safe_async(val)
    else:
        return val
    
def make_async(val=None, description=None, max_errors=None):
    '''Convert *val* into an :class:`Deferred` asynchronous instance
so that callbacks can be attached to it.

:parameter val: can be a generator or any other value. If a generator, a
    :class:`DeferredGenerator` instance will be returned.
:parameter max_errors: the maximum number of errors tolerated if *val* is
    a generator. Default `None`.
:rtype: a :class:`Deferred` instance.

This function is useful when someone needs to treat a value as a deferred::

    v = ...
    make_async(v).add_callback(...)
    
'''
    if not is_async(val):
        if isgenerator(val):
            return DeferredGenerator(val, max_errors=max_errors,
                                     description=description)
        else:
            d = Deferred(description=description)
            d.callback(val)
            return d
    else:
        if description:
            val._description = description
        return val


############################################################### Failure
class Failure(object):
    '''Aggregate failures during :class:`Deferred` callbacks.
    
.. attribute:: traces

    List of (``errorType``, ``errvalue``, ``traceback``) occured during
    the execution of a :class:`Deferred`.
    
'''
    def __init__(self, err=None):
        self.should_stop = False
        if isinstance(err, self.__class__):
            self.traces = copy(err.traces)
        else:
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
    
    def raise_all(self):
        if self.traces and isinstance(self.traces[-1][1],Exception):
            eclass, error, trace = self.traces.pop()
            self.log()
            raise_error_trace(error,trace)
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
                
    def log(self, log = None):
        log = log or logger
        for e in self:
            log.critical('', exc_info = e)
            

############################################################### Deferred            
class Deferred(object):
    """This is a callback which will be put off until later.
The idea is the same as the ``twisted.defer.Deferred`` object.

Use this class to return from functions which otherwise would block the
program execution. Instead, it should return a Deferred.

.. attribute:: called

    ``True`` if the deferred was called. In this case the asynchronous result
    is ready and available in the attr:`result`.
    
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
:meth:`callback` method."""
        errback = errback if errback is not None else pass_through
        if hasattr(callback,'__call__') and hasattr(errback,'__call__'):
            self._callbacks.append((callback, errback))
            self._run_callbacks()
        else:
            raise TypeError('callback must be callable')
        return self
    
    def add_errback(self, errback):
        return self.add_callback(pass_through, errback)
        
    def addBoth(self, callback):
        return self.add_callback(callback, callback)
                    
    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result,*args,**kwargs))
    
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
        elif self.called:
            raise AlreadyCalledError('Deferred %s already called'.format(self))
        self.result = as_failure(result)
        self._called = True
        self._run_callbacks()
        return self.result
        
    def result_or_self(self):
        '''Obtain the result if available, otherwise it returns self.'''
        return self if not self.called else self.result
        
    def wait(self, timeout = 1):
        '''Wait until *timeout* for a result to be available'''
        if not self.called:
            sleep(timeout)
            if not self.called:
                raise DeferredFailure('Deferred not called')
        return self.result
    
    ##################################################    INTERNAL METHODS
    def _run_callbacks(self):
        if not self.called or self._runningCallbacks or self.paused:
            return
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            callback = callbacks[isinstance(self.result, Failure)]
            try:
                self._runningCallbacks = True
                try:
                    self.result = callback(self.result)
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
    '''A :class:`Deferred` for a generator (iterable) over deferred.
The callback will occur once the generator has stopped
(when it raises StopIteration).

:parameter gen: a generator or iterable.
:parameter max_errors: The maximum number of exceptions allowed before
    stopping the generator and raise exceptions. By default the
    generator will continue regardless of errors, and raise them at the
    end (if any).'''
    def __init__(self, gen, max_errors=None, description=None):
        self.gen = gen
        self.max_errors = max(1, max_errors) if max_errors else 0
        self._consumed = 0
        self.errors = Failure()
        self.deferred = Deferred()
        super(DeferredGenerator,self).__init__(description=description)
        self._genvalue = self._consume()
    
    #def result_or_self(self):
    #    if self.called:
    #        return self.result
    #    else:
    #        return self._genvalue
        
    def _consume(self, last_result=None):
        '''override the deferred consume private method for handling the
generator.'''
        if isinstance(last_result, Failure):
            if self.should_stop(last_result):
                return self.conclude()
        try:
            result = next(self.gen)
            self._consumed += 1
        except KeyboardInterrupt:
            raise
        except StopIteration:
            return self.conclude(last_result)
        except Exception as e:
            if self.should_stop(e):
                return self.conclude()
            return self._consume()
        else:
            if result == NOT_DONE:
                # The NOT_DONE element indicate that we need to abort the
                # generator and add a callback to the eventloop to resume the
                # loop at the next iteration. Return self so that it can
                # restart all its ancestors.
                ioloop = thread_loop()
                ioloop.add_callback(self._consume, wake=False)
                return self
            else:
                # Convert to async only if needed
                result = maybe_async(result)
            if is_async(result):
                result = result.result_or_self()
                if is_async(result):
                    return result.addBoth(self._consume)
            # continue with the loop
            return self._consume(result)
    
    def should_stop(self, failure):
        self.errors.append(failure)
        return self.max_errors and len(self.errors) >= self.max_errors
        
    def conclude(self, last_result=None):
        result = last_result if not self.errors else self.errors
        return self.callback(result)
    

############################################################### MultiDeferred
class MultiDeferred(Deferred):
    _locked = False
    
    def __init__(self, type=list):
        self._deferred = {}
        self._stream = type()
        if self.type not in ('list','dict'):
            raise TypeError('Multideferred type container must be a dictionary '
                            ' or a string')
        super(MultiDeferred, self).__init__()
        
    @property
    def type(self):
        return self._stream.__class__.__name__
    
    def lock(self):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                        ' cannot be locked twice.')
        self._locked = True
        if not self._deferred:
            self._finish()
        return self
    
    def update(self, stream):
        add = self._add
        for key, value in iterdata(stream, len(self._stream)):
            add(key, value)
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
        if is_async(value):
            value = value.result_or_self()
        if is_async(value):
            self._add_deferred(key, value)
        else:
            if is_generalised_generator(value):
                value = list(value)
            if isinstance(value, (dict,list,tuple,set,frozenset)):
                if isinstance(value, dict):
                    md = MultiDeferred(type=dict)
                else:
                    md = MultiDeferred()
                md.update(value)
                md.lock()
                value = md
                if value.called:
                    value = value.result
                else:
                    self._add_deferred(key, value)
        self._setitem(key, value)
                    
    def _add_deferred(self, key, value):
        self._deferred[key] = value
        value.add_callback_args(self._deferred_done, key)
        
    def _deferred_done(self, result, key):
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
        if self.called:
            raise RuntimeError(self.__class__.__name__ +\
                               ' done before finishing.')
        self.callback(self._stream)
        
    def _setitem(self, key, value):
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value