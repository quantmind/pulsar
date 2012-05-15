'''A deferred module with almost the same API as twisted.
'''
import sys
from copy import copy
import logging
import traceback
from collections import deque
from inspect import isgenerator, isfunction, ismethod, istraceback
from time import sleep, time
from collections import namedtuple

from pulsar import AlreadyCalledError, DeferredFailure, NOT_DONE
from pulsar.utils.py2py3 import raise_error_trace, map


__all__ = ['Deferred',
           'MultiDeferred',
           'Failure',
           'as_failure',
           'is_failure',
           'is_async',
           'maybe_async',
           #'async_func_call',
           'make_async',
           'safe_async',
           'raise_failure']


logger = logging.getLogger('pulsar.async.defer')

remote_stacktrace = namedtuple('remote_stacktrace', 'error_class error trace')


def is_stack_trace(trace):
    if isinstance(trace,remote_stacktrace):
        return True
    elif isinstance(trace,tuple) and len(trace) == 3:
        return istraceback(trace[2]) or\
                 (trace[2] is None and isinstance(trace[1],trace[0]))
    return False


class Failure(object):
    '''Aggregate failures during :class:`Deferred` callbacks.
    
.. attribute:: traces

    List of (``errorType``, ``errvalue``, ``traceback``) occured during
    the execution of a :class:`Deferred`.
    
'''
    def __init__(self, err=None):
        self.should_stop = False
        if isinstance(err,self.__class__):
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
    
    
def update_failure(f):
    '''If *f* is an instance of :class:`Failure` add the current
 ``sys.exc_info`` otherwuise return a new :class:`Failure` with current
 ``sys.exc_info``.'''
    if not isinstance(f,Failure):
        f = Failure()
    return f.append(sys.exc_info())
    
    
def is_failure(data):
    if isinstance(data,Failure):
        return True
    else:
        return is_stack_trace(data)
    
    
def as_failure(data):
    if isinstance(data, Failure):
        return data
    elif is_stack_trace(data):
        return Failure(data)
    elif isinstance(data, Exception):
        exc_info = sys.exc_info()
        if data == exc_info[1]:
            return Failure(exc_info)
        else:
            return Failure((data.__class__, data, None))
    else:
        return data
    

def raise_failure(result):
    '''Utility callback function which stop execution of callbacks on failure
and raise errors.'''
    if isinstance(result,Failure):
        result.should_stop = True
    return result

        
def is_async(obj):
    return isinstance(obj, Deferred)


def async_func_call(func, result, *args, **kwargs):
    callback = lambda : func(*args,**kwargs)
    if is_async(result):
        return result.add_callback(callback)
    else:
        return callback()

async_value = lambda value : lambda result : value
 
pass_through = lambda result: result

def maybe_async(val=None, description=None):
    if isgenerator(val):
        return DeferredGenerator(val, description=description)
    else:
        return val
    
def make_async(val=None, description=None):
    '''Convert *val* into an :class:`Deferred` asynchronous instance
so that callbacks can be attached to it.

:parameter val: can be a generator or any other value. If a generator, a
    :class:`DeferredGenerator` instance will be returned.
:rtype: a :class:`Deferred` instance.

This function is useful when someone whants to treat a value as a deferred::

    v = ...
    make_async(v).add_callback(...)
    
'''
    if not is_async(val):
        if isgenerator(val):
            return DeferredGenerator(val, description=description)
        else:
            d = Deferred(description=description)
            d.callback(val)
            return d
    else:
        return val


def safe_async(f):
    try:
        result = f()
    except Exception as e:
        result = e
    return make_async(result)


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
    _ioloop = None
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
    
    def pause(self):
        """Stop processing until :meth:`unpause` is called.
        """
        self.paused += 1

    def unpause(self):
        """Process all callbacks made since :meth:`pause` was called.
        """
        self.paused -= 1
        self._run_callbacks()
    
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
        
    def addBoth(self, callback):
        return self.add_callback(callback, callback)
    
    def _run_callbacks(self):
        if not self.called or self._runningCallbacks:
            return
        
        if self.paused:
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
                    self.pause()
                    # Add a callback to the result to resume callbacks
                    self.result.add_callback(self._continue)
                    break
    
    def _add_exception(self, e):
        if not isinstance(self.result, Failure):
            self.result = Failure()
        else:
            self.result.add(e)
                    
    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result,*args,**kwargs))
        
    def _continue(self, result):
        self.result = result
        self.unpause()
        return self.result
    
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
        if self.called:
            raise AlreadyCalledError('Deferred %s already called'.format(self))
        self.result = as_failure(result)
        self._called = True
        self._run_callbacks()
        return self.result
        
    def wait(self, timeout = 1):
        '''Wait until *timeout* for a result to be available'''
        if not self.called:
            sleep(timeout)
            if not self.called:
                raise DeferredFailure('Deferred not called')
        return self.result
            

class DeferredGenerator(Deferred):
    '''A :class:`Deferred` for a generator (iterable) over deferred.
The callback will occur once the generator has stopped
(when it raises StopIteration).

:parameter gen: a generator or iterable.
:parameter max_errors: The maximum number of exceptions allowed before stopping.
    Default ``None``, no limit.'''
    def __init__(self, gen, description=None):
        self.gen = gen
        self._consumed = 0
        self.errors = Failure()
        self.deferred = Deferred()
        super(DeferredGenerator,self).__init__(description=description)
        self._consume()
        
    def _consume(self, last_result=None):
        '''override the deferred consume private method for handling the
generator.'''
        if isinstance(last_result, Failure):
            self.errors.append(last_result)
        try:
            result = next(self.gen)
            self._consumed += 1
        except KeyboardInterrupt:
            raise
        except StopIteration:
            result = last_result if not self.errors else self.errors
            self.callback(result)
        except Exception as e:
            self.errors.append(e)
            self._consume(None)
        else:
            if result == NOT_DONE:
                result = Deferred()
            else:
                result = maybe_async(result)
            if is_async(result):
                return result.addBoth(self._consume)
            else:
                self._consume(result)
    

class MultiDeferred(Deferred):
    
    def __init__(self, type=list):
        self._locked = False
        self._deferred = {}
        self._stream = type()
        super(MultiDeferred, self).__init__()
        
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
        for key, value in iterdata(stream):
            add(key, value)
        
    def _add(self, key, value):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot add a dependent once locked.')
        if isinstance(value, Deferred):
            if value.called:
                value = value.result
            else:
                self._add_deferred(key, value)
        else:
            if isgenerator(value):
                value = list(value)
            if isinstance(value, (dict,list,tuple,set,frozenset)):
                if isinstance(value,dict):
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