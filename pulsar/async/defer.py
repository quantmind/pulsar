'''\
A lightweight deferred module inspired by twisted.
'''
import sys
from copy import copy
import logging
import traceback
from inspect import isgenerator, isfunction, ismethod, istraceback
from time import sleep, time
from collections import namedtuple

try:    #pragma nocover
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue

from pulsar import AlreadyCalledError, DeferredFailure, Timeout,\
                     NOT_DONE, CLEAR_ERRORS
from pulsar.utils.py2py3 import raise_error_trace, map


__all__ = ['Deferred',
           'MultiDeferred',
           'Failure',
           'DeferredGenerator',
           'SafeAsync',
           'as_failure',
           'is_failure',
           'is_async',
           'async_pair',
           #'async_func_call',
           'make_async',
           'raise_failure',
           'simple_callback',
           'ThreadQueue']


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
    def __init__(self, err = None):
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
            if isinstance(trace,self.__class__):
                self.traces.extend(trace.traces)
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
    if isinstance(data,Failure):
        return data
    elif is_stack_trace(data):
        return Failure(data)
    elif isinstance(data,Exception):
        exc_info = sys.exc_info()
        if data == exc_info[1]:
            return Failure(exc_info)
        else:
            return Failure((data.__class__,data,None))
    

def raise_failure(result):
    '''Utility callback function which stop execution of callbacks on failure
and raise errors.'''
    if isinstance(result,Failure):
        result.should_stop = True
    return result

        
def is_async(obj):
    return isinstance(obj,Deferred)


def async_func_call(func, result, *args, **kwargs):
    callback = lambda : func(*args,**kwargs)
    if is_async(result):
        return result.add_callback(callback)
    else:
        return callback()

async_value = lambda value : lambda result : value 
    

def make_async(val = None, max_errors = None, description = None):
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
            return DeferredGenerator(val,max_errors,description=description)
        else:
            d = Deferred(description=description)
            d.callback(val)
            return d
    else:
        return val 
    

class SafeAsync(object):
    '''A callable class for running function on a remote actor.'''
    def __init__(self, max_errors = None):
        self.max_errors = max_errors
        
    def _call(self, actor):
        raise NotImplemented
    
    def __call__(self, actor):
        try:
            res = self._call(actor)
        except:
            res = Failure(sys.exc_info())
        return make_async(res, max_errors = self.max_errors)
        
        
def async_pair(val, max_errors = None):
    '''Convert *val* into an asynchronous pair or a function returning an
asynchronous pair wich is a two element tuple with the first element being
an asynchronous representation of *val* and the second a :class:`Deferred`
which is called back once *val* is ready.

:parameter val: A function or an object.
:parameter max_errors: maximum number of errors allowed.
    Default ``None``.
'''
    if isfunction(val) or ismethod(val):
        def _(*args, **kwargs):
            try:
                r = val(*args, **kwargs)
            except:
                r = Failure(err = sys.exc_info())
            d = Deferred()
            r = make_async(r,max_errors=max_errors).add_callback(d.callback)
            return r,d
        
        return _
    
    elif val is None:
        return None
    
    else:
        d = Deferred()
        r = make_async(val,max_errors=max_errors).add_callback(d.callback)
        return r,d


def simple_callback(func, *args, **kwargs):
    '''Wrap a function which does not include the callback
result as argument. Raise exceptions if result is one.'''
    def _(result, *args, **kwargs):
        if isinstance(result,Exception):
            raise result
        else:
            func(*args,**kwargs)
    
    return _


class Deferred(object):
    """This is a callback which will be put off until later.
The idea is the same as the ``twisted.defer.Deferred`` object.

Use this class to return from functions which otherwise would block the
program execution. Instead, it should return a Deferred.

.. attribute:: called

    ``True`` if the deferred was called. In this case the asynchronous result
    is ready and available in the attr:`result`.
    
"""
    def __init__(self, rid = None, description = None):
        self._called = False
        self._description = description
        self._runningCallbacks = False
        self.paused = 0
        self.rid = rid
        self._ioloop = None
        self._callbacks = []
    
    def set_actor(self, actor):
        pass
    
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
    
    def add_callback(self, callback, raise_on_error = False):
        """Add a callback as a callable function.
The function takes at most one argument, the result passed to the
:meth:`callback` method."""
        if hasattr(callback,'__call__'):
            self._callbacks.append(callback)
            if raise_on_error:
                self._callbacks.append(raise_failure)
            self._run_callbacks()
        return self
        
    def _run_callbacks(self):
        if not self.called or self._runningCallbacks:
            return
        
        if not self.paused:        
            while self._callbacks:
                if isinstance(self.result ,Failure):
                    if self.result.should_stop:
                        self.result.raise_all()
                callback = self._callbacks.pop(0)
                try:
                    self._runningCallbacks = True
                    try:
                        self.result = callback(self.result)
                    finally:
                        self._runningCallbacks = False
                    if isinstance(self.result, Deferred):
                        # Add a pause
                        self.pause()
                        # Add a callback to the result to resume callbacks
                        self.result.add_callback(self._continue)
                        break
                except:
                    self.result = update_failure(self.result)
            
        if isinstance(self.result,Failure):
            if self.result.should_stop:
                self.result.raise_all()
    
    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result,*args,**kwargs))
        
    def _continue(self, result):
        self.result = result
        self.unpause()
        return self.result
    
    def callback(self, result = None):
        '''Run registered callbacks with the given *result*.
This can only be run once. Later calls to this will raise
:class:`AlreadyCalledError`. If further callbacks are added after
this point, :meth:`add_callback` will run the *callbacks* immediately.

:return: the *result* input parameter
'''
        #TODO, this is a hack, not working properly yet, but required as
        #un-handled exception stall the asynchronous engine
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        if self.called:
            raise AlreadyCalledError('already called')
        self.result = as_failure(result) or result
        self._called = True
        self._run_callbacks()
        return self.result
        
    def is_failure(self):
        '''return ``True`` if the result is a failure. If the result is not
ready it throws a :class:`DeferredFailure` exception'''
        if not self.called:
            raise DeferredFailure('Deferred not called')
        return is_failure(self.result)
    
    def wait(self, timeout = 1):
        '''Wait until *timeout* for a result to be available'''
        if not self.called:
            sleep(timeout)
            if not self.called:
                raise DeferredFailure('Deferred not called')
        return self.result

    def start(self, ioloop, timeout = 5):
        '''Start running the deferred into an Event loop.
If the deferred was already started do nothing.

:parameter ioloop: :class:`IOLoop` instance where to run the deferred.
:parameter timeout: Optional timeout in seconds. If the deferred has not
    been called within this time period it will raise a :class:`Timeout`
    exception and it won't add itself to the callbacks.
    
    Default: 5
    
:rtype: ``self``.

A common usage pattern::

    def blocking_function():
        ...
        
    def callback(result):
        ...
        
    make_async(blocking_function).start(ioloop).add_callback(callback)
'''
        if not self._ioloop:
            self._ioloop = ioloop
            self._timeout = timeout
            self._started = time()
            self.on_start()
            self._consume()
        return self
            
    def on_start(self):
        '''Callback just before being added to the event loop
in :meth:`start`.'''
        pass
    
    def _consume(self):
        if not self.called:
            try:
                if self._timeout and time() - self._started > self._timeout:
                    raise Timeout('Deferred "{0}" not called.'.format(self),
                                  self._timeout)
                self._ioloop.add_callback(self._consume)
            except:
                self.callback(sys.exc_info())
        

class MultiDeferred(Deferred):
    '''A :class:`Deferred` which depends on multiple other :class:`Deferred`.'''
    def __init__(self, **kwargs):
        self._underlyings = set()
        self._results = []
        self._locked = False
        super(MultiDeferred,self).__init__(**kwargs)
    
    def lock(self):
        if self._locked:
            raise RuntimeError(
                        'MultiDeferred cannot be locked twice.')
        self._locked = True
        if not self._underlyings:
            self._finish()
        return self
            
    def add(self, d):
        if not isinstance(d, Deferred):
            raise ValueError('Expected a Deferred received %s' % d)
        if self._locked:
            raise RuntimeError(
                        'MultiDeferred cannot add a dependent once locked.')
        self._results.append(d)
        if d not in self._underlyings:
            self._underlyings.add(d)
            d.add_callback_args(self._underlying_done, d)
        return self
    
    def update(self, deferred):
        '''Update the multideferred with an iterable over :class:`Deferred`'''
        for d in deferred:
            self.add(d)
        return self
    
    def _underlying_done(self, result, d):
        self._underlyings.remove(d)
        if self._locked and not self._underlyings and not self.called:
            self._finish()
        return result
    
    def _finish(self):
        if not self._locked:
            raise RuntimeError('MultiDeferred cannot finish until completed.')
        if self._underlyings:
            raise RuntimeError('MultiDeferred cannot finish whilst waiting for '
                               'dependents %r' % self._underlyings)
        if self.called:
            raise RuntimeError('MultiDeferred done before finishing.')
        result = [r.result for r in self._results]
        return self.callback(result)            
        
        
class DeferredGenerator(Deferred):
    '''A :class:`Deferred` for a generator (iterable) over deferred.
The callback will occur once the generator has stopped
(when it raises StopIteration).

:parameter gen: a generator or iterable.
:parameter max_errors: The maximum number of exceptions allowed before stopping.
    Default ``None``, no limit.'''
    def __init__(self, gen, max_errors = None, description = None):
        self.gen = gen
        self.max_errors = max_errors
        super(DeferredGenerator,self).__init__(description=description)
        
    def on_start(self):
        self._consumed = 0
        self._last_result = None
        self._errors = Failure()
        
    def next(self):
        return next(self.gen)
    
    def _consume(self):
        '''override the deferred consume private method for handling the
generator.'''
        consume = True
        
        while consume:
            try:
                result = self.next()
                self._consumed += 1
            except KeyboardInterrupt:
                raise
            except StopIteration:
                break
            except Exception as e:
                consume = not self._should_stop(sys.exc_info())
            else:
                if result == NOT_DONE:
                    return self._ioloop.add_callback(self._consume)
                elif result == CLEAR_ERRORS:
                    self._errors = Failure()
                else:
                    d = make_async(result, description = self._description)
                    if d.called:
                        # the deferred was paused
                        if d.paused:
                            d = d.result
                        else:
                            consume = not self._should_stop(d.result)
                            continue
                    # the deferred is not ready.
                    return d.add_callback(self._resume)\
                                .start(self._ioloop, self._timeout)
        
        if consume:
            if self._errors:
                self.callback(self._errors)
            else:
                self.callback(self._last_result)
    
    def _resume(self, result = None):
        '''Callback to restart the generator. If the result is an error
and the generator should stop, return the errors so that callbacks
can be chained. Otherwise keep consuming.'''
        if not self._should_stop(result):
            self._consume()
        else:
            return self._errors
    _resume.description = 'Callback to resume a DeferredGenerator'
            
    def _should_stop(self, result):
        if is_failure(result):
            self._errors.append(result)
            if self.max_errors and len(self._errors) >= self.max_errors:
                self.callback(self._errors)
                return True
        else:
            self._last_result = result
            if result == CLEAR_ERRORS:
                self._errors = Failure()
            
            