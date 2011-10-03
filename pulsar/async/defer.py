'''\
A lightweight deferred module inspired by twisted.
'''
import sys
from copy import copy
from inspect import isgenerator
from time import sleep, time
try:
    import queue
except ImportError:
    import Queue as queue
ThreadQueue = queue.Queue

from pulsar import AlreadyCalledError, Timeout, NOT_DONE
from pulsar.utils.mixins import Synchronized


__all__ = ['Deferred',
           'Failure',
           'DeferredGenerator',
           'is_stack_trace',
           'is_async',
           'async_func_call',
           'make_async',
           'simple_callback',
           'ThreadQueue']


class Failure(object):
    
    def __init__(self, err = None):
        if isinstance(err,self.__class__):
            self.traces = copy(err.traces)
        else:
            self.traces = []
            self.append(err)
    
    def append(self, trace):
        if trace:
            if isinstance(trace,self.__class__):
                self.traces.extend(trace.traces)
            else:
                self.traces.append(trace)
        return self
            
    def __len__(self):
        return len(self.traces)
    
    def __iter__(self):
        return iter(self.traces)
    
    def log(self, log):
        for e in self:
            log.critical('', exc_info = e)
        
        
def is_stack_trace(data):
    if isinstance(data,Failure):
        return True
    elif isinstance(data,tuple) and len(data) == 3:
        return True
    return False
    
    
def is_async(obj):
    return isinstance(obj,Deferred)


def async_func_call(func, result, *args, **kwargs):
    callback = lambda : func(*args,**kwargs)
    if is_async(result):
        return result.add_callback(callback)
    else:
        return callback()

async_value = lambda value : lambda result : value 
    

def make_async(val = None):
    '''Convert *val* into an asyncronous object which accept callbacks.

:parameter val: can be a generator or any other value.
:rtype: a :class:`Deferred` instance.'''
    if not is_async(val):
        if isgenerator(val):
            return DeferredGenerator(val)
        else:
            d = Deferred()
            d.callback(val)
            return d
    else:
        return val 


def simple_callback(func, *args, **kwargs):
    '''Wrap a function which does not include the callback
result as argument. Raise exceptions if result is one.'''
    def _(result, *args, **kwargs):
        if isinstance(result,Exception):
            raise result
        else:
            func(*args,**kwargs)
    
    return _


class Deferred(Synchronized):
    """This is a callback which will be put off until later. The idea is the same
as the ``twisted.defer.Deferred`` object.

Use this class to return from functions which otherwise would block the
program execution. Instead, it should return a Deferred."""
    def __init__(self, rid = None):
        self._called = False
        self.paused = 0
        self.rid = rid
        self._ioloop = None
        self._callbacks = []
    
    def set_actor(self, actor):
        pass
    
    @property
    def called(self):
        return self._called
    
    def pause(self):
        """Stop processing until :meth:`unpause` is called.
        """
        self.paused += 1


    def unpause(self):
        """
        Process all callbacks made since L{pause}() was called.
        """
        self.paused -= 1
        if self.paused:
            return
        if self.called:
            self._run_callbacks()
    
    def add_callback(self, callback):
        """Add a callback as a callable function.
The function takes at most one argument, the result passed to the
:meth:`callback` method."""
        self._callbacks.append(callback)
        self._run_callbacks()
        return self
        
    def _run_callbacks(self):
        if self._called and self._callbacks:
            callbacks = self._callbacks
            while callbacks:
                callback = callbacks.pop(0)
                try:
                    self._runningCallbacks = True
                    try:
                        self.result = callback(self.result)
                    finally:
                        self._runningCallbacks = False
                    if isinstance(self.result, Deferred):
                        # Add a pause and add new callback
                        self.pause()
                        self.result.add_callback(self._continue)
                except Exception as e:
                    self.result = e
                
        return self
    
    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result,*args,**kwargs))
        
    def _continue(self, result):
        self.result = result
        self.unpause()
    
    def callback(self, result = None):
        '''Run registered callbacks with the given *result*.
This can only be run once. Later calls to this will raise
:class:`pulsar.AlreadyCalledError`. If further callbacks are added after
this point, add_callback will run the *callbacks* immediately.'''
        if isinstance(result,Deferred):
            raise ValueError('Received a deferred instance from\
 callback function')
        if self.called:
            raise AlreadyCalledError
        self.result = result
        self._called = True
        return self._run_callbacks()
        
    def wait(self, timeout = 1):
        '''Wait until result is available'''
        while not self.called:
            sleep(timeout)
        if isinstance(self.result,Deferred):
            return self.result.wait(timeout)
        else:
            return self.result

    def start(self, ioloop, timeout = None):
        '''Start running the deferred into an Event loop.
If the deferred was already started do nothing.

:parameter ioloop: :class:`IOLoop` instance where to run the deferred.
:parameter timeout: Optional timeout in seconds. If the deferred has not done within
    this time period it will raise a :class:`pulsar.Timeout` exception.
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
                    raise Timeout('Timeout {0} reached without results.\
 Aborting.'.format(self._timeout))
                self._ioloop.add_callback(self._consume)
            except:
                self.callback(sys.exc_info())
        
            
class DeferredGenerator(Deferred):
    '''A :class:`Deferred` for a generator (iterable) over deferred.
The callback will occur once the generator has stopped
(when it raises StopIteration).

:parameter gen: a generator or iterable.
:parameter max_errors: The maximum number of exceptions allowed before stopping.
    Default ``None``, no limit.'''
    def __init__(self, gen, max_errors = None):
        self.gen = gen
        self.max_errors = max_errors
        super(DeferredGenerator,self).__init__()
        
    def on_start(self):
        self._consumed = 0
        self._last_result = None
        self._errors = Failure()
        
    @Synchronized.make
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
            except StopIteration:
                break
            except Exception as e:
                consume = not self._should_stop(sys.exc_info())
            else:
                if result == NOT_DONE:
                    return self._ioloop.add_callback(self._consume)
                else:
                    d = make_async(result)
                    if d.called:
                        consume = not self._should_stop(d.result)
                    else:
                        return d.add_callback(self._resume).start(self._ioloop,
                                                                  self._timeout)
        
        if consume:
            if self._errors:
                self.callback(self._errors)
            else:
                self.callback(self._last_result)
    
    def _resume(self, result = None):
        if not self._should_stop(result):
            self._consume()
            
    def _should_stop(self, trace):
        if is_stack_trace(trace):
            self._errors.append(trace)
            if self.max_errors and len(self._errors) >= self.max_errors:
                self.callback(self._errors)
                return True
        else:
            self._last_result = trace
            
            
    