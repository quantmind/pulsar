import sys
import inspect
from time import sleep

from pulsar import AlreadyCalledError


__all__ = ['Deferred',
           'async',
           'is_async',
           'make_async',
           'make_deferred',
           'simple_callback']


def is_async(obj):
    return isinstance(obj,Deferred)

async_value = lambda value : lambda result : value 

def make_deferred(val = None):
    if not is_async(val):
        d = Deferred()
        d.callback(val)
        return d
    else:
        return val
    

def make_async(val = None):
    '''Convert ``val`` into an asyncronous object which accept callbacks'''
    if not is_async(val):
        if inspect.isgenerator(val):
            d = make_async() 
            for v in val:
                if is_async(v):
                    dv = make_async(v)
                else:
                    d.add_callback(async_value(v))
            return d
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


def async(o, *args, **kwargs):
    '''Transform ``o`` into a Deferred instance'''       
    if hasattr(o,'__call__'):
        def _(*args, **kwargs):
            res = o(*args, **kwargs)
            return make_async(res)
        
        return _
    else:
        return make_async(o)


class Deferred(object):
    """
    This is a callback which will be put off until later. The idea is the same
    as twisted.defer.Deferred object.

    Use this class to return from functions which otherwise would block the
    program execution. Instead, it should return a Deferred.
    """
    def __init__(self, rid = None):
        self._called = False
        self.paused = 0
        self.rid = rid
        self._callbacks = []
    
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
        """Add a callback as a callable function. The function takes one argument,
the result of the callback.
        """
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
                        self.pause()
                        self.result.add_callback(self._continue)
                except Exception as e:
                    self.result = callback(e)
                
        return self
    
    def _continue(self, result):
        self.result = result
        self.unpause()
    
    def callback(self, result):
        if isinstance(result,Deferred):
            raise ValueError('Received a deferred instance from callback function')
        if self.called:
            raise AlreadyCalledError
        self.result = result
        self._called = True
        self._run_callbacks()
        
    def wait(self, timeout = 1):
        '''Wait until result is available'''
        while not self.called:
            sleep(timeout)
        if isinstance(self.result,Deferred):
            return self.result.wait(timeout)
        else:
            return self.result

    
