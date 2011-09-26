import threading


class Synchronized(object):
    
    @property
    def lock(self):
        if not hasattr(self,'_lock'):
            self._lock = threading.Lock()
        return self._lock
    
    @classmethod
    def make(cls, f):
        """Synchronization decorator for Synchronized member functions. """
    
        def _(self, *args, **kw):
            self.lock.acquire()
            try:
                return f(self, *args, **kw)
            finally:
                self.lock.release()
                
        _.thread_safe = True
        _.__doc__ = f.__doc__
        
        return _