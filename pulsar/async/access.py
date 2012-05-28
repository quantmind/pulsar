import threading
from threading import Thread, current_thread
from multiprocessing import current_process

__all__ = ['thread_loop',
           'thread_ioloop',
           'get_actor',
           'set_local_data',
           'is_mainthread',
           'PulsarThread',
           'thread_local_data']

def is_mainthread(thread=None):
    '''Check if thread is the main thread. If *thread* is not supplied check
the current thread'''
    thread = thread if thread is not None else current_thread() 
    return isinstance(thread, threading._MainThread)

def thread_local_data(name, value=None):
    ct = current_thread()
    if is_mainthread(ct):
        ct = current_process()
        if not hasattr(ct, '_pulsar_local'):
            ct._pulsar_local = plocal()
    elif not hasattr(ct,'_pulsar_local'):
        ct._pulsar_local = threading.local()
    loc = ct._pulsar_local
    if value is not None:
        if hasattr(loc, name):
            raise RuntimeError('%s is already available on this thread'%name)
        setattr(loc, name, value)
    return getattr(loc, name, None)

def thread_loop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('eventloop', ioloop)

def thread_ioloop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('ioloop', ioloop)

def get_actor(value=None):
    '''Returns the actor running the current thread.'''
    return thread_local_data('actor', value=value)

def set_local_data(actor):
    get_actor(actor)
    thread_loop(actor.requestloop)
    thread_ioloop(actor.ioloop)
    
    
class PulsarThread(Thread):
    
    def __init__(self, *args, **kwargs):
        self.actor = get_actor()
        super(PulsarThread, self).__init__(*args, **kwargs)
        
    def run(self):
        set_local_data(self.actor)
        super(PulsarThread, self).run()
        
        
class plocal(object):
    pass