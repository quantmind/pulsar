import threading
import logging
from threading import current_thread
from multiprocessing import current_process

from pulsar.utils.pep import get_event_loop_policy

__all__ = ['get_request_loop',
           'get_actor',
           'is_mainthread',
           'process_local_data',
           'thread_local_data',
           'logger',
           'NOTHING']

LOGGER = logging.getLogger('pulsar')
NOTHING = object()

def is_mainthread(thread=None):
    '''Check if thread is the main thread. If ``thread`` is not supplied check
the current thread.'''
    thread = thread if thread is not None else current_thread() 
    return isinstance(thread, threading._MainThread)

def get_request_loop():
    return get_event_loop_policy().get_request_loop()

def logger(event_loop=None):
    return getattr(event_loop or get_request_loop(), 'logger', LOGGER)

def process_local_data(name=None):
    '''Fetch the current process local data dictionary. If *name* is not
``None`` it returns the value at *name*, otherwise it return the process data
dictionary.'''
    ct = current_process()
    if not hasattr(ct, '_pulsar_local'):
        ct._pulsar_local = ProcessLocal()
    loc = ct._pulsar_local
    if name:
        return getattr(loc, name, None)
    else:
        return loc
            
def thread_local_data(name, value=NOTHING, ct=None):
    '''Set or retrieve an attribute *name* from the curren thread. If *value*
is None, it will get the value otherwise it will set the value.'''
    ct = ct or current_thread()
    if is_mainthread(ct):
        loc = process_local_data()
    elif not hasattr(ct, '_pulsar_local'):
        ct._pulsar_local = threading.local()
        loc = ct._pulsar_local
    else:
        loc = ct._pulsar_local
    if value is not NOTHING:
        if hasattr(loc, name):
            if getattr(loc, name) is not value:
                raise RuntimeError(
                            '%s is already available on this thread' % name)
        else:
            setattr(loc, name, value)
    return getattr(loc, name, None)

get_actor = lambda: thread_local_data('actor')

def set_actor(actor):
    '''Set and returns the actor running the current thread.'''
    actor = thread_local_data('actor', value=actor)
    #if actor.impl.kind == 'thread':
    #    process_local_data('thread_actors')[actor.aid] = actor
    return actor

def remove_actor(actor):
    '''Remove actor from threaded_actors dictionary'''
    if actor.impl.kind == 'thread':
        LOGGER.debug('Removing threaded actor %s' % actor)
        #process_local_data('thread_actors').pop(actor.aid, None)
        
        
class plocal(object):
    pass
        
class ProcessLocal(object):
    #def __init__(self):
    #    self.thread_actors = {}
        
    def local(self):
        return plocal()