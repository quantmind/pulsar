import threading
from threading import Thread, current_thread
from multiprocessing import current_process

__all__ = ['thread_loop',
           'thread_ioloop',
           'get_actor',
           'set_local_data',
           'is_mainthread',
           'PulsarThread',
           'process_local_data',
           'thread_local_data']

def is_mainthread(thread=None):
    '''Check if thread is the main thread. If *thread* is not supplied check
the current thread'''
    thread = thread if thread is not None else current_thread() 
    return isinstance(thread, threading._MainThread)

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
            
def thread_local_data(name, value=None):
    ct = current_thread()
    if is_mainthread(ct):
        loc = process_local_data()
    elif not hasattr(ct,'_pulsar_local'):
        ct._pulsar_local = threading.local()
        loc = ct._pulsar_local
    else:
        loc = ct._pulsar_local
    if value is not None:
        if hasattr(loc, name):
            if getattr(loc, name) is not value:
                raise RuntimeError(
                            '%s is already available on this thread' % name)
        else:
            setattr(loc, name, value)
    return getattr(loc, name, None)

    
def thread_loop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('eventloop', ioloop)

def thread_ioloop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('ioloop', ioloop)

get_actor = lambda: thread_local_data('actor')

def set_actor(actor):
    '''Returns the actor running the current thread.'''
    actor = thread_local_data('actor', value=actor)
    if actor.impl == 'thread':
        process_local_data('thread_actors')[actor.aid] = actor
    return actor

def get_actor_from_id(aid):
    '''Retrieve an actor from its actor id. This function can be used by
actors with thread concurrency ince they live in the arbiter process domain.'''
    actors = process_local_data('thread_actors')
    if actors:
        return actors.get(aid)

def set_local_data(actor):
    set_actor(actor)
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
        
class ProcessLocal(object):
    def __init__(self):
        self.thread_actors = {}
        
    def local(self):
        return plocal()