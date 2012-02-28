from multiprocessing import Process, current_process
from threading import Thread, current_thread

from pulsar import system, wrap_socket, platform, socket_pair
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.py2py3 import pickle

from .iostream import AsyncIOStream
from .proxy import ActorProxyMonitor


__all__ = ['ActorImpl','actor_impl']
    

def actor_impl(concurrency, actor_class, timeout, arbiter, aid, kwargs):
    if concurrency == 'monitor':
        return ActorMonitorImpl(concurrency,actor_class, timeout, arbiter, aid,
                                kwargs)
    elif concurrency == 'thread':
        return ActorThread(concurrency,actor_class, timeout, arbiter, aid,
                           kwargs)
    elif concurrency == 'process':
        return ActorProcess(concurrency, actor_class, timeout, arbiter, aid,
                            kwargs)
    else:
        raise ValueError('Concurrency {0} not supported by pulsar'\
                         .format(concurrency))
    
    
class ActorImpl(object):
    '''Actor implementation is responsible for the actual spawning of
actors according to a concurrency implementation. Instances are pickable
and are shared between the :class:`Actor` and its
:class:`ActorProxyMonitor`.

:parameter concurrency: string indicating the concurrency implementation.
    Valid choices are ``monitor``, ``process`` and ``thread``.
:parameter actor_class: :class:`Actor` or one of its subclasses.
:parameter timeout: timeout in seconds for the actor.
:parameter kwargs: additional key-valued arguments to be passed to the actor
    constructor.
'''
    def __init__(self, concurrency, actor_class, timeout, arbiter, aid, kwargs):
        if not aid:
            aid = gen_unique_id()[:8] if arbiter else 'arbiter'
        self.aid = aid
        self.impl = concurrency
        self.timeout = timeout
        self.actor_class = actor_class
        self.loglevel = kwargs.pop('loglevel',None)
        self.remotes = actor_class.remotes
        self.a_kwargs = kwargs
        self.process_actor(arbiter)
       
    @property
    def name(self):
        return '{0}({1})'.format(self.actor_class.code(),self.aid)
     
    def __str__(self):
        return self.name
    
    def proxy_monitor(self):
        return ActorProxyMonitor(self)
    
    def process_actor(self, arbiter):
        '''Called at initialization, it set up communication layers for the
actor. In particular here is where the outbox handler is created.'''
        monitor = self.a_kwargs.pop('monitor',None)
        self.a_kwargs.update({'arbiter':arbiter.proxy,
                              'monitor':monitor.proxy if monitor else None})
        
    def make_actor(self):
        '''create an instance of :class:`Actor`. For standard actors, this
function is called after forking, therefore in the new process
(or thread if using a concurrency based on threads).
For the :class:`Arbiter` and for :class:`Monitor` instances it is
called in the main process since those special actors always live in the
main process.'''
        self.actor = self.actor_class(self,**self.a_kwargs)
        ct = current_thread()
        if not hasattr(ct,'actor'):
            ct.actor = self.actor
    
    
class ActorMonitorImpl(ActorImpl):
    '''An actor implementation for Monitors. Monitors live in the main process
loop and therefore do not require an inbox.'''
    def process_actor(self, arbiter):
        self.a_kwargs['arbiter'] = arbiter
        self.timeout = 0
        self.make_actor()
        
    def proxy_monitor(self):
        return None
    
    def start(self):
        pass
    
    def is_active(self):
        return self.actor.is_alive()
    
    @property    
    def pid(self):
        return current_process().pid


def init_actor(self,Impl,*args):
    Impl.__init__(self)
    ActorImpl.__init__(self,*args)
    self.daemon = True
        
        
class ActorProcess(Process,ActorImpl):
    
    def __init__(self, *args):
        init_actor(self, Process, *args)
        
    def run(self):
        self.make_actor()
        self.actor.start()
        
        
class ActorThread(Thread,ActorImpl):
    '''Actor on a thread'''
    def __init__(self, *args):
        init_actor(self, Thread, *args)
        
    def run(self):
        self.make_actor()
        self.actor.start()
    
    def terminate(self):
        self.actor.stop()
    
    @property    
    def pid(self):
        return current_process().pid

