from multiprocessing import Process, Queue, current_process
from threading import Thread

from pulsar import system
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.ioqueue import IOQueue

from .proxy import ActorProxyMonitor


__all__ = ['ActorImpl','Queue']
    
    
class ActorImpl(object):
    '''Actor implementation is responsible for the actual spawning of
actors according to a concurrency implementation.

:parameter actor_class: a :class:`Actor` or one of its subclasses.
:parameter impl: string indicating the concurrency implementation. Valid choices
    are ``process`` and ``thread``.
:parameter timeout: timeout in seconds for the actor.
:parameter args: additional arguments to be passed to the arbiter constructor.
:parameter kwargs: additional key-valued arguments to be passed to the arbiter
    constructor.
'''
    def __init__(self, actor_class, impl, timeout, arbiter, args, kwargs):
        self.inbox = Queue()
        self.aid = gen_unique_id()[:8]
        self.impl = impl
        self.timeout = timeout
        self.actor_class = actor_class
        self.loglevel = kwargs.pop('loglevel',None)
        self.remotes = actor_class.remotes
        self.a_args = args
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
        kwargs = self.a_kwargs
        monitor = kwargs.pop('monitor',None)
        if monitor:
            monitor = monitor.proxy
        kwargs.update({'arbiter':arbiter.proxy,
                       'arbiter_address':arbiter.address,
                       'monitor':monitor})
        
    def make_actor(self):
        '''create an instance of :class:`Actor`. For standard actors, this
function is called after forking, therefore in the new process
(or thread if using a concurrency based on threads).
For the :class:`Arbiter` and for :class:`Monitor` instances it is
called in the main process since those special actors always live in the
main process.'''
        self.actor = self.actor_class(self,*self.a_args,**self.a_kwargs)
        
    def get_io(self, actor):
        '''Create a Input/Output object used in the :class:`IOLoop` instance
of the actor. By default return None so that the default system implementation
will be used.

:parameter actor: instance of :class:`Actor`.
:rtype: An ``epoll``-like object used as the edge and level trigger polling
    element in the *actor* :class:`IOLoop` instance.'''
        return None
    
    
class ActorMonitorImpl(ActorImpl):
    '''A dummy actor implementation to create Monitors.'''
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
    
    
def run_actor(self):
    self.make_actor()
    self.actor.start()
        
        
class ActorProcess(Process,ActorImpl):
    
    def __init__(self, *args):
        init_actor(self, Process, *args)
        
    def run(self):
        run_actor(self)
        
        
class ActorThread(Thread,ActorImpl):
    '''Actor on a thread'''
    def __init__(self, *args):
        init_actor(self, Thread, *args)
        
    def run(self):
        run_actor(self)
        
    def get_io(self, worker):
        '''Actors on a thread by default do not use select or epoll, instead
 they use a queue where the monitor add tasks to be consumed.'''
        tq = worker.task_queue
        if tq:
            ioq = IOQueue(tq)
            return ioq
    
    def terminate(self):
        self.actor.stop()
    
    @property    
    def pid(self):
        return current_process().pid

