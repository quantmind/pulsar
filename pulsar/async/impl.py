from multiprocessing import Process, Queue, current_process
from threading import Thread

from pulsar import system
from pulsar.utils.tools import gen_unique_id

from .proxy import ActorProxyMonitor


__all__ = ['Queue','IODummy']


class IODummy(system.EpollProxy):
    '''The polling mechanism for a task queue.
No select or epoll performed here, simply
return task from the queue if available.
This is an interface for using the same IOLoop class of other workers.'''
    def __init__(self):
        super(IODummy,self).__init__()
        self._fd = gen_unique_id()
        self._empty = []
    
    def fileno(self):
        return self._fd
    
    def poll(self, timeout = 0):
        return self._empty
    
    
class ActorImpl(object):
    
    def __init__(self, actor_class, impl, timeout, arbiter, args, kwargs):
        self.inbox = Queue()
        self.aid = gen_unique_id()
        self.impl = impl
        self.timeout = timeout
        self.actor_class = actor_class
        self.loglevel = kwargs.pop('loglevel',None)
        self.actor_functions = actor_class.actor_functions
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
        self.a_kwargs['arbiter'] = arbiter.proxy
        monitor = self.a_kwargs.pop('monitor',None)
        if monitor:
            monitor = monitor.proxy
        self.a_kwargs['monitor'] = monitor
        
    def make_actor(self):
        '''create an instance of :class:`pulsar.Actor`.'''
        self.actor = self.actor_class(self,*self.a_args,**self.a_kwargs)
        
    def get_ioimpl(self):
        return None
    
    
class ActorMonitorImpl(ActorImpl):
    '''This is a dummy actor implementation used to create Monitors.'''
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
    
    def __init__(self, *args):
        init_actor(self, Thread, *args)
        
    def run(self):
        run_actor(self)
        
    def get_ioimpl(self):
        return IODummy()
    
    def terminate(self):
        self.actor.stop()
    
    @property    
    def pid(self):
        return current_process().pid

