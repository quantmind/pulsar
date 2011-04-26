from multiprocessing import Process
from threading import Thread

from pulsar import system
from pulsar.utils.tools import gen_unique_id


class IODummy(system.EpollProxy):
    '''The polling mechanism for a task queue. No select or epoll performed here, simply
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
    
    def proxy_monitor(self, actor):
        return actor.proxy_monitor(impl = self)
    
    def process_actor(self, actor, args, kwargs):
        kwargs['arbiter'] = kwargs['arbiter'].proxy()
        monitor = kwargs.pop('monitor',None)
        if monitor:
            monitor = monitor.proxy()
        kwargs['monitor'] = monitor        
        
    @classmethod
    def get_ioimpl(cls):
        return None
    
    
class ActorMonitorImpl(ActorImpl):
    
    def __init__(self, actor, args, kwargs):
        self.actor = actor
        actor._timeout = 0
        actor._init(*args, **kwargs)
        
    def proxy_monitor(self, actor):
        return None
    
    def start(self):
        pass
    
    def is_active(self):
        return self.actor.is_alive()

        
class ActorProcess(Process,ActorImpl):
    
    def __init__(self, actor, args, kwargs):
        self.process_actor(actor,args,kwargs)
        super(ActorProcess,self).__init__(target = actor.start, name = actor.name, args = args, kwargs = kwargs)
        self.daemon = True
        

class ActorThread(Thread,ActorImpl):
    
    def __init__(self, actor, args, kwargs):
        self.process_actor(actor,args,kwargs)
        self.actor = actor
        super(ActorThread,self).__init__(target = actor.start, name = actor.name, args = args, kwargs = kwargs)
        self.daemon = True
        
    @classmethod
    def get_ioimpl(cls):
        return IODummy()
    
    def terminate(self):
        self.actor.stop()

