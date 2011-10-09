from multiprocessing import Process, current_process
from threading import Thread

from pulsar import system, wrap_socket, platform, socket_pair
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.py2py3 import pickle

from .iostream import AsyncIOStream
from .mailbox import mailbox, IOQueue, SocketServerMailbox
from .proxy import ActorProxyMonitor


__all__ = ['ActorImpl','actor_impl']
    

def actor_impl(concurrency, actor_class, timeout, arbiter, kwargs):
    if concurrency == 'monitor':
        return ActorMonitorImpl(concurrency,actor_class, timeout, arbiter,
                                kwargs)
    elif concurrency == 'thread':
        return ActorThread(concurrency,actor_class, timeout, arbiter, kwargs)
    elif concurrency == 'process':
        return ActorProcess(concurrency, actor_class, timeout, arbiter, kwargs)
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
    def __init__(self, concurrency, actor_class, timeout, arbiter, kwargs):
        self.aid = gen_unique_id()[:8] if arbiter else 'arbiter'
        self.impl = concurrency
        self.timeout = timeout
        self.actor_class = actor_class
        self.loglevel = kwargs.pop('loglevel',None)
        self.remotes = actor_class.remotes
        self.a_kwargs = kwargs
        self.inbox = self.get_inbox(arbiter,kwargs.get('monitor'))
        self.outbox = None
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
actor. In particular here is where the inbox and outbox handlers are created.
The outbox is either based on a socket or a pipe, while the inbox could be
a socket, a pipe or a queue.'''
        kwargs = self.a_kwargs
        monitor = kwargs.pop('monitor',None)
        if arbiter.inbox:
            self.outbox = mailbox(address = arbiter.inbox.address())
        if monitor:
            monitor = monitor.proxy
        kwargs.update({'arbiter':arbiter.proxy,
                       'monitor':monitor})
        
    def get_inbox(self, arbiter, monitor):
        '''Create the inbox :class:`Mailbox`.

:parameter arbiter: The :class:`Arbiter`
:parameter monitor: Optional instance of the :class:`Monitor` supervising
    the actor.
:rtype: an instance of :class:`Mailbox`

If a monitor is available, check if it has a task queue.
If so the mailbox will be based on the queue since the actor
won't have a select/epoll type ionput/output but one based on
:class:`IOQueue`.
'''
        ioq = self.a_kwargs.get('ioqueue')
        if ioq:
            return mailbox(id = 'inbox', queue = ioq)
        else:
            return SocketServerMailbox()
        
    def make_actor(self):
        '''create an instance of :class:`Actor`. For standard actors, this
function is called after forking, therefore in the new process
(or thread if using a concurrency based on threads).
For the :class:`Arbiter` and for :class:`Monitor` instances it is
called in the main process since those special actors always live in the
main process.'''
        self.actor = self.actor_class(self,**self.a_kwargs)
    
    
class ActorMonitorImpl(ActorImpl):
    '''An actor implementation for Monitors. Monitors live in the main process
loop and therefore do not require an inbox.'''
    def process_actor(self, arbiter):
        self.a_kwargs['arbiter'] = arbiter
        self.timeout = 0
        self.make_actor()
        
    def get_inbox(self, arbiter, monitor):
        if not arbiter:
            # This is the arbiter implementation
            return SocketServerMailbox()
        
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

