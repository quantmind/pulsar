import sys
import os
from time import time
from multiprocessing import Process, reduction, current_process
from multiprocessing.queues import Queue, Empty
from threading import Thread, current_thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   NotRegisteredWithServer, PickableMixin,\
                   ActorAlreadyStarted,\
                   logerror, LogSelf, LogginMixin
from pulsar.utils.tools import gen_unique_id
from pulsar.http import get_httplib
from pulsar.utils.py2py3 import iteritems, itervalues


from .defer import Deferred
from .eventloop import IOLoop
from .impl import ActorProcess, ActorThread, ActorMonitorImpl


__all__ = ['is_actor',
           'Actor',
           'ActorRequest',
           'ActorProxy']


EMPTY_TUPLE = ()
EMPTY_DICT = {}
STD_MESSAGE = '__message__'



def is_actor(obj):
    return isinstance(obj,Actor)


def get_proxy(obj):
    if is_actor(obj):
        return obj.proxy()
    elif isinstance(obj,ActorProxy):
        return obj
    else:
        raise ValueError('"{0}" is not a remote or remote proxy.'.format(obj))


class HttpMixin(object):
    
    @property
    def http(self):
        return get_httplib(self.cfg)
    
    

class ActorRequest(Deferred):
    REQUESTS = {}
    
    def __init__(self, actor, name, ack, msg):
        super(ActorRequest,self).__init__(rid = gen_unique_id())
        if hasattr(actor,'aid'):
            actor = actor.aid
        self.aid = actor
        self.name = name
        self.msg = msg
        self.ack = ack
        if self.ack:
            self.REQUESTS[self.rid] = self
        
    def __str__(self):
        return '{0} - {1}'.format(self.name,self.rid[:8])
    
    def __repr__(self):
        return self.__str__()
    
    def __getstate__(self):
        #Remove the list of callbacks
        d = self.__dict__.copy()
        d['_callbacks'] = []
        return d
        
    @classmethod
    def actor_callback(cls, rid, result):
        r = cls.REQUESTS.pop(rid,None)
        if r:
            r.callback(result)
            

class ActorProxyRequest(object):
    __slots__ = ('caller','_func_name','ack')
    
    def __init__(self, caller, name, ack):
        self.caller = caller
        self._func_name = name
        self.ack = ack
        
    def __repr__(self):
        return '{0} calling "{1}"'.format(self.caller,self._func_name)
        
    def __call__(self, *args, **kwargs):
        if len(args) == 0:
            actor = self.caller
        else:
            actor = args[0]
            args = args[1:]
        ser = self.serialize
        args = tuple((ser(a) for a in args))
        kwargs = dict((k,ser(a)) for k,a in iteritems(kwargs))
        actor = get_proxy(actor)
        return actor.send(self.caller.aid,(args,kwargs), name = self._func_name, ack = self.ack)
    
    def serialize(self, obj):
        if is_actor(obj):
            return obj.proxy()
        else:
            return obj


class ActorProxy(object):
    '''A proxy for a :class:`pulsar.utils.async.RemoteMixin` instance.
This is a light class which delegates function calls to a remote object.

.. attribute: proxyid

    Unique ID for the remote object
    
.. attribute: remotes

    dictionary of remote functions names with value indicationg if the
    remote function will acknowledge the call.
'''
    __slots__ = ('aid','remotes','inbox','timeout')
     
    def __init__(self, aid, inbox, remotes, timeout):
        self.aid = aid
        self.remotes = remotes
        self.inbox = inbox
        self.timeout = timeout
        
    def send(self, aid, msg, name = None, ack = False):
        '''Send a message to another Actor'''
        name = name or STD_MESSAGE
        request = ActorRequest(aid,name,ack,msg)
        try:
            self.inbox.put(request)
            return request
        except Exception as e:
            pass
        
    def __repr__(self):
        return self.aid[:8]
    
    def __str__(self):
        return self.__repr__()
    
    def __eq__(self, o):
        return is_actor(o) and self.aid == o.aid
    
    def __ne__(self, o):
        return not is_actor(o) or self.aid != o.aid 
    
    def __getstate__(self):
        '''Because of the __getattr__ implementation,
we need to manually implement the pcikling and unpickling of thes object.'''
        return (self.aid,self.remotes,self.inbox)
    
    def __setstate__(self, state):
        self.aid,self.remotes,self.inbox = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return ActorProxyRequest(self, name, ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))
           

class ActorProxyMonitor(ActorProxy):
    
    def __init__(self, aid, inbox, remotes, timeout, actor, impl):
        self.actor = actor
        self.impl = impl
        self.notified = time()
        self.stopping = 0
        super(ActorProxyMonitor,self).__init__(aid, inbox, remotes, timeout)
        
    def is_alive(self):
        return self.impl.is_alive()
        
    def terminate(self):
        self.impl.terminate()
            
    def __str__(self):
        return self.actor.__str__()
    

class ActorMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(ActorMetaClass, cls).__new__
        fprefix = 'actor_'
        attrib  = '{0}functions'.format(fprefix)
        cont = {}
        for key, method in attrs.items():
            if hasattr(method,'__call__') and key.startswith(fprefix):
                meth_name = key[len(fprefix):]
                ack = getattr(method,'ack',True)
                cont[meth_name] = ack
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs[attrib] = cont
        return make(cls, name, bases, attrs)

    
ActorBase = ActorMetaClass('BaseActor',(object,),{})


class Actor(ActorBase,LogginMixin,HttpMixin):
    '''A Remote server mixin to be used as Base class
for python :class:`multiprocessing.Process` or
or :class:`threading.Thread` classes. For example::

    class MyProcessServer(ProcessWithRemote,Process):
        _done = False
        
        def _run(self):
            while not self._done:
                self.flush()
                time.sleep(0.1)
        
        def _stop(self):
            self._done = True
'''
    INITIAL = 0X0
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3
    status = {0x0:'not started',
              0x1:'started',
              0x2:'closed',
              0x3:'terminated'}
    INBOX_TIMEOUT = 0.02
    DEFAULT_IMPLEMENTATION = 'process'
    _stopping = False
    _runner_impl = {'monitor':ActorMonitorImpl,
                    'thread':ActorThread,
                    'process':ActorProcess}
    
    def __init__(self):
        self._impl = None
        
    @property
    def aid(self):
        '''Actor unique identifier'''
        return self._aid
    
    @property
    def ppid(self):
        '''Parent process id.'''
        return self._ppid
    
    @property
    def impl(self):
        return self._impl
    
    @property
    def timeout(self):
        return self._timeout
    
    @property
    def pid(self):
        return os.getpid()
    
    @property
    def tid(self):
        return self.current_thread().name
    
    @property
    def name(self):
        return '{0}({1})'.format(self.class_code,self.aid[:8])
    
    @property
    def inbox(self):
        return self._inbox
    
    def proxy(self):
        return ActorProxy(self._aid,self._inbox,self.actor_functions,self.timeout)
    
    def proxy_monitor(self, impl = None):
        return ActorProxyMonitor(self._aid,self._inbox,self.actor_functions,self.timeout,actor=self,impl=impl)
    
    def __reduce__(self):
        return (ActorProxy,(self._aid,self._inbox,self.remotes,self.timeout))
    
    # HOOKS
    
    def on_start(self,*args,**kwargs):
        pass
    
    def on_task(self):
        '''Callback in the actor event loop'''
        pass
    
    def on_stop(self):
        '''Called before exiting an actor'''
        pass
    
    def on_exit(self):
        '''Called once the actor is exting'''
        pass
    
    def on_manage_actor(self, actor):
        pass
    
    # INITIALIZATION AFTER FORKING
    def _init(self, arbiter = None, monitor = None):
        self.arbiter = arbiter
        self.monitor = monitor
        self._state = self.INITIAL
        self.log = self.getLogger()
        self._linked_actors = {}
        self.ioloop = self._get_eventloop()
        self.ioloop.add_loop_task(self)
    
    def start(self,*args,**kwargs):
        if not hasattr(self,'_state'):
            self._init(arbiter = kwargs.pop('arbiter'), monitor = kwargs.pop('monitor'))
            #self.link(self.arbiter)
        if self._state == self.INITIAL:
            on_task = kwargs.pop('on_task',None)
            if on_task:
                self.on_task = on_task
            self.on_start(*args,**kwargs)
            self._state = self.RUN
            self._run()
            return self
        else:
            raise ActorAlreadyStarted('Already started')
    
    def _get_eventloop(self):
        impl_cls = self._runner_impl.get(self.impl,None)
        ioimpl = None if not impl_cls else impl_cls.get_ioimpl()
        return IOLoop(impl = ioimpl, logger = LogSelf(self,self.log))
    
    def link(self, actor):
        self._linked_actors[actor.aid] = LinkedActor(actor)
        
    def is_alive(self):
        return self._state == self.RUN
    
    def started(self):
        return self._state >= self.RUN
    
    def stopped(self):
        return self._state >= self.CLOSE
    
    # STOPPING TERMINATIONG AND STARTING
    
    def stop(self):
        # This may be called on a different process domain.
        # In that case there is no ioloop and therefore skip altogether
        if hasattr(self,'ioloop'):
            if self.is_alive() and not self._stopping:
                self._stopping = True
                if not self.on_stop():
                    self._stop_ioloop().add_callback(lambda r : self._stop())
        
    def _stop(self):
        '''Callback after the event loop has stopped.'''
        if self._stopping:
            self._inbox.close()
            self.on_exit()
            self._state = self.CLOSE
            self.ioloop.remove_loop_task(self)
            self.proxy().on_actor_exit(self.arbiter)
            self._stopping = False
        
    def terminate(self):
        self.stop()
        
    def shut_down(self):
        '''Called by ``self`` to shut down the arbiter'''
        if self.arbiter:
            self.proxy().stop(self.arbiter)
            
    # LOW LEVEL API
    def _stop_ioloop(self):
        return self.ioloop.stop()
        
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        try:
            self.ioloop.start()
        except SystemExit:
            raise
        except Exception as e:
            self.log.exception("Exception in worker {0}: {1}".format(self,e))
        finally:
            self.log.info("exiting {0}".format(self))
            self._stop()
    
    @classmethod
    def is_thread(cls,impl):
        return issubclass(cls,Thread)
    
    @classmethod
    def is_process(cls):
        return issubclass(cls,Process)
    
    def linked_actors(self):
        '''Iterator over linked-actor proxies'''
        return itervalues(self._linked_actors)
    
    @logerror
    def flush(self):
        '''Flush the pipe and runs callbacks. This function should live
on a event loop.'''
        inbox = self._inbox
        timeout = self.INBOX_TIMEOUT
        while True:
            request = None
            try:
                request = inbox.get(timeout = timeout)
            except Empty:
                break
            except IOError:
                break
            if request:
                try:
                    actor = self.get_actor(request.aid)
                    if actor == False:
                        self.log.info('Message from an un-linked actor')
                        continue
                    self.handle_request_from_actor(actor,request)
                except Exception as e:
                    #self.handle_request_error(request,e)
                    if self.log:
                        self.log.error('Error while processing worker request: {0}'.format(e),
                                        exc_info=sys.exc_info())
                        
    def get_actor(self, aid):
        if aid == self.aid:
            return self.proxy()
        elif aid in self._linked_actors:
            return self._linked_actors[aid]
        elif self.arbiter and aid == self.arbiter.aid:
            return self.arbiter
        elif self.monitor and aid == self.monitor.aid:
            return self.monitor
        else:
            return False
    
    def handle_request_from_actor(self, caller, request):
        func = getattr(self,'actor_{0}'.format(request.name),None)
        if func:
            ack = getattr(func,'ack',True)
            args = request.msg[0]
            kwargs = request.msg[1]
            result = func(caller, *args, **kwargs)
        else:
            ack = False
            pass
        #uns = self.unserialize
        #args = tuple((uns(a) for a in request.args))
        #kwargs = dict(((k,uns(a)) for k,a in iteritems(request.kwargs)))
        if ack:
            self.log.debug('Sending callback {0}'.format(request.rid))
            caller.callback(self.proxy(),request.rid,result)
    
    def __call__(self):
        '''Called in the main eventloop, It flush the inbox queue and notified linked actors'''
        self.flush()
        if self.arbiter and self.impl != 'monitor':
            self.proxy().notify(self.arbiter,time())
            #self.arbiter.notify(self,time())
        #notify = self.arbiter.notify
        #for actor in self.linked_actors():
        #    actor.notify(self,)
        #   notify(actor.aid,self.aid,time.time())
        self.on_task()
    
    def current_thread(self):
        '''Return the current thread'''
        return current_thread()
    
    def current_process(self):
        return current_process()
    
    def isprocess(self):
        return self.impl == 'process'
    
    def info(self):
        return {'aid':self.aid,
                'pid':self.pid,
                'ppid':self.ppid,
                'thread':self.current_thread().name,
                'process':self.current_process().name,
                'isprocess':self.isprocess()}
        
    def actorcall(self, actor, name, *args, **kwargs):
        return getattr(actor,name)(self.aid, *args, **kwargs)
        
    # BUILT IN ACTOR FUNCTIONS
    
    def actor_callback(self, caller, rid, result):
        self.log.debug('Received Callaback {0}'.format(rid))
        ActorRequest.actor_callback(rid,result)
    actor_callback.ack = False
    
    def actor_stop(self, caller):
        self.stop()
    actor_stop.ack = False
    
    def actor_notify(self, caller, t):
        '''An actor notified itself'''
        caller.notified = t
    actor_notify.ack = False
    
    def actor_on_actor_exit(self, caller, reason = None):
        self._linked_actors.pop(caller.aid)
    actor_on_actor_exit.ack = False
    
    def actor_info(self, caller):
        '''Get server Info and send it back.'''
        return self.info()
    
    def actor_ping(self, caller):
        return 'pong'


    # CLASS METHODS
    
    @classmethod
    def modify_arbiter_loop(cls, wp, ioloop):
        '''Called by an instance of :class:`pulsar.WorkerPool`, it modify the 
event loop of the arbiter if required.

:parameter wp: Instance of :class:`pulsar.WorkerPool`
:parameter ioloop: Arbiter event loop
'''
        pass
    
    @classmethod
    def clean_arbiter_loop(cls, wp, ioloop):
        pass


