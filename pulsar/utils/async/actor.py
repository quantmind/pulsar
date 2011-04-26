import sys
import time
from multiprocessing import Process, reduction
from multiprocessing.queues import Queue, Empty
from threading import Thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   NotRegisteredWithServer, PickableMixin,\
                   logerror
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred


__all__ = ['is_actor',
           'get_proxy',
           'Actor',
           'MainActor',
           'ActorProxy']


def is_actor(obj):
    return isinstance(obj,Actor)


def get_proxy(obj):
    if is_actor(obj):
        return obj.get_proxy()
    elif isinstance(obj,ActorProxy):
        return obj
    else:
        raise ValueError('"{0}" is not a remote or remote proxy.'.format(obj))


class ActorRequest(Deferred):
    
    def __init__(self, actor, name, ack, args, kwargs):
        super(ActorRequest,self).__init__(rid = gen_unique_id())
        self.actor = actor
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.ack = ack
        
    def __str__(self):
        return '{0} - {1}'.format(self.name,self.rid[:8])
    
    def __repr__(self):
        return self.__str__()
    
    def __getstate__(self):
        #Remove the list of callbacks
        d = self.__dict__.copy()
        d['_callbacks'] = []
        return d
        
    def callback(self, result):
        super(RemoteRequest,self).callback(result)
            

class ActorProxyRequest(object):
    __slots__ = ('actor','_func_name','ack')
    
    def __init__(self, actor, name, ack):
        self.actor = actor
        self._func_name = name
        self.ack = ack
        
    def __call__(self, *args, **kwargs):
        ser = self.serialize
        args = tuple((ser(a) for a in args))
        kwargs = dict((k,ser(a)) for k,a in iteritems(kwargs))
        request = ActorRequest(self.actor,self._func_name,self.ack,args,kwargs)
        self.actor._inbox.put(request)
        return request
    
    def serialize(self, obj):
        if is_remote(obj):
            return obj.get_proxy()
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
    __slots__ = ('actor_id','remotes','inbox')
    
    def __init__(self, actor_id, inbox, remotes):
        self.actor_id = actor_id
        self.remotes = remotes
        self.inbox = inbox
    
    def __repr__(self):
        return self.proxyid[:8]
    
    def __str__(self):
        return self.__repr__()
    
    def __getstate__(self):
        '''Because of the __getattr__ implementation,
we need to manually implement the pcikling and unpickling of thes object.
Furthermore, the connection is not serialized.'''
        return (self.actor_id,self.remotes,self.inbox)
    
    def __setstate__(self, state):
        self.actor_id,self.remotes,self.inbox = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return ActorRequest(self, name, ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))    
    

class ActorMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(ActorMetaClass, cls).__new__
        fprefix = 'remote_'
        attrib  = '{0}functions'.format(fprefix)
        cont = {}
        for key, method in list(attrs.items()):
            if hasattr(method,'__call__') and key.startswith(fprefix):
                meth_name = key[len(fprefix):]
                method = attrs.pop(key)
                method.__name__ = meth_name 
                cont[meth_name] = method
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs[attrib] = cont
        attrs['remotes'] = dict((meth_name ,getattr(attr,'ack',True))\
                                 for meth_name ,attr in cont.items())
        return make(cls, name, bases, attrs)

    
    
ActorBase = ActorMetaClass('BaseActor',(object,),{})



class Actor(ActorBase,PickableMixin):
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
    LIVE_ACTORS = {}
    INBOX_TIMEOUT = 0.02
    _actor_implementation = None
    
    @classmethod
    def spawn(cls, *args, **kwargs):
        '''Create a new concurrent Actor'''
        actor = cls()
        queue = Queue()
        aid = gen_unique_id()
        self._actor_implementation(target = self.__startme, name = aid, arg = (queue,))
        actor._aid = aid
        actor._queue = queue
        return actor
    
    def _running(self):
        return self.actor_id in self.LIVE_ACTORS
        
    def get_proxy(self):
        return (ActorProxy,(self.actorid,self._inbox,self.remotes))
    
    def init_in_process(self):
        '''Called After forking (if a process)'''
        if not hasattr(self,'log'):
            self.log = self.getLogger()
        if self._actor_id in self.LIVE_ACTORS:
            raise AlreadyRegistered
        self.LIVE_ACTORS[self.actor_id] = self
        
    def run(self):
        '''Run the actor'''
        self.init_in_process()
        self._run()
        
    def close(self):
        '''Close the server connection.'''
        self._inbox.close()
        
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        raise NotImplementedError

    def stop(self):
        '''The stop implementation which must be done by a derived class.'''
        raise NotImplementedError
    
    @classmethod
    def is_thread(cls):
        return issubclass(cls,Thread)
    
    @classmethod
    def is_process(cls):
        return issubclass(cls,Process)

    def on_remote(self,remote):
        '''Callback when the remote server proxy arrives.'''
        return remote
    
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
            if request:
                try:
                    pid = request.actor_id
                    if pid in self.LIVE_ACTORS:
                        obj = self.LIVE_ACTORS[pid]
                        obj.handle_remote_request(request)
                    else:
                        self.handle_remote(request)
                except Exception as e:
                    if obj:
                        obj.run_remote_callback(request,e)
                    self.handle_request_error(request,e)
                    if self.log:
                        self.log.error('Error while processing worker request: {0}'.format(e),
                                        exc_info=sys.exc_info())
    
    def remote_stop(self):
        self._stop()
    remote_stop.ack = False

    
class MainActor(Actor):
    '''An actor which resides in the main process and main thread'''
    
    def start(self):
        self.run()
        return self


