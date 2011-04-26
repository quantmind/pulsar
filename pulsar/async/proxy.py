from time import time
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred

__all__ = ['ActorRequest',
           'ActorProxy',
           'get_proxy']


STD_MESSAGE = '__message__'


def get_proxy(obj, safe = False):
    if isinstance(obj,ActorProxy):
        return obj
    elif hasattr(obj,'proxy'):
        return get_proxy(obj.proxy)
    else:
        if safe:
            return None
        else:
            raise ValueError('"{0}" is not a remote or remote proxy.'.format(obj))


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
        if hasattr(obj,'aid'):
            return obj.aid
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
    def __init__(self, impl):
        self.aid = impl.aid
        self.remotes = impl.actor_functions
        self.inbox = impl.inbox
        self.timeout = impl.timeout
        self.loglevel = impl.loglevel
        
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
        o = get_proxy(o,True)
        return o and self.aid == o.aid
    
    def __ne__(self, o):
        return not self.__eq__(o) 
    
    def __getstate__(self):
        '''Because of the __getattr__ implementation,
we need to manually implement the pcikling and unpickling of thes object.'''
        return (self.aid,self.remotes,self.inbox,self.timeout,self.loglevel)
    
    def __setstate__(self, state):
        self.aid,self.remotes,self.inbox,self.timeout,self.loglevel = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return ActorProxyRequest(self, name, ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))
           

class ActorProxyMonitor(ActorProxy):
    
    def __init__(self, impl):
        self.impl = impl
        self.notified = time()
        self.stopping = 0
        super(ActorProxyMonitor,self).__init__(impl)
        
    @property
    def pid(self):
        return self.impl.pid
    
    def is_alive(self):
        return self.impl.is_alive()
        
    def terminate(self):
        self.impl.terminate()
            
    def __str__(self):
        return self.impl.__str__()
    
