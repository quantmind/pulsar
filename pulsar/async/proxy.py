from time import time
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred, is_async

__all__ = ['ActorRequest',
           'ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
           'ActorCallBack',
           'ActorCallBacks',
           'DEFAULT_MESSAGE_CHANNEL']


DEFAULT_MESSAGE_CHANNEL = '__message__'


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
        


class ActorCallBack(Deferred):
    '''An actor callback run on the actor event loop'''
    def __init__(self, actor, request, *args, **kwargs):
        super(ActorCallBack,self).__init__()
        self.args = args
        self.kwargs = kwargs
        self.actor = actor
        self.request = request
        if is_async(request):
            self()
        else:
            self.callback(self.request)
        
    def __call__(self):
        if self.request.called:
            self.callback(self.request.result)
        else:
            self.actor.ioloop.add_callback(self)


class ActorCallBacks(Deferred):
    
    def __init__(self, actor, requests):
        super(ActorCallBacks,self).__init__()
        self.actor = actor
        self.requests = []
        self._tmp_results = []
        for r in requests:
            if is_async(r):
                self.requests.append(r)
            else:
                self._tmp_results.append(r)
        actor.ioloop.add_callback(self)
        
    def __call__(self):
        if self.requests:
            nr = []
            for r in self.requests:
                if r.called:
                    self._tmp_results.append(r.result)
                else:
                    nr.append(r)
            self.requests = nr
        if self.requests:
            self.actor.ioloop.add_callback(self)
        else:
            self.callback(self._tmp_results)
            

class CallerCallBack(object):
    __slots__ = ('rid','proxy','caller')
    
    def __init__(self, request, actor, caller):
        self.rid = request.rid
        self.proxy = actor.proxy
        self.caller = caller
        
    def __call__(self, result):
        self.proxy.callback(self.caller,self.rid,result)
        

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
    
    def make_actor_callback(self, actor, caller):
        return CallerCallBack(self,actor,caller)
            
    @classmethod
    def actor_callback(cls, rid, result):
        r = cls.REQUESTS.pop(rid,None)
        if r:
            r.callback(result)
            

class ActorProxyRequest(object):
    '''A class holding information about a message to be sent from one
actor to another'''
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
    '''This is an important component in pulsar concurrent framework. An
instance of this class behaves as a proxy for a remote `underlying` 
:class:`pulsar.Actor` instance.
This is a lightweight class which delegates function calls to the underlying
remote object.

It is pickable and therefore can be send from actor to actor using pulsar
messaging.

A proxy exposes all the underlying remote functions which have been implemented
in the actor class by prefixing with ``actor_``
(see the :class:`pulsar.ActorMetaClass` documentation).
By default each actor comes with a set of remote functions: info, ping,
notify, stop, on_actor_exit and callback

For example, lets say we have a proxy ``a`` and an actor (or proxy) ``b``::

    a.notify(b)
    
will call notify on the actor underlying ``a`` with ``b`` as the caller.
This is equivalent as to using the lower level call::

    a.send(b.aid,(),'notify')
    

.. attribute:: proxyid

    Unique ID for the remote object
    
.. attribute:: remotes

    dictionary of remote functions names with value indicating if the
    remote function will acknowledge the call or not.
    
.. attribute:: timeout

    the value of the underlying :attr:`pulsar.Actor.timeout` attribute
'''     
    def __init__(self, impl):
        self.aid = impl.aid
        self.remotes = impl.remotes
        self.inbox = impl.inbox
        self.timeout = impl.timeout
        self.loglevel = impl.loglevel
        
    def send(self, aid, msg, name = None, ack = False):
        '''\
Send a message to the underlying actor. This is the low level function
call for communicating between actors.

:parameter aid: the actor id of the actor sending the message
:parameter msg: the message body.
:parameter name: the name of the message. If not provided,
                 the message will be broadcasted by the receiving actor,
                 otherwise a specific action will be performed.
                 Default ``None``.
:parameter ack: If ``True`` the receiving actor will send a callback.'''
        name = name or DEFAULT_MESSAGE_CHANNEL
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
we need to manually implement the pickling and unpickling of the object.'''
        return (self.aid,self.remotes,self.inbox,self.timeout,self.loglevel)
    
    def __setstate__(self, state):
        self.aid,self.remotes,self.inbox,self.timeout,self.loglevel = state
        
    def __getattr__(self, name):
        if name in self.remotes:
            ack = self.remotes[name]
            return ActorProxyRequest(self, name, ack)
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self,name))

    def local_info(self):
        '''Return a dictionary containing information about the remote
object including, aid (actor id), timeout and inbox size.'''
        return {'aid':self.aid[:8],
                'timeout':self.timeout,
                'inbox_size':self.inbox.qsize()}


class ActorProxyMonitor(ActorProxy):
    '''A specialized :class:`pulsar.ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class are not pickable and
therefore remain in the process where they have been created.'''
    def __init__(self, impl):
        self.impl = impl
        self.notified = time()
        self.stopping = 0
        super(ActorProxyMonitor,self).__init__(impl)
        
    @property
    def pid(self):
        return self.impl.pid
    
    def is_alive(self):
        '''True if underlying actor is alive'''
        return self.impl.is_alive()
        
    def terminate(self):
        '''Terminate life of underlying actor.'''
        self.impl.terminate()
            
    def __str__(self):
        return self.impl.__str__()
    
    def local_info(self):
        '''Return a dictionary containing information about the remote
object including, aid (actor id), timeout inbox size, last notified time and
process id.'''
        data = super(ActorProxyMonitor,self).local_info()
        data.update({'notified':self.notified,
                     'pid':self.pid})
        return data
