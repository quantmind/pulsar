from time import time

from pulsar import create_connection
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred, is_async, make_async, raise_failure

__all__ = ['ActorMessage',
           'ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
           #'ActorCallBack',
           #'ActorCallBacks',
           'process_message',
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

        
def actorid(actor):
    return actor.aid if hasattr(actor,'aid') else actor
        
        
def process_message(receiver, sender, message):
    '''Process *message* received by *receiver* by perfroming the message
action. If an acknowledgment is required send back the result using
the receiver eventloop.'''
    ack = message.ack
    try:
        func = receiver.actor_functions.get(message.action,None)
        if func:
            ack = getattr(func,'ack',True)
            args,kwargs = message.msg
            result = func(receiver, sender, *args, **kwargs)
        else:
            result = receiver.channel_messsage(sender, message)
    except Exception as e:
        result = e
        if receiver.log:
            receiver.log.critical('Error while processing message: {0}.'\
                              .format(e), exc_info=True)
    finally:
        if ack:
            make_async(result).start(receiver.ioloop)\
            .add_callback(
                    lambda res : sender.send(receiver, 'callback',\
                                             message.rid, res))\
            .add_callback(raise_failure)
            #ActorCallBack(self,result).\
            #    add_callback(message.make_actor_callback(self,sender))


class ActorCallBack(Deferred):
    '''An actor callback run on the actor event loop'''
    def __init__(self, actor, request, *args, **kwargs):
        super(ActorCallBack,self).__init__()
        self.args = args
        self.kwargs = kwargs
        self.actor = actor
        self.request = request
        if is_async(request):
            self.__call__()
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
        

class ActorMessage(Deferred):
    '''A message class which travels from :class:`Actor` to
:class:`Actor` to perform a specific action.

:param sender: The :class:`Actor` or :class:`ActorProxy` or actor id sending
    the message.
:param target: The :class:`Actor` or :class:`ActorProxy` or actor id receiving
    the message.
:param action: Action to perform on the receiving actor. It is a string which
    is used to obtain the remote function.
:parameter ack: Boolean indicating if message needs to be acknowledge by the
    receiver.
:param msg: Message to send.
'''
    MESSAGES = {}
    
    def __init__(self, sender, target, action, ack, msg):
        super(ActorMessage,self).__init__(rid = gen_unique_id()[:8])
        self.sender = actorid(sender)
        self.receiver = actorid(target)
        self.action = action
        self.msg = msg
        self.ack = ack
        if self.ack:
            self.MESSAGES[self.rid] = self
        
    def __str__(self):
        return '[{0}] - {1} {2} {3}'.format(self.rid,self.sender,self.action,
                                            self.receiver)
    
    def __repr__(self):
        return self.__str__()
    
    def __getstate__(self):
        #Remove the list of callbacks and lock
        d = self.__dict__.copy()
        d.pop('_lock',None)
        d['_callbacks'] = []
        return d
    
    def make_actor_callback(self, actor, caller):
        return CallerCallBack(self, actor, caller)
            
    @classmethod
    def actor_callback(cls, rid, result):
        r = cls.MESSAGES.pop(rid,None)
        if r:
            r.callback(result)
            

class ActorProxy(object):
    '''This is an important component in pulsar concurrent framework. An
instance of this class behaves as a proxy for a remote `underlying` 
:class:`Actor` instance.
This is a lightweight class which delegates function calls to the underlying
remote object.

It is pickable and therefore can be send from actor to actor using pulsar
messaging.

A proxy exposes all the underlying remote functions which have been implemented
in the actor class by prefixing with ``actor_``
(see the :class:`pulsar.ActorMetaClass` documentation).

By default each actor comes with a set of remote functions:

 * ``info`` returns a dictionary of information about the actor
 * ``ping`` returns ``pong``.
 * ``notify``
 * ``stop`` stop the actor
 * ``on_actor_exit``
 * ``callback``

For example, lets say we have a proxy ``a`` and an actor (or proxy) ``b``::

    a.send(b,'notify','hello there!')
    
will send a message to actor ``a`` from sender ``b`` invoking
action ``notify`` with parameter ``"hello there!"``.
    

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
        self.mailbox = impl.inbox
        self.timeout = impl.timeout
        self.loglevel = impl.loglevel
        
    def message(self, sender, action, *args, **kwargs):
        ack = False
        if action in self.remotes:
            ack = self.remotes[action]
        return ActorMessage(sender,self.aid,action,ack,(args,kwargs))
        
    def send(self, sender, action, *args, **kwargs):
        '''\
Send an :class:`ActorMessage` to the underlying actor
(the receiver). This is the low level function call for
communicating between actors.

:parameter sender: the actor sending the message.
:parameter action: the action of the message. If not provided,
    the message will be broadcasted by the receiving actor,
    otherwise a specific action will be performed.
    Default ``None``.
:parameter args: non key-valued part of parameters of message body.
:parameter kwargs: key-valued part of parameters of message body.
:parameter ack: If ``True`` the receiving actor will send a callback.
    If the action is provided and available, this parameter will be overritten.
:rtype: an instance of :class:`ActorMessage`.

When sending a message, first we check the ``sender`` outbox. If that is
not available, we get the receiver ``inbox`` and hope it can carry the message.
If there is no inbox either, abort the message passing and log a critical error.
    '''
        mailbox = sender.outbox
        # if the sender has no outbox, pick the receiver mailbox an hope
        # for the best
        if not mailbox:
            mailbox = self.mailbox
        
        if not mailbox:
            sender.log.critical('Cannot send a message to {0}. There is no\
 mailbox available.'.format(self))
            return
        
        msg = self.message(sender,action,*args,**kwargs)
        try:
            mailbox.put(msg)
            return msg
        except Exception as e:
            sender.log.error('Failed to send message {0}: {1}'.\
                             format(msg,e), exc_info = True)
        
    def __repr__(self):
        return self.aid[:8]
    
    def __str__(self):
        return self.__repr__()
    
    def __eq__(self, o):
        o = get_proxy(o,True)
        return o and self.aid == o.aid
    
    def __ne__(self, o):
        return not self.__eq__(o) 
    
#    #def __getstate__(self):
#        '''Because of the __getattr__ implementation,
#we need to manually implement the pickling and unpickling of the object.'''
#        return (self.aid,self.remotes,self.mailbox,self.timeout,self.loglevel)
#    
#    def __setstate__(self, state):
#        self.aid,self.remotes,self.mailbox,self.timeout,self.loglevel = state
#        
#    def get_request(self, action):
#        if action in self.remotes:
#           ack = self.remotes[action]
#           return ActorProxyRequest(self, action, ack)
#        else:
#            raise AttributeError("'{0}' object has no attribute '{1}'"\
#                                 .format(self,action))
#
#    def __getattr__(self, name):
#        return self.get_request(name)

    def local_info(self):
        '''Return a dictionary containing information about the remote
object including, aid (actor id), timeout and mailbox size.'''
        return {'aid':self.aid[:8],
                'timeout':self.timeout,
                'mailbox_size':self.mailbox.qsize()}


class ActorProxyMonitor(ActorProxy):
    '''A specialized :class:`pulsar.ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class are not pickable and
therefore remain in the process where they have been created.'''
    def __init__(self, impl):
        self.impl = impl
        self.info = {'last_notified':time()}
        self.stopping = 0
        super(ActorProxyMonitor,self).__init__(impl)
    
    @property
    def notified(self):
        return self.info['last_notified']
    
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
object including, aid (actor id), timeout mailbox size, last notified time and
process id.'''
        return self.info

