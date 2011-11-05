import sys
from time import time

from pulsar import create_connection
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred, is_async, make_async, raise_failure, Failure
from .mailbox import SocketMailbox

__all__ = ['ActorMessage',
           'ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
           'ActorCallBacks',
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
            raise ValueError('"{0}" is not an actor or actor proxy.'\
                             .format(obj))

        
def actorid(actor):
    return actor.aid if hasattr(actor,'aid') else actor
        
        
def process_message(receiver, sender, message):
    '''Process *message* received by *receiver* by performing the message
action. If an acknowledgment is required, send back the result using
the receiver eventloop.'''
    ack = message.ack
    try:
        func = receiver.actor_functions.get(message.action,None)
        if func:
            ack = getattr(func,'ack',True)
            result = func(receiver, sender, *message.args, **message.kwargs)
        else:
            result = receiver.handle_message(sender, message)
    except Exception as e:
        result = Failure(sys.exc_info())
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

.. attribute:: sender

    id of the actor sending the message.
    
.. attribute:: receiver

    id of the actor receiving the message.
    
.. attribute:: action

    action to be performed
    
.. attribute:: args

    Positional arguments in the message body
    
.. attribute:: kwargs

    Optional arguments in the message body
    
.. attribute:: ack

    ``True`` if the message needs acknowledgment
'''
    MESSAGES = {}
    
    def __init__(self, sender, target, action, ack, args, kwargs):
        super(ActorMessage,self).__init__(rid = gen_unique_id()[:8])
        # Set the event loop to be the one of the sender
        # This way we can yield the deferred without kick starting callbacks
        self._ioloop = sender.ioloop
        self.sender = actorid(sender)
        self.receiver = actorid(target)
        self.action = action
        self.args = args
        self.kwargs = kwargs
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
        d.pop('_ioloop',None)
        d['_callbacks'] = []
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        
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
        self.inbox = impl.inbox.clone() if impl.inbox else None
        self.timeout = impl.timeout
        self.loglevel = impl.loglevel
        
    def message(self, sender, action, *args, **kwargs):
        ack = False
        if action in self.remotes:
            ack = self.remotes[action]
        return ActorMessage(sender,self.aid,action,ack,args,kwargs)
        
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
            mailbox = self.inbox
        
        if not mailbox:
            sender.log.critical('Cannot send a message to {0}. There is no\
 mailbox available.'.format(self))
            return
        
        msg = self.message(sender,action,*args,**kwargs)
        try:
            mailbox.put(msg)
            return msg
        except Exception as e:
            sender.log.critical('Failed to send message {0}: {1}'.\
                             format(msg,e), exc_info = True)
        
    def __repr__(self):
        return self.aid
    __str__ = __repr__
    
    def __eq__(self, o):
        o = get_proxy(o,True)
        return o and self.aid == o.aid
    
    def __ne__(self, o):
        return not self.__eq__(o)

    def local_info(self):
        '''Return a dictionary containing information about the remote
object including, aid (actor id), timeout and mailbox size.'''
        return {'aid':self.aid[:8],
                'timeout':self.timeout,
                'mailbox_size':self.mailbox.qsize()}
        
    def stop(self,sender):
        '''Stop the remote :class:`Actor`'''
        self.send(sender,'stop')
        

class ActorProxyMonitor(ActorProxy):
    '''A specialised :class:`pulsar.ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class are not pickable and
therefore remain in the process where they have been created.

.. attribute:: impl

    Instance of the remote actor :class:`ActorImpl`
    
.. attribute:: info

    Dictionary of information regarding the remote actor
    
.. attribute:: on_address

    A :class:`Deferred` called back when the inbox address is ready.
    You can use this funvtion in the following way::
    
        import pulsar
        
        a = pulsar.spawn()
        a.on_address.add_callback(...)
'''
    def __init__(self, impl):
        self.impl = impl
        self.info = {'last_notified':time()}
        self.stopping_loops = 0
        super(ActorProxyMonitor,self).__init__(impl)
        self.on_address = Deferred()
    
    def inbox_address(self, address):
        '''Callback from the :class:`Arbiter` when the remote underlying actor
has registered its inbox address.

:parameter address: the address for the undrlying remote :attr:`Arbiter.inbox`
'''
        if address:
            self.inbox = SocketMailbox(address)
            self.inbox.register(self,False)
        self.on_address.callback(address)
        
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
    
    def join(self, timeout = None):
        '''Wait until the underlying actor terminates'''
        self.impl.join(timeout = timeout)
        
    def local_info(self):
        '''Return a dictionary containing information about the remote
object including, aid (actor id), timeout mailbox size, last notified time and
process id.'''
        return self.info

