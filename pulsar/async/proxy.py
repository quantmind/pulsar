import sys
from time import time

from pulsar import create_connection, CannotCallBackError
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.log import LocalMixin
from pulsar.utils.tools import gen_unique_id

from .defer import Deferred, is_async, make_async, raise_failure, Failure
from .mailbox import mailbox

__all__ = ['ActorMessage',
           'ActorProxyDeferred',
           'ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
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
            raise ValueError('"{0}" is not an actor or actor proxy.'\
                             .format(obj))

        
def actorid(actor):
    return actor.aid if hasattr(actor,'aid') else actor


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
:class:`Actor` to perform a specific *action*. :class:`ActorMessage`
are not directly initialized using the constructor, instead they are
created by :meth:`ActorProxy.send` method.

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
    
.. attribute:: rid

    :class:`ActorMessage` request id. This id used for managing remote
    callbacks, if :attr:`ack` is ``True``.
'''
    MESSAGES = {}
    
    def __init__(self, sender, target, action, ack, args, kwargs):
        super(ActorMessage,self).__init__(rid = gen_unique_id()[:8])
        # Set the event loop to be the one of the sender inbox
        # This way we can yield the deferred without kick starting callbacks
        #self._ioloop = sender.messageloop
        self.sender = actorid(sender)
        self.receiver = actorid(target)
        self.action = action
        self.args = args
        self.kwargs = kwargs
        self.ack = ack
        if self.ack:
            self.MESSAGES[self.rid] = self
        
    def __str__(self):
        return '[{0} - from {1}] - {3} {2}'.format(self.rid,self.sender,
                                                   self.action,self.receiver)
    
    def add_callback(self, callback, raise_on_error = False):
        if not self.ack:
            raise CannotCallBackError('Cannot add callback to "{0}".\
 It does not acknowledge'.format(self))
        return super(ActorMessage,self).add_callback(callback, raise_on_error)
    
    def __repr__(self):
        return self.__str__()
    
    def __getstate__(self):
        #Remove the list of callbacks and lock
        d = self.__dict__.copy()
        d.pop('_lock',None)
        #d.pop('_ioloop',None)
        d['_callbacks'] = []
        return d
    
    def __setstate__(self, state):
        self.__dict__ = state
        
    @classmethod
    def actor_callback(cls, rid, result):
        r = cls.MESSAGES.pop(rid,None)
        if r:
            r.callback(result)
            
            
class ActorProxyDeferred(Deferred):
    '''A :class:`Deferred` for an :class:`ActorProxy`. This instance will be
obtain and :class:`ActorProxy` result once the remote :class:`Actor` is fully
functional.

.. attribute:: aid

    The the remote :attr:`Actor` id
'''
    def __init__(self, aid, msg):
        super(ActorProxyDeferred,self).__init__(rid = aid)
        self.aid = aid
        self.proxy = None
        self._msg = msg.add_callback(self._store_proxy)
        
    def _store_proxy(self, proxy):
        self.proxy = proxy
        return self.callback(proxy)
    
    def __str__(self):
        return '{0}({1})'.format(self.__class__,self.aid)
    __repr__ = __str__
    
    
class ActorProxy(LocalMixin):
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
        # impl can be an actor or an actor impl,
        # which does not have the address attribute
        self.__address = getattr(impl,'address',None)
        self.timeout = impl.timeout
        self.loglevel = impl.loglevel
        
    def __get_address(self):
        return self.__address
    def __set_address(self, address):
        '''Callback from the :class:`Arbiter` when the remote underlying actor
has registered its inbox address.

:parameter address: the address for the underlying remote :attr:`Arbiter.inbox`
'''
        self.__address = address
        m = self.local.pop('mailbox',None)
        if m:
            m.close()
        if address:
            self.on_address.callback(address)
    address = property(__get_address,__set_address)
    
    @property
    def on_address(self):
        if 'on_address' not in self.local:
            self.local['on_address'] = Deferred()
        return self.local['on_address']
        
    @property
    def mailbox(self):
        '''Actor mailbox'''
        if self.address:
            if 'mailbox' not in self.local:
                self.local['mailbox'] = mailbox(self, self.address)
            return self.local['mailbox']
        
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

:parameter sender: :class:`Actor` sending the message.
:parameter action: the action of the message. If not provided,
    the message will be broadcasted by the receiving actor,
    otherwise a specific action will be performed.
    Default ``None``.
:parameter args: non positional arguments of message body.
:parameter kwargs: key-valued arguments of message body.
:parameter ack: If ``True`` the receiving actor will send a callback.
    If the action is provided and available, this parameter will be overritten.
:rtype: an instance of :class:`ActorMessage`.

When sending a message, first we check the ``sender`` outbox. If that is
not available, we get the receiver ``inbox`` and hope it can carry the message.
If there is no inbox either, abort the message passing and log a critical error.
'''
        mailbox = self.mailbox        
        if not mailbox:
            sender.log.critical('Cannot send a message to {0}. No\
 mailbox available.'.format(self))
            return
        
        msg = self.message(sender, action, *args, **kwargs)
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
    '''A specialised :class:`ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class are not pickable and
therefore remain in :class:`Arbiter` domain, which is the
process where they have been created.

.. attribute:: impl

    Instance of the remote actor :class:`ActorImpl`
    
.. attribute:: info

    Dictionary of information regarding the remote actor
    
.. attribute:: on_address

    A :class:`Deferred` called back when the mailbox address is ready.
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
        
    @property
    def notified(self):
        return self.info['last_notified']
    
    @property
    def pid(self):
        return self.impl.pid
    
    @property
    def proxy(self):
        return ActorProxy(self)
    
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

