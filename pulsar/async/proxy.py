import sys
import json
from collections import deque

from pulsar import AuthenticationError, CommandNotFound
from pulsar.utils.pep import default_timer

from .defer import Deferred
from .consts import *

__all__ = ['ActorProxyDeferred',
           'ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
           'command',
           'get_command']

global_commands_table = {}
        
        
def get_proxy(obj, safe=False):
    if isinstance(obj, ActorProxy):
        return obj
    elif hasattr(obj, 'proxy'):
        return get_proxy(obj.proxy)
    else:
        if safe:
            return None
        else:
            raise ValueError('"%s" is not an actor or actor proxy.' % obj)

def actorid(actor):
    return actor.identity() if hasattr(actor, 'identity') else actor

def get_command(name):
    '''Get the command function *name*'''
    command = global_commands_table.get(name.lower())
    if not command:
        raise CommandNotFound(name)
    return command
    
    
class command:
    '''Decorator for pulsar command functions.
    
:parameter ack: ``True`` if the command acknowledge the sender with a
    response. Usually is set to ``True`` (which is also the default value).
'''
    def __init__(self, ack=True):
        self.ack = ack
    
    def __call__(self, f):
        self.name = f.__name__.lower()
        def command_function(request, args, kwargs):
            return f(request, *args, **kwargs)
        command_function.ack = self.ack
        command_function.__name__ = self.name
        command_function.__doc__ = f.__doc__
        global_commands_table[self.name] = command_function
        return command_function
            

class ActorIdentity(object):
    
    def identity(self):
        return self.aid
    
    
class ActorProxyDeferred(Deferred, ActorIdentity):
    '''A :class:`Deferred` for an :class:`ActorProxy`. The callback will
be an :class:`ActorProxy` which will be received once the remote :class:`Actor`
is fully functional.

.. attribute:: aid

    The the remote :attr:`Actor` id
     
'''
    def __init__(self, aid, msg=None):
        super(ActorProxyDeferred,self).__init__()
        if isinstance(aid, ActorProxyMonitor):
            aid.callback = self
            self.aid = aid.aid
        else:
            self.aid = aid
            # Listen for the callbacks and errorbacks
            msg.add_both(self.callback)
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__, self.aid)
    __str__ = __repr__
    
    
class ActorProxy(ActorIdentity):
    '''This is an important component in pulsar concurrent framework. An
instance of this class is as a proxy for a remote `underlying` 
:class:`Actor`. This is a lightweight class which delegates
function calls to the underlying remote object.

It is pickable and therefore can be send from actor to actor using pulsar
messaging. It exposes all the underlying :class:`command` which have been
implemented.

For example, lets say we have a proxy ``a``, to send a message to it::

    from pulsar import send
    
    send(a, 'echo', 'hello there!')
    
will send the command ``echo`` to actor ``a`` with
parameter ``"hello there!"``.
    
.. attribute:: aid

    Unique ID for the remote :class:`Actor`
    
.. attribute:: address

    the socket address of the underlying :attr:`Actor.mailbox`.
    
'''
    def __init__(self, impl):
        self.aid = impl.aid
        self.name = impl.name
        self.cfg = impl.cfg
        if hasattr(impl, 'address'):
            self.address = impl.address
    
    def __repr__(self):
        return '%s(%s)' % (self.name, self.aid)
    __str__ = __repr__
        
    @property
    def proxy(self):
        return self
    
    def __eq__(self, o):
        o = get_proxy(o,True)
        return o and self.aid == o.aid
    
    def __ne__(self, o):
        return not self.__eq__(o)
        

class ActorProxyMonitor(ActorProxy):
    '''A specialised :class:`ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class serialise
into their :attr:`proxy` attribute..

.. attribute:: impl

    The :class:`Concurrency` instance for the remote :class:`Actor`.
    
.. attribute:: info

    Dictionary of information regarding the remote :class:`Actor`
'''
    monitor = None
    def __init__(self, impl):
        self.impl = impl
        self.info = {}
        self.mailbox = None
        self.callback = None
        self.spawning_start = None
        self.stopping_start = None
        super(ActorProxyMonitor,self).__init__(impl)
        
    @property
    def notified(self):
        return self.info.get('last_notified')
    
    @property
    def pid(self):
        return self.impl.pid
    
    @property
    def proxy(self):
        return ActorProxy(self)
    
    def __reduce__(self):
        return self.proxy.__reduce__()
    
    def is_alive(self):
        '''True if underlying actor is alive'''
        return self.impl.is_alive()
        
    def terminate(self):
        '''Terminate life of underlying actor.'''
        self.impl.terminate()
    
    def join(self, timeout=None):
        '''Wait until the underlying actor terminates. If *timeout* is
provided, it raises an exception if the timeout is reached.'''
        self.impl.join(timeout=timeout)

    def start(self):
        '''Start the remote actor.'''
        self.spawning_start = default_timer()
        self.impl.start()
        
    def should_be_alive(self):
        return default_timer() - self.spawning_start > ACTOR_ACTION_TIMEOUT

    def should_terminate(self):
        if self.stopping_start is None:
            self.stopping_start = default_timer()
            return False
        else:
            return default_timer() - self.stopping_start > ACTOR_ACTION_TIMEOUT
    