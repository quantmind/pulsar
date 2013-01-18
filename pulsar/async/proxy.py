import sys
import json
from collections import deque

from pulsar import AuthenticationError

from .access import get_actor
from .defer import Deferred

__all__ = ['ActorProxyDeferred',
           'ActorProxy',
           'ActorProxyMonitor',
           'CommandNotFound',
           'get_proxy',
           'command',
           'get_command']

global_commands_table = {}
actor_only_commands = set()
actor_commands = set()
arbiter_commands = set()

class CommandNotFound(Exception):

    def __init__(self, name):
        super(CommandNotFound, self).__init__(
                            'Command "%s" not available' % name)
        
        
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
    return actor.aid if hasattr(actor, 'aid') else actor

def get_command(name, commands_set=None):
    '''Get the command function *name*'''
    name = name.lower()
    if commands_set and name not in commands_set:
        return
    return global_commands_table.get(name)
    
    
class command:
    '''Decorator for pulsar command functions.
    
:parameter ack: ``True`` if the command acknowledge the sender with a
    response. Usually is set to ``True`` (which is also the default value).
:parameter authenticated: If ``True`` the action can only be invoked by
    remote actors which have authenticated with the actor for which
    the action has been requested.
:parameter internal: Internal commands are for internal use only, not for
    external clients. They accept the actor proxy calling as third positional
    arguments.
'''
    def __init__(self, ack=True, authenticated=False, internal=False,
                 commands_set=None):
        self.ack = ack
        self.authenticated = authenticated
        self.internal = internal
        if commands_set is None:
            commands_set = actor_commands
        self.commands_set = commands_set
    
    def __call__(self, f):
        self.name = f.__name__.lower()
        
        def command_function(request, args, kwargs):
            if self.authenticated:
                password = request.actor.cfg.get('password')
                if password and not request.connection.authenticated:
                    raise AuthenticationError
            if self.internal:
                request.caller = get_proxy(request.caller, safe=True)
                if not request.caller:
                    raise AuthenticationError
                return f(request, *args, **kwargs)
            else:
                return f(request, *args, **kwargs)
        
        command_function.ack = self.ack
        command_function.internal = self.internal
        command_function.__name__ = self.name
        command_function.__doc__ = f.__doc__
        self.commands_set.add(self.name)
        global_commands_table[self.name] = command_function
        return command_function
            
            
class ActorProxyDeferred(Deferred):
    '''A :class:`Deferred` for an :class:`ActorProxy`. The callback will
be an :class:`ActorProxy` which will be received once the remote :class:`Actor`
is fully functional.

.. attribute:: aid

    The the remote :attr:`Actor` id
     
'''
    def __init__(self, aid, msg=None):
        super(ActorProxyDeferred,self).__init__()
        if msg is None:
            # In this case aid is an instance of an ActorProxyMonitor
            self.aid = aid.aid
        else:
            self.aid = aid
            # Listen for the callbacks and errorbacks
            msg.add_both(self.callback)
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__, self.aid)
    __str__ = __repr__
    
    
class ActorProxy(object):
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
        self.commands_set = tuple(impl.commands_set)
        self.address = impl.address
        self.cfg = impl.cfg
    
    def __repr__(self):
        return '%s(%s)' % (self.name, self.aid)
    __str__ = __repr__
    
    @property
    def mailbox(self):
        '''Actor mailbox'''
        if self.address:
            return get_actor().proxy_mailbox(self.address)
        
    @property
    def proxy(self):
        return self
    
    def request(self, sender, command, *args, **params):
        '''Send an message to the underlying actor
(the receiver). This is the low level function call for
communicating between actors.

:parameter sender: :class:`Actor` sending the message.
:parameter command: the :class:`command` to perform in the actor underlying
    this proxy.
:parameter args: non positional arguments of command.
:rtype: an asynchronous :class:`ActorMessage`.'''
        if sender is None:
            sender = get_actor()
        if not self.mailbox:
            sender.logger.critical('Cannot send a message to %s. No\
 mailbox available.', self)
            return
        cmd = get_command(command, self.commands_set)
        if not cmd:
            raise CommandNotFound(command)
        return self.mailbox.request(cmd, sender, self.aid, args, params)
    
    def __eq__(self, o):
        o = get_proxy(o,True)
        return o and self.aid == o.aid
    
    def __ne__(self, o):
        return not self.__eq__(o)
        
    def stop(self, sender=None):
        '''Stop the remote :class:`Actor`'''
        self.receive_from(sender, 'stop')
        

class ActorProxyMonitor(ActorProxy):
    '''A specialised :class:`ActorProxy` class which contains additional
information about the remote underlying :class:`pulsar.Actor`. Unlike the
:class:`pulsar.ActorProxy` class, instances of this class are not pickable and
therefore remain in the :class:`Arbiter` process domain, which is the
process where they have been created.

.. attribute:: impl

    The :class:`Concurrency` instance for the remote :class:`Actor`.
    
.. attribute:: info

    Dictionary of information regarding the remote :class:`Actor`
'''
    monitor = None
    def __init__(self, impl):
        self.impl = impl
        self.info = {}
        self.stopping_loops = 0
        self.spawning_loops = 0
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
        self.impl.start()

    