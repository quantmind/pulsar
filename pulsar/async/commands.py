from time import time

from pulsar import CommandError

from .defer import is_async
from .proxy import command, ActorProxyMonitor
from .consts import NOT_DONE


@command()
def ping(request):
    return 'pong'

@command()
def echo(request, message):
    '''Returns *message*'''
    return message

@command()
def config(request, setget, name, *value):
    setget = setget.lower()
    if setget == 'get':
        if len(values) > 0:
            raise CommandError('"config get" accept only one parameter')
        return client.actor.cfg.get(name)
    elif setget == 'set':
        if len(values) > 1:
            raise CommandError('"config get" accept only two parameters')
        client.actor.cfg.set(name, value[0])
    else:
        raise CommandError('config must be followed by set or get')

@command()
def run(request, callable, *args, **kwargs):
    '''Execute a python *callable*.'''
    return callable(request.actor, *args, **kwargs)

@command(ack=False)
def stop(request):
    '''Stop the actor from running.'''
    return request.actor.stop()
    
@command()
def notify(request, info):
    '''The actor notify itself with a dictionary of information.
The command perform the following actions:

* Update the mailbox to the current consumer of the actor connection
* Update the info dictionary
* Returns the time of the update
'''
    t = time()
    remote_actor = request.caller
    if isinstance(remote_actor, ActorProxyMonitor):
        remote_actor.mailbox = request.connection.current_consumer
        info['last_notified'] = t
        remote_actor.info = info
        callback = remote_actor.callback
        # if a callback is still available, this is the first
        # time we got notified
        if callback:
            remote_actor.callback = None
            callback.callback(remote_actor)
    return t
    
@command()
def spawn(request, **kwargs):
    '''Spawn a new actor.'''
    return request.actor.spawn(**kwargs)

@command()
def info(request):
    ''' Returns information and statistics about the server as a json string'''
    return request.actor.info()
     
@command()
def kill_actor(request, aid):
    '''Kill an actor with id ``aid``. This command can only be executed by the
arbiter, therefore a valid sintax is only::

    send('arbiter', 'kill_actor', 'abc')

Return 'stopped abc` if succesful, otherwise it returns ``None``.
'''
    arb = request.actor 
    if arb.is_arbiter():
        arb.send(aid, 'stop')
        while arb.get_actor(aid):
            yield NOT_DONE
        yield 'stopped %s' % aid
