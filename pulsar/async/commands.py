from time import time

from pulsar import CommandError

from .proxy import command, ActorProxyMonitor
from .futures import async_while


@command()
def ping(request):
    return 'pong'


@command()
def echo(request, message):
    '''Returns *message*'''
    return message


@command()
def config(request, setget, name, *values):
    setget = setget.lower()
    if setget == 'get':
        if len(values) > 0:
            raise CommandError('"config get" accept only one parameter')
        return request.actor.cfg.get(name)
    elif setget == 'set':
        if len(values) > 1:
            raise CommandError('"config get" accept only two parameters')
        request.actor.cfg.set(name, values[0])
        return True
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
    actor = request.actor
    remote_actor = request.caller
    if isinstance(remote_actor, ActorProxyMonitor):
        remote_actor.mailbox = request.connection
        info['last_notified'] = t
        remote_actor.info = info
        callback = remote_actor.callback
        # if a callback is still available, this is the first
        # time we got notified
        if callback:
            remote_actor.callback = None
            callback.set_result(remote_actor)
            if actor.cfg.debug:
                actor.logger.debug('Got first notification from %s',
                                   remote_actor)
        elif actor.cfg.debug:
            actor.logger.debug('Got notification from %s', remote_actor)
    else:
        actor.logger.warning('notify got a bad actor')
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
async def kill_actor(request, aid, timeout=5):
    '''Kill an actor with id ``aid``.
    This command can only be executed by the arbiter,
    therefore a valid sintax is only::

        send('arbiter', 'kill_actor', 'abc')

    Return 'killed abc` if successful, otherwise it returns ``None``.
    '''
    arb = request.actor
    if arb.is_arbiter():
        await arb.send(aid, 'stop')
        proxy = await async_while(timeout, arb.get_actor, aid)
        if proxy:
            arb.logger.warning('Could not kill actor %s', aid)
        else:
            return 'killed %s' % aid
