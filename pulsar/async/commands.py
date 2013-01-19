from time import time

from pulsar import AuthenticationError
from . import proxy 

#############################################################  COMMANDS

@proxy.command()
def ping(request):
    return 'pong'

@proxy.command()
def echo(request, message):
    '''Returns *message*'''
    return message

@proxy.command()
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

@proxy.command()
def run(request, callable, *args, **kwargs):
    '''Execute a python script in the server'''
    return callable(request.actor, *args, **kwargs)

@proxy.command(ack=False)
def stop(request, aid=None):
    '''Stop the actor from running.'''
    return request.actor.stop()
    
@proxy.command(ack=False)
def notify(request, info):
    '''caller notify itself.'''
    if isinstance(info, dict):
        info['last_notified'] = time()
        request.caller.info = info
    
@proxy.command()
def spawn(request, linked_actors=None, **kwargs):
    linked_actors = linked_actors if linked_actors is not None else {}
    linked_actors[request.caller.aid] = request.caller
    return request.actor.spawn(linked_actors=linked_actors, **kwargs)

@proxy.command()
def info(request):
    ''' Returns information and statistics about the server as a json string'''
    return request.actor.info()
     
@proxy.command()
def auth(request, password):
    '''Request for authentication in a password protected Pulsar server.
Pulsar can be instructed to require a password before allowing clients to
execute commands.'''
    actor = request.actor
    p = actor.cfg.get('password')
    if p and p != password:
        request.connection.authenticated = False
        raise AuthenticationError
    else:
        request.connection.authenticated = True
        return True
    
@proxy.command()
def kill_actor(request, aid=None):
    '''Kill an actor with id ``aid``'''
    if not aid:
        return request.actor.stop()
    else:
        a = request.actor.get_actor(aid)
        if a:
            a.stop()
            return 'stopped %s' % a
        else:
            request.actor.logger.info('Could not kill "%s" no such actor', aid)
        