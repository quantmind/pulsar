from time import time

from pulsar import AuthenticationError
from .proxy import command, CommandError

#############################################################  COMMANDS

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
def stop(request, aid=None):
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
    remote_actor.mailbox = request.connection.current_consumer
    info['last_notified'] = t
    remote_actor.info = info
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
    
@command()
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
        