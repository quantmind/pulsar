from time import time

from pulsar import AuthenticationError
from . import proxy 

#############################################################  COMMANDS

@proxy.command()
def auth(client, actor, password):
    '''Request for authentication in a password protected Pulsar server.
Pulsar can be instructed to require a password before allowing clients to
execute commands.'''
    p = actor.cfg.get('password')
    if p and p != password:
        client.connection.authenticated = False
        raise AuthenticationError()
    else:
        client.connection.authenticated = True
        return True

@proxy.command()
def ping(client, actor):
    return 'pong'

@proxy.command()
def echo(client, actor, message):
    '''Returns mmessage'''
    return message

@proxy.command(ack=False)
def quit(client, actor):
    '''Ask the server to close the connection. The connection is closed as
soon as all pending replies have been written to the client.'''
    client.connection.close()

@proxy.command(authenticated=True)
def info(client, actor):
    ''' Returns information and statistics about the server as a json string'''
    return actor.info()

@proxy.command(authenticated=True)
def shutdown(client, actor):
    '''Shutdown the server'''
    return actor.stop()

@proxy.command(authenticated=True)
def config(client, actor, setget, name, *value):
    setget = setget.lower()
    if setget == 'get':
        if len(values) > 0:
            raise CommandError('"config get" accept only one parameter')
        return client.actor.cfg.get(name)
    elif setget == 'set':
        if len(values) > 1:
            raise CommandError()
        client.actor.cfg.set(name, value[0])
    else:
        raise CommandError('config must be followed by set or get')


###########################################################  INTERNAL COMMANDS
@proxy.command(internal=True)
def mailbox_address(client, actor, caller, address):
    '''The remote *caller* register its mailbox ``address``.'''
    actor.link_actor(caller, address)
    return actor.proxy

@proxy.command(internal=True)
def run(client, actor, caller, callable, *args, **kwargs):
    '''Execute a python script in the server'''
    return callable(actor, *args, **kwargs)

@proxy.command(ack=False, internal=True)
def stop(client, actor, caller):
    '''Stop the actor from running. Same as shut_down but for internal use'''
    return actor.stop()
    
@proxy.command(ack=False, internal=True)
def notify(client, actor, caller, info):
    '''caller notify itself.'''
    if isinstance(info, dict):
        info['last_notified'] = time()
        caller.info = info
    
###########################################################  ARBITER COMMANDS
@proxy.command(internal=True, commands_set=proxy.arbiter_commands)
def spawn(client, actor, caller, linked_actors=None, **kwargs):
    linked_actors = linked_actors if linked_actors is not None else {}
    linked_actors[caller.aid] = caller
    return actor.spawn(linked_actors=linked_actors, **kwargs)
     
@proxy.command(authenticated=True, commands_set=proxy.arbiter_commands)
def kill_actor(client, actor, aid=None):
    '''Kill an actor with id ``aid``'''
    if not aid:
        return actor.stop()
    else:
        a = actor.get_actor(aid)
        if a:
            a.stop()
            return 'stopped %s' % a
        else:
            actor.logger.info('Could not kill "%s" no such actor', aid)
        