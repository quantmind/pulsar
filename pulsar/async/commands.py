from pulsar import AuthenticationError

global_commands_table = {}
actor_commands = set()
arbiter_commands = set()

class pulsar_command:
    '''Decorator for pulsar command functions'''
    def __init__(self, ack=True, authenticated=False, internal=False,
                 commands_set=None):
        self.ack = ack
        self.authenticated = authenticated
        self.internal = internal
        if commands_set is None:
            commands_set = actor_commands
        self.commands_set = commands_set
    
    def __call__(self, f):
        name = f.__name__.lower()
        
        def command_function(client, actor, *args, **kwargs):
            if self.authenticated:
                password = actor.cfg.get('password')
                if password and not client.connection.authenticated:
                    raise AuthenticationError()
            return f(client, actor, *args, **kwargs)
        
        command_function.ack = self.ack
        command_function.internal = self.internal
        command_function.__name__ = name
        command_function.__doc__ = f.__doc__
        self.commands_set.add(name)
        global_commands_table[name] = command_function
        return command_function

command = lambda f: pulsar_command()(f)
authenticated = lambda f: pulsar_command(authenticated=True)(f)
internal = lambda f: pulsar_command(internal=True)(f)


def get(name, commands_set=None):
    '''Get the command function *name*'''
    name = name.lower()
    if commands_set and name not in commands_set:
        return
    return global_commands_table.get(name)

#############################################################  COMMANDS

@command
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

@command
def ping(client, actor):
    return 'pong'

@command
def echo(client, actor, message):
    '''Returns mmessage'''
    return message

@pulsar_command(ack=False)
def quit(client, actor):
    '''Ask the server to close the connection. The connection is closed as
soon as all pending replies have been written to the client.'''
    client.connection.close()

@authenticated
def info(client, actor):
    ''' Returns information and statistics about the server as a json string'''
    return actor.info()

@authenticated
def shutdown(client, actor):
    '''Shutdown the server'''
    return actor.stop()

@authenticated
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
    
@authenticated
def run_code(client, code):
    '''Execute a python script in the server'''
    return

###########################################################  INTERNAL COMMANDS
@internal
def mailbox_address(client, actor, caller, address):
    '''The remote *caller* register its mailbox ``address``.'''
    if address:
        actor.log.debug('Registering %s inbox address %s', caller, address)    
    actor.link_actor(caller, address)
    return actor.proxy

@internal
def run(client, actor, caller, callable):
    '''Execute a python script in the server'''
    return callable(actor)

@pulsar_command(ack=False, internal=True)
def stop(client, actor, caller):
    '''Stop the actor from running. Same as shut_down but for internal use'''
    return actor.stop()
    
@pulsar_command(ack=False, internal=True)
def notify(client, actor, caller, info):
    '''caller notify itself.'''
    caller.info = info
    
###########################################################  ARBITER COMMANDS
@pulsar_command(internal=True, commands_set=arbiter_commands)
def spawn(client, actor, caller, linked_actors=None, **kwargs):
    linked_actors = linked_actors if linked_actors is not None else {}
    linked_actors[caller.aid] = caller
    return actor.spawn(linked_actors=linked_actors, **kwargs)
     
@pulsar_command(authenticated=True, commands_set=arbiter_commands)
def kill_actor(client, actor, aid):
    '''Kill an actor with id ``aid``'''
    a = actor.get_actor(aid)
    if a:
        a.stop()
        return 'stopped {0}'.format(a)
    else:
        self.log.info('Could not kill "%s" no such actor', aid)
        