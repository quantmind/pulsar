from .actor import get_actor

def authenticated(f):
    def _(client, *args, **kwargs):
        if not client.actor.authenticated:
            raise AuthenticationError()
        else:
            return f(client, *args, **kwargs)
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _

class internal(object):
    
    def __init__(self, ack=True):
        self.ack = ack
        
    def __call__(self, f):
        def _(client, caller, *args, **kwargs):
            r = f(client, caller, *args, **kwargs)
            return r
        

def auth(client, password):
    '''Request for authentication in a password protected Pulsar server.
Pulsar can be instructed to require a password before allowing clients to
execute commands.'''
    p = client.actor.cfg.get('password')
    if p and p != password:
        client.authenticated = False
    else:
        client.authenticated = True
    return client.authenticated

def ping(client):
    return 'pong'

def echo(client, message):
    '''Returns mmessage'''
    return message

def quit(client):
    '''Ask the server to close the connection. The connection is closed as
soon as all pending replies have been written to the client.'''
    client.close()

@authenticated
def info(client):
    return client.actor.info()

@authenticated
def shutdown(client):
    return client.actor.stop()

@authenticated
def config(client, setget, name, *value):
    setget = setget.lower()
    if setget == 'get':
        if len(values) > 0:
            raise CommandError()
        return client.actor.cfg.get(name)
    elif setget == 'set':
        if len(values) > 1:
            raise CommandError()
        client.actor.cfg.set(name, value[0])
    else:
        raise CommandError('config must be followed by set or get')
    
@authenticated
def execute(client, code):
    '''Execute a python script in the server'''
    return

@internal
def notify(client, caller, info):
    caller.info = info
    
