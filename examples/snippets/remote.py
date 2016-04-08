from pulsar import arbiter, spawn, send, ensure_future, Config, command


PREFIX = 'remote_'


@command()
def remote_call(request, cls, method, args, kw):
    '''Command for executing remote calls on a remote object
    '''
    actor = request.actor
    name = 'remote_%s' % cls.__name__
    if not hasattr(actor, name):
        object = cls(actor)
        setattr(actor, name, object)
    else:
        object = getattr(actor, name)
    method_name = '%s%s' % (PREFIX, method)
    return getattr(object, method_name)(request, *args, **kw)


class RemoteType(type):
    '''Metaclass for remote objects.

    Collect a set of method prefixed with ``remote_``
    '''
    def __new__(cls, name, bases, attrs):
        remotes = set()
        for base in bases[::-1]:
            if hasattr(base, 'remote_methods'):
                remotes.update(base.remote_methods)

        for key, method in list(attrs.items()):
            if hasattr(method, '__call__') and key.startswith(PREFIX):
                method_name = key[len(PREFIX):]
                remotes.add(method_name)
        for base in bases[::-1]:
            if hasattr(base, 'remote_methods'):
                remotes.update(base.remote_methods)

        attrs['remote_methods'] = frozenset(remotes)
        return super().__new__(cls, name, bases, attrs)


class RemoteCall:
    __slots__ = ('remote', 'name')

    def __init__(self, remote, name):
        self.remote = remote
        self.name = name

    def __call__(self, *args, **kwargs):
        return send(self.remote.proxy, 'remote_call', self.remote.__class__,
                    self.name, args, kwargs)


class Remote(metaclass=RemoteType):
    '''Base class for Remote Objects

    Subclass your remote object from this class and add `remote_*`
    methods which are executed in the remote actor.
    '''
    proxy = None

    def __init__(self, proxy):
        self.proxy = proxy

    @classmethod
    async def spawn(cls, **kwargs):
        proxy = await spawn(**kwargs)
        return cls(proxy)

    def __getattr__(self, name):
        if name in self.remote_methods:
            return RemoteCall(self, name)
        else:
            return getattr(self.proxy, name)


# EXAMPLE APPLICATION

class Calculator(Remote):
    '''A simple example
    '''
    value = 0

    def remote_set_value(self, request, value):
        request.actor.logger.info('Setting value')
        self.value = value

    def remote_get_value(self, request):
        request.actor.logger.info('Getting value')
        return self.value


def start(arbiter, **kw):
    ensure_future(app(arbiter))


async def app(arbiter):
    # Spawn a new actor
    calc = await Calculator.spawn(name='calc1')
    print(calc.name)
    # set value in the remote calculator
    await calc.set_value(46)
    # get value from the remote calculator
    value = await calc.get_value()
    print(value)

    # Stop the application
    arbiter.stop()


if __name__ == '__main__':
    cfg = Config()
    cfg.parse_command_line()
    arbiter(cfg=cfg, start=start).start()
