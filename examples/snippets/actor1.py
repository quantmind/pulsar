'''Simple actor message passing
'''
from pulsar import arbiter, spawn, send, async, Config


def start(arbiter, **kw):
    async(app(arbiter))


def app(arbiter):
    # Spawn a new actor
    proxy = yield from spawn(name='actor1')
    print(proxy.name)
    # Execute inner method in actor1
    result = yield from send(proxy, 'run', inner_method)
    print(result)

    yield from send(proxy, 'run', set_value, 10)
    value = yield from send(proxy, 'run', get_value)
    print(value)

    # Stop the application
    arbiter.stop()


def inner_method(actor):
    return 42


def set_value(actor, value):
    actor.saved_value = value


def get_value(actor):
    return actor.saved_value


if __name__ == '__main__':
    cfg = Config()
    cfg.parse_command_line()
    arbiter(cfg=cfg, start=start).start()
