"""Simple actor message passing"""
from pulsar import arbiter, spawn, send, ensure_future, Config


def start(arbiter, **kw):
    ensure_future(app(arbiter))


async def app(arbiter):
    # Spawn a new actor
    proxy = await spawn(name='actor1')
    print(proxy.name)
    # Execute inner method in actor1
    result = await send(proxy, 'run', inner_method)
    print(result)

    await send(proxy, 'run', set_value, 10)
    value = await send(proxy, 'run', get_value)
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
