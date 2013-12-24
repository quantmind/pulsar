try:
    import pulsar
except ImportError:     # pragma nocover
    import sys
    sys.path.append('../../')
    import pulsar


from pulsar import arbiter, command

names = ['john', 'luca', 'carl', 'jo', 'alex']


@command()
def greetme(request, message):
    echo = 'Hello {}!'.format(message['name'])
    request.actor.logger.info(echo)
    return echo


def interact(actor, a=None):
    if a is None:
        a = yield actor.spawn(name='greeter')
    if names:
        name = names.pop()
        msg = yield actor.send(a, 'greetme', {'name': name})
        actor._loop.call_later(1, interact, actor, a)
    else:
        actor.stop()


def onstart(actor):
    actor._loop.call_soon(interact, actor)
    return actor


if __name__ == '__main__':
    arbiter(start=onstart).start()
