from pulsar import arbiter, task, command, spawn, send

names = ['john', 'luca', 'jo', 'alex']


@command()
def greetme(request, message):
    echo = 'Hello {}!'.format(message['name'])
    request.actor.logger.info(echo)
    return echo


class Greeter:

    def __init__(self):
        a = arbiter()
        self._loop = a._loop
        self._loop.call_later(1, self)
        a.start()

    @task
    def __call__(self, a=None):
        if a is None:
            a = yield spawn(name='greeter')
        if names:
            name = names.pop()
            send(a, 'greetme', {'name': name})
            self._loop.call_later(1, self, a)
        else:
            arbiter().stop()


if __name__ == '__main__':
    Greeter()
