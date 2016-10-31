================
Using command
================

We take a look on how to spawn an actor which echoes us when
we send it a message.
The first method for achieving this is to write an
:ref:`actor command <actor_commands>`::

    from asyncio import ensure_future

    from pulsar import arbiter, command, spawn, send


    names = ['john', 'luca', 'carl', 'jo', 'alex']


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

        def __call__(self, a=None):
            ensure_future(self._work(a))

        async def _work(self, a=None):
            if a is None:
                a = await spawn(name='greeter')
            if names:
                name = names.pop()
                await send(a, 'greetme', {'name': name})
                self._loop.call_later(1, self, a)
            else:
                arbiter().stop()

    if __name__ == '__main__':
        Greeter()
