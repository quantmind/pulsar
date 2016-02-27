

The Arbiter
~~~~~~~~~~~~~~~~~~
To use actors in pulsar you need to start the :ref:`Arbiter <arbiter>`,
a very special actor which controls the life of all actors spawned during the
execution of your code.

The obtain the arbiter::

    >>> from pulsar import arbiter
    >>> a = arbiter()
    >>> a.is_running()
    False

Note that the arbiter is a singleton, therefore calling :func:`.arbiter`
multiple times always return the same object.

To run the arbiter::

    >>> a.start()

This command will start the arbiter :ref:`event loop <asyncio-event-loop>`,
and therefore no more inputs are read from the command line!


Using command
~~~~~~~~~~~~~~~
We take a look on how to spawn an actor which echos us when
we send it a message.
The first method for achieving this is to write an
:ref:`actor command <actor_commands>`::

    from pulsar import arbiter, task, command, spawn, send

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

        @task
        def __call__(self, a=None):
            if a is None:
                a = yield from spawn(name='greeter')
            if names:
                name = names.pop()
                send(a, 'greetme', {'name': name})
                self._loop.call_later(1, self, a)
            else:
                arbiter().stop()

    if __name__ == '__main__':
        Greeter()
