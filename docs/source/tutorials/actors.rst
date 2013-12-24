

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

Note that the arbiter is a singleton, therefore calling :ref:`.arbiter`
multiple times always return the same object.

To run the arbiter::

    >>> a.start()

This command will start the arbiter event loop, and therefore no more inputs
are read from the command line, not that useful!


Using command
~~~~~~~~~~~~~~~
We take a look on how to spawn an actor which echos us when we send it a message.
The first method for achieving this is to write an
:ref:`actor command <actor_commands>`::

    from pulsar import command

    @command()
    def greetme(request, message):
        echo = 'Hello {}!'.format(message['name'])
        print(echo)
        return echo

    def on_start(actor):
        a = yield spawn(name='greeter')
        msg = yield send(a, 'greetme', {'name': 'John'})
        actor.stop()



There are several way to achieve this simple outcome using pulsar, however they all
use the ref:`start hook <actor-hooks>`.

    def on_start(actor):

