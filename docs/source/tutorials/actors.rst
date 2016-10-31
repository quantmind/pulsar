
==================
The Arbiter
==================

To use actors in pulsar you need to start the :ref:`Arbiter <design-arbiter>`,
a very special actor which controls the life of all actors spawned during the
execution of your code.

The obtain the arbiter::

    >>> from pulsar import arbiter
    >>> a = arbiter()
    >>> a.is_running()
    False

Note that the arbiter is a singleton, therefore calling :func:`~.arbiter`
multiple times always return the same object.

To run the arbiter::

    >>> a.start()

This command will start the arbiter :ref:`event loop <asyncio-event-loop>`,
and therefore no more inputs are read from the command line!
