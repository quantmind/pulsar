.. _pulsar-advantage:

Pulsar Advantage
========================


Python 3 - Asyncio
--------------------
Pulsar is written in python 3 and it is built on top of asyncio_, the new
asynchronous module in the python standard library.

Flexibility
-------------------
Pulsar's codebase is relatively small, at about 20,000 lines of code.
The source code is open and it has a very :ref:`liberal license <license>`.
You can :ref:`fork the code <contributing>` add feature you need and send
us your patch.

Multiprocessing
-------------------
Multiprocessing is the default parallel execution mechanism, therefore
each pulsar components have been designed in a share nothing architecture.
Communication between workers is obtained via tcp sockets which
:ref:`exchange messages <tutorials-messages>` using the websocket protocol.
You can also run workers in threading mode.

Documented and Samples
---------------------------
Pulsar documentation is continuously updated and extended and there are several
examples in the :mod:`examples` module of the distribution directory.


.. _asyncio: http://python.readthedocs.org/en/latest/library/asyncio.html
