.. _pulsar-advantage:

Pulsar Advantage
========================


Python 3
--------------------
Pulsar was written using python 3.2 and backported to python 2.7 and python 2.6.
It is now developed in python 3.3 while keeping backward compatibility.


Flexibility
-------------------
Pulsar's codebase is small, at about 14,000 lines of code. The source code is
open and it has a very :ref:`liberal license <license>`.
You can :ref:`fork the code <contributing>` add feature you need and send us your patch.

Multiprocessing
-------------------
Multiprocessing is the default parallel execution mechanism, therefore each pulsar
components have been designed in a share nothing architecture. Communication between
workers is obtained via tcp sockets which :ref:`exchange messages <tutorials-messages>`
using the websocket protocol. You can also run workers in threading mode.