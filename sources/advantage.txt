.. _pulsar-advantage:

Pulsar Advantage
========================


Python 3
--------------------
Pulsar was written using python 3.2 and backported to python 2.7 and python 2.6.
It is now developed in python 3.3 while keeping backward compatibility.


Flexibility
-------------------
Pulsar's codebase is relatively small, at about 15,000 lines of code. The source code is
open and it has a very :ref:`liberal license <license>`.
You can :ref:`fork the code <contributing>` add feature you need and send us your patch.

Multiprocessing
-------------------
Multiprocessing is the default parallel execution mechanism, therefore each pulsar
components have been designed in a share nothing architecture. Communication between
workers is obtained via tcp sockets which :ref:`exchange messages <tutorials-messages>`
using the websocket protocol. You can also run workers in threading mode.

Pep 3156 Ready
----------------
Pulsar internals are implemented along the lines of the new asynchronous IO
proposal pep-3156_. This means that once the new asynchronous interface will
be part of the standard lib, pulsar will be compatible with it from day 1.

Documented and Samples
---------------------------
Pulsar documentation is continuously updated and extended and there are several
examples in the :mod:`examples` module of the distribution directory. 


.. _pep-3156: http://www.python.org/dev/peps/pep-3156/