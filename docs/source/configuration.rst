
=====================
Configurations
=====================

With pulsar several server configurations are possible.



.. _configuration-ioqueue:

Monitor with IO queue
=============================

it is possible to create a :class:`pulsar.Monitor` with a distributed
queue as communication mechanism.
In this case, actors controlled by the monitor will use a
:class:`pulsar.IOQueue` as I/O in their :class:`pulsar.IOLoop`.
For examle::

    from multiprocessing import Queue
    import pulsar
    
    m = pulsar.arbiter().add_monitor(pulsar.Monitor,'mymonitor',ioqueue=Queue())

Threaded WSGI
~~~~~~~~~~~~~~~~~

An example of this configuration is the threaded :ref:`WSGI server <apps-wsgi>`.
When using windows with python 2.6 or 2.7, it is not possible to
reconstruct the socket from its file descriptor, which makes the
use of multiprocessing not possible.

In this configuration, the workers and the monitor use a the ``Queue`` class
from the python ``threading`` module as mechanism for exchanging web requests.



Task Queue
~~~~~~~~~~~~~~~~

A more relevant example of this configuration is muti-threaded or
muti-process task queue application.

