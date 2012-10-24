.. _design:

.. module:: pulsar

=====================
Design
=====================

Pulsar is a concurrent framework for python. It implements a double layer
of components for building a vast array of parallel and asynchronous
applications.

* The first layer is based on the building block of pulsar library,
  the :class:`Actor` class.
* The second layer is the based on the :class:`Application`
  which is built on top of the :class:`Actor` model.
   

Server Model
==================

When running as server, Pulsar has central master process that manages
a set of worker pools. In multi-processing mode, the master never knows anything
about individual clients. All requests and responses are handled completely by worker pools.



Actors
=================

Event loop
~~~~~~~~~~~~~~~
Each actor has its own :class:`IOLoop` to perform its normal operations.
The :attr:`Actor.ioloop` is initiated just after
forking.
Once the event loop is created, the actor add itself to
:ref:`the event loop tasks <ioloop-tasks>`, so that it can perform
its operations at each iteration in the event loop.
 
IO-bound
~~~~~~~~~~~~~~~
The most common :class:`Actor` has a :meth:`Actor.requestloop` which tells
the operating system (through `epoll` or `select`) that it should be notified
when a new connection is made, and then it goes to sleep.
Serving the new request should occur as fast as possible so that other
connections can be served simultaneously. 

.. _cpubound:

CPU-bound
~~~~~~~~~~~~~~~
The second type of :class:`Actor` can be used to perform CPU intensive
operations, such as calculations, data manipulation or whatever you need
them to do. CPU-bound :class:`Actors` have the following properties:

* Their :attr:`Actor.requestloop` listen for requests for a distributed queue
  rather than from a socket.
* Once they receive a new requests, they can block their request loop
  for a long time. 
* In addition to their request loop, they have an I/O eventloop running on a
  separate thread.
   

.. _remote-functions:

Remote functions
~~~~~~~~~~~~~~~~~~~~~~~~



.. _actor-callbacks:

Actor Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~

:class:`Actor` exposes five callback functions which can be
used to customize the behaviour of the actor.

 * :meth:`Actor.on_init` called just after initialization after forking.
 * :meth:`Actor.on_start` called just before the actor event loop starts.
 * :meth:`Actor.on_task` called at every actor event loop.
 * :meth:`Actor.on_stop`.
 * :meth:`Actor.on_exit`.
 * :meth:`Actor.on_info`.
 * :meth:`Actor.on_message`.
 * :meth:`Actor.on_message_processed`.

These functions do nothing in the :class:`Actor` implementation. 

.. _gunicorn: http://gunicorn.org/


Event Loop
====================


.. _ioloop-tasks:

Event loop tasks
~~~~~~~~~~~~~~~~~~~~~~


.. _remote-actions:

Actor remote actions
========================
Actors communicate with each other by sending *actions* with or
without parameters. Furthermore, some actions can require authentication while
other can only be executed internally by pulsar.


ping
~~~~~~~

Ping a remote *arbiter* and recive an asynchronous `'pong``::

    send(arbiter, 'ping')


echo
~~~~~~~

received an asynchronous echo from a remote *arbiter*::

    send('echo', arbiter, 'Hello!')


.. _application-framework:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`Application` class.