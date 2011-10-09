.. _design:

=====================
Design
=====================

The building block of pulsar is the :class:`pulsar.Actor` class.

Server Model
==================

When running as server, Pulsar has central master process that manages
a set of worker pools. In multi-processing mode, the master never knows anything
about individual clients. All requests and responses are handled completely by worker pools.



Actors
=================

Event loop
~~~~~~~~~~~~~~~
Each actor has its own instance of an :class:`pulsar.IOLoop` to perform its
normal operations. The :attr:`pulsar.Actor.ioloop` is initiated just after
forking.
Once the event loop is created, the actor add itself to
:ref:`the event loop tasks <ioloop-tasks>`, so that it can perform
its operations at each iteration in the event loop.
 

.. _remote-functions:

Remote functions
~~~~~~~~~~~~~~~~~~~~~~~~



.. _actor-callbacks:

Actor Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~

:class:`pulsar.Actor` exposes five callback functions which can be
used to customize the behaviour of the actor.

 * :meth:`pulsar.Actor.on_init` called just after initialization after forking.
 * :meth:`pulsar.Actor.on_start` called just before the actor event loop starts.
 * :meth:`pulsar.Actor.on_task` called at every actor event loop.
 * :meth:`pulsar.Actor.on_stop`.
 * :meth:`pulsar.Actor.on_exit`.
 * :meth:`pulsar.Actor.on_info`.

These functions do nothing in the :class:`pulsar.Actor` implementation. 

.. _gunicorn: http://gunicorn.org/


Event Loop
====================


.. _ioloop-tasks:

Event loop tasks
~~~~~~~~~~~~~~~~~~~~~~



.. _application-framework:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`pulsar.Application` class.