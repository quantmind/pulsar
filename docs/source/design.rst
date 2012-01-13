.. _design:

=====================
Design
=====================

Pulsar is a concurrent framework for python. It implements a double layer
of components for building a vast array of parallel and asynchronous
applications.

* The first layer is based on the building block of pulsar library,
  the :class:`pulsar.Actor` class.
* The second layer is the based on the :class:`pulsar.Application`
  which is built on top of the :class:`pulsar.Actor` model.
   

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
 * :meth:`pulsar.Actor.on_message`.
 * :meth:`pulsar.Actor.on_message_processed`.

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