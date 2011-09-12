
=====================
Design
=====================

The building block of pulsar is the :class:`pulsar.Actor` class.

Server Model
==================

When running as server, Pulsar has central master process that manages
a set of worker pools. In multi-processing mode, the master never knows anything
about individual clients. All requests and responses are handled completely by worker pools.


Actor Callbacks
===================

:class:`pulsar.Actor` exposes four callback functions which can be
used to customize the behaviour of the actor.

 * :meth:`pulsar.Actor.on_start` called just after forking.
 * :meth:`pulsar.Actor.on_task` called at every actor event loop.
 * :meth:`pulsar.Actor.on_stop`.
 * :meth:`pulsar.Actor.on_exit`.

These functions do nothing in the :class:`pulsar.Actor` implementation. 

.. _gunicorn: http://gunicorn.org/
