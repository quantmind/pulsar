
=====================
Design
=====================

The building block of pulsar is the :class:`pulsar.Actor` class.

Server Model
==================

When running as server, Pulsar has central master process that manages
a set of worker pools. In multi-processing mode, the master never knows anything
about individual clients. All requests and responses are handled completely by worker pools.



.. _gunicorn: http://gunicorn.org/
