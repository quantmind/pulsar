.. _apps-framework:

.. module:: pulsar.apps

=============================
Application framework
=============================

Pulsar application framework is built on top of :mod:`pulsar` concurrent
framework. It is designed to facilitae the development of server-side applications
such as web servers, task queues or any type asynchronous and/or parallel 
idea you may have.

The idea is simple, you write a new application class by subclassing
:class:`pulsar.Application` and by implementing some of the callbacks available.