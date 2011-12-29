================
Pulsar
================

An asynchronous concurrent framework for Python. Tested in Windows and Linux,
it requires python 2.6 up to python 3.3 in a single code base.


.. toctree::
   :maxdepth: 1
   
   overview
   changelog
   
Concurrency Framework
=========================

.. toctree::
   :maxdepth: 1
   
   design
   configuration
   api
   internals
   

.. _apps-framework:

.. module:: pulsar.apps

Application Framework
=========================

Pulsar application framework is built on top of :mod:`pulsar` concurrent
framework. It is designed to facilitate the development of server-side applications
such as web servers, task queues or any type asynchronous and/or parallel 
idea you may have.

The idea is simple, you write a new application class by subclassing
:class:`pulsar.Application` and by implementing some of the callbacks available.

.. toctree::
   :maxdepth: 1
   
   apps/api
   settings
   apps/wsgi
   apps/rpc
   apps/tasks
   apps/websockets
   apps/test
   apps/shell
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`