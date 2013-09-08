================
Pulsar
================

A concurrent framework for Python. **Concurrent** here stands for asynchronous
(event driven) and parallel execution. Tested in Windows and Linux,
it requires python 2.6 up to python 3.3 in a single code base.


.. toctree::
   :maxdepth: 1
   
   overview
   design
   faq
   advantage
   tutorials/index
   api/index
   changelog
   settings

.. _apps-framework:

Application Framework
=========================

Pulsar application framework is built on top of :mod:`pulsar` concurrent
framework. It is designed to facilitate the development of both server-side
applications such as web servers, task queues as well as asynchronous
clients.

.. toctree::
   :maxdepth: 1
   
   apps/socket
   apps/wsgi/index
   apps/tasks/index
   apps/test
   apps/shell
   apps/rpc
   apps/websockets
   apps/http
   apps/pubsub
   apps/twisted
   apps/pulse
   apps/green
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

