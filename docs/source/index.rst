================
Pulsar
================

A concurrent framework for Python. **Concurrent** here stands for asynchronous
and parallel execution. Tested in Windows and Linux, it requires python 2.6 up to python 3.3
in a single code base.


.. toctree::
   :maxdepth: 1
   
   overview
   design
   faq
   advantage
   api
   changelog
   http
   settings
   
.. Note::

    This documentation is incomplete and with several spelling errors.
    It will improve with time and, hopefully, with help from the community.
    In the mean time, a lot of insight can be gained by
    visiting and running, the :ref:`example applications <examples>`
    which are located in the ``examples`` module at the top level of
    pulsar distribution.

.. _apps-framework:

.. module:: pulsar.apps

Application Framework
=========================

Pulsar application framework is built on top of :mod:`pulsar` concurrent
framework. It is designed to facilitate the development of server-side applications
such as web servers, task queues or any asynchronous and/or parallel 
idea you may have in mind.
To write a new application, you subclass :class:`pulsar.Application` or
any of the shipped applications listed below, and implement some of
the callbacks available.

Currently, pulsar is shipped with the following applications which can be
found in the :mod:`pulsar.apps` module:

.. toctree::
   :maxdepth: 1
   
   apps/socket
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