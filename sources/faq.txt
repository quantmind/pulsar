.. _faq:

.. module:: pulsar

FAQ
===========

This is a list of Frequently Asked Questions regarding pulsar.

.. contents::
    :local:


General
---------------------


What is pulsar?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Check the :ref:`overview <intro-overview>`.

Why should I use pulsar?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Check the :ref:`pulsar advantage <pulsar-advantage>`.

What is an actor?
~~~~~~~~~~~~~~~~~~~~~~
Check the :ref:`actor design documentation <design-actor>`.

How pulsar handle asynchronous data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Check the :ref:`asynchronous components documentation <tutorials-coroutine>`.

How is pulsar different from Twisted?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pulsar is written for python 3.4 or above, Twisted is python 2. Pulsar is written
on top of asyncio_ and the multiprocessing_ module, twisted uses its own
implementations. In pulsar each actor has its own event loop, twisted as a global
event loop (reactor) in the main process. Apart from all this, the underlying
philosophy is very similar, to use an event-loop to register file descriptors
and wait for events.

How is pulsar different from Tornado?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pulsar is written for python 3.4 or above, Tornado is python 2 and python 3 compatible.
This sounds like a big bonus for Tornado, however python 2 & 3 compatibility
means Tornado cannot use some of the best features of python 3.
Tornado is mainly a web framework, pulsar is not, with pulsar one can write
asynchronous multiprocessing applications not just for the web.
In addition Tornado does not provide multiprocessing out of the box.
Like Twisted and Pulsar, Tornado uses an event-loop to listen for events on
file descriptors and therefore its core implementation is not too dissimilar.

Socket Servers
--------------------

Can I run a web-server with multiple process?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes you can, both posix and windows, Check :ref:`wsgi in multi process <multi-wsgi>`.


WSGI Server
-----------------

Is pulsar WSGI server pep 3333 compliant?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes it is, to the best of our knowledge. If you find an issue,
please :ref:`file a bug report <contributing>`.

Is a WSGI response asynchronous?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It depends on the WSGI application serving the request. Blocking application
such as django or flask can be made more pulsar-friendly by using the
:func:`.middleware_in_executor` utility as explained in the
:ref:`wsgi tutorial <blocking-wsgi>`.


Logging
---------------

log level
~~~~~~~~~~~~~~~~

Pulsar uses the :ref:`log-level <setting-loglevel>` setting to control
logging level on the command line or on your :ref:`config <setting-config>`
file::

    python script.py --log-level debug

Did you know you can pass several namespaces to ``--log-level``::

    python script.py --log-level debug asyncio.warning


Internals
---------------

How is inter-actor message passing implemented?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the :ref:`actor messages documentation <tutorials-messages>`.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _multiprocessing: http://docs.python.org/library/multiprocessing.html
