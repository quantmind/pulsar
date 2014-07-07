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


Socket Servers
--------------------

Can I run a web-server with multiple process?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes you can, in posix always, in windows only for python 3.2 or above.
Check :ref:`wsgi in multi process <multi-wsgi>`.


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

Pulsar uses the :ref:`log-level <setting_log-level>` setting to control
logging level on the command line or on your :ref:`config <setting_config`
file::

    python script.py --log-level debug

Did you know you can pass several namespaces to ``--log-level``::

    python script.py --log-level debug asyncio.warning


Internals
---------------

How is inter-actor message passing implemented?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the :ref:`actor messages documentation <tutorials-messages>`.

