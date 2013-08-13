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

Is pulsar another twisted?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
No, pulsar is a concurrent framework based on Actor primitives, check the
:ref:`design documentation <design>` for more information. Twisted is a library
which can be used to write a pulsar equivalent and it has a vast array of
protocols which pulsar will never have. However, pulsar and twisted have in common
the :class:`pulsar.Deferred` implementation.
You can use twisted protocols within pulsar if you like, check
:ref:`pulsar & twisted integration <tutorials-twisted>` for more information.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It depends on the WSGI application serving the request.



Internals
---------------

How is inter-actor message passing implemented?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the :ref:`actor messages documentation <tutorials-messages>`.


Tips
-------------

Pause execution asynchronously
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Within a :ref:`coroutine <coroutine>` you can pause execution by using
the :func:`pulsar.async_sleep` function. The function switches task and resumes
the coroutine after *timeout* seconds.