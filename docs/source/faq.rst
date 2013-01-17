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

Is pulsar another twisted?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
No, pulsar is a concurrent framework based on Actor primitive, check the
:ref:`design documentation <design>` for more information. Twisted is a library
which can be used to write a pulsar equivalent and it has a vast array of
protocols which pulsar will never have. However, pulsar and twisted have in common
the Deferred implementation. One could, in theory, use twisted protocols to write a
pulsar :ref:`socket server application <apps-socket>`.


Socket Servers
--------------------

Can I run a Web-server with multiple process?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes you can, in posix always, in windows only for python 3.2 or above.
Check :ref:`wsgi in multi process <multi-wsgi>`.


