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


Is pulsar another Twisted?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
No, pulsar is a concurrent framework based on Actor primitive, check the
:ref:`design documentation <design>` for more information. Twisted is a library
which can be used to write a pulsar equivalent and it has a vast array of
protocols which pulsar will never have. However, pulsar and twisted have in common
the Deferred implementation. One could, in theory, use twisted protocols to write a
pulsar :ref:`socket server application <apps-socket>`.


Socket Servers
--------------------

