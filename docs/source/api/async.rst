.. module:: pulsar

.. _async-api:

==================
Asynchonous API
==================

Event loop
=================

Timed Call
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TimedCall
   :members:
   :member-order: bysource
   
   
EventLoop
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EventLoop
   :members:
   :member-order: bysource
   
     
IOQueue
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: IOQueue
   :members:
   :member-order: bysource
   
   
Asyncronous Tools
=====================

This section describes the asynchronous utilities used throughout the library
and which form the building block of the event driven concurrent framework.
While :class:`Actor` represents the concurrent side of pulsar,
the :class:`Deferred` adds the asynchronous flavour to it by using callbacks
functions similar to twisted_.

Make Async
~~~~~~~~~~~~~~~~~

.. autofunction:: make_async


Safe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: safe_async


Maybe Async
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: maybe_async


Deferred
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Deferred
   :members:
   :member-order: bysource
   
Multi Deferred
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: MultiDeferred
   :members:
   :member-order: bysource
   
Deferred Generator
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DeferredGenerator
   :members:
   :member-order: bysource
   
Failure
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Failure
   :members:
   :member-order: bysource


Decorators
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: async

.. autofunction:: multi_async

.. autofunction:: raise_failure



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/