.. _apps-tasks:

=============================
Distributed Task Queue
=============================

.. automodule:: pulsar.apps.tasks

Tasks
============

A :class:`Task` can have one of the following `status`:

* ``PENDING`` A task waiting for execution and unknown.
* ``RECEIVED`` when the task is received by the task queue.
* ``STARTED`` task execution has started.
* ``REVOKED`` the task execution has been revoked. One possible reason could be
  the task has timed out.
* ``SUCCESS`` task execution has finished with success.
* ``FAILURE`` task execution has finished with failure.



Utilities
=================

.. autoclass:: TaskQueueRpcMixin
   :members:
   :member-order: bysource
   
   
API
===============


Taskqueue and Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskQueue
   :members:
   :member-order: bysource


Job Registry
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JobRegistry
   :members:
   :member-order: bysource

Job meta Class
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JobMetaClass
   :members:
   :member-order: bysource

Job
~~~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: Job
   :members:
   :member-order: bysource
   
Periodic Job
~~~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: PeriodicJob
   :members:
   :member-order: bysource

Task
~~~~~~~~

.. autoclass:: Task
   :members:
   :member-order: bysource
   
      
Utilities and Decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SendToQueue
   :members:
   :member-order: bysource