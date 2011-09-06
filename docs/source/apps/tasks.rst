=============================
Distributed Task Queue
=============================

.. automodule:: pulsar.apps.tasks

A task can have one of the following `status`:

.. automodule:: pulsar.apps.tasks.states

API
=========


Job and Tasks
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.JobRegistry
   :members:
   :member-order: bysource

.. autoclass:: pulsar.apps.tasks.JobMetaClass
   :members:
   :member-order: bysource
   
.. autoclass:: pulsar.apps.tasks.Job
   :members:
   :member-order: bysource

.. autoclass:: pulsar.apps.tasks.Task
   :members:
   :member-order: bysource
   
   
Application and Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.TaskQueue
   :members:
   :member-order: bysource
   
Utilities and Decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.SendToQueue
   :members:
   :member-order: bysource