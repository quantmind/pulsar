.. _apps-taskqueue:

=============================
Distributed Task Queue
=============================

Pulsar ships with an asynchronous :class:`.TaskQueue` for distributing
work across threads/processes or machines. It is highly customisable,
it can run in multi-threading or multi-processing (default) mode and
can share :class:`.Task` across several machines.

By creating :class:`.Job` classes in a similar way you do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks

    if __name__ == '__main__':
        tasks.TaskQueue(tasks_path=['path.to.tasks.*']).start()

Check the :ref:`task queue tutorial <tutorials-taskqueue>` for a running
example with simple tasks.


Getting Started
=================

.. automodule:: pulsar.apps.tasks


.. _app-taskqueue-job:

Jobs
====================

.. automodule:: pulsar.apps.tasks.models


.. _apps-taskqueue-backend:

Task Backend
======================

.. automodule:: pulsar.apps.tasks.backend


API
====================

List of all classes used by this application.

Task queue application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.TaskQueue
   :members:
   :member-order: bysource


Job class
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.models.Job
   :members:
   :member-order: bysource

Periodic job
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.models.PeriodicJob
   :members:
   :member-order: bysource

Job registry
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.models.JobRegistry
   :members:
   :member-order: bysource


.. _apps-taskqueue-task:

Task
~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.backend.Task
   :members:
   :member-order: bysource


TaskBackend
~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.backend.TaskBackend
   :members:
   :member-order: bysource

TaskConsumer
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.backend.TaskConsumer
   :members:
   :member-order: bysource

Scheduler Entry
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.tasks.backend.SchedulerEntry
   :members:
   :member-order: bysource


.. module:: pulsar.apps.tasks.rpc

TaskQueue Rpc Mixin
~~~~~~~~~~~~~~~~~~~~~

The :class:`.TaskQueue` application does not expose
an external API to run new tasks or retrieve task information.
The :class:`.TaskQueueRpcMixin` class can be used to achieve just that.

It is a :class:`.JSONRPC` handler which exposes six functions
for executing tasks and retrieving task information.

The :ref:`task-queue example <tutorials-taskqueue>` shows how to use this class
in the context of a WSGI server running along side the task-queue application.

.. autoclass:: TaskQueueRpcMixin
   :members:
   :member-order: bysource
