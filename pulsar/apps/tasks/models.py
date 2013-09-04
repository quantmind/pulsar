'''
An application implements several :class:`Job`
classes which specify the way each :ref:`task <apps-taskqueue-task>` is run.
Each :class:`Job` class is a task-factory, therefore,
a :ref:`task <apps-taskqueue-task>` is always associated
with one :class:`Job`, which can be of two types:

* standard (:class:`Job`)
* periodic (:class:`PeriodicJob`), a generator of scheduled tasks.

.. _job-callable:

Implementing jobs
========================

To define a job is simple, subclass from :class:`Job` and implement the
**job callable method**::

    from pulsar.apps import tasks

    class Addition(tasks.Job):

        def __call__(self, consumer, a, b):
            "Add two numbers"
            return a+b

The ``consumer``, instance of :class:`pulsar.apps.tasks.backends.TaskConsumer`,
is passed by the :ref:`Task backend <apps-taskqueue-backend>` and should
always be the first positional parameter in the callable method.
The remaining (optional) positional and key-valued parameters are needed by
your job implementation.

A :ref:`job callable <job-callable>` can also return a
:ref:`coroutine <coroutine>` if it needs to perform asynchronous IO during its
execution::

    class Crawler(tasks.Job):

        def __call__(self, consumer, sample, size=10):
            response = yield http.request(...)
            content = response.content
            ...
            
This allows for cooperative task execution on each task thread workers.

.. _job-non-overlap:

Non overlapping Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~

The :attr:`Job.can_overlap` attribute controls the way tasks are generated
by a specific :class:`Job`. By default, a :class:`Job` creates a new task
every time the :ref:`task backend <apps-taskqueue-backend>` requests it.

However, when setting the :attr:`Job.can_overlap` attribute to ``False``,
a new task cannot be started unless a previous task of the same job
is done.


Job class
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Job
   :members:
   :member-order: bysource

Periodic job
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PeriodicJob
   :members:
   :member-order: bysource

Job registry
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JobRegistry
   :members:
   :member-order: bysource

'''
from datetime import datetime, date
from hashlib import sha1
import logging

from pulsar.utils.pep import iteritems
from pulsar.utils.importer import import_modules
from pulsar.utils.security import gen_unique_id


__all__ = ['JobMetaClass', 'Job', 'PeriodicJob',
           'anchorDate', 'JobRegistry', 'create_task_id']


def create_task_id():
    return gen_unique_id()[:8]


class JobRegistry(dict):
    """Site registry for tasks."""

    def regular(self):
        """A generator of all regular jobs."""
        return self.filter_types("regular")

    def periodic(self):
        """A generator of all periodic jobs."""
        return self.filter_types("periodic")

    def register(self, job):
        """Register a job in the job registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        if isinstance(job, JobMetaClass) and job.can_register:
            name = job.name
            self[name] = job()

    def filter_types(self, type):
        """Return a generator of all tasks of a specific type."""
        return ((job_name, job)
                    for job_name, job in iteritems(self)
                            if job.type == type)
        
    @classmethod
    def load(cls, paths):
        self = cls()
        for mod in import_modules(paths):
            for name in dir(mod):
                self.register(getattr(mod, name))
        return self


class JobMetaClass(type):

    def __new__(cls, name, bases, attrs):
        attrs['can_register'] = not attrs.pop('abstract', False)
        job_name = attrs.get("name", name).lower()
        log_prefix = attrs.get("log_prefix") or "pulsar"
        attrs["name"] = job_name
        logname = '%s.job.%s' % (log_prefix, name)
        attrs['logger'] = logging.getLogger(logname)
        return super(JobMetaClass, cls).__new__(cls, name, bases, attrs)



class Job(JobMetaClass('JobBase', (object,), {'abstract': True})):
    '''The Job class which is used in a distributed task queue.

.. attribute:: name

    The unique name which defines the Job and which can be used to retrieve
    it from the job registry. This attribute is set to the Job class name
    in lower case by default, unless a ``name`` class attribute is defined.

.. attribute:: abstract

    If set to ``True`` (default is ``False``), the :class:`Job` won't be
    registered with the :class:`JobRegistry`. Useful when creating a new
    base class for several other jobs.

.. attribute:: type

    Type of Job, one of ``regular`` and ``periodic``.

.. attribute:: timeout

    An instance of a datetime.timedelta or ``None``. If set, it represents the
    time lag after which a task which did not start expires.

    Default: ``None``.

.. attribute:: can_overlap

    Boolean indicating if this job can generate overlapping tasks. It can
    also be a callable which accept the same input parameters as the job
    callable function.

    Default: ``True``.

.. attribute:: doc_syntax

    The doc string syntax.

    Default: ``markdown``

.. attribute:: logger

    an instance of a logger. Created at runtime.
'''
    abstract = True
    timeout = None
    expires = None
    doc_syntax = 'markdown'
    can_overlap = True

    def __call__(self, consumer, *args, **kwargs):
        '''The Jobs' task executed by the consumer. This function needs to be
implemented by subclasses.'''
        raise NotImplementedError("Jobs must define the run method.")
    
    @property
    def type(self):
        '''Type of Job, one of ``regular`` and ``periodic``.'''
        return 'regular'

    def run_job(self, consumer, jobname, *args, **kwargs):
        '''Run a new task in the task queue.
        
This utility method can be used from within the
:ref:`job callable <job-callable>` method and it allows tasks to act
as tasks factories.

:parameter consumer: the :class:`pulsar.apps.tasks.backends.TaskConsumer`
    handling the :ref:`Task <apps-taskqueue-task>`. Must be the same instance
    as the one passed to the :ref:`job callable <job-callable>` method.
:parameter jobname: The name of the :class:`Job` to run.
:parameter args: positional argument for the :ref:`job callable <job-callable>`.
:parameter kwargs: key-valued parameters for the
    :ref:`job callable <job-callable>`.
:return: a :class:`pulsar.Deferred` called back with the task id of the new job.

This method invokes the :meth:`pulsar.apps.tasks.backends.TaskBackend.run_job`
method with the additional ``from_task`` argument equal to the
id of the task invoking the method.
'''
        return consumer.backend.run_job(jobname, args, kwargs,
                                        from_task=consumer.task_id)

    def generate_task_ids(self, args, kwargs):
        '''Generate a task unique identifiers.

:parameter args: tuple of positional arguments passed to the
    :ref:`job callable <job-callable>` method.
:parameter kwargs: dictionary of key-valued parameters passed to the
    :ref:`job callable <job-callable>` method.
:return: a two-elements tuple containing the unique id and an
    identifier for overlapping tasks if the :attr:`can_overlap` results
    in ``False``.

Called by the :ref:`TaskBackend <apps-taskqueue-backend>` when creating
a new task.
'''
        can_overlap = self.can_overlap
        if hasattr(can_overlap,'__call__'):
            can_overlap = can_overlap(*args, **kwargs)
        id = create_task_id()
        if can_overlap:
            return id, None
        else:
            suffix = ''
            if args:
                suffix = ' args(%s)' % ', '.join((str(a) for a in args))
            if kwargs:
                suffix += ' kwargs(%s)' % ', '.join(('%s=%s' % (k, kwargs[k])\
                            for k in sorted(kwargs)))
            name = '%s%s' % (self.name, suffix)
            return id, sha1(name.encode('utf-8')).hexdigest()[:8]
        

class PeriodicJob(Job):
    '''A periodic :class:`Job` implementation.'''
    abstract  = True
    anchor    = None
    '''If specified it must be a :class:`datetime.datetime` instance.
It controls when the periodic Job is run.'''
    run_every = None
    '''Periodicity as a :class:`datetime.timedelta` instance.'''

    def __init__(self, run_every = None):
        self.run_every = run_every or self.run_every
        if self.run_every is None:
            raise NotImplementedError('Periodic Jobs must have\
 a run_every attribute set, "{0}" does not have one'.format(self.name))

    @property
    def type(self):
        return 'periodic'
    
    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
where next time to run is in seconds. e.g.

* ``(True, 20)``, means the Job should be run now, and the next
    time to run is in 20 seconds.

* ``(False, 12)``, means the Job should be run in 12 seconds.

You can override this to decide the interval at runtime.
        """
        return self.run_every.is_due(last_run_at)



def anchorDate(hour=0, minute=0, second=0):
    '''Create an anchor date.'''
    td = date.today()
    return datetime(year=td.year, month=td.month, day=td.day,
                    hour=hour, minute=minute, second=second)


