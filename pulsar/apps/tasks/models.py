from datetime import datetime, date, timedelta
from hashlib import sha1
import logging
import inspect

from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id


__all__ = ['JobMetaClass','Job','PeriodicJob',
           'anchorDate','JobRegistry','registry',
           'create_task_id']


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
        job = inspect.isclass(job) and job() or job
        name = job.name
        self[name] = job

    def filter_types(self, type):
        """Return a generator of all tasks of a specific type."""
        return ((job_name, job)
                    for job_name, job in iteritems(self)
                            if job.type == type)


registry = JobRegistry()



class JobMetaClass(type):
    """Metaclass for Jobs. It performs a little ammount of magic
by:

* Automatic registration of :class:`Job` instances to the
  global :class:`JobRegistry`, unless
  the :attr:`Job.abstract` attribute is set to ``True``.
* If no :attr:`Job.name`` attribute is provided,
  it is automatically set to the class name in lower case.
* Add a logger instance with name given by 'job.name` where name is the
  same as above.
"""

    def __new__(cls, name, bases, attrs):
        super_new = super(JobMetaClass, cls).__new__
        job_module = attrs.get("__module__",None)

        # Abstract class, remove the abstract attribute so
        # any class inheriting from this won't be abstract by default.
        if attrs.pop("abstract", None) or not attrs.get("autoregister", True)\
                                       or not job_module:
            return super_new(cls, name, bases, attrs)

        # Automatically generate missing name.
        job_name = attrs.get("name",name).lower()

        # Because of the way import happens (recursively)
        # we may or may not be the first time the Job tries to register
        # with the framework. There should only be one class for each Job
        # name, so we always return the registered version.
        if job_name not in registry:
            attrs["name"] = job_name
            attrs['logger'] = logging.getLogger('job.{0}'.format(name))
            job_cls = super_new(cls, name, bases, attrs)
            registry.register(job_cls)
        return registry[job_name].__class__


JobBase = JobMetaClass('JobBase',(object,),{'abstract':True})


class Job(JobBase):
    '''The Job class which is used in a distributed task queue.
    
.. attribute:: name

    The unique name which defines the Job and which can be used to retrieve
    it from the job registry.
    
.. attribute:: abstract

    If set to ``True`` (default is ``False``), the Job won't be registered
    with the :class:`JobRegistry`.
    
.. attribute:: autoregister

    If ``False`` (default is ``True``), the Job need to be registered
    manually with the Job registry.
    
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
    
.. attribute:: loglevel

    Level of task logging
    
    Default: ``None``
    
.. attribute:: logger

    an instance of a logger.
'''
    abstract = True
    autoregister = True
    type = "regular"
    timeout = None
    expires = None
    loglevel = None
    logformatter = None
    doc_syntax = 'markdown'
    can_overlap = True
    _ack = True
        
    def __call__(self, consumer, *args, **kwargs):
        '''The Jobs' task executed by the consumer. This function needs to be
implemented by subclasses.'''
        raise NotImplementedError("Jobs must define the run method.")
    
    def make_task_id(self, args, kwargs):
        '''Get the task unique identifier.
This can be overridden by Job implementation.

:parameter args: tuple of positional arguments passed to the job callable.
:parameter kwargs: dictionary of key-valued parameters passed to
    the job callable.
:rtype: a native string.
    
Called by the :attr:`TaskQueue.scheduler` when creating a new task.
'''
        can_overlap = self.can_overlap
        if hasattr(can_overlap,'__call__'):
            can_overlap = can_overlap(*args, **kwargs)
        if can_overlap:
            return create_task_id()
        else:
            suffix = ''
            if args:
                suffix = 'args(' + ', '.join((str(a) for a in args)) + ')'
            if kwargs:
                suffix += 'kwargs(' + ', '.join('{0}= {1}'.\
                            format(k,kwargs[k]) for k in sorted(kwargs)) + ')'
            name = self.name + suffix
            return sha1(name.encode('utf-8')).hexdigest()[:8]
    
    def on_same_id(self, task):
        '''Callback invocked when a task has an id equal to a task already
submitted to the task queue. By default return None and the
task is aborted.'''
        return None
        
    def ack(self, args, kwargs):
        '''Return ``True`` if a Job task will acknowledge the task queue
 once the result is ready or an error has occured.
 By default it returns ``True`` but it can be overridden so that its
 behaviour can change at runtime.'''
        return self._ack
    
    def send_to_queue(self, consumer, jobname, ack = True, **kwargs):
        '''Send a new task request to the :class:`TaskQueue`
from within another :class:`Task`.
This allow for tasks acting as tasks factories.

:parameter consumer: The :class:`TaskConsumer` handling the :class:`Task`.
:parameter jobname: The name of the :class:`Job` to run.
:parameter ack: if acknowledgment is needed. Default ``True``.
:parameter kwargs: key-valued parameters for the task.
:rtype: an :class:`pulsar.ActorMessage`.
'''
        worker = consumer.worker
        oper = "addtask" if ack else "addtask_noack"
        from_task = consumer.task.id
        return worker.monitor.send(worker, oper, jobname,
                                   {'from_task':from_task},
                                   **kwargs)
    

class PeriodicJob(Job):
    '''A periodic :class:`Job` implementation.'''
    abstract  = True
    type      = "periodic"
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
   
    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
where next time to run is in seconds. e.g.

* ``(True, 20)``, means the Job should be run now, and the next
    time to run is in 20 seconds.

* ``(False, 12)``, means the Job should be run in 12 seconds.

You can override this to decide the interval at runtime.
        """
        return self.run_every.is_due(last_run_at)
    


def anchorDate(hour = 0, minute = 0, second = 0):
    td = date.today()
    return datetime(year = td.year, month = td.month, day = td.day,
                    hour = hour, minute = minute, second = second)
    

