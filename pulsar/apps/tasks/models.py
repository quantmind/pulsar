import sys
from datetime import datetime, timedelta, date

from pulsar.utils.tools import gen_unique_id

from .registry import registry


__all__ = ['Task','PeriodicTask','anchorDate']


class TaskMetaClass(type):
    """Metaclass for tasks.

    Automatically registers the task in the task registry, except
    if the ``abstract`` attribute is set.

    If no ``name`` attribute is provided, the name is automatically
    set to the name of the module it was defined in, and the class name.
    """

    def __new__(cls, name, bases, attrs):
        super_new = super(TaskMetaClass, cls).__new__
        task_module = attrs.get("__module__",None)

        # Abstract class, remove the abstract attribute so
        # any class inheriting from this won't be abstract by default.
        if attrs.pop("abstract", None) or not attrs.get("autoregister", True) or not task_module:
            return super_new(cls, name, bases, attrs)

        # Automatically generate missing name.
        task_name = attrs.get("name",None)
        if not task_name:
            task_name = name
            #module_name = sys.modules[task_module].__name__.split('.')[1]
            #task_name = ".".join((module_name, name))
        task_name = task_name.lower()
        attrs["name"] = task_name

        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework. There should only be one class for each task
        # name, so we always return the registered version.

        task_name = attrs["name"]
        if task_name not in registry:
            task_cls = super_new(cls, name, bases, attrs)
            registry.register(task_cls)
        return registry[task_name].__class__


TaskBase = TaskMetaClass('TaskBase',(object,),{'abstract':True})


class Task(TaskBase):
    '''The task class which is used in a distributed task queue.'''
    abstract = True
    '''If set to ``True`` (default is ``False``), the task won't be registered with the task registry.'''
    autoregister = True
    '''If ``False`` (default is ``True``), the task need to be registered manually with the task registry.'''
    type = "regular"
    '''Type of task, one of ``regular`` and ``periodic``'''
    _ack = True
        
    def __call__(self, consumer, *args, **kwargs):
        '''The body of the task executed by the worker. This function needs to be
implemented by subclasses.'''
        raise NotImplementedError("Tasks must define the run method.")
    
    def make_task_id(self, args, kwargs):
        '''Get the task unique identifier. This can be overridden by Task implementation.'''
        return gen_unique_id()
        
    def ack(self, args, kwargs):
        '''Return ``True`` if the task will acknowledge the task queue once the result is ready or an error has occured.
By default it returns ``True`` but it can be overridden so that its behaviour can change at runtime.'''
        return self._ack
    

class PeriodicTask(Task):
    '''A periodic :class:`Task` implementation.'''
    abstract  = True
    type      = "periodic"
    anchor    = None
    '''If specified it must be a :class:`datetime.datetime` instance.
It controls when the periodic task is run.'''
    run_every = None
    '''Periodicity as a :class:`datetime.timedelta` instance.'''
    
    def __init__(self, run_every = None):
        self.run_every = run_every or self.run_every
        if self.run_every is None:
            raise NotImplementedError("Periodic tasks must have a run_every attribute set.")
   
    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
where next time to run is in seconds. e.g.

* ``(True, 20)``, means the task should be run now, and the next
    time to run is in 20 seconds.

* ``(False, 12)``, means the task should be run in 12 seconds.

You can override this to decide the interval at runtime.
        """
        return self.run_every.is_due(last_run_at)


def anchorDate(hour = 0, minute = 0, second = 0):
    td = date.today()
    return datetime(year = td.year, month = td.month, day = td.day,
                    hour = hour, minute = minute, second = second)
    

