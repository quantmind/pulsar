'''
The :class:`Scheduler` is at the hart of the
:ref:`task queue application <apps-taskqueue>`. It exposes
all the functionalities for running new tasks, scheduling periodic tasks
and retrieving task information.

Scheduler
~~~~~~~~~~~~~

.. autoclass:: Scheduler
   :members:
   :member-order: bysource
   
Scheduler Entry
~~~~~~~~~~~~~~~~~~~

.. autoclass:: SchedulerEntry
   :members:
   :member-order: bysource
'''
import time
import logging
from datetime import timedelta, datetime


from pulsar import EMPTY_TUPLE, EMPTY_DICT, NOT_DONE, get_actor, async, send
from pulsar.utils.httpurl import itervalues, iteritems
from pulsar.utils.importer import import_modules
from pulsar.utils.timeutils import remaining, timedelta_seconds,\
                                     humanize_seconds
from pulsar.utils.importer import import_modules
from pulsar import Empty

from .models import registry
from .exceptions import SchedulingError, TaskNotAvailable
from .states import PENDING, READY_STATES


__all__ = ['Scheduler', 'LOGGER']

LOGGER = logging.getLogger('pulsar.tasks')


def get_datetime(expiry, start):
    if isinstance(expiry, datetime):
        return expiry
    elif isinstance(expiry, timedelta):
        return start + expiry
    else:
        return datetime.fromtimestamp(expiry)


class Schedule(object):

    def __init__(self, run_every=None, anchor=None):
        self.run_every = run_every
        self.anchor = anchor

    def remaining_estimate(self, last_run_at, now=None):
        """Returns when the periodic task should run next as a timedelta."""
        return remaining(last_run_at, self.run_every, now=now)

    def is_due(self, last_run_at, now=None):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        See :meth:`unuk.contrib.tasks.models.PeriodicTask.is_due` for more information.
        """
        rem_delta = self.remaining_estimate(last_run_at, now = now)
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem


class SchedulerEntry(object):
    """A class used as a schedule entry in by a :class:`Scheduler`."""
    name = None
    '''Task name'''
    schedule = None
    '''The schedule'''
    last_run_at = None
    '''The time and date of when this task was last run.'''
    total_run_count = None
    '''Total number of times this periodic task has been executed by the
    :class:`Scheduler`.'''

    def __init__(self, name, schedule, args=(), kwargs={},
                 last_run_at = None, total_run_count=None):
        self.name = name
        self.schedule = schedule
        self.last_run_at = last_run_at or datetime.now()
        self.total_run_count = total_run_count or 0

    def __repr__(self):
        return self.name
    __str__ = __repr__

    @property
    def scheduled_last_run_at(self):
        '''The scheduled last run datetime. This is different from
:attr:`last_run_at` only when :attr:`anchor` is set.'''
        last_run_at = self.last_run_at
        anchor = self.anchor
        if last_run_at and anchor:
            run_every = self.run_every
            times = int(timedelta_seconds(last_run_at - anchor)\
                            /timedelta_seconds(run_every))
            if times:
                anchor += times*run_every
                while anchor <= last_run_at:
                    anchor += run_every
                while anchor > last_run_at:
                    anchor -= run_every
                self.schedule.anchor = anchor
            return anchor
        else:
            return last_run_at

    @property
    def run_every(self):
        return self.schedule.run_every

    @property
    def anchor(self):
        return self.schedule.anchor

    def next(self, now = None):
        """Returns a new instance of the same class, but with
        its date and count fields updated. Function called by :class:`Scheduler`
        when the ``this`` is due to run."""
        now = now or datetime.now()
        self.last_run_at = now or datetime.now()
        self.total_run_count += 1
        return self

    def is_due(self, now = None):
        return self.schedule.is_due(self.scheduled_last_run_at, now=now)


class Scheduler(object):
    """Scheduler is responsible for creating tasks and put them into
the distributed :attr:`queue`. It also schedule the run of periodic tasks if
enabled to do so.
This class is the main driver of tasks and task scheduling.

.. attribute:: queue

    The distributed :class:`pulsar.MessageQueue` where to send tasks.
    
.. attribute:: task_class

    The :ref:`task class <tasks-interface>` for producing new tasks.
    
.. attribute:: task_path

    List of paths where to upload :ref:`jobs <app-taskqueue-job>` which
    are factory of tasks.
    
.. attribute:: schedule_periodic

    `True` if this :class:`Scheduler` can schedule periodic tasks.
"""
    def __init__(self, name, queue, task_class, tasks_path=None, logger=None,
                 schedule_periodic=False):
        self.name = name
        if tasks_path:
            import_modules(tasks_path)
        self.schedule_periodic = schedule_periodic
        self._entries = self._setup_schedule()
        self.next_run = datetime.now()
        self.task_class = task_class
        self.queue = queue or QueueProxy(name)
        self.logger = logger or LOGGER

    @property
    def entries(self):
        '''Dictionary of :class:`SchedulerEntry`, available only if
:attr:`schedule_periodic` is ``True``.'''
        return self._entries

    def run(self, jobname, *args, **kwargs):
        '''A shortcut for :meth:`queue_task`.'''
        return self.queue_task(jobname, args, kwargs)
    
    @async()
    def queue_task(self, jobname, targs=None, tkwargs=None, **params):
        '''Create a new :class:`Task` which may or may not be queued. This
method returns a :ref:`coroutine <coroutine>`. If *jobname* is not a valid
:attr:`pulsar.apps.tasks.models.Job.name`, an ``TaskNotAvailable`` exception
occurs.

:parameter jobname: the name of a :class:`Job` registered
    with the :class:`TaskQueue` application.
:parameter targs: optional tuple used for the positional arguments in the
    task callable.
:parameter tkwargs: optional dictionary used for the key-valued arguments
    in the task callable.
:parameter params: Additional parameters to be passed to the :class:`Task`
    constructor (not its callable function).
:return: a :ref:`task <apps-taskqueue-task>`.'''
        task, put = yield self._make_request(jobname, targs, tkwargs, **params)
        if put:
            task._queued = True
            self.queue.put(task.id)
        else:
            task._queued = False
            self.logger.debug('Task %s already requested, abort.', task)
        yield task

    def tick(self, now=None):
        '''Run a tick, that is one iteration of the scheduler. This
method only works when :attr:`schedule_periodic` is ``True`` and
the arbiter context.

Executes all due tasks and calculate the time in seconds to wait before
running a new :meth:`tick`. For testing purposes a :class:`datetime.datetime`
value ``now`` can be passed.'''
        if not self.schedule_periodic or not get_actor().is_arbiter(): 
            return
        remaining_times = []
        try:
            for entry in itervalues(self._entries):
                is_due, next_time_to_run = entry.is_due(now=now)
                if is_due:
                    self.queue_task(entry.name)
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except Exception:
            self.logger.error('Error in task scheduler', exc_info=True)
        self.next_run = now or datetime.now()
        if remaining_times:
            self.next_run += timedelta(seconds = min(remaining_times))

    def flush(self):
        '''Remove all pending tasks'''
        try:
            while True:
                task = self.queue.get(timeout=0.5)
        except Empty:
            pass

    def job_list(self, jobnames=None):
        '''A generator of two-elements tuples with the first element given by
a job name and the second a dictionary of job information.'''
        jobnames = jobnames or registry
        for name in jobnames:
            if name not in registry:
                continue
            job = registry[name]
            can_overlap = job.can_overlap
            if hasattr(can_overlap, '__call__'):
                can_overlap = 'maybe'
            d = {'doc':job.__doc__,
                 'doc_syntax':job.doc_syntax,
                 'type':job.type,
                 'can_overlap': can_overlap}
            if name in self.entries:
                entry = self.entries[name]
                _,next_time_to_run = self.next_scheduled((name,))
                run_every = 86400*job.run_every.days + job.run_every.seconds
                d.update({'next_run':next_time_to_run,
                          'run_every':run_every,
                          'runs_count':entry.total_run_count})
            yield (name, d)

    def next_scheduled(self, jobnames=None):
        if not self.schedule_periodic:
            return
        if jobnames:
            entries = (self._entries.get(name, None) for name in jobnames)
        else:
            entries = itervalues(self._entries)
        next_entry = None
        next_time = None
        for entry in entries:
            if entry is None:
                continue
            is_due, next_time_to_run = entry.is_due()
            if is_due:
                next_time = 0
                next_entry = entry
                break
            elif next_time_to_run is not None:
                if next_time is None or next_time_to_run < next_time:
                    next_time = next_time_to_run
                    next_entry = entry
        if next_entry:
            return (next_entry.name, max(next_time, 0))
        else:
            return (jobnames, None)

    def get_task(self, id, remove=False):
        '''Retrieve a :ref:`task <tasks-interface>` given its id. If *id*
is a task, simply return it.'''
        if isinstance(id, self.task_class):
            task = id
        else:
            task = yield self.task_class.get_task(id)
        if task and task.done() and remove:
            self.delete_tasks([task.id])
        else:
            yield task
        
    def get_tasks(self, **parameters):
        return self.task_class.get_tasks(self, **parameters)
    
    def save_task(self, task):
        return self.task_class.save_task(self, task)
    
    def delete_tasks(self, ids=None):
        return self.task_class.delete_tasks(self, ids)

    def wait_for_task(self, task):
        '''Return a :ref:`coroutine <coroutine>` which will be ready once
the *task* is in a :ref:`ready state <task-state-ready>`.'''            
        while task['status'] not in READY_STATES:
            yield NOT_DONE
            task = yield self.get_task(result['id'])
        yield task
        
            
    ############################################################################
    ##    PRIVATE METHODS
    ############################################################################

    def _setup_schedule(self):
        if not self.schedule_periodic:
            return
        entries = {}
        for name, task in registry.filter_types('periodic'):
            schedule = self._maybe_schedule(task.run_every, task.anchor)
            entries[name] = SchedulerEntry(name, schedule)
        return entries
    
    def _maybe_schedule(self, s, anchor):
        if not self.schedule_periodic:
            return
        if isinstance(s, int):
            s = timedelta(seconds=s)
        if not isinstance(s, timedelta):
            raise ValueError('Schedule %s is not a timedelta' % s)
        return Schedule(s, anchor)
    
    def _make_request(self, jobname, targs=None, tkwargs=None, expiry=None,
                      **params):
        if jobname in registry:
            task_class = self.task_class
            job = registry[jobname]
            targs = targs or EMPTY_TUPLE
            tkwargs = tkwargs or EMPTY_DICT
            id = job.make_task_id(targs, tkwargs)
            task = yield self.get_task(id, remove=True)
            if task:
                yield task, False
            else:
                if self.entries and job.name in self.entries:
                    self.entries[job.name].next()
                time_executed = datetime.now()
                if expiry is not None:
                    expiry = get_datetime(expiry, time_executed)
                elif job.timeout:
                    expiry = get_datetime(job.timeout, time_executed)
                task = yield task_class(id=id, name=job.name,
                                        time_executed=time_executed,
                                        expiry=expiry, args=targs,
                                        kwargs=tkwargs, status=PENDING,
                                        **params).save()
                yield task, True
        else:
            raise TaskNotAvailable(jobname)


class QueueProxy(object):
    '''A proxy for a :class:`pulsar.MessageQueue`'''
    def __init__(self, name):
        self.name = name
        
    def put(self, task):
        send(self.name, 'put_task', task)
    
    def get(self, timeout=0.5):
        raise NotImplementedError('Cannot get tasks')

    