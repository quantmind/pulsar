import time
from datetime import timedelta, datetime

from pulsar.utils.httpurl import itervalues, iteritems
from pulsar.utils.timeutils import remaining, timedelta_seconds,\
                                     humanize_seconds

from .models import registry
from .exceptions import SchedulingError, TaskNotAvailable
from .states import PENDING


__all__ = ['Scheduler']


EMPTY_TUPLE = ()
EMPTY_DICT = {}


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
    """A class which can be used as a schedule entry in a
:class:`Scheduler` instance."""
    name = None
    '''Task name'''
    schedule = None
    '''The schedule'''
    last_run_at = None
    '''The time and date of when this task was last run.'''
    total_run_count = None
    '''Total number of times this periodic task has been executed.'''

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
        '''The scheduled last run datetime. This is different from :attr:`last_run_at` only when :attr:`anchor` is set.'''
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
        return self.schedule.is_due(self.scheduled_last_run_at, now = now)


class Scheduler(object):
    """Scheduler for periodic tasks. This class is the main driver of tasks
and task scheduling.

.. attribute:: task_class

    The :attr:`TaskQueue.task_class` for producing new :class:`Task`.

"""
    def __init__(self, app):
        self._entries = self.setup_schedule()
        self.next_run = datetime.now()
        self.task_class = app.task_class
        self.logger = app.logger

    @property
    def entries(self):
        return self._entries

    def queue_task(self, monitor, jobname, targs=None, tkwargs=None, **params):
        '''Create a new :class:`Task` which may or may not queued.

:parameter monitor: the :class:`pulsar.ApplicationMonitor` running the
    :class:`TaskQueue` application.
:parameter jobname: the name of a :class:`Job` registered
    with the :class:`TaskQueue` application.
:parameter targs: optional tuple used for the positional arguments in the
    task callable.
:parameter tkwargs: optional dictionary used for the key-valued arguments
    in the task callable.
:parameter params: Additional parameters to be passed to the :class:`Task`
    constructor (not its callable function).

:rtype: an instance of :class:`Task`'''
        task = self._make_request(jobname, targs, tkwargs, **params)
        if task.needs_queuing():
            task._queued = True
            monitor.put(task.serialize_for_queue())
        else:
            task._queued = False
            self.logger.debug('Task %s already requested, abort.', task)
        return task

    def tick(self, monitor, now=None):
        '''Run a tick, that is one iteration of the scheduler.
Executes all due tasks calculate the time in seconds to wait before
running a new :meth:`tick`. For testing purposes a :class:`datetime.datetime`
value ``now`` can be passed.'''
        remaining_times = []
        try:
            for entry in itervalues(self._entries):
                is_due, next_time_to_run = entry.is_due(now=now)
                if is_due:
                    self.queue_task(monitor, entry.name)
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except:
            self.logger.error('Error in task scheduler', exc_info=True)
        self.next_run = now or datetime.now()
        if remaining_times:
            self.next_run += timedelta(seconds = min(remaining_times))

    def maybe_schedule(self, s, anchor):
        if isinstance(s, int):
            s = timedelta(seconds=s)
        if not isinstance(s, timedelta):
            raise ValueError('Schedule %s is not a timedelta' % s)
        return Schedule(s, anchor)

    def setup_schedule(self):
        entries = {}
        for name, task in registry.filter_types('periodic'):
            schedule = self.maybe_schedule(task.run_every, task.anchor)
            entries[name] = SchedulerEntry(name, schedule)
        return entries

    def job_list(self, jobnames = None):
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
            yield (name,d)

    def next_scheduled(self, jobnames=None):
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
        if isinstance(id, self.task_class):
            task = id
        else:
            task = self.task_class.get_task(self, id)
        if task and task.done() and remove:
            self.delete_tasks([task.id])
        else:
            return task
        
    def get_tasks(self, **parameters):
        return self.task_class.get_tasks(self, **parameters)
    
    def save_task(self, task):
        return self.task_class.save_task(self, task)
    
    def delete_tasks(self, ids=None):
        return self.task_class.delete_tasks(self, ids)
    
    ############################################################################
    ##    PRIVATE METHODS
    ############################################################################

    def _make_request(self, jobname, targs=None, tkwargs=None, expiry=None,
                      **params):
        if jobname in registry:
            task_class = self.task_class
            job = registry[jobname]
            targs = targs or EMPTY_TUPLE
            tkwargs = tkwargs or EMPTY_DICT
            id = job.make_task_id(targs, tkwargs)
            task = self.get_task(id, remove=True)
            if task:
                return task.to_queue(self)
            else:
                if job.name in self.entries:
                    self.entries[job.name].next()
                time_executed = datetime.now()
                if expiry is not None:
                    expiry = get_datetime(expiry, time_executed)
                elif job.timeout:
                    expiry = get_datetime(job.timeout, time_executed)
                task = task_class(id=id, name=job.name,
                                  time_executed=time_executed, expiry=expiry,
                                  args=targs, kwargs=tkwargs, status=PENDING,
                                  **params)
                task.on_created(self)
                return task.to_queue(self)
        else:
            raise TaskNotAvailable(jobname)
