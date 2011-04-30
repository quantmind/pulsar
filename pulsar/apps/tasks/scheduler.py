import time
from datetime import timedelta, datetime

from pulsar.utils.py2py3 import itervalues
from pulsar.utils.timeutils import remaining, timedelta_seconds, humanize_seconds

from .registry import registry
from .consumer import TaskRequest
from .exceptions import SchedulingError, TaskNotAvailable


class Schedule(object):

    def __init__(self, run_every=None, anchor=None):
        self.run_every = run_every
        self.anchor = anchor

    def remaining_estimate(self, last_run_at, now = None):
        """Returns when the periodic task should run next as a timedelta."""
        return remaining(last_run_at, self.run_every, now = now)

    def is_due(self, last_run_at, now = None):
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
    """A class which can be used as a schedule entry in a :class:`Scheduler` instance.
    """
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

    @property
    def scheduled_last_run_at(self):
        '''The scheduled last run datetime. This is different from :attr:`last_run_at` only when :attr:`anchor` is set.'''
        last_run_at = self.last_run_at
        anchor = self.anchor
        if last_run_at and anchor:
            run_every = self.run_every
            times = int(timedelta_seconds(last_run_at - anchor)/timedelta_seconds(run_every))
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
    """Scheduler for periodic tasks.
    """
    def __init__(self):
        self._entries = self.setup_schedule()
        self.next_run = datetime.now()
        
    @property
    def entries(self):
        return self._entries
        
    def make_request(self, name, targs = None, tkwargs = None, **kwargs):
        '''Create a new task request'''
        if name in registry:
            task = registry[name]
            return TaskRequest(task, targs, tkwargs, **kwargs)
        else:
            raise TaskNotAvailable(name)

    def tick(self, queue, now = None):
        '''Run a tick, that is one iteration of the scheduler.
Executes all due tasks calculate the time in seconds to wait before
running a new :meth:`tick`. For testing purposes a :class:`datetime.datetime`
value ``now`` can be passed.'''
        remaining_times = []
        try:
            for entry in itervalues(self._entries):
                is_due, next_time_to_run = entry.is_due(now = now)
                if is_due:
                    entry = entry.next()
                    request = self.make_request(entry.name)
                    queue.put((request.id,request))
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except RuntimeError:
            pass
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
            schedule = self.maybe_schedule(task.run_every,task.anchor)
            entries[name] = SchedulerEntry(name,schedule)
        return entries

