import threading
import time
import logging
from datetime import timedelta, datetime
from UserDict import UserDict

from unuk.core.exceptions import SchedulingError
from unuk.utils.timeutils import remaining, timedelta_seconds, humanize_seconds


class SchedulerEntry(object):
    """A class which can be used as a schedule entry in a :class:`Scheduler` instance.
    """
    name = None
    '''Task name'''
    schedule = None
    '''The schedule'''
    args = None
    '''Args to apply.'''
    kwargs = None
    '''Keyword arguments to apply.'''
    last_run_at = None
    '''The time and date of when this task was last run.'''
    total_run_count = None
    '''Total number of times this periodic task has been executed.'''
    
    def __init__(self, name, schedule, args=(), kwargs={},
                 options={}, last_run_at = None, total_run_count=None):
        self.name = name
        self.schedule = schedule
        self.args = args
        self.kwargs = kwargs
        self.options = options
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
        return self.__class__(self.name,
                              self.schedule.next(),
                              self.args,
                              self.kwargs,
                              self.options,
                              now,
                              self.total_run_count + 1)

    def is_due(self, now = None):
        """See :meth:`celery.task.base.PeriodicTask.is_due`."""
        return self.schedule.is_due(self.scheduled_last_run_at, now = now)
    
    def __repr__(self):
        return "<Entry: %s(*%s, **%s) {%s}>" % (self.name,
                                                self.args,
                                                self.kwargs,
                                                self.schedule)
    

class Scheduler(UserDict):
    """Scheduler for periodic tasks.
    """
    Entry = SchedulerEntry
    '''Class for used to create periodic entries. By default is set to :class:`SchedulerEntry`.'''
    controller = None
    '''A :class:`unuk.contrib.tasks.Controller` instance where to send tasks. Default ``None``.'''
    beat = None
    '''Maximum time in seconds to sleep between re-checking the schedule. It is wise to keep this value
not too high. Few seconds is a good choice.'''

    def __init__(self, entries = None, controller = None, beat = 5):
        UserDict.__init__(self)
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.beat = beat
        self.logger = logging.getLogger('unuk.task.Scheduler')
        self.controller = controller
        self.setup_schedule(entries)
    
    def maybe_due(self, entry, now = None):
        is_due, next_time_to_run = entry.is_due(now = now)
        if is_due:
            self.logger.debug("Sending due task %s" % entry.name)
            try:
                self.apply_async(entry)
            except SchedulingError, exc:
                self.logger.error("%s" % exc)
        return next_time_to_run

    def tick(self, now = None):
        '''Run a tick, that is one iteration of the scheduler.
Executes all due tasks and return the time in seconds to wait before running a new :meth:`tick`.
For testing purposes a :class:`datetime.datetime` value ``now`` can be passed.'''
        remaining_times = []
        try:
            for entry in self.data.itervalues():
                next_time_to_run = self.maybe_due(entry, now = now)
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except RuntimeError:
            pass
        next = min(remaining_times)
        return min(next,self.beat),next

    def reserve(self, entry, now = None):
        new_entry = self.data[entry.name] = entry.next(now = None)
        return new_entry

    def apply_async(self, entry, now = None):
        # Update timestamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry, now = None)
        try:
            c = self.controller
            if not c:
                self.logger.error('Task controller not available.')
            else:
                return c.dispatch(entry.name, *entry.args, **entry.kwargs)
        except Exception, exc:
            raise SchedulingError("Couldn't apply scheduled task %s: %s" % (
                    entry.name, exc))

    def maybe_schedule(self, s, anchor):
        if isinstance(s, int):
            s = timedelta(seconds=s)
        if not isinstance(s, timedelta):
            raise ValueError('Schedule %s is not a timedelta' % s)
        return schedule(s, anchor)

    def setup_schedule(self, schedule):
        if schedule:
            entries = {}
            for name, entry in schedule.items():
                anchor = entry.pop('anchor',None)
                entry['schedule'] = self.maybe_schedule(entry['schedule'],
                                                        anchor)
                entries[name] = self.Entry(**entry)
            self.data.update(**entries)
    
    def run(self):
        '''The scheduler loop.'''
        try:
            log = self.logger.log
            lev = getattr(logging,'SUBDEBUG',5)
            try:
                while True:
                    if self._shutdown.isSet():
                        break
                    interval, next = self.tick()
                    log(lev,"Waking up in %s. Next run in %s" % 
                        (humanize_seconds(interval, prefix="in "),humanize_seconds(next, prefix="in ")))
                    time.sleep(interval)
            except (KeyboardInterrupt, SystemExit):
                self.sync()
        finally:
            self.sync()

    def sync(self):
        self._stopped.set()

    def stop(self, wait=False):
        self.logger.info("Shutting down periodic task loop...")
        self._shutdown.set()
        wait and self._stopped.wait() # block until shutdown done.



class schedule(object):

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
    
    def next(self):
        return self.__class__(self.run_every,self.anchor)