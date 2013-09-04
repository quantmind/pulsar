'''
The :class:`TaskBackend` is at the heart of the
:ref:`task queue application <apps-taskqueue>`. It exposes
all the functionalities for running new tasks, scheduling periodic tasks
and retrieving task information. Pulsar ships with two backends, one which uses
pulsar internals and store tasks in the arbiter domain and another which stores
tasks in redis_.


Overview
===============

The backend is created by the :class:`pulsar.apps.tasks.TaskQueue`
as soon as it starts. It is then passed to all task queue workers
which, in turns, invoke the :class:`TaskBackend.start` method
to start pulling tasks form the distributed task queue.

Implementation
~~~~~~~~~~~~~~~~~
When creating a new :class:`TaskBackend` there are six methods which must
be implemented:

* The :meth:`TaskBackend.put_task` method, invoked when putting a new
  :class:`Task.id` into the distributed task queue, whatever that is.
* The :meth:`TaskBackend.get_task` method, invoked when retrieving
  a :class:`Task` from the backend server.
* The :meth:`TaskBackend.get_tasks` method, invoked when retrieving
  a group of :class:`Task` from the backend server.
* The :meth:`TaskBackend.save_task` method, invoked when creating
  or updating a :class:`Task`.
* The :meth:`TaskBackend.delete_tasks` method, invoked when deleting
  a bunch of :class:`Task`.
* The :meth:`TaskBackend.flush` method, invoked flushing a backend (remove
  all tasks and clear the task queue).

.. _task-state:

Task states
~~~~~~~~~~~~~

A :class:`Task` can have one of the following :attr:`Task.status` string:

* ``PENDING`` A task waiting to be queued for execution.
* ``QUEUED`` A task queued but not yet executed.
* ``RETRY`` A task is retrying calculation.
* ``STARTED`` task where execution has started.
* ``REVOKED`` the task execution has been revoked. One possible reason could be
  the task has timed out.
* ``UNKNOWN`` task execution is unknown.
* ``FAILURE`` task execution has finished with failure.
* ``SUCCESS`` task execution has finished with success.


**FULL_RUN_STATES**

The set of states for which a :class:`Task` has run:
``FAILURE`` and ``SUCCESS``


**READY_STATES**

The set of states for which a :class:`Task` has finished:
``REVOKED``, ``FAILURE`` and ``SUCCESS``

.. _tasks-pubsub:

Task status broadcasting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A :class:`TaskBackend` broadcast :class:`Task` state into three different
channels via the :attr:`TaskBackend.pubsub` handler.

The pubsub handler is constructed using the same
:attr:`pulsar.apps.Backend.connection_string` and
:attr:`pulsar.apps.Backend.name` as the the :class:`TaskBackend`. 

API
=========

.. _apps-taskqueue-task:

Task
~~~~~~~~~~~~~

.. autoclass:: Task
   :members:
   :member-order: bysource
      
   
TaskBackend
~~~~~~~~~~~~~

.. autoclass:: TaskBackend
   :members:
   :member-order: bysource
   
TaskConsumer
~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskConsumer
   :members:
   :member-order: bysource
   
Scheduler Entry
~~~~~~~~~~~~~~~~~~~

.. autoclass:: SchedulerEntry
   :members:
   :member-order: bysource
   

Local Backend
==================

.. automodule:: pulsar.apps.tasks.backends.local

Redis Backend
==================

.. automodule:: pulsar.apps.tasks.backends.redis


.. _redis: http://redis.io/
'''
import sys
import logging
from datetime import datetime, timedelta
from threading import Lock

from pulsar import (maybe_async, EMPTY_TUPLE, EMPTY_DICT, Failure,
                    PulsarException, Backend, Deferred, coroutine_return)
from pulsar.utils.pep import itervalues
from pulsar.apps.tasks.models import JobRegistry
from pulsar.apps.tasks import states, create_task_id
from pulsar.apps import pubsub
from pulsar.utils.timeutils import remaining, timedelta_seconds
from pulsar.utils.log import local_property

__all__ = ['Task', 'Backend', 'TaskBackend', 'TaskNotAvailable',
           'nice_task_message', 'LOGGER']

LOGGER = logging.getLogger('pulsar.tasks')


def get_datetime(expiry, start):
    if isinstance(expiry, datetime):
        return expiry
    elif isinstance(expiry, timedelta):
        return start + expiry
    else:
        return datetime.fromtimestamp(expiry)

def format_time(dt):
    return dt.isoformat() if dt else '?'

def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = req['status'].lower()
    user = req.get('user')
    ti = req.get('time_start', req.get('time_executed'))
    name = '%s (%s) ' % (req['name'], req['id'][:8])
    msg = '%s %s at %s' % (name, status, smart_time(ti))
    return '%s by %s' % (msg, user) if user else msg

class TaskNotAvailable(PulsarException):
    MESSAGE = 'Task {0} is not registered. Check your settings.'
    def __init__(self, task_name):
        self.task_name = task_name
        super(TaskNotAvailable,self).__init__(self.MESSAGE.format(task_name))
        
        
class TaskTimeout(PulsarException):
    pass


class TaskConsumer(object):
    '''A context manager for consuming tasks.

Instances of this consumer are created by the :class:`TaskBackend` when
a task is executed.

.. attribute:: task_id

    the :attr:`Task.id` being consumed.

.. attribute:: job

    the :ref:`Job <apps-taskqueue-job>` which generated the :attr:`task`.

.. attribute:: worker

    the :class:`pulsar.Actor` running the task worker.
    
.. attribute:: backend

    Access to the :class:`TaskBackend`. This is useful when creating
    tasks from within a :ref:`job callable <job-callable>`.
'''
    def __init__(self, backend, worker, task_id, job):
        self.backend = backend
        self.worker = worker
        self.job = job
        self.task_id = task_id
    
    
class Task(object):
    '''Interface for tasks which are produced by
:ref:`jobs or periodic jobs <apps-taskqueue-job>`.

.. attribute:: id

    :class:`Task` unique id.

.. attribute:: name

    :class:`Job` name.

.. attribute:: status

    The current :ref:`status string <task-state>` of task.

.. attribute:: time_executed

    date time when the task was executed.

.. attribute:: time_start

    date-time when the task calculation has started.

.. attribute:: time_end

    date-time when the task has finished.

.. attribute:: expiry

    optional date-time indicating when the task should expire.

.. attribute:: timeout

    A datetime or ``None`` indicating whether a timeout has occurred.

.. attribute:: from_task

    Optional :attr:`Task.id` for the :class:`Task` which queued
    this :class:`Task`. This is a usuful for monitoring the creation
    of tasks within other tasks.
'''
    stack_trace = None
    def __init__(self, id, overlap_id='', name=None, time_executed=None,
                 expiry=None, args=None, kwargs=None, status=None,
                 from_task=None, result=None, **params):
        self.id = id
        self.overlap_id = overlap_id
        self.name = name
        self.time_executed = time_executed
        self.from_task = from_task
        self.time_started = None
        self.time_ended = None
        self.expiry = expiry
        self.args = args
        self.kwargs = kwargs
        self.status = status
        self.result = result
        self.params = params

    def __repr__(self):
        return '%s (%s)' % (self.name, self.id)
    __str__ = __repr__
    
    @property
    def status_code(self):
        '''Integer indicating :attr:`status` precedence.
Lower number higher precedence.'''
        return states.PRECEDENCE_MAPPING.get(self.status, states.UNKNOWN_STATE)

    def done(self):
        '''Return ``True`` if the :class:`Task` has finshed
(its status is one of :ref:`READY_STATES <task-state>`).'''
        return self.status in states.READY_STATES

    def execute2start(self):
        if self.time_start:
            return self.time_start - self.time_executed

    def execute2end(self):
        if self.time_end:
            return self.time_ended - self.time_executed

    def duration(self):
        '''The :class:`Task` duration. Only available if the task status is in
:attr:`FULL_RUN_STATES`.'''
        if self.time_end and self.time_started:
            return self.time_ended - self.time_started

    def tojson(self):
        '''Convert the task instance into a JSON-serializable dictionary.'''
        return self.__dict__.copy()
    

class PubSubClient(pubsub.Client):
    
    def __init__(self, be):
        self._be = be
        self.task_done = be.channel('task_done')
        
    def __call__(self, channel, message):
        if channel == self.task_done:
            maybe_async(self._be.task_done_callback(message))
            
        
class TaskBackend(Backend):
    '''A :class:`pulsar.apps.Backend` class for :class:`Task`.
A :class:`TaskBackend` is responsible for creating tasks and put them
into the distributed queue.
It also schedules the run of periodic tasks if enabled to do so.
    
.. attribute:: task_paths

    List of paths where to upload :ref:`jobs <app-taskqueue-job>` which
    are factory of tasks. Passed by the task-queue application
    :ref:`task paths setting <setting-task_paths>`.
    
.. attribute:: schedule_periodic

    `True` if this :class:`TaskBackend` can schedule periodic tasks. Passed
    by the task-queue application
    :ref:`schedule-periodic setting <setting-schedule_periodic>`.
    
.. attribute:: backlog

    The maximum number of concurrent tasks running on a task-queue
    :class:`pulsar.apps.Worker`. A number in the order of 5 to 10 is normally
    used. Passed by the task-queue application
    :ref:`concurrent tasks setting <setting-concurrent_tasks>`.
    
.. attribute:: max_tasks

    The maximum number of tasks a worker will process before restarting.
    Passed by the task-queue application
    :ref:`max requests setting <setting-max_requests>`.
    
.. attribute:: poll_timeout

    The (asynchronous) timeout for polling tasks from the task queue. It is
    always a positive number and it can be specified via the backend
    connection string::
    
        local://?poll_timeout=3
        
    There shouldn't be any reason to modify the default value.
    
    Default: ``2``.
    
.. attribute:: processed

    The number of tasks processed (so far) by the worker running this backend.
    This value is important in connection with the :attr:`max_tasks` attribute.
    
'''
    def setup(self, task_paths=None, schedule_periodic=False, backlog=1,
              max_tasks=0, poll_timeout=None, **params):
        self.task_paths = task_paths
        self.backlog = backlog
        self.max_tasks = max_tasks
        self.poll_timeout = max(poll_timeout or 0, 2)
        self.processed = 0
        self.local.schedule_periodic = schedule_periodic
        self.next_run = datetime.now()
        return params
    
    @classmethod
    def path_from_scheme(cls, scheme):
        return 'pulsar.apps.tasks.backends.%s' % scheme
        
    @property
    def schedule_periodic(self):
        return self.local.schedule_periodic
    
    @local_property
    def lock(self):
        return Lock()
    
    @local_property
    def pubsub(self):
        '''A :class:`pulsar.apps.pubsub.PubSub` handler which notifies
and listen tasks execution status. There are three channels:

* ``<name>_task_created`` published when a new task is created.
* ``<name>_task_start`` published when the task queue starts executing a task.
* ``<name>_task_done`` published when a task is done.

All three messages are composed by the task id only. Here ``<name>`` is replaced
by the :attr:`pulsar.Backend.name` attribute of this task backend.

Check the :ref:`task broadcasting documentation <tasks-pubsub>` for more
information.
'''
        p = pubsub.PubSub(backend=self.connection_string, name=self.name)
        p.add_client(PubSubClient(self))
        c = self.channel
        p.subscribe(c('task_created'), c('task_start'), c('task_done'))
        return p
        
    @local_property
    def concurrent_tasks(self):
        '''Concurrent set of task ids.
        
        The task with id in this set are currentlty being executed
        by the task queue worker running this :class:`TaskBackend`..'''
        return set()
    
    @property
    def num_concurrent_tasks(self):
        '''The number of :attr:`concurrent_tasks`.'''
        return len(self.concurrent_tasks)
    
    @local_property
    def entries(self):
        return self._setup_schedule()
    
    @local_property
    def registry(self):
        '''The :class:`pulsar.apps.tasks.models.JobRegistry` for this backend.'''
        return JobRegistry.load(self.task_paths)
    
    def channel(self, name):
        return '%s_%s' % (self.name, name)
    
    def run(self, jobname, *args, **kwargs):
        '''A shortcut for :meth:`run_job` without task meta parameters'''
        return self.run_job(jobname, args, kwargs)
    
    def run_job(self, jobname, targs=None, tkwargs=None, **meta_params):
        '''Create a new :ref:`task <apps-taskqueue-task>` which may or
may not be queued. This method returns a :class:`pulsar.Deferred` which
results in the :attr:`Task.id` created.
If ``jobname`` is not a valid :attr:`pulsar.apps.tasks.models.Job.name`,
a ``TaskNotAvailable`` exception occurs.

:parameter jobname: the name of a :class:`pulsar.apps.tasks.models.Job`
    registered with the :class:`pulsar.apps.tasks.TaskQueue` application.
:parameter targs: optional tuple used for the positional arguments in the
    task callable.
:parameter tkwargs: optional dictionary used for the key-valued arguments
    in the task callable.
:parameter meta_params: Additional parameters to be passed to the :class:`Task`
    constructor (not its callable function).
:return: a :class:`pulsar.Deferred` resulting in a :attr:`Task.id`
    on success.'''
        c = self.create_task(jobname, targs, tkwargs, **meta_params)
        return maybe_async(c, get_result=False).add_callback(self.put_task)
        
    def wait_for_task(self, task_id, timeout=None):
        '''Asynchronously wait for a task with ``task_id`` to have finished
its execution. It returns a :class:`pulsar.Deferred`.'''
        # make sure we are subscribed to the task_done channel
        def _():
            self.pubsub
            task = yield self.get_task(task_id)
            if task:
                if task.done(): # task done, simply return it
                    when_done = self.pop_callback(task.id)
                    if when_done:
                        when_done.callback(task)
                    yield task
                else:
                    yield self.get_callback(task_id)
        return maybe_async(_(), timeout=timeout, get_result=False)
     
    ############################################################################
    ##    START/CLOSE METHODS FOR TASK WORKERS
    ############################################################################
    def start(self, worker):
        '''invoked by the task queue ``worker`` when it starts.
        
        Here, the ``worker`` creates its thread pool via
        :meth:`pulsar.Actor.create_thread_pool` and register the
        :meth:`may_pool_task` callback in its event loop.'''
        worker.create_thread_pool()
        self.local.task_poller = worker.event_loop.call_soon(
                                    self.may_pool_task, worker)
        worker.logger.debug('started polling tasks')
        
    def close(self, worker):
        '''Close this :class:`TaskBackend`. Invoked by the
:class:`pulsar.apps.Worker` when is stopping.'''
        if self.local.task_poller:
            self.local.task_poller.cancel()
            worker.logger.debug('stopped polling tasks')
    
    ############################################################################
    ##    ABSTRACT METHODS
    ############################################################################
    def put_task(self, task_id):
        '''Put the ``task_id`` into the queue.

:parameter task_id: the task id.
:return: an :ref:`asynchronous component <tutorial-coroutine>` which results in
    the ``task_id`` added.
    
**Must be implemented by subclasses.**'''
        raise NotImplementedError
    
    def num_tasks(self):
        '''Retrieve the number of tasks in the task queue.'''
        raise NotImplementedError
    
    def get_task(self, task_id=None, when_done=False):
        '''Retrieve a :class:`Task` from a ``task_id``. Must be implemented
by subclasses.

:param task_id: the :attr:`Task.id` of the task to retrieve.
:param when_done: if ``True`` return only when the task is in a ready state.
:return: a :class:`Task` or ``None``.

**Must be implemented by subclasses.**
'''
        raise NotImplementedError
    
    def get_tasks(self, **filters):
        '''Retrieve a group of :class:`Task` from the backend.
        
        **Must be implemented by subclasses.**'''
        raise NotImplementedError
    
    def save_task(self, task_id, **params):
        '''Create or update a :class:`Task` with ``task_id`` and key-valued
parameters ``params``.

**Must be implemented by subclasses.**'''        
        raise NotImplementedError
    
    def delete_tasks(self, task_ids=None):
        '''Delete a group of task. Must be implemented by subclasses.
        
        **Must be implemented by subclasses.**'''
        raise NotImplementedError
    
    def flush(self):
        '''Flush task backend by removing all tasks and clearing the queue.
        
        **Must be implemented by subclasses.**'''
        raise NotImplementedError
    
    ############################################################################
    ##    PRIVATE METHODS
    ############################################################################
    def tick(self, now=None):
        '''Run a tick, that is one iteration of the scheduler. This
method only works when :attr:`schedule_periodic` is ``True`` and
the arbiter context.

Executes all due tasks and calculate the time in seconds to wait before
running a new :meth:`tick`. For testing purposes a :class:`datetime.datetime`
value ``now`` can be passed.'''
        if not self.schedule_periodic: 
            return
        remaining_times = []
        try:
            for entry in itervalues(self.entries):
                is_due, next_time_to_run = entry.is_due(now=now)
                if is_due:
                    self.run_job(entry.name)
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except Exception:
            LOGGER.exception('Unhandled error in task backend')
        self.next_run = now or datetime.now()
        if remaining_times:
            self.next_run += timedelta(seconds = min(remaining_times))
            
    def create_task(self, jobname, targs=None, tkwargs=None, expiry=None,
                    **params):
        '''Create a new :class:`Task` from ``jobname``, positional arguments
``targs``, key-valued arguments ``tkwargs`` and :class:`Task` meta parameters
``params``. This method can be called by any process domain.
        
:param jobname: the name of the :class:`Job` which create the task.
:param targs: task positional arguments (a ``tuple`` or ``None``).
:param tkwargs: task key-valued arguments (a ``dict`` or ``None``).
:return: a :ref:`coroutine <coroutine>` resulting in a :attr:`Task.id`
    or ``None`` if no task was created.
'''
        if jobname in self.registry:
            pubsub = self.pubsub
            job = self.registry[jobname]
            targs = targs or EMPTY_TUPLE
            tkwargs = tkwargs or EMPTY_DICT
            task_id, overlap_id = job.generate_task_ids(targs, tkwargs)
            task = None
            if overlap_id:
                tasks = yield self.get_tasks(overlap_id=overlap_id)
                # Tasks with overlap id already available
                for task in tasks:
                    if task.done():
                        yield self.save_task(task.id, overlap_id='')
                        task = None
                    else:
                        break
            if task:
                LOGGER.debug('Task %s cannot run.', task)
                yield None
            else:
                if self.entries and job.name in self.entries:
                    self.entries[job.name].next()
                time_executed = datetime.now()
                if expiry is not None:
                    expiry = get_datetime(expiry, time_executed)
                elif job.timeout:
                    expiry = get_datetime(job.timeout, time_executed)
                LOGGER.debug('Queue new task %s (%s).', job.name, task_id)
                yield self.save_task(task_id, overlap_id=overlap_id,
                                     name=job.name, time_executed=time_executed,
                                     expiry=expiry, args=targs, kwargs=tkwargs,
                                     status=states.PENDING, **params)
                pubsub.publish(self.channel('task_created'), task_id)
        else:
            raise TaskNotAvailable(jobname)
    
    def job_list(self, jobnames=None):
        registry = self.registry
        jobnames = jobnames or registry
        all = []
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
            if self.entries and name in self.entries:
                entry = self.entries[name]
                _,next_time_to_run = self.next_scheduled((name,))
                run_every = 86400*job.run_every.days + job.run_every.seconds
                d.update({'next_run':next_time_to_run,
                          'run_every':run_every,
                          'runs_count':entry.total_run_count})
            all.append((name,d))
        return all
            
    def next_scheduled(self, jobnames=None):
        if not self.schedule_periodic:
            return
        if jobnames:
            entries = (self.entries.get(name, None) for name in jobnames)
        else:
            entries = itervalues(self.entries)
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
        
    def may_pool_task(self, worker):
        '''Called in the ``worker`` event loop.
        
        It pools a new task if possible, and add it to the queue of
        tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = 0
        if worker.is_running():
            thread_pool = worker.thread_pool
            if not thread_pool:
                worker.logger.warning('No thread pool, cannot poll tasks.')
            elif self.num_concurrent_tasks < self.backlog:
                if self.max_tasks and self.processed >= self.max_tasks:
                    if not self.num_concurrent_tasks:
                        worker.logger.warning('Processed %s tasks. Restarting.')
                        worker.stop()
                        coroutine_return()
                else:
                    task = yield self.get_task()
                    if task:    # Got a new task
                        self.processed += 1
                        self.concurrent_tasks.add(task.id)
                        thread_pool.apply(self._execute_task, worker, task)
            else:
                worker.logger.info('%s concurrent requests. Cannot poll.',
                                   self.num_concurrent_tasks)
                next_time = 1
        worker.event_loop.call_later(next_time, self.may_pool_task, worker)
    
    def _execute_task(self, worker, task):
        #Asynchronous execution of a Task. This method is called
        #on a separate thread of execution from the worker event loop thread.
        pubsub = self.pubsub
        task_id = task.id
        result = None
        status = None
        consumer = None
        time_ended = datetime.now()
        try:
            job = self.registry.get(task.name)
            consumer = TaskConsumer(self, worker, task_id, job)
            if not consumer.job:
                raise RuntimeError('Task "%s" not in registry %s' %
                                   (task.name, self.registry))
            if task.status_code > states.PRECEDENCE_MAPPING[states.STARTED]:
                if task.expiry and time_ended > task.expiry:
                    raise TaskTimeout
                else:
                    worker.logger.info('starting task %s', task)
                    yield self.save_task(task_id, status=states.STARTED,
                                         time_started=time_ended)
                    pubsub.publish(self.channel('task_start'), task_id)
                    result = yield job(consumer, *task.args, **task.kwargs)
                    time_ended = datetime.now()
            else:
                consumer = None
        except TaskTimeout:
            worker.logger.info('Task %s timed-out', task)
            status = states.REVOKED
        except Exception:
            result = Failure(sys.exc_info())
        if isinstance(result, Failure):
            result.log(msg='Failure in task %s' % task, log=worker.logger)
            status = states.FAILURE
            result = str(result)
        elif not status:
            status = states.SUCCESS
        if consumer:
            yield self.save_task(task_id, time_ended=time_ended,
                                 status=status, result=result)
            worker.logger.info('Finished task %s', task)
            # PUBLISH task_done
            pubsub.publish(self.channel('task_done'), task.id)
        self.concurrent_tasks.discard(task.id)
        yield task_id
        
    def _setup_schedule(self):
        if not self.local.schedule_periodic:
            return ()
        entries = {}
        for name, task in self.registry.filter_types('periodic'):
            schedule = self._maybe_schedule(task.run_every, task.anchor)
            entries[name] = SchedulerEntry(name, schedule)
        return entries
    
    def _maybe_schedule(self, s, anchor):
        if not self.local.schedule_periodic:
            return
        if isinstance(s, int):
            s = timedelta(seconds=s)
        if not isinstance(s, timedelta):
            raise ValueError('Schedule %s is not a timedelta' % s)
        return Schedule(s, anchor)
    
    def task_done_callback(self, task_id):
        '''Got a task_id from the ``<name>_task_done`` channel.
        
Check if a ``callback`` is available in the :attr:`callbacks` dictionary. If
so fire the callback with the ``task`` instance corresponsding to the input
``task_id``.

If a callback is not available, it must have been fired already.'''
        task = yield self.get_task(task_id)
        if task:
            when_done = self.pop_callback(task.id)
            if when_done:
                when_done.callback(task)
                
    def pop_callback(self, task_id):
        with self.lock:
            return self.callbacks.pop(task_id, None)
        
    def get_callback(self, task_id):
        with self.lock:
            callbacks = self.callbacks
            when_done = callbacks.get(task_id)
            if not when_done:
                # No deferred, create one and check again
                callbacks[task_id] = when_done = Deferred()
            return when_done
    
    @local_property
    def callbacks(self):
        return {}            
            
    
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

    def next(self, now=None):
        """Returns a new instance of the same class, but with
        its date and count fields updated. Function called by :class:`Scheduler`
        when the ``this`` is due to run."""
        now = now or datetime.now()
        self.last_run_at = now or datetime.now()
        self.total_run_count += 1
        return self

    def is_due(self, now=None):
        return self.schedule.is_due(self.scheduled_last_run_at, now=now)