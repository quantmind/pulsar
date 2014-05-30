'''
The :class:`TaskBackend` is at the heart of the
:ref:`task queue application <apps-taskqueue>`. It exposes
all the functionalities for running new tasks, scheduling periodic tasks
and retrieving task information. Pulsar ships with two backends, one which uses
pulsar internals and store tasks in the arbiter domain and another which stores
tasks in redis_.

The backend is created by the :class:`.TaskQueue`
as soon as it starts. It is then passed to all task queue workers
which, in turns, invoke the :class:`TaskBackend.start` method
to start pulling tasks form the distributed task queue.

.. _task-state:

Task states
~~~~~~~~~~~~~

A :class:`Task` can have one of the following :attr:`~.Task.status` string:

* ``QUEUED = 6`` A task queued but not yet executed.
* ``STARTED = 5`` task where execution has started.
* ``RETRY = 4`` A task is retrying calculation.
* ``REVOKED = 3`` the task execution has been revoked (or timed-out).
* ``FAILURE = 2`` task execution has finished with failure.
* ``SUCCESS = 1`` task execution has finished with success.

.. _task-run-state:

**FULL_RUN_STATES**

The set of states for which a :class:`Task` has run:
``FAILURE`` and ``SUCCESS``

.. _task-ready-state:

**READY_STATES**

The set of states for which a :class:`Task` has finished:
``REVOKED``, ``FAILURE`` and ``SUCCESS``

.. _tasks-pubsub:

Task status broadcasting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A :class:`TaskBackend` broadcast :class:`Task` state into three different
channels via the a :meth:`~.Store.pubsub` handler.


Implementation
~~~~~~~~~~~~~~~~~~~
When creating a new :class:`TaskBackend` there are three methods which must
be implemented:

* The :meth:`~TaskBackend.get_task` method, invoked when retrieving
  a :class:`Task` from the backend server.
* The :meth:`~TaskBackend.maybe_queue_task` method, invoked when a new
  class:`.Task` is created and ready to be queued.
* The :meth:`~TaskBackend.finish_task` method, invoked when a
  :class:`.Task` reaches a :ref:`ready state <task-ready-state>`.

For example::

    from pulsar.apps import tasks

    class TaskBackend(tasks.TaskBackend):
        ...

Once the custom task backend is implemented it must be registered::

    tasks.task_backends['mybackend'] = TaskBackend

And the backend will be selected via::

    --task-backend mybackend://host:port

.. _redis: http://redis.io/
'''
import sys
import time
from functools import partial
from datetime import datetime, timedelta
from hashlib import sha1

from pulsar import (task, async, EventHandler, PulsarException, get_logger,
                    Future, coroutine_return, get_request_loop)
from pulsar.utils.pep import itervalues, to_string
from pulsar.utils.security import gen_unique_id
from pulsar.apps.data import create_store, PubSubClient, odm
from pulsar.utils.log import (LocalMixin, lazyproperty, lazymethod,
                              LazyString)

from .models import JobRegistry
from . import states


__all__ = ['task_backends', 'Task', 'TaskBackend', 'TaskNotAvailable',
           'nice_task_message']

task_backends = {}


if hasattr(timedelta, "total_seconds"):
    timedelta_seconds = lambda delta: max(delta.total_seconds(), 0)

else:   # pragma    nocover
    def timedelta_seconds(delta):
        if delta.days < 0:
            return 0
        return delta.days * 86400 + delta.seconds + (delta.microseconds / 10e5)


def get_time(expiry, start):
    if isinstance(expiry, timedelta):
        return (start + 86400*expiry.days + expiry.seconds +
                0.000001*expiry.microseconds)
    else:
        return start + expiry


def format_time(dt):
    if isinstance(dt, (float, int)):
        dt = datetime.fromtimestamp(dt)
    return dt.isoformat() if dt else '?'


def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = states.status_string(req.get('status'))
    user = req.get('user')
    ti = req.get('time_start', req.get('time_executed'))
    name = '%s (%s) ' % (req['name'], req['id'][:8])
    msg = '%s %s at %s' % (name, status, smart_time(ti))
    return '%s by %s' % (msg, user) if user else msg


class TaskNotAvailable(PulsarException):
    MESSAGE = 'Task {0} is not registered'

    def __init__(self, task_name):
        self.task_name = task_name
        super(TaskNotAvailable, self).__init__(self.MESSAGE.format(task_name))


class TaskTimeout(PulsarException):
    pass


class TaskConsumer(object):
    '''A context manager for consuming tasks.

    Instances of this consumer are created by the :class:`TaskBackend` when
    a task is executed.

    .. attribute:: _loop

        the :ref:`queue-based loop <queue-based-loop>` of the thread
        executing the task.

    .. attribute:: task_id

        the :attr:`Task.id` being consumed.

    .. attribute:: job

        the :class:`.Job` which generated the task.

    .. attribute:: worker

        the :class:`.Actor` executing the task.

    .. attribute:: backend

        The :class:`.TaskBackend`. This is useful when creating
        tasks from within a :ref:`job callable <job-callable>`.
    '''
    def __init__(self, backend, worker, task_id, job):
        self._loop = get_request_loop()
        self.logger = self._loop.logger
        self.backend = backend
        self.worker = worker
        self.job = job
        self.task_id = task_id


class Task(odm.Model):
    '''A data :class:`.Model` containing task execution data.
    '''
    id = odm.CharField(primary_key=True)
    '''Task unique identifier.
    '''
    lock_id = odm.CharField(required=False)
    name = odm.CharField(index=True)
    time_queued = odm.FloatField(default=time.time)
    time_started = odm.FloatField(required=False)
    time_ended = odm.FloatField(required=False)
    '''The timestamp indicating when this has finished.
    '''
    expiry = odm.FloatField(required=False)
    '''The timestamp indicating when this task expires.

    If the task is not started before this value it is ``REVOKED``.
    '''
    status = odm.IntegerField(index=True, default=states.QUEUED)
    '''flag indicating the :ref:`task status <task-state>`
    '''
    kwargs = odm.PickleField()
    result = odm.PickleField()

    def done(self):
        '''Return ``True`` if the :class:`Task` has finshed.

        Its status is one of :ref:`READY_STATES <task-ready-state>`.
        '''
        return self.get('status') in states.READY_STATES

    def status_string(self):
        '''A string representation of :attr:`status` code
        '''
        return states.status_string(self.get('status'))

    def info(self):
        return 'task.%s(%s)' % (self.get('name'), self.get('id'))

    def lazy_info(self):
        return LazyString(self.info)


class TaskBackend(EventHandler):
    '''A backend class for running :class:`.Task`.
    A :class:`TaskBackend` is responsible for creating tasks and put them
    into the distributed queue.
    It also schedules the run of periodic tasks if enabled to do so.

    .. attribute:: task_paths

        List of paths where to upload :ref:`jobs <app-taskqueue-job>` which
        are factory of tasks. Passed by the task-queue application
        :ref:`task paths setting <setting-task_paths>`.

    .. attribute:: schedule_periodic

        ``True`` if this :class:`TaskBackend` can schedule periodic tasks.

        Passed by the task-queue application
        :ref:`schedule-periodic setting <setting-schedule_periodic>`.

    .. attribute:: backlog

        The maximum number of concurrent tasks running on a task-queue
        for an :class:`.Actor`. A number in the order of 5 to 10 is normally
        used. Passed by the task-queue application
        :ref:`concurrent tasks setting <setting-concurrent_tasks>`.

    .. attribute:: max_tasks

        The maximum number of tasks a worker will process before restarting.
        Passed by the task-queue application
        :ref:`max requests setting <setting-max_requests>`.

    .. attribute:: poll_timeout

        The (asynchronous) timeout for polling tasks from the task queue.

        It is always a positive number and it can be specified via the
        backend connection string::

            local://?poll_timeout=3

        There shouldn't be any reason to modify the default value.

        Default: ``2``.

    .. attribute:: processed

        The number of tasks processed (so far) by the worker running this
        backend.
        This value is important in connection with the :attr:`max_tasks`
        attribute.

    '''
    task_poller = None

    def __init__(self, store, logger=None, task_paths=None,
                 schedule_periodic=False, backlog=1, max_tasks=0, name=None,
                 poll_timeout=None):
        self.store = store
        self._logger = logger
        super(TaskBackend, self).__init__(self._loop,
                                          many_times_events=('task_queued',
                                                             'task_started',
                                                             'task_done'))
        self.name = name
        self.task_paths = task_paths
        self.backlog = backlog
        self.max_tasks = max_tasks
        self.poll_timeout = max(poll_timeout or 0, 2)
        self.concurrent_tasks = set()
        self.processed = 0
        self.schedule_periodic = schedule_periodic
        self.next_run = time.time()
        self.callbacks = {}
        self.models = odm.Mapper(self.store)
        self.models.register(Task)
        self._pubsub = self.get_pubsub()

    def __repr__(self):
        if self.schedule_periodic:
            return 'task scheduler %s' % self.store.dns
        else:
            return 'task consumer %s' % self.store.dns
    __str__ = __repr__

    @property
    def _loop(self):
        '''Eventloop running this task backend'''
        return self.store._loop

    @property
    def num_concurrent_tasks(self):
        '''The number of :attr:`concurrent_tasks`.

        This number is never greater than the :attr:`backlog` attribute.
        '''
        return len(self.concurrent_tasks)

    @lazyproperty
    def entries(self):
        return self._setup_schedule()

    @lazyproperty
    def registry(self):
        '''The :class:`.JobRegistry` for this backend.
        '''
        return JobRegistry.load(self.task_paths)

    def channel(self, name):
        '''Given an event ``name`` returns the corresponding channel name.

        The event ``name`` is one of ``task_queued``, ``task_started``
        or ``task_done``
        '''
        return '%s_%s' % (self.name, name)

    def event_name(self, channel):
        return channel[len(self.name)+1:]

    @task
    def queue_task(self, jobname, meta_params=None, expiry=None, **kwargs):
        '''Try to queue a new :ref:`Task`.

        This method returns a :class:`.Future` which results in the
        task ``id`` created. If ``jobname`` is not a valid
        :attr:`.Job.name`, a ``TaskNotAvailable`` exception occurs.

        :param jobname: the name of a :class:`.Job`
            registered with the :class:`.TaskQueue` application.
        :param meta_params: Additional parameters to be passed to the
            :class:`Task` constructor (not its callable function).
        :param expiry: optional expiry timestamp to override the default
            expiry of a task.
        :param kwargs: optional dictionary used for the key-valued arguments
            in the task callable.
        :return: a :class:`.Future` resulting in a task id on success.
        '''
        pubsub = self._pubsub
        if jobname in self.registry:
            job = self.registry[jobname]
            task_id, lock_id = self.generate_task_ids(job, kwargs)
            queued = time.time()
            if expiry is not None:
                expiry = get_time(expiry, queued)
            elif job.timeout:
                expiry = get_time(job.timeout, queued)
            meta_params = meta_params or {}
            task = self.models.task(id=task_id, lock_id=lock_id, name=job.name,
                                    time_queued=queued, expiry=expiry,
                                    kwargs=kwargs, status=states.QUEUED)
            if meta_params:
                task.update(meta_params)
            task = yield self.maybe_queue_task(task)
            if task:
                pubsub.publish(self.channel('task_queued'), task['id'])
                scheduled = self.entries.get(job.name)
                if scheduled:
                    scheduled.next()
                self.logger.debug('queued %s', task.lazy_info())
                coroutine_return(task['id'])
            else:
                self.logger.debug('%s cannot queue new task. Locked', jobname)
                coroutine_return()
        else:
            raise TaskNotAvailable(jobname)

    def wait_for_task(self, task_id, timeout=None):
        '''Asynchronously wait for a task with ``task_id`` to have finished
        its execution.
        '''
        # This coroutine is run on the worker event loop
        def _(task_id):
            task = yield self.get_task(task_id)
            if task:
                task_id = task['id']
                callbacks = self.callbacks
                if task.done():  # task done, simply return it
                    done = callbacks.pop(task_id, None)
                    if done:
                        done.set_result(task)
                else:
                    done = callbacks.get(task_id)
                    if not done:
                        # No future, create one
                        callbacks[task_id] = done = Future(loop=self._loop)
                    task = yield done
                coroutine_return(task)

        fut = async(_(task_id), self._loop)
        return future_timeout(fut, timeout) if timeout else fut

    def get_tasks(self, ids):
        return self.models.task.filter(id=ids).all()

    def get_pubsub(self):
        '''Create a publish/subscribe handler from the backend :attr:`store`.
        '''
        pubsub = self.store.pubsub()
        pubsub.add_client(self)
        # pubsub channels names from event names
        channels = tuple((self.channel(name) for name in self.events))
        pubsub.subscribe(*channels)
        self.bind_event('task_done', self.task_done_callback)
        return pubsub

    # #######################################################################
    # #    ABSTRACT METHODS
    # #######################################################################
    def maybe_queue_task(self, task):
        '''Actually queue a :class:`.Task` if possible.
        '''
        raise NotImplementedError

    def get_task(self, task_id=None):
        '''Asynchronously retrieve a :class:`Task` from a ``task_id``.

        :param task_id: the ``id`` of the task to retrieve.
        :return: a :class:`Task` or ``None``.
        '''
        raise NotImplementedError

    def finish_task(self, task_id, lock_id):
        '''Invoked at the end of task execution.

        The :class:`.Task` with ``task_id`` has been executed (either
        successfully or not) or has been revoked. This method perform
        backend specific operations.

        Must be implemented by subclasses.
        '''
        raise NotImplementedError

    def flush(self):
        '''Remove all queued :class:`.Task`
        '''
        raise NotImplementedError()

    # #######################################################################
    # #    START/CLOSE METHODS FOR TASK WORKERS
    # #######################################################################
    def start(self, worker):
        '''Invoked by the task queue ``worker`` when it starts.
        '''
        assert self.task_poller is None
        self.task_poller = worker._loop.call_soon(self.may_pool_task, worker)
        self.logger.debug('started polling tasks')
        store = self.store

    def close(self):
        '''Close this :class:`TaskBackend`.

        Invoked by the :class:`.Actor` when stopping.
        '''
        if self.task_poller:
            self.task_poller.cancel()
            self.task_poller = None
            self.logger.debug('stopped polling tasks')
        self._pubsub.close()

    def generate_task_ids(self, job, kwargs):
        '''An internal method to generate task unique identifiers.

        :parameter job: The :class:`.Job` creating the task.
        :parameter kwargs: dictionary of key-valued parameters passed to the
            :ref:`job callable <job-callable>` method.
        :return: a two-elements tuple containing the unique id and an
            identifier for overlapping tasks if the :attr:`.Job.can_overlap`
            results in ``False``.

        Called by the :ref:`TaskBackend <apps-taskqueue-backend>` when
        creating a new task.
        '''
        can_overlap = job.can_overlap
        if hasattr(can_overlap, '__call__'):
            can_overlap = can_overlap(**kwargs)
        tid = gen_unique_id()
        if can_overlap:
            return tid, None
        else:
            if kwargs:
                kw = ('%s=%s' % (k, kwargs[k]) for k in sorted(kwargs))
                name = '%s %s' % (self.name, ', '.join(kw))
            else:
                name = self.name
            return tid, sha1(name.encode('utf-8')).hexdigest()

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def tick(self, now=None):
        # Run a tick, that is one iteration of the scheduler.
        if not self.schedule_periodic:
            return
        remaining_times = []
        for entry in itervalues(self.entries):
            is_due, next_time_to_run = entry.is_due(now=now)
            if is_due:
                self.queue_task(entry.name)
            if next_time_to_run:
                remaining_times.append(next_time_to_run)
        self.next_run = now or time.time()
        if remaining_times:
            self.next_run += min(remaining_times)

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
            d = {'doc': job.__doc__,
                 'doc_syntax': job.doc_syntax,
                 'type': job.type,
                 'can_overlap': can_overlap}
            if self.entries and name in self.entries:
                entry = self.entries[name]
                _, next_time_to_run = self.next_scheduled((name,))
                run_every = 86400*job.run_every.days + job.run_every.seconds
                d.update({'next_run': next_time_to_run,
                          'run_every': run_every,
                          'runs_count': entry.total_run_count})
            all.append((name, d))
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

    @task
    def may_pool_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = 0
        if worker.is_running():
            executor = worker.executor()
            if self.num_concurrent_tasks < self.backlog:
                if self.max_tasks and self.processed >= self.max_tasks:
                    if not self.num_concurrent_tasks:
                        self.logger.warning('Processed %s tasks. Restarting.',
                                            self.processed)
                        worker._loop.stop()
                        coroutine_return()
                else:
                    task = yield self.get_task()
                    if task:    # Got a new task
                        self.processed += 1
                        self.concurrent_tasks.add(task['id'])
                        executor.submit(self._execute_task, worker, task)
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                next_time = 1
        worker._loop.call_later(next_time, self.may_pool_task, worker)

    def _execute_task(self, worker, task):
        # Asynchronous execution of a Task. This method is called
        # on a separate thread of execution from the worker event loop thread.
        logger = get_logger(worker.logger)
        pubsub = self._pubsub
        task_id = task.id
        lock_id = task.get('lock_id')
        time_ended = time.time()
        job = self.registry.get(task.get('name'))
        consumer = TaskConsumer(self, worker, task_id, job)
        task_info = task.lazy_info()
        try:
            if not consumer.job:
                raise RuntimeError('%s not in registry' % task_info)
            if task['status'] > states.STARTED:
                expiry = task.get('expiry')
                if expiry and time_ended > expiry:
                    raise TaskTimeout
                else:
                    logger.info('starting %s', task_info)
                    kwargs = task.get('kwargs') or {}
                    self.models.task.update(task, status=states.STARTED,
                                            time_started=time_ended,
                                            worker=worker.aid)
                    pubsub.publish(self.channel('task_started'), task_id)
                    # This may block for a while
                    result = yield job(consumer, **kwargs)
                    status = states.SUCCESS
            else:
                logger.error('invalid status for %s', task_info)
                self.concurrent_tasks.discard(task_id)
                coroutine_return(task_id)
        except TaskTimeout:
            logger.info('%s timed-out', task_info)
            result = None
            status = states.REVOKED
        except Exception as exc:
            logger.exception('failure in %s', task_info)
            result = str(exc)
            status = states.FAILURE
        #
        try:
            yield self.models.task.update(task, time_ended=time.time(),
                                          status=status, result=result)
        finally:
            self.concurrent_tasks.discard(task_id)
            self.finish_task(task_id, lock_id)
        #
        logger.info('finished %s', task_info)
        # publish into the task_done channel
        pubsub.publish(self.channel('task_done'), task_id)
        coroutine_return(task_id)

    def _setup_schedule(self):
        entries = {}
        if not self.schedule_periodic:
            return entries
        for name, task in self.registry.filter_types('periodic'):
            every = task.run_every
            if isinstance(every, int):
                every = timedelta(seconds=every)
            if not isinstance(every, timedelta):
                raise ValueError('Schedule %s is not a timedelta' % every)
            entries[name] = SchedulerEntry(name, every, task.anchor)
        return entries

    def task_done_callback(self, task_id, exc=None):
        # Got a task_id from the ``<name>_task_done`` channel.
        # Check if a ``callback`` is available in the :attr:`callbacks`
        # dictionary. If so fire the callback with the ``task`` instance
        # corresponsding to the input ``task_id``.
        # If a callback is not available, it must have been fired already
        self.wait_for_task(task_id)

    def __call__(self, channel, message):
        # PubSub callback
        name = self.event_name(channel)
        self.fire_event(name, message)


class SchedulerEntry(object):
    '''A class used as a schedule entry by the :class:`.TaskBackend`.

    .. attribute:: name

        Task name

    .. attribute:: run_every

        Interval in seconds

    .. attribute:: anchor

        Datetime anchor

    .. attribute:: last_run_at

        last run datetime

    .. attribute:: total_run_count

        Total number of times this periodic task has been executed by the
        :class:`.TaskBackend`.
    '''
    def __init__(self, name, run_every, anchor=None):
        self.name = name
        self.run_every = run_every
        self.anchor = anchor
        self.last_run_at = datetime.now()
        self.total_run_count = 0

    def __repr__(self):
        return self.name
    __str__ = __repr__

    @property
    def scheduled_last_run_at(self):
        '''The scheduled last run datetime.

        This is different from :attr:`last_run_at` only when
        :attr:`anchor` is set.
        '''
        last_run_at = self.last_run_at
        anchor = self.anchor
        if last_run_at and anchor:
            run_every = self.run_every
            times = int(timedelta_seconds(last_run_at - anchor)
                        / timedelta_seconds(run_every))
            if times:
                anchor += times*run_every
                while anchor <= last_run_at:
                    anchor += run_every
                while anchor > last_run_at:
                    anchor -= run_every
                self.anchor = anchor
            return anchor
        else:
            return last_run_at

    def next(self, now=None):
        '''Increase the :attr:`total_run_count` attribute by one and set the
        value of :attr:`last_run_at` to ``now``.
        '''
        self.last_run_at = now or datetime.now()
        self.total_run_count += 1

    def is_due(self, now=None):
        '''Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        See :meth:`unuk.contrib.tasks.models.PeriodicTask.is_due`
        for more information.
        '''
        last_run_at = self.scheduled_last_run_at
        now = now or datetime.now()
        rem_delta = last_run_at + self.run_every - now
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem


class PulsarTaskBackend(TaskBackend):

    @lazyproperty
    def store_client(self):
        return self.store.client()

    def maybe_queue_task(self, task):
        free = True
        store = self.store
        c = self.channel
        if task['lock_id']:
            free = yield store.execute('hsetnx', c('locks'),
                                       task['lock_id'], task['id'])
        if free:
            with self.models.begin() as t:
                t.add(task)
                t.execute('lpush', c('inqueue'), task.id)
            yield t.wait()
            coroutine_return(task)
        else:
            coroutine_return()

    def get_task(self, task_id=None):
        store = self.store
        if not task_id:
            inq = self.channel('inqueue')
            ouq = self.channel('outqueue')
            task_id = yield store.execute('brpoplpush', inq, ouq,
                                          self.poll_timeout)
            if not task_id:
                coroutine_return()
        task = yield self.models.task.get(task_id)
        coroutine_return(task or None)

    def finish_task(self, task_id, lock_id):
        store = self.store
        pipe = store.pipeline()
        if lock_id:
            pipe.hdel(self.channel('locks'), lock_id)
        # Remove the task_id from the inqueue list
        pipe.lrem(self.channel('inqueue'), 0, task_id)
        return pipe.commit()

    def get_tasks(self, ids):
        base = self.models.task._meta.table_name
        store = self.models.task._read_store
        pipeline = store.pipeline()
        for pk in ids:
            pipeline.hgetall('%s:%s' % (base, pk),
                             factory=partial(store.build_model, Task))
        return pipeline.commit()

    def flush(self):
        return self.store.flush()


task_backends['pulsar'] = PulsarTaskBackend
task_backends['redis'] = PulsarTaskBackend
