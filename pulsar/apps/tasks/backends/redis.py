'''
An implementation of a :class:`TaskBackend` which uses redis as data server.
Requires python-stdnet_

.. _python-stdnet: https://pypi.python.org/pypi/python-stdnet
'''
from stdnet import odm

from pulsar import Deferred, async
from pulsar.apps.tasks import backends, states
from pulsar.utils.log import local_method
from pulsar.apps.pubsub import PubSub


class TaskData(odm.StdModel):
    id = odm.SymbolField(primary_key=True)
    name = odm.SymbolField()
    status = odm.SymbolField()
    args = odm.PickleObjectField()
    kwargs = odm.PickleObjectField()
    result = odm.PickleObjectField()
    from_task = odm.SymbolField(required=False)
    time_executed = odm.DateTimeField(index=False)
    time_started = odm.DateTimeField(required=False, index=False)
    time_ended = odm.DateTimeField(required=False, index=False)
    expiry = odm.DateTimeField(required=False, index=False)
    meta = odm.JSONField()
    queue = odm.ListField(class_field=True)
    
    def as_task(self):
        params = dict(self.meta or {})
        for field in self._meta.dfields.values():
            params[field.name] = getattr(self, field.attname, None)
        id = params.pop('id')
        return backends.Task(id, **params)
    
    class Meta:
        app_label = 'tasks'


class TaskBackend(backends.TaskBackend):
    
    @local_method
    def load(self):
        p = PubSub(backend=self.connection_string, name=self.name)
        p.add_client(self)
        p.subscribe('task_done')
        self.local.pubsub = p
        self.local.watched_tasks = {}
        odm.register(TaskData, self.connection_string)
    
    def put_task(self, task_id):
        self.load()
        tasks = yield TaskData.objects.filter(id=task_id)
        if tasks:
            task_data = tasks[0]
            task_data.status = states.PENDING
            task_data = yield task_data.save()
            TaskData.queue.push_back(task_data.id)
            yield task_data.id
        
    def get_task(self, task_id=None, when_done=False, timeout=1):
        self.load()
        if not task_id:
            task_id = yield TaskData.queue.block_pop_front(timeout=timeout)
        if task_id:
            if task_id not in self.local.watched_tasks:
                self.local.watched_tasks[task_id] = Deferred()
            tasks = yield TaskData.objects.filter(id=task_id).all()
            if tasks:
                task_data = tasks[0]
                task = task_data.as_task()
                if when_done:
                    fut = self.local.watched_tasks.get(task_id)
                    if fut: # future not yet called
                        if task.done():
                            self.local.watched_tasks.pop(task_id, None)
                            yield task
                        else:
                            yield fut
                    else:
                        # the future was called already, the task should be done
                        yield task
                else:
                    yield task
        
    def get_tasks(self, **filters):
        self.load()
        tasks = yield TaskData.objects.filter(**filters).all()
        yield [t.as_task() for t in tasks]
        
    def save_task(self, task_id, **params):
        self.load()
        if task_id not in self.local.watched_tasks:
            self.local.watched_tasks[task_id] = Deferred()
        tasks = yield TaskData.objects.filter(id=task_id).all()
        if tasks:
            task_data = tasks[0]
            for field, value in params.items():
                if field in task._meta.dfields:
                    setattr(task_data, field, value)
                else:
                    task_data.meta[field] = value
        else:
            task_data = TaskData(id=task_id, **params)
        yield task_data.save()
        task = task_data.as_task()
        if task.done():
            # task is done, publish task_id into the channel
            self.local.pubsub.publish('task_done', task_id)
        yield task_id
        
    @async()
    def write(self, message):
        '''Got a new message from redis pubsub task_done channel.'''
        self.load()
        fut = self.local.watched_tasks.pop(message, None)
        if fut:
            task = yield TaskData.objects.filter(id=task_id)
            task = tasks[0].as_task() if tasks else None
            fut.callback(task)
    