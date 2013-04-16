'''
An implementation of a :class:`TaskBackend` which uses redis as data server.
Requires python-stdnet_

.. _python-stdnet: https://pypi.python.org/pypi/python-stdnet
'''
from stdnet import odm

from pulsar import Deferred, async
from pulsar.apps.tasks import backends, states
from pulsar.apps.pubsub import PubSub


class TaskData(odm.StdModel):
    id = odm.SymbolField(primary_key=True)
    name = odm.SymbolField()
    run_id = odm.SymbolField()
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
            params[name] = getattr(self, field.attname, None)
        return backends.Task(self.id, **params)
    
    class Meta:
        app_label = 'tasks'


class TaskBackend(backends.TaskBackend):
    
    def setup(self, **params):
        params = super(TaskBackend, self).setup(**params)
        # We need to register the model with backend server
        odm.register(TaskData, self.connection_string)
        # Create the PubSub for broadcasting done tasks
        self.local.pubsub = PubSub.make(backend=self.connection_string,
                                        name=self.name)
        self.local.watched_tasks = {}
        self.local.pubsub.add_client(self)
        return params
        
    def put_task(self, task_id):
        tasks = yield TaskData.objects.filter(id=task_id)
        if tasks:
            task_data = tasks[0]
            task_data.status = states.PENDING
            task_data = yield task_data.save()
            TaskData.queue.push_back(task_data.id)
            yield task_data.id
        
    def get_task(self, task_id=None, when_done=False, timeout=1):
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
                    if fut:
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
        tasks = yield TaskData.objects.filter(**filters).all()
        yield [t.as_task() for t in tasks]
        
    def save_task(self, task_id, **params):
        if task_id not in self.local.watched_tasks:
            self.local.watched_tasks[task_id] = Deferred()
        tasks = yield TaskData.objects.filter(id=task_id).all()
        if tasks:
            task = tasks[0]
            for field, value in params.items():
                if field in task._meta.dfields:
                    setattr(task, field, value)
                else:
                    task.meta[field] = value
        else:
            task = TaskData(id=task_id, **params)
        yield task.save()
        if task.done():
            # task is done, publish task id into the channel
            self.local.pubsub.publish(task_id)
        
    @async()
    def write(self, message):
        fut = self.local.watched_tasks.pop(message, None)
        if fut:
            task = yield TaskData.objects.filter(id=task_id)
            task = tasks[0].as_task() if tasks else None
            fut.callback(task)
    