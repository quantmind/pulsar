'''
An implementation of a :class:`TaskBackend` which uses redis as data server.
'''
from stdnet import odm

from pulsar.apps.tasks import backends, states


class TaskData(odm.Task):
    id = odm.SymbolField(primary_key=True)
    name = odm.SymbolField()
    status = odm.SymbolField()
    meta = odm.JSONField()
    queue = odm.ListField(class_field=True)
    
    def as_task(self):
        return backends.Task(self.id, name=self.name, status=self.status,
                             **self.meta)


class TaskBackend(backends.TaskBackend):
    
    def put_task(self, task_id):
        tasks = yield TaskData.objects.filter(id=task_id)
        if tasks:
            task_data = tasks[0]
            task_data.status = states.PENDING
            task_data = yield task_data.save()
            TaskData.queue.push_back(task_data.id)
            yield task_data.id
        
    def get_task(self, task_id=None):
        if not task_id:
            task_id = yield TaskData.queue.pop_front()
        task_data = None
        if task_id:
            tasks = yield TaskData.objects.filter(id=task_id)
            if tasks:
                task_data = tasks[0]
        yield task_data.as_task() if task_data else None
        
    def save_task(self, task_id, **params):
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
        