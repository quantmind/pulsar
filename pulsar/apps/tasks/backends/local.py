from collections import deque

from pulsar import send, command
from pulsar.utils.pep import itervalues
from pulsar.apps.tasks import backends, states
from pulsar.apps.tasks.backends import TaskCallbacks


class TaskBackend(backends.TaskBackend):
        
    def put_task(self, task_id):
        return send(self.name, 'put_task', task_id)
    
    def get_task(self, task_id=None, when_done=False, timeout=1):
        return send(self.name, 'get_task', task_id, when_done, timeout)
    
    def get_tasks(self, **filters):
        return send(self.name, 'get_tasks', **filters)
        
    def save_task(self, task_id, **params):
        from pulsar import get_actor
        if not get_actor():
            from threading import current_thread
            print('##############################################################')
            print('##############################################################')
            print('THREAD %s SAVING TASK' % current_thread().name)
            print('##############################################################')
            return task_id
        else:
            return send(self.name, 'save_task', task_id, **params)
    
    def delete_tasks(self, ids=None):
        return send(self.name, 'delete_tasks', ids)
    
        
#################################################    TASKQUEUE COMMANDS
@command()
def save_task(request, task_id, **params):
    TASKS = _get_tasks(request.actor)
    if task_id in TASKS.map:
        task = TASKS.map[task_id]
        for field, value in params.items():
            setattr(task, field, value)
    else:
        task = backends.Task(task_id, **params)
        TASKS.map[task.id] = task
    TASKS.when_done(task)
    return task.id

@command()
def delete_tasks(request, ids):
    TASKS = _get_tasks(request.actor)
    ids = ids or list(TASKS.map)
    deleted = 0
    for id in ids:
        task = TASKS.map.pop(id, None)
        if task:
            deleted += 1
            TASKS.finish(task)
    return deleted

@command()
def get_task(request, task_id=None, when_done=False, timeout=1):
    TASKS = _get_tasks(request.actor)
    if not task_id:
        try:
            task_id = TASKS.queue.popleft()
        except IndexError:
            return None
    if task_id in TASKS.map:
        task = TASKS.map[task_id]
        if when_done:
            return TASKS.when_done(task)
        else:
            return task

@command()
def get_tasks(request, **filters):
    TASKS = _get_tasks(request.actor)
    tasks = []
    if filters:
        fs = []
        for name, value in filters.items():
            if not isinstance(value, (list, tuple, set, frozenset)):
                value = (value,)
            fs.append((name, value))
        # Loop over tasks
        for task in itervalues(TASKS.map):
            select = True
            for name, values in fs:
                value = getattr(task, name, None)
                if value not in values:
                    select = False
                    break
            if select:
                tasks.append(task)
    return tasks

@command()
def put_task(request, task_id):
    TASKS = _get_tasks(request.actor)
    if task_id in TASKS.map:
        task = TASKS.map[task_id]
        task.status = states.QUEUED 
        TASKS.queue.append(task.id)
        return task.id


class LocalTasks(TaskCallbacks):
    
    def __init__(self):
        super(LocalTasks, self).__init__()
        self.map = {}
        self.queue = deque()


def _get_tasks(actor):
    tasks = actor.params.TASKQUEUE_TASKS
    if tasks is None:
        actor.params.TASKQUEUE_TASKS = tasks = LocalTasks()
    return tasks