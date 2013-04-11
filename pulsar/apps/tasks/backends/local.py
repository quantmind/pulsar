from collections import namedtuple, deque

from pulsar import send, command
from pulsar.apps.tasks import backends, states


class TaskBackend(backends.TaskBackend):
        
    def put_task(self, task_id):
        return send('arbiter', 'put_task', task_id)
    
    def get_task(self, task_id=None):
        return send('arbiter', 'get_task', task_id)
        
    def save_task(self, task_id, **params):
        return send('arbiter', 'save_task', task_id, **params)
    
    def delete_tasks(self, ids=None):
        return send('arbiter', 'delete_tasks', ids)
    
        
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
    return task.id

@command()
def delete_tasks(request, ids):
    TASKS = _get_tasks(request.actor)
    if ids:
        for id in ids:
            TASKS.map.pop(id, None)
    else:
        TASKS.map.clear()

@command()
def get_task(request, task_id=None):
    TASKS = _get_tasks(request.actor)
    if task_id:
        return TASKS.map.get(task_id)
    else:
        try:
            task_id = TASKS.queue.popleft()
            return TASKS.map.get(task_id)
        except IndexError:
            return None

@command()
def get_tasks(request, **filters):
    TASKS = _get_tasks(request.actor)
    tasks = []
    if filters:
        fs = []
        for name, value in iteritems(filters):
            if not isinstance(value, (list, tuple)):
                value = (value,)
            fs.append((name, value))
        for t in itervalues(TASKS):
            for name, values in fs:
                value = getattr(t, name, None)
                if value in values:
                    tasks.append(t)
    return tasks

@command()
def put_task(request, task_id):
    TASKS = _get_tasks(request.actor)
    task = TASKS.map.get(task_id)
    if task:
        task.status = states.QUEUED 
        TASKS.queue.append(task.id)
        return task.id


local_tasks = namedtuple('local_tasks', 'map queue')

def _get_tasks(actor):
    tasks = actor.params.TASKQUEUE_TASKS
    if tasks is None:
        actor.params.TASKQUEUE_TASKS = tasks = local_tasks({}, deque())
    return tasks