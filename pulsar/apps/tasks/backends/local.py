from collections import namedtuple, deque

from pulsar import send, command, Deferred
from pulsar.utils.pep import itervalues
from pulsar.apps.tasks import backends, states


class TaskBackend(backends.TaskBackend):
        
    def put_task(self, task_id):
        return send('arbiter', 'put_task', task_id)
    
    def get_task(self, task_id=None, when_done=False):
        return send('arbiter', 'get_task', task_id, when_done)
    
    def get_tasks(self, **filters):
        return send('arbiter', 'get_tasks', **filters)
        
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
            return send('arbiter', 'save_task', task_id, **params)
    
    def delete_tasks(self, ids=None):
        return send('arbiter', 'delete_tasks', ids)
    
        
#################################################    TASKQUEUE COMMANDS
@command()
def save_task(request, task_id, **params):
    TASKS = _get_tasks(request.actor)
    if task_id in TASKS.map:
        task, when_done = TASKS.map[task_id]
        for field, value in params.items():
            setattr(task, field, value)
    else:
        task = backends.Task(task_id, **params)
        when_done = Deferred()
        TASKS.map[task.id] = (task, Deferred())
    if task.done() and not when_done.done():
        when_done.callback(task)
    return task.id

@command()
def delete_tasks(request, ids):
    TASKS = _get_tasks(request.actor)
    ids = ids or list(TASKS.map)
    deleted = 0
    for id in ids:
        if id not in TASKS.map:
            continue
        deleted += 1
        task, when_done = TASKS.map.pop(id)
        if not when_done.done():
            if not task.done():
                task.status = states.REVOKED
            when_done.callback(task)
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
        task, when_done_event = TASKS.map[task_id]
        return when_done_event if when_done else task

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
        for t, _ in itervalues(TASKS.map):
            select = True
            for name, values in fs:
                value = getattr(t, name, None)
                if value not in values:
                    select = False
                    break
            if select:
                tasks.append(t)
    return tasks

@command()
def put_task(request, task_id):
    TASKS = _get_tasks(request.actor)
    if task_id in TASKS.map:
        task, _ = TASKS.map[task_id]
        task.status = states.QUEUED 
        TASKS.queue.append(task.id)
        return task.id


local_tasks = namedtuple('local_tasks', 'map queue')

def _get_tasks(actor):
    tasks = actor.params.TASKQUEUE_TASKS
    if tasks is None:
        actor.params.TASKQUEUE_TASKS = tasks = local_tasks({}, deque())
    return tasks