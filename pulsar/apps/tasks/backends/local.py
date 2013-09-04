'''
The local task backend store tasks in pulsar process domain and therefore
is accessed only from one running task queue.
'''
from pulsar import send, command, Queue, Empty, coroutine_return
from pulsar.utils.pep import itervalues
from pulsar.apps.tasks import backends, states
from pulsar.apps.pubsub import PubSub


class TaskBackend(backends.TaskBackend):
        
    def put_task(self, task_id):
        if task_id:
            return send(self.name, 'put_task', task_id)
    
    def get_task(self, task_id=None):
        return send(self.name, 'get_task', task_id, self.poll_timeout)
    
    def get_tasks(self, **filters):
        return send(self.name, 'get_tasks', **filters)
        
    def save_task(self, task_id, **params):
        return send(self.name, 'save_task', task_id, **params)
    
    def delete_tasks(self, ids=None):
        return send(self.name, 'delete_tasks', ids or ())
    
    def flush(self):
        return send(self.name, 'delete_tasks', None)
    

#########################################################    INTERNALS

class LocalTaskBackend(object):
    
    def __init__(self, name):
        self.pubsub = PubSub(name=name)
        self._init()
    
    def put_task(self, task_id):
        if task_id in self._tasks:
            task = self._tasks[task_id]
            task.status = states.QUEUED 
            yield self.queue.put(task.id)
            yield task.id

    def get_task(self, task_id, timeout):        
        if not task_id:
            try:
                task_id = yield self.queue.get(timeout)
            except Empty:
                coroutine_return()
        yield self._tasks.get(task_id)
        
    def save_task(self, task_id, **params):
        task = self._tasks.get(task_id)
        if task:
            for field, value in params.items():
                setattr(task, field, value)
        else: # create a new task
            task = backends.Task(task_id, **params)
            self._tasks[task.id] = task
        return task.id

    def delete_tasks(self, ids):
        if ids is None:
            deleted = list(self._tasks)
            self._init()
        else:
            deleted = []
            for id in ids:
                task = self._tasks.pop(id, None)
                if task:
                    deleted.append(id)
        return deleted
    
    def get_tasks(self, **filters):
        tasks = []
        if filters:
            fs = []
            for name, value in filters.items():
                if not isinstance(value, (list, tuple, set, frozenset)):
                    value = (value,)
                fs.append((name, value))
            # Loop over tasks
            for task in itervalues(self._tasks):
                select = True
                for name, values in fs:
                    value = getattr(task, name, None)
                    if value not in values:
                        select = False
                        break
                if select:
                    tasks.append(task)
        return tasks
    
    def _init(self):
        self._tasks = {}
        self.queue = Queue()
    
#################################################    TASKQUEUE COMMANDS
@command()
def save_task(request, task_id, **params):
    return _get_tasks(request.actor).save_task(task_id, **params)

@command()
def delete_tasks(request, ids):
    return _get_tasks(request.actor).delete_tasks(ids)

@command()
def get_task(request, task_id=None, timeout=1):
    return _get_tasks(request.actor).get_task(task_id, timeout)

@command()
def get_tasks(request, **filters):
    return _get_tasks(request.actor).get_tasks(**filters)

@command()
def put_task(request, task_id):
    return _get_tasks(request.actor).put_task(task_id)

def _get_tasks(actor):
    tasks = getattr(actor, '_TASKQUEUE_TASKS', None)
    if tasks is None:
        actor._TASKQUEUE_TASKS = tasks = LocalTaskBackend(name=actor.name)
    return tasks