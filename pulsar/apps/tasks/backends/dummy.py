from pulsar import send, command
from pulsar.apps.tasks import backends


class TaskBackend(backends.TaskBackend):
    
    def get_task(self, task_id):
        pass
        
    def save_task(self, task_id, **params):
        pass
    
    def delete_tasks(self, ids=None):
        pass