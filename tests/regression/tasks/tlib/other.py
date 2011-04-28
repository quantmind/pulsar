

from unuk.contrib.tasks import Task



class Addition(Task):
    
    def run(self, task_name, task_id, logger, a, b):
        return a+b
    
    
class ValueErrorTask(Task):
    
    def run(self, task_name, task_id, logger, *args, **kwargs):
        raise ValueError