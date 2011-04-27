
from pulsar.apps.tasks import Task, PeriodicTask


class CodeTask(Task):
    
    def __call__(self, consumer, code, *args, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local,ns)
        func = ns['task_function']
        return func(*args,**kwargs)
        