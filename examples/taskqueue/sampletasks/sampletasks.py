from pulsar.apps.tasks import Job


class RunPyCode(Job):
    '''execute python code in *code*. There must be a *task_function*
function defined.'''
    def __call__(self, consumer, code, *args, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local,ns)
        func = ns['task_function']
        return func(*args,**kwargs)
        

class Addition(Job):
    
    def __call__(self, consumer, a, b):
        return a+b