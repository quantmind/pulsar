import time
from datetime import timedelta

from pulsar import thread_loop, NOT_DONE
from pulsar.apps import tasks


class RunPyCode(tasks.Job):
    '''execute python code in *code*. There must be a *task_function*
function defined which accept key-valued parameters only.'''
    timeout = timedelta(seconds=60)
    def __call__(self, consumer, code, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local,ns)
        func = ns['task_function']
        return func(**kwargs)
        

class Addition(tasks.Job):
    timeout = timedelta(seconds=60)
    def __call__(self, consumer, a, b):
        return a+b
    
    
class Asynchronous(tasks.Job):
    
    def __call__(self, consumer, loops=1):
        loop = 0
        rl = thread_loop()
        start = rl.num_loops
        while loop < loops:
            yield NOT_DONE
            loop += 1
        yield {'start': start,
               'end': rl.num_loops,
               'loops': loop}
    
    
class NotOverLap(tasks.Job):
    can_overlap = False
    
    def __call__(self, consumer, lag):
        time.sleep(lag)
        return 'OK'