import time
from datetime import timedelta

from pulsar import get_request_loop, async_sleep
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
        return a + b
    
    
class Asynchronous(tasks.Job):
    
    def __call__(self, consumer, lag=1):
        rl = get_request_loop()
        start = time.time()
        loop = rl.num_loops
        yield async_sleep(lag)
        yield {'time': time.time() - start,
               'loops': rl.num_loops - loop}
    
    
class NotOverLap(tasks.Job):
    can_overlap = False
    
    def __call__(self, consumer, lag):
        start = time.time()
        yield async_sleep(lag)
        yield time.time() - start
        
        
class CheckWorker(tasks.Job):
    
    def __call__(self, consumer):
        worker = consumer.worker
        backend = worker.app.backend
        return {'tasks': list(backend.concurrent_tasks)}