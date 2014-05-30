import time
import math
from datetime import timedelta
from random import random
from functools import reduce

from pulsar import asyncio, get_request_loop, coroutine_return
from pulsar.apps import tasks


class RunPyCode(tasks.Job):
    '''execute python code in *code*. There must be a *task_function*
function defined which accept key-valued parameters only.'''
    timeout = timedelta(seconds=60)

    def __call__(self, consumer, code, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local, ns)
        func = ns['task_function']
        return func(**kwargs)


class Addition(tasks.Job):
    timeout = timedelta(seconds=60)

    def __call__(self, consumer, a=0, b=0):
        return a + b


class Asynchronous(tasks.Job):

    def __call__(self, consumer, lag=1):
        start = time.time()
        yield asyncio.sleep(lag)
        coroutine_return(time.time() - start)


class NotOverLap(tasks.Job):
    can_overlap = False

    def __call__(self, consumer, lag=1):
        start = time.time()
        yield asyncio.sleep(lag)
        coroutine_return(time.time() - start)


class CheckWorker(tasks.Job):

    def __call__(self, consumer):
        worker = consumer.worker
        backend = worker.app.backend
        return {'tasks': list(backend.concurrent_tasks)}


class StandardDeviation(tasks.Job):

    def can_overlap(self, inputs=None, **kwargs):
        return inputs is not None

    def __call__(self, consumer, inputs=None, sample=10, size=100):
        if inputs is None:
            for n in range(sample):
                inputs = [random() for i in range(size)]
                self.queue_task(consumer, self.name, inputs=inputs)
            return 'produced %s new tasks' % sample
        else:
            time.sleep(0.1)
            v2 = reduce(lambda x, y: x+y,
                        map(lambda x: x*x, inputs))/len(inputs)
            return math.sqrt(v2)
