import math
import time
from datetime import timedelta
from random import random
from functools import reduce

from pulsar.apps.tasks import PeriodicJob,  anchorDate


class TestPeriodicJob(PeriodicJob):
    abstract = True
    run_every = timedelta(hours=1)


class TestPeriodic(TestPeriodicJob):
    run_every = timedelta(seconds=10)
    def __call__(self, consumer):
        return 'OK'
    

class TestPeriodicError(TestPeriodicJob):
    run_every = timedelta(seconds=60)
    def __call__(self, consumer):
        raise Exception('kaputt')
    

class AnchoredEveryHour(TestPeriodicJob):
    anchor = anchorDate(minute=25)
    def __call__(self, consumer):
        raise Exception('kaputt')
    
    
class StandardDeviation(TestPeriodicJob):
    
    def can_overlap(self, inputs=None, **kwargs):
        return inputs is not None
    
    def __call__(self, consumer, inputs=None, sample=10, size=100):
        if inputs is None:
            for n in range(sample):
                inputs = [random() for i in range(size)]
                self.send_to_queue(consumer, self.name, inputs=inputs)
            return 'produced %s new tasks' % sample
        else:
            time.sleep(0.1)
            v2 = reduce(lambda x,y: x+y, map(lambda x: x*x, inputs))/len(inputs)
            return math.sqrt(v2)
            
        
