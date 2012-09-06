from datetime import timedelta
from random import random

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
    anchor    = anchorDate(minute=25)
    def __call__(self, consumer):
        raise Exception('kaputt')
    
    
#class FastAndFurious(PeriodicJob):
#    run_every = timedelta(seconds=0.1)
#    
#    def __call__(self, consumer):
#        return random()
    
