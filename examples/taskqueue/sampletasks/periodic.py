from datetime import timedelta
from random import random

from pulsar.apps.tasks import PeriodicTask,  anchorDate

class TestPeriodicTask(PeriodicTask):
    abstract = True
    run_every = timedelta(hours=1)


class TestPeriodic(TestPeriodicTask):
        
    def __call__(self, consumer):
        return 'OK'
    
    
class TestPeriodicError(TestPeriodicTask):
    
    def __call__(self, consumer):
        raise Exception('kaputt')
    

class AnchoredEveryHour(TestPeriodicTask):
    anchor    = anchorDate(minute = 25)
    
    def __call__(self, consumer):
        raise Exception('kaputt')
    
    
#class FastAndFurious(PeriodicTask):
#    run_every = timedelta(seconds=0.1)
#    
#    def __call__(self, consumer):
#        return random()
    
